//! `SeekSage` — a high-performance, multi-threaded text search engine.
//!
//! Transport layer: [`queue`] — lock-free exchange between threads.
//! Ingestion layer: [`parser`] — directory crawl and streaming chunk delivery.
//! Index layer: [`index`] — inverted index, posting lists, parallel merge.
//!
//! The crate is dual-target: a `rlib` + `cdylib` library (`seeksage`) and a CLI
//! binary (`seeksage`). The library binds the three modules into a stable C-ABI
//! surface around [`SeekSageEngine`]:
//!
//! - [`seeksage_build_index`]   crawl + build the merged index, return an opaque handle
//! - [`seeksage_search`]        run one AND query, return a JSON string
//! - [`seeksage_string_free`]   free a string returned by [`seeksage_search`]
//! - [`seeksage_free`]          destroy an engine handle
//!
//! Every FFI boundary is `unsafe`, null-checked, and wrapped in
//! `std::panic::catch_unwind` so a panic inside the engine can never unwind
//! across the C ABI. The engine is immutable after building, so
//! [`seeksage_search`] is safe to call from many threads concurrently;
//! [`seeksage_free`] must be the last call for a given handle.
//!
//! Export layer: [`export`] — streaming JSON, CSV, and Markdown renderers for a
//! filtered hit list, written straight to a `Write` sink.
//!
//! Result rendering stays bounded in memory: [`extract_snippet`] seeks a fixed
//! 1 KiB window around a hit's byte offset, so a line preview never loads a
//! whole file, and a multi-gigabyte repository costs one small seek plus one
//! small read per previewed line. [`extract_context`] does the same with a 4 KiB
//! window for a GUI that shows the hit line plus its neighbouring lines.

pub mod export;
pub mod index;

// Lint exceptions scoped to the two frozen modules so the whole workspace stays
// clippy-clean while `queue.rs` and `parser.rs` are out of scope for this
// upgrade. The index, library, and CLI modules keep every lint enabled.
#[allow(
    clippy::io_other_error,
    clippy::assertions_on_constants,
    clippy::bool_assert_comparison,
    clippy::explicit_counter_loop
)]
pub mod parser;
pub mod queue;

use std::ffi::{c_char, CStr, CString};
use std::fs;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Instant, UNIX_EPOCH};

use index::merge_shards;
use index::GlobalIndex;
use index::IndexShard;
use index::SearchResult;
use index::{put_f64, put_str, put_u32, put_u64, Reader};
use parser::{
    crawl, drain_until_done, partition, run_producer, ConsumerStats, FileRecord, ProducerStats,
    WalkStats,
};
use queue::Lanes;

/// Producer lane slot capacity, shared by the CLI binary and the C-ABI build.
pub const LANE_CAP: usize = 8192;

/// Producer lanes chosen from available parallelism, clamped to `2..=8`.
#[must_use]
pub fn default_lanes() -> usize {
    let n = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    n.clamp(2, 8)
}

// ---------------------------------------------------------------------------
// Bounded-memory snippet extraction
// ---------------------------------------------------------------------------

/// Bytes sampled before a hit when locating the enclosing line.
pub const SNIPPET_LEAD: u64 = 256;
/// Fixed stack window read by [`extract_snippet`], so one call never reads
/// more than this many bytes into memory.
pub const SNIPPET_WINDOW: usize = 1024;
/// Bytes sampled before a hit when assembling a GUI context window.
pub const CONTEXT_LEAD: u64 = 2048;
/// Fixed stack window read by [`extract_context`].
pub const CONTEXT_WINDOW: usize = 4096;

/// Seek to `window_start` and fill `buf` from `file`, returning the resulting
/// position and the number of bytes read. A window that opens at or past EOF
/// reads nothing.
fn read_window(file: &mut File, window_start: u64, buf: &mut [u8]) -> io::Result<(u64, usize)> {
    let pos = file.seek(SeekFrom::Start(window_start))?;
    let mut filled = 0usize;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok((pos, filled))
}

/// Index to start decoding at, skipping the continuation bytes of a 2-, 3-, or
/// 4-byte sequence split by `window_start` (up to 3 bytes), so a split
/// character never surfaces as a leading `U+FFFD`. Only applies to a window
/// that opens inside a file: at `pos == 0` the window starts at byte 0.
fn trim_lead_partial(buf: &[u8], pos: u64, filled: usize) -> usize {
    if pos == 0 {
        return 0;
    }
    let mut skip = 0usize;
    while skip < 3 && skip < filled && (buf[skip] & 0xC0) == 0x80 {
        skip += 1;
    }
    skip
}

/// End index for a full window, dropping a trailing multi-byte sequence that
/// the window cut short: the last lead byte inside the window is asked for
/// `4`, `3`, `2`, or `1` bytes, and the sequence is kept only if they all fit.
/// A window that ended at EOF is left intact, since that sequence is genuinely
/// incomplete in the file.
fn trim_tail_partial(buf: &[u8], filled: usize) -> usize {
    let mut cont = 0usize;
    while cont < 3 && cont < filled && (buf[filled - 1 - cont] & 0xC0) == 0x80 {
        cont += 1;
    }
    if cont == filled {
        return 0; // whole window is continuation bytes
    }
    let lead_index = filled - 1 - cont;
    let lead = buf[lead_index];
    let need = if lead >= 0xF0 {
        4
    } else if lead >= 0xE0 {
        3
    } else if lead >= 0xC2 {
        2
    } else {
        1
    };
    if lead_index + need > filled {
        lead_index
    } else {
        filled
    }
}

/// Flatten one decoded slice: strip a trailing `\r`, expand tabs to spaces,
/// trim leading and trailing blanks.
fn flatten_window(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw)
        .trim_end_matches('\r')
        .trim()
        .chars()
        .map(|c| if c == '\t' { ' ' } else { c })
        .collect()
}

/// Read one bounded window of `path` around `byte_offset` and return the
/// enclosing line: flattened (leading/trailing blanks trimmed, `\r` stripped,
/// tabs expanded to spaces) and clamped to `width` characters, with a trailing
/// `...` when the clamp cut characters off.
///
/// Cost is fixed, not proportional to the file: one open, one `Seek`, at most
/// [`SNIPPET_WINDOW`] stack bytes read, zero heap beyond the returned `String`.
/// The byte offset itself is never loaded. A byte offset at or past EOF yields
/// an empty string. Both window edges are trimmed of split multi-byte
/// sequences, so `String::from_utf8_lossy` never replaces a character the
/// window cut in half.
pub fn extract_snippet(path: &Path, byte_offset: u64, width: usize) -> io::Result<String> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "file is not in the crawled set",
        ));
    }
    let mut file = File::open(path)?;
    let window_start = byte_offset.saturating_sub(SNIPPET_LEAD);
    let mut buf = [0u8; SNIPPET_WINDOW];
    let (pos, filled) = read_window(&mut file, window_start, &mut buf)?;
    if filled == 0 {
        return Ok(String::new()); // byte offset at or past EOF
    }
    let start = trim_lead_partial(&buf, pos, filled);
    let end = if filled == SNIPPET_WINDOW {
        trim_tail_partial(&buf, filled)
    } else {
        filled
    };
    // The hit sits inside the trimmed window, so the search ranges stay ordered.
    let hit = byte_offset
        .saturating_sub(pos)
        .min(end as u64)
        .max(start as u64) as usize;
    // Enclosing line boundaries, searched only inside the window.
    let line_start = buf[start..hit]
        .iter()
        .rposition(|&b| b == b'\n')
        .map(|p| start + p + 1)
        .unwrap_or(start);
    let line_end = buf[hit..end]
        .iter()
        .position(|&b| b == b'\n')
        .map(|p| hit + p)
        .unwrap_or(end);
    let flat = flatten_window(&buf[line_start..line_end]);
    if width == 0 || flat.is_empty() {
        return Ok(String::new());
    }
    let mut out: String = flat.chars().take(width).collect();
    if flat.chars().count() > width {
        out.push_str("...");
    }
    Ok(out)
}

/// Read one bounded window of `path` around `byte_offset` and return the line
/// containing it plus up to `radius_lines` neighbours on each side, as
/// `(relative_offset, line_text)` in ascending order: `0` is the hit line,
/// `-1` the line before, `1` the line after, out to `-radius..=+radius`.
///
/// Cost is fixed: one open, one `Seek`, at most [`CONTEXT_WINDOW`] stack bytes
/// read. A window that opens mid-line reports the part of that line it can
/// see, and lines before the window are simply absent, so a hit near the start
/// of a file yields fewer negative offsets. Windows are edge-trimmed of split
/// multi-byte sequences, so no `U+FFFD` is introduced, tabs are expanded, and
/// `\r` is stripped. A byte offset at or past EOF yields an empty list.
pub fn extract_context(
    path: &Path,
    byte_offset: u64,
    radius_lines: usize,
) -> io::Result<Vec<(i32, String)>> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "file is not in the crawled set",
        ));
    }
    let mut file = File::open(path)?;
    let window_start = byte_offset.saturating_sub(CONTEXT_LEAD);
    let mut buf = [0u8; CONTEXT_WINDOW];
    let (pos, filled) = read_window(&mut file, window_start, &mut buf)?;
    if filled == 0 {
        return Ok(Vec::new()); // byte offset at or past EOF
    }
    let start = trim_lead_partial(&buf, pos, filled);
    let end = if filled == CONTEXT_WINDOW {
        trim_tail_partial(&buf, filled)
    } else {
        filled
    };
    if end <= start {
        return Ok(Vec::new());
    }
    let hit = byte_offset
        .saturating_sub(pos)
        .min(end as u64)
        .max(start as u64) as usize;

    // Line boundaries inside the window, each entry `(start, end)` in window
    // coordinates, newline excluded.
    let mut lines: Vec<(usize, usize)> = Vec::new();
    let mut s = start;
    while s < end {
        let e = buf[s..end]
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| s + p)
            .unwrap_or(end);
        lines.push((s, e));
        if e == end {
            break; // the window ends inside this line
        }
        s = e + 1;
    }

    // Lines are ordered by end index, so the hit line is the first whose end
    // reaches the hit.
    let idx = lines
        .partition_point(|&(_, e)| e < hit)
        .min(lines.len() - 1);
    let first = idx.saturating_sub(radius_lines);
    let last = (idx + radius_lines).min(lines.len() - 1);
    let mut out = Vec::with_capacity(last - first + 1);
    for j in first..=last {
        out.push((
            j as i32 - idx as i32,
            flatten_window(&buf[lines[j].0..lines[j].1]),
        ));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Build report (shared by the CLI binary and the C-ABI build)
// ---------------------------------------------------------------------------

/// Everything one build produces: the merged index, the `file_id -> path` table,
/// stream counters, lane faults, and the three timed phases.
pub struct Built {
    pub index: GlobalIndex,
    pub paths: Vec<PathBuf>,
    pub walk: WalkStats,
    pub producers: ProducerStats,
    pub consumer: ConsumerStats,
    pub lane_errors: Vec<String>,
    pub crawl_ms: f64,
    pub ingest_ms: f64,
    pub merge_ms: f64,
}

impl Built {
    pub fn total_ms(&self) -> f64 {
        self.crawl_ms + self.ingest_ms + self.merge_ms
    }

    pub fn payload_mib(&self) -> f64 {
        self.producers.payload_bytes as f64 / (1024.0 * 1024.0)
    }

    pub fn wire_mib(&self) -> f64 {
        self.producers.wire_bytes as f64 / (1024.0 * 1024.0)
    }

    pub fn memory_mib(&self) -> f64 {
        self.index.memory_bytes() as f64 / (1024.0 * 1024.0)
    }

    pub fn throughput(&self) -> f64 {
        let secs = self.total_ms() / 1e3;
        if secs > 0.0 {
            self.payload_mib() / secs
        } else {
            0.0
        }
    }

    /// Encode a whole build report into the on-disk cache byte stream. The
    /// header carries the root string and the root directory's modification
    /// time so [`Built::from_cache_bytes`] can reject a stale entry.
    pub fn to_cache_bytes(&self, root: &Path) -> Vec<u8> {
        let mut out = Vec::new();
        put_u64(&mut out, CACHE_MAGIC);
        put_u32(&mut out, CACHE_VERSION);
        put_str(&mut out, &root.display().to_string());
        put_u64(&mut out, root_mtime_secs(root));
        index::put_paths(&mut out, &self.paths);
        self.index.serialize_into(&mut out);
        for v in self.walk.to_u64s() {
            put_u64(&mut out, v);
        }
        for v in self.producers.to_u64s() {
            put_u64(&mut out, v);
        }
        for v in self.consumer.to_u64s() {
            put_u64(&mut out, v);
        }
        put_u32(&mut out, self.lane_errors.len() as u32);
        for e in &self.lane_errors {
            put_str(&mut out, e);
        }
        put_f64(&mut out, self.crawl_ms);
        put_f64(&mut out, self.ingest_ms);
        put_f64(&mut out, self.merge_ms);
        out
    }

    /// Decode a build report, validating the header against `root`: the stored
    /// root string and the stored root modification time must both match the
    /// current directory, otherwise the entry is stale and returns `None`.
    pub fn from_cache_bytes(bytes: &[u8], root: &Path) -> Option<Built> {
        let mut r = Reader::new(bytes);
        if r.u64()? != CACHE_MAGIC {
            return None;
        }
        if r.u32()? != CACHE_VERSION {
            return None;
        }
        if r.string()? != root.display().to_string() {
            return None;
        }
        if r.u64()? != root_mtime_secs(root) {
            return None;
        }
        let npaths = r.u32()? as usize;
        let mut paths = Vec::new();
        for _ in 0..npaths {
            paths.push(r.path()?);
        }
        let index = GlobalIndex::deserialize(&mut r)?;
        let mut walk_fields = Vec::with_capacity(WALK_FIELDS);
        for _ in 0..WALK_FIELDS {
            walk_fields.push(r.u64()?);
        }
        let mut producer_fields = Vec::with_capacity(PRODUCER_FIELDS);
        for _ in 0..PRODUCER_FIELDS {
            producer_fields.push(r.u64()?);
        }
        let mut consumer_fields = Vec::with_capacity(CONSUMER_FIELDS);
        for _ in 0..CONSUMER_FIELDS {
            consumer_fields.push(r.u64()?);
        }
        let nerr = r.u32()? as usize;
        // A corrupt header can claim a huge count; reject absurd fault lists.
        if nerr > 1_000_000 {
            return None;
        }
        let mut lane_errors = Vec::with_capacity(nerr);
        for _ in 0..nerr {
            lane_errors.push(r.string()?);
        }
        Some(Built {
            index,
            paths,
            walk: WalkStats::from_u64s(&walk_fields),
            producers: ProducerStats::from_u64s(&producer_fields),
            consumer: ConsumerStats::from_u64s(&consumer_fields),
            lane_errors,
            crawl_ms: r.f64()?,
            ingest_ms: r.f64()?,
            merge_ms: r.f64()?,
        })
    }
}

// ---------------------------------------------------------------------------
// On-disk index cache
// ---------------------------------------------------------------------------
//
// Built indexes live in `.seeksage_cache` inside the crate's own package
// directory, one entry file per indexed root, named by the root string's hash.
// An entry is valid only when its stored root string and stored root
// modification time both match the directory being opened, so a change to the
// directory's contents drops the entry and the next build is a fresh scan.

/// Folder holding the cache, inside this package's directory.
pub const CACHE_DIR_NAME: &str = ".seeksage_cache";
/// Header magic: the ASCII "SEEKSAGE" as one `u64`.
pub const CACHE_MAGIC: u64 = 0x5345_454b_5341_4745;
/// Cache format version.
pub const CACHE_VERSION: u32 = 2;

const WALK_FIELDS: usize = 14;
const PRODUCER_FIELDS: usize = 12;
const CONSUMER_FIELDS: usize = 6;

/// Where the cache lives, computed from the package directory at compile time.
pub fn cache_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(CACHE_DIR_NAME)
}

/// FNV-1a over the root's path string, the key for one cache entry.
pub fn cache_key(root: &Path) -> u64 {
    let mut h: u64 = index::HASH_SEED;
    for &b in root.display().to_string().as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(index::HASH_PRIME);
    }
    h
}

/// One entry file per root, named by `cache_key`.
pub fn cache_entry_path(root: &Path) -> PathBuf {
    cache_dir().join(format!("{:016x}", cache_key(root)))
}

/// A directory's modification time in whole seconds since the Unix epoch, the
/// stamp a cache entry is validated against; a missing directory reads as 0.
pub fn root_mtime_secs(root: &Path) -> u64 {
    fs::metadata(root)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Write `built` to the entry file for `root`. Best-effort: the entry file
/// lives in a package-owned folder, so a failure to write leaves the build
/// usable in memory and only skips the cache.
pub fn save_cache(built: &Built, root: &Path) -> io::Result<()> {
    fs::create_dir_all(cache_dir())?;
    fs::write(cache_entry_path(root), built.to_cache_bytes(root))
}

/// Load the cached build for `root` if the entry is present and valid; `None`
/// on any read failure, decode failure, or stale header, so a cache miss is
/// never a hard error for the caller.
pub fn load_cache(root: &Path) -> Option<Built> {
    let bytes = fs::read(cache_entry_path(root)).ok()?;
    Built::from_cache_bytes(&bytes, root)
}

/// The cache-aware build: return the disk cache when it is valid, otherwise
/// scan, merge, and save the result for next time. The second `bool` reports
/// whether the index came from the cache.
pub fn build_index_cached(root: &Path, lane_count: usize) -> io::Result<(Built, bool)> {
    if let Some(built) = load_cache(root) {
        return Ok((built, true));
    }
    let built = build_index(root, lane_count)?;
    let _ = save_cache(&built, root);
    Ok((built, false))
}

/// Crawl, publish chunks over `lane_count` lanes, ingest them into per-lane
/// shards, then merge the shards in parallel.
pub fn build_index(root: &Path, lane_count: usize) -> io::Result<Built> {
    let mut records: Vec<FileRecord> = Vec::new();
    let mut walk = WalkStats::default();
    let crawl_t = Instant::now();
    crawl(root, &mut records, &mut walk)?;
    let crawl_ms = crawl_t.elapsed().as_secs_f64() * 1e3;

    // `file_id` is the crawl-order index, so the path table is a direct lookup.
    let mut paths: Vec<PathBuf> = Vec::new();
    for r in &records {
        let id = r.file_id as usize;
        if id >= paths.len() {
            paths.resize(id + 1, PathBuf::new());
        }
        paths[id] = r.path.clone();
    }

    let parts = partition(&records, lane_count);
    let lanes = Lanes::new(lane_count, LANE_CAP);
    let mut token = lanes.consumer().expect("single consumer token");
    let active = AtomicUsize::new(lane_count);

    let (producers, consumer, shards, ingest_secs, lane_errors) = thread::scope(|s| {
        let handles: Vec<_> = (0..lane_count)
            .map(|i| {
                // Copy the references into locals so the inner `move` closure
                // takes its own handles instead of moving out of the outer capture.
                let lanes_ref = &lanes;
                let active_ref = &active;
                let part = &parts[i];
                s.spawn(move || -> (ProducerStats, String) {
                    let outcome = run_producer(lanes_ref, i, part);
                    active_ref.fetch_sub(1, Ordering::Release);
                    match outcome {
                        Ok(ps) => (ps, String::new()),
                        Err(e) => (ProducerStats::default(), format!("lane {i} stopped: {e}")),
                    }
                })
            })
            .collect();

        let consumer_ref = &mut token;
        let active_ref = &active;
        let shard_count = lane_count;
        let ch = s.spawn(move || {
            let mut cons = ConsumerStats::default();
            // Every chunk of one file lands in the same shard, so the per-file
            // line/offset state stays consistent inside that shard.
            let mut shards: Vec<IndexShard> = (0..shard_count).map(|_| IndexShard::new()).collect();
            let t = Instant::now();
            drain_until_done(consumer_ref, active_ref, |bytes| {
                cons.record(bytes);
                let fid = if bytes.len() >= 4 {
                    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
                } else {
                    0
                };
                shards[fid % shard_count].ingest_wire(bytes);
            });
            (cons, shards, t.elapsed().as_secs_f64())
        });

        let mut agg = ProducerStats::default();
        let mut errors: Vec<String> = Vec::new();
        for (i, h) in handles.into_iter().enumerate() {
            match h.join() {
                Ok((ps, msg)) => {
                    agg.merge(ps);
                    if !msg.is_empty() {
                        errors.push(msg);
                    }
                }
                Err(_) => errors.push(format!("producer lane {i} panicked")),
            }
        }
        let (cons, sh, secs) = ch.join().expect("consumer thread panicked");
        (agg, cons, sh, secs, errors)
    });

    let merge_t = Instant::now();
    let index = merge_shards(shards, lane_count);
    let merge_ms = merge_t.elapsed().as_secs_f64() * 1e3;

    Ok(Built {
        index,
        paths,
        walk,
        producers,
        consumer,
        lane_errors,
        crawl_ms,
        ingest_ms: ingest_secs * 1e3,
        merge_ms,
    })
}

// ---------------------------------------------------------------------------
// Opaque engine handle
// ---------------------------------------------------------------------------

/// The immutable, merged index wrapped as an opaque handle for the C-ABI.
/// After building it is read-only, so concurrent [`SeekSageEngine::search`]
/// calls are safe; `GlobalIndex` is `Send + Sync`.
pub struct SeekSageEngine {
    index: GlobalIndex,
}

impl SeekSageEngine {
    /// Wrap a merged index as an engine handle.
    #[must_use]
    pub fn from_index(index: GlobalIndex) -> Self {
        Self { index }
    }

    /// Exact keyword matching; multiple terms are AND-intersected.
    #[must_use]
    pub fn search(&self, query: &str) -> Vec<SearchResult> {
        self.index.search(query)
    }

    /// Resident index footprint in bytes.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.index.memory_bytes()
    }

    /// Distinct terms in the merged index.
    #[must_use]
    pub fn terms(&self) -> u64 {
        self.index.terms
    }

    /// Total posting occurrences.
    #[must_use]
    pub fn occurrences(&self) -> u64 {
        self.index.occurrences
    }
}

// The engine must be shareable across threads for concurrent FFI searches.
// A compile-time assertion: if `SeekSageEngine` ever stops being `Send + Sync`
// this call site fails to satisfy the trait bound.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SeekSageEngine>();
};

// ---------------------------------------------------------------------------
// C-ABI surface
// ---------------------------------------------------------------------------

/// Format hits as a JSON string. Only integers are emitted, so no escaping is
/// needed and the result is always valid JSON.
fn format_hits_json(hits: &[SearchResult]) -> String {
    let mut s = String::new();
    s.push_str("{\"hits\":");
    s.push_str(&hits.len().to_string());
    s.push_str(",\"results\":[");
    for (i, h) in hits.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "{{\"file_id\":{},\"line\":{},\"offset\":{}}}",
            h.file_id, h.line, h.byte_offset
        ));
    }
    s.push_str("]}");
    s
}

/// Crawl and build the merged index from `dir_path`, returning an opaque
/// engine handle. Returns null on a null argument, a build I/O error, or any
/// panic inside the engine. The caller must release the handle with
/// [`seeksage_free`].
///
/// # Safety
/// `dir_path` must be a valid, NUL-terminated C string (or null).
#[no_mangle]
pub unsafe extern "C" fn seeksage_build_index(dir_path: *const c_char) -> *mut SeekSageEngine {
    if dir_path.is_null() {
        return std::ptr::null_mut();
    }
    let bytes = unsafe { CStr::from_ptr(dir_path) }.to_bytes();
    let path = PathBuf::from(String::from_utf8_lossy(bytes).into_owned());
    let lanes = default_lanes();
    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| build_index(&path, lanes)));
    match outcome {
        Ok(Ok(built)) => Box::into_raw(Box::new(SeekSageEngine::from_index(built.index))),
        Ok(Err(_)) => std::ptr::null_mut(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Run one AND query on `engine` and return a heap-allocated, NUL-terminated
/// JSON string. The caller must release it with [`seeksage_string_free`].
/// Returns null on a null argument, an engine panic, or an unencodable result.
///
/// # Safety
/// `engine` must be a live handle from [`seeksage_build_index`]; `query` must
/// be a valid, NUL-terminated C string (or null). Neither is used after the
/// matching [`seeksage_free`] / string release.
#[no_mangle]
pub unsafe extern "C" fn seeksage_search(
    engine: *mut SeekSageEngine,
    query: *const c_char,
) -> *mut c_char {
    if engine.is_null() || query.is_null() {
        return std::ptr::null_mut();
    }
    let engine_ref = unsafe { &*engine };
    let query_bytes = unsafe { CStr::from_ptr(query) }.to_bytes();
    let query_str = String::from_utf8_lossy(query_bytes).into_owned();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let hits = engine_ref.search(&query_str);
        format_hits_json(&hits)
    }));
    match outcome {
        Ok(json) => match CString::new(json) {
            Ok(s) => s.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

/// Release a string returned by [`seeksage_search`]. A null pointer is a no-op.
///
/// # Safety
/// `s` must be a pointer returned by [`seeksage_search`], used exactly once.
#[no_mangle]
pub unsafe extern "C" fn seeksage_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe { drop(CString::from_raw(s)) };
}

/// Destroy an engine handle built by [`seeksage_build_index`]. A null pointer
/// is a no-op. The handle must not be used after this call.
///
/// # Safety
/// `engine` must be a handle returned by [`seeksage_build_index`], used exactly
/// once for the release.
#[no_mangle]
pub unsafe extern "C" fn seeksage_free(engine: *mut SeekSageEngine) {
    if engine.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(engine)) };
}

// ---------------------------------------------------------------------------
// Test-only counting allocator.
//
// Rust requires the global allocator static to live at the crate root, so the
// counters are declared here; `parser`'s tests use them to prove the producer
// hot loop performs zero heap allocations. Only allocations issued by the
// thread that started the window are counted, so parallel test threads cannot
// pollute the measurement.
// ---------------------------------------------------------------------------

#[cfg(test)]
thread_local! {
    static ALLOC_OWNER: std::cell::Cell<Option<std::thread::ThreadId>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub struct CountingAlloc {
    pub count: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        if ALLOC_OWNER.with(|o| o.get()) == Some(std::thread::current().id()) {
            self.count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        std::alloc::System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        std::alloc::System.dealloc(ptr, layout)
    }
}

#[cfg(test)]
#[global_allocator]
static COUNTING_ALLOC: CountingAlloc = CountingAlloc {
    count: std::sync::atomic::AtomicUsize::new(0),
};

/// Open an allocation-counting window for the calling thread.
#[cfg(test)]
pub fn alloc_count_start() {
    ALLOC_OWNER.with(|o| o.set(Some(std::thread::current().id())));
    COUNTING_ALLOC
        .count
        .store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Allocations issued by the calling thread since the window opened.
#[cfg(test)]
pub fn alloc_count() -> usize {
    COUNTING_ALLOC
        .count
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// Close the counting window.
#[cfg(test)]
pub fn alloc_count_stop() {
    ALLOC_OWNER.with(|o| o.set(None));
}

// ---------------------------------------------------------------------------
// Tests: bounded snippet extraction
// ---------------------------------------------------------------------------

#[cfg(test)]
mod snippet_tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(tag);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    #[test]
    fn snippet_returns_the_enclosing_line() {
        let root = tmp_root("snippet-line");
        let content = "первая строка\nГлава Первая: Ёжик\nтретья строка\n";
        let file = root.join("s.txt");
        std::fs::write(&file, content).unwrap();

        // Postings store the byte offset of the term start; that is the input.
        let off = content.find("Глава").unwrap() as u64;
        assert_eq!(
            extract_snippet(&file, off, 88).unwrap(),
            "Глава Первая: Ёжик"
        );
        assert_eq!(extract_snippet(&file, 0, 88).unwrap(), "первая строка");
        let off3 = content.find("третья").unwrap() as u64;
        assert_eq!(extract_snippet(&file, off3, 88).unwrap(), "третья строка");
        // A hit in the middle of a line still reports the whole enclosing line.
        let off_mid = content.find("Первая").unwrap() as u64;
        assert_eq!(
            extract_snippet(&file, off_mid, 88).unwrap(),
            "Глава Первая: Ёжик"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn snippet_clamps_to_width_with_an_ellipsis() {
        let root = tmp_root("snippet-clamp");
        let content = "Глава Первая: Ёжик\n";
        let file = root.join("s.txt");
        std::fs::write(&file, content).unwrap();
        assert_eq!(extract_snippet(&file, 0, 5).unwrap(), "Глава...");
        assert_eq!(extract_snippet(&file, 0, 18).unwrap(), "Глава Первая: Ёжик");
        assert_eq!(extract_snippet(&file, 0, 0).unwrap(), "");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn snippet_strips_carriage_returns_and_expands_tabs() {
        let root = tmp_root("snippet-flatten");
        let file = root.join("flat.txt");
        std::fs::write(&file, b"a\tb\tc\r\nsecond\r\n").unwrap();
        assert_eq!(extract_snippet(&file, 0, 32).unwrap(), "a b c");
        assert_eq!(extract_snippet(&file, 8, 32).unwrap(), "second");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn snippet_past_eof_is_empty_and_missing_path_is_an_error() {
        let root = tmp_root("snippet-edges");
        let file = root.join("short.txt");
        std::fs::write(&file, "tiny\n").unwrap();
        assert_eq!(extract_snippet(&file, 100_000, 32).unwrap(), "");
        assert_eq!(extract_snippet(&file, 4, 32).unwrap(), "tiny");
        assert!(extract_snippet(Path::new(""), 0, 32).is_err());
        assert!(extract_snippet(&root.join("nope.txt"), 0, 32).is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A line far longer than the read window: the window opens inside the line
    /// and, on an odd byte offset, mid-character. Both window edges are trimmed
    /// of split multi-byte sequences, so the snippet is clean characters with
    /// no `U+FFFD`, clamped on `chars()` boundaries.
    #[test]
    fn snippet_is_safe_on_long_lines_and_multibyte_window_edges() {
        let root = tmp_root("snippet-long");
        let long_line = "я".repeat(900); // 1800 bytes, larger than the window
        let file = root.join("long.txt");
        std::fs::write(&file, format!("{long_line}\nlast\n")).unwrap();

        // Even byte offset: the window opens on a character boundary.
        let s = extract_snippet(&file, 500, 88).unwrap();
        assert_eq!(s, "я".repeat(88) + "...");
        // Odd byte offset: the window opens inside a 2-byte pair, so the split
        // lead character is trimmed away.
        let s = extract_snippet(&file, 501, 88).unwrap();
        assert_eq!(s, "я".repeat(88) + "...");
        assert!(!s.contains('\u{FFFD}'), "snippet: {s}");
        assert!(s.chars().all(|c| c == 'я' || c == '.'), "snippet: {s}");
        // A hit on the last line, near the end of the file.
        assert_eq!(extract_snippet(&file, 1801, 88).unwrap(), "last");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The GUI context helper: signed relative offsets around the hit, radius
    /// clamping at both ends of the file, tabs expanded, `\r` stripped, and
    /// edge-trimmed windows on a line far longer than [`CONTEXT_WINDOW`].
    #[test]
    fn context_window_returns_lines_around_a_hit() {
        let root = tmp_root("context-window");
        let file = root.join("ctx.txt");
        let content = "первая строка\nвторой ряд\tтаб\nТРЕТИЯ строка\nчетвёртый\nend\r\n";
        std::fs::write(&file, content).unwrap();

        // Full radius on both sides, in ascending order, `0` is the hit line.
        let off = content.find("ТРЕТИЯ").unwrap() as u64;
        let ctx = extract_context(&file, off, 2).unwrap();
        assert_eq!(
            ctx,
            [
                (-2, "первая строка".to_string()),
                (-1, "второй ряд таб".to_string()),
                (0, "ТРЕТИЯ строка".to_string()),
                (1, "четвёртый".to_string()),
                (2, "end".to_string()),
            ]
        );

        // Radius 0 reports only the hit line.
        assert_eq!(
            extract_context(&file, off, 0).unwrap(),
            [(0, "ТРЕТИЯ строка".to_string())]
        );

        // Clamping: a hit on the first line has no lines before the window.
        let head = extract_context(&file, 0, 2).unwrap();
        assert_eq!(head[0], (0, "первая строка".to_string()));
        assert_eq!(head.len(), 3); // offsets 0, 1, 2

        // A byte offset at or past EOF yields an empty list; missing paths error.
        assert!(extract_context(&file, content.len() as u64 + 100_000, 2)
            .unwrap()
            .is_empty());
        assert!(extract_context(Path::new(""), 0, 2).is_err());

        // A line far longer than CONTEXT_WINDOW: an odd byte offset opens the
        // window mid-character, so both edges are trimmed and the window holds
        // only clean 'я' characters.
        let long_file = root.join("ctx-long.txt");
        let long_line = "я".repeat(5000); // 10_000 bytes, larger than the window
        std::fs::write(&long_file, format!("{long_line}\nlast\n")).unwrap();
        let ctx = extract_context(&long_file, 5001, 1).unwrap();
        assert_eq!(ctx.len(), 1);
        assert_eq!(ctx[0].0, 0, "ctx: {ctx:?}");
        let text = &ctx[0].1;
        assert!(!text.is_empty(), "ctx: {ctx:?}");
        assert!(text.chars().all(|c| c == 'я'), "ctx: {text}");
        assert!(!text.contains('\u{FFFD}'), "ctx: {text}");

        let _ = std::fs::remove_dir_all(&root);
    }
}

// ---------------------------------------------------------------------------
// Tests: the on-disk index cache
// ---------------------------------------------------------------------------

#[cfg(test)]
mod cache_tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(tag);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    fn build_small(root: &Path) -> Built {
        std::fs::write(
            root.join("a.rs"),
            b"pub fn index_file() {}\nindex_file();\n",
        )
        .unwrap();
        std::fs::write(
            root.join("b.md"),
            b"# index_file heading\nindex_file again\n",
        )
        .unwrap();
        build_index(root, 3).expect("build small tree")
    }

    #[test]
    fn cache_round_trip_restores_the_index() {
        let root = tmp_root("cache-roundtrip");
        let built = build_small(&root);
        let bytes = built.to_cache_bytes(&root);
        let restored = Built::from_cache_bytes(&bytes, &root).expect("valid entry decodes");
        assert!(restored.index.same_index(&built.index));
        assert_eq!(restored.paths, built.paths);
        assert_eq!(restored.walk, built.walk);
        assert_eq!(restored.producers, built.producers);
        assert_eq!(restored.consumer, built.consumer);
        assert_eq!(restored.lane_errors, built.lane_errors);
        assert_eq!(restored.crawl_ms, built.crawl_ms);
        assert_eq!(restored.ingest_ms, built.ingest_ms);
        assert_eq!(restored.merge_ms, built.merge_ms);
        // The restored index answers every sampled query the same way.
        for q in ["index_file", "pub fn", "heading", "again", "missingterm"] {
            assert_eq!(restored.index.search(q), built.index.search(q), "query {q}");
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn cache_round_trip_preserves_prefix_queries() {
        let root = tmp_root("cache-prefix");
        std::fs::write(
            root.join("p.txt"),
            "the player plays piano\nабсурдные коровы\n",
        )
        .unwrap();
        let built = build_index(&root, 3).expect("build prefix tree");
        // Prefixes reach longer words in the freshly built index.
        assert_eq!(built.index.search("play").len(), 1);
        assert_eq!(built.index.search("player").len(), 1);
        assert_eq!(built.index.search("play piano").len(), 1);
        assert_eq!(built.index.search("абсурд").len(), 1);
        assert_eq!(built.index.search("АБСУРД").len(), 1);

        let bytes = built.to_cache_bytes(&root);
        let restored = Built::from_cache_bytes(&bytes, &root).expect("prefix cache decodes");
        // Canonical term bytes round-trip: structural equality proves it.
        assert!(restored.index.same_index(&built.index));
        // The same prefix queries survive the cache round-trip identically.
        for q in [
            "play",
            "player",
            "play piano",
            "абсурд",
            "АБСУРД",
            "коро",
            "quart",
        ] {
            assert_eq!(
                restored.index.search(q),
                built.index.search(q),
                "prefix {q}"
            );
        }
        assert_eq!(restored.index.search("play").len(), 1);
        assert_eq!(restored.index.search("абсурд").len(), 1);
        assert_eq!(restored.index.search("коро").len(), 1);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn cache_validates_the_root_string_and_the_mtime() {
        let root = tmp_root("cache-validate");
        let built = build_small(&root);
        let bytes = built.to_cache_bytes(&root);

        // A header bound to a different root string is rejected.
        let other = root.join("subdir");
        std::fs::create_dir_all(&other).unwrap();
        assert!(Built::from_cache_bytes(&bytes, &other).is_none());

        // A header whose stored root modification time differs is stale: flip the
        // stored mtime to 0, which cannot equal a freshly created directory.
        let len = root.display().to_string().len();
        let mtime_off = 16 + len;
        let mut tampered = bytes.clone();
        tampered[mtime_off..mtime_off + 8].fill(0u8);
        assert_ne!(root_mtime_secs(&root), 0);
        assert!(Built::from_cache_bytes(&tampered, &root).is_none());

        // The clean header round-trips, and save/load agree with it on disk.
        assert!(Built::from_cache_bytes(&bytes, &root).is_some());
        save_cache(&built, &root).expect("save cache");
        let loaded = load_cache(&root).expect("load cache");
        assert!(loaded.index.same_index(&built.index));

        // A truncated file decodes to None, not a partial index.
        assert!(Built::from_cache_bytes(&bytes[..bytes.len() / 2], &root).is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn cache_entry_is_a_separate_file_per_root() {
        let r1 = tmp_root("cache-key-1");
        let r2 = tmp_root("cache-key-2");
        let p1 = cache_entry_path(&r1);
        let p2 = cache_entry_path(&r2);
        assert_ne!(p1, p2);
        assert_eq!(p1.parent().unwrap(), cache_dir().as_path());
        // The cache folder lives inside the package directory, so indexing that
        // directory never re-ingests the cache: a leading dot prunes it in the crawl.
        assert!(cache_dir().starts_with(env!("CARGO_MANIFEST_DIR")));
    }

    #[test]
    fn build_index_cached_scans_then_hits_the_cache() {
        let root = tmp_root("cache-cached-build");
        std::fs::write(root.join("x.rs"), b"index_file\n").unwrap();
        // First call: no cache entry, so it scans and saves.
        let (first, from_cache) = build_index_cached(&root, 3).expect("first build");
        assert!(!from_cache);
        assert_eq!(first.walk.files_matched, 1);
        // Second call: same root and mtime, so the entry is loaded, not rebuilt.
        let (second, from_cache2) = build_index_cached(&root, 3).expect("cached build");
        assert!(from_cache2);
        assert!(second.index.same_index(&first.index));
        // The entry file sits under the package directory, not the tree.
        assert_eq!(
            cache_entry_path(&root).parent().unwrap(),
            cache_dir().as_path()
        );
        let _ = std::fs::remove_dir_all(&root);
        // With the tree gone, no cache is found for its hash.
        assert!(load_cache(&root).is_none());
    }
}

// ---------------------------------------------------------------------------
// Tests: the C-ABI boundary
// ---------------------------------------------------------------------------

#[cfg(test)]
mod ffi_tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(tag);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    /// Build, search, and free through the exact C-ABI signatures.
    #[test]
    fn build_search_and_free_roundtrip() {
        let root = tmp_root("ffi-roundtrip");
        std::fs::write(root.join("a.txt"), "alpha beta\nbeta gamma\nalpha only\n").unwrap();
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(
            root.join("nested").join("b.rs"),
            "seeksage parser\nseeksage queue lane\n",
        )
        .unwrap();

        let path = CString::new(root.to_str().unwrap().to_string()).unwrap();
        let engine = unsafe { seeksage_build_index(path.as_ptr()) };
        assert!(!engine.is_null(), "build must succeed");

        let q = CString::new("seeksage queue").unwrap();
        let out = unsafe { seeksage_search(engine, q.as_ptr()) };
        assert!(!out.is_null(), "search must return a string");
        let json = unsafe { CStr::from_ptr(out) }
            .to_string_lossy()
            .into_owned();
        // Only line 2 of b.rs matches the AND of "seeksage" and "queue".
        assert!(json.contains("\"hits\":1"), "json: {json}");
        assert!(json.contains("\"results\":["), "json: {json}");
        unsafe { seeksage_string_free(out) };

        // The engine is still live: a second query works.
        let q2 = CString::new("seeksage").unwrap();
        let out2 = unsafe { seeksage_search(engine, q2.as_ptr()) };
        let json2 = unsafe { CStr::from_ptr(out2) }
            .to_string_lossy()
            .into_owned();
        assert!(json2.contains("\"hits\":2"), "json: {json2}");
        unsafe { seeksage_string_free(out2) };

        unsafe { seeksage_free(engine) };
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Null arguments are handled without dereferencing or panic.
    #[test]
    fn null_pointers_are_handled() {
        let engine = unsafe { seeksage_build_index(std::ptr::null()) };
        assert!(engine.is_null());
        let out = unsafe { seeksage_search(std::ptr::null_mut(), std::ptr::null()) };
        assert!(out.is_null());
        let out2 = unsafe { seeksage_search(std::ptr::null_mut(), std::ptr::null()) };
        assert!(out2.is_null());
        // No-op releases on null are safe.
        unsafe { seeksage_string_free(std::ptr::null_mut()) };
        unsafe { seeksage_free(std::ptr::null_mut()) };
    }

    /// A build over a directory with no matching text files yields a valid,
    /// empty engine that is still released cleanly.
    #[test]
    fn build_empty_tree_is_releasable() {
        let root = tmp_root("ffi-empty");
        std::fs::write(root.join("skip.bin"), b"not text").unwrap();
        let path = CString::new(root.to_str().unwrap().to_string()).unwrap();
        let engine = unsafe { seeksage_build_index(path.as_ptr()) };
        assert!(!engine.is_null(), "empty tree must build an engine");
        let q = CString::new("anything").unwrap();
        let out = unsafe { seeksage_search(engine, q.as_ptr()) };
        let json = unsafe { CStr::from_ptr(out) }
            .to_string_lossy()
            .into_owned();
        assert!(json.contains("\"hits\":0"), "json: {json}");
        assert!(json.contains("\"results\":[]"), "json: {json}");
        unsafe { seeksage_string_free(out) };
        unsafe { seeksage_free(engine) };
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A missing path is an I/O error, surfaced as a null handle, not a panic.
    #[test]
    fn missing_path_returns_null() {
        let root = tmp_root("ffi-missing").join("nope");
        let path = CString::new(root.to_str().unwrap().to_string()).unwrap();
        let engine = unsafe { seeksage_build_index(path.as_ptr()) };
        assert!(engine.is_null(), "missing path must return null");
        unsafe { seeksage_free(engine) }; // null release is a no-op
        let _ = std::fs::remove_dir_all(tmp_root("ffi-missing"));
    }
}
