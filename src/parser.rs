//! `SeekSage` ingestion front-end: a zero-allocation directory crawler and a
//! lock-free chunk-stream producer.
//!
//! The producer hot loop never touches the heap:
//! - one 64 KiB reusable read buffer per file, allocated once in
//!   [`ChunkStream::open`] and reused for every refill;
//! - fixed-size [`TextChunk`] value objects living on the stack;
//! - a reusable wire buffer owned by [`LaneWriter`] / [`SeqWriter`].
//!
//! Wire format of one published chunk (little-endian):
//!
//! ```text
//! [0..4]    file_id   u32
//! [4..8]    chunk_seq u32
//! [8..10]   data_len  u16
//! [10]      flags     u8
//! [11]      reserved  u8
//! [12..]    payload   `data_len` bytes
//! ```
//!
//! [`TextChunk`] is sized to exactly one lane slot ([`LANE_SLOT_PAYLOAD`]), so a
//! chunk is a structured payload slot: metadata + fixed inline bytes, no
//! per-read heap allocation. The queue keeps publishing raw `&[u8]`, so the
//! chunk is encoded into a wire buffer on the stack and decoded zero-copy into
//! [`ChunkRef`] on the consumer side.
//!
//! Cheap filters decide which files are worth reading at all: the crawler keeps
//! the [`TEXT_EXTENSIONS`] set, drops empty files, drops anything larger than
//! [`MAX_FILE_SIZE`], and probes the first [`BINARY_SNIFF`] bytes for a NUL
//! marker so a binary blob never enters a producer lane. Every per-file fault
//! is local: a locked, vanished, mid-read failed, or runaway oversized file
//! increments its own counter and the lane keeps streaming the next record, so
//! one bad directory entry can never end a crawl, a producer lane, or a build.

use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use crate::queue::{
    LaneProducer, Lanes, LanesConsumer, SeqProducer, SeqRing, LANE_SLOT_PAYLOAD, SEQ_SLOT_PAYLOAD,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Fixed wire header: `file_id` + `chunk_seq` + `data_len` + `flags` + reserved.
pub const CHUNK_HEADER: usize = 4 + 4 + 2 + 1 + 1;

/// Inline payload of a lane chunk: the widest payload that fits one lane slot.
pub const CHUNK_DATA_MAX: usize = LANE_SLOT_PAYLOAD - CHUNK_HEADER;

/// Inline payload of a ring chunk: the widest payload that fits one ring slot.
pub const RING_CHUNK_DATA_MAX: usize = SEQ_SLOT_PAYLOAD - CHUNK_HEADER;

/// Reusable read buffer: allocated once per file, never reallocated per line.
pub const READ_BUF: usize = 64 * 1024;

/// Leading bytes inspected for a NUL marker before a file is treated as binary:
/// one `File::open` plus one read of at most this many bytes per candidate.
pub const BINARY_SNIFF: usize = 1024;

/// Upper bound on ingested file size. A file larger than this is never opened
/// by the crawl, and a file that grows past it mid-stream is closed cleanly,
/// so one oversized artifact cannot stretch a lane's ingest time or the index.
pub const MAX_FILE_SIZE: u64 = 64 * 1024 * 1024;

/// Hard depth limit so a pathological tree cannot blow the crawler stack.
pub const MAX_DEPTH: u32 = 32;

/// Pure spin attempts before the producer escalates to `yield_now`.
pub const SPIN_YIELD_THRESHOLD: u32 = 128;

/// Escalation point where `yield_now` starts being paired with a doubling sleep.
pub const YIELD_ESCALATION_LIMIT: u32 = 8;

pub const FLAG_LINE_START: u8 = 1 << 0;
pub const FLAG_CONTINUED: u8 = 1 << 1;
pub const FLAG_LINE_END: u8 = 1 << 2;
pub const FLAG_EOF: u8 = 1 << 3;

/// Text extensions accepted by the crawler (case-insensitive). The list is
/// organised by format family so it covers every popular plain-text format in
/// one place: docs & notes, tables & structured data, configuration & system
/// files, logs, web & front-end sources, back-end & script sources, and the
/// native systems-programming sources (`rs`, `h`, `hpp`, `c`, `cc`, `cpp`,
/// `cxx`, `idl`) that this tool has always indexed.
pub const TEXT_EXTENSIONS: [&str; 51] = [
    // Docs & notes:
    "md",
    "markdown",
    "txt",
    "adoc",
    "rst",
    // Tables & structured data:
    "csv",
    "tsv",
    "json",
    "yaml",
    "yml",
    "toml",
    "xml",
    // Configs & system files:
    "env",
    "properties",
    "conf",
    "config",
    "cfg",
    "ini",
    // Logs:
    "log",
    "out",
    "err",
    // Web & front-end sources:
    "html",
    "htm",
    "css",
    "scss",
    "less",
    "js",
    "ts",
    "tsx",
    "jsx",
    "vue",
    "svelte",
    // Back-end & script sources:
    "py",
    "lua",
    "php",
    "rb",
    "go",
    "cs",
    "java",
    "sh",
    "bat",
    "ps1",
    "sql",
    // Native systems-programming sources, kept in the `Systems` family:
    "rs",
    "h",
    "hpp",
    "c",
    "cc",
    "cpp",
    "cxx",
    "idl",
];

/// Directory names pruned from the walk (dot-prefixed names are always pruned).
pub const SKIP_DIR_NAMES: [&str; 8] = [
    "target",
    "node_modules",
    "out",
    "dist",
    "build",
    "vendor",
    "venv",
    "__pycache__",
];

const _: () = assert!(core::mem::size_of::<TextChunk>() == LANE_SLOT_PAYLOAD);
const _: () = assert!(core::mem::size_of::<TextChunk>() <= SEQ_SLOT_PAYLOAD + 8);
const _: () = assert!(SEQ_SLOT_PAYLOAD >= CHUNK_HEADER);
const _: () = assert!(CHUNK_DATA_MAX >= 32);

// ---------------------------------------------------------------------------
// Structured payload slot
// ---------------------------------------------------------------------------

/// Fixed, cache-line sized metadata + inline bytes for one streamed segment.
///
/// `repr(C)` keeps the layout stable for the wire encode, and the size is
/// asserted at compile time to fill exactly one lane slot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct TextChunk {
    pub file_id: u32,
    pub chunk_seq: u32,
    pub data_len: u16,
    pub flags: u8,
    pub reserved: u8,
    pub data: [u8; CHUNK_DATA_MAX],
}

impl Default for TextChunk {
    fn default() -> Self {
        Self::new(0, 0, 0)
    }
}

impl TextChunk {
    #[inline]
    #[must_use]
    pub fn new(file_id: u32, chunk_seq: u32, flags: u8) -> Self {
        Self {
            file_id,
            chunk_seq,
            data_len: 0,
            flags,
            reserved: 0,
            data: [0u8; CHUNK_DATA_MAX],
        }
    }

    /// Copy `src` into the inline payload, clamped to the slot capacity.
    #[inline]
    pub fn fill(&mut self, src: &[u8]) {
        let n = src.len().min(CHUNK_DATA_MAX);
        self.data[..n].copy_from_slice(&src[..n]);
        self.data_len = n as u16;
    }

    #[inline]
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.data[..self.data_len as usize]
    }

    #[inline]
    #[must_use]
    pub fn has_flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    /// Serialize into a wire buffer; returns the number of bytes written.
    ///
    /// The producer owns a reusable buffer, so this never allocates.
    #[inline]
    pub fn encode(&self, out: &mut [u8]) -> usize {
        let n = self.data_len as usize;
        debug_assert!(out.len() >= CHUNK_HEADER + n);
        out[0..4].copy_from_slice(&self.file_id.to_le_bytes());
        out[4..8].copy_from_slice(&self.chunk_seq.to_le_bytes());
        out[8..10].copy_from_slice(&self.data_len.to_le_bytes());
        out[10] = self.flags;
        out[11] = 0;
        out[CHUNK_HEADER..CHUNK_HEADER + n].copy_from_slice(&self.data[..n]);
        CHUNK_HEADER + n
    }

    /// Decode a wire payload zero-copy into [`ChunkRef`].
    #[inline]
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<ChunkRef<'_>> {
        if bytes.len() < CHUNK_HEADER {
            return None;
        }
        let file_id = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let chunk_seq = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let data_len = u16::from_le_bytes([bytes[8], bytes[9]]);
        let flags = bytes[10];
        let n = data_len as usize;
        if CHUNK_HEADER + n > bytes.len() {
            return None;
        }
        Some(ChunkRef {
            file_id,
            chunk_seq,
            data_len,
            flags,
            data: &bytes[CHUNK_HEADER..CHUNK_HEADER + n],
        })
    }
}

/// Zero-copy decoded view of a published chunk.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ChunkRef<'a> {
    pub file_id: u32,
    pub chunk_seq: u32,
    pub data_len: u16,
    pub flags: u8,
    pub data: &'a [u8],
}

impl<'a> ChunkRef<'a> {
    #[inline]
    #[must_use]
    pub fn has_flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    /// Payload bytes as text, valid only for the text pipeline.
    #[inline]
    #[must_use]
    pub fn as_text(&self) -> &'a [u8] {
        self.data
    }
}

// ---------------------------------------------------------------------------
// Directory walking
// ---------------------------------------------------------------------------

/// A matched text file handed to the producers.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FileRecord {
    pub file_id: u32,
    pub size: u64,
    pub path: PathBuf,
}

/// Crawler counters.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct WalkStats {
    pub dirs_visited: u64,
    pub dirs_skipped: u64,
    pub dirs_failed: u64,
    pub depth_cut: u64,
    pub hidden_skipped: u64,
    pub symlinks_skipped: u64,
    pub files_seen: u64,
    pub files_skipped: u64,
    pub empty_files: u64,
    pub files_too_large: u64,
    pub files_binary: u64,
    pub files_faulted: u64,
    pub files_matched: u64,
    pub bytes_matched: u64,
}

impl WalkStats {
    /// The counters in a fixed order, for the on-disk cache.
    pub fn to_u64s(&self) -> [u64; 14] {
        [
            self.dirs_visited,
            self.dirs_skipped,
            self.dirs_failed,
            self.depth_cut,
            self.hidden_skipped,
            self.symlinks_skipped,
            self.files_seen,
            self.files_skipped,
            self.empty_files,
            self.files_too_large,
            self.files_binary,
            self.files_faulted,
            self.files_matched,
            self.bytes_matched,
        ]
    }

    /// Rebuild from the counters in the same order; any missing slot reads as 0.
    pub fn from_u64s(v: &[u64]) -> Self {
        let g = |i: usize| v.get(i).copied().unwrap_or(0);
        WalkStats {
            dirs_visited: g(0),
            dirs_skipped: g(1),
            dirs_failed: g(2),
            depth_cut: g(3),
            hidden_skipped: g(4),
            symlinks_skipped: g(5),
            files_seen: g(6),
            files_skipped: g(7),
            empty_files: g(8),
            files_too_large: g(9),
            files_binary: g(10),
            files_faulted: g(11),
            files_matched: g(12),
            bytes_matched: g(13),
        }
    }
}

#[inline]
fn is_text_file(name: &str) -> bool {
    let ext = match name.rfind('.') {
        Some(i) => &name[i + 1..],
        None => return false,
    };
    let ext = ext.to_ascii_lowercase();
    TEXT_EXTENSIONS.iter().any(|e| *e == ext)
}

#[inline]
fn is_skipped_dir(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    SKIP_DIR_NAMES.iter().any(|d| *d == lower)
}

/// Verdict for one candidate file.
enum Probe {
    Accept,
    SkippedExt,
    Empty,
    TooLarge,
    Binary,
    Faulted,
}

/// Read at most [`BINARY_SNIFF`] leading bytes (fewer when the file is smaller)
/// and report whether they contain a NUL marker. Short reads and
/// `Interrupted` are absorbed, so the probe costs one `open` plus one fill
/// loop on a stack buffer, and a locked file surfaces as an error, not a panic.
fn head_is_binary(path: &Path) -> io::Result<bool> {
    let mut file = File::open(path)?;
    let mut buf = [0u8; BINARY_SNIFF];
    let mut filled = 0usize;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(buf[..filled].contains(&0u8))
}

/// Classify one candidate: extension whitelist, size cap, then the head probe.
fn probe_candidate(path: &Path, size: u64) -> Probe {
    let name = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    if !is_text_file(&name) {
        return Probe::SkippedExt;
    }
    if size == 0 {
        return Probe::Empty;
    }
    if size > MAX_FILE_SIZE {
        return Probe::TooLarge;
    }
    match head_is_binary(path) {
        Ok(true) => Probe::Binary,
        Ok(false) => Probe::Accept,
        Err(_) => Probe::Faulted,
    }
}

/// Apply [`probe_candidate`] to one entry: push an accepted record and consume
/// a `file_id`, or increment the matching skip counter. `PermissionDenied` and
/// a file that vanished between `read_dir` and the probe are counted as faults,
/// never as a walk failure, so the crawl continues with the next entry.
fn record_candidate(
    path: PathBuf,
    size: u64,
    next_id: &mut u32,
    records: &mut Vec<FileRecord>,
    stats: &mut WalkStats,
) {
    match probe_candidate(&path, size) {
        Probe::Accept => {
            records.push(FileRecord {
                file_id: *next_id,
                size,
                path,
            });
            *next_id += 1;
            stats.files_matched += 1;
            stats.bytes_matched += size;
        }
        Probe::SkippedExt => stats.files_skipped += 1,
        Probe::Empty => stats.empty_files += 1,
        Probe::TooLarge => stats.files_too_large += 1,
        Probe::Binary => stats.files_binary += 1,
        Probe::Faulted => stats.files_faulted += 1,
    }
}

/// Collect text files below `root` with an explicit stack, pruning build/VCS/
/// vendor trees, hidden entries and symlinks. Allocation here is setup cost,
/// not the hot loop; every entry-level failure is counted and skipped, so the
/// walk returns `Ok` for the whole tree once the root itself is readable.
pub fn crawl(root: &Path, records: &mut Vec<FileRecord>, stats: &mut WalkStats) -> io::Result<()> {
    let meta = std::fs::symlink_metadata(root)?;
    let mut next_id = records.len() as u32;

    if meta.is_file() {
        record_candidate(root.to_path_buf(), meta.len(), &mut next_id, records, stats);
        return Ok(());
    }

    // Explicit stack: no recursion, no per-directory frame allocation beyond PathBuf.
    let mut stack: Vec<(PathBuf, u32)> = Vec::with_capacity(16);
    stack.push((root.to_path_buf(), 0));

    while let Some((dir, depth)) = stack.pop() {
        stats.dirs_visited += 1;
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(_) => {
                stats.dirs_failed += 1;
                continue;
            }
        };
        for entry in rd {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => {
                    stats.files_skipped += 1;
                    continue;
                }
            };
            let ft = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                stats.hidden_skipped += 1;
                continue;
            }
            if ft.is_symlink() {
                stats.symlinks_skipped += 1;
                continue;
            }
            if ft.is_dir() {
                if is_skipped_dir(&name) {
                    stats.dirs_skipped += 1;
                    continue;
                }
                if depth + 1 >= MAX_DEPTH {
                    stats.depth_cut += 1;
                    continue;
                }
                stack.push((entry.path(), depth + 1));
            } else if ft.is_file() {
                stats.files_seen += 1;
                let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                record_candidate(entry.path(), size, &mut next_id, records, stats);
            }
        }
    }
    Ok(())
}

/// Spread files round-robin over lanes so each lane gets a balanced byte load.
#[must_use]
pub fn partition(files: &[FileRecord], lanes: usize) -> Vec<Vec<FileRecord>> {
    let mut out = vec![Vec::new(); lanes];
    for (i, f) in files.iter().enumerate() {
        out[i % lanes].push(f.clone());
    }
    out
}

// ---------------------------------------------------------------------------
// Backoff ladder
// ---------------------------------------------------------------------------

/// Spin / yield / sleep counters for a producer that meets a full lane.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct Backoff {
    pub spins: u64,
    pub yields: u64,
    pub sleeps: u64,
    escalation: u32,
}

impl Backoff {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn reset(&mut self) {
        self.escalation = 0;
    }
}

/// Publish-capability bridge over the queue tokens.
pub trait Publish {
    fn ready(&self) -> bool;
    fn publish_bytes(&mut self, bytes: &[u8]) -> bool;
}

impl<'r> Publish for LaneProducer<'r> {
    #[inline]
    fn ready(&self) -> bool {
        LaneProducer::ready(self)
    }
    #[inline]
    fn publish_bytes(&mut self, bytes: &[u8]) -> bool {
        LaneProducer::publish(self, bytes).is_some()
    }
}

impl<'r> Publish for SeqProducer<'r> {
    #[inline]
    fn ready(&self) -> bool {
        SeqProducer::ready(self)
    }
    #[inline]
    fn publish_bytes(&mut self, bytes: &[u8]) -> bool {
        SeqProducer::publish(self, bytes).is_some()
    }
}

/// Ladder proven in our benchmarks: pure spin, then `yield_now`, then a
/// doubling sleep. Returns `true` once the chunk is accepted.
fn publish_with_backoff<P: Publish>(p: &mut P, wire: &[u8], backoff: &mut Backoff) -> bool {
    if p.publish_bytes(wire) {
        backoff.reset();
        return true;
    }
    for _ in 0..SPIN_YIELD_THRESHOLD {
        std::hint::spin_loop();
        backoff.spins += 1;
        if p.ready() && p.publish_bytes(wire) {
            backoff.reset();
            return true;
        }
    }
    loop {
        thread::yield_now();
        backoff.yields += 1;
        backoff.escalation = backoff.escalation.saturating_add(1);
        if p.publish_bytes(wire) {
            backoff.reset();
            return true;
        }
        if backoff.escalation > YIELD_ESCALATION_LIMIT {
            let micros = 64u64 << backoff.escalation.min(6);
            thread::sleep(Duration::from_micros(micros));
            backoff.sleeps += 1;
            if p.publish_bytes(wire) {
                backoff.reset();
                return true;
            }
        }
    }
}

/// Lane producer context: token + reusable wire buffer + backoff state.
pub struct LaneWriter<'l> {
    producer: LaneProducer<'l>,
    wire: [u8; LANE_SLOT_PAYLOAD],
    backoff: Backoff,
    published: u64,
    wire_bytes: u64,
}

impl<'l> LaneWriter<'l> {
    /// Claim the monophonic lane token for lane `lane_index`.
    pub fn acquire(lanes: &'l Lanes, lane_index: usize) -> Option<Self> {
        let producer = lanes.lane(lane_index)?;
        Some(Self {
            producer,
            wire: [0u8; LANE_SLOT_PAYLOAD],
            backoff: Backoff::new(),
            published: 0,
            wire_bytes: 0,
        })
    }

    #[inline]
    pub fn publish(&mut self, chunk: &TextChunk) -> bool {
        let n = chunk.encode(&mut self.wire);
        let ok = publish_with_backoff(&mut self.producer, &self.wire[..n], &mut self.backoff);
        if ok {
            self.published += 1;
            self.wire_bytes += n as u64;
        }
        ok
    }

    #[inline]
    #[must_use]
    pub fn written(&self) -> usize {
        self.producer.written()
    }
    #[inline]
    #[must_use]
    pub fn published(&self) -> u64 {
        self.published
    }
    #[inline]
    #[must_use]
    pub fn wire_bytes(&self) -> u64 {
        self.wire_bytes
    }
    #[inline]
    #[must_use]
    pub fn backoff(&self) -> Backoff {
        self.backoff
    }
}

/// Ring producer context, same ladder, sized to the ring slot.
pub struct SeqWriter<'r> {
    producer: SeqProducer<'r>,
    wire: [u8; SEQ_SLOT_PAYLOAD],
    backoff: Backoff,
    published: u64,
    wire_bytes: u64,
}

impl<'r> SeqWriter<'r> {
    pub fn acquire(ring: &'r SeqRing) -> Option<Self> {
        let producer = ring.producer()?;
        Some(Self {
            producer,
            wire: [0u8; SEQ_SLOT_PAYLOAD],
            backoff: Backoff::new(),
            published: 0,
            wire_bytes: 0,
        })
    }

    #[inline]
    pub fn publish(&mut self, chunk: &TextChunk) -> bool {
        let n = chunk.encode(&mut self.wire);
        let ok = publish_with_backoff(&mut self.producer, &self.wire[..n], &mut self.backoff);
        if ok {
            self.published += 1;
            self.wire_bytes += n as u64;
        }
        ok
    }

    #[inline]
    #[must_use]
    pub fn published(&self) -> u64 {
        self.published
    }
    #[inline]
    #[must_use]
    pub fn wire_bytes(&self) -> u64 {
        self.wire_bytes
    }
    #[inline]
    #[must_use]
    pub fn backoff(&self) -> Backoff {
        self.backoff
    }
}

// ---------------------------------------------------------------------------
// Zero-allocation chunk stream
// ---------------------------------------------------------------------------

/// Per-file counters produced by [`ChunkStream::pump`].
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct FileStats {
    pub file_id: u32,
    pub chunks: u64,
    pub lines: u64,
    pub payload_bytes: u64,
    pub binary: bool,
    pub oversized: bool,
}

/// Streaming reader over one file with a single reusable 64 KiB buffer.
///
/// Newlines are found directly on the raw byte slice; segments are written into
/// stack [`TextChunk`]s. No `BufRead::lines`, no heap `String`, no reallocation.
pub struct ChunkStream {
    file: File,
    buf: Box<[u8; READ_BUF]>,
    pending: usize,
    next: usize,
    filled: usize,
    eof: bool,
    snuffed: bool,
    binary: bool,
    oversized: bool,
    file_id: u32,
    chunk_seq: u32,
    chunks: u64,
    lines: u64,
    payload_bytes: u64,
    chunk_cap: usize,
}

impl ChunkStream {
    /// Lane path: chunks fill the widest lane slot.
    pub fn open(path: &Path, file_id: u32) -> io::Result<Self> {
        Self::open_with(path, file_id, CHUNK_DATA_MAX)
    }

    /// Ring path: chunks are capped to the ring slot payload.
    pub fn open_ring(path: &Path, file_id: u32) -> io::Result<Self> {
        Self::open_with(path, file_id, RING_CHUNK_DATA_MAX)
    }

    fn open_with(path: &Path, file_id: u32, chunk_cap: usize) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self {
            file,
            buf: Box::new([0u8; READ_BUF]),
            pending: 0,
            next: 0,
            filled: 0,
            eof: false,
            snuffed: false,
            binary: false,
            oversized: false,
            file_id,
            chunk_seq: 0,
            chunks: 0,
            lines: 0,
            payload_bytes: 0,
            chunk_cap,
        })
    }

    #[inline]
    #[must_use]
    pub fn stats(&self) -> FileStats {
        FileStats {
            file_id: self.file_id,
            chunks: self.chunks,
            lines: self.lines,
            payload_bytes: self.payload_bytes,
            binary: self.binary,
            oversized: self.oversized,
        }
    }

    /// Refill the buffer up to capacity, handling short reads and interrupts.
    #[inline]
    fn fill(&mut self) -> io::Result<()> {
        while self.filled < READ_BUF && !self.eof {
            match self.file.read(&mut self.buf[self.filled..]) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(n) => self.filled += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// One-shot binary sniff over the bytes read so far.
    #[inline]
    fn sniff(&mut self) {
        if !self.snuffed {
            self.snuffed = true;
            if self.buf[..self.filled.min(BINARY_SNIFF)].contains(&0u8) {
                self.binary = true;
            }
        }
    }

    /// Move the unemitted tail to the front of the buffer; no allocation.
    #[inline]
    fn compact(&mut self) {
        let rem = self.filled - self.pending;
        if rem > 0 {
            // Overlap-safe in-place move inside the same buffer, no allocation.
            self.buf.copy_within(self.pending..self.filled, 0);
        }
        self.filled = rem;
        self.pending = 0;
        self.next = rem;
    }

    /// Emit one segment `[start, end)` as chunks, splitting long lines with
    /// `FLAG_CONTINUED`; `tail_flag` marks how the segment terminated.
    fn emit_segment<F>(&mut self, start: usize, end: usize, tail_flag: u8, emit: &mut F)
    where
        F: FnMut(&TextChunk),
    {
        if start == end {
            // Blank line: a zero-length chunk keeps the line count exact.
            if tail_flag & FLAG_LINE_END != 0 {
                let chunk = TextChunk::new(
                    self.file_id,
                    self.chunk_seq,
                    FLAG_LINE_START | FLAG_LINE_END,
                );
                self.chunk_seq += 1;
                self.chunks += 1;
                self.lines += 1;
                emit(&chunk);
            }
            return;
        }
        let mut idx = start;
        let mut first = true;
        while idx < end {
            let n = (end - idx).min(self.chunk_cap);
            let more = idx + n < end;
            let flags = (if first { FLAG_LINE_START } else { 0 })
                | (if more { FLAG_CONTINUED } else { tail_flag });
            let mut chunk = TextChunk::new(self.file_id, self.chunk_seq, flags);
            chunk.data[..n].copy_from_slice(&self.buf[idx..idx + n]);
            chunk.data_len = n as u16;
            self.chunk_seq += 1;
            self.chunks += 1;
            self.payload_bytes += n as u64;
            if flags & FLAG_LINE_END != 0 {
                self.lines += 1;
            }
            emit(&chunk);
            idx += n;
            first = false;
        }
    }

    /// Drive the hot loop: refill, scan for `\n`, emit chunks through `emit`.
    pub fn pump<F>(&mut self, mut emit: F) -> io::Result<FileStats>
    where
        F: FnMut(&TextChunk),
    {
        loop {
            if self.next >= self.filled {
                if self.eof {
                    self.emit_segment(self.pending, self.filled, FLAG_EOF, &mut emit);
                    break;
                }
                self.compact();
                self.fill()?;
                self.sniff();
                if self.binary {
                    return Ok(self.stats());
                }
                continue;
            }
            let nl = find_newline(&self.buf[self.next..self.filled]);
            if let Some(i) = nl {
                let seg_end = self.next + i;
                self.emit_segment(self.pending, seg_end, FLAG_LINE_END, &mut emit);
                self.pending = seg_end + 1;
                self.next = self.pending;
                continue;
            }
            // No newline in the scanned window:
            // EOF known -> tail is the final line; buffer not full -> read more
            // before deciding; buffer full -> force continuation chunks.
            if self.eof {
                self.emit_segment(self.pending, self.filled, FLAG_EOF, &mut emit);
                self.pending = self.filled;
                break;
            }
            if self.filled < READ_BUF {
                self.compact();
                self.fill()?;
                self.sniff();
                if self.binary {
                    return Ok(self.stats());
                }
                continue;
            }
            self.emit_segment(self.pending, self.filled, 0, &mut emit);
            self.pending = self.filled;
            self.next = self.pending;
            if self.payload_bytes > MAX_FILE_SIZE {
                // The file outgrew the ingest cap while we streamed it (a log
                // that exploded mid-crawl): stop cleanly, the chunks published
                // so far stay valid, the lane moves to the next record.
                self.oversized = true;
                break;
            }
        }
        Ok(self.stats())
    }
}

#[inline]
fn find_newline(buf: &[u8]) -> Option<usize> {
    for (i, b) in buf.iter().enumerate() {
        if *b == b'\n' {
            return Some(i);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Producer / consumer wiring
// ---------------------------------------------------------------------------

/// Aggregated producer counters for one lane.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ProducerStats {
    pub files_read: u64,
    pub files_binary: u64,
    pub files_oversized: u64,
    pub files_faulted: u64,
    pub chunks: u64,
    pub lines: u64,
    pub payload_bytes: u64,
    pub published: u64,
    pub wire_bytes: u64,
    pub spins: u64,
    pub yields: u64,
    pub sleeps: u64,
}

impl ProducerStats {
    pub fn merge(&mut self, other: ProducerStats) {
        self.files_read += other.files_read;
        self.files_binary += other.files_binary;
        self.files_oversized += other.files_oversized;
        self.files_faulted += other.files_faulted;
        self.chunks += other.chunks;
        self.lines += other.lines;
        self.payload_bytes += other.payload_bytes;
        self.published += other.published;
        self.wire_bytes += other.wire_bytes;
        self.spins += other.spins;
        self.yields += other.yields;
        self.sleeps += other.sleeps;
    }

    /// The counters in a fixed order, for the on-disk cache.
    pub fn to_u64s(&self) -> [u64; 12] {
        [
            self.files_read,
            self.files_binary,
            self.files_oversized,
            self.files_faulted,
            self.chunks,
            self.lines,
            self.payload_bytes,
            self.published,
            self.wire_bytes,
            self.spins,
            self.yields,
            self.sleeps,
        ]
    }

    /// Rebuild from the counters in the same order; any missing slot reads as 0.
    pub fn from_u64s(v: &[u64]) -> Self {
        let g = |i: usize| v.get(i).copied().unwrap_or(0);
        ProducerStats {
            files_read: g(0),
            files_binary: g(1),
            files_oversized: g(2),
            files_faulted: g(3),
            chunks: g(4),
            lines: g(5),
            payload_bytes: g(6),
            published: g(7),
            wire_bytes: g(8),
            spins: g(9),
            yields: g(10),
            sleeps: g(11),
        }
    }
}

/// Consumer counters; `record` is the zero-copy sink for each drained payload.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ConsumerStats {
    pub chunks: u64,
    pub wire_bytes: u64,
    pub payload_bytes: u64,
    pub lines: u64,
    pub mid_chunks: u64,
    pub bad_chunks: u64,
}

impl ConsumerStats {
    #[inline]
    pub fn record(&mut self, bytes: &[u8]) {
        match TextChunk::decode(bytes) {
            Some(c) => {
                self.chunks += 1;
                self.wire_bytes += bytes.len() as u64;
                self.payload_bytes += c.data_len as u64;
                if c.has_flag(FLAG_LINE_END) {
                    self.lines += 1;
                }
                if !c.has_flag(FLAG_LINE_START) {
                    self.mid_chunks += 1;
                }
            }
            None => self.bad_chunks += 1,
        }
    }

    pub fn merge(&mut self, other: ConsumerStats) {
        self.chunks += other.chunks;
        self.wire_bytes += other.wire_bytes;
        self.payload_bytes += other.payload_bytes;
        self.lines += other.lines;
        self.mid_chunks += other.mid_chunks;
        self.bad_chunks += other.bad_chunks;
    }

    /// The counters in a fixed order, for the on-disk cache.
    pub fn to_u64s(&self) -> [u64; 6] {
        [
            self.chunks,
            self.wire_bytes,
            self.payload_bytes,
            self.lines,
            self.mid_chunks,
            self.bad_chunks,
        ]
    }

    /// Rebuild from the counters in the same order; any missing slot reads as 0.
    pub fn from_u64s(v: &[u64]) -> Self {
        let g = |i: usize| v.get(i).copied().unwrap_or(0);
        ConsumerStats {
            chunks: g(0),
            wire_bytes: g(1),
            payload_bytes: g(2),
            lines: g(3),
            mid_chunks: g(4),
            bad_chunks: g(5),
        }
    }
}

/// One producer thread: claim lane `lane`, stream every file in `files`.
///
/// A per-file fault is scoped to that file. `PermissionDenied`, a record whose
/// file vanished after the crawl, and a mid-read I/O error all increment
/// `files_faulted` and the lane continues with the next record; the function
/// only returns `Err` when the lane token is already claimed, so a bad file
/// can never end a lane, a drain, or a build.
pub fn run_producer(lanes: &Lanes, lane: usize, files: &[FileRecord]) -> io::Result<ProducerStats> {
    let mut writer = match LaneWriter::acquire(lanes, lane) {
        Some(w) => w,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("lane {lane} token already taken"),
            ))
        }
    };
    let mut stats = ProducerStats::default();
    for rec in files {
        let outcome = match ChunkStream::open(&rec.path, rec.file_id) {
            Ok(mut stream) => stream.pump(|chunk| {
                writer.publish(chunk);
            }),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(fs) => {
                stats.files_read += 1;
                if fs.binary {
                    stats.files_binary += 1;
                }
                if fs.oversized {
                    stats.files_oversized += 1;
                }
                stats.chunks += fs.chunks;
                stats.lines += fs.lines;
                stats.payload_bytes += fs.payload_bytes;
            }
            Err(_) => stats.files_faulted += 1,
        }
    }
    stats.published = writer.published();
    stats.wire_bytes = writer.wire_bytes();
    let b = writer.backoff();
    stats.spins += b.spins;
    stats.yields += b.yields;
    stats.sleeps += b.sleeps;
    Ok(stats)
}

/// Drain every lane until `active` producers reach zero and the lanes are empty.
pub fn drain_until_done<F>(
    consumer: &mut LanesConsumer,
    active: &AtomicUsize,
    mut on_chunk: F,
) -> usize
where
    F: FnMut(&[u8]),
{
    let mut total = 0;
    loop {
        let n = consumer.drain(&mut on_chunk);
        total += n;
        if n == 0 {
            if active.load(Ordering::Acquire) == 0 {
                let m = consumer.drain(&mut on_chunk);
                if m == 0 {
                    return total;
                }
                total += m;
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn tmp_root(tag: &str) -> PathBuf {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(tag);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    fn write(path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    /// Reassemble chunks the way a consumer pipeline would: append payload bytes,
    /// close a line when `FLAG_LINE_END` is set.
    fn reassemble(chunks: &[(u32, Vec<u8>, u8)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (_, d, flags) in chunks {
            out.extend_from_slice(d);
            if flags & FLAG_LINE_END != 0 {
                out.push(b'\n');
            }
        }
        out
    }

    fn collect_chunks(stream: &mut ChunkStream) -> Vec<(u32, Vec<u8>, u8)> {
        let mut out: Vec<(u32, Vec<u8>, u8)> = Vec::new();
        let stats = stream
            .pump(|c| {
                out.push((c.chunk_seq, c.data[..c.data_len as usize].to_vec(), c.flags));
            })
            .unwrap();
        assert_eq!(stats.chunks, out.len() as u64);
        out
    }

    #[test]
    fn chunk_slot_is_exactly_one_lane_line() {
        assert_eq!(core::mem::size_of::<TextChunk>(), LANE_SLOT_PAYLOAD);
        assert_eq!(CHUNK_HEADER + CHUNK_DATA_MAX, LANE_SLOT_PAYLOAD);
        assert!(CHUNK_HEADER + RING_CHUNK_DATA_MAX <= SEQ_SLOT_PAYLOAD);
    }

    #[test]
    fn wire_roundtrip_preserves_header() {
        let mut c = TextChunk::new(0xdead_beef, 7, FLAG_LINE_START | FLAG_LINE_END);
        c.fill(b"seek sage");
        let mut wire = [0u8; LANE_SLOT_PAYLOAD];
        let n = c.encode(&mut wire);
        assert_eq!(n, CHUNK_HEADER + c.data_len as usize);
        let d = TextChunk::decode(&wire[..n]).expect("decode");
        assert_eq!(d.file_id, 0xdead_beef);
        assert_eq!(d.chunk_seq, 7);
        assert_eq!(d.flags, FLAG_LINE_START | FLAG_LINE_END);
        assert_eq!(d.data, b"seek sage");
    }

    #[test]
    fn crawl_filters_extensions_and_prunes_trees() {
        let root = tmp_root("crawl-filter");
        write(&root.join("a.txt"), b"one");
        write(&root.join("B.MD"), b"two");
        write(&root.join("c.rs"), b"three");
        write(&root.join("d.log"), b"four");
        write(&root.join("skip.bin"), b"bin");
        write(&root.join("noext"), b"x");
        write(&root.join("empty.txt"), b"");
        write(&root.join("target/x.txt"), b"x");
        write(&root.join(".git/y.txt"), b"x");
        write(&root.join("node_modules/z.md"), b"x");
        write(&root.join("sub/deep/n.rs"), b"x");
        write(&root.join("sub/.hidden_dir/h.txt"), b"x");
        write(&root.join("sub/out/n.txt"), b"x");
        write(&root.join("sub/.dot.txt"), b"x");

        let mut recs = Vec::new();
        let mut st = WalkStats::default();
        crawl(&root, &mut recs, &mut st).unwrap();

        let names: Vec<String> = recs
            .iter()
            .map(|r| r.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(recs.len(), 5, "matched names: {names:?}");
        assert_eq!(st.files_matched, 5);
        assert!(names.contains(&"a.txt".to_string()));
        assert!(names.contains(&"B.MD".to_string()));
        assert!(names.contains(&"n.rs".to_string()));
        assert_eq!(st.dirs_skipped, 3, "target/.git/node_modules pruned");
        assert!(st.hidden_skipped >= 3);
        assert_eq!(st.empty_files, 1);

        // File ids are unique and dense.
        let ids: Vec<u32> = recs.iter().map(|r| r.file_id).collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn extensions_cover_every_supported_format() {
        assert_eq!(TEXT_EXTENSIONS.len(), 51);
        for ext in TEXT_EXTENSIONS {
            let lower = format!("some_file.{ext}");
            assert!(is_text_file(&lower), ".{ext} must be indexed");
            let upper = format!("SOME_FILE.{}", ext.to_uppercase());
            assert!(is_text_file(&upper), "uppercase .{ext} must be indexed");
        }
        assert!(!is_text_file("binary.exe"));
        assert!(!is_text_file("image.png"));
        assert!(!is_text_file("archive.zip"));
        assert!(!is_text_file("archive.tar.gz"));
        assert!(!is_text_file("noext"));
    }

    #[test]
    fn crawl_indexes_every_supported_extension() {
        let root = tmp_root("crawl-extensions");
        for ext in TEXT_EXTENSIONS {
            write(&root.join(format!("sample.{ext}")), b"sample content");
            write(
                &root.join(format!("CAP.{}", ext.to_uppercase())),
                b"upper sample",
            );
        }
        let mut recs = Vec::new();
        let mut st = WalkStats::default();
        crawl(&root, &mut recs, &mut st).unwrap();

        assert_eq!(recs.len(), 2 * TEXT_EXTENSIONS.len());
        assert_eq!(st.files_matched, recs.len() as u64);
        assert_eq!(st.files_seen, recs.len() as u64);
        assert_eq!(st.files_skipped, 0);
        assert_eq!(st.files_binary, 0);
        assert_eq!(st.files_faulted, 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn size_and_binary_caps_are_decided_before_ingest() {
        assert_eq!(MAX_FILE_SIZE, 64 * 1024 * 1024);
        assert_eq!(BINARY_SNIFF, 1024);

        // Size gates are decided from the directory-entry metadata, so an
        // oversized or empty file is never opened at all.
        assert!(matches!(
            probe_candidate(Path::new("x.txt"), 0),
            Probe::Empty
        ));
        assert!(matches!(
            probe_candidate(Path::new("x.txt"), MAX_FILE_SIZE + 1),
            Probe::TooLarge
        ));
        assert!(matches!(
            probe_candidate(Path::new("x.bin"), 10),
            Probe::SkippedExt
        ));

        // A head that cannot be read (locked, vanished, permission-denied file)
        // is a counted fault, never a walk failure and never a producer abort.
        assert!(matches!(
            probe_candidate(Path::new("absent-missing-file.txt"), 10),
            Probe::Faulted
        ));
        assert!(matches!(
            probe_candidate(Path::new("x.txt"), 10),
            Probe::Faulted
        ));
    }

    #[test]
    fn crawl_head_probe_rejects_binary_and_accepts_text() {
        let root = tmp_root("crawl-probe");
        // NUL inside the first BINARY_SNIFF bytes: binary, never ingested.
        let mut bin = Vec::new();
        bin.extend_from_slice(b"looks like text");
        bin.push(0u8);
        bin.extend_from_slice(b"then bytes");
        write(&root.join("fake.rs"), &bin);
        write(&root.join("blob.log"), &bin);
        write(&root.join("blob.json"), &bin);
        // NUL past the sniff window: the head is text, so the probe accepts it.
        let mut late = vec![b'a'; BINARY_SNIFF + 16];
        late[BINARY_SNIFF + 8] = 0u8;
        write(&root.join("late.rs"), &late);
        write(&root.join("tiny.md"), b"tiny");
        write(&root.join("skip.bin"), &bin);
        write(&root.join("empty.txt"), b"");
        let huge = vec![b'a'; MAX_FILE_SIZE as usize + 1];
        write(&root.join("huge.h"), &huge);

        let mut recs = Vec::new();
        let mut st = WalkStats::default();
        crawl(&root, &mut recs, &mut st).unwrap();

        let names: Vec<String> = recs
            .iter()
            .map(|r| r.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["late.rs", "tiny.md"]);
        assert_eq!(st.files_matched, 2);
        assert_eq!(st.files_binary, 3);
        assert_eq!(st.files_too_large, 1);
        assert_eq!(st.empty_files, 1);
        assert_eq!(st.files_skipped, 1);
        assert_eq!(st.bytes_matched, (BINARY_SNIFF + 16 + 4) as u64);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn producer_lane_survives_faulted_files() {
        let root = tmp_root("producer-fault");
        write(
            &root.join("good.rs"),
            b"seeksage survives indexing\nsecond line\n",
        );
        write(&root.join("ghost.rs"), b"ghost");

        // One record names a file that never existed, one names a file deleted
        // after the crawl, one names the readable file.
        let records = vec![
            FileRecord {
                file_id: 0,
                size: 5,
                path: root.join("missing.txt"),
            },
            FileRecord {
                file_id: 1,
                size: 20,
                path: root.join("good.rs"),
            },
            FileRecord {
                file_id: 2,
                size: 5,
                path: root.join("ghost.rs"),
            },
        ];
        std::fs::remove_file(root.join("ghost.rs")).unwrap();

        let lanes = Lanes::new(2, 1024);
        let active = AtomicUsize::new(1);
        let mut consumer = lanes.consumer().expect("consumer token");

        let (lane, shard, drained) = std::thread::scope(|s| {
            let producer = s.spawn(|| {
                let outcome = run_producer(&lanes, 0, &records);
                active.fetch_sub(1, Ordering::Release);
                outcome
            });
            let mut shard = crate::index::IndexShard::new();
            let drained = drain_until_done(&mut consumer, &active, |wire| {
                shard.ingest_wire(wire);
            });
            (producer.join().unwrap().unwrap(), shard, drained)
        });

        // Two files faulted; the lane never aborted, never broke the channel.
        assert_eq!(lane.files_faulted, 2);
        assert_eq!(lane.files_read, 1);
        assert_eq!(lane.published, drained as u64);
        assert_eq!(shard.chunks, lane.published);
        assert_eq!(shard.bad_chunks, 0);
        assert_eq!(shard.term_postings("seeksage").len(), 1);
        assert_eq!(shard.term_postings("indexing").len(), 1);
        assert_eq!(shard.term_postings("second").len(), 1);
        assert!(shard.term_postings("ghost").is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn pump_splits_on_newlines_and_blank_lines() {
        let root = tmp_root("pump-lines");
        let body = "alpha\nbeta\n\nlong line that is definitely longer than the payload cap\nzeta";
        write(&root.join("l.txt"), body.as_bytes());

        let mut stream = ChunkStream::open(&root.join("l.txt"), 11).unwrap();
        let chunks = collect_chunks(&mut stream);
        let stats = stream.stats();

        assert_eq!(reassemble(&chunks), body.as_bytes());
        assert_eq!(
            stats.lines, 4,
            "alpha, beta, blank, long line end with newline"
        );
        assert_eq!(stats.payload_bytes, body.len() as u64 - 4); // \n are delimiters, not payload
        assert_eq!(stats.binary, false);

        let first = &chunks[0];
        assert_eq!(first.1, b"alpha");
        assert_eq!(first.2, FLAG_LINE_START | FLAG_LINE_END);

        // Continuation chunks: the long line spans more than one slot.
        let cont = chunks.iter().filter(|c| c.2 & FLAG_CONTINUED != 0).count();
        assert!(cont >= 1, "long line must split into continued chunks");

        let last = chunks.last().unwrap();
        assert_eq!(last.1, b"zeta");
        assert_eq!(last.2, FLAG_LINE_START | FLAG_EOF);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn pump_handles_buffer_wrap_and_long_lines() {
        let root = tmp_root("pump-wrap");
        // A single 200 KiB line, no trailing newline: forces compaction and continuation.
        let mut body = Vec::new();
        body.extend_from_slice(b"short\n");
        let mut big_line = Vec::new();
        for i in 0..200_000u32 {
            big_line.push(if i % 7 == 0 { b'x' } else { b'y' });
        }
        body.extend_from_slice(&big_line);
        body.extend_from_slice(b"\ntail");
        write(&root.join("big.rs"), &body);

        let mut stream = ChunkStream::open(&root.join("big.rs"), 12).unwrap();
        let chunks = collect_chunks(&mut stream);
        let stats = stream.stats();
        assert_eq!(reassemble(&chunks), body);
        assert_eq!(stats.lines, 2);
        assert_eq!(stats.payload_bytes, body.len() as u64 - 2);

        // chunk_seq must be strictly monotonic.
        let mut prev = 0u32;
        for (seq, _, _) in &chunks {
            assert_eq!(*seq, prev);
            prev += 1;
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn pump_flags_binary_file_and_stops() {
        let root = tmp_root("pump-binary");
        // A .rs name with NUL bytes inside: the extension passes, the sniff stops it.
        let mut body = Vec::new();
        body.extend_from_slice(b"leading text");
        body.push(0u8);
        body.extend_from_slice(b"more bytes after the nul marker");
        write(&root.join("fake.rs"), &body);

        let mut stream = ChunkStream::open(&root.join("fake.rs"), 13).unwrap();
        let stats = stream.pump(|_| {}).unwrap();
        assert!(stats.binary);
        assert_eq!(stats.chunks, 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn hot_loop_allocates_nothing() {
        let root = tmp_root("pump-alloc");
        let mut body = Vec::new();
        for i in 0..70_000u64 {
            body.push(((i % 26) as u8) + b'a');
            if i % 41 == 0 {
                body.push(b'\n');
            }
        }
        write(&root.join("flow.txt"), &body);

        // Lane capacity covers the whole chunk count: this test is single-threaded,
        // so no consumer runs during the counted window.
        let lanes = Lanes::new(1, 8192);
        let mut writer = LaneWriter::acquire(&lanes, 0).unwrap();
        let mut stream = ChunkStream::open(&root.join("flow.txt"), 14).unwrap();

        crate::alloc_count_start();
        let stats = stream
            .pump(|c| {
                writer.publish(c);
            })
            .unwrap();
        let allocations = crate::alloc_count();
        crate::alloc_count_stop();

        assert!(stats.chunks > 1_500, "chunks: {}", stats.chunks);
        assert_eq!(allocations, 0, "the hot loop must not touch the heap");

        let mut consumer = lanes.consumer().unwrap();
        let active = AtomicUsize::new(0);
        let mut st = ConsumerStats::default();
        let drained = drain_until_done(&mut consumer, &active, |bytes| st.record(bytes));
        assert_eq!(drained as u64, writer.published());
        assert_eq!(st.payload_bytes, stats.payload_bytes);
        assert_eq!(st.bad_chunks, 0);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn lanes_end_to_end_reassembly() {
        let root = tmp_root("e2e-lanes");
        // Four files, one of them huge, one with very long lines.
        write(&root.join("a.txt"), b"alpha\nbeta\ngamma\n");
        let mut big = Vec::new();
        for i in 0..300_000u64 {
            big.push(((i % 90) as u8) + b'a');
            if i % 53 == 0 {
                big.push(b'\n');
            }
        }
        write(&root.join("big.md"), &big);
        let mut long_lines = Vec::new();
        for i in 0..400u64 {
            long_lines.extend_from_slice(b"word ");
            if i % 30 == 0 {
                long_lines.extend_from_slice(b"a long line that exceeds the lane payload cap\n");
            }
            if i % 11 == 0 {
                long_lines.push(b'\n');
            }
        }
        write(&root.join("c.rs"), &long_lines);
        write(&root.join("d.log"), b"no trailing newline at all");

        let originals: HashMap<u32, Vec<u8>> = (0..4u32)
            .map(|id| {
                let p = match id {
                    0 => root.join("a.txt"),
                    1 => root.join("big.md"),
                    2 => root.join("c.rs"),
                    _ => root.join("d.log"),
                };
                (id, std::fs::read(&p).unwrap())
            })
            .collect();

        let lane_count = 3;
        let files: Vec<FileRecord> = (0..4u32)
            .map(|id| {
                let p = match id {
                    0 => root.join("a.txt"),
                    1 => root.join("big.md"),
                    2 => root.join("c.rs"),
                    _ => root.join("d.log"),
                };
                FileRecord {
                    file_id: id,
                    size: originals[&id].len() as u64,
                    path: p,
                }
            })
            .collect();
        let parts = partition(&files, lane_count);

        let lanes = Lanes::new(lane_count, 256);
        let mut consumer = lanes.consumer().expect("single consumer token");
        let active = AtomicUsize::new(lane_count);

        let (prods, cons, map) = std::thread::scope(|s| {
            let handles: Vec<_> = (0..lane_count)
                .map(|i| {
                    // Copy the references into locals so the inner `move` closure
                    // takes its own handles instead of moving out of the outer capture.
                    let lanes_ref = &lanes;
                    let active_ref = &active;
                    let part = &parts[i];
                    s.spawn(move || -> std::io::Result<ProducerStats> {
                        let r = run_producer(lanes_ref, i, part);
                        active_ref.fetch_sub(1, Ordering::Release);
                        r
                    })
                })
                .collect();
            let consumer_ref = &mut consumer;
            let active_ref = &active;
            let ch = s.spawn(move || {
                let mut st = ConsumerStats::default();
                let mut map: HashMap<u32, Vec<u8>> = HashMap::new();
                drain_until_done(consumer_ref, active_ref, |bytes| {
                    st.record(bytes);
                    let c = TextChunk::decode(bytes).expect("decode");
                    let buf = map.entry(c.file_id).or_default();
                    buf.extend_from_slice(c.data);
                    if c.has_flag(FLAG_LINE_END) {
                        buf.push(b'\n');
                    }
                });
                (st, map)
            });
            let mut agg = ProducerStats::default();
            for h in handles {
                agg.merge(h.join().unwrap().unwrap());
            }
            let (st, map) = ch.join().unwrap();
            (agg, st, map)
        });

        assert_eq!(cons.bad_chunks, 0);
        for (id, original) in &originals {
            assert_eq!(
                map.get(id),
                Some(original),
                "file {id} must reassemble byte-for-byte"
            );
        }
        // Payload counters exclude the '\n' delimiters: they are not carried inside chunks.
        let payload_total: u64 = originals
            .values()
            .map(|v| v.iter().filter(|b| **b != b'\n').count() as u64)
            .sum();
        assert_eq!(prods.payload_bytes, payload_total);
        assert_eq!(cons.payload_bytes, payload_total);
        assert_eq!(cons.chunks, prods.published);
        assert_eq!(cons.lines, prods.lines);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn ring_end_to_end_reassembly() {
        let root = tmp_root("e2e-ring");
        let body = b"ring chunk stream\nwith lines that are long enough to be split across ring slots\nlast";
        write(&root.join("r.txt"), body);

        let ring = SeqRing::new(256);
        let mut writer = SeqWriter::acquire(&ring).expect("ring producer token");
        let mut stream = ChunkStream::open_ring(&root.join("r.txt"), 99).unwrap();
        let stats = stream
            .pump(|c| {
                writer.publish(c);
            })
            .unwrap();

        let mut consumer = ring.consumer().expect("ring consumer token");
        let mut out = Vec::new();
        let drained = consumer.drain(|bytes| {
            let c = TextChunk::decode(bytes).expect("decode");
            out.extend_from_slice(c.data);
            if c.has_flag(FLAG_LINE_END) {
                out.push(b'\n');
            }
        });
        assert_eq!(drained as u64, writer.published());
        assert_eq!(out, body.to_vec());
        assert_eq!(stats.payload_bytes, body.len() as u64 - 2);
        assert!(stats.chunks > 2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_failed_build_leaves_nothing_behind_for_the_next_one() {
        let root = tmp_root("failed-then-valid");
        let good = root.join("good");
        write(&good.join("a.rs"), b"pub fn index_file() {}");
        write(&good.join("b.h"), b"int index_file(void);");

        // Request one: a root that cannot be crawled at all.
        let missing = root.join("missing");
        let first = crate::build_index(&missing, 4);
        assert!(first.is_err(), "a missing root must be a plain error");

        // Request two: a fresh directory must build exactly as cleanly as a
        // first request, because no build state survives the previous request.
        let second = crate::build_index(&good, 4).expect("a later directory must build cleanly");
        assert_eq!(second.walk.files_matched, 2);
        assert!(second.lane_errors.is_empty());
        assert!(second.index.search("index_file").len() >= 2);
        assert!(second.index.lists_sorted());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_panicking_build_does_not_poison_the_next_one() {
        // Same shape as the GUI worker: catch_unwind turns a panicking job
        // into an ordinary Err, and the next request gets a fresh thread.
        let root = tmp_root("panic-then-valid");
        let good = root.join("good");
        write(&good.join("a.rs"), b"pub fn index_file() {}");

        let first = {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = crate::build_index(&root, 4);
                panic!("simulated worker panic");
            }));
            if outcome.is_ok() {
                Ok(())
            } else {
                Err("recovered from a panicking build")
            }
        };
        assert_eq!(first, Err("recovered from a panicking build"));

        // The next request runs the identical code path and must be clean.
        let second = crate::build_index(&good, 4).expect("the next build must be clean");
        assert_eq!(second.walk.files_matched, 1);
        assert_eq!(second.index.search("index_file").len(), 1);
        let _ = std::fs::remove_dir_all(&root);
    }
}
