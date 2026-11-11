#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
//! SeekSage native desktop console, built on `eframe` + `egui`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::panic::{AssertUnwindSafe, UnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use eframe::egui;
use eframe::egui::FontId;
use seeksage::export::{write_file_export, ExportFormat, ExportHit, ExportMeta, ExportSummary};
use seeksage::index::{key_file, key_line, merge_shards, IndexShard};
use seeksage::parser::{
    crawl, drain_until_done, partition, run_producer, ConsumerStats, FileRecord, ProducerStats,
    WalkStats,
};
use seeksage::queue::Lanes;
use seeksage::{
    default_lanes, extract_context, extract_snippet, load_cache, save_cache, Built, LANE_CAP,
};

// Palette ---------------------------------------------------------------------

/// Complete color scheme for Midnight Owl (dark) and Ceramic Mint (light).
#[derive(Clone, Copy, Debug)]
struct Palette {
    /// Deepest background (code editor canvas & inset fields).
    canvas: egui::Color32,
    /// Primary panel background.
    panel: egui::Color32,
    /// Top/bottom chrome bars.
    chrome: egui::Color32,
    /// Interactive card and pill background at rest.
    card: egui::Color32,
    /// Hovered card or button background.
    raised: egui::Color32,
    /// Crisp 1px structural border.
    border: egui::Color32,
    /// Primary turquoise accent (Wise Owl lens identity).
    accent: egui::Color32,
    /// Secondary emerald accent (Ready state & primary actions).
    emerald: egui::Color32,
    /// Warm amber-gold accent (Owl beak / sun / match markers).
    gold: egui::Color32,
    /// Subtle translucent fill for selected rows and active badges.
    selected_fill: egui::Color32,
    /// Subtle translucent background behind matched text tokens.
    match_bg: egui::Color32,
    /// Primary readable text.
    text: egui::Color32,
    /// Muted secondary text.
    dim: egui::Color32,
    /// Warning / error / clear accent.
    warn: egui::Color32,
}

impl Palette {
    /// Midnight Owl: deep obsidian-slate surfaces with turquoise and emerald accents.
    fn dark() -> Self {
        Self {
            canvas: rgb(11, 17, 23),         // #0B1117
            panel: rgb(16, 24, 32),          // #101820
            chrome: rgb(13, 20, 27),         // #0D141B
            card: rgb(23, 35, 46),           // #17232E
            raised: rgb(32, 48, 62),         // #20303E
            border: rgba(148, 197, 204, 26), // subtle icy-teal hairline
            accent: rgb(45, 212, 191),       // #2DD4BF turquoise
            emerald: rgb(16, 185, 129),      // #10B981 emerald
            gold: rgb(245, 158, 11),         // #F59E0B warm gold
            // Solid dark-teal fills: a translucent wash over a dark canvas reads
            // muddy under light text, so the selected row and the matched tokens
            // get opaque, calm fills that keep the text crisp.
            selected_fill: rgb(22, 52, 54), // solid dark teal, selected row
            match_bg: rgb(19, 78, 74),      // solid dark teal, token highlight
            text: rgb(228, 238, 244),       // #E4EEF4
            dim: rgb(125, 147, 161),        // #7D93A1
            warn: rgb(248, 113, 113),       // #F87171
        }
    }

    /// Ceramic Mint: soft daylight surfaces with deep teal and emerald accents.
    fn light() -> Self {
        Self {
            canvas: rgb(255, 255, 255),            // #FFFFFF
            panel: rgb(243, 248, 248),             // #F3F8F8
            chrome: rgb(232, 242, 242),            // #E8F2F2
            card: rgb(255, 255, 255),              // #FFFFFF
            raised: rgb(223, 238, 237),            // #DFEEED
            border: rgba(15, 76, 82, 30),          // crisp slate-teal hairline
            accent: rgb(13, 148, 136),             // #0D9488 deep turquoise
            emerald: rgb(5, 150, 105),             // #059669 deep emerald
            gold: rgb(217, 119, 6),                // #D97706 amber
            selected_fill: rgba(13, 148, 136, 28), // soft teal wash
            match_bg: rgba(13, 148, 136, 38),      // token highlight wash
            text: rgb(19, 42, 48),                 // #132A30
            dim: rgb(94, 120, 128),                // #5E7880
            warn: rgb(220, 38, 38),                // #DC2626
        }
    }
}

const fn rgb(r: u8, g: u8, b: u8) -> egui::Color32 {
    egui::Color32::from_rgb(r, g, b)
}

const fn rgba(r: u8, g: u8, b: u8, a: u8) -> egui::Color32 {
    egui::Color32::from_rgba_premultiplied(r, g, b, a)
}

const ROW_HEIGHT: f32 = 50.0;
const LINE_HEIGHT: f32 = 21.0;
const SNIPPET_CAP: usize = 512;
const SNIPPET_CHARS: usize = 150;
/// How far up a result row looks for the enclosing scope header it sits in.
const SCOPE_RADIUS: usize = 40;
const CONTEXT_RADIUS: usize = 18;
const NO_SELECTION: usize = usize::MAX;
const POLL_DELAY: Duration = Duration::from_millis(30);
/// The repaint cadence `Watch` mode keeps the UI warm with.
const WATCH_REPAINT_DELAY: Duration = Duration::from_millis(500);
/// How often `Watch` mode looks at the tree metadata.
const WATCH_CHECK_INTERVAL: Duration = Duration::from_millis(1000);
/// How long the tree must hold still before a watched rebuild fires, so a
/// running writer is never caught mid-save.
const WATCH_QUIET_WINDOW: Duration = Duration::from_millis(600);
/// The 64-bit FNV-1a offset and prime constants.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const RECENT_CAP: usize = 8;
const SUBDIR_CAP: usize = 100;
/// Committed queries kept for Alt+Up / Alt+Down history in the search box.
const HISTORY_CAP: usize = 10;
/// Chunks between two live progress writes: the consumer publishes counters every
/// this many drained chunks, so the bar moves smoothly without flooding the UI.
const PROGRESS_STEP: u64 = 256;

// Indexing phase --------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Phase {
    Idle,
    Indexing,
    Ready,
    Failed,
}

impl Phase {
    fn word(self) -> &'static str {
        match self {
            Phase::Idle => "Idle",
            Phase::Indexing => "Indexing",
            Phase::Ready => "Ready",
            Phase::Failed => "Failed",
        }
    }

    fn color(self, p: &Palette) -> egui::Color32 {
        match self {
            Phase::Idle => p.dim,
            Phase::Indexing => p.accent,
            Phase::Ready => p.emerald,
            Phase::Failed => p.warn,
        }
    }
}

// How the result rows are ordered --------------------------------------------

/// The two orders the results panel shows.
///
/// `By File` keeps every hit inside its own file, in file and line order, the
/// shape a code search reads best. `Timeline` puts every row that carries a log
/// timestamp on one chronological line across all the files, oldest first, and
/// marks the gap from the row before it, which is how a set of interleaved log
/// files reads as one story.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ResultOrder {
    #[default]
    ByFile,
    Timeline,
}

// Live build progress ---------------------------------------------------------

/// The pipeline stages a build runs through, in order. The worker writes the
/// running stage into [`LiveProgress`], so the UI shows a real percentage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum BuildStage {
    #[default]
    Queued,
    Cache,
    Walk,
    Ingest,
    Merge,
    Done,
}

impl BuildStage {
    fn word(self) -> &'static str {
        match self {
            BuildStage::Queued => "queued",
            BuildStage::Cache => "cache",
            BuildStage::Walk => "walk",
            BuildStage::Ingest => "ingest",
            BuildStage::Merge => "merge",
            BuildStage::Done => "done",
        }
    }

    /// The fraction the stage starts at.
    fn floor(self) -> f32 {
        match self {
            BuildStage::Queued => 0.02,
            BuildStage::Cache => 0.08,
            BuildStage::Walk => 0.22,
            BuildStage::Ingest => 0.30,
            BuildStage::Merge => 0.94,
            BuildStage::Done => 1.0,
        }
    }

    /// The fraction the stage can reach, so the bar never moves backwards.
    fn ceiling(self) -> f32 {
        match self {
            BuildStage::Queued => 0.04,
            BuildStage::Cache => 0.10,
            BuildStage::Walk => 0.24,
            BuildStage::Ingest => 0.92,
            BuildStage::Merge => 0.97,
            BuildStage::Done => 1.0,
        }
    }

    fn from_index(i: usize) -> Self {
        match i {
            1 => BuildStage::Cache,
            2 => BuildStage::Walk,
            3 => BuildStage::Ingest,
            4 => BuildStage::Merge,
            5 => BuildStage::Done,
            _ => BuildStage::Queued,
        }
    }
}

/// Counters the build worker publishes while it runs. Shared through an `Arc`
/// so the UI reads them with plain acquire loads: one snapshot per frame, no
/// channel traffic, and a lagging read is at most one stage behind.
#[derive(Default)]
struct LiveProgress {
    stage: AtomicUsize,
    dirs: AtomicUsize,
    seen: AtomicUsize,
    total: AtomicUsize,
    done: AtomicUsize,
    chunks: AtomicUsize,
    wire: AtomicU64,
    shards: AtomicUsize,
}

impl LiveProgress {
    fn set_stage(&self, stage: BuildStage) {
        self.stage.store(stage as usize, Ordering::Release);
    }

    /// Publish the walk verdict: directories visited, candidate files found, and
    /// the file count ingest has to finish.
    fn publish_walk(&self, dirs: usize, seen: usize, total: usize) {
        self.dirs.store(dirs, Ordering::Release);
        self.seen.store(seen, Ordering::Release);
        self.total.store(total, Ordering::Release);
    }

    fn publish_ingest(&self, done: usize, chunks: usize, wire: u64) {
        self.done.store(done, Ordering::Release);
        self.chunks.store(chunks, Ordering::Release);
        self.wire.store(wire, Ordering::Release);
    }

    fn set_shards(&self, shards: usize) {
        self.shards.store(shards, Ordering::Release);
    }

    fn snapshot(&self) -> Progress {
        Progress {
            stage: BuildStage::from_index(self.stage.load(Ordering::Acquire)),
            dirs: self.dirs.load(Ordering::Acquire),
            seen: self.seen.load(Ordering::Acquire),
            total: self.total.load(Ordering::Acquire),
            done: self.done.load(Ordering::Acquire),
            chunks: self.chunks.load(Ordering::Acquire),
            wire: self.wire.load(Ordering::Acquire),
            shards: self.shards.load(Ordering::Acquire),
        }
    }
}

/// One frame's worth of build counters, ready for painting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Progress {
    stage: BuildStage,
    dirs: usize,
    seen: usize,
    total: usize,
    done: usize,
    chunks: usize,
    wire: u64,
    shards: usize,
}

impl Progress {
    /// The bar fraction. Stage floors keep the bar honest between stages; the
    /// ingest stage maps the real `files ingested / files discovered` ratio onto
    /// the widest slice, so the percentage is a measurement, not an estimate.
    fn fraction(self) -> f32 {
        let lo = self.stage.floor();
        match self.stage {
            BuildStage::Ingest => {
                let hi = self.stage.ceiling();
                let ratio = if self.total > 0 {
                    (self.done as f32 / self.total as f32).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                lo + (hi - lo) * ratio
            }
            _ => self.stage.ceiling(),
        }
    }

    fn percent(self) -> f32 {
        (self.fraction() * 100.0).clamp(0.0, 100.0)
    }

    /// The one-line human-readable read-out: what the pipeline is doing right now
    /// and how far it has got, in real units.
    fn detail(self, lanes: usize) -> String {
        match self.stage {
            BuildStage::Queued => format!("queued · {lanes} lanes"),
            BuildStage::Cache => String::from("checking the disk cache"),
            BuildStage::Walk => format!(
                "walking tree · {} dirs · {} candidates",
                self.dirs, self.seen
            ),
            BuildStage::Ingest => format!(
                "ingesting {} of {} files · {} chunks · {:.2} MiB",
                self.done,
                self.total,
                self.chunks,
                mib(self.wire)
            ),
            BuildStage::Merge => format!("merging {} shards on {lanes} lanes", self.shards),
            BuildStage::Done => String::from("pipeline complete"),
        }
    }
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// The human-readable results summary, e.g. `Found 42 matches across 5 files
/// (0.8 ms)`. When the ext filter narrows the view, the summary names both sides
/// of the narrowing so the numbers never read as a bug: `Found 12 of 42 matches
/// across 2 of 5 files`.
fn result_summary(
    rows: usize,
    uniq_rows: usize,
    view_files: usize,
    matched_files: usize,
    hits: usize,
    query_us: f64,
) -> String {
    let counts = if rows == uniq_rows {
        format!("Found {rows} matches across {view_files} files")
    } else {
        format!("Found {rows} of {uniq_rows} matches across {view_files} of {matched_files} files")
    };
    let hits_part = if hits > rows {
        format!(" · {hits} hits")
    } else {
        String::new()
    };
    format!("{counts}{hits_part} ({})", human_duration_us(query_us))
}

/// A duration in the unit that reads best: sub-millisecond timings stay in µs,
/// the rest speak as ms.
fn human_duration_us(us: f64) -> String {
    if us >= 1000.0 {
        format!("{:.1} ms", us / 1000.0)
    } else {
        format!("{:.1} µs", us)
    }
}

/// The subtle "×" clear affordance beside a text box. It is enabled only when
/// the box holds text, so an empty box shows a quiet glyph and a filled box
/// shows a live control.
fn clear_button(ui: &mut egui::Ui, has_text: bool, p: &Palette, tip: &str) -> egui::Response {
    let glyph = egui::RichText::new("×")
        .size(15.0)
        .color(if has_text { p.warn } else { p.dim });
    let resp = ui.add_enabled(
        has_text,
        egui::Button::new(glyph)
            // Same height as the natural single-line boxes, so the "×" and its box
            // share one centre axis.
            .min_size(egui::vec2(26.0, 25.0))
            .rounding(egui::Rounding::same(3.0)),
    );
    resp.on_hover_text(tip)
}

// Per-request worker ----------------------------------------------------------

fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "worker panicked".to_string()
    }
}

fn spawn_request<F, T>(f: F) -> Receiver<io::Result<T>>
where
    F: FnOnce() -> io::Result<T> + UnwindSafe + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let worker_tx = tx.clone();
    let spawned = thread::Builder::new()
        .name(String::from("seeksage-index"))
        .spawn(move || {
            let reply = match std::panic::catch_unwind(AssertUnwindSafe(f)) {
                Ok(res) => res,
                Err(payload) => Err(io::Error::other(panic_text(payload))),
            };
            let _ = worker_tx.send(reply);
        });
    match spawned {
        Ok(_) => rx,
        Err(e) => {
            let _ = tx.send(Err(io::Error::other(format!(
                "worker thread could not start: {e}"
            ))));
            rx
        }
    }
}

/// Run the pipeline with live counters: crawl the tree, ingest over `lanes`
/// producer lanes while publishing the real "files ingested / files discovered"
/// ratio, then merge the shards in parallel. This is `seeksage::build_index` with
/// one addition: the shared counters the UI reads every frame.
fn staged_build(root: &Path, lanes: usize, live: &LiveProgress) -> io::Result<Built> {
    live.set_stage(BuildStage::Walk);
    let mut records: Vec<FileRecord> = Vec::new();
    let mut walk = WalkStats::default();
    let crawl_t = Instant::now();
    crawl(root, &mut records, &mut walk)?;
    let crawl_ms = crawl_t.elapsed().as_secs_f64() * 1e3;
    live.publish_walk(
        walk.dirs_visited as usize,
        walk.files_seen as usize,
        records.len(),
    );

    // `file_id` is the crawl-order index, so the path table is a direct lookup.
    let mut paths: Vec<PathBuf> = Vec::new();
    for r in &records {
        let id = r.file_id as usize;
        if id >= paths.len() {
            paths.resize(id + 1, PathBuf::new());
        }
        paths[id] = r.path.clone();
    }

    live.set_stage(BuildStage::Ingest);
    live.set_shards(lanes);
    let parts = partition(&records, lanes);
    let queue_lanes = Lanes::new(lanes, LANE_CAP);
    let mut token = queue_lanes.consumer().expect("single consumer token");
    let active = AtomicUsize::new(lanes);

    let (producers, consumer, shards, ingest_secs, lane_errors) = thread::scope(|s| {
        let handles: Vec<_> = (0..lanes)
            .map(|i| {
                // Copy the references into locals so the inner `move` closure
                // takes its own handles instead of moving out of the outer capture.
                let lanes_ref = &queue_lanes;
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
        let shard_count = lanes;
        let live_ref = live;
        let total_files = records.len();
        let ch = s.spawn(move || {
            let mut cons = ConsumerStats::default();
            // Every chunk of one file lands in the same shard, so the per-file
            // line/offset state stays consistent inside that shard.
            let mut shards: Vec<IndexShard> = (0..shard_count).map(|_| IndexShard::new()).collect();
            // Chunks are ordered per file, so a file's first chunk is the moment
            // it moves from "discovered" to "processed": the honest percent base.
            let mut seen_file = vec![false; total_files];
            let mut done = 0usize;
            let t = Instant::now();
            drain_until_done(consumer_ref, active_ref, |bytes| {
                cons.record(bytes);
                let fid = if bytes.len() >= 4 {
                    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
                } else {
                    0
                };
                shards[fid % shard_count].ingest_wire(bytes);
                if fid < total_files && !seen_file[fid] {
                    seen_file[fid] = true;
                    done += 1;
                }
                if cons.chunks % PROGRESS_STEP == 0 {
                    live_ref.publish_ingest(done, cons.chunks as usize, cons.wire_bytes);
                }
            });
            live_ref.publish_ingest(done, cons.chunks as usize, cons.wire_bytes);
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

    live.set_stage(BuildStage::Merge);
    let merge_t = Instant::now();
    let index = merge_shards(shards, lanes);
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

/// The cache-aware build: a valid disk cache is the only stage that needs no
/// pipeline, so it reports a full progress run in one hop.
fn spawn_indexer(
    root: PathBuf,
    lanes: usize,
    live: Arc<LiveProgress>,
) -> Receiver<io::Result<(Built, bool)>> {
    spawn_request(move || {
        live.set_stage(BuildStage::Cache);
        if let Some(built) = load_cache(&root) {
            let files = built.walk.files_matched as usize;
            live.publish_walk(built.walk.dirs_visited as usize, files, files);
            live.set_stage(BuildStage::Done);
            return Ok((built, true));
        }
        let built = staged_build(&root, lanes, &live)?;
        let _ = save_cache(&built, &root);
        live.set_stage(BuildStage::Done);
        Ok((built, false))
    })
}

fn spawn_rebuild(
    root: PathBuf,
    lanes: usize,
    live: Arc<LiveProgress>,
) -> Receiver<io::Result<(Built, bool)>> {
    spawn_request(move || {
        let built = staged_build(&root, lanes, &live)?;
        let _ = save_cache(&built, &root);
        live.set_stage(BuildStage::Done);
        Ok((built, false))
    })
}

// Export worker ---------------------------------------------------------------

/// One message from the export worker: a progress tick, the finished summary,
/// or the failure text. Every variant is owned so it crosses the thread
/// boundary with no reference into the index.
enum ExportEvent {
    Progress { rows: usize },
    Done(ExportSummary),
    Failed(String),
}

/// Everything one export needs: all fields owned, so the job moves to the
/// worker thread and the UI keeps no borrow on the index.
struct ExportJob {
    target: PathBuf,
    format: ExportFormat,
    meta: ExportMeta,
    hits: Vec<ExportHit>,
    chars: usize,
}

/// Write one export file off the UI thread. The worker streams straight to a
/// `BufWriter` on disk, so a large result set never builds a string in memory,
/// and forwards every `PROGRESS_STEP` tick over the channel for the panel to
/// show. A panic inside the writer is reported as `Failed`, never as a crash.
fn spawn_export(job: ExportJob) -> Receiver<ExportEvent> {
    let (tx, rx) = mpsc::channel();
    let worker_tx = tx.clone();
    let spawned = thread::Builder::new()
        .name(String::from("seeksage-export"))
        .spawn(move || {
            let ExportJob {
                target,
                format,
                meta,
                hits,
                chars,
            } = job;
            let reply = match std::panic::catch_unwind(AssertUnwindSafe(|| {
                write_file_export(&target, format, &meta, &hits, chars, |rows| {
                    let _ = worker_tx.send(ExportEvent::Progress { rows });
                })
            })) {
                Ok(Ok(summary)) => ExportEvent::Done(summary),
                Ok(Err(e)) => ExportEvent::Failed(e.to_string()),
                Err(payload) => ExportEvent::Failed(panic_text(payload)),
            };
            let _ = worker_tx.send(reply);
        });
    match spawned {
        Ok(_) => rx,
        Err(e) => {
            let _ = tx.send(ExportEvent::Failed(format!(
                "export worker thread could not start: {e}"
            )));
            rx
        }
    }
}

// Bounded LRU of rendered snippets --------------------------------------------

#[derive(Default)]
struct SnippetCache {
    map: HashMap<(u32, u32), String>,
    order: VecDeque<(u32, u32)>,
}

impl SnippetCache {
    fn get(&mut self, path: &Path, file_id: u32, line: u32, byte_offset: u64) -> String {
        let key = (file_id, line);
        if let Some(found) = self.map.get(&key).cloned() {
            self.touch(key);
            return found;
        }
        let text = match extract_snippet(path, byte_offset, SNIPPET_CHARS) {
            Ok(text) => text,
            Err(e) => format!("(preview failed: {e})"),
        };
        self.touch(key);
        self.map.insert(key, text.clone());
        while self.order.len() > SNIPPET_CAP {
            match self.order.pop_front() {
                Some(evicted) => self.map.remove(&evicted),
                None => break,
            };
        }
        text
    }

    fn touch(&mut self, key: (u32, u32)) {
        if let Some(pos) = self.order.iter().position(|k| *k == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key);
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }
}

// The application state -------------------------------------------------------

/// Scope badges shown next to result rows.
///
/// Each row is keyed by (file_id, line) and caches the badge for the enclosing
/// scope of that row: `fn search()`, `[Media Players]`, `struct Config`.
/// Computing the badge reads a window of lines above the hit, so the cache is
/// bounded the same way the snippet cache is. A miss is stored as `None` so a
/// row without a scope never rescans.
#[derive(Default)]
struct ScopeCache {
    map: HashMap<(u32, u32), Option<String>>,
    order: VecDeque<(u32, u32)>,
}

impl ScopeCache {
    /// Get the badge for a result row, computing it from a window of lines
    /// above the hit if it is not cached.
    fn get(&mut self, path: &Path, file_id: u32, line: u32, byte_offset: u64) -> Option<String> {
        let key = (file_id, line);
        if let Some(found) = self.map.get(&key) {
            let found = found.clone();
            self.touch(key);
            return found;
        }
        let ext = path
            .extension()
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        let badge = match extract_context(path, byte_offset, SCOPE_RADIUS) {
            Ok(ctx) => detect_scope(&ctx, &ext),
            Err(_) => None,
        };
        self.touch(key);
        self.map.insert(key, badge.clone());
        while self.order.len() > SNIPPET_CAP {
            match self.order.pop_front() {
                Some(evicted) => self.map.remove(&evicted),
                None => break,
            };
        }
        badge
    }

    fn touch(&mut self, key: (u32, u32)) {
        if let Some(pos) = self.order.iter().position(|k| *k == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key);
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }
}

struct SeekSageApp {
    root_text: String,
    lanes: usize,
    query: String,
    ext_filter: String,
    phase: Phase,
    status: String,
    build_rx: Option<Receiver<io::Result<(Built, bool)>>>,
    build_start: Option<Instant>,
    build_ms: f64,
    /// The counters the running worker publishes; the UI reads them every frame.
    live: Arc<LiveProgress>,
    /// The last snapshot, so painting never touches the atomics directly.
    progress: Progress,
    built: Option<Built>,
    uniq: Vec<(u32, u32, u64)>,
    view: Vec<usize>,
    /// Which order `view` is in: rows grouped per file, or one shared timeline.
    result_order: ResultOrder,
    /// The gap from the previous timestamped row, one entry per visible row in
    /// `view`, so `timeline_deltas[i]` belongs to `view[i]`. Filled only while
    /// `ResultOrder::Timeline` is active; `None` for a row whose line carries
    /// no parseable stamp, which gets no badge.
    timeline_deltas: Vec<Option<String>>,
    /// `Watch` mode: the live file watcher that rebuilds on its own once the
    /// tree stops moving. Off by default; a chip in the toolbar turns it on.
    watch_enabled: bool,
    /// The fingerprint of the tree the current index was built from. The watcher
    /// compares the live tree against this baseline.
    last_fingerprint: Option<u64>,
    /// When the tree was last inspected, so the check runs on one second.
    last_watch_check: Option<Instant>,
    /// When the pending change first showed up. The quiet window is measured from
    /// here, and restarts every time the tree moves again.
    pending_change_since: Option<Instant>,
    /// The fingerprint seen while a change is pending, so the quiet window only
    /// counts while the tree keeps the same shape.
    pending_fingerprint: Option<u64>,
    /// In tests, the interaction zone of every result card painted in the last
    /// frame, in `view` order. A headless test presses the pointer on the text
    /// of a card and checks which row takes the click.
    #[cfg(test)]
    row_rects: Vec<egui::Rect>,
    /// In tests, the interaction zones of the two order chips painted in the last
    /// frame: `Timeline` first, then `By File`.
    #[cfg(test)]
    chip_rects: Vec<egui::Rect>,
    /// In tests, where the `ext:` label and the extension filter box sit in the
    /// last painted frame, so a headless test can check the label is in front of
    /// the box and that the box paints no placeholder text inside itself.
    #[cfg(test)]
    ext_label_rect: Option<egui::Rect>,
    #[cfg(test)]
    ext_box_rect: Option<egui::Rect>,
    /// In tests, where the query "×" button sits in the last painted search bar,
    /// so a headless test can measure the gap between it and the `ext:` label.
    #[cfg(test)]
    query_clear_rect: Option<egui::Rect>,
    /// In tests, the query box of the last painted search bar, so a headless test
    /// can check it shares one centre axis with the `ext:` field.
    #[cfg(test)]
    search_box_rect: Option<egui::Rect>,
    /// In tests, the logo, the path box, the `Index` button and the theme square
    /// of the last painted toolbar, so a headless test can check they sit on one
    /// centre axis and the square is anchored to the far-right corner.
    #[cfg(test)]
    logo_rect: Option<egui::Rect>,
    #[cfg(test)]
    path_box_rect: Option<egui::Rect>,
    #[cfg(test)]
    index_rect: Option<egui::Rect>,
    #[cfg(test)]
    theme_rect: Option<egui::Rect>,
    /// `Copy AI Context` and the live progress bar, recorded in the same headless
    /// pass, so the gap the row keeps between the last button and the right-anchored
    /// tail is checked on real painted rects.
    #[cfg(test)]
    pack_rect: Option<egui::Rect>,
    #[cfg(test)]
    progress_bar_rect: Option<egui::Rect>,
    /// The ext filter "×" and the trailing latency read-out: painted rects for the
    /// gap the search bar keeps between them.
    #[cfg(test)]
    ext_clear_rect: Option<egui::Rect>,
    #[cfg(test)]
    latency_rect: Option<egui::Rect>,
    total_hits: usize,
    matched_files: usize,
    view_files: usize,
    query_us: f64,
    selected: usize,
    pending_scroll: Option<usize>,
    tokens: Vec<String>,
    /// The last committed queries, oldest first, capped to `HISTORY_CAP`.
    query_history: VecDeque<String>,
    /// Which history entry the Alt+Up / Alt+Down cycle is on; `None` means the
    /// user is on the live, uncommitted text they are typing.
    history_pos: Option<usize>,
    /// The text the user was typing, saved the moment history browsing begins,
    /// so Alt+Down at the newest entry restores it.
    pending_query: String,
    snippets: SnippetCache,
    scopes: ScopeCache,
    context: Vec<(i32, String)>,
    context_key: Option<(u32, u32)>,
    nav_path: PathBuf,
    subdirs: Vec<PathBuf>,
    subdirs_src: PathBuf,
    nav_error: Option<String>,
    recents: VecDeque<PathBuf>,
    drives: Vec<PathBuf>,
    startup: Option<PathBuf>,
    export_open: bool,
    export_format: ExportFormat,
    export_path: String,
    export_target: PathBuf,
    export_rx: Option<Receiver<ExportEvent>>,
    export_start: Option<Instant>,
    export_rows: usize,
    export_ms: f64,
    export_summary: Option<ExportSummary>,
    pub dark_mode: bool,
    theme_for: Option<bool>,
}

impl Default for SeekSageApp {
    fn default() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let startup = startup_root(&args);
        let here = startup
            .clone()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("C:\\")));
        Self {
            root_text: here.display().to_string(),
            lanes: default_lanes(),
            query: String::new(),
            // The extension box starts empty on purpose: a fresh app searches
            // every parsed text file in the tree, and typing `.log` is a
            // deliberate narrowing the user asks for, never a startup default.
            ext_filter: String::new(),
            phase: Phase::Idle,
            status: String::from("Ready — select a folder and press Index"),
            build_rx: None,
            build_start: None,
            build_ms: 0.0,
            live: Arc::new(LiveProgress::default()),
            progress: Progress::default(),
            built: None,
            uniq: Vec::new(),
            view: Vec::new(),
            result_order: ResultOrder::ByFile,
            timeline_deltas: Vec::new(),
            watch_enabled: false,
            last_fingerprint: None,
            last_watch_check: None,
            pending_change_since: None,
            pending_fingerprint: None,
            #[cfg(test)]
            row_rects: Vec::new(),
            #[cfg(test)]
            chip_rects: Vec::new(),
            #[cfg(test)]
            ext_label_rect: None,
            #[cfg(test)]
            ext_box_rect: None,
            #[cfg(test)]
            query_clear_rect: None,
            #[cfg(test)]
            search_box_rect: None,
            #[cfg(test)]
            logo_rect: None,
            #[cfg(test)]
            path_box_rect: None,
            #[cfg(test)]
            index_rect: None,
            #[cfg(test)]
            theme_rect: None,
            #[cfg(test)]
            pack_rect: None,
            #[cfg(test)]
            progress_bar_rect: None,
            #[cfg(test)]
            ext_clear_rect: None,
            #[cfg(test)]
            latency_rect: None,
            total_hits: 0,
            matched_files: 0,
            view_files: 0,
            query_us: 0.0,
            selected: NO_SELECTION,
            pending_scroll: None,
            tokens: Vec::new(),
            query_history: VecDeque::new(),
            history_pos: None,
            pending_query: String::new(),
            snippets: SnippetCache::default(),
            scopes: ScopeCache::default(),
            context: Vec::new(),
            context_key: None,
            nav_path: here,
            subdirs: Vec::new(),
            subdirs_src: PathBuf::new(),
            nav_error: None,
            recents: VecDeque::new(),
            drives: drive_roots(),
            startup,
            export_open: false,
            export_format: ExportFormat::Json,
            export_path: String::new(),
            export_target: PathBuf::new(),
            export_rx: None,
            export_start: None,
            export_rows: 0,
            export_ms: 0.0,
            export_summary: None,
            dark_mode: true,
            theme_for: None,
        }
    }
}

impl SeekSageApp {
    fn palette(&self) -> Palette {
        if self.dark_mode {
            Palette::dark()
        } else {
            Palette::light()
        }
    }

    fn apply_theme(ctx: &egui::Context, p: &Palette, dark: bool) {
        let mut style = (*ctx.style()).clone();
        style
            .text_styles
            .insert(egui::TextStyle::Heading, FontId::proportional(15.5));
        style
            .text_styles
            .insert(egui::TextStyle::Body, FontId::proportional(13.0));
        style
            .text_styles
            .insert(egui::TextStyle::Monospace, FontId::monospace(12.5));
        style
            .text_styles
            .insert(egui::TextStyle::Button, FontId::proportional(12.5));
        style
            .text_styles
            .insert(egui::TextStyle::Small, FontId::proportional(11.5));

        style.spacing.item_spacing = egui::vec2(8.0, 5.0);
        style.spacing.button_padding = egui::vec2(10.0, 5.0);
        style.spacing.slider_width = 96.0;
        style.spacing.indent = 12.0;
        style.spacing.scroll.bar_width = 8.0;
        style.spacing.scroll.handle_min_length = 24.0;
        // The bar takes real layout space instead of floating over the content,
        // so a scrollbar can never cover folder names, chips, or row text.
        style.spacing.scroll.floating = false;

        let round = egui::Rounding::same(6.0);
        style.visuals.window_rounding = round;
        style.visuals.widgets.noninteractive.rounding = round;
        style.visuals.widgets.inactive.rounding = round;
        style.visuals.widgets.hovered.rounding = round;
        style.visuals.widgets.active.rounding = round;
        style.visuals.widgets.open.rounding = round;

        style.visuals.dark_mode = dark;
        style.visuals.panel_fill = p.panel;
        style.visuals.extreme_bg_color = p.canvas;
        style.visuals.faint_bg_color = p.card;
        style.visuals.window_fill = p.panel;
        style.visuals.window_stroke = egui::Stroke::new(1.0_f32, p.border);
        style.visuals.override_text_color = Some(p.text);

        // Non-interactive separators and panel borders.
        style.visuals.widgets.noninteractive.bg_fill = p.panel;
        style.visuals.widgets.noninteractive.weak_bg_fill = p.panel;
        style.visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0_f32, p.border);
        style.visuals.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0_f32, p.text);

        // Inactive buttons & inputs: clean solid card fill, single hairline border.
        style.visuals.widgets.inactive.bg_fill = p.card;
        style.visuals.widgets.inactive.weak_bg_fill = p.card;
        style.visuals.widgets.inactive.bg_stroke = egui::Stroke::new(1.0_f32, p.border);
        style.visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0_f32, p.text);

        // Hovered widgets: lifted tone + turquoise accent border.
        style.visuals.widgets.hovered.bg_fill = p.raised;
        style.visuals.widgets.hovered.weak_bg_fill = p.raised;
        style.visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0_f32, p.accent);
        style.visuals.widgets.hovered.fg_stroke = egui::Stroke::new(1.0_f32, p.text);

        // Active / focused inputs & pressed buttons: clear turquoise focus ring.
        style.visuals.widgets.active.bg_fill = p.selected_fill;
        style.visuals.widgets.active.weak_bg_fill = p.selected_fill;
        style.visuals.widgets.active.bg_stroke = egui::Stroke::new(1.5_f32, p.accent);
        style.visuals.widgets.active.fg_stroke = egui::Stroke::new(1.0_f32, p.accent);

        style.visuals.widgets.open.bg_fill = p.raised;
        style.visuals.widgets.open.weak_bg_fill = p.raised;
        style.visuals.widgets.open.bg_stroke = egui::Stroke::new(1.0_f32, p.accent);
        style.visuals.widgets.open.fg_stroke = egui::Stroke::new(1.0_f32, p.text);

        style.visuals.selection.bg_fill = p.selected_fill;
        style.visuals.selection.stroke = egui::Stroke::new(1.0_f32, p.accent);
        style.visuals.striped = false;

        ctx.set_style(style);
    }

    // Path helpers ------------------------------------------------------------

    fn path_of(&self, file_id: u32) -> PathBuf {
        self.built
            .as_ref()
            .and_then(|b| b.paths.get(file_id as usize).cloned())
            .unwrap_or_default()
    }

    fn file_ext(&self, file_id: u32) -> String {
        self.built
            .as_ref()
            .and_then(|b| b.paths.get(file_id as usize))
            .and_then(|p| p.extension())
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default()
    }

    /// Pack every filtered match into one LLM-ready markdown block.
    ///
    /// Matches are grouped by file, and the context windows around them are
    /// merged whenever they overlap or touch, so a run of hits in one file
    /// becomes one contiguous block of lines with no line shown twice. The
    /// token estimate is the body size divided by four.
    fn build_ai_context(&self) -> (String, usize, usize) {
        if self.view.is_empty() {
            return (String::new(), 0, 0);
        }
        // Group the filtered rows by file, in the order the results show them.
        let mut file_order: Vec<u32> = Vec::new();
        let mut by_file: HashMap<u32, Vec<(u32, u64)>> = HashMap::new();
        for &i in &self.view {
            let (file_id, line, byte_offset) = self.uniq[i];
            let entry = by_file.entry(file_id).or_default();
            if entry.is_empty() {
                file_order.push(file_id);
            }
            entry.push((line, byte_offset));
        }
        let matches = self.view.len();
        let files = file_order.len();
        let mut body = String::new();
        for file_id in file_order {
            let hits = by_file.get(&file_id).expect("file present");
            let path = self.path_of(file_id);
            let mut sorted = hits.clone();
            sorted.sort_by_key(|&(line, _)| line);
            for (start, end) in merge_hit_ranges(&sorted, CONTEXT_RADIUS) {
                let center = ((start + end) / 2).max(1) as u32;
                let idx = sorted
                    .partition_point(|&(line, _)| line <= center)
                    .saturating_sub(1);
                let (center_line, center_byte) = sorted[idx];
                let radius = (end - center_line as i64)
                    .max(center_line as i64 - start)
                    .max(0) as usize;
                if let Ok(ctx) = extract_context(&path, center_byte, radius) {
                    body.push_str(&format!(
                        "### File: `{}` (lines {start}-{end})\n",
                        path.display()
                    ));
                    body.push_str("```\n");
                    for (rel, text) in &ctx {
                        let abs = center_line as i64 + *rel as i64;
                        if abs >= start && abs <= end {
                            body.push_str(&format!("{abs}: {text}\n"));
                        }
                    }
                    body.push_str("```\n\n");
                }
            }
        }
        let tokens = body.chars().count() / 4;
        (
            format!(
                "<!-- SeekSage Context: {files} files, {matches} matches, ~{} tokens -->\n\n{body}",
                format_tokens(tokens)
            ),
            tokens,
            files,
        )
    }

    /// Copy the packed context to the clipboard and say so in the status bar.
    fn copy_ai_context(&mut self, ctx: &egui::Context) {
        let (block, tokens, files) = self.build_ai_context();
        let matches = self.view.len();
        ctx.copy_text(block);
        self.status = format!(
            "Copied AI Context: {} matches across {} files (~{} tokens)",
            matches,
            files,
            format_tokens(tokens)
        );
    }

    // Indexing ----------------------------------------------------------------

    fn start_build(&mut self) {
        self.begin_build(false);
    }

    fn start_rebuild(&mut self) {
        self.begin_build(true);
    }

    fn begin_build(&mut self, force: bool) {
        if self.phase == Phase::Indexing {
            return;
        }
        let raw = self.root_text.trim().to_string();
        if raw.is_empty() {
            self.status = String::from("no directory path given");
            return;
        }
        let root = PathBuf::from(&raw);
        if !root.is_dir() {
            self.phase = Phase::Failed;
            self.status = format!("'{raw}' is not a directory");
            return;
        }
        self.push_recent(&root);
        self.nav_path = root.clone();
        let lanes = self.lanes;
        // One shared counter block per build: the worker writes it, the UI reads
        // it, and a fresh build always starts from a clean zero.
        let live = Arc::new(LiveProgress::default());
        self.live = live.clone();
        self.progress = Progress::default();
        self.build_rx = Some(if force {
            spawn_rebuild(root, lanes, live)
        } else {
            spawn_indexer(root, lanes, live)
        });
        self.build_start = Some(Instant::now());
        self.build_ms = 0.0;
        self.phase = Phase::Indexing;
        self.status = if force {
            format!("rebuilding '{raw}' on {lanes} lanes · cache bypassed")
        } else {
            format!("indexing '{raw}' on {lanes} lanes")
        };
        self.reset_results();
    }

    fn push_recent(&mut self, path: &Path) {
        if let Some(pos) = self.recents.iter().position(|p| p == path) {
            self.recents.remove(pos);
        }
        self.recents.push_front(path.to_path_buf());
        while self.recents.len() > RECENT_CAP {
            self.recents.pop_back();
        }
    }

    fn poll_build(&mut self, ctx: &egui::Context) {
        if self.phase != Phase::Indexing {
            return;
        }
        ctx.request_repaint_after(POLL_DELAY);
        if let Some(start) = self.build_start {
            self.build_ms = start.elapsed().as_secs_f64() * 1e3;
        }
        // The worker publishes its counters into the shared `LiveProgress`; one
        // acquire snapshot per frame keeps the bar, the percent and the status
        // line live without flooding the UI with channel traffic.
        self.progress = self.live.snapshot();
        let outcome = self.build_rx.as_mut().map(|rx| rx.try_recv());
        match outcome {
            Some(Ok(res)) => {
                self.build_rx = None;
                match res {
                    Ok((built, from_cache)) => {
                        let files = built.walk.files_matched;
                        let faults = built.lane_errors.len();
                        let ms = built.total_ms();
                        // The pipeline is finished: publish a complete snapshot
                        // from the build report so the bar and the counters agree.
                        self.progress = Progress {
                            stage: BuildStage::Done,
                            dirs: built.walk.dirs_visited as usize,
                            seen: files as usize,
                            total: files as usize,
                            done: files as usize,
                            chunks: built.producers.chunks as usize,
                            wire: built.producers.wire_bytes,
                            shards: self.lanes,
                        };
                        self.build_ms = ms;
                        // The tree this index was built from is the baseline the
                        // watcher compares against, so a rebuild that changes the
                        // tree never re-triggers itself.
                        let watch_base = tree_fingerprint(&built.paths);
                        self.built = Some(built);
                        self.last_fingerprint = Some(watch_base);
                        self.pending_fingerprint = None;
                        self.pending_change_since = None;
                        self.phase = Phase::Ready;
                        self.status = if from_cache {
                            format!("{files} files loaded from disk cache · {ms:.1} ms prior build · {faults} lane faults")
                        } else {
                            format!("{files} files indexed · {ms:.1} ms · {faults} lane faults")
                        };
                        self.run_search();
                    }
                    Err(e) => {
                        self.phase = Phase::Failed;
                        self.status = format!("index failed: {e}");
                    }
                }
            }
            Some(Err(TryRecvError::Empty)) => {}
            Some(Err(TryRecvError::Disconnected)) => {
                self.build_rx = None;
                self.phase = Phase::Failed;
                self.status = String::from("worker thread vanished before reporting");
            }
            None => {}
        }
    }

    // Live file watcher --------------------------------------------------------

    /// Turn `Watch` mode on or off. The baseline is the tree as the current index
    /// sees it, so a fresh `Watch` starts comparing from the tree it is standing
    /// on, not from whatever the user typed next.
    fn set_watch(&mut self, on: bool) {
        self.watch_enabled = on;
        self.last_watch_check = None;
        self.pending_change_since = None;
        self.pending_fingerprint = None;
        self.last_fingerprint = self
            .built
            .as_ref()
            .map(|built| tree_fingerprint(&built.paths));
        self.status = if on {
            String::from("watch on · rebuilding after a quiet window")
        } else {
            String::from("watch off")
        };
    }

    /// One debounce step of `Watch` mode. The tree is looked at at most once a
    /// second; the first change is recorded and announced, and a rebuild fires
    /// only once the tree has held the same shape across the quiet window, so a
    /// writer in the middle of a save is never caught mid-write. The query, the
    /// extension filter and the result order are plain app state, so
    /// `run_search` at the end of the rebuild restores the live view.
    fn poll_watch(&mut self, ctx: &egui::Context) {
        if !(self.watch_enabled && self.phase == Phase::Ready) {
            return;
        }
        // The watcher runs on frames, so it asks eframe for one on a half-second
        // cadence; the tree itself is only read once a second.
        ctx.request_repaint_after(WATCH_REPAINT_DELAY);
        let fingerprint = match &self.built {
            Some(built) => tree_fingerprint(&built.paths),
            None => return,
        };
        let now = Instant::now();
        if let Some(ago) = self.last_watch_check {
            if now.saturating_duration_since(ago) < WATCH_CHECK_INTERVAL {
                return;
            }
        }
        self.last_watch_check = Some(now);
        let baseline = match self.last_fingerprint {
            Some(seen) => seen,
            // A fresh index has nothing to compare against yet, so the first look
            // sets the baseline and reports no change.
            None => {
                self.last_fingerprint = Some(fingerprint);
                return;
            }
        };
        if fingerprint == baseline {
            // The tree matches the index, so there is nothing to wait for.
            self.pending_change_since = None;
            self.pending_fingerprint = None;
            return;
        }
        if self.pending_fingerprint == Some(fingerprint) {
            // The same changed shape twice running: the quiet window counts from
            // when this change first appeared.
            if let Some(since) = self.pending_change_since {
                if now.saturating_duration_since(since) >= WATCH_QUIET_WINDOW {
                    self.pending_change_since = None;
                    self.pending_fingerprint = None;
                    self.start_rebuild();
                }
            }
            return;
        }
        // A new shape: the window restarts, because a tree that is still moving
        // is a tree that is being written to.
        self.pending_fingerprint = Some(fingerprint);
        self.pending_change_since = Some(now);
        self.status = String::from("file change detected · waiting for quiet window…");
    }

    // Export ------------------------------------------------------------------

    /// The query context written into the export header, owned so it can move to
    /// the worker thread with the rows.
    fn export_meta(&self) -> ExportMeta {
        ExportMeta {
            root: self.nav_path.display().to_string(),
            query: self.query.clone(),
            ext_filter: self.ext_filter.clone(),
            total_hits: self.total_hits,
        }
    }

    /// Every row the user can currently see, in the order the results panel
    /// shows them: the extension filter and the query together decide the rows,
    /// so an export is exactly the visible list.
    fn export_hits(&self) -> Vec<ExportHit> {
        self.view
            .iter()
            .map(|i| {
                let (file_id, line, byte_offset) = self.uniq[*i];
                ExportHit {
                    path: self.path_of(file_id),
                    line,
                    byte_offset,
                }
            })
            .collect()
    }

    fn export_dir(&self) -> PathBuf {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("C:\\"))
    }

    /// Open the export panel, filling in a default file name the first time.
    fn open_export_panel(&mut self) {
        if self.view.is_empty() {
            self.status = String::from("nothing to export — the result view is empty");
            return;
        }
        self.export_open = true;
        if self.export_path.is_empty() {
            self.export_path = self.export_format.suggested_name(&self.export_dir());
        }
        self.status = format!(
            "export panel · {} rows as {} → '{}'",
            self.view.len(),
            self.export_format.label(),
            self.export_path
        );
    }

    /// Pick a format and keep the file name in step with it, so a `.csv` name
    /// becomes `.json` the moment JSON is chosen and the saved file always
    /// carries the extension its format writes.
    fn set_export_format(&mut self, format: ExportFormat) {
        self.export_format = format;
        if self.export_path.is_empty() {
            self.export_path = format.suggested_name(&self.export_dir());
            return;
        }
        let mut target = PathBuf::from(&self.export_path);
        target.set_extension(format.extension());
        self.export_path = target.display().to_string();
    }

    /// The native OS save dialog (COM common dialog on Windows, xdg-portal on
    /// Linux): filters per format, default name from the current format. The
    /// dialog is modal, so it blocks this one call; the write itself is the part
    /// that must stay off the UI thread, and `start_export` does that.
    fn export_via_dialog(&mut self) {
        let format = self.export_format;
        let picked = rfd::FileDialog::new()
            .add_filter("JSON", &["json"])
            .add_filter("CSV", &["csv"])
            .add_filter("Markdown", &["md", "markdown"])
            .set_title("Export SeekSage results")
            .set_file_name(format!("seeksage-results.{}", format.extension()))
            .set_directory(self.export_dir())
            .save_file();
        match picked {
            Some(path) => {
                let format = ExportFormat::for_path(&path, format);
                self.export_open = false;
                self.start_export(path, format);
            }
            None => {
                self.export_open = true;
                self.status = String::from("export dialog cancelled · type a path in the panel");
            }
        }
    }

    /// Send one export to the worker thread. Guards keep the UI honest: only one
    /// export writes at a time, an empty view has nothing to write, and a path
    /// with no file name cannot be created.
    fn start_export(&mut self, target: PathBuf, format: ExportFormat) {
        if self.export_rx.is_some() {
            self.status = String::from("an export is already writing");
            return;
        }
        if self.view.is_empty() {
            self.status = String::from("nothing to export — the result view is empty");
            return;
        }
        let target = format.completed(&target);
        if target.as_os_str().is_empty() || target.file_name().is_none() {
            self.status = String::from("no file name to write");
            return;
        }
        let hits = self.export_hits();
        let rows = hits.len();
        let job = ExportJob {
            target: target.clone(),
            format,
            meta: self.export_meta(),
            hits,
            chars: SNIPPET_CHARS,
        };
        self.export_target = target;
        self.export_format = format;
        self.export_path = self.export_target.display().to_string();
        self.export_rows = 0;
        self.export_ms = 0.0;
        self.export_summary = None;
        self.export_rx = Some(spawn_export(job));
        self.export_start = Some(Instant::now());
        self.status = format!("exporting {rows} rows to '{}'", self.export_path);
    }

    /// Drain every message the export worker has sent this frame, so the panel
    /// and the status bar show live progress, and the finished summary sticks.
    fn poll_export(&mut self, ctx: &egui::Context) {
        if self.export_rx.is_none() {
            return;
        }
        ctx.request_repaint_after(POLL_DELAY);
        if let Some(start) = self.export_start {
            self.export_ms = start.elapsed().as_secs_f64() * 1e3;
        }
        let mut events: Vec<ExportEvent> = Vec::new();
        let mut vanished = false;
        if let Some(rx) = self.export_rx.as_mut() {
            loop {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        vanished = true;
                        break;
                    }
                }
            }
        }
        let mut finished = false;
        for event in events {
            match event {
                ExportEvent::Progress { rows } => self.export_rows = rows,
                ExportEvent::Done(summary) => {
                    self.export_rows = summary.rows;
                    self.export_summary = Some(summary);
                    self.status = format!(
                        "export wrote {} rows · {} bytes · {:.0} ms",
                        summary.rows, summary.bytes, self.export_ms
                    );
                    if summary.missing > 0 {
                        self.status =
                            format!("{} · {} previews unread", self.status, summary.missing);
                    }
                    finished = true;
                }
                ExportEvent::Failed(msg) => {
                    self.status = format!("export failed: {msg}");
                    finished = true;
                }
            }
        }
        if finished {
            self.export_rx = None;
        } else if vanished {
            self.export_rx = None;
            self.status = String::from("export worker vanished before reporting");
        }
    }

    // Global hotkeys ----------------------------------------------------------

    /// Window-wide shortcuts, read once per frame before any panel paints.
    /// `Ctrl+F` focuses the search box from anywhere; `Ctrl+Shift+E` opens the
    /// export panel; `Esc` unwinds one layer at a time, closing the export panel,
    /// then clearing the focused field, then releasing focus. `key_pressed`
    /// ignores modifiers, so every combination is checked against the modifier
    /// set on this frame, and plain typing in a text field never fires a
    /// shortcut.
    fn handle_hotkeys(&mut self, ctx: &egui::Context) {
        let (focus_search, open_export, escape) = ctx.input(|i| {
            let m = i.modifiers;
            (
                m.command && !m.shift && i.key_pressed(egui::Key::F),
                m.command && m.shift && i.key_pressed(egui::Key::E),
                i.key_pressed(egui::Key::Escape),
            )
        });

        if focus_search {
            ctx.memory_mut(|mem| mem.request_focus(egui::Id::from("search_box")));
        }

        if open_export {
            if self.export_open {
                self.export_open = false;
                self.status = String::from("export panel closed");
            } else {
                self.open_export_panel();
            }
        }

        if !escape {
            return;
        }
        if self.export_open {
            self.export_open = false;
            self.status = String::from("export panel closed");
            return;
        }
        let search_id = egui::Id::from("search_box");
        let ext_id = egui::Id::from("ext_filter");
        let focused = ctx.memory(|mem| mem.focused());
        if focused == Some(search_id) {
            if self.query.is_empty() {
                ctx.memory_mut(|mem| mem.surrender_focus(search_id));
            } else {
                self.clear_query();
            }
            return;
        }
        if focused == Some(ext_id) {
            if self.ext_filter.is_empty() {
                ctx.memory_mut(|mem| mem.surrender_focus(ext_id));
            } else {
                self.clear_ext_filter();
            }
            return;
        }
        // Escape from anywhere else: clear both filters, the way the two "×"
        // buttons do, then release focus from every text field.
        if !self.query.is_empty() || !self.ext_filter.is_empty() {
            self.clear_query();
            self.clear_ext_filter();
        }
        for name in ["search_box", "ext_filter", "path_box", "export_path_box"] {
            ctx.memory_mut(|mem| mem.surrender_focus(egui::Id::from(name)));
        }
    }

    /// The export panel: format chips, a path field, the native dialog, live
    /// progress, and the summary of the last write.
    fn paint_export_panel(&mut self, ctx: &egui::Context) {
        if !self.export_open {
            return;
        }
        let p = self.palette();
        let rows = self.view.len();
        let hits = self.total_hits;
        let mut open = true;
        let mut close = false;
        egui::Window::new("Export results")
            .id(egui::Id::from("export_panel"))
            .open(&mut open)
            .default_pos(egui::pos2(150.0, 170.0))
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, -70.0))
            .constrain(true)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(
                    egui::RichText::new(format!("{} rows in view · {} total hits", rows, hits))
                        .small()
                        .color(p.dim),
                );
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Format").strong().color(p.text));
                    for f in ExportFormat::ALL {
                        let chosen = self.export_format == f;
                        let tip = format!("write as {} · .{}", f.label(), f.extension());
                        let chip = ui.selectable_label(
                            chosen,
                            egui::RichText::new(f.label()).strong().color(p.text),
                        );
                        chip.clone().on_hover_text(tip);
                        if chip.clicked() {
                            self.set_export_format(f);
                        }
                    }
                });
                ui.add_space(4.0);

                let path_box = ui.add_sized(
                    [390.0, 28.0],
                    egui::TextEdit::singleline(&mut self.export_path)
                        .font(FontId::monospace(12.5))
                        .hint_text("F:\\reports\\seeksage-results.json")
                        .id(egui::Id::from("export_path_box"))
                        .lock_focus(true),
                );
                let enter = path_box.has_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));

                ui.horizontal(|ui| {
                    let writing = self.export_rx.is_some();
                    let dialog = ui.add_enabled(
                        !writing,
                        egui::Button::new(
                            egui::RichText::new("Native dialog…").strong().color(p.text),
                        )
                        .min_size(egui::vec2(126.0, 28.0)),
                    );
                    dialog
                        .clone()
                        .on_hover_text("Pick the file with the OS save dialog");
                    if !writing && dialog.clicked() {
                        self.export_via_dialog();
                    }

                    let save = ui.add_enabled(
                        !writing,
                        egui::Button::new(egui::RichText::new("Save").strong().color(p.emerald))
                            .min_size(egui::vec2(58.0, 28.0)),
                    );
                    if !writing && (enter || save.clicked()) {
                        let target = PathBuf::from(self.export_path.trim());
                        let format = self.export_format;
                        self.start_export(target, format);
                    }

                    if ui
                        .button(egui::RichText::new("Cancel").color(p.dim))
                        .clicked()
                    {
                        close = true;
                    }
                });

                if self.export_rx.is_some() {
                    ui.add_space(3.0);
                    ui.horizontal(|ui| {
                        ui.add(egui::Spinner::new().size(14.0).color(p.gold));
                        ui.label(
                            egui::RichText::new(format!(
                                "writing {} of {} rows · {:.0} ms",
                                self.export_rows, rows, self.export_ms
                            ))
                            .monospace()
                            .color(p.gold)
                            .size(11.5),
                        );
                    });
                }

                if let Some(summary) = self.export_summary {
                    ui.add_space(3.0);
                    ui.label(
                        egui::RichText::new(format!(
                            "wrote {} rows · {} bytes · {:.0} ms",
                            summary.rows, summary.bytes, self.export_ms
                        ))
                        .strong()
                        .monospace()
                        .color(p.emerald)
                        .size(11.5),
                    );
                    if summary.missing > 0 {
                        ui.label(
                            egui::RichText::new(format!(
                                "{} previews could not be read",
                                summary.missing
                            ))
                            .small()
                            .color(p.warn),
                        );
                    }
                    ui.label(
                        egui::RichText::new(self.export_target.display().to_string())
                            .small()
                            .monospace()
                            .color(p.dim),
                    );
                    if ui
                        .button(egui::RichText::new("Reveal in Explorer").color(p.text))
                        .clicked()
                    {
                        reveal_in_explorer(&self.export_target);
                    }
                }
            });
        self.export_open = open && !close;
    }

    fn reset_results(&mut self) {
        self.uniq.clear();
        self.view.clear();
        self.timeline_deltas.clear();
        self.total_hits = 0;
        self.matched_files = 0;
        self.view_files = 0;
        self.query_us = 0.0;
        self.selected = NO_SELECTION;
        self.pending_scroll = None;
        self.context.clear();
        self.context_key = None;
        self.snippets.clear();
        self.scopes.clear();
    }

    fn run_search(&mut self) {
        // A token that starts with '-' is a negative constraint: lines that
        // match it are filtered out of the positive search. Negation uses exact
        // term matches, so `-err` excludes lines containing the term `err`, not
        // the `error` prefix the positive side would widen to.
        //
        // `path:` and `-path:` constrain the file's full path, not the text of
        // the line, so they are pulled out of both term lists before anything
        // reaches the index. They must never enter `self.tokens`, where they
        // would paint highlights over the snippet and feed the scope badges.
        let mut positive: Vec<String> = Vec::new();
        let mut negative: Vec<String> = Vec::new();
        let mut path_keep: Vec<String> = Vec::new();
        let mut path_drop: Vec<String> = Vec::new();
        for t in query_tokens(&self.query) {
            let path_side = t
                .strip_prefix("-path:")
                .map(|rest| (rest, true))
                .or_else(|| t.strip_prefix("path:").map(|rest| (rest, false)));
            if let Some((needle, negated)) = path_side {
                // `query_tokens` already lowercases, so a `PATH:Foo` query reads
                // the same as `path:foo`; the needle is folded into the same
                // shape the file paths are compared in.
                if !needle.is_empty() {
                    let needle = path_needle_key(needle);
                    if negated {
                        path_drop.push(needle);
                    } else {
                        path_keep.push(needle);
                    }
                }
                // A bare `path:` with nothing after it constrains nothing, and
                // the token is spent either way: it never joins the content list.
                continue;
            }
            match t.strip_prefix('-') {
                Some(rest) => {
                    if !rest.is_empty() {
                        negative.push(rest.to_string());
                    }
                }
                None => positive.push(t),
            }
        }
        self.tokens = positive.clone();
        let Some(built) = self.built.as_ref() else {
            self.reset_results();
            return;
        };
        let t = Instant::now();
        let hits = built.index.search(&positive.join(" "));
        let query_us = t.elapsed().as_secs_f64() * 1e6;
        let excluded = collect_excluded(built, &negative);
        // A `path:` needle is one decision per file, so each file's normalized
        // path key is lowered once and reused for every hit inside it.
        let mut path_keys: HashMap<u32, String> = HashMap::new();
        let mut uniq: Vec<(u32, u32, u64)> = Vec::with_capacity(hits.len());
        let mut last: Option<(u32, u32)> = None;
        let mut files = 0usize;
        let mut prev_file = u32::MAX;
        for h in &hits {
            let key = (h.file_id, h.line);
            if last == Some(key) {
                continue;
            }
            last = Some(key);
            if excluded.contains(&key) {
                continue;
            }
            if !(path_keep.is_empty() && path_drop.is_empty()) {
                let path_key = path_keys.entry(h.file_id).or_insert_with(|| {
                    match built.paths.get(h.file_id as usize) {
                        Some(p) => path_match_key(p),
                        None => String::new(),
                    }
                });
                let keeps = path_keep.iter().all(|n| path_key.contains(n.as_str()));
                let drops = path_drop.iter().any(|n| path_key.contains(n.as_str()));
                if !keeps || drops {
                    continue;
                }
            }
            uniq.push((h.file_id, h.line, h.byte_offset));
            if h.file_id != prev_file {
                files += 1;
                prev_file = h.file_id;
            }
        }
        self.query_us = query_us;
        self.total_hits = hits.len();
        self.matched_files = files;
        self.uniq = uniq;
        self.rebuild_view();
    }

    fn rebuild_view(&mut self) {
        let exts = ext_tokens(&self.ext_filter);
        let view: Vec<usize> = if exts.is_empty() {
            (0..self.uniq.len()).collect()
        } else {
            self.uniq
                .iter()
                .enumerate()
                .filter(|(_, (file_id, _, _))| exts.iter().any(|e| *e == self.file_ext(*file_id)))
                .map(|(i, _)| i)
                .collect()
        };
        self.view = view;
        if self.result_order == ResultOrder::Timeline {
            self.order_view_by_timestamp();
            self.timeline_deltas = self.compute_timeline_deltas();
        } else {
            self.timeline_deltas.clear();
        }
        // Rows sharing one file count once each. In `By File` order a file's
        // rows sit together, so counting distinct files gives the same number
        // as counting file transitions; in `Timeline` order the files
        // interleave, and transitions would count one file over and over.
        let mut files: HashSet<u32> = HashSet::new();
        for i in &self.view {
            files.insert(self.uniq[*i].0);
        }
        self.view_files = files.len();
        self.selected = if self.view.is_empty() {
            NO_SELECTION
        } else {
            0
        };
        self.pending_scroll = if self.view.is_empty() { None } else { Some(0) };
        self.context.clear();
        self.context_key = None;
    }

    /// Put the rows that carry a parseable log stamp on one chronological line,
    /// oldest first, ties broken by `(file_id, line)`. Rows whose line has no
    /// stamp keep their normal file/line order and sit after every timed row,
    /// so the timeline is never interrupted by a line that cannot be placed.
    fn order_view_by_timestamp(&mut self) {
        let mut timed: Vec<(i64, u32, u32, usize)> = Vec::with_capacity(self.view.len());
        let mut untimed: Vec<(u32, u32, usize)> = Vec::new();
        for &i in &self.view {
            let (file_id, line, byte_offset) = self.uniq[i];
            let path = self.path_of(file_id);
            let snippet = self.snippets.get(&path, file_id, line, byte_offset);
            match parse_line_timestamp_ms(&snippet) {
                Some(ms) => timed.push((ms, file_id, line, i)),
                None => untimed.push((file_id, line, i)),
            }
        }
        timed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
        untimed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let ordered: Vec<usize> = timed
            .into_iter()
            .map(|(_, _, _, i)| i)
            .chain(untimed.into_iter().map(|(_, _, i)| i))
            .collect();
        self.view = ordered;
    }

    /// The gap between each visible row and the timestamped row before it, in
    /// the order the rows are shown. The first timed row opens the timeline with
    /// `+0ms`, and a row with no stamp on its line gets no badge at all.
    /// Snippets come from the same bounded cache the rows paint from, so a
    /// rebuild reads each file at most once.
    fn compute_timeline_deltas(&mut self) -> Vec<Option<String>> {
        let mut out: Vec<Option<String>> = Vec::with_capacity(self.view.len());
        let mut prev: Option<i64> = None;
        for &i in &self.view {
            let (file_id, line, byte_offset) = self.uniq[i];
            let path = self.path_of(file_id);
            let snippet = self.snippets.get(&path, file_id, line, byte_offset);
            match parse_line_timestamp_ms(&snippet) {
                Some(ms) => {
                    let delta = prev.map(|p| ms - p).unwrap_or(0);
                    prev = Some(ms);
                    out.push(Some(format_delta_ms(delta)));
                }
                None => out.push(None),
            }
        }
        out
    }

    /// The "×" beside the query box: empty the field and rebuild the results in
    /// the same repaint, exactly as typing a cleared query does.
    fn clear_query(&mut self) {
        self.query.clear();
        self.run_search();
    }

    /// The "×" beside the ext filter: empty the field and rebuild the view from
    /// the cached hits, so the header counter updates instantly.
    fn clear_ext_filter(&mut self) {
        self.ext_filter.clear();
        self.rebuild_view();
    }

    /// Commit the current query into the bounded history, exactly as pressing
    /// Enter in the search box does. An empty query is never recorded, and a
    /// repeated query refreshes its position to the newest slot instead of
    /// duplicating an entry.
    fn commit_query(&mut self) {
        let q = self.query.trim();
        if q.is_empty() {
            return;
        }
        let entry = q.to_string();
        if let Some(pos) = self.query_history.iter().position(|e| *e == entry) {
            self.query_history.remove(pos);
        }
        self.query_history.push_back(entry);
        while self.query_history.len() > HISTORY_CAP {
            self.query_history.pop_front();
        }
        self.history_pos = None;
    }

    /// Alt+Up walks to the older committed query, Alt+Down to the newer one.
    /// The first Alt+Up saves the live text the user is typing; Alt+Down at the
    /// newest entry restores it. Each move sets the query and runs the search
    /// immediately, so the results follow the history as it cycles.
    fn history_cycle(&mut self, older: bool) {
        let len = self.query_history.len();
        if len == 0 {
            return;
        }
        match self.history_pos {
            None => {
                if !older {
                    return;
                }
                self.pending_query = self.query.clone();
                self.history_pos = Some(len - 1);
                self.query = self.query_history[len - 1].clone();
                self.run_search();
            }
            Some(p) => {
                if older {
                    if p == 0 {
                        return;
                    }
                    self.history_pos = Some(p - 1);
                    self.query = self.query_history[p - 1].clone();
                    self.run_search();
                } else if p == len - 1 {
                    self.history_pos = None;
                    self.query = std::mem::take(&mut self.pending_query);
                    self.run_search();
                } else {
                    self.history_pos = Some(p + 1);
                    self.query = self.query_history[p + 1].clone();
                    self.run_search();
                }
            }
        }
    }

    /// The user edited the query text: the history cycle resets, so the next
    /// Alt+Up starts from the newest committed entry, and the next Enter commits
    /// whatever they typed.
    fn note_query_edit(&mut self) {
        self.history_pos = None;
        self.run_search();
    }

    /// The one-line human-readable summary the results header shows every frame.
    fn header_text(&self) -> String {
        result_summary(
            self.view.len(),
            self.uniq.len(),
            self.view_files,
            self.matched_files,
            self.total_hits,
            self.query_us,
        )
    }

    fn refresh_context(&mut self) {
        if self.selected >= self.view.len() {
            self.context.clear();
            self.context_key = None;
            return;
        }
        let (file_id, line, byte_offset) = self.uniq[self.view[self.selected]];
        if self.context_key == Some((file_id, line)) {
            return;
        }
        let path = self.path_of(file_id);
        self.context = match extract_context(&path, byte_offset, CONTEXT_RADIUS) {
            Ok(lines) => lines,
            Err(e) => vec![(-1, format!("(context unavailable: {e})"))],
        };
        self.context_key = Some((file_id, line));
    }

    // Folder browser ----------------------------------------------------------

    fn refresh_nav(&mut self) {
        if self.subdirs_src == self.nav_path {
            return;
        }
        self.subdirs_src = self.nav_path.clone();
        self.subdirs.clear();
        self.nav_error = None;
        match std::fs::read_dir(&self.nav_path) {
            Ok(rd) => {
                for entry in rd.flatten() {
                    let path = entry.path();
                    let Ok(ft) = entry.file_type() else { continue };
                    if !ft.is_dir() {
                        continue;
                    }
                    let Some(name_os) = path.file_name() else {
                        continue;
                    };
                    let name = name_os.to_string_lossy();
                    if is_hidden_or_system_dir(&name) {
                        continue;
                    }
                    self.subdirs.push(path);
                }
                self.subdirs.sort();
                self.subdirs.truncate(SUBDIR_CAP);
            }
            Err(e) => self.nav_error = Some(format!("folder browser: {e}")),
        }
    }

    fn navigate(&mut self, path: &Path) {
        self.nav_path = path.to_path_buf();
        self.root_text = path.display().to_string();
        self.status = format!("browsing {}", path.display());
    }

    // Paint -------------------------------------------------------------------

    fn paint_toolbar(&mut self, ctx: &egui::Context) {
        let p = self.palette();
        egui::TopBottomPanel::top("toolbar")
            .frame(bar_frame(p.chrome, p.border, 10.0, 7.0))
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    // The logo slot is as tall as the tallest control, which fixes
                    // the row height for the whole bar. Its rect is kept for the
                    // geometry tests, and unused in a normal build.
                    let _logo_rect = paint_owl_lens_logo(ui, &p);
                    #[cfg(test)]
                    {
                        self.logo_rect = Some(_logo_rect);
                    }
                    ui.label(
                        egui::RichText::new("SEEKSAGE")
                            .strong()
                            .color(p.accent)
                            .size(14.5),
                    );
                    ui.add_space(4.0);

                    let busy = self.phase == Phase::Indexing;
                    // Reserve every pixel the rest of the row consumes, measured
                    // from the painted pieces at the sizes this bar uses. The path
                    // box then gets exactly the room that is left, so no item is
                    // ever pushed under the right-aligned theme square.
                    // Tail: the bar's 10 px padding, the 28 px square toggle, and
                    // while a build runs the ms read-out, spinner and progress bar
                    // with the row spacing between each piece.
                    let tail = if busy {
                        10.0 + 28.0 + 8.0 + 40.0 + 8.0 + 15.0 + 8.0 + 150.0
                    } else {
                        10.0 + 28.0
                    };
                    // The five buttons between the path box and the tail, with the
                    // row spacing that sits before each of them.
                    let buttons = 58.0 + 68.0 + 64.0 + 66.0 + 120.0 + 5.0 * 8.0;
                    // The clean horizontal gap the tail keeps from the last button,
                    // plus the row spacing that sits between the buttons and the
                    // right-anchored block. The painted rects say the busy tail costs
                    // 9 px more than the pieces above (the ms read-out paints 49 px
                    // wide, not 40), so 25 px here lands 16 px of clear space while a
                    // build runs and 33 px when idle, both measured at a 1100 px window.
                    let gap = 25.0;
                    // `desired_width` is the inner text width, so the frame adds its
                    // own 8 px margin on each side on top of what is reserved here.
                    let frame = 16.0;
                    let path_w = (ui.available_width() - tail - buttons - gap - frame).max(160.0);
                    // Symmetric vertical padding gives the box its natural
                    // single-line height, so the text sits centered inside the
                    // frame, on the same centre axis as the logo, the title and
                    // the toolbar buttons, instead of hanging under a tall border.
                    let path_box = ui.add(
                        egui::TextEdit::singleline(&mut self.root_text)
                            .font(FontId::monospace(13.0))
                            .hint_text("C:\\path\\to\\repository")
                            .id(egui::Id::from("path_box"))
                            .lock_focus(true)
                            .desired_width(path_w)
                            .margin(egui::Margin::symmetric(8.0, 5.0)),
                    );
                    #[cfg(test)]
                    {
                        self.path_box_rect = Some(path_box.rect);
                    }
                    let enter =
                        path_box.has_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));

                    let index_btn = ui.add_enabled(
                        !busy,
                        egui::Button::new(
                            egui::RichText::new("Index").strong().color(p.emerald),
                        )
                        .min_size(egui::vec2(58.0, 28.0)),
                    );
                    #[cfg(test)]
                    {
                        self.index_rect = Some(index_btn.rect);
                    }
                    if !busy && (enter || index_btn.clicked()) {
                        self.start_build();
                    }

                    let rebuild_btn = ui.add_enabled(
                        !busy,
                        egui::Button::new(
                            egui::RichText::new("Rebuild").strong().color(p.accent),
                        )
                        .min_size(egui::vec2(68.0, 28.0)),
                    );
                    rebuild_btn.clone().on_hover_text(
                        "Force a fresh scan and overwrite the disk cache (picks up edits in nested files)",
                    );
                    if !busy && rebuild_btn.clicked() {
                        self.start_rebuild();
                    }

                    // `Watch` is the live file watcher, a chip next to Rebuild. It
                    // lights while it is on, so the user can see the tree is being
                    // followed and the rows will refresh themselves.
                    let watch_btn = ui.add(
                        egui::Button::new(
                            egui::RichText::new("Watch")
                                .strong()
                                .color(if self.watch_enabled { p.emerald } else { p.dim }),
                        )
                        .fill(if self.watch_enabled {
                            p.selected_fill
                        } else {
                            p.card
                        })
                        .min_size(egui::vec2(64.0, 28.0)),
                    );
                    watch_btn.clone().on_hover_text(
                        "Watch the tree: rebuild automatically once the files stop moving, \
                         keeping the query, the filter and the result order",
                    );
                    if watch_btn.clicked() {
                        self.set_watch(!self.watch_enabled);
                    }

                    let exporting = self.export_rx.is_some();
                    let can_export = !busy && !exporting && !self.view.is_empty();
                    let export_btn = ui.add_enabled(
                        can_export,
                        egui::Button::new(
                            egui::RichText::new("Export").strong().color(p.gold),
                        )
                        .min_size(egui::vec2(66.0, 28.0)),
                    );
                    export_btn.clone().on_hover_text(
                        "Export the filtered results as JSON, CSV, or Markdown (Ctrl+Shift+E)",
                    );
                    if can_export && export_btn.clicked() {
                        self.open_export_panel();
                    }

                    let pack_btn = ui.add_enabled(
                        can_export,
                        egui::Button::new(
                            egui::RichText::new("Copy AI Context")
                                .strong()
                                .color(p.emerald),
                        )
                        .min_size(egui::vec2(120.0, 28.0)),
                    );
                    #[cfg(test)]
                    {
                        self.pack_rect = Some(pack_btn.rect);
                    }
                    pack_btn.clone().on_hover_text(
                        "Copy the filtered results as LLM-ready markdown, \
                         with context merged per file (Ctrl+Shift+C)",
                    );
                    if can_export && pack_btn.clicked() {
                        self.copy_ai_context(ctx);
                    }

                    // The tail of the row is right-anchored: the compact square
                    // theme toggle sits in the far-right corner, and while a build
                    // runs the live progress block renders to its left. `lanes` is
                    // no longer a control here: it comes from `default_lanes()`
                    // (`available_parallelism` clamped to 2..=8) and is managed
                    // automatically by the indexer.
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        // Keep the bar's right padding, so the square sits inside
                        // the corner instead of flush against the window edge.
                        ui.add_space(10.0);
                        let theme_resp = theme_toggle_button(ui, self.dark_mode, &p);
                        #[cfg(test)]
                        {
                            self.theme_rect = Some(theme_resp.rect);
                        }
                        let theme_clicked = theme_resp
                            .clone()
                            .on_hover_text("Switch theme: Midnight Owl ↔ Ceramic Mint")
                            .clicked();
                        if theme_clicked {
                            self.dark_mode = !self.dark_mode;
                        }

                        if busy {
                            ui.label(
                                egui::RichText::new(format!("{:.0} ms", self.build_ms))
                                    .monospace()
                                    .color(p.emerald)
                                    .size(11.5),
                            );
                            ui.add(egui::Spinner::new().size(15.0).color(p.emerald));
                            // The honest bar: fraction comes from the worker's real
                            // counters, so percent = files ingested / files discovered.
                            let bar_text = format!(
                                "{:.0}% · {}",
                                self.progress.percent(),
                                self.progress.stage.word()
                            );
                            let bar_resp = ui.add(
                                egui::ProgressBar::new(self.progress.fraction())
                                    .text(
                                        egui::RichText::new(bar_text)
                                            .strong()
                                            .size(11.0)
                                            .color(p.emerald),
                                    )
                                    .desired_width(150.0)
                                    .fill(p.emerald)
                                    .rounding(egui::Rounding::same(3.0))
                                    .animate(true),
                            );
                            #[cfg(test)]
                            {
                                self.progress_bar_rect = Some(bar_resp.rect);
                            }
                            bar_resp.on_hover_text(self.progress.detail(self.lanes));
                        }
                    });
                });
            });
    }

    fn paint_navbar(&mut self, ctx: &egui::Context) {
        let p = self.palette();
        egui::TopBottomPanel::top("nav_bar")
            .frame(bar_frame(p.panel, p.border, 10.0, 6.0))
            .show(ctx, |ui| {
                egui::CollapsingHeader::new(
                    egui::RichText::new("Quick Navigation · Breadcrumbs & Folder Browser")
                        .strong()
                        .size(12.0)
                        .color(p.dim),
                )
                .default_open(true)
                .id_salt("nav_header")
                .show(ui, |ui| {
                    ui.spacing_mut().item_spacing.y = 4.0;

                    let drive_items: Vec<(String, PathBuf)> = self
                        .drives
                        .iter()
                        .map(|d| (d.display().to_string(), d.clone()))
                        .collect();
                    if let Some(picked) = chip_row(ui, "Drives", &drive_items, "drives", &p) {
                        self.navigate(&picked);
                    }

                    let mut crumbs: Vec<(String, PathBuf)> = Vec::new();
                    if let Some(parent) = self.nav_path.parent() {
                        crumbs.push((String::from("↑ parent"), parent.to_path_buf()));
                    }
                    crumbs.extend(breadcrumb_segments(&self.nav_path));
                    if let Some(picked) = chip_row(ui, "Path", &crumbs, "crumbs", &p) {
                        self.navigate(&picked);
                    }

                    let sub_items: Vec<(String, PathBuf)> = self
                        .subdirs
                        .iter()
                        .map(|d| (file_name(d), d.clone()))
                        .collect();
                    if sub_items.is_empty() {
                        ui.horizontal(|ui| {
                            nav_label(ui, "Subdirs", &p);
                            ui.label(
                                egui::RichText::new("no subdirectories")
                                    .small()
                                    .color(p.dim),
                            );
                        });
                    } else if let Some(picked) = chip_row(ui, "Subdirs", &sub_items, "subdirs", &p)
                    {
                        self.navigate(&picked);
                    }

                    if let Some(e) = &self.nav_error {
                        ui.label(egui::RichText::new(e).small().color(p.warn));
                    }

                    if !self.recents.is_empty() {
                        let recent_items: Vec<(String, PathBuf)> = self
                            .recents
                            .iter()
                            .map(|d| (d.display().to_string(), d.clone()))
                            .collect();
                        if let Some(picked) = chip_row(ui, "Recent", &recent_items, "recents", &p) {
                            self.nav_path = picked.clone();
                            self.root_text = picked.display().to_string();
                            self.start_build();
                        }
                    }
                });
            });
    }

    fn paint_searchbar(&mut self, ctx: &egui::Context) {
        let p = self.palette();
        egui::TopBottomPanel::top("search_bar")
            .frame(bar_frame(p.chrome, p.border, 10.0, 7.0))
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    // Tight spacing so each "×" reads as part of its own box.
                    ui.spacing_mut().item_spacing.x = 2.0;
                    // The row reserves everything that follows the query box,
                    // measured from the painted pieces at the sizes this bar uses,
                    // so the query box takes exactly the room that is left and the
                    // trailing items never land on top of each other: the frame
                    // `desired_width` adds on its own sides (16), the query "×" (26),
                    // the clean gap after it (8) with the row spacing (2 + 2), the
                    // `ext:` label (21), the ext box frame (221, inner 205 + 16),
                    // the ext "×" (26), the row spacing before the latency read-out
                    // (2), the µs label (48) and the clear gap it keeps from the ext
                    // "×" (24, measured 21 px with the row spacing). The two "×" own
                    // their boxes and Esc clears both, so no shared Clear button is
                    // needed.
                    let reserve = 16.0 + 26.0 + 8.0 + 4.0 + 21.0 + 221.0 + 26.0 + 2.0 + 48.0 + 24.0;
                    let search_w = (ui.available_width() - reserve).max(220.0);
                    // Symmetric vertical padding keeps the box at its natural
                    // single-line height, so the text is centered inside the frame
                    // and lines up with the toolbar row on the same centre axis.
                    let search_box = ui.add(
                        egui::TextEdit::singleline(&mut self.query)
                            .font(FontId::monospace(13.5))
                            .hint_text("Search terms (AND, prefix / substring, ASCII & Cyrillic) - Enter, Alt+Up / Alt+Down history")
                            .id(egui::Id::from("search_box"))
                            .desired_width(search_w)
                            .margin(egui::Margin::symmetric(8.0, 5.0)),
                    );
                    #[cfg(test)]
                    {
                        self.search_box_rect = Some(search_box.rect);
                    }
                    let (alt, up, down, enter) = ui.input(|i| {
                        let m = i.modifiers;
                        (
                            m.alt,
                            i.key_pressed(egui::Key::ArrowUp),
                            i.key_pressed(egui::Key::ArrowDown),
                            i.key_pressed(egui::Key::Enter),
                        )
                    });
                    // A TextEdit gives up focus the moment Enter lands, so the
                    // commit reads the `lost_focus` half of the pair, the way the
                    // egui docs recommend. Alt+Up / Alt+Down walk the committed
                    // query history, each step running the search on the spot;
                    // plain arrows stay with the row selection in
                    // `paint_results`, which skips row navigation while Alt is
                    // down.
                    let box_holds_focus = search_box.has_focus();
                    let surrendered_on_enter = search_box.lost_focus() && enter;
                    if box_holds_focus {
                        if alt && up {
                            self.history_cycle(true);
                        }
                        if alt && down {
                            self.history_cycle(false);
                        }
                    }
                    if surrendered_on_enter && !alt {
                        self.commit_query();
                        // Enter ends TextEdit's editing session, so take the box
                        // back: the user keeps typing and commits again without
                        // clicking.
                        search_box.request_focus();
                    }
                    if search_box.changed() {
                        self.note_query_edit();
                    }
                    search_box.on_hover_text(
                        "Enter commits the query to history; Alt+Up / Alt+Down cycle the last 10 committed queries",
                    );
                    let query_clear =
                        clear_button(ui, !self.query.is_empty(), &p, "× clear this search box (Esc)");
                    #[cfg(test)]
                    {
                        self.query_clear_rect = Some(query_clear.rect);
                    }
                    if query_clear.clicked() {
                        self.clear_query();
                    }

                    // A clean gap between the query "×" and the `ext:` label, so
                    // the × stays read as part of the query box and the labelled
                    // extension field opens its own space.
                    ui.add_space(8.0);

                    // The label sits directly in front of the box in the same style
                    // as the other labelled fields of the app, and the box carries
                    // no placeholder: on a fresh app the field is clean and empty,
                    // so the tree is searched in full until the user narrows it.
                    let ext_label = ui.label(egui::RichText::new("ext:").strong().color(p.text));
                    #[cfg(test)]
                    {
                        self.ext_label_rect = Some(ext_label.rect);
                    }
                    ext_label.clone().on_hover_text(
                        "Extensions to keep, separated by spaces, e.g. .rs .md .txt. \
                         An empty box searches every parsed text file in the tree.",
                    );
                    let ext_box = ui.add(
                        egui::TextEdit::singleline(&mut self.ext_filter)
                            .font(FontId::monospace(12.5))
                            .hint_text("")
                            .id(egui::Id::from("ext_filter"))
                            .desired_width(205.0)
                            .margin(egui::Margin::symmetric(8.0, 5.0)),
                    );
                    #[cfg(test)]
                    {
                        self.ext_box_rect = Some(ext_box.rect);
                    }
                    if ext_box.changed() {
                        self.rebuild_view();
                    }
                    let ext_clear =
                        clear_button(ui, !self.ext_filter.is_empty(), &p, "× clear the ext filter (Esc)");
                    #[cfg(test)]
                    {
                        self.ext_clear_rect = Some(ext_clear.rect);
                    }
                    if ext_clear.clicked() {
                        self.clear_ext_filter();
                    }

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let _latency = ui.label(
                            egui::RichText::new(format!("{:.2} µs", self.query_us))
                                .strong()
                                .monospace()
                                .color(p.accent)
                                .size(12.0),
                        );
                        #[cfg(test)]
                        {
                            self.latency_rect = Some(_latency.rect);
                        }
                    });
                });
            });
    }

    fn paint_statusbar(&mut self, ctx: &egui::Context) {
        let p = self.palette();
        egui::TopBottomPanel::bottom("status_bar")
            .frame(bar_frame(p.chrome, p.border, 10.0, 5.0))
            .exact_height(30.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    let status_c = self.phase.color(&p);
                    let dot_center = egui::pos2(ui.cursor().min.x + 5.0, ui.cursor().min.y + 9.0);
                    ui.allocate_exact_size(egui::vec2(12.0, 18.0), egui::Sense::hover());
                    ui.painter().circle_filled(dot_center, 3.5, status_c);

                    let mut job = egui::text::LayoutJob::default();
                    let phase_text = if self.phase == Phase::Indexing {
                        format!("{} {:.0}%  ", self.phase.word(), self.progress.percent())
                    } else {
                        format!("{}  ", self.phase.word())
                    };
                    job.append(&phase_text, 0.0, mono(11.5, status_c));
                    if self.phase == Phase::Indexing {
                        // The textual half of the progress read-out: the live stage
                        // and the real counters the worker is publishing.
                        job.append(&self.progress.detail(self.lanes), 0.0, mono(11.5, p.text));
                    }
                    let mut breakdown: Option<String> = None;
                    if let Some(b) = &self.built {
                        job.append(
                            &format!(
                                "Files: {}  ·  Lines: {}  ·  Terms: {}  ·  RAM: {:.2} MiB  ·  Build: {:.1} ms  ·  {:.2} MiB/s",
                                b.walk.files_matched,
                                b.producers.lines,
                                b.index.terms,
                                b.memory_mib(),
                                b.total_ms(),
                                b.throughput(),
                            ),
                            0.0,
                            mono(11.5, p.text),
                        );
                        // The stage breakdown lives in a tooltip, so the bar keeps
                        // one short line and the status text on the right has room.
                        breakdown = Some(format!(
                            "crawl {:.1} ms  ·  ingest {:.1} ms  ·  merge {:.1} ms",
                            b.crawl_ms, b.ingest_ms, b.merge_ms
                        ));
                    }
                    let stats_label = ui.label(job);
                    if let Some(tip) = breakdown {
                        stats_label.on_hover_text(tip);
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.add(
                            egui::Label::new(
                                egui::RichText::new(&self.status).small().color(status_c),
                            )
                            .truncate(),
                        );
                    });
                });
            });
    }

    fn paint_results(&mut self, ctx: &egui::Context) {
        let p = self.palette();
        let search_focused = ctx.memory(|m| m.focused() == Some(egui::Id::from("search_box")));
        let (down, up, alt) = ctx.input(|i| {
            let m = i.modifiers;
            (
                i.key_pressed(egui::Key::ArrowDown),
                i.key_pressed(egui::Key::ArrowUp),
                m.alt,
            )
        });
        // Alt+Up / Alt+Down belong to the query history in the search bar, so
        // the row selection only answers the plain arrows.
        if search_focused && !alt && (down || up) && !self.view.is_empty() {
            let len = self.view.len();
            let cur = if self.selected == NO_SELECTION {
                0
            } else {
                self.selected
            };
            self.selected = if down {
                (cur + 1).min(len - 1)
            } else {
                cur.saturating_sub(1)
            };
            self.pending_scroll = Some(self.selected);
        }

        egui::SidePanel::left("results_panel")
            .resizable(true)
            .width_range(320.0..=900.0)
            .default_width(500.0)
            .frame(bar_frame(p.panel, p.border, 8.0, 8.0))
            .show(ctx, |ui| {
                // In tests the panel records the zones it paints in the frame, in
                // paint order, so a headless test can press the pointer on the
                // exact spot a card or a chip occupies.
                #[cfg(test)]
                {
                    self.row_rects.clear();
                    self.chip_rects.clear();
                }
                ui.horizontal(|ui| {
                    // The right-hand controls have fixed sizes, so the width they
                    // take is known up front: `Timeline`, `By File` and the
                    // `selected / total` counter. The header label gets everything
                    // left of that strip and truncates inside its own width, so it
                    // can never slide under the chips at the default panel width.
                    // `Export` and `Copy AI Context` are not repeated here; they
                    // live in the top toolbar, where the panel width does not
                    // squeeze them.
                    let strip_w = 74.0 + 66.0 + 56.0 + 3.0 * ui.spacing().item_spacing.x;
                    let label_w = (ui.available_width() - strip_w).max(140.0);
                    let header = self.header_text();
                    ui.add_sized(
                        [label_w, 24.0],
                        egui::Label::new(
                            egui::RichText::new(header)
                                .strong()
                                .color(p.text)
                                .size(12.0),
                        )
                        .truncate(),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        // The result order sits as two chips in the same bar:
                        // `By File` keeps rows grouped inside their own file,
                        // `Timeline` puts every stamped row on one chronological
                        // line across all files and shows the gap from the row
                        // before it. A chip only rebuilds the view when it
                        // actually changes the order, so clicking the active one
                        // never re-sorts the list.
                        // Each chip gets its own fixed slot through `add_sized`, so
                        // the button fills exactly the width the header reserved and
                        // the hover tooltip hangs on the chip's own response, never
                        // on the label text next to it.
                        let timeline_chip = ui.add_sized(
                            [74.0, 24.0],
                            egui::Button::new(egui::RichText::new("Timeline").strong().color(
                                if self.result_order == ResultOrder::Timeline {
                                    p.gold
                                } else {
                                    p.dim
                                },
                            ))
                            .fill(
                                if self.result_order == ResultOrder::Timeline {
                                    p.selected_fill
                                } else {
                                    p.card
                                },
                            ),
                        );
                        #[cfg(test)]
                        self.chip_rects.push(timeline_chip.rect);
                        timeline_chip.clone().on_hover_text(
                            "Sort every match on one chronological line across files, \
                             with the gap from the row before it",
                        );
                        if timeline_chip.clicked() && self.result_order != ResultOrder::Timeline {
                            self.result_order = ResultOrder::Timeline;
                            self.rebuild_view();
                        }
                        let by_file_chip = ui.add_sized(
                            [66.0, 24.0],
                            egui::Button::new(egui::RichText::new("By File").strong().color(
                                if self.result_order == ResultOrder::ByFile {
                                    p.accent
                                } else {
                                    p.dim
                                },
                            ))
                            .fill(
                                if self.result_order == ResultOrder::ByFile {
                                    p.selected_fill
                                } else {
                                    p.card
                                },
                            ),
                        );
                        #[cfg(test)]
                        self.chip_rects.push(by_file_chip.rect);
                        by_file_chip
                            .clone()
                            .on_hover_text("Keep rows grouped by file, in file and line order");
                        if by_file_chip.clicked() && self.result_order != ResultOrder::ByFile {
                            self.result_order = ResultOrder::ByFile;
                            self.rebuild_view();
                        }
                        if self.selected != NO_SELECTION && self.selected < self.view.len() {
                            ui.label(
                                egui::RichText::new(format!(
                                    "{} / {}",
                                    self.selected + 1,
                                    self.view.len()
                                ))
                                .small()
                                .monospace()
                                .color(p.accent),
                            );
                        }
                    });
                });
                ui.add_space(2.0);
                ui.separator();
                ui.add_space(2.0);

                if self.built.is_none() {
                    ui.add_space(12.0);
                    ui.label(
                        egui::RichText::new(
                            "No index loaded yet — choose a folder above and click Index.",
                        )
                        .color(p.dim)
                        .size(12.5),
                    );
                    return;
                }
                if self.view.is_empty() {
                    ui.add_space(12.0);
                    ui.label(
                        egui::RichText::new(
                            "No matching lines — type a query or clear the extension filter.",
                        )
                        .color(p.dim)
                        .size(12.5),
                    );
                    return;
                }

                let rows = self.view.len();
                let tokens = self.tokens.clone();
                egui::ScrollArea::vertical()
                    .id_salt("results_list")
                    .auto_shrink([false, false])
                    .show_rows(ui, ROW_HEIGHT, rows, |ui, range| {
                        ui.spacing_mut().item_spacing.y = 0.0;
                        let first_top = ui.cursor().min.y;
                        if let Some(target) = self.pending_scroll {
                            let width = ui.available_width();
                            if range.contains(&target) {
                                let top = first_top + (target - range.start) as f32 * ROW_HEIGHT;
                                let rect = egui::Rect::from_min_size(
                                    egui::pos2(ui.cursor().min.x, top),
                                    egui::vec2(width, ROW_HEIGHT),
                                );
                                ui.scroll_to_rect(rect, Some(egui::Align::Center));
                            } else if target > range.start {
                                ui.scroll_with_delta(egui::vec2(0.0, ROW_HEIGHT));
                            } else {
                                ui.scroll_with_delta(egui::vec2(0.0, -ROW_HEIGHT));
                            }
                            self.pending_scroll = None;
                        }
                        for i in range {
                            let (file_id, line, byte_offset) = self.uniq[self.view[i]];
                            // In timeline order, the badge opens with the gap from
                            // the stamped row before it. A row whose line carries
                            // no stamp has no badge, and shows none.
                            let delta = if self.result_order == ResultOrder::Timeline {
                                self.timeline_deltas
                                    .get(i)
                                    .and_then(|d| d.as_ref())
                                    .cloned()
                            } else {
                                None
                            };
                            let path = self.path_of(file_id);
                            let name = file_name(&path);
                            let parent_dir = path
                                .parent()
                                .and_then(|pr| pr.file_name())
                                .map(|n| format!("{}/", n.to_string_lossy()))
                                .unwrap_or_default();
                            let snippet = self.snippets.get(&path, file_id, line, byte_offset);
                            let scope = self.scopes.get(&path, file_id, line, byte_offset);
                            let on = i == self.selected;
                            let size = egui::vec2(ui.available_width(), ROW_HEIGHT);
                            let (rect, resp) = ui.allocate_exact_size(size, egui::Sense::click());
                            #[cfg(test)]
                            self.row_rects.push(rect);

                            let card = egui::Rect::from_min_max(
                                egui::pos2(rect.min.x + 2.0, rect.min.y + 2.0),
                                egui::pos2(rect.max.x - 4.0, rect.max.y - 2.0),
                            );
                            let fill = if on {
                                p.selected_fill
                            } else if resp.hovered() {
                                p.raised
                            } else {
                                p.card
                            };
                            // The accent edge shows on the selected row and on a
                            // hovered row, so one condition covers both cases.
                            let edge = if on || resp.hovered() {
                                p.accent
                            } else {
                                p.border
                            };
                            paint_card(ui, card, fill, edge);

                            if on {
                                let bar = egui::Rect::from_min_size(
                                    egui::pos2(card.min.x + 1.5, card.min.y + 6.0),
                                    egui::vec2(3.0, card.height() - 12.0),
                                );
                                ui.painter().rect_filled(bar, 1.5, p.accent);
                            }

                            let inner = egui::Rect::from_min_max(
                                egui::pos2(card.min.x + 10.0, card.min.y + 5.0),
                                egui::pos2(card.max.x - 8.0, card.max.y - 5.0),
                            );
                            // The child ui only senses hover. It carries the badge
                            // and the snippet text, and a click on that text has to
                            // reach the row behind it, so the card keeps the click.
                            let builder = egui::UiBuilder::new()
                                .max_rect(inner)
                                .layout(egui::Layout::top_down(egui::Align::LEFT))
                                .sense(egui::Sense::hover());
                            ui.allocate_new_ui(builder, |ui| {
                                ui.spacing_mut().item_spacing.y = 2.0;
                                let lower = snippet.to_lowercase();
                                let matched = all_tokens_substring(&lower, &tokens);

                                let mut badge = egui::text::LayoutJob::default();
                                // The timeline gap leads the badge, in gold, so
                                // the eye reads the step forward before the name.
                                if let Some(d) = &delta {
                                    let stamp = format!("[{d}] ");
                                    badge.append(&stamp, 0.0, mono(12.0, p.gold));
                                }
                                if !matched {
                                    badge.append("~ ", 0.0, mono(12.0, p.dim));
                                }
                                if !parent_dir.is_empty() {
                                    badge.append(&parent_dir, 0.0, mono(11.5, p.dim));
                                }
                                badge.append(
                                    &name,
                                    0.0,
                                    mono(
                                        12.5,
                                        if matched {
                                            if on {
                                                p.accent
                                            } else {
                                                p.emerald
                                            }
                                        } else {
                                            p.dim
                                        },
                                    ),
                                );
                                badge.append(" : ", 0.0, mono(12.0, p.dim));
                                badge.append(
                                    &line.to_string(),
                                    0.0,
                                    mono(12.0, if on { p.gold } else { p.text }),
                                );
                                if let Some(s) = &scope {
                                    badge.append(" · ", 0.0, mono(12.0, p.dim));
                                    badge.append(s, 0.0, mono(12.0, p.accent));
                                }
                                ui.add(egui::Label::new(badge).truncate());

                                let mut job = egui::text::LayoutJob::default();
                                append_highlight(
                                    &mut job,
                                    &snippet,
                                    &tokens,
                                    mono(12.0, if on { p.text } else { p.dim }),
                                    mono_bg(12.0, p.text, p.match_bg),
                                );
                                ui.add(egui::Label::new(job).truncate());
                            });
                            // The row owns the whole card. Registering the
                            // interaction zone after the text puts the row on top of
                            // its own labels in the hit test, so a press anywhere
                            // inside the card — on the badge, on the snippet, on the
                            // accent bar, on the empty right margin — selects that
                            // row. It senses clicks only, never drags, so dragging
                            // the list to scroll stays with the scroll area.
                            let card_hit = ui.interact(
                                rect,
                                egui::Id::new(("result_row", i)),
                                egui::Sense::click(),
                            );
                            if card_hit.clicked() {
                                self.selected = i;
                            }
                        }
                    });
            });
    }

    fn paint_preview(&mut self, ctx: &egui::Context) {
        let p = self.palette();
        egui::CentralPanel::default()
            .frame(bar_frame(p.canvas, p.border, 10.0, 8.0))
            .show(ctx, |ui| {
                self.refresh_context();
                if self.selected >= self.view.len() {
                    ui.add_space(12.0);
                    ui.label(
                        egui::RichText::new(
                            "Select a result row on the left (or press ArrowDown / ArrowUp) to inspect surrounding code.",
                        )
                        .color(p.dim)
                        .size(12.5),
                    );
                    return;
                }
                let (file_id, line, _) = self.uniq[self.view[self.selected]];
                let path = self.path_of(file_id);
                let path_text = path.display().to_string();

                ui.horizontal(|ui| {
                    // Reserve the room the `line N` label and the three buttons
                    // need, then truncate the path into the remaining width so a
                    // long path never runs under Copy Path / Reveal in Explorer /
                    // Open in Editor.
                    let path_w = (ui.available_width() - 412.0).max(120.0);
                    let path_label = ui.add_sized(
                        [path_w, 22.0],
                        egui::Label::new(
                            egui::RichText::new(path_text.as_str())
                                .monospace()
                                .color(p.emerald)
                                .size(12.5),
                        )
                        .truncate(),
                    );
                    path_label.on_hover_text(path_text.as_str());
                    ui.label(
                        egui::RichText::new(format!("line {line}"))
                            .strong()
                            .monospace()
                            .color(p.gold)
                            .size(12.5),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let editor_btn = ui.add(egui::Button::new(
                            egui::RichText::new("Open in Editor").size(12.0),
                        ));
                        if editor_btn.clicked() {
                            open_in_editor(&path, line);
                        }
                        editor_btn.on_hover_text(format!(
                            "code --goto {}:{} (opens the exact line in VS Code)",
                            path.display(),
                            line
                        ));
                        if ui
                            .add(egui::Button::new(
                                egui::RichText::new("Reveal in Explorer").size(12.0),
                            ))
                            .clicked()
                        {
                            reveal_in_explorer(&path);
                        }
                        if ui
                            .add(egui::Button::new(
                                egui::RichText::new("Copy Path").size(12.0),
                            ))
                            .clicked()
                        {
                            ctx.copy_text(path_text.clone());
                        }
                    });
                });
                ui.add_space(2.0);
                ui.separator();
                ui.add_space(2.0);

                let tokens = self.tokens.clone();
                egui::ScrollArea::vertical()
                    .id_salt("preview")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.spacing_mut().item_spacing.y = 0.0;
                        for (rel, text) in &self.context {
                            let hit = *rel == 0;
                            let abs = line as i32 + rel;
                            let size = egui::vec2(ui.available_width(), LINE_HEIGHT);
                            let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
                            if hit {
                                ui.painter().rect_filled(rect, 4.0, p.selected_fill);
                                let bar = egui::Rect::from_min_size(
                                    rect.min,
                                    egui::vec2(3.0, rect.height()),
                                );
                                ui.painter().rect_filled(bar, 1.5, p.accent);
                            }
                            let inner = egui::Rect::from_min_max(
                                egui::pos2(rect.min.x + 6.0, rect.min.y),
                                egui::pos2(rect.max.x - 4.0, rect.max.y),
                            );
                            let builder = egui::UiBuilder::new()
                                .max_rect(inner)
                                .layout(egui::Layout::left_to_right(egui::Align::Center));
                            ui.allocate_new_ui(builder, |ui| {
                                ui.label(
                                    egui::RichText::new(format!("{abs:>5}"))
                                        .monospace()
                                        .size(12.0)
                                        .color(if hit { p.gold } else { p.dim }),
                                );
                                // The gutter divider is a painted hairline: the
                                // box-drawing `│` glyph is not in the monospace
                                // face, so as a label it renders as a tofu box.
                                let (div_rect, _) = ui.allocate_exact_size(
                                    egui::vec2(9.0, LINE_HEIGHT),
                                    egui::Sense::hover(),
                                );
                                ui.painter().vline(
                                    div_rect.center().x,
                                    div_rect.y_range(),
                                    egui::Stroke::new(1.0_f32, p.border),
                                );
                                let mut job = egui::text::LayoutJob::default();
                                append_highlight(
                                    &mut job,
                                    text,
                                    &tokens,
                                    mono(12.5, if hit { p.text } else { p.dim }),
                                    mono_bg(12.5, p.accent, p.match_bg),
                                );
                                ui.label(job);
                            });
                        }
                    });
            });
    }
}

// The frame callback ----------------------------------------------------------

impl eframe::App for SeekSageApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.theme_for != Some(self.dark_mode) {
            let p = self.palette();
            Self::apply_theme(ctx, &p, self.dark_mode);
            self.theme_for = Some(self.dark_mode);
        }
        if let Some(root) = self.startup.take() {
            self.nav_path = root;
            self.root_text = self.nav_path.display().to_string();
            self.start_build();
        }
        // Shortcuts are read before any panel paints so a key press lands the
        // same frame it arrived in, and the search box is focused before the
        // search bar is drawn.
        self.handle_hotkeys(ctx);
        self.poll_build(ctx);
        self.poll_watch(ctx);
        self.poll_export(ctx);
        self.refresh_nav();
        self.paint_toolbar(ctx);
        self.paint_navbar(ctx);
        self.paint_searchbar(ctx);
        self.paint_statusbar(ctx);
        self.paint_results(ctx);
        self.paint_preview(ctx);
        self.paint_export_panel(ctx);
    }
}

fn startup_root(args: &[String]) -> Option<PathBuf> {
    args.get(1).map(PathBuf::from).filter(|p| p.is_dir())
}

// Helpers ---------------------------------------------------------------------

fn drive_roots() -> Vec<PathBuf> {
    ('A'..='Z')
        .map(|c| PathBuf::from(format!("{c}:\\")))
        .filter(|p| p.is_dir())
        .collect()
}

fn is_hidden_or_system_dir(name: &str) -> bool {
    if name.starts_with('.') || name.starts_with('$') {
        return true;
    }
    matches!(
        name.to_ascii_lowercase().as_str(),
        "system volume information" | "recovery" | "perflogs" | "config.msi"
    )
}

fn breadcrumb_segments(path: &Path) -> Vec<(String, PathBuf)> {
    let mut out: Vec<(String, PathBuf)> = Vec::new();
    let mut acc = PathBuf::new();
    for comp in path.components() {
        match comp {
            std::path::Component::Prefix(p) => {
                acc.push(p.as_os_str());
                continue;
            }
            std::path::Component::RootDir => {
                acc.push(std::path::MAIN_SEPARATOR_STR);
                out.push((acc.to_string_lossy().into_owned(), acc.clone()));
                continue;
            }
            std::path::Component::CurDir => continue,
            std::path::Component::ParentDir => acc.push(".."),
            std::path::Component::Normal(s) => acc.push(s),
        }
        out.push((comp.as_os_str().to_string_lossy().into_owned(), acc.clone()));
    }
    out
}

fn nav_label(ui: &mut egui::Ui, label: &str, p: &Palette) {
    ui.add_sized(
        [52.0, 22.0],
        egui::Label::new(egui::RichText::new(label).small().color(p.dim)),
    );
}

/// Clean horizontal row of directory pills with a fixed-width left label column.
fn chip_row(
    ui: &mut egui::Ui,
    label: &str,
    items: &[(String, PathBuf)],
    id: &str,
    p: &Palette,
) -> Option<PathBuf> {
    let mut picked: Option<PathBuf> = None;
    ui.horizontal_top(|ui| {
        nav_label(ui, label, p);
        // Chips wrap to the next line instead of sliding behind a floating
        // bar, so no folder name is ever covered on hover. `push_id` keeps the
        // chip ids stable across frames.
        ui.push_id(id, |ui| {
            ui.spacing_mut().item_spacing.x = 5.0;
            ui.spacing_mut().button_padding = egui::vec2(8.0, 3.0);
            ui.horizontal_wrapped(|ui| {
                ui.add_space(2.0);
                for (text, path) in items {
                    let btn = egui::Button::new(
                        egui::RichText::new(text)
                            .monospace()
                            .size(11.5)
                            .color(p.text),
                    )
                    .rounding(egui::Rounding::same(6.0));
                    if ui.add(btn).clicked() {
                        picked = Some(path.clone());
                    }
                }
                ui.add_space(2.0);
            });
        });
    });
    picked
}

/// Vector Wise Owl & Search Lens brand mark painted in the toolbar. Returns the
/// slot it occupies, so a headless test can check it lines up with the title and
/// the path box on one centre axis.
fn paint_owl_lens_logo(ui: &mut egui::Ui, p: &Palette) -> egui::Rect {
    // The slot is as tall as the tallest control in the bar, so the row height is
    // fixed by the first item and every control after it is centered on the same
    // axis. The lens sits one pixel left of the slot centre to keep the handle
    // inside the slot, level with the SEEKSAGE title and the path box.
    let (rect, _) = ui.allocate_exact_size(egui::vec2(28.0, 28.0), egui::Sense::hover());
    let painter = ui.painter();
    let c = egui::pos2(rect.center().x - 1.0, rect.center().y);
    let r = 7.5;
    // Magnifying lens ring and handle
    painter.circle_filled(c, r, p.card);
    painter.circle_stroke(c, r, egui::Stroke::new(1.8_f32, p.accent));
    let handle_start = egui::pos2(c.x + 5.2, c.y + 5.2);
    let handle_end = egui::pos2(c.x + 9.2, c.y + 9.2);
    painter.line_segment(
        [handle_start, handle_end],
        egui::Stroke::new(2.4_f32, p.emerald),
    );
    // Owl eyes and golden beak inside the lens
    let left_eye = egui::pos2(c.x - 2.6, c.y - 0.8);
    let right_eye = egui::pos2(c.x + 2.6, c.y - 0.8);
    painter.circle_filled(left_eye, 2.1, p.accent);
    painter.circle_filled(right_eye, 2.1, p.accent);
    painter.circle_filled(left_eye, 0.9, p.canvas);
    painter.circle_filled(right_eye, 0.9, p.canvas);
    painter.circle_filled(egui::pos2(c.x, c.y + 2.2), 1.3, p.gold);
    rect
}

/// Compact square, icon-only Sun / Moon theme switcher. The glyph is drawn as
/// vectors, so it survives missing emoji glyphs, and the square keeps a clean
/// centre axis shared with the logo, the title and the single-line inputs.
fn theme_toggle_button(ui: &mut egui::Ui, dark_mode: bool, p: &Palette) -> egui::Response {
    let desired = egui::vec2(28.0, 28.0);
    let (rect, resp) = ui.allocate_exact_size(desired, egui::Sense::click());
    let fill = if resp.is_pointer_button_down_on() {
        p.selected_fill
    } else if resp.hovered() {
        p.raised
    } else {
        p.card
    };
    let edge = if resp.hovered() { p.accent } else { p.border };
    ui.painter().rect_filled(rect, 6.0, fill);
    ui.painter()
        .rect_stroke(rect, 6.0, egui::Stroke::new(1.0_f32, edge));

    // Icon-only, with the glyph centered in the square.
    let icon_c = rect.center();
    if dark_mode {
        // Moon: a full disc, with the crescent cutout nudged up and to the right
        // of the centre so the open side of the moon reads clearly.
        ui.painter().circle_filled(icon_c, 5.5, p.accent);
        ui.painter()
            .circle_filled(egui::pos2(icon_c.x + 2.2, icon_c.y - 1.5), 4.4, fill);
    } else {
        // Sun: a solid gold core inside a thin ray ring.
        ui.painter().circle_filled(icon_c, 4.0, p.gold);
        ui.painter()
            .circle_stroke(icon_c, 6.2, egui::Stroke::new(1.2_f32, p.gold));
    }
    resp
}

fn reveal_in_explorer(path: &Path) {
    let arg = format!("/select,{}", path.display());
    let _ = std::process::Command::new("explorer").arg(arg).spawn();
}

/// The target `code --goto` expects for one exact hit line, e.g.
/// `C:\src\a.rs:42`, so the editor opens with the cursor on that line.
fn editor_target(path: &Path, line: u32) -> String {
    format!("{}:{}", path.display(), line)
}

/// Spawn VS Code on the exact line. A missing `code` on the PATH is ignored,
/// the same posture as `reveal_in_explorer`: the button is a convenience, not
/// a promise that the external tool is installed.
fn open_in_editor(path: &Path, line: u32) {
    let target = editor_target(path, line);
    let _ = std::process::Command::new("code")
        .arg("--goto")
        .arg(target)
        .spawn();
}

/// `(file_id, line)` keys covered by the negative tokens, taken as exact term
/// postings: a `-token` excludes the lines where that exact term occurs, and
/// prefix widening stays on the positive side of the query.
fn collect_excluded(built: &Built, negative: &[String]) -> HashSet<(u32, u32)> {
    let mut out: HashSet<(u32, u32)> = HashSet::new();
    for t in negative {
        for p in built.index.term_postings(t) {
            out.insert((key_file(p.key), key_line(p.key)));
        }
    }
    out
}

fn paint_card(ui: &egui::Ui, rect: egui::Rect, fill: egui::Color32, edge: egui::Color32) {
    let painter = ui.painter();
    painter.rect_filled(rect, 6.0, fill);
    painter.rect_stroke(rect, 6.0, egui::Stroke::new(1.0_f32, edge));
}

/// Flush architectural panel frame (zero outer gaps, clean 1px border).
fn bar_frame(fill: egui::Color32, border: egui::Color32, pad_x: f32, pad_y: f32) -> egui::Frame {
    egui::Frame::default()
        .fill(fill)
        .stroke(egui::Stroke::new(1.0_f32, border))
        .rounding(egui::Rounding::ZERO)
        .inner_margin(egui::Margin::symmetric(pad_x, pad_y))
        .outer_margin(egui::Margin::ZERO)
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

/// Merge the context windows around the hit lines of one file.
///
/// Each hit covers `radius` lines above and below it. Windows that overlap,
/// touch, or nest into one another collapse into a single span, so packed
/// context never repeats a line. Lines are 1-based; the merged spans are
/// clamped at the top of the file.
fn merge_hit_ranges(hits: &[(u32, u64)], radius: usize) -> Vec<(i64, i64)> {
    let radius = radius as i64;
    let mut windows: Vec<(i64, i64)> = hits
        .iter()
        .map(|&(line, _)| (line as i64 - radius, line as i64 + radius))
        .collect();
    windows.sort_by_key(|&(start, _)| start);
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for (start, end) in windows {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    for span in &mut merged {
        if span.0 < 1 {
            span.0 = 1;
        }
    }
    merged
}

/// Name the scope a header line opens: `fn search()` from a Rust function,
/// `[Media Players]` from an INI section or a markdown heading, `struct Config`
/// from a type, `def load` from a Python function. `None` for an ordinary line.
fn scope_badge(line: &str, ext: &str) -> Option<String> {
    let t = line.trim();
    if t.is_empty() {
        return None;
    }
    // Section headers, INI style `[Name]`.
    if let Some(rest) = t.strip_prefix('[') {
        let inner = rest.split(']').next().unwrap_or("").trim();
        return (!inner.is_empty()).then(|| cap_badge(format!("[{inner}]")));
    }
    // A `#` line is only a heading in a markdown file. In ini, python, shell,
    // and toml files it is a comment, so it must not shadow the real `[Section]`
    // or `fn` header above it. A heading is 1 to 6 `#`, a space, then a title
    // that opens with an uppercase letter or digit and carries no `=`.
    if is_markdown(ext) && t.starts_with('#') {
        let hashes = t.chars().take_while(|&c| c == '#').count();
        if (1..=6).contains(&hashes) {
            let title = t[hashes..].trim_start();
            let opens_upper = title
                .chars()
                .next()
                .is_some_and(|c| c.is_uppercase() || c.is_ascii_digit());
            if opens_upper && !title.is_empty() && !title.contains('=') {
                return Some(cap_badge(format!("[{title}]")));
            }
        }
    }
    // Function scopes keep the signature, dropping any `pub` prefix.
    if let Some(sig) = t.strip_prefix("fn ").or_else(|| t.strip_prefix("pub fn ")) {
        let name = sig
            .split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .next()
            .unwrap_or("");
        return (!name.is_empty()).then(|| cap_badge(format!("fn {name}()")));
    }
    // Type and class scopes keep the keyword plus the first name.
    for kw in ["impl", "struct", "enum", "class", "def"] {
        if let Some(rest) = t.strip_prefix(kw) {
            if let Some(name) = rest.strip_prefix(char::is_whitespace).map(|r| {
                r.split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .next()
                    .unwrap_or("")
            }) {
                if !name.is_empty() {
                    return Some(cap_badge(format!("{kw} {name}")));
                }
            }
        }
    }
    None
}

/// A markdown heading is only a heading in `.md` / `.markdown` files.
fn is_markdown(ext: &str) -> bool {
    matches!(ext, "md" | "markdown")
}

/// Trim an over-long badge to fit one card line: keep 35 characters and add the
/// ellipsis, closing the `]` for a bracketed badge, e.g.
/// `[Mozilla/5.0 (Macintosh; U; PPC Ma…]`.
fn cap_badge(badge: String) -> String {
    if badge.chars().count() <= 36 {
        return badge;
    }
    if badge.starts_with('[') && badge.ends_with(']') {
        // Reserve one slot each for the brackets, so the inner text gets 33 and
        // the ellipsis lands before the closing bracket: `[inner…]`.
        let inner: String = badge[1..badge.len() - 1].chars().take(33).collect();
        format!("[{inner}…]")
    } else {
        let base: String = badge.chars().take(35).collect();
        format!("{base}…")
    }
}

/// Find the nearest enclosing scope header above a hit line.
///
/// The context is read top-down, so entries with a negative offset sit above
/// the hit. The scan walks upward from the line just before the hit and stops
/// at the first header; past the window the file is out of reach and no scope
/// is reported, leaving the row in its plain format.
///
/// A hit line that is itself a header gets no scope. Its own header is already
/// what the row shows, so the section above it is not an enclosing scope, and
/// reporting it would be a self-shadowing read.
fn detect_scope(context: &[(i32, String)], ext: &str) -> Option<String> {
    if context
        .iter()
        .any(|(rel, text)| *rel == 0 && scope_badge(text, ext).is_some())
    {
        return None;
    }
    for (rel, text) in context.iter().rev() {
        if *rel >= 0 {
            continue;
        }
        if let Some(badge) = scope_badge(text, ext) {
            return Some(badge);
        }
    }
    None
}

/// Short token count for the status bar: 500 stays 500, 1200 reads `1.2k`.
fn format_tokens(tokens: usize) -> String {
    if tokens >= 1000 {
        format!("{:.1}k", tokens as f64 / 1000.0)
    } else {
        tokens.to_string()
    }
}

// Inline `path:` filters ------------------------------------------------------

/// The shape a `path:` needle and a crawled file path are compared in:
/// lowercased, with every backslash folded to `/`, so a needle typed with `/`
/// matches a Windows path and one typed with `\` matches a POSIX path.
fn path_needle_key(raw: &str) -> String {
    raw.to_lowercase().replace('\\', "/")
}

/// One crawled file's path in the same shape `path:` needles are compared in.
fn path_match_key(path: &Path) -> String {
    path_needle_key(&path.display().to_string())
}

// Cross-file log timeline -----------------------------------------------------

/// Read exactly `n` ASCII digits from `i`, returning the value and the index
/// just past them, or `None` if any of the `n` slots is not a digit. Reading a
/// fixed width is what keeps `1234:56:78` from being read as a clock time.
fn read_digits(bytes: &[u8], i: usize, n: usize) -> Option<(u32, usize)> {
    let mut val = 0u32;
    let mut pos = i;
    for _ in 0..n {
        let b = *bytes.get(pos)?;
        if !b.is_ascii_digit() {
            return None;
        }
        val = val * 10 + u32::from(b - b'0');
        pos += 1;
    }
    Some((val, pos))
}

/// The optional `.sss` / `,sss` fraction after seconds, scaled to whole
/// milliseconds: `.7` is 700 ms, `.7891` keeps 789. A fraction sign with no
/// digit after it is not a fraction, so the position is returned untouched.
fn read_frac_ms(bytes: &[u8], i: usize) -> (u32, usize) {
    if !matches!(bytes.get(i), Some(&b'.') | Some(&b',')) {
        return (0, i);
    }
    let mut frac = 0u32;
    let mut pos = i + 1;
    let mut taken = 0;
    while taken < 3 {
        match bytes.get(pos) {
            Some(&b) if b.is_ascii_digit() => {
                frac = frac * 10 + u32::from(b - b'0');
                pos += 1;
                taken += 1;
            }
            _ => break,
        }
    }
    if taken == 0 {
        return (0, i);
    }
    for _ in taken..3 {
        frac *= 10;
    }
    (frac, pos)
}

/// `HH:MM:SS` plus an optional fraction, as milliseconds of that day. A time
/// out of range (hour 24, minute 60) is not a stamp.
fn read_time_ms(bytes: &[u8], i: usize) -> Option<(i64, usize)> {
    let (h, p1) = read_digits(bytes, i, 2)?;
    if bytes.get(p1) != Some(&b':') {
        return None;
    }
    let (m, p2) = read_digits(bytes, p1 + 1, 2)?;
    if bytes.get(p2) != Some(&b':') {
        return None;
    }
    let (s, p3) = read_digits(bytes, p2 + 1, 2)?;
    if h > 23 || m > 59 || s > 60 {
        return None;
    }
    let (ms, p4) = read_frac_ms(bytes, p3);
    Some((
        i64::from(h) * 3_600_000 + i64::from(m) * 60_000 + i64::from(s) * 1_000 + i64::from(ms),
        p4,
    ))
}

/// `YYYY-MM-DD` as the number of days since 1970-01-01, plus the index just
/// past the date. A month or day out of range is not a date.
fn read_date_days(bytes: &[u8], i: usize) -> Option<(i64, usize)> {
    let (y, p1) = read_digits(bytes, i, 4)?;
    if bytes.get(p1) != Some(&b'-') {
        return None;
    }
    let (mo, p2) = read_digits(bytes, p1 + 1, 2)?;
    if bytes.get(p2) != Some(&b'-') {
        return None;
    }
    let (d, p3) = read_digits(bytes, p2 + 1, 2)?;
    if mo < 1 || mo > 12 || d < 1 || d > 31 {
        return None;
    }
    Some((
        days_from_civil(i64::from(y), i64::from(mo), i64::from(d)),
        p3,
    ))
}

/// Days from 1970-01-01 in the proleptic Gregorian calendar, counting from a
/// March-based year so the leap day sits at the end of each year.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + yoe / 400 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Scan the front of a log snippet for a stamp and return it as one monotonic
/// millisecond value, so rows from many files can be compared with one `i64`.
///
/// Two shapes are recognized, the two that cover the bulk of real log lines:
/// - a full date-time `YYYY-MM-DD[T ]HH:MM:SS` with an optional `.sss` / `,sss`
///   fraction, turned into milliseconds since 1970-01-01, so stamps from
///   different days order against each other;
/// - a time-only `HH:MM:SS` with the same optional fraction at the start of the
///   line, including inside leading brackets such as `[12:34:56.789] INFO`,
///   read as milliseconds of that day.
///
/// Anything else reports `None`, so an unstamped line simply carries no badge.
fn parse_line_timestamp_ms(line: &str) -> Option<i64> {
    let bytes = line.as_bytes();
    let mut i = 0usize;
    // Leading wrappers: `[12:34:56.789] INFO ready`, `<12:34:56> INFO ready`.
    while matches!(bytes.get(i), Some(&b'[') | Some(&b'<')) {
        i += 1;
    }
    if let Some((days, after_date)) = read_date_days(bytes, i) {
        // A date alone is not a usable stamp; it needs a clock time behind it.
        if matches!(
            bytes.get(after_date),
            Some(&b'T') | Some(&b't') | Some(&b' ')
        ) {
            if let Some((time_ms, _)) = read_time_ms(bytes, after_date + 1) {
                return Some(days * 86_400_000 + time_ms);
            }
        }
        return None;
    }
    let (time_ms, _) = read_time_ms(bytes, i)?;
    Some(time_ms)
}

/// The gap badge: milliseconds under a second, seconds under a minute, minutes
/// beyond that, always with the leading `+` so the row reads as a step forward.
fn format_delta_ms(delta_ms: i64) -> String {
    if delta_ms == 0 {
        String::from("+0ms")
    } else if delta_ms < 1_000 {
        format!("+{delta_ms}ms")
    } else if delta_ms < 60_000 {
        format!("+{:.2}s", delta_ms as f64 / 1_000.0)
    } else {
        format!("+{:.1}m", delta_ms as f64 / 60_000.0)
    }
}

/// One FNV-1a step over the little-endian bytes of a value. FNV-1a is defined on
/// wrapping arithmetic, so the multiply wraps instead of trapping a debug build.
fn fnv_mix(hash: u64, value: u64) -> u64 {
    let mut h = hash;
    for byte in value.to_le_bytes() {
        h = (h ^ byte as u64).wrapping_mul(FNV_PRIME);
    }
    h
}

/// A fast FNV-1a fingerprint over the byte size and modified stamp of every path
/// in the indexed tree. One `metadata` call per file and no file contents read,
/// so a periodic watch check stays cheap on a tree of tens of thousands of files.
/// A rewrite that leaves the size alone still moves the hash, because the stamp
/// moves with it.
fn tree_fingerprint(paths: &[PathBuf]) -> u64 {
    // The number of paths goes in first, so a file added or removed changes the
    // fingerprint even when every other entry is untouched.
    let mut hash = fnv_mix(FNV_OFFSET, paths.len() as u64);
    for path in paths {
        let (len, mtime_ms) = match std::fs::metadata(path) {
            Ok(meta) => {
                let stamp = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok());
                (meta.len(), stamp.map(|d| d.as_millis() as u64).unwrap_or(0))
            }
            // A file that vanished between the build and the check has no
            // metadata, so it fingerprints as its own distinct value.
            Err(_) => (u64::MAX, 0),
        };
        hash = fnv_mix(hash, len);
        hash = fnv_mix(hash, mtime_ms);
    }
    hash
}

fn query_tokens(q: &str) -> Vec<String> {
    q.split_terminator(char::is_whitespace)
        .map(|t| t.to_lowercase())
        .filter(|t| !t.is_empty())
        .collect()
}

fn ext_tokens(raw: &str) -> Vec<String> {
    raw.split([' ', ',', ';', '|', '+'])
        .map(|t| t.trim_start_matches('.').to_lowercase())
        .filter(|t| !t.is_empty())
        .collect()
}

fn mono(size: f32, color: egui::Color32) -> egui::text::TextFormat {
    egui::text::TextFormat {
        font_id: FontId::monospace(size),
        color,
        ..Default::default()
    }
}

fn mono_bg(size: f32, color: egui::Color32, background: egui::Color32) -> egui::text::TextFormat {
    egui::text::TextFormat {
        font_id: FontId::monospace(size),
        color,
        background,
        ..Default::default()
    }
}

fn all_tokens_substring(lower: &str, tokens: &[String]) -> bool {
    tokens.iter().all(|t| lower.contains(t.as_str()))
}

fn append_highlight(
    job: &mut egui::text::LayoutJob,
    text: &str,
    tokens: &[String],
    base: egui::text::TextFormat,
    hit: egui::text::TextFormat,
) {
    if tokens.is_empty() {
        job.append(text, 0.0, base);
        return;
    }
    let lower = text.to_lowercase();
    if lower.len() != text.len() {
        job.append(text, 0.0, base);
        return;
    }
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for t in tokens {
        if t.is_empty() {
            continue;
        }
        let mut from = 0;
        while let Some(off) = lower[from..].find(t.as_str()) {
            let start = from + off;
            let end = start + t.len();
            if text.is_char_boundary(start) && text.is_char_boundary(end) {
                ranges.push((start, end));
            }
            from = end;
        }
    }
    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for (s, e) in ranges {
        if let Some(last) = merged.last_mut() {
            if s <= last.1 {
                last.1 = last.1.max(e);
                continue;
            }
        }
        merged.push((s, e));
    }
    let mut pos = 0usize;
    for (s, e) in merged {
        if s > pos {
            job.append(&text[pos..s], 0.0, base.clone());
        }
        job.append(&text[s..e], 0.0, hit.clone());
        pos = e;
    }
    if pos < text.len() {
        job.append(&text[pos..], 0.0, base);
    }
}

fn main() -> eframe::Result {
    let options = egui::ViewportBuilder::default()
        .with_title("SeekSage · Native Search Console")
        .with_inner_size([1280.0, 820.0])
        .with_min_inner_size([1020.0, 640.0])
        .with_resizable(true);
    eframe::run_native(
        "SeekSage",
        eframe::NativeOptions {
            viewport: options,
			centered: true,
            ..Default::default()
        },
        Box::new(|_cc| Ok(Box::new(SeekSageApp::default()))),
    )
}

// Headless tests --------------------------------------------------------------

#[cfg(test)]
mod tests {
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

    fn write_file(path: &Path, bytes: &[u8]) {
        std::fs::write(path, bytes).expect("write fixture");
    }

    fn drain(app: &mut SeekSageApp, ctx: &egui::Context, budget: u32) {
        let mut polls = 0;
        while app.phase == Phase::Indexing && polls < budget {
            app.poll_build(ctx);
            polls += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(polls < budget, "the request never reported back");
    }

    fn drain_export(app: &mut SeekSageApp, ctx: &egui::Context, budget: u32) {
        let mut polls = 0;
        while app.export_rx.is_some() && polls < budget {
            app.poll_export(ctx);
            polls += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(polls < budget, "the export never reported back");
    }

    /// Queue one key press with the given modifiers, replacing everything the
    /// input state held before, so each hotkey test sees exactly one press.
    fn press_key(ctx: &egui::Context, key: egui::Key, m: egui::Modifiers) {
        ctx.input_mut(|i| {
            i.events.clear();
            i.modifiers = m;
            i.events.push(egui::Event::Key {
                key,
                physical_key: None,
                pressed: true,
                repeat: false,
                modifiers: m,
            });
        });
    }

    /// Run one full frame of the real search bar, delivering one key press with
    /// the given modifiers, so the history gestures are tested through the same
    /// panel the app paints on screen. `focused` says who owns the keyboard:
    /// egui applies focus at the end of a frame, so a test warms the box up on
    /// one frame before it presses the keys it cares about.
    fn paint_searchbar_with_key(
        ctx: &egui::Context,
        app: &mut SeekSageApp,
        key: egui::Key,
        m: egui::Modifiers,
        focused: bool,
    ) {
        let box_id = egui::Id::from("search_box");
        if !focused {
            ctx.memory_mut(|mem| mem.surrender_focus(box_id));
        }
        let mut raw = egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                egui::vec2(1200.0, 700.0),
            )),
            modifiers: m,
            ..Default::default()
        };
        raw.events.push(egui::Event::Key {
            key,
            physical_key: None,
            pressed: true,
            repeat: false,
            modifiers: m,
        });
        let _ = ctx.run(raw, |ctx| {
            if focused {
                ctx.memory_mut(|mem| mem.request_focus(box_id));
            }
            app.paint_searchbar(ctx);
        });
    }

    #[test]
    fn a_failed_request_leaves_the_next_one_clean() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-worker");
        write_file(&root.join("a.rs"), b"pub fn index_file() {}");
        let mut app = SeekSageApp {
            build_rx: Some(spawn_indexer(
                root.join("missing"),
                4,
                Arc::new(LiveProgress::default()),
            )),
            phase: Phase::Indexing,
            build_start: Some(Instant::now()),
            ..Default::default()
        };
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Failed, "status: {}", app.status);
        assert!(
            app.status.starts_with("index failed"),
            "status: {}",
            app.status
        );

        app.root_text = root.display().to_string();
        app.start_build();
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);
        let Some(built) = app.built.as_ref() else {
            panic!("the second request must produce an index");
        };
        assert_eq!(built.walk.files_matched, 1);
        assert!(
            built.lane_errors.is_empty(),
            "lane errors: {:?}",
            built.lane_errors
        );
        app.query = String::from("index_file");
        app.run_search();
        assert_eq!(app.view.len(), 1);
        assert_eq!(app.selected, 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_panicking_request_does_not_break_the_next_one() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-panic");
        write_file(&root.join("a.rs"), b"pub fn index_file() {}");
        let mut app = SeekSageApp {
            build_rx: Some(spawn_request(move || -> io::Result<(Built, bool)> {
                panic!("simulated bad lane");
            })),
            phase: Phase::Indexing,
            build_start: Some(Instant::now()),
            ..Default::default()
        };
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Failed, "status: {}", app.status);
        assert!(
            app.status.contains("simulated bad lane"),
            "status: {}",
            app.status
        );

        app.build_rx = Some(spawn_indexer(
            root.clone(),
            4,
            Arc::new(LiveProgress::default()),
        ));
        app.phase = Phase::Indexing;
        app.build_start = Some(Instant::now());
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);
        let Some(built) = app.built.as_ref() else {
            panic!("the next request must produce an index");
        };
        assert_eq!(built.walk.files_matched, 1);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_missing_root_is_reported_through_the_app() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-missing");
        let mut app = SeekSageApp {
            build_rx: Some(spawn_indexer(
                root.join("nope"),
                4,
                Arc::new(LiveProgress::default()),
            )),
            phase: Phase::Indexing,
            build_start: Some(Instant::now()),
            ..Default::default()
        };
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Failed, "status: {}", app.status);
        assert!(
            app.status.starts_with("index failed"),
            "status: {}",
            app.status
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_directory_argument_becomes_the_first_index() {
        let root = tmp_root("gui-argument");
        write_file(&root.join("a.md"), b"# index_file");
        let args = vec![String::from("seeksage-gui"), root.display().to_string()];
        assert_eq!(startup_root(&args), Some(root.clone()));
        assert_eq!(startup_root(&[String::from("seeksage-gui")]), None);
        assert_eq!(
            startup_root(&[
                String::from("seeksage-gui"),
                root.join("nope").display().to_string()
            ]),
            None
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn breadcrumbs_are_clickable_prefixes() {
        let segs = breadcrumb_segments(Path::new(r"C:\Program Files\Windows Kits\10\Include"));
        let labels: Vec<&str> = segs.iter().map(|(l, _)| l.as_str()).collect();
        assert_eq!(
            labels,
            ["C:\\", "Program Files", "Windows Kits", "10", "Include"]
        );
        assert_eq!(segs[0].1, PathBuf::from(r"C:\"));
        assert_eq!(segs[1].1, PathBuf::from(r"C:\Program Files"));
        assert_eq!(
            segs.last().expect("last chip").1,
            PathBuf::from(r"C:\Program Files\Windows Kits\10\Include")
        );
    }

    #[test]
    fn extension_filter_accepts_the_documented_forms() {
        assert_eq!(
            ext_tokens(".rs, .h;.ini|toml +C++"),
            ["rs", "h", "ini", "toml", "c"]
        );
        assert_eq!(ext_tokens("rs"), ["rs"]);
        assert!(ext_tokens("   ").is_empty());
    }

    #[test]
    fn the_view_and_selection_follow_the_ext_filter() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-view");
        write_file(&root.join("a.rs"), b"pub fn index_file() {}");
        write_file(&root.join("b.h"), b"int index_file(void);");
        write_file(&root.join("c.ini"), b"index_file = 1");

        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);

        app.query = String::from("index_file");
        app.run_search();
        assert_eq!(app.view.len(), 3);
        assert_eq!(app.matched_files, 3);

        app.ext_filter = String::from(".rs .h");
        app.rebuild_view();
        assert_eq!(app.view.len(), 2);
        assert_eq!(app.selected, 0);
        for i in 0..app.view.len() {
            app.selected = i;
            app.pending_scroll = Some(i);
        }
        assert_eq!(app.pending_scroll, Some(1));

        app.selected = 99;
        app.rebuild_view();
        assert_eq!(app.selected, 0);

        app.ext_filter = String::new();
        app.rebuild_view();
        assert_eq!(app.view.len(), 3);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn snippet_cache_stays_capped() {
        let root = tmp_root("gui-snippets");
        let path = root.join("a.rs");
        write_file(&path, b"line one\nline two\n");
        let mut cache = SnippetCache::default();
        for i in 0..(SNIPPET_CAP + 64) as u32 {
            cache.get(&path, i, 0, 0);
        }
        assert_eq!(cache.map.len(), SNIPPET_CAP);
        assert_eq!(cache.order.len(), SNIPPET_CAP);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn an_export_writes_exactly_the_filtered_view() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-export");
        write_file(
            &root.join("a.rs"),
            b"pub fn index_file() {}\nsecond index_file line\n",
        );
        write_file(&root.join("b.h"), b"int index_file(void);");
        write_file(&root.join("c.ini"), b"index_file = 1");

        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        app.query = String::from("index_file");
        app.run_search();
        // Two matching lines in `a.rs`, one in `b.h`, one in `c.ini`.
        assert_eq!(app.view.len(), 4);
        assert_eq!(app.matched_files, 3);

        // A CSV export writes exactly the rows the extension filter leaves in
        // the view: the header, then one row per visible line.
        app.ext_filter = String::from(".rs");
        app.rebuild_view();
        assert_eq!(app.view.len(), 2);
        let csv = root.join("rs.csv");
        app.start_export(csv.clone(), ExportFormat::Csv);
        drain_export(&mut app, &ctx, 5000);
        assert!(
            app.status.starts_with("export wrote"),
            "status: {}",
            app.status
        );
        let text = std::fs::read_to_string(&csv).expect("read csv");
        let rows: Vec<&str> = text.lines().collect();
        assert_eq!(rows[0], "File,Line,Snippet");
        assert_eq!(rows.len(), 1 + app.view.len());
        assert!(rows[1].contains(".rs"), "row: {}", rows[1]);
        assert!(
            !text.contains(".h") && !text.contains(".ini"),
            "csv: {text}"
        );

        // JSON covers the whole view, and carries the byte offset the panel
        // shows alongside the snippet.
        app.ext_filter = String::new();
        app.rebuild_view();
        let json = root.join("all.json");
        app.start_export(json.clone(), ExportFormat::Json);
        drain_export(&mut app, &ctx, 5000);
        let text = std::fs::read_to_string(&json).expect("read json");
        assert!(text.contains("\"byte_offset\""), "json: {text}");
        assert!(text.contains("\"file_path\""));
        assert!(text.contains(".ini"));
        let summary = app.export_summary.expect("the summary sticks after Done");
        assert_eq!(summary.rows, app.view.len());

        // A failure inside the worker is reported as a status line, never as a
        // crash, and releases the worker slot for the next export.
        app.start_export(root.join("gone").join("x.md"), ExportFormat::Markdown);
        drain_export(&mut app, &ctx, 5000);
        assert!(
            app.status.starts_with("export failed"),
            "status: {}",
            app.status
        );
        assert!(app.export_rx.is_none(), "the worker slot must be released");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn an_export_refuses_an_empty_view_and_a_nameless_path() {
        let mut app = SeekSageApp::default();
        app.start_export(PathBuf::from(r"C:\"), ExportFormat::Csv);
        assert!(
            app.status.contains("nothing to export"),
            "status: {}",
            app.status
        );
        assert!(app.export_rx.is_none());

        app.uniq.push((0, 0, 0));
        app.view.push(0);
        app.start_export(PathBuf::from(r"C:\"), ExportFormat::Csv);
        assert!(
            app.status.contains("no file name"),
            "status: {}",
            app.status
        );
        assert!(app.export_rx.is_none(), "nothing may start writing");
    }

    #[test]
    fn the_format_choice_keeps_the_file_name_in_step() {
        let mut app = SeekSageApp::default();
        app.uniq.push((0, 0, 0));
        app.view.push(0);
        app.open_export_panel();
        assert!(
            app.export_path.ends_with(".json"),
            "path: {}",
            app.export_path
        );
        app.set_export_format(ExportFormat::Csv);
        assert!(
            app.export_path.ends_with(".csv"),
            "path: {}",
            app.export_path
        );

        app.export_path = String::from(r"D:\reports\out.md");
        app.set_export_format(ExportFormat::Json);
        assert_eq!(app.export_path, r"D:\reports\out.json");
        assert_eq!(
            ExportFormat::for_path(Path::new(r"D:\x.csv"), ExportFormat::Json),
            ExportFormat::Csv
        );
    }

    #[test]
    fn ctrl_f_focuses_the_search_box_from_anywhere() {
        let ctx = egui::Context::default();
        let mut app = SeekSageApp::default();
        press_key(&ctx, egui::Key::F, egui::Modifiers::COMMAND);
        app.handle_hotkeys(&ctx);
        assert_eq!(
            ctx.memory(|mem| mem.focused()),
            Some(egui::Id::from("search_box"))
        );

        // Plain typing in a field must never fire the shortcut.
        ctx.memory_mut(|mem| mem.surrender_focus(egui::Id::from("search_box")));
        press_key(&ctx, egui::Key::F, egui::Modifiers::NONE);
        app.handle_hotkeys(&ctx);
        assert_eq!(ctx.memory(|mem| mem.focused()), None);
    }

    #[test]
    fn ctrl_shift_e_toggles_the_export_panel() {
        let ctx = egui::Context::default();
        let mut app = SeekSageApp::default();
        let combo = egui::Modifiers::COMMAND.plus(egui::Modifiers::SHIFT);

        // With nothing visible, the request is refused in words, not by opening
        // a panel with no rows to write.
        press_key(&ctx, egui::Key::E, combo);
        app.handle_hotkeys(&ctx);
        assert!(!app.export_open);
        assert!(
            app.status.contains("nothing to export"),
            "status: {}",
            app.status
        );

        app.uniq.push((0, 0, 0));
        app.view.push(0);
        press_key(&ctx, egui::Key::E, combo);
        app.handle_hotkeys(&ctx);
        assert!(app.export_open);
        assert!(
            app.export_path.ends_with(".json"),
            "path: {}",
            app.export_path
        );

        // The same shortcut closes it again, so one key carries both directions.
        press_key(&ctx, egui::Key::E, combo);
        app.handle_hotkeys(&ctx);
        assert!(!app.export_open);

        // Ctrl+E alone is not the export shortcut: it must not open the panel.
        app.export_path = String::new();
        press_key(&ctx, egui::Key::E, egui::Modifiers::COMMAND);
        app.handle_hotkeys(&ctx);
        assert!(!app.export_open);
    }

    #[test]
    fn escape_unwinds_one_layer_at_a_time() {
        let ctx = egui::Context::default();
        let mut app = SeekSageApp::default();

        // Layer 1: the export panel closes before any text field is touched.
        app.uniq.push((0, 0, 0));
        app.view.push(0);
        app.open_export_panel();
        ctx.memory_mut(|mem| mem.request_focus(egui::Id::from("search_box")));
        app.query = String::from("index_file");
        press_key(&ctx, egui::Key::Escape, egui::Modifiers::NONE);
        app.handle_hotkeys(&ctx);
        assert!(!app.export_open);
        assert_eq!(app.query, "index_file");
        assert_eq!(
            ctx.memory(|mem| mem.focused()),
            Some(egui::Id::from("search_box"))
        );

        // Layer 2: Escape on the focused search box clears the query and keeps
        // focus, so a second Escape is what releases the field.
        app.run_search();
        press_key(&ctx, egui::Key::Escape, egui::Modifiers::NONE);
        app.handle_hotkeys(&ctx);
        assert!(app.query.is_empty());
        assert_eq!(
            ctx.memory(|mem| mem.focused()),
            Some(egui::Id::from("search_box"))
        );
        press_key(&ctx, egui::Key::Escape, egui::Modifiers::NONE);
        app.handle_hotkeys(&ctx);
        assert_eq!(ctx.memory(|mem| mem.focused()), None);

        // Layer 3: Escape on the focused extension box clears the filter.
        ctx.memory_mut(|mem| mem.request_focus(egui::Id::from("ext_filter")));
        app.ext_filter = String::from(".rs");
        app.rebuild_view();
        press_key(&ctx, egui::Key::Escape, egui::Modifiers::NONE);
        app.handle_hotkeys(&ctx);
        assert!(app.ext_filter.is_empty());

        // Escape from nowhere clears both filters the way the two "×" buttons do
        // and releases focus from every text field.
        app.query = String::from("index_file");
        app.ext_filter = String::from(".rs");
        ctx.memory_mut(|mem| mem.request_focus(egui::Id::from("path_box")));
        press_key(&ctx, egui::Key::Escape, egui::Modifiers::NONE);
        app.handle_hotkeys(&ctx);
        assert!(app.query.is_empty());
        assert!(app.ext_filter.is_empty());
        assert_eq!(ctx.memory(|mem| mem.focused()), None);
    }

    /// Drive exactly one headless frame: paint `paint` into an isolated root ui
    /// and, when `at` is given, press and release the primary button at that
    /// point. Returns the widget's response, so a test can read `clicked()`.
    fn headless_frame(
        ctx: &egui::Context,
        at: Option<egui::Pos2>,
        mut paint: impl FnMut(&mut egui::Ui) -> egui::Response,
    ) -> egui::Response {
        let mut raw = egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                egui::vec2(1200.0, 700.0),
            )),
            ..Default::default()
        };
        if let Some(pos) = at {
            raw.events.push(egui::Event::PointerMoved(pos));
            for pressed in [true, false] {
                raw.events.push(egui::Event::PointerButton {
                    pos,
                    button: egui::PointerButton::Primary,
                    pressed,
                    modifiers: egui::Modifiers::NONE,
                });
            }
        }
        let mut captured: Option<egui::Response> = None;
        let _ = ctx.run(raw, |ctx| {
            let mut ui = egui::Ui::new(
                ctx.clone(),
                egui::LayerId::new(egui::Order::Middle, egui::Id::from("probe_ui")),
                egui::Id::from("probe_ui"),
                egui::UiBuilder::new().max_rect(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(1200.0, 40.0),
                )),
            );
            captured = Some(paint(&mut ui));
        });
        captured.expect("the probe ui painted its widget")
    }

    #[test]
    fn the_progress_bar_measures_real_files_ingested() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-progress");
        // One fat file plus one small one: the ingest stage runs long enough for
        // the UI to observe it, and emits far more than PROGRESS_STEP chunks.
        let mut big = String::with_capacity(4_000_000);
        for i in 0..120000 {
            big.push_str(&format!("pub fn index_file_{i:05}() {{}}\n"));
        }
        write_file(&root.join("big.rs"), big.as_bytes());
        write_file(&root.join("tiny.h"), b"void index_file(void);\n");

        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            lanes: 4,
            ..Default::default()
        };
        app.start_rebuild();
        assert_eq!(app.phase, Phase::Indexing, "status: {}", app.status);

        let mut stages: Vec<BuildStage> = Vec::new();
        let mut ingest_line = String::new();
        let mut polls = 0;
        while app.phase == Phase::Indexing && polls < 5000 {
            app.poll_build(&ctx);
            polls += 1;
            let snap = app.progress;
            if !stages.contains(&snap.stage) {
                stages.push(snap.stage);
            }
            if snap.stage == BuildStage::Ingest {
                ingest_line = snap.detail(app.lanes);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(polls < 5000, "the build never reported back");

        assert_eq!(app.progress.stage, BuildStage::Done, "stages: {stages:?}");
        assert_eq!(
            app.progress.total, 2,
            "both discovered files are the percent base"
        );
        assert_eq!(
            app.progress.done, app.progress.total,
            "every file ends up processed"
        );
        assert_eq!(app.progress.fraction(), 1.0);
        assert!(app.progress.percent() > 99.0);
        assert!(
            stages.contains(&BuildStage::Ingest),
            "the ingest stage must be observable: {stages:?} · polls {polls} · build {:.1} ms · chunks {} · total {}",
            app.build_ms,
            app.progress.chunks,
            app.progress.total
        );
        assert!(
            app.progress.chunks > PROGRESS_STEP as usize,
            "chunks drained: {}",
            app.progress.chunks
        );
        assert!(
            ingest_line.contains("ingesting"),
            "the status line must read as text: {ingest_line}"
        );
        let shared = app.live.snapshot();
        assert_eq!(
            shared.done, app.progress.done,
            "the UI reads the worker's counters"
        );
        assert_eq!(shared.stage, BuildStage::Done);

        // Half-way ingest is half of the ingest slice, not a guess.
        let mid = Progress {
            stage: BuildStage::Ingest,
            total: 1000,
            done: 500,
            ..Default::default()
        };
        assert!(
            (mid.percent() - 61.0).abs() < 0.05,
            "percent: {}",
            mid.percent()
        );
        assert!(
            mid.detail(4).contains("ingesting 500 of 1000 files"),
            "detail: {}",
            mid.detail(4)
        );

        // Stage floors are monotonic, so the bar never moves backwards.
        let mut prev = -1.0;
        for s in [
            BuildStage::Queued,
            BuildStage::Cache,
            BuildStage::Walk,
            BuildStage::Ingest,
            BuildStage::Merge,
            BuildStage::Done,
        ] {
            assert!(s.floor() > prev, "stage {} floor regressed", s.word());
            assert!(
                s.ceiling() >= s.floor(),
                "stage {} range is inverted",
                s.word()
            );
            prev = s.floor();
        }
        assert!(BuildStage::Merge.floor() > BuildStage::Ingest.ceiling());
        assert_eq!(
            Progress {
                stage: BuildStage::Queued,
                ..Default::default()
            }
            .detail(4),
            "queued · 4 lanes"
        );
        assert_eq!(
            Progress {
                stage: BuildStage::Merge,
                shards: 4,
                ..Default::default()
            }
            .detail(4),
            "merging 4 shards on 4 lanes"
        );

        // The second build reads the disk cache the worker wrote, and the progress
        // run ends complete on the very first snapshot.
        app.start_build();
        assert_eq!(app.phase, Phase::Indexing);
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);
        assert!(app.status.contains("disk cache"), "status: {}", app.status);
        assert_eq!(app.progress.stage, BuildStage::Done);
        assert_eq!(app.progress.total, 2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_results_header_reads_as_a_plain_sentence() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-header");
        write_file(
            &root.join("a.rs"),
            b"pub fn index_file() {}\nfn index_file_is_fast() {}\n",
        );
        write_file(&root.join("b.h"), b"void index_file(void);\n");
        write_file(&root.join("c.ini"), b"index_file=1\n");
        let mut app = SeekSageApp {
            build_rx: Some(spawn_rebuild(
                root.clone(),
                4,
                Arc::new(LiveProgress::default()),
            )),
            phase: Phase::Indexing,
            build_start: Some(Instant::now()),
            ..Default::default()
        };
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);

        // Every hit, unfiltered: one count, one file count, one timing.
        app.query = String::from("index_file");
        app.run_search();
        assert_eq!(
            app.uniq.len(),
            4,
            "two .rs lines, one .h line, one .ini line"
        );
        let h = app.header_text();
        assert!(
            h.starts_with("Found 4 matches across 3 files ("),
            "header: {h}"
        );
        assert!(h.ends_with(')'), "header: {h}");

        // A narrowing ext filter names both sides of the narrowing.
        app.ext_filter = String::from(".rs");
        app.rebuild_view();
        let h = app.header_text();
        assert!(
            h.starts_with("Found 2 of 4 matches across 1 of 3 files · "),
            "header: {h}"
        );
        assert!(h.contains(" hits ("), "header: {h}");

        // The summary is a sentence the header repaints every frame, so a
        // cleared filter shows up in it immediately.
        app.ext_filter = String::new();
        app.rebuild_view();
        assert!(app
            .header_text()
            .starts_with("Found 4 matches across 3 files"));

        // Units: sub-millisecond timings stay in µs, the rest speak as ms.
        assert_eq!(human_duration_us(800.0), "800.0 µs");
        assert_eq!(human_duration_us(1000.0), "1.0 ms");
        assert_eq!(human_duration_us(1234.0), "1.2 ms");

        // An empty query is an honest zero, not a placeholder string.
        app.clear_query();
        assert_eq!(app.view.len(), 0);
        assert!(
            app.header_text()
                .starts_with("Found 0 matches across 0 files ("),
            "header: {}",
            app.header_text()
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_clear_controls_empty_their_boxes_and_rebuild_instantly() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-clear");
        write_file(
            &root.join("a.rs"),
            b"pub fn index_file() {}\nfn index_file_is_fast() {}\n",
        );
        write_file(&root.join("b.h"), b"void index_file(void);\n");
        let mut app = SeekSageApp {
            build_rx: Some(spawn_rebuild(
                root.clone(),
                4,
                Arc::new(LiveProgress::default()),
            )),
            phase: Phase::Indexing,
            build_start: Some(Instant::now()),
            ..Default::default()
        };
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);

        app.query = String::from("index_file");
        app.ext_filter = String::from(".rs");
        app.run_search();
        assert_eq!(app.uniq.len(), 3);
        assert_eq!(app.view.len(), 2, "the ext filter narrows the view to .rs");

        // A filled box makes the "×" a live control, and clicking it empties the
        // box, rebuilds the view and repaints the header counter.
        let rect = headless_frame(&ctx, None, |ui| {
            let p = app.palette();
            clear_button(ui, !app.ext_filter.is_empty(), &p, "× clear the ext filter")
        })
        .rect;
        let resp = headless_frame(&ctx, Some(rect.center()), |ui| {
            let p = app.palette();
            let resp = clear_button(ui, !app.ext_filter.is_empty(), &p, "× clear the ext filter");
            if resp.clicked() {
                app.clear_ext_filter();
            }
            resp
        });
        assert!(
            resp.clicked(),
            "the ext × must be clickable when the box has text"
        );
        assert!(app.ext_filter.is_empty(), "the × empties its own box");
        assert_eq!(
            app.view.len(),
            3,
            "the view rebuilds the moment the filter is gone"
        );

        // The query "×" is the same contract on the other box.
        let rect = headless_frame(&ctx, None, |ui| {
            let p = app.palette();
            clear_button(ui, !app.query.is_empty(), &p, "× clear the search box")
        })
        .rect;
        let resp = headless_frame(&ctx, Some(rect.center()), |ui| {
            let p = app.palette();
            let resp = clear_button(ui, !app.query.is_empty(), &p, "× clear the search box");
            if resp.clicked() {
                app.clear_query();
            }
            resp
        });
        assert!(
            resp.clicked(),
            "the query × must be clickable when the box has text"
        );
        assert!(app.query.is_empty());
        assert_eq!(
            app.view.len(),
            0,
            "clearing the query rebuilds the results in the same repaint"
        );

        // An empty box keeps its "×" quiet: greyed out, and not clickable.
        let quiet = headless_frame(&ctx, Some(rect.center()), |ui| {
            let p = app.palette();
            clear_button(ui, !app.query.is_empty(), &p, "× clear the search box")
        });
        assert!(
            !quiet.clicked(),
            "an empty box must not read as a live control"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_query_history_is_bounded_to_the_last_ten_commits() {
        let mut app = SeekSageApp::default();
        for i in 0..12 {
            app.query = format!("term{i}");
            app.commit_query();
        }
        assert_eq!(app.query_history.len(), HISTORY_CAP);
        assert_eq!(
            app.query_history.back().unwrap(),
            "term11",
            "newest sits at the back"
        );
        assert_eq!(
            app.query_history.front().unwrap(),
            "term2",
            "the oldest two fell off"
        );

        // A re-committed query refreshes its slot instead of duplicating.
        app.query = String::from("term2");
        app.commit_query();
        assert_eq!(app.query_history.len(), HISTORY_CAP);
        assert_eq!(app.query_history.back().unwrap(), "term2");
        assert_eq!(
            app.query_history.iter().filter(|q| *q == "term2").count(),
            1
        );

        // Empty and whitespace-only commits are never recorded.
        app.query = String::from("   ");
        app.commit_query();
        assert_eq!(app.query_history.len(), HISTORY_CAP);

        // Alt+Up from live typing starts at the newest commit, walks to older
        // ones, and Alt+Down back past the newest restores the typed text.
        app.query = String::from("typing");
        app.history_cycle(true);
        assert_eq!(
            app.query, "term2",
            "the refreshed term2 is now the newest commit"
        );
        app.history_cycle(true);
        assert_eq!(app.query, "term11");
        app.history_cycle(false);
        assert_eq!(app.query, "term2");
        app.history_cycle(false);
        assert_eq!(
            app.query, "typing",
            "Alt+Down past the newest restores the live text"
        );
        assert_eq!(app.history_pos, None);
        app.history_cycle(false);
        assert_eq!(
            app.query, "typing",
            "nothing newer than the live text exists to walk to"
        );

        // Walking down to the oldest entry: the walk ends at index 0, and the
        // oldest entry is a wall, not a wrap.
        for _ in 0..HISTORY_CAP {
            app.history_cycle(true);
        }
        assert_eq!(app.history_pos, Some(0));
        assert_eq!(
            app.query, "term3",
            "the two oldest commits fell off the cap"
        );
        app.history_cycle(true);
        assert_eq!(app.query, "term3", "the oldest entry is a wall, not a wrap");
        app.history_cycle(false);
        assert_eq!(app.query, "term4");
        for _ in 0..(HISTORY_CAP - 2) {
            app.history_cycle(false);
        }
        assert_eq!(app.history_pos, Some(HISTORY_CAP - 1));
        app.history_cycle(false);
        assert_eq!(
            app.query, "typing",
            "the walk ends where it began, on the live text"
        );
    }

    #[test]
    fn enter_in_the_search_box_commits_the_typed_query() {
        let ctx = egui::Context::default();
        let mut app = SeekSageApp::default();
        let none = egui::Modifiers::NONE;

        // Warm-up frame: the box takes focus the way Ctrl+F gives it in the app.
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::Space, none, true);

        app.query = String::from("index_file");
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::Enter, none, true);
        assert_eq!(app.query_history.len(), 1, "Enter commits what is typed");
        assert_eq!(app.query_history.back().unwrap(), "index_file");

        // A second Enter on the same text dedupes; Enter on empty text records
        // nothing.
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::Enter, none, true);
        assert_eq!(
            app.query_history.len(),
            1,
            "a repeat Enter never duplicates a commit"
        );
        app.query = String::new();
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::Enter, none, true);
        assert_eq!(
            app.query_history.len(),
            1,
            "an empty query is never recorded"
        );

        // Enter belongs to the focused box, not to the search bar at large.
        app.query = String::from("other");
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::Enter, none, false);
        assert_eq!(
            app.query_history.len(),
            1,
            "Enter belongs to the focused box, not to the search bar at large"
        );
    }

    #[test]
    fn alt_arrows_walk_the_history_in_the_search_bar_and_run_each_query() {
        let ctx = egui::Context::default();
        let none = egui::Modifiers::NONE;
        let alt = egui::Modifiers {
            alt: true,
            ..Default::default()
        };
        let root = tmp_root("gui-history");
        write_file(
            &root.join("a.rs"),
            b"fn alpha() {}\nfn beta() {}\nfn index_file() {}\n",
        );
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            lanes: 4,
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);

        // Warm-up frame: the box takes focus the way Ctrl+F gives it in the app.
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::Space, none, true);

        // Two committed queries, entered through the box exactly as a user does.
        app.query = String::from("beta");
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::Enter, none, true);
        app.query = String::from("alpha");
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::Enter, none, true);
        assert_eq!(app.query_history.len(), 2);
        assert_eq!(app.query, "alpha");

        // Alt+Up: newest first, then older. The search runs on each step, so the
        // results follow the history as it cycles.
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::ArrowUp, alt, true);
        assert_eq!(app.query, "alpha");
        assert_eq!(app.view.len(), 1, "the cycled query runs immediately");
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::ArrowUp, alt, true);
        assert_eq!(app.query, "beta");
        assert_eq!(app.view.len(), 1);

        // Alt+Down walks back to newer, and past the newest restores the live
        // text the user was typing when the walk began.
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::ArrowDown, alt, true);
        assert_eq!(app.query, "alpha");
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::ArrowDown, alt, true);
        assert_eq!(
            app.query, "alpha",
            "the live text was alpha when the walk began"
        );
        assert_eq!(app.history_pos, None);

        // Plain arrows belong to the row selection, not to the query, and
        // Alt+arrows belong to the search box only while it has focus.
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::ArrowDown, none, true);
        assert_eq!(
            app.query, "alpha",
            "plain ArrowDown never touches the query text"
        );
        paint_searchbar_with_key(&ctx, &mut app, egui::Key::ArrowUp, alt, false);
        assert_eq!(
            app.query, "alpha",
            "history cycling is the search box's own gesture"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_minus_token_excludes_the_lines_matching_that_term() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-negation");
        write_file(
            &root.join("a.rs"),
            b"index_file ok\nindex_file error\npub fn index_file() {}\n",
        );
        write_file(&root.join("b.h"), b"int index_file(void); // error flag\n");
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            lanes: 4,
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);

        // Four lines match `index_file`; the term `error` drops the two of them
        // that carry it, so what survives is a.rs lines 1 and 3, both in one file.
        app.query = String::from("index_file -error");
        app.run_search();
        assert_eq!(
            app.tokens,
            vec![String::from("index_file")],
            "the positive tokens stay the driver of badges and highlight"
        );
        assert_eq!(app.uniq.len(), 2, "-error excludes its two lines");
        assert_eq!(app.view.len(), 2);
        assert_eq!(app.matched_files, 1, "both survivors sit in a.rs");

        app.query = String::from("index_file -fn");
        app.run_search();
        assert_eq!(app.uniq.len(), 3, "-fn drops only the pub fn line");

        // Negation reads exact terms, not the prefix widening the positive side
        // gets: `-err` finds no term `err`, so nothing is excluded.
        app.query = String::from("index_file -err");
        app.run_search();
        assert_eq!(app.uniq.len(), 4);

        // Negatives alone have no positive base to filter, so they find nothing.
        app.query = String::from("-error");
        app.run_search();
        assert_eq!(app.uniq.len(), 0);

        // The whole query string, negatives included, goes into history.
        app.commit_query();
        assert_eq!(app.query_history.back().unwrap(), "-error");

        // The ext filter composes with the negation: `.rs` keeps both surviving
        // a.rs lines, `.h` leaves nothing after `-error` removed b.h's only line.
        app.query = String::from("index_file -error");
        app.ext_filter = String::from(".rs");
        app.run_search();
        assert_eq!(app.view.len(), 2, "both survivors are .rs lines");
        app.ext_filter = String::from(".h");
        app.run_search();
        assert_eq!(app.view.len(), 0, "the .h line was excluded by -error");
        app.ext_filter.clear();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn open_in_editor_points_the_external_command_at_the_exact_line() {
        assert_eq!(
            editor_target(Path::new(r"C:\src\a.rs"), 42),
            r"C:\src\a.rs:42",
            "code --goto takes path:line"
        );
        assert_eq!(editor_target(Path::new("/tmp/a.h"), 1), "/tmp/a.h:1");
    }

    #[test]
    fn the_open_in_editor_button_is_clickable_in_the_preview_header() {
        let ctx = egui::Context::default();
        let rect = headless_frame(&ctx, None, |ui| {
            ui.add(egui::Button::new(
                egui::RichText::new("Open in Editor").size(12.0),
            ))
        })
        .rect;
        let resp = headless_frame(&ctx, Some(rect.center()), |ui| {
            ui.add(egui::Button::new(
                egui::RichText::new("Open in Editor").size(12.0),
            ))
        });
        assert!(
            resp.clicked(),
            "the preview header button must respond to a click"
        );
    }

    #[test]
    fn scope_badge_names_the_enclosing_header_of_each_flavor() {
        assert_eq!(
            scope_badge("pub fn search() {", "rs").as_deref(),
            Some("fn search()")
        );
        assert_eq!(
            scope_badge("fn main() {}", "rs").as_deref(),
            Some("fn main()")
        );
        assert_eq!(
            scope_badge("impl Config {", "rs").as_deref(),
            Some("impl Config")
        );
        assert_eq!(
            scope_badge("struct MediaPlayers {", "rs").as_deref(),
            Some("struct MediaPlayers")
        );
        assert_eq!(
            scope_badge("enum Mode {", "rs").as_deref(),
            Some("enum Mode")
        );
        assert_eq!(
            scope_badge("class Player {", "java").as_deref(),
            Some("class Player")
        );
        assert_eq!(
            scope_badge("def load():", "py").as_deref(),
            Some("def load")
        );
        assert_eq!(
            scope_badge("[Media Players]", "ini").as_deref(),
            Some("[Media Players]")
        );
        // Markdown headings are headings only in a markdown file.
        assert_eq!(
            scope_badge("## Media Players", "md").as_deref(),
            Some("[Media Players]")
        );
        assert_eq!(scope_badge("# Title", "md").as_deref(), Some("[Title]"));
        assert_eq!(
            scope_badge("### Quick Start", "markdown").as_deref(),
            Some("[Quick Start]")
        );
        // A `#` comment in a non-markdown file is not a scope. It must not
        // shadow the real `[main::connectivity]` header, so the scanner keeps
        // walking up. A `#` with no space, or a title with `=`, is not a heading.
        assert_eq!(scope_badge("# 1=do not send server details", "ini"), None);
        assert_eq!(scope_badge("# do not send server details", "ini"), None);
        assert_eq!(scope_badge("#title", "md"), None);
        assert_eq!(scope_badge("####### Too deep", "md"), None);
        assert_eq!(scope_badge("plain line", "txt"), None);
        assert_eq!(scope_badge("", "txt"), None);
        assert_eq!(scope_badge("[]", "ini"), None);
        assert_eq!(scope_badge("###", "md"), None);
        assert_eq!(scope_badge("fn()", "rs"), None);
    }

    #[test]
    fn an_overlong_badge_is_capped_at_one_card_line() {
        // A bracketed badge keeps its closing bracket, ellipsis lands inside.
        let b = scope_badge("[Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en)]", "ini")
            .expect("section header");
        assert_eq!(b, "[Mozilla/5.0 (Macintosh; U; PPC Ma…]");
        assert_eq!(b.chars().count(), 36, "capped to one card line");
        assert!(b.starts_with('[') && b.ends_with(']'), "brackets kept");
        // A plain badge truncates to 35 characters plus the ellipsis.
        let long = "def ".to_string() + &"a".repeat(50);
        let capped = cap_badge(long);
        assert_eq!(capped.chars().count(), 36);
        assert!(capped.ends_with('…'), "ellipsis appended");
        // A short badge is returned untouched.
        assert_eq!(cap_badge("fn main()".to_string()), "fn main()");
    }

    #[test]
    fn detect_scope_walks_upward_to_the_nearest_header_above_the_hit() {
        // Nearest header wins: the line just above the hit is checked first.
        let ctx = vec![
            (-3, "struct Config".to_string()),
            (-2, "let mut x;".to_string()),
            (-1, "let y;".to_string()),
            (0, "let z;".to_string()),
        ];
        assert_eq!(detect_scope(&ctx, "rs").as_deref(), Some("struct Config"));
        let ctx = vec![
            (-1, "fn main() {".to_string()),
            (0, "let x;".to_string()),
            (1, "let y;".to_string()),
        ];
        assert_eq!(detect_scope(&ctx, "rs").as_deref(), Some("fn main()"));
        // In an ini file a `#` comment is not a scope, so the scan walks past it
        // to the real section header above.
        let ctx = vec![
            (-3, "[main::connectivity]".to_string()),
            (-2, "connect_to = example.com".to_string()),
            (-1, "# 1=do not send server details".to_string()),
            (0, "value = 42".to_string()),
        ];
        assert_eq!(
            detect_scope(&ctx, "ini").as_deref(),
            Some("[main::connectivity]"),
            "a # comment must not shadow the real section header"
        );
        // No line above the window: the row falls back to its plain format.
        let ctx = vec![(0, "x".to_string()), (1, "y".to_string())];
        assert_eq!(detect_scope(&ctx, "txt"), None);
        // A hit line that is itself a header shows its own header, so the section
        // above is not its enclosing scope: the scan must stop at the hit line
        // instead of walking up to an unrelated previous header.
        let ctx = vec![
            (-3, "[PhpDig/*]".to_string()),
            (
                -1,
                "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.7.12)".to_string(),
            ),
            (0, "[PlantyNet_WebRobot*]".to_string()),
            (1, "PhpDig_WebSpider_1.0.3".to_string()),
        ];
        assert_eq!(
            detect_scope(&ctx, "ini"),
            None,
            "a header line gets no scope badge from the header above it"
        );
        // The same line, when it is not the hit, is still the scope of the hit below.
        let ctx = vec![
            (-2, "[PhpDig/*]".to_string()),
            (0, "PhpDig_WebSpider_1.0.3".to_string()),
            (
                1,
                "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.7.12)".to_string(),
            ),
        ];
        assert_eq!(detect_scope(&ctx, "ini").as_deref(), Some("[PhpDig/*]"));
    }

    #[test]
    fn the_context_windows_of_nearby_hits_merge_into_one_block() {
        // Lines 5, 8, 40 with radius 18: 5+8 overlap, 40 reaches line 22 which
        // sits inside the merged 1..26 span, so all three collapse to one block.
        assert_eq!(merge_hit_ranges(&[(5, 0), (8, 0), (40, 0)], 18), [(1, 58)]);
        // Far-apart hits keep their own spans.
        assert_eq!(
            merge_hit_ranges(&[(10, 0), (60, 0)], 18),
            [(1, 28), (42, 78)]
        );
        // Consecutive hits merge.
        assert_eq!(merge_hit_ranges(&[(5, 0), (6, 0)], 18), [(1, 24)]);
        // A span that runs off the top is clamped at line 1.
        assert_eq!(merge_hit_ranges(&[(3, 0)], 18), [(1, 21)]);
    }

    #[test]
    fn the_token_estimate_writes_thousands_as_k() {
        assert_eq!(format_tokens(500), "500");
        assert_eq!(format_tokens(999), "999");
        assert_eq!(format_tokens(1000), "1.0k");
        assert_eq!(format_tokens(1200), "1.2k");
        assert_eq!(format_tokens(2500), "2.5k");
    }

    #[test]
    fn the_scope_cache_stays_capped_and_mirrors_its_order() {
        let root = tmp_root("gui-scopecache");
        let path = root.join("a.txt");
        write_file(&path, b"line a\nline b\nline c\nline d\nline e\n");
        let mut cache = ScopeCache::default();
        for line in 0..600u32 {
            cache.get(&path, 0, line, line as u64);
        }
        assert!(cache.map.len() <= SNIPPET_CAP, "cache grew past the cap");
        assert_eq!(cache.order.len(), cache.map.len(), "order mirrors the map");
        cache.clear();
        assert!(cache.map.is_empty() && cache.order.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_ai_packer_merges_ranges_and_formats_one_block_per_span() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-aicontext");
        write_file(
            &root.join("a.rs"),
            b"pub fn search() {\n    let quiet = 1;\n    return search;\n}\n",
        );
        write_file(&root.join("b.ini"), b"[Media Players]\nsearch = 1\n");
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        app.query = String::from("search");
        app.run_search();
        assert_eq!(app.view.len(), 3, "two hits in a.rs, one in b.ini");

        let (block, tokens, files) = app.build_ai_context();
        assert_eq!(files, 2);
        assert!(
            block.starts_with("<!-- SeekSage Context: 2 files, 3 matches, ~"),
            "block: {block}"
        );
        assert_eq!(
            block.matches("### File:").count(),
            2,
            "one header per merged span"
        );
        assert_eq!(
            block.matches("```").count(),
            4,
            "one fenced block per merged span"
        );
        // The two hits in a.rs share one merged block: each line shows exactly once.
        assert_eq!(
            block.matches("1: pub fn search() {").count(),
            1,
            "the fn line shows once"
        );
        assert_eq!(
            block.matches("2: let quiet = 1;").count(),
            1,
            "the line between two hits shows once"
        );
        assert_eq!(block.matches("3: return search;").count(), 1);
        assert_eq!(
            block.matches("2: search = 1").count(),
            1,
            "the b.ini hit line"
        );
        assert!(
            block.contains("a.rs") && block.contains("b.ini"),
            "block: {block}"
        );
        assert!(tokens > 0, "tokens: {tokens}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_ai_packer_status_names_the_matches_files_and_tokens() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-aistatus");
        write_file(
            &root.join("a.rs"),
            b"pub fn search() {\n    let quiet = 1;\n    return search;\n}\n",
        );
        write_file(&root.join("b.ini"), b"[Media Players]\nsearch = 1\n");
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        app.query = String::from("search");
        app.run_search();
        app.copy_ai_context(&ctx);
        assert!(
            app.status.starts_with("Copied AI Context:"),
            "status: {}",
            app.status
        );
        assert!(app.status.contains("3 matches"), "status: {}", app.status);
        assert!(app.status.contains("2 files"), "status: {}", app.status);
        assert!(app.status.contains("(~"), "status: {}", app.status);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_copy_ai_context_button_is_clickable_in_the_top_toolbar() {
        let ctx = egui::Context::default();
        let rect = headless_frame(&ctx, None, |ui| {
            ui.add(egui::Button::new(
                egui::RichText::new("Copy AI Context").size(12.0),
            ))
        })
        .rect;
        let resp = headless_frame(&ctx, Some(rect.center()), |ui| {
            ui.add(egui::Button::new(
                egui::RichText::new("Copy AI Context").size(12.0),
            ))
        });
        assert!(
            resp.clicked(),
            "the top toolbar copy button must respond to a click"
        );
    }

    // Phase 4: cross-file log timeline and inline `path:` filters --------------

    /// The rows the view shows, as (file name, line), in display order.
    fn view_rows(app: &SeekSageApp) -> Vec<(String, u32)> {
        app.view
            .iter()
            .map(|&i| {
                let (file_id, line, _) = app.uniq[i];
                (file_name(&app.path_of(file_id)), line)
            })
            .collect()
    }

    /// The rows grouped by the file they sit in, with each group's lines sorted.
    /// A file-order assertion built on this never depends on the order the
    /// crawler happened to discover the files in.
    fn rows_grouped_by_file(app: &SeekSageApp) -> Vec<(String, Vec<u32>)> {
        let mut order: Vec<String> = Vec::new();
        let mut by_file: HashMap<String, Vec<u32>> = HashMap::new();
        for &i in &app.view {
            let (file_id, line, _) = app.uniq[i];
            let name = file_name(&app.path_of(file_id));
            if !by_file.contains_key(&name) {
                order.push(name.clone());
            }
            by_file.entry(name).or_default().push(line);
        }
        order
            .into_iter()
            .map(|name| {
                let mut lines = by_file.remove(&name).expect("group present");
                lines.sort_unstable();
                (name, lines)
            })
            .collect()
    }

    #[test]
    fn stamps_parse_from_iso_date_times_and_bracketed_clock_times() {
        // A date-time is milliseconds since 1970-01-01, so lines from different
        // days and different files compare on one number.
        assert_eq!(parse_line_timestamp_ms("1970-01-01T00:00:00.000Z"), Some(0));
        assert_eq!(
            parse_line_timestamp_ms("1970-01-01 00:00:00.500"),
            Some(500)
        );
        assert_eq!(
            days_from_civil(1970, 1, 1),
            0,
            "the epoch is the day the calendar starts from"
        );
        assert_eq!(days_from_civil(2000, 2, 29), 11_016, "the leap day of 2000");
        assert_eq!(days_from_civil(2026, 11, 7), 20_764);
        assert_eq!(
            parse_line_timestamp_ms("2026-11-07T09:30:00.500"),
            Some(20_764 * 86_400_000 + 34_200_500),
            "date, clock and fraction add up in milliseconds"
        );
        // `T`, a space, `.` and `,` fractions are all the same stamp.
        assert_eq!(
            parse_line_timestamp_ms("2026-11-07T09:30:00.250"),
            parse_line_timestamp_ms("2026-11-07 09:30:00,250"),
            "space-separated log lines read the same as ISO ones"
        );
        assert_eq!(
            parse_line_timestamp_ms("2026-11-08T00:00:00").unwrap()
                - parse_line_timestamp_ms("2026-11-07T00:00:00").unwrap(),
            86_400_000,
            "one calendar day is exactly one day of milliseconds"
        );
        assert_eq!(
            parse_line_timestamp_ms("2026-11-07T09:30:00").unwrap()
                - parse_line_timestamp_ms("2026-11-07T09:29:59.750").unwrap(),
            250,
            "the fraction sits inside the same millisecond scale"
        );
        // A time-only prefix is milliseconds of that day, plain or bracketed.
        assert_eq!(
            parse_line_timestamp_ms("[12:34:56.789] INFO ready"),
            Some(45_296_789)
        );
        assert_eq!(
            parse_line_timestamp_ms("12:34:56,123 INFO ready"),
            Some(45_296_123)
        );
        assert_eq!(
            parse_line_timestamp_ms("<12:34:56.100> warn"),
            Some(45_296_100)
        );
        assert_eq!(parse_line_timestamp_ms("01:02:03 warn"), Some(3_723_000));
        assert_eq!(parse_line_timestamp_ms("00:00:00"), Some(0));
        // A one-digit fraction is whole milliseconds; a longer one keeps three.
        assert_eq!(
            parse_line_timestamp_ms("[12:34:56.7] INFO"),
            Some(45_296_700)
        );
        assert_eq!(
            parse_line_timestamp_ms("[12:34:56.7891] INFO"),
            Some(45_296_789)
        );
        // Nothing usable at the front of the line reports no stamp at all.
        assert_eq!(parse_line_timestamp_ms("plain line, no stamp"), None);
        assert_eq!(
            parse_line_timestamp_ms("12:34 warn"),
            None,
            "no seconds is not a stamp"
        );
        assert_eq!(parse_line_timestamp_ms("1234:56:78 warn"), None);
        assert_eq!(
            parse_line_timestamp_ms("2026-11-07 warn"),
            None,
            "a bare date is not a stamp"
        );
        assert_eq!(parse_line_timestamp_ms("2026-13-01T00:00:00"), None);
        assert_eq!(parse_line_timestamp_ms("2026-11-07T00:60:00"), None);
        assert_eq!(parse_line_timestamp_ms("24:00:00 warn"), None);
        assert_eq!(parse_line_timestamp_ms(""), None);
    }

    #[test]
    fn the_delta_badge_switches_units_at_one_second_and_one_minute() {
        assert_eq!(format_delta_ms(0), "+0ms");
        assert_eq!(format_delta_ms(1), "+1ms");
        assert_eq!(format_delta_ms(125), "+125ms");
        assert_eq!(format_delta_ms(999), "+999ms");
        assert_eq!(format_delta_ms(1_000), "+1.00s");
        assert_eq!(format_delta_ms(4_750), "+4.75s");
        // Under a minute shows as whole seconds, so the last of them rounds up to
        // a full minute's worth of seconds on display.
        assert_eq!(format_delta_ms(59_999), "+60.00s");
        assert_eq!(format_delta_ms(60_000), "+1.0m");
        assert_eq!(format_delta_ms(90_000), "+1.5m");
        assert_eq!(format_delta_ms(32_400_200), "+540.0m");
    }

    #[test]
    fn timeline_mode_sorts_two_logs_into_one_chronological_line() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-timeline");
        write_file(
            &root.join("a.log"),
            b"12:00:00.100 WARN alpha log start\n12:00:01.250 INFO alpha log step\nplain log line with no stamp\n",
        );
        write_file(
            &root.join("b.log"),
            b"12:00:00.500 INFO beta log start\n12:00:06.000 INFO beta log end\n",
        );
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);

        app.query = String::from("log");
        app.run_search();
        assert_eq!(app.view.len(), 5, "three rows in a.log, two in b.log");
        // `By File` keeps each file's rows together, in line order, and shows no
        // gap badges at all.
        assert_eq!(
            rows_grouped_by_file(&app),
            [
                (String::from("a.log"), vec![1, 2, 3]),
                (String::from("b.log"), vec![1, 2])
            ],
            "rows stay inside their own file"
        );
        assert!(app.timeline_deltas.is_empty(), "no badges in By File order");

        // `Timeline` merges the files onto one chronological line, oldest first,
        // and marks the gap from the stamped row before it. The unstamped row
        // goes last with no badge.
        app.result_order = ResultOrder::Timeline;
        app.rebuild_view();
        assert_eq!(
            view_rows(&app),
            [
                (String::from("a.log"), 1),
                (String::from("b.log"), 1),
                (String::from("a.log"), 2),
                (String::from("b.log"), 2),
                (String::from("a.log"), 3),
            ],
            "interleaved stamps sort oldest to newest across files"
        );
        assert_eq!(
            app.timeline_deltas,
            [
                Some(String::from("+0ms")),
                Some(String::from("+400ms")),
                Some(String::from("+750ms")),
                Some(String::from("+4.75s")),
                None,
            ],
            "each badge is the step from the stamped row before it"
        );
        // The header counts every file once, even when a timeline interleaves them.
        assert_eq!(app.view_files, 2, "the same two files in timeline order");
        assert_eq!(app.matched_files, 2);
        assert_eq!(app.selected, 0, "the timeline starts on the oldest row");

        // Back to `By File`: rows return to their files and the badges go away.
        app.result_order = ResultOrder::ByFile;
        app.rebuild_view();
        assert_eq!(app.view.len(), 5);
        assert!(app.timeline_deltas.is_empty());
        assert_eq!(
            rows_grouped_by_file(&app),
            [
                (String::from("a.log"), vec![1, 2, 3]),
                (String::from("b.log"), vec![1, 2])
            ],
            "the grouping is intact after the round trip"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn timeline_orders_iso_stamps_across_files_by_their_day() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-timeline-iso");
        write_file(&root.join("a.log"), b"2026-11-07 09:00:00.100 log start\n");
        write_file(&root.join("b.log"), b"2026-11-07T09:00:00.400 log step\n");
        write_file(
            &root.join("c.log"),
            b"2026-11-06 23:59:59.900 log earlier\n",
        );
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        app.query = String::from("log");
        app.run_search();
        assert_eq!(app.view.len(), 3);

        app.result_order = ResultOrder::Timeline;
        app.rebuild_view();
        // The row from the previous day leads the timeline, and its gap to the
        // next morning is long enough to show in minutes.
        assert_eq!(
            view_rows(&app),
            [
                (String::from("c.log"), 1),
                (String::from("a.log"), 1),
                (String::from("b.log"), 1),
            ],
            "a stamp from the day before sorts first"
        );
        assert_eq!(
            app.timeline_deltas,
            [
                Some(String::from("+0ms")),
                Some(String::from("+540.0m")),
                Some(String::from("+300ms")),
            ],
            "the gap crosses midnight and changes unit as it grows"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn path_tokens_filter_by_the_file_path_and_never_reach_the_token_list() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-pathfilter");
        std::fs::create_dir_all(root.join("app")).expect("app dir");
        std::fs::create_dir_all(root.join("logs")).expect("logs dir");
        write_file(
            &root.join("app").join("a.log"),
            b"index_file first app line\n",
        );
        write_file(
            &root.join("app").join("b.log"),
            b"index_file second app line\n",
        );
        write_file(
            &root.join("logs").join("c.log"),
            b"index_file third logs line\n",
        );
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);

        // The normalized path key: lowercased, backslashes folded to `/`.
        assert_eq!(
            path_needle_key(r"App\Logs"),
            "app/logs",
            "a needle typed with backslashes is compared as a slash path"
        );
        assert_eq!(
            path_match_key(Path::new(r"C:\Logs\App\a.log")),
            "c:/logs/app/a.log"
        );

        // `path:` keeps only hits inside a folder, and the needle never joins the
        // content tokens that drive highlighting.
        app.query = String::from("index_file path:app");
        app.run_search();
        assert_eq!(
            app.tokens,
            vec![String::from("index_file")],
            "path: tokens never reach self.tokens"
        );
        assert_eq!(app.view.len(), 2, "both app logs, not the logs folder one");
        assert_eq!(app.view_files, 2);

        // `-path:` drops hits by path, and the two sides compose.
        app.query = String::from("index_file -path:app");
        app.run_search();
        assert_eq!(app.view.len(), 1, "only the logs folder row survives");
        assert_eq!(file_name(&app.path_of(app.uniq[app.view[0]].0)), "c.log");

        app.query = String::from("index_file path:app -path:a.log");
        app.run_search();
        assert_eq!(app.view.len(), 1);
        assert_eq!(file_name(&app.path_of(app.uniq[app.view[0]].0)), "b.log");

        // A slash in the needle matches a Windows path, and the needle is
        // case-insensitive.
        app.query = String::from("index_file path:/app/");
        app.run_search();
        assert_eq!(
            app.view.len(),
            2,
            "the folded path key matches a `/` needle"
        );
        app.query = String::from("index_file PATH:APP");
        app.run_search();
        assert_eq!(app.view.len(), 2, "PATH: and path: are the same filter");

        // The needle is not a content term, so the file's text is never asked to
        // say `c.log`.
        app.query = String::from("index_file path:c.log");
        app.run_search();
        assert_eq!(app.view.len(), 1);
        assert_eq!(app.tokens, vec![String::from("index_file")]);

        // A path filter with no content term shows nothing, gracefully.
        app.query = String::from("path:app");
        app.run_search();
        assert_eq!(app.view.len(), 0);
        assert_eq!(app.uniq.len(), 0);
        assert!(app.tokens.is_empty());
        assert!(
            app.header_text()
                .starts_with("Found 0 matches across 0 files"),
            "header: {}",
            app.header_text()
        );

        // A bare `path:` constrains nothing, so the content terms stand alone.
        app.query = String::from("index_file path:");
        app.run_search();
        assert_eq!(app.view.len(), 3);

        // The ext filter and the path filter compose in the same view.
        app.query = String::from("index_file path:app");
        app.ext_filter = String::from(".rs");
        app.run_search();
        assert_eq!(app.view.len(), 0, "no .rs rows in the fixture");
        app.ext_filter = String::from(".log");
        app.run_search();
        assert_eq!(
            app.view.len(),
            2,
            "the path filter still holds the rows back"
        );
        app.ext_filter.clear();

        // A path filter keeps working in timeline order.
        app.query = String::from("index_file -path:logs");
        app.run_search();
        assert_eq!(app.view.len(), 2);
        app.result_order = ResultOrder::Timeline;
        app.rebuild_view();
        assert_eq!(app.view.len(), 2);
        assert_eq!(app.view_files, 2, "two files, rows in each");
        assert!(
            app.timeline_deltas.iter().all(|d| d.is_none()),
            "lines with no stamp get no badge"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_timeline_chip_is_clickable_in_the_results_toolbar() {
        let ctx = egui::Context::default();
        let rect = headless_frame(&ctx, None, |ui| {
            ui.add(egui::Button::new(
                egui::RichText::new("Timeline").size(12.0),
            ))
        })
        .rect;
        let resp = headless_frame(&ctx, Some(rect.center()), |ui| {
            ui.add(egui::Button::new(
                egui::RichText::new("Timeline").size(12.0),
            ))
        });
        assert!(
            resp.clicked(),
            "the results toolbar timeline chip must respond to a click"
        );
    }

    // Live file watcher ---------------------------------------------------------

    #[test]
    fn the_tree_fingerprint_follows_size_and_stamp_and_stays_still_on_a_quiet_tree() {
        let root = tmp_root("gui-fingerprint");
        write_file(&root.join("a.log"), b"one two three");
        write_file(&root.join("b.log"), b"alpha beta");
        let paths = [root.join("a.log"), root.join("b.log")];
        let base = tree_fingerprint(&paths);
        assert_eq!(
            tree_fingerprint(&paths),
            base,
            "a tree nobody has touched fingerprints identically"
        );

        // A longer file: the byte size is part of the hash.
        write_file(&root.join("a.log"), b"one two three four five");
        let grown = tree_fingerprint(&paths);
        assert_ne!(
            grown, base,
            "a file that grows in byte size moves the fingerprint"
        );

        // One more path in the list is a change, even with its own size and stamp.
        write_file(&root.join("c.log"), b"gamma delta");
        let mut wider = paths.to_vec();
        wider.push(root.join("c.log"));
        assert_ne!(
            tree_fingerprint(&wider),
            grown,
            "an added file moves the fingerprint"
        );

        // Rewriting the same bytes keeps the size, so only the modified stamp
        // moves; the watcher has to read that as a change as well.
        std::thread::sleep(Duration::from_millis(15));
        write_file(&root.join("b.log"), b"alpha beta");
        assert_ne!(
            tree_fingerprint(&paths),
            grown,
            "a rewrite at the same size still counts, on the modified stamp"
        );

        // A file that is gone is its own fingerprint value, not the same hash as
        // the tree that had it.
        std::fs::remove_file(root.join("b.log")).expect("remove b.log");
        assert_ne!(
            tree_fingerprint(&paths),
            tree_fingerprint(&[root.join("a.log")])
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn watch_mode_waits_for_a_quiet_window_and_rebuilds_keeping_the_query() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-watch");
        write_file(&root.join("a.log"), b"index_file first line\n");
        write_file(&root.join("b.log"), b"index_file second line\n");
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);

        app.query = String::from("index_file");
        app.ext_filter = String::from(".log");
        app.result_order = ResultOrder::Timeline;
        app.run_search();
        assert_eq!(app.view.len(), 2, "the live view before the file change");

        // `Watch` is off until the chip is turned on, and the baseline is the tree
        // the index was built from.
        assert!(!app.watch_enabled, "watch is off by default");
        app.set_watch(true);
        assert!(app.watch_enabled);
        assert_eq!(
            app.status, "watch on · rebuilding after a quiet window",
            "status: {}",
            app.status
        );
        assert!(
            app.last_fingerprint.is_some(),
            "the baseline is the built tree"
        );

        // A quiet tree: the check runs, nothing is pending, no rebuild starts.
        app.last_watch_check = Some(Instant::now() - Duration::from_millis(1100));
        app.poll_watch(&ctx);
        assert!(
            app.pending_change_since.is_none(),
            "a quiet tree has nothing pending"
        );
        assert_eq!(app.phase, Phase::Ready, "a quiet tree triggers nothing");

        // The first change is recorded and announced, and the rebuild does not fire
        // on the frame the change was seen.
        write_file(
            &root.join("a.log"),
            b"index_file first line\nindex_file added line\n",
        );
        app.last_watch_check = Some(Instant::now() - Duration::from_millis(1100));
        app.poll_watch(&ctx);
        assert_eq!(
            app.status, "file change detected · waiting for quiet window…",
            "status: {}",
            app.status
        );
        assert_eq!(
            app.phase,
            Phase::Ready,
            "a change waits for the quiet window"
        );
        assert!(app.pending_change_since.is_some(), "the change is pending");

        // A tree that keeps moving restarts the window, so a writer mid-save is
        // never caught.
        write_file(&root.join("b.log"), b"index_file second line rewritten\n");
        app.last_watch_check = Some(Instant::now() - Duration::from_millis(1100));
        app.poll_watch(&ctx);
        assert_eq!(
            app.phase,
            Phase::Ready,
            "a tree that keeps moving does not rebuild"
        );

        // Once the shape holds across the quiet window, the rebuild fires on its
        // own, and the query, the filter and the order are app state, so they
        // survive the rebuild.
        app.last_watch_check = Some(Instant::now() - Duration::from_millis(1100));
        app.pending_change_since = Some(Instant::now() - Duration::from_millis(700));
        app.poll_watch(&ctx);
        assert_eq!(
            app.phase,
            Phase::Indexing,
            "the quiet window fires a rebuild"
        );
        assert_eq!(app.query, "index_file", "watch keeps the query");
        assert_eq!(app.ext_filter, ".log", "watch keeps the extension filter");
        assert_eq!(
            app.result_order,
            ResultOrder::Timeline,
            "watch keeps the result order"
        );
        assert!(
            app.pending_change_since.is_none(),
            "the window is cleared for the next change"
        );

        // The rebuild finishes, and `run_search` restores the live view on the
        // refreshed rows.
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);
        assert_eq!(app.view.len(), 3, "the added line is in the refreshed view");
        assert_eq!(app.query, "index_file");
        assert_eq!(app.ext_filter, ".log");
        assert_eq!(app.result_order, ResultOrder::Timeline);

        // The rebuilt tree is the new baseline, so the same quiet tree does not
        // fire a second rebuild.
        app.last_watch_check = Some(Instant::now() - Duration::from_millis(1100));
        app.poll_watch(&ctx);
        assert_eq!(
            app.phase,
            Phase::Ready,
            "a quiet rebuilt tree does not rebuild again"
        );

        // `Watch` off stops the watcher, and the rows on screen stay.
        app.set_watch(false);
        assert!(!app.watch_enabled);
        assert_eq!(app.status, "watch off", "status: {}", app.status);
        write_file(
            &root.join("a.log"),
            b"index_file first line\nindex_file added line\nindex_file third line\n",
        );
        app.last_watch_check = Some(Instant::now() - Duration::from_millis(1100));
        app.poll_watch(&ctx);
        assert_eq!(app.phase, Phase::Ready, "watch off means no rebuild");
        assert_eq!(app.view.len(), 3, "the rows the user is looking at stay");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_watch_chip_is_clickable_in_the_top_toolbar() {
        let ctx = egui::Context::default();
        let rect = headless_frame(&ctx, None, |ui| {
            ui.add(egui::Button::new(egui::RichText::new("Watch").size(12.0)))
        })
        .rect;
        let resp = headless_frame(&ctx, Some(rect.center()), |ui| {
            ui.add(egui::Button::new(egui::RichText::new("Watch").size(12.0)))
        });
        assert!(
            resp.clicked(),
            "the top toolbar watch chip must respond to a click"
        );
    }

    /// Drive one headless frame over a window of `screen` points, and press and
    /// release the primary button at `at` when a point is given. Returns the shapes
    /// the frame drew, so a test can read what the app painted.
    fn headless_output(
        ctx: &egui::Context,
        screen: egui::Vec2,
        at: Option<egui::Pos2>,
        mut paint: impl FnMut(&egui::Context),
    ) -> egui::FullOutput {
        let mut raw = egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(egui::Pos2::ZERO, screen)),
            ..Default::default()
        };
        if let Some(pos) = at {
            raw.events.push(egui::Event::PointerMoved(pos));
            for pressed in [true, false] {
                raw.events.push(egui::Event::PointerButton {
                    pos,
                    button: egui::PointerButton::Primary,
                    pressed,
                    modifiers: egui::Modifiers::NONE,
                });
            }
        }
        ctx.run(raw, |ctx| paint(ctx))
    }

    /// Drive one headless frame over a full screen, so the panel the app paints
    /// takes the whole area, and press and release the primary button at `at`
    /// when a point is given. The panel records the zones it paints, so a test can
    /// press on the exact spot a card or a chip occupies.
    fn headless_screen(
        ctx: &egui::Context,
        at: Option<egui::Pos2>,
        paint: impl FnMut(&egui::Context),
    ) {
        let mut paint = paint;
        let _ = headless_output(ctx, egui::vec2(500.0, 600.0), at, |ctx| paint(ctx));
    }

    /// Every string the frame painted, paired with the rect its glyphs occupy.
    fn painted_text(out: &egui::FullOutput) -> Vec<(egui::Rect, String)> {
        out.shapes
            .iter()
            .filter_map(|clipped| match &clipped.shape {
                egui::Shape::Text(text) => {
                    let rect = egui::Rect::from_min_max(
                        text.pos + text.galley.rect.min.to_vec2(),
                        text.pos + text.galley.rect.max.to_vec2(),
                    );
                    Some((rect, text.galley.job.text.clone()))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn the_ext_filter_starts_empty_so_every_text_file_is_searched() {
        assert!(
            SeekSageApp::default().ext_filter.is_empty(),
            "the app must not ship a startup extension filter"
        );

        let ctx = egui::Context::default();
        let root = tmp_root("gui-ext-default");
        write_file(&root.join("a.rs"), b"pub fn index_file() {}");
        write_file(&root.join("b.md"), b"# index_file");
        write_file(&root.join("c.ini"), b"[main]\nindex_file = 1\n");
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        assert!(app.ext_filter.is_empty(), "the box is empty on a fresh app");
        app.start_build();
        drain(&mut app, &ctx, 5000);
        assert_eq!(app.phase, Phase::Ready, "status: {}", app.status);
        app.query = String::from("index_file");
        app.run_search();
        assert_eq!(
            app.view.len(),
            3,
            "an empty ext filter reaches every file in the tree ({} files)",
            app.view_files
        );
        assert!(
            app.ext_filter.is_empty(),
            "running a search never fills the extension box"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn clicking_the_text_of_a_result_card_selects_that_row() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-card-click");
        write_file(
            &root.join("a.log"),
            b"index_file first line\nindex_file second line\n",
        );
        write_file(&root.join("b.log"), b"index_file third line\n");
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        app.query = String::from("index_file");
        app.run_search();
        assert_eq!(app.view.len(), 3, "the fixture has three rows");
        app.selected = 0;
        app.pending_scroll = None;

        // One frame with no click records where the panel put each card.
        headless_screen(&ctx, None, |ctx| app.paint_results(ctx));
        assert_eq!(app.row_rects.len(), 3, "the panel painted all three cards");

        // The centre of a card is exactly where the badge and the snippet text sit.
        // The press has to reach the row behind the text, not slip through a gap.
        let middle = app.row_rects[1].center();
        headless_screen(&ctx, Some(middle), |ctx| app.paint_results(ctx));
        assert_eq!(
            app.selected, 1,
            "a click on the text of a card selects that card"
        );

        // The left accent bar and the empty right margin past the text are the same
        // card, and pressing there selects it too.
        let third = app.row_rects[2];
        let on_bar = egui::pos2(third.min.x + 3.0, third.center().y);
        headless_screen(&ctx, Some(on_bar), |ctx| app.paint_results(ctx));
        assert_eq!(
            app.selected, 2,
            "the accent edge of a card belongs to the card"
        );
        let far_right = egui::pos2(third.max.x - 2.0, third.center().y);
        headless_screen(&ctx, Some(far_right), |ctx| app.paint_results(ctx));
        assert_eq!(
            app.selected, 2,
            "the empty right margin of a card belongs to the card"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_order_chips_take_the_click_across_their_whole_slot() {
        let ctx = egui::Context::default();
        let root = tmp_root("gui-chip-click");
        write_file(
            &root.join("a.log"),
            b"index_file first line\nindex_file second line\n",
        );
        write_file(&root.join("b.log"), b"index_file third line\n");
        let mut app = SeekSageApp {
            root_text: root.display().to_string(),
            ..Default::default()
        };
        app.start_build();
        drain(&mut app, &ctx, 5000);
        app.query = String::from("index_file");
        app.run_search();
        assert_eq!(
            app.result_order,
            ResultOrder::ByFile,
            "rows grouped by file is the default order"
        );

        // One frame with no click records both chip slots, `Timeline` then `By File`.
        headless_screen(&ctx, None, |ctx| app.paint_results(ctx));
        assert_eq!(
            app.chip_rects.len(),
            2,
            "the header painted both order chips"
        );
        let timeline = app.chip_rects[0];
        let by_file = app.chip_rects[1];
        assert!(timeline.width() >= 74.0, "the timeline slot: {}", timeline);
        assert!(by_file.width() >= 66.0, "the by file slot: {}", by_file);

        // The far edge of a chip is inside its own slot, so a press there switches
        // the view and every row stays in the list.
        let left_edge = egui::pos2(timeline.min.x + 1.0, timeline.center().y);
        headless_screen(&ctx, Some(left_edge), |ctx| app.paint_results(ctx));
        assert_eq!(
            app.result_order,
            ResultOrder::Timeline,
            "the whole slot of the Timeline chip takes the click"
        );
        assert_eq!(app.view.len(), 3, "the timeline view keeps every row");

        let right_edge = egui::pos2(by_file.max.x - 1.0, by_file.center().y);
        headless_screen(&ctx, Some(right_edge), |ctx| app.paint_results(ctx));
        assert_eq!(
            app.result_order,
            ResultOrder::ByFile,
            "the whole slot of the By File chip takes the click"
        );
        assert_eq!(app.view.len(), 3, "the by file view keeps every row");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_ext_filter_field_is_labelled_and_starts_clean() {
        let ctx = egui::Context::default();
        let mut app = SeekSageApp::default();
        let out = headless_output(&ctx, egui::vec2(1100.0, 600.0), None, |ctx| {
            app.paint_searchbar(ctx)
        });

        // The box has a label, and the label sits in front of the box on its line.
        let label = app
            .ext_label_rect
            .expect("the ext label must be painted in the search bar");
        let box_rect = app
            .ext_box_rect
            .expect("the ext filter box must be painted in the search bar");
        assert!(
            label.max.x <= box_rect.min.x + 1.0,
            "the ext label must sit in front of the box (label {}, box {})",
            label,
            box_rect
        );
        assert!(
            label.min.y >= box_rect.min.y - 1.0 && label.max.y <= box_rect.max.y + 1.0,
            "the ext label must share the line of the box (label {}, box {})",
            label,
            box_rect
        );

        let texts = painted_text(&out);
        assert!(
            texts.iter().any(|(_, t)| t.as_str() == "ext:"),
            "the search bar must paint the ext label: {:?}",
            texts
        );

        // The field starts clean: nothing with glyphs is painted inside the box, so
        // there is no placeholder text to read as a value.
        let inner = box_rect.shrink(6.0);
        let inside: Vec<String> = texts
            .iter()
            .filter(|(rect, t)| !t.is_empty() && rect.intersects(inner))
            .map(|(_, t)| t.clone())
            .collect();
        assert!(
            inside.is_empty(),
            "the ext filter box must paint no placeholder text, it paints {:?} inside \
             itself",
            inside
        );
        assert!(
            !texts.iter().any(|(_, t)| t.contains(".rs .md .txt")),
            "the ext box must carry no leftover placeholder: {:?}",
            texts
        );

        // Typing an extension shows it in the box, so `ext:` reads with the value.
        app.ext_filter = String::from(".log");
        let out = headless_output(&ctx, egui::vec2(1100.0, 600.0), None, |ctx| {
            app.paint_searchbar(ctx)
        });
        let texts = painted_text(&out);
        let typed: Vec<String> = texts
            .iter()
            .filter(|(rect, t)| t.as_str() == ".log" && rect.intersects(inner))
            .map(|(_, t)| t.clone())
            .collect();
        assert_eq!(
            typed,
            vec![String::from(".log")],
            "a typed extension shows inside the box"
        );
    }

    #[test]
    fn the_toolbar_row_shares_one_centre_axis_and_the_theme_square_anchors_the_right_edge() {
        let ctx = egui::Context::default();
        let mut app = SeekSageApp::default();
        let out = headless_output(&ctx, egui::vec2(1100.0, 600.0), None, |ctx| {
            app.paint_toolbar(ctx)
        });

        let logo = app
            .logo_rect
            .expect("the logo must be painted in the toolbar");
        let path = app
            .path_box_rect
            .expect("the path box must be painted in the toolbar");
        let index = app
            .index_rect
            .expect("the Index button must be painted in the toolbar");
        let theme = app
            .theme_rect
            .expect("the theme square must be painted in the toolbar");

        // The logo, the path box, the Index button and the theme square all sit on
        // one centre axis, so the bar reads as a single line of aligned controls
        // instead of a row of boxes hanging at different heights.
        let axis = path.center().y;
        for (name, r) in [
            ("logo", logo),
            ("Index button", index),
            ("theme square", theme),
        ] {
            assert!(
                (r.center().y - axis).abs() <= 1.0,
                "{name} must sit on the centre axis of the path box (axis {axis}, {name} centre {cy})",
                cy = r.center().y
            );
        }

        // No horizontal overlap: `Copy AI Context`, the last button of the row,
        // keeps clear space from the theme square anchored at the right corner, so
        // the row never slides one control under another.
        let pack = app
            .pack_rect
            .expect("the Copy AI Context button must be painted in the toolbar");
        assert!(
            pack.max.x + 12.0 <= theme.min.x,
            "Copy AI Context must keep a clear gap to the theme square (pack right {pr}, theme left {tl})",
            pr = pack.max.x,
            tl = theme.min.x
        );

        // Icon-only: a clean 28 x 28 square, with no "Dark" / "Light" text painted.
        assert!(
            (theme.width() - 28.0).abs() <= 1.0 && (theme.height() - 28.0).abs() <= 1.0,
            "the theme toggle must be a square, it paints {theme}"
        );
        let texts = painted_text(&out);
        assert!(
            !texts
                .iter()
                .any(|(_, t)| t.as_str() == "Dark" || t.as_str() == "Light"),
            "the theme toggle must paint no text label: {texts:?}"
        );

        // The square takes the far-right corner the lanes slider used to hold, and
        // the slider is gone: `lanes` is set by `default_lanes()` under the hood.
        // The bar has 10 px of inner padding and the right-to-left block keeps a
        // 10 px spacer, so the square lands just inside the corner, not flush on
        // the window edge and not overlapping the buttons.
        let corner_right = 1100.0 - 10.0 - 10.0;
        assert!(
            theme.max.x >= corner_right - 2.0 && theme.max.x <= corner_right + 1.0,
            "the theme square must anchor the far-right corner (corner right {corner_right}, theme {theme})"
        );
        // The whole row stays on screen: nothing is painted past the window edge.
        assert!(
            theme.max.x <= 1100.0 && path.max.x <= 1100.0,
            "the row must not overflow the window (theme {theme}, path {path})"
        );
        assert!(
            theme.min.x > path.max.x,
            "the theme square must sit right of the path box (path {path}, theme {theme})"
        );
        assert!(
            !texts.iter().any(|(_, t)| t.contains("lanes")),
            "the toolbar must not paint a lanes slider: {texts:?}"
        );

        // The path box widens into the space the slider and the wide theme button
        // left behind, keeps a full `C:\path\to\repository` visible, and gives the
        // row the room the clear gap to the theme square needs.
        assert!(
            path.width() >= 460.0,
            "the path box must take the freed width, it paints {path}"
        );

        // While a build runs, the progress bar, its spinner and the ms read-out sit
        // to the left of the square, so the same clear gap must land between the
        // last button and that block, on the painted rects.
        app.phase = Phase::Indexing;
        app.progress = Progress {
            stage: BuildStage::Ingest,
            total: 1000,
            done: 400,
            chunks: 8,
            ..Default::default()
        };
        app.build_ms = 1234.0;
        headless_output(&ctx, egui::vec2(1100.0, 600.0), None, |ctx| {
            app.paint_toolbar(ctx)
        });
        let pack_busy = app
            .pack_rect
            .expect("the Copy AI Context button must be painted while indexing");
        let bar = app
            .progress_bar_rect
            .expect("the progress bar must be painted while indexing");
        assert!(
            pack_busy.max.x + 12.0 <= bar.min.x,
            "Copy AI Context must keep a clear gap to the progress block (pack right {pr}, bar left {bl})",
            pr = pack_busy.max.x,
            bl = bar.min.x
        );
    }

    #[test]
    fn the_search_bar_is_one_line_with_one_clear_button_per_box() {
        let ctx = egui::Context::default();
        let mut app = SeekSageApp::default();
        app.query = String::from("index_file");
        app.ext_filter = String::from(".rs");
        let out = headless_output(&ctx, egui::vec2(1100.0, 600.0), None, |ctx| {
            app.paint_searchbar(ctx)
        });

        // Each box owns its "×", and Esc clears both filters, so the bar paints no
        // shared Clear button.
        let texts = painted_text(&out);
        assert!(
            !texts.iter().any(|(_, t)| t.as_str() == "Clear"),
            "the search bar must paint no shared Clear button: {texts:?}"
        );

        let q = app
            .search_box_rect
            .expect("the query box must be painted in the search bar");
        let qc = app
            .query_clear_rect
            .expect("the query × must be painted in the search bar");
        let label = app
            .ext_label_rect
            .expect("the ext label must be painted in the search bar");
        let ext = app
            .ext_box_rect
            .expect("the ext box must be painted in the search bar");

        // No horizontal overlap: the ext "×" keeps clear space from the latency
        // read-out anchored at the right edge of the bar.
        let extclear = app
            .ext_clear_rect
            .expect("the ext × must be painted in the search bar");
        let latency = app
            .latency_rect
            .expect("the latency read-out must be painted in the search bar");
        assert!(
            extclear.max.x + 10.0 <= latency.min.x,
            "the ext × must keep clear space before the latency read-out (ext × right {er}, latency left {ll})",
            er = extclear.max.x,
            ll = latency.min.x
        );

        // A clean gap, the 8 px spacer plus the row spacing, between the × and the
        // label, so the × stays read as part of the query box.
        let gap = label.min.x - qc.max.x;
        assert!(
            gap >= 8.0 && gap <= 12.0,
            "the query × must keep a clean gap before the ext label, measured {gap}"
        );

        // One centre axis: the query box, the ext label and the ext box.
        let axis = q.center().y;
        for (name, r) in [("ext label", label), ("ext box", ext)] {
            assert!(
                (r.center().y - axis).abs() <= 1.0,
                "{name} must sit on the centre axis of the query box (axis {axis}, {name} centre {cy})",
                cy = r.center().y
            );
        }

        // The query box takes the room the Clear button left, so it stays the
        // primary field of the bar.
        assert!(
            q.width() >= ext.width() + 200.0,
            "the query box must read as the primary field (query {q}, ext {ext})"
        );
    }
}
