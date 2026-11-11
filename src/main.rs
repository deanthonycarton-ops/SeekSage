//! `SeekSage` end-user CLI.
//!
//! Commands:
//! - `seeksage index  <PATH>`               crawl, build the global index, report metrics.
//! - `seeksage search "<QUERY>" <PATH>`     build once, run one multi-term AND query.
//! - `seeksage repl   <PATH>`              interactive shell: index built once, queried repeatedly.
//!
//! Options: `--lanes <N>` producer lanes, `--show <N>` preview limit.
//!
//! This binary is a pure consumer of the `seeksage` library: it drives the
//! public `build_index` API, the `Built` report, and the `GlobalIndex` query
//! path, exactly as an external crate linking `seeksage` would. Line previews
//! stream through `extract_snippet` windows held in a bounded LRU, so a
//! multi-gigabyte log or book repository never loads into RAM.

use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use seeksage::index::SearchResult;
use seeksage::{build_index, default_lanes, extract_snippet, Built, LANE_CAP};

const MAX_LANES: usize = 32;
const PREVIEW_WIDTH: usize = 88;
/// Bounded LRU of line previews: at most this many clamped strings are alive
/// at any time, so the CLI never grows RAM with the size of the repository.
const PREVIEW_CACHE_CAP: usize = 256;
const DEFAULT_SHOW: usize = 20;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

struct Options {
    lanes: usize,
    show: usize,
}

fn usage_error(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("usage: {msg}"))
}

fn parse_size(s: &str) -> io::Result<usize> {
    s.parse::<usize>()
        .map_err(|_| usage_error(&format!("'{s}' is not a number")))
}

fn clamp_lanes(n: usize) -> usize {
    n.clamp(1, MAX_LANES)
}

/// Peel `--lanes` / `--show` flags out of the command line.
fn split_flags(args: &[String]) -> io::Result<(Vec<String>, Options)> {
    let mut positional: Vec<String> = Vec::new();
    let mut lanes = default_lanes();
    let mut show = DEFAULT_SHOW;
    let mut i = 0usize;
    while i < args.len() {
        let a = args[i].as_str();
        i += 1;
        if let Some(v) = a.strip_prefix("--lanes=") {
            lanes = clamp_lanes(parse_size(v)?);
        } else if a == "--lanes" {
            let v = args
                .get(i)
                .ok_or_else(|| usage_error("--lanes needs a value"))?;
            i += 1;
            lanes = clamp_lanes(parse_size(v.as_str())?);
        } else if let Some(v) = a.strip_prefix("--show=") {
            show = parse_size(v)?;
        } else if a == "--show" {
            let v = args
                .get(i)
                .ok_or_else(|| usage_error("--show needs a value"))?;
            i += 1;
            show = parse_size(v.as_str())?;
        } else {
            positional.push(a.to_string());
        }
    }
    Ok((positional, Options { lanes, show }))
}

fn print_usage() {
    println!("seeksage - ultra-low-latency text search over a crawled tree");
    println!("usage:");
    println!("  seeksage index  <PATH>              build the index, report build metrics");
    println!(
        "  seeksage search \"<QUERY>\" <PATH>   build the index, run one multi-term AND query"
    );
    println!("  seeksage repl   <PATH>              interactive shell: index built once, query repeatedly");
    println!("options:");
    println!(
        "  --lanes <N>  producer lanes (1..={MAX_LANES}, default {})",
        default_lanes()
    );
    println!("  --show  <N>  maximum matching lines to preview");
    println!("repl commands: /help  /stats  /show <N>  /index <PATH>  /quit");
}

// ---------------------------------------------------------------------------
// Path handling
// ---------------------------------------------------------------------------

fn resolve_root(raw: &str) -> io::Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(usage_error("<PATH> is empty"));
    }
    let p = PathBuf::from(trimmed);
    match fs::symlink_metadata(&p) {
        Ok(m) if m.is_dir() || m.is_file() => Ok(p),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("'{raw}' is neither a regular file nor a directory"),
        )),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("path '{raw}' is missing, locked, or not readable"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Build report
// ---------------------------------------------------------------------------

fn report_build(root: &Path, b: &Built, lane_count: usize) {
    println!("root      : {}", root.display());
    println!(
        "crawl     : dirs={} pruned={} hidden={} symlinks={} seen={} matched={} empty={} too_large={} binary={} faults={} bytes={:.2} MiB in {:.1} ms",
        b.walk.dirs_visited,
        b.walk.dirs_skipped,
        b.walk.hidden_skipped,
        b.walk.symlinks_skipped,
        b.walk.files_seen,
        b.walk.files_matched,
        b.walk.empty_files,
        b.walk.files_too_large,
        b.walk.files_binary,
        b.walk.files_faulted,
        b.walk.bytes_matched as f64 / (1024.0 * 1024.0),
        b.crawl_ms
    );
    println!(
        "ingest    : lanes={lane_count} x {LANE_CAP} slots, files={} binary={} oversized={} faults={} chunks={} lines={} payload={:.2} MiB wire={:.2} MiB in {:.1} ms",
        b.producers.files_read,
        b.producers.files_binary,
        b.producers.files_oversized,
        b.producers.files_faulted,
        b.producers.chunks,
        b.producers.lines,
        b.payload_mib(),
        b.wire_mib(),
        b.ingest_ms
    );
    println!("merge     : shards={lane_count} in {:.1} ms", b.merge_ms);
    println!(
        "index     : terms={} occurrences={} entries={} files={} lines={}",
        b.index.terms, b.index.occurrences, b.index.entries, b.index.files, b.index.lines
    );
    println!(
        "memory    : {:.2} MiB resident, {:.1} bytes/occurrence",
        b.memory_mib(),
        b.index.memory_bytes() as f64 / b.index.occurrences.max(1) as f64
    );
    println!(
        "build     : total {:.1} ms, throughput {:.2} MiB/s",
        b.total_ms(),
        b.throughput()
    );
    if b.consumer.bad_chunks > 0 {
        println!(
            "warning   : {} malformed chunks skipped by the consumer",
            b.consumer.bad_chunks
        );
    }
    for e in &b.lane_errors {
        println!("warning   : {e}");
    }
}

// ---------------------------------------------------------------------------
// Result formatting
// ---------------------------------------------------------------------------

/// Bounded LRU of line previews: at most `PREVIEW_CACHE_CAP` strings, each
/// clamped to `PREVIEW_WIDTH` characters. Every line comes from
/// `extract_snippet`, which seeks a fixed 1 KiB window around the hit, so no
/// file is ever read whole into RAM.
struct PreviewCache {
    /// `(file_id, line, preview)`, most recently used first.
    entries: Vec<(u32, u32, String)>,
}

impl PreviewCache {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
    }

    /// Preview the line containing `byte_offset` in file `file_id`.
    fn preview(&mut self, path: &Path, file_id: u32, line: u32, byte_offset: u64) -> String {
        if let Some(i) = self
            .entries
            .iter()
            .position(|&(f, l, _)| f == file_id && l == line)
        {
            // LRU hit: promote to the front, no I/O.
            let entry = self.entries.remove(i);
            self.entries.insert(0, entry.clone());
            return entry.2;
        }
        let text = match extract_snippet(path, byte_offset, PREVIEW_WIDTH) {
            Ok(s) if !s.is_empty() => s,
            Ok(_) => "<empty line>".to_string(),
            Err(e) => format!("<unreadable file: {e}"),
        };
        if self.entries.len() >= PREVIEW_CACHE_CAP {
            self.entries.pop(); // evict the least recently used preview
        }
        self.entries.insert(0, (file_id, line, text.clone()));
        text
    }
}

/// Print hit counts, the microsecond query time, and the previews.
/// Postings arrive sorted by `(file_id, line)`, so consecutive duplicates are
/// the same line matched by several terms.
fn render_hits(
    paths: &[PathBuf],
    hits: &[SearchResult],
    show: usize,
    us: f64,
    cache: &mut PreviewCache,
) -> io::Result<()> {
    let mut w = io::stdout().lock();
    if hits.is_empty() {
        writeln!(w, "time      : {us:.3} us   hits: 0   no matches")?;
        return Ok(());
    }

    // Deduplicate consecutive postings down to one preview per matching line,
    // keeping the byte offset of the first hit on that line.
    let mut uniq: Vec<(u32, u32, u64)> = Vec::with_capacity(hits.len());
    let mut prev: Option<(u32, u32)> = None;
    for r in hits {
        let k = (r.file_id, r.line);
        if prev != Some(k) {
            uniq.push((r.file_id, r.line, r.byte_offset));
            prev = Some(k);
        }
    }
    let mut file_ids: Vec<u32> = uniq.iter().map(|&(f, _, _)| f).collect();
    file_ids.sort_unstable();
    file_ids.dedup();

    writeln!(
        w,
        "time      : {us:.3} us   hits: {}   matching lines: {}   files: {}",
        hits.len(),
        uniq.len(),
        file_ids.len()
    )?;

    let display = uniq.len().min(show.max(1));
    for &(fid, line, offset) in uniq.iter().take(display) {
        let path = paths.get(fid as usize).cloned().unwrap_or_default();
        let text = cache.preview(&path, fid, line, offset);
        if path.as_os_str().is_empty() {
            writeln!(w, "  file #{fid}:{line}: {text}")?;
        } else {
            writeln!(w, "  {}:{}", path.display(), line)?;
            writeln!(w, "    L{line:>5} | {text}")?;
        }
    }
    if uniq.len() > display {
        writeln!(
            w,
            "  ... {} more matching lines (raise --show, or /show <N> in repl)",
            uniq.len() - display
        )?;
    }
    Ok(())
}

/// Build, run one query, print the report. Returns the measured microseconds.
fn run_query(built: &Built, query: &str, show: usize, cache: &mut PreviewCache) -> io::Result<f64> {
    let t = Instant::now();
    let hits = built.index.search(query);
    let us = t.elapsed().as_secs_f64() * 1e6;
    println!("query     : \"{query}\"");
    render_hits(&built.paths, &hits, show, us, cache)?;
    Ok(us)
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn cmd_index(rest: &[String], opts: &Options) -> io::Result<()> {
    if rest.is_empty() {
        return Err(usage_error("seeksage index <PATH>"));
    }
    let root = resolve_root(&rest[0])?;
    let built = build_index(&root, opts.lanes)?;
    report_build(&root, &built, opts.lanes);
    if built.paths.is_empty() {
        println!(
            "note      : no supported text files under this root (see TEXT_EXTENSIONS), index is empty"
        );
    }
    Ok(())
}

fn cmd_search(rest: &[String], opts: &Options) -> io::Result<()> {
    if rest.len() < 2 {
        return Err(usage_error("seeksage search \"<QUERY>\" <PATH>"));
    }
    // Documented order: query words first, path last. Extra words join into the query.
    let raw_path = rest[rest.len() - 1].clone();
    let query = rest[..rest.len() - 1].join(" ");
    let root = resolve_root(&raw_path)?;
    let built = build_index(&root, opts.lanes)?;
    report_build(&root, &built, opts.lanes);
    if query.trim().is_empty() {
        println!("note      : empty query, nothing to match");
        return Ok(());
    }
    // Previews are bounded: one fixed window read per matching line, cached in
    // a small LRU, so a multi-gigabyte repository never lands in RAM.
    let mut cache = PreviewCache::new();
    run_query(&built, &query, opts.show, &mut cache)?;
    Ok(())
}

fn is_quit(line: &str) -> bool {
    matches!(line, "quit" | "exit" | "/quit" | "/exit" | ":q" | "/exit()")
}

fn print_repl_help() {
    println!("repl commands:");
    println!("  <text>          multi-term AND query (case-insensitive, exact terms)");
    println!("  /stats          index size, query counters");
    println!("  /show <N>       matching lines to preview (current limit)");
    println!("  /index <PATH>   re-crawl and rebuild the index in place");
    println!("  /quit           leave the shell");
}

fn print_repl_stats(built: &Built, queries: usize, total_us: f64, worst_us: f64) {
    println!(
        "index     : terms={} occurrences={} files={} lines={} memory={:.2} MiB",
        built.index.terms,
        built.index.occurrences,
        built.index.files,
        built.index.lines,
        built.memory_mib()
    );
    if queries > 0 {
        println!(
            "queries   : {} avg={:.3} us worst={:.3} us total={:.1} ms",
            queries,
            total_us / queries as f64,
            worst_us,
            total_us / 1e3
        );
    } else {
        println!("queries   : 0");
    }
}

fn cmd_repl(rest: &[String], opts: &Options) -> io::Result<()> {
    if rest.is_empty() {
        return Err(usage_error("seeksage repl <PATH>"));
    }
    let root = resolve_root(&rest[0])?;
    let mut built = build_index(&root, opts.lanes)?;
    report_build(&root, &built, opts.lanes);
    println!("repl      : type a query, /help for commands, /quit to leave");

    // Previews are bounded: a small LRU of clamped lines, each read through a
    // fixed window, never a whole file.
    let mut cache = PreviewCache::new();
    let mut show = opts.show;
    let mut queries = 0usize;
    let mut total_us = 0f64;
    let mut worst_us = 0f64;

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    loop {
        print!("seeksage> ");
        io::stdout().flush()?;
        match lines.next() {
            None => {
                println!("bye");
                break;
            }
            Some(Err(e)) => {
                eprintln!("seeksage: stdin stopped: {e}");
                break;
            }
            Some(Ok(raw)) => {
                let line = raw.trim().to_string();
                if line.is_empty() {
                    continue;
                }
                if is_quit(&line) {
                    println!("bye");
                    break;
                }
                if line == "/help" {
                    print_repl_help();
                    continue;
                }
                if line == "/stats" {
                    print_repl_stats(&built, queries, total_us, worst_us);
                    continue;
                }
                if let Some(v) = line.strip_prefix("/show") {
                    let v = v.trim();
                    if v.is_empty() {
                        println!("show      : limit is {show} matching lines");
                        continue;
                    }
                    match v.parse::<usize>() {
                        Ok(n) => {
                            show = n;
                            println!("show      : {show} matching lines");
                        }
                        Err(_) => println!("seeksage: /show needs a number"),
                    }
                    continue;
                }
                if let Some(v) = line.strip_prefix("/index") {
                    let v = v.trim();
                    if v.is_empty() {
                        println!("/index <PATH> : re-crawl and rebuild in place");
                        continue;
                    }
                    match resolve_root(v) {
                        Ok(p) => {
                            cache.clear();
                            built = build_index(&p, opts.lanes)?;
                            report_build(&p, &built, opts.lanes);
                        }
                        Err(e) => println!("seeksage: {e}"),
                    }
                    continue;
                }
                let us = run_query(&built, &line, show, &mut cache)?;
                queries += 1;
                total_us += us;
                worst_us = worst_us.max(us);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn run() -> io::Result<()> {
    let argv: Vec<String> = env::args().skip(1).collect();
    let (pos, opts) = split_flags(&argv)?;
    let Some(cmd) = pos.first() else {
        print_usage();
        return Err(usage_error("no command given"));
    };
    match cmd.as_str() {
        "index" => cmd_index(&pos[1..], &opts),
        "search" => cmd_search(&pos[1..], &opts),
        "repl" => cmd_repl(&pos[1..], &opts),
        "help" | "--help" | "-h" => {
            print_usage();
            Ok(())
        }
        other => Err(usage_error(&format!(
            "unknown command '{other}' (expected index, search, repl, help)"
        ))),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("seeksage: {e}");
            ExitCode::FAILURE
        }
    }
}
