//! Result export: JSON, CSV, and Markdown renderers for the filtered hit list.
//!
//! An export is a stream, never a string. Every renderer writes straight into a
//! `std::io::Write` sink, so a 100k-line result set costs one bounded preview
//! per row ([`extract_snippet`] windows) and no accumulated document in RAM. The
//! sink can be a `Vec<u8>` in a test or a `BufWriter<File>` in the GUI worker
//! thread, and the byte count of the finished document is reported by the
//! [`Counted`] wrapper that every renderer runs through.
//!
//! Preview failures are counted, not fatal: a file deleted between the index
//! build and the export keeps its row, reports `missing`, and carries an
//! explanatory snippet, so a long export never dies half-way through.
//!
//! [`extract_snippet`]: crate::extract_snippet

use crate::extract_snippet;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Line preview width for exported rows: wider than the GUI list, narrow enough
/// that a snippet stays one line in every format.
pub const EXPORT_SNIPPET_CHARS: usize = 180;
/// Rows between progress callbacks: a 100k-line export reports 97 ticks instead
/// of 100k channel messages, so the UI stays cheap to keep alive.
pub const PROGRESS_STEP: usize = 1024;
/// CSV header line, exactly the three columns the export contract promises.
pub const CSV_HEADER: &str = "File,Line,Snippet";

// ---------------------------------------------------------------------------
// Format selection
// ---------------------------------------------------------------------------

/// One of the three export documents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ExportFormat {
    /// Structured hit list: `file_path`, `line`, `byte_offset`, `snippet`.
    #[default]
    Json,
    /// `File,Line,Snippet` rows with an RFC 4180 header.
    Csv,
    /// Bulleted header plus one Markdown table of the hits.
    Markdown,
}

impl ExportFormat {
    /// Every format in toolbar order.
    pub const ALL: [ExportFormat; 3] = [
        ExportFormat::Json,
        ExportFormat::Csv,
        ExportFormat::Markdown,
    ];

    /// Label shown on the format chips.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            ExportFormat::Json => "JSON",
            ExportFormat::Csv => "CSV",
            ExportFormat::Markdown => "Markdown",
        }
    }

    /// File extension this format is written as.
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            ExportFormat::Json => "json",
            ExportFormat::Csv => "csv",
            ExportFormat::Markdown => "md",
        }
    }

    /// Format implied by a file extension, `None` when the extension is not one
    /// of the export formats. Matching is case-insensitive and whitespace-tolerant
    /// because a dialog can return `"Results . JSON"`.
    #[must_use]
    pub fn from_extension(ext: &str) -> Option<ExportFormat> {
        match ext.trim().to_ascii_lowercase().as_str() {
            "json" => Some(ExportFormat::Json),
            "csv" => Some(ExportFormat::Csv),
            "md" | "markdown" => Some(ExportFormat::Markdown),
            _ => None,
        }
    }

    /// Format implied by a path's extension, falling back to `fallback` for an
    /// extension the export formats do not own (a dialog can type `results.txt`).
    #[must_use]
    pub fn for_path(path: &Path, fallback: ExportFormat) -> ExportFormat {
        path.extension()
            .and_then(|e| ExportFormat::from_extension(&e.to_string_lossy()))
            .unwrap_or(fallback)
    }

    /// Complete a path that carries no extension, so a dialog that returns
    /// `F:\reports\results` writes `F:\reports\results.md`.
    #[must_use]
    pub fn completed(self, path: &Path) -> PathBuf {
        let mut out = path.to_path_buf();
        if out.extension().is_none() {
            out.set_extension(self.extension());
        }
        out
    }

    /// Suggested file name for an export of `format`, inside `dir`.
    #[must_use]
    pub fn suggested_name(self, dir: &Path) -> String {
        dir.join(format!("seeksage-results.{}", self.extension()))
            .display()
            .to_string()
    }
}

// ---------------------------------------------------------------------------
// What gets written
// ---------------------------------------------------------------------------

/// Query context written into the export header so a saved document stays
/// self-describing years later.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExportMeta {
    pub root: String,
    pub query: String,
    pub ext_filter: String,
    /// Occurrences before line de-duplication, the number the index reports.
    pub total_hits: usize,
}

/// One export row: a line in a file, addressed by the index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportHit {
    pub path: PathBuf,
    pub line: u32,
    pub byte_offset: u64,
}

/// What an export produced: rows written, bytes flushed to the sink, and rows
/// whose preview could not be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportSummary {
    pub rows: usize,
    pub bytes: usize,
    pub missing: usize,
}

/// One sink that also counts every byte written through it.
struct Counted<W> {
    inner: W,
    bytes: usize,
}

impl<W: Write> Write for Counted<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Preview one row's line: the text, and `true` when the read failed.
fn preview(path: &Path, byte_offset: u64, chars: usize) -> (String, bool) {
    match extract_snippet(path, byte_offset, chars) {
        Ok(text) => (text, false),
        Err(e) => (format!("(preview failed: {e})"), true),
    }
}

// ---------------------------------------------------------------------------
// Escapes
// ---------------------------------------------------------------------------

/// `s` as a JSON string literal, quotes included. Control bytes below U+20 are
/// escaped as `\uXXXX` so a path with a stray control byte stays parseable.
#[must_use]
pub fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < '\u{20}' => {
                let code = c as u32;
                out.push_str(&format!("\\u{code:04x}"));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `s` as one CSV field: quoted and doubled-quote escaped only when the content
/// can move a column boundary (a comma, a quote, or a line break).
#[must_use]
pub fn csv_field(s: &str) -> String {
    if s.contains(['"', ',', '\n', '\r']) {
        let mut out = String::with_capacity(s.len() + 2);
        out.push('"');
        for c in s.chars() {
            if c == '"' {
                out.push_str("\"\"");
            } else {
                out.push(c);
            }
        }
        out.push('"');
        out
    } else {
        s.to_string()
    }
}

/// Longest run of backticks in `s`: a code span fence must be one longer than
/// any run inside it, so the span cannot terminate early.
fn longest_backtick_run(s: &str) -> usize {
    let mut best = 0usize;
    let mut run = 0usize;
    for c in s.chars() {
        if c == '`' {
            run += 1;
            best = best.max(run);
        } else {
            run = 0;
        }
    }
    best
}

/// `s` as one Markdown table cell: fenced as a code span, pipes escaped so a
/// snippet containing `|` cannot split its row, line breaks folded to spaces.
#[must_use]
pub fn md_cell(s: &str) -> String {
    let fence = "`".repeat(longest_backtick_run(s) + 1);
    let mut out = String::with_capacity(s.len() + fence.len() * 2);
    out.push_str(&fence);
    for c in s.chars() {
        match c {
            '|' => out.push_str("\\|"),
            '\n' | '\r' => out.push(' '),
            c => out.push(c),
        }
    }
    out.push_str(&fence);
    out
}

// ---------------------------------------------------------------------------
// Renderers
// ---------------------------------------------------------------------------

/// Every renderer reports rows written, previews lost, and calls `progress`
/// after each [`PROGRESS_STEP`] rows and once more at the end, so a long export
/// can be watched from the UI thread without blocking it.
fn write_json<W, P>(
    w: &mut W,
    meta: &ExportMeta,
    hits: &[ExportHit],
    chars: usize,
    mut progress: P,
) -> io::Result<(usize, usize)>
where
    W: Write,
    P: FnMut(usize),
{
    writeln!(w, "{{")?;
    writeln!(w, "  \"generator\": \"SeekSage\",")?;
    writeln!(
        w,
        "  \"version\": {},",
        json_string(env!("CARGO_PKG_VERSION"))
    )?;
    writeln!(w, "  \"root\": {},", json_string(&meta.root))?;
    writeln!(w, "  \"query\": {},", json_string(&meta.query))?;
    writeln!(w, "  \"ext_filter\": {},", json_string(&meta.ext_filter))?;
    writeln!(w, "  \"rows\": {},", hits.len())?;
    writeln!(w, "  \"total_hits\": {},", meta.total_hits)?;
    writeln!(w, "  \"results\": [")?;

    let mut rows = 0usize;
    let mut missing = 0usize;
    for hit in hits {
        let (snippet, failed) = preview(&hit.path, hit.byte_offset, chars);
        rows += 1;
        if failed {
            missing += 1;
        }
        write!(
            w,
            "    {{ \"file_path\": {}, \"line\": {}, \"byte_offset\": {}, \"snippet\": {} }}",
            json_string(&hit.path.display().to_string()),
            hit.line,
            hit.byte_offset,
            json_string(&snippet)
        )?;
        if rows < hits.len() {
            writeln!(w, ",")?;
        } else {
            writeln!(w)?;
        }
        if rows.is_multiple_of(PROGRESS_STEP) {
            progress(rows);
        }
    }
    progress(rows);

    writeln!(w, "  ]")?;
    writeln!(w, "}}")?;
    Ok((rows, missing))
}

fn write_csv<W, P>(
    w: &mut W,
    hits: &[ExportHit],
    chars: usize,
    mut progress: P,
) -> io::Result<(usize, usize)>
where
    W: Write,
    P: FnMut(usize),
{
    writeln!(w, "{CSV_HEADER}")?;
    let mut rows = 0usize;
    let mut missing = 0usize;
    for hit in hits {
        let (snippet, failed) = preview(&hit.path, hit.byte_offset, chars);
        rows += 1;
        if failed {
            missing += 1;
        }
        writeln!(
            w,
            "{},{},{}",
            csv_field(&hit.path.display().to_string()),
            hit.line,
            csv_field(&snippet)
        )?;
        if rows.is_multiple_of(PROGRESS_STEP) {
            progress(rows);
        }
    }
    progress(rows);
    Ok((rows, missing))
}

fn write_markdown<W, P>(
    w: &mut W,
    meta: &ExportMeta,
    hits: &[ExportHit],
    chars: usize,
    mut progress: P,
) -> io::Result<(usize, usize)>
where
    W: Write,
    P: FnMut(usize),
{
    writeln!(w, "# SeekSage export")?;
    writeln!(w)?;
    writeln!(w, "- **root**: {}", md_cell(&meta.root))?;
    writeln!(w, "- **query**: {}", md_cell(&meta.query))?;
    writeln!(w, "- **ext filter**: {}", md_cell(&meta.ext_filter))?;
    writeln!(
        w,
        "- **rows**: {} · **hits**: {}",
        hits.len(),
        meta.total_hits
    )?;
    writeln!(w)?;
    writeln!(w, "| File | Line | Snippet |")?;
    writeln!(w, "| --- | --- | --- |")?;

    let mut rows = 0usize;
    let mut missing = 0usize;
    for hit in hits {
        let (snippet, failed) = preview(&hit.path, hit.byte_offset, chars);
        rows += 1;
        if failed {
            missing += 1;
        }
        writeln!(
            w,
            "| {} | {} | {} |",
            md_cell(&hit.path.display().to_string()),
            hit.line,
            md_cell(&snippet)
        )?;
        if rows.is_multiple_of(PROGRESS_STEP) {
            progress(rows);
        }
    }
    progress(rows);
    Ok((rows, missing))
}

/// Render `hits` into `w` in `format`, counting every byte that reaches the
/// sink. `w` can be any writer, so a test captures into a `Vec<u8>` and the GUI
/// worker streams into a `BufWriter<File>`.
pub fn write_export<W, P>(
    w: &mut W,
    format: ExportFormat,
    meta: &ExportMeta,
    hits: &[ExportHit],
    snippet_chars: usize,
    progress: P,
) -> io::Result<ExportSummary>
where
    W: Write,
    P: FnMut(usize),
{
    let mut counted = Counted {
        inner: w,
        bytes: 0usize,
    };
    let (rows, missing) = match format {
        ExportFormat::Json => write_json(&mut counted, meta, hits, snippet_chars, progress)?,
        ExportFormat::Csv => write_csv(&mut counted, hits, snippet_chars, progress)?,
        ExportFormat::Markdown => {
            write_markdown(&mut counted, meta, hits, snippet_chars, progress)?
        }
    };
    counted.flush()?;
    Ok(ExportSummary {
        rows,
        bytes: counted.bytes,
        missing,
    })
}

/// Render and write one export file in one pass: a `BufWriter` over the target
/// file, so a large result set streams to disk without building the document in
/// memory. `progress` runs on the calling thread, so the GUI worker can forward
/// ticks to the UI over a channel.
pub fn write_file_export<P>(
    target: &Path,
    format: ExportFormat,
    meta: &ExportMeta,
    hits: &[ExportHit],
    snippet_chars: usize,
    progress: P,
) -> io::Result<ExportSummary>
where
    P: FnMut(usize),
{
    let mut file = std::fs::File::create(target)?;
    let mut buf = io::BufWriter::new(&mut file);
    let summary = write_export(&mut buf, format, meta, hits, snippet_chars, progress)?;
    buf.flush()?;
    Ok(summary)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp_root(tag: &str) -> PathBuf {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(tag);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    fn fixture(root: &Path, name: &str, body: &str) -> PathBuf {
        let path = root.join(name);
        std::fs::write(&path, body).expect("write fixture");
        path
    }

    fn meta(root: &Path) -> ExportMeta {
        ExportMeta {
            root: root.display().to_string(),
            query: String::from("alpha"),
            ext_filter: String::from(".rs"),
            total_hits: 2,
        }
    }

    /// The JSON document carries all four fields per row, the header carries the
    /// query context, and the byte offsets are real numbers, not strings.
    #[test]
    fn json_carries_all_four_fields_per_row() {
        let root = tmp_root("export-json");
        let a = fixture(&root, "a.rs", "line zero\nalpha here\nthird\n");
        let b = fixture(&root, "b.rs", "nothing\nALPHA second line\n");
        let hits = vec![
            ExportHit {
                path: a.clone(),
                line: 1,
                byte_offset: 10,
            },
            ExportHit {
                path: b.clone(),
                line: 1,
                byte_offset: 7,
            },
        ];

        let mut out: Vec<u8> = Vec::new();
        let summary = write_export(
            &mut out,
            ExportFormat::Json,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |_rows| {},
        )
        .expect("json export must render");
        let doc = String::from_utf8(out).expect("json is utf-8");

        assert_eq!(summary.rows, 2);
        assert_eq!(summary.missing, 0);
        assert_eq!(summary.bytes, doc.len());
        assert!(doc.contains("\"file_path\": \""), "doc: {doc}");
        assert!(doc.contains("\"line\": 1"), "doc: {doc}");
        assert!(doc.contains("\"byte_offset\": 10"), "doc: {doc}");
        assert!(doc.contains("\"snippet\": \"alpha here\""), "doc: {doc}");
        assert!(doc.contains("\"query\": \"alpha\""), "doc: {doc}");
        assert!(doc.contains("\"total_hits\": 2"), "doc: {doc}");
        assert!(doc.contains("\"results\": ["), "doc: {doc}");
        // Two rows, one comma between them, no trailing comma before the close.
        assert_eq!(doc.matches("file_path").count(), 2);
        assert_eq!(doc.matches(",\n    {").count(), 1);
        assert!(
            !doc.contains(",\n  ]"),
            "json must not have a trailing comma"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Quotes, backslashes, and control bytes inside a snippet are escaped, so
    /// the document stays parseable.
    #[test]
    fn json_escapes_the_dangerous_characters() {
        let root = tmp_root("export-escape");
        let p = fixture(&root, "q.rs", "say \"hi\" and C:\\tmp\n");
        let hits = [ExportHit {
            path: p,
            line: 0,
            byte_offset: 0,
        }];
        let mut out: Vec<u8> = Vec::new();
        write_export(
            &mut out,
            ExportFormat::Json,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |_rows| {},
        )
        .expect("json export must render");
        let doc = String::from_utf8(out).expect("json is utf-8");

        assert!(doc.contains("\\\"hi\\\""), "escaped quotes: {doc}");
        assert!(doc.contains("C:\\\\tmp"), "escaped backslash: {doc}");
        assert_eq!(json_string("a\u{7}b"), "\"a\\u0007b\"");
        assert_eq!(json_string(""), "\"\"");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// CSV emits the documented header, quotes only what can move a column
    /// boundary, and doubles quotes inside a quoted field.
    #[test]
    fn csv_writes_the_documented_header_and_quotes() {
        let root = tmp_root("export-csv");
        let plain = fixture(&root, "plain.rs", "alpha plain line\n");
        let messy = fixture(&root, "messy.rs", "alpha, \"quoted\", line\n");
        let plain_display = plain.display().to_string();
        let plain_field = csv_field(&plain_display);
        let messy_field = csv_field(&messy.display().to_string());
        let hits = vec![
            ExportHit {
                path: plain,
                line: 0,
                byte_offset: 0,
            },
            ExportHit {
                path: messy,
                line: 0,
                byte_offset: 0,
            },
        ];
        let mut out: Vec<u8> = Vec::new();
        let summary = write_export(
            &mut out,
            ExportFormat::Csv,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |_rows| {},
        )
        .expect("csv export must render");
        let doc = String::from_utf8(out).expect("csv is utf-8");

        assert_eq!(summary.rows, 2);
        assert_eq!(summary.bytes, doc.len());
        let mut lines = doc.lines();
        assert_eq!(lines.next(), Some(CSV_HEADER));
        // A path with no comma, quote, or line break needs no quoting at all.
        assert_eq!(plain_field, plain_display, "unquoted path field");
        let first = lines.next().expect("first row");
        assert_eq!(
            first,
            format!("{plain_field},0,alpha plain line"),
            "row: {first}"
        );
        // A snippet carrying a comma and a quote is quoted, quotes doubled.
        let second = lines.next().expect("second row");
        assert_eq!(
            second,
            format!("{messy_field},0,\"alpha, \"\"quoted\"\", line\""),
            "row: {second}"
        );
        assert_eq!(
            second.matches(",0,").count(),
            1,
            "the line column is intact"
        );
        // A plain field needs no quoting at all.
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Markdown is a header block plus one table: pipes inside a snippet cannot
    /// split a row, and a backtick run inside a snippet forces a longer fence.
    #[test]
    fn markdown_table_survives_pipes_and_backticks() {
        let root = tmp_root("export-md");
        let snippet = "alpha | beta `gamma`";
        let body = format!("{snippet}\n");
        let p = fixture(&root, "m.rs", &body);
        let p_cell = md_cell(&p.display().to_string());
        let snippet_cell = md_cell(snippet);
        let hits = [ExportHit {
            path: p,
            line: 0,
            byte_offset: 0,
        }];
        let mut out: Vec<u8> = Vec::new();
        let summary = write_export(
            &mut out,
            ExportFormat::Markdown,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |_rows| {},
        )
        .expect("markdown export must render");
        let doc = String::from_utf8(out).expect("markdown is utf-8");

        assert_eq!(summary.rows, 1);
        let row = doc
            .lines()
            .find(|l| l.starts_with("| `"))
            .expect("table row");
        // Three cells, and the pipe inside the snippet is escaped, so the row
        // cannot be split by the snippet's own pipe.
        let cells = row.split(" | ").collect::<Vec<_>>();
        assert_eq!(cells.len(), 3, "row: {row}");
        assert_eq!(cells[0], format!("| {p_cell}"), "file column: {row}");
        assert_eq!(cells[1], "0", "line column: {row}");
        assert_eq!(
            cells[2],
            format!("{snippet_cell} |"),
            "snippet column: {row}"
        );
        assert!(row.contains("\\|"), "escaped pipe: {row}");
        // One isolated backtick inside forces a two-backtick fence; the snippet's
        // own trailing backtick sits next to the closing fence.
        assert_eq!(snippet_cell, "``alpha \\| beta `gamma```");
        assert!(doc.contains("| File | Line | Snippet |"), "header row");
        assert!(doc.contains("| --- | --- | --- |"), "separator row");
        assert!(doc.contains("- **query**: `alpha`"), "header block");
        assert!(doc.contains("- **rows**: 1 · **hits**: 2"), "header counts");
        // A lone backtick forces a two-backtick fence, a double run forces three.
        assert_eq!(md_cell("a`b"), "``a`b``");
        assert_eq!(md_cell("a``b"), "```a``b```");
        assert_eq!(md_cell("a|b"), "`a\\|b`");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The format comes from the file extension a dialog returns, case- and
    /// space-tolerant, with an extension the export formats do not own falling
    /// back to the caller's format, and a missing extension completed.
    #[test]
    fn format_is_chosen_from_the_chosen_file_extension() {
        assert_eq!(
            ExportFormat::from_extension("json"),
            Some(ExportFormat::Json)
        );
        assert_eq!(ExportFormat::from_extension("CSV"), Some(ExportFormat::Csv));
        assert_eq!(
            ExportFormat::from_extension("  Markdown "),
            Some(ExportFormat::Markdown)
        );
        assert_eq!(ExportFormat::from_extension("txt"), None);

        let p = PathBuf::from("F:\\reports\\Results.JSON");
        assert_eq!(
            ExportFormat::for_path(&p, ExportFormat::Csv),
            ExportFormat::Json
        );
        let p = PathBuf::from("F:\\reports\\results.txt");
        assert_eq!(
            ExportFormat::for_path(&p, ExportFormat::Csv),
            ExportFormat::Csv
        );

        let completed = ExportFormat::Markdown.completed(Path::new("F:\\reports\\results"));
        assert_eq!(completed.display().to_string(), "F:\\reports\\results.md");
        let kept = ExportFormat::Markdown.completed(Path::new("F:\\reports\\x.csv"));
        assert_eq!(kept.display().to_string(), "F:\\reports\\x.csv");
        assert_eq!(ExportFormat::Json.extension(), "json");
        assert_eq!(ExportFormat::Csv.label(), "CSV");
        assert_eq!(ExportFormat::ALL.len(), 3);
        assert_eq!(
            ExportFormat::Json.suggested_name(Path::new("D:\\")),
            "D:\\seeksage-results.json"
        );
        assert_eq!(
            ExportFormat::Csv.suggested_name(Path::new("D:\\")),
            "D:\\seeksage-results.csv"
        );
    }

    /// A file deleted after the index build keeps its row, counts as `missing`,
    /// and never aborts the export.
    #[test]
    fn a_missing_preview_is_counted_not_fatal() {
        let root = tmp_root("export-missing");
        let live = fixture(&root, "live.rs", "alpha live\n");
        let gone = root.join("gone.rs");
        let hits = vec![
            ExportHit {
                path: live,
                line: 0,
                byte_offset: 0,
            },
            ExportHit {
                path: gone,
                line: 0,
                byte_offset: 4,
            },
        ];
        let mut out: Vec<u8> = Vec::new();
        let summary = write_export(
            &mut out,
            ExportFormat::Csv,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |_rows| {},
        )
        .expect("csv export must render");
        let doc = String::from_utf8(out).expect("csv is utf-8");

        assert_eq!(summary.rows, 2, "every row survives");
        assert_eq!(summary.missing, 1, "exactly one preview failed");
        assert!(doc.contains("(preview failed"), "doc: {doc}");
        assert_eq!(doc.lines().count(), 3, "header + 2 rows");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Progress ticks every `PROGRESS_STEP` rows and reports the final row count
    /// once, so the UI can watch a long export without a message per row.
    #[test]
    fn progress_ticks_at_the_step_and_reports_the_final_rows() {
        let root = tmp_root("export-progress");
        let p = fixture(&root, "many.rs", "alpha line\nalpha line\nalpha line\n");
        let hits = [ExportHit {
            path: p,
            line: 0,
            byte_offset: 0,
        }];
        let mut ticks: Vec<usize> = Vec::new();
        let mut out: Vec<u8> = Vec::new();
        let summary = write_export(
            &mut out,
            ExportFormat::Csv,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |rows| ticks.push(rows),
        )
        .expect("csv export must render");

        assert_eq!(summary.rows, 1);
        assert_eq!(ticks, vec![1]);

        // Above the step, the tick lands exactly on the step boundary.
        let step = PROGRESS_STEP;
        let big: Vec<ExportHit> = (0..step + 3)
            .map(|i| ExportHit {
                path: root.join("many.rs"),
                line: (i % 3) as u32,
                byte_offset: (i * 11) as u64,
            })
            .collect();
        let mut ticks = Vec::new();
        let mut out: Vec<u8> = Vec::new();
        let summary = write_export(
            &mut out,
            ExportFormat::Csv,
            &meta(&root),
            &big,
            EXPORT_SNIPPET_CHARS,
            |rows| ticks.push(rows),
        )
        .expect("big csv export must render");
        assert_eq!(summary.rows, step + 3);
        assert_eq!(ticks, vec![step, step + 3]);
        assert_eq!(out.len(), summary.bytes, "the counter counts every byte");
        assert_eq!(String::from_utf8_lossy(&out).lines().count(), step + 4);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The file form streams the same document to disk that the writer form
    /// streams into memory, with the same summary.
    #[test]
    fn the_file_form_writes_what_the_writer_form_renders() {
        let root = tmp_root("export-file");
        let p = fixture(&root, "f.rs", "alpha in file\nbeta\n");
        let hits = [ExportHit {
            path: p,
            line: 0,
            byte_offset: 0,
        }];
        let mut out: Vec<u8> = Vec::new();
        let in_memory = write_export(
            &mut out,
            ExportFormat::Markdown,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |_rows| {},
        )
        .expect("markdown export must render");

        let target = root.join("out.md");
        let on_disk = write_file_export(
            &target,
            ExportFormat::Markdown,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |_rows| {},
        )
        .expect("file export must write");

        assert_eq!(on_disk.rows, in_memory.rows);
        assert_eq!(on_disk.bytes, in_memory.bytes);
        let written = std::fs::read(&target).expect("read back the export");
        assert_eq!(written.len(), in_memory.bytes);
        assert_eq!(
            String::from_utf8(written).expect("utf-8"),
            String::from_utf8(out).expect("utf-8")
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A directory that does not exist is an I/O error surfaced to the caller,
    /// not a panic.
    #[test]
    fn an_unwritable_target_is_an_io_error() {
        let root = tmp_root("export-bad");
        let hits = [ExportHit {
            path: root.join("x.rs"),
            line: 0,
            byte_offset: 0,
        }];
        let target = root.join("missing-dir").join("out.json");
        let res = write_file_export(
            &target,
            ExportFormat::Json,
            &meta(&root),
            &hits,
            EXPORT_SNIPPET_CHARS,
            |_rows| {},
        );
        assert!(res.is_err(), "unwritable target must report an error");
        let _ = std::fs::remove_dir_all(&root);
    }
}
