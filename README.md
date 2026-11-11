# SeekSage

[![Download Windows Executable](https://img.shields.io/badge/Download-seeksage--gui.exe%20(v0.1.0--beta)-2ea44f?style=for-the-badge&logo=windows)](https://github.com/deanthonycarton-ops/seeksage/releases/download/v0.1.0-beta/seeksage-gui.exe)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPLv3-blue.svg?style=for-the-badge)](LICENSE)


**Microsecond local code & log search engine with one-click LLM context packing.**
Built from scratch in pure Rust with a lock-free ingestion pipeline, memory-resident inverted index, C-ABI FFI surface, and native `egui` desktop console.

## Why SeekSage?

Line-by-line scanners (`grep`, `ripgrep`) rescan the disk or page cache on every query. **SeekSage** indexes a directory tree once across parallel producer lanes into a compact, memory-resident inverted index (`16 bytes` per posting) and answers subsequent queries in **5-35 us**.

- **Microsecond Query Latency (`us`)**: Prefix, exact, and multi-term `AND` queries with galloping posting-list intersection.
- **One-Click AI Context Packer**: Groups filtered matches by file, merges overlapping context windows so no line is ever duplicated, estimates token count (`~1.2k tokens`), and copies an LLM-ready Markdown block straight to the clipboard.
- **Cross-File Log Timeline**: Parses ISO-8601 and bracketed timestamps (`YYYY-MM-DD HH:MM:SS.mmm`, `[HH:MM:SS.sss]`) on the fly, interleaving hits from multiple log files into a single chronological stream with relative time-delta badges (`+0ms`, `+125ms`, `+4.75s`).
- **Scope-Aware Result Cards**: Automatically detects the enclosing function, struct, class, or config section (`fn search()`, `struct Config`, `[BaseSettings]`) above each hit.
- **Inline Query Operators**: Narrow results on the fly with negative terms (`-error`), inline path filters (`path:mono`, `-path:backup`), and extension filters (`ext:`).
- **Zero-Dependency Live File Watcher**: Optional debounced background metadata fingerprinting (`Watch`) that automatically rebuilds the index when files are saved in an external editor.
- **Full Bilingual Folding**: Zero-allocation ASCII and UTF-8 Cyrillic case folding (including `Ё`/`ё` -> `е` normalization even across split chunk boundaries).

## Quick Start

Run the native desktop console:

    cargo run --release --bin seeksage-gui

Or pass a target directory on launch:

    cargo run --release --bin seeksage-gui -- "C:\path\to\project"

Run the full test suite:

    cargo test

## License

GPL-3.0-only

