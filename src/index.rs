//! In-memory inverted index over the `SeekSage` chunk stream.
//!
//! Design:
//! - A posting list is a flat, sorted `Vec<Posting>`. `Posting` packs
//!   `(file_id, line)` into one `u64` key and keeps the 64-bit byte offset:
//!   16 bytes per occurrence, contiguous memory, no per-entry heap allocation.
//! - Ingestion runs inside a thread-private [`IndexShard`]. Tokens are hashed
//!   incrementally over raw bytes (a token may span chunk boundaries), so the
//!   hot loop performs no `String` allocation, and posting lists grow amortized.
//!   ASCII and UTF-8 Cyrillic uppercase are folded to canonical lowercase
//!   inside the hash pipeline, with one `pending_lead` byte per file carrying a
//!   2-byte sequence split across two chunks, so `А..Я`, `Р..Я`, and `Ё`/`ё`
//!   hash identically to their lowercase forms, split or not.
//! - Shards are merged in parallel: the global key space is partitioned into
//!   contiguous ranges, every merge thread writes its own disjoint slice of the
//!   shared posting arena, and merged lists stay sorted for galloping
//!   intersection.
//! - After the merge the index is immutable: lookups are a binary search over
//!   sorted keys, so the query path needs no hash table and no locking.

use crate::parser::{ChunkRef, TextChunk, FLAG_EOF, FLAG_LINE_END, FLAG_LINE_START};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::thread;

/// FNV-1a offset basis.
pub const HASH_SEED: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a prime.
pub const HASH_PRIME: u64 = 0x0000_0100_0000_01b3;
/// Hard cap on tracked `file_id` values: guards against a stream emitting a
/// bogus id that would blow up the per-file state table.
pub const MAX_TRACKED_FILES: usize = 8 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Tokenizer and hash
// ---------------------------------------------------------------------------

/// A byte continues a token when it is an ASCII alphanumeric, an underscore,
/// or a UTF-8 continuation byte (>= 0x80). ASCII whitespace, control bytes,
/// and ASCII punctuation are separators.
const fn is_token_char(b: u8) -> bool {
    b >= 0x80 || b == b'_' || b.wrapping_sub(b'0') <= 9 || (b | 0x20).wrapping_sub(b'a') <= 25
}

const fn build_token_table() -> [bool; 256] {
    let mut t = [false; 256];
    let mut i = 0usize;
    while i < 256 {
        t[i] = is_token_char(i as u8);
        i += 1;
    }
    t
}

/// `TOKEN_TABLE[b]` is true when byte `b` continues a token.
const TOKEN_TABLE: [bool; 256] = build_token_table();

#[inline]
fn token_byte(b: u8) -> bool {
    TOKEN_TABLE[b as usize]
}

#[inline]
fn lower_ascii(b: u8) -> u8 {
    if b.wrapping_sub(b'A') <= 25 {
        b + 32
    } else {
        b
    }
}

/// Canonical form of one 2-byte UTF-8 Cyrillic pair, folded to lowercase:
///
/// * `D0 90..=9F` (А..П) -> `D0 B0..=BF` (а..п)
/// * `D0 A0..=AF` (Р..Я) -> `D1 80..=8F` (р..я)
/// * `D0 81` (Ё) and `D1 91` (ё) -> `D0 B5` (е): е and ё are unified, so an
///   index built from `Ёжик` answers `ёжик` and `ежик` identically.
///
/// Any other pair is returned byte-for-byte, so unhandled sequences hash
/// exactly as stored. The folded bytes are always `>= 0x80`, so `lower_ascii`
/// is a no-op on them.
#[inline]
#[must_use]
pub const fn fold_cyrillic_pair(lead: u8, second: u8) -> (u8, u8) {
    match (lead, second) {
        (0xD0, 0x90..=0x9F) => (0xD0, second + 0x20),
        (0xD0, 0xA0..=0xAF) => (0xD1, second - 0x20),
        (0xD0, 0x81) => (0xD0, 0xB5),
        (0xD1, 0x91) => (0xD0, 0xB5),
        _ => (lead, second),
    }
}

/// Canonical lowercase bytes for a token: ASCII upper folded, Cyrillic
/// uppercase pairs folded via [`fold_cyrillic_pair`], everything else kept
/// byte-for-byte. These are the bytes the FNV hash consumes, so a canonical
/// token string and its hash always agree: two spellings that hash identically
/// canonicalize identically, and prefix matching on canonical bytes is exactly
/// prefix matching on tokens. A trailing, unpaired Cyrillic lead byte stays raw,
/// matching `hash_update_deferred`'s raw flush.
#[must_use]
pub fn canonicalize(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if matches!(b, 0xC2..=0xDF) {
            if i + 1 < bytes.len() {
                let (l, s) = fold_cyrillic_pair(b, bytes[i + 1]);
                out.push(lower_ascii(l));
                out.push(lower_ascii(s));
                i += 2;
                continue;
            }
            out.push(lower_ascii(b));
            i += 1;
            continue;
        }
        out.push(lower_ascii(b));
        i += 1;
    }
    out
}

/// Split `query` into canonical token bytes in order. Duplicates are kept: the
/// AND pipeline intersects the same list with itself, which is a no-op, so a
/// repeated term never breaks a query.
#[must_use]
pub fn tokenize_bytes(query: &str) -> Vec<Vec<u8>> {
    let bytes = query.as_bytes();
    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        while i < bytes.len() && !token_byte(bytes[i]) {
            i += 1;
        }
        let start = i;
        while i < bytes.len() && token_byte(bytes[i]) {
            i += 1;
        }
        if i > start {
            out.push(canonicalize(&bytes[start..i]));
        }
    }
    out
}

/// Compare a canonical term against a prefix, treating a term shorter than the
/// prefix as smaller: a term can only match a prefix it is at least as long as.
fn prefix_cmp(term: &[u8], prefix: &[u8]) -> std::cmp::Ordering {
    let m = term.len().min(prefix.len());
    let c = term[..m].cmp(&prefix[..m]);
    if c != std::cmp::Ordering::Equal {
        return c;
    }
    if term.len() < prefix.len() {
        std::cmp::Ordering::Less
    } else {
        std::cmp::Ordering::Equal
    }
}

/// Push two canonical bytes through one FNV-1a multiply per byte.
#[inline]
fn hash_pair(h: u64, lead: u8, second: u8) -> u64 {
    let h = (h ^ lower_ascii(lead) as u64).wrapping_mul(HASH_PRIME);
    (h ^ lower_ascii(second) as u64).wrapping_mul(HASH_PRIME)
}

/// Extend an incremental FNV-1a hash with `term`, canonicalizing ASCII *and*
/// Cyrillic uppercase to lowercase. One multiply per byte, no allocation, and
/// no `String`: 2-byte Cyrillic pairs are folded as units inside the pipeline.
#[inline]
pub fn hash_update(h: u64, term: &[u8]) -> u64 {
    hash_update_deferred(h, term, false).0
}

/// Same as [`hash_update`]. When `defer_lead` is set and `term` ends with the
/// lead byte of a 2-byte sequence, that byte is left out of `h` and returned
/// as `pending_lead` (`0` = none): its continuation byte lives in the next
/// chunk of the same line, the pair is folded there, and a token split across
/// chunk boundaries hashes exactly like an unsplit token.
fn hash_update_deferred(h: u64, term: &[u8], defer_lead: bool) -> (u64, u8) {
    let mut h = h;
    let n = term.len();
    let mut i = 0usize;
    while i < n {
        let b = term[i];
        if matches!(b, 0xC2..=0xDF) {
            // Lead byte of a 2-byte sequence.
            if i + 1 < n {
                let (l, s) = fold_cyrillic_pair(b, term[i + 1]);
                h = hash_pair(h, l, s);
                i += 2;
                continue;
            }
            if defer_lead {
                return (h, b);
            }
            h = (h ^ lower_ascii(b) as u64).wrapping_mul(HASH_PRIME);
            i += 1;
            continue;
        }
        h = (h ^ lower_ascii(b) as u64).wrapping_mul(HASH_PRIME);
        i += 1;
    }
    (h, 0)
}

/// Final mix for slot locality; the result is never zero (0 is the map
/// sentinel), enforced by `| 1`.
#[inline]
pub fn hash_finalize(h: u64) -> u64 {
    let mut x = h ^ (h >> 29);
    x = x.wrapping_mul(0x3c79_ac49_2ba7_b65d);
    x ^= x >> 32;
    x | 1
}

#[inline]
pub fn term_hash(term: &[u8]) -> u64 {
    hash_finalize(hash_update(HASH_SEED, term))
}

/// Split `query` into lowercase term hashes, sorted and deduplicated.
pub fn tokenize(query: &str) -> Vec<u64> {
    let bytes = query.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        while i < bytes.len() && !token_byte(bytes[i]) {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        while i < bytes.len() && token_byte(bytes[i]) {
            i += 1;
        }
        out.push(term_hash(&bytes[start..i]));
    }
    out.sort_unstable();
    out.dedup();
    out
}

#[inline]
pub fn pack_key(file_id: u32, line: u32) -> u64 {
    ((file_id as u64) << 32) | (line as u64)
}

#[inline]
pub fn key_file(key: u64) -> u32 {
    (key >> 32) as u32
}

#[inline]
pub fn key_line(key: u64) -> u32 {
    key as u32
}

// ---------------------------------------------------------------------------
// Posting layout
// ---------------------------------------------------------------------------

/// One token occurrence: packed `(file_id, line)` key and absolute byte offset
/// of the token start inside the file. 16 bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Posting {
    pub key: u64,
    pub offset: u64,
}

/// Contiguous posting-list descriptor into the global arena.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct PostList {
    pub start: u32,
    pub len: u32,
}

/// A hit: which file, which line, and where the term starts.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SearchResult {
    pub file_id: u32,
    pub line: u32,
    pub byte_offset: u64,
}

impl SearchResult {
    #[inline]
    pub fn from_posting(p: &Posting) -> Self {
        Self {
            file_id: key_file(p.key),
            line: key_line(p.key),
            byte_offset: p.offset,
        }
    }
}

// ---------------------------------------------------------------------------
// Open-addressed map (shard hot path)
// ---------------------------------------------------------------------------

/// Open-addressed map: term hash -> posting-list id. Linear probing over a
/// power-of-two table, load factor 0.5, key `0` reserved as the empty slot.
struct OpenMap {
    keys: Vec<u64>,
    ids: Vec<u32>,
    mask: usize,
    len: usize,
}

impl OpenMap {
    fn new(cap: usize) -> Self {
        let cap = cap.max(2).next_power_of_two();
        Self {
            keys: vec![0u64; cap],
            ids: vec![0u32; cap],
            mask: cap - 1,
            len: 0,
        }
    }

    #[inline]
    fn get(&self, h: u64) -> Option<u32> {
        let mut i = h as usize & self.mask;
        loop {
            let k = self.keys[i];
            if k == 0 {
                return None;
            }
            if k == h {
                return Some(self.ids[i]);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Return the list id for `h`, creating it with `candidate` when absent.
    /// The second value is true when a new list must be appended to the shard.
    #[inline]
    fn place(&mut self, h: u64, candidate: u32) -> (u32, bool) {
        let mut i = h as usize & self.mask;
        loop {
            let k = self.keys[i];
            if k == h {
                return (self.ids[i], false);
            }
            if k == 0 {
                self.keys[i] = h;
                self.ids[i] = candidate;
                self.len += 1;
                if self.len * 2 > self.keys.len() {
                    self.grow();
                }
                return (candidate, true);
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let cap = self.keys.len() * 2;
        let old_keys = std::mem::replace(&mut self.keys, vec![0u64; cap]);
        let old_ids = std::mem::replace(&mut self.ids, vec![0u32; cap]);
        self.mask = cap - 1;
        for (k, id) in old_keys.into_iter().zip(old_ids) {
            if k == 0 {
                continue;
            }
            let mut i = k as usize & self.mask;
            while self.keys[i] != 0 {
                i = (i + 1) & self.mask;
            }
            self.keys[i] = k;
            self.ids[i] = id;
        }
    }

    /// Live `(hash, list id)` pairs in slot order.
    fn live(&self) -> impl Iterator<Item = (u64, u32)> + '_ {
        self.keys
            .iter()
            .zip(self.ids.iter())
            .filter(|(k, _)| **k != 0)
            .map(|(k, id)| (*k, *id))
    }
}

/// Per-file reconstruction state: the consumer sees chunks in lane FIFO order,
/// so lines and byte offsets are rebuilt with no extra buffering.
#[derive(Clone, Copy, Default)]
struct FileState {
    pos: u64,
    line_no: u32,
    completed: u64,
    partial_start: u64,
    partial_hash: u64,
    has_partial: bool,
    /// Lead byte of a 2-byte UTF-8 sequence whose continuation byte lives in
    /// the next chunk of the same line. `0` means none (a lead byte is never
    /// `0`). It keeps split Cyrillic pairs folding as a unit across chunk
    /// boundaries with one byte of state, no heap allocation.
    pending_lead: u8,
}

impl FileState {
    fn fresh() -> Self {
        Self {
            pos: 0,
            line_no: 0,
            completed: 0,
            partial_start: 0,
            partial_hash: HASH_SEED,
            has_partial: false,
            pending_lead: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Thread-local shard (ingestion side)
// ---------------------------------------------------------------------------

/// A thread-private index shard built by consuming wire chunks.
pub struct IndexShard {
    map: OpenMap,
    lists: Vec<Vec<Posting>>,
    state: Vec<FileState>,
    /// Canonical lowercase bytes for one posting list, indexed by list id. This
    /// is what prefix search reads: a query's canonical token bytes are matched
    /// against these, so `"play"` finds `"player"` and `"абсурд"` finds
    /// `"абсурдные"` — exactly the tokens the FNV hash already unifies.
    term_of_list: Vec<Vec<u8>>,
    /// Raw bytes of the current cross-chunk partial token, per file, indexed by
    /// `file_id` alongside `state`. Canonicalized on flush into `term_of_list`;
    /// kept in a side table so `FileState` stays `Copy`.
    partial_bytes: Vec<Vec<u8>>,
    pub chunks: u64,
    pub wire_bytes: u64,
    pub payload_bytes: u64,
    pub occurrences: u64,
    pub lines: u64,
    pub bad_chunks: u64,
    pub skipped_files: u64,
}

impl Default for IndexShard {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexShard {
    pub fn new() -> Self {
        Self {
            map: OpenMap::new(4096),
            lists: Vec::new(),
            state: Vec::new(),
            term_of_list: Vec::new(),
            partial_bytes: Vec::new(),
            chunks: 0,
            wire_bytes: 0,
            payload_bytes: 0,
            occurrences: 0,
            lines: 0,
            bad_chunks: 0,
            skipped_files: 0,
        }
    }

    #[inline]
    pub fn terms(&self) -> usize {
        self.lists.len()
    }

    #[inline]
    pub fn files(&self) -> usize {
        self.state.len()
    }

    /// Consume one wire-encoded chunk record, as delivered by the lane consumer.
    #[inline]
    pub fn ingest_wire(&mut self, wire: &[u8]) {
        self.wire_bytes += wire.len() as u64;
        match TextChunk::decode(wire) {
            Some(c) => self.ingest_chunk(&c),
            None => self.bad_chunks += 1,
        }
    }

    /// Index one decoded chunk: rebuild line numbers, split raw bytes on
    /// whitespace and punctuation, append postings. Raw bytes of a token that
    /// runs to the chunk end are kept in `partial_bytes` so the canonical
    /// spelling can be reconstructed when the token flushes.
    pub fn ingest_chunk(&mut self, c: &ChunkRef<'_>) {
        self.chunks += 1;
        self.payload_bytes += c.data_len as u64;
        let flags = c.flags;
        let id = c.file_id as usize;
        if id >= MAX_TRACKED_FILES {
            self.skipped_files += 1;
            return;
        }
        if id >= self.state.len() {
            self.state.resize(id + 1, FileState::fresh());
            self.partial_bytes.resize(id + 1, Vec::new());
        }

        let f = self.state[id];
        let mut pos = f.pos;
        let mut line_no = f.line_no;
        let mut completed = f.completed;
        let mut partial_start = f.partial_start;
        let mut partial_hash = f.partial_hash;
        let mut has_partial = f.has_partial;
        let mut pending = f.pending_lead;
        // Raw bytes of the open partial token for this file; taken out so the
        // ingest loop can grow it while `push_token` mutates `self`.
        let mut partial = std::mem::take(&mut self.partial_bytes[id]);

        if flags & FLAG_LINE_START != 0 {
            line_no = (completed + 1) as u32;
            // A completed line closes everything, so a pending lead byte never
            // crosses a line boundary.
            pending = 0;
        }
        let key = pack_key(c.file_id, line_no);
        let base = pos;
        let data = c.data;
        let n = data.len();

        let mut i = 0usize;
        if n > 0 && token_byte(data[0]) {
            if has_partial {
                // The leading run continues the token pending from the previous
                // chunk of the same line.
                let mut start = 0usize;
                if pending != 0 {
                    // The deferred lead byte pairs with this byte: fold them as
                    // one canonical unit inside the hash pipeline. `partial`
                    // already ends with that lead byte, so appending just the
                    // continuation byte rebuilds the pair: pushing both copies
                    // the lead twice and `canonicalize` reads the duplicate as
                    // a stray pair, leaving term bytes that no prefix can match.
                    let (l, s) = fold_cyrillic_pair(pending, data[0]);
                    partial_hash = hash_pair(partial_hash, l, s);
                    partial.push(data[0]);
                    start = 1;
                }
                while i < n && token_byte(data[i]) {
                    i += 1;
                }
                let (h, def) = hash_update_deferred(partial_hash, &data[start..i], i == n);
                partial_hash = h;
                partial.extend_from_slice(&data[start..i]);
                if i < n {
                    let canon = canonicalize(&partial);
                    self.push_token(hash_finalize(partial_hash), key, partial_start, canon);
                    partial.clear();
                    has_partial = false;
                    partial_hash = HASH_SEED;
                    pending = 0;
                } else {
                    pending = def;
                }
            }
        } else if n > 0 && has_partial {
            // This chunk opens on a separator, so the token pending at the end
            // of the previous chunk is complete: close it with its deferred
            // lead byte hashed raw, then start this chunk's tokens fresh.
            if pending != 0 {
                partial_hash = (partial_hash ^ pending as u64).wrapping_mul(HASH_PRIME);
                partial.push(pending);
                pending = 0;
            }
            let canon = canonicalize(&partial);
            self.push_token(hash_finalize(partial_hash), key, partial_start, canon);
            partial.clear();
            has_partial = false;
            partial_hash = HASH_SEED;
        }
        while i < n {
            while i < n && !token_byte(data[i]) {
                i += 1;
            }
            if i >= n {
                break;
            }
            let start = i;
            while i < n && token_byte(data[i]) {
                i += 1;
            }
            if i < n {
                // Token completes inside this chunk.
                let h = term_hash(&data[start..i]);
                let canon = canonicalize(&data[start..i]);
                self.push_token(h, key, base + start as u64, canon);
            } else {
                // Token runs to the chunk end: it may continue in the next
                // chunk of the same line, so keep the incremental hash, defer a
                // trailing lead byte into `pending_lead`, and keep the raw bytes.
                if !has_partial {
                    has_partial = true;
                    partial_start = base + start as u64;
                    partial.clear();
                }
                let (h, def) = hash_update_deferred(partial_hash, &data[start..i], true);
                partial_hash = h;
                partial.extend_from_slice(&data[start..i]);
                pending = def;
            }
        }
        pos += n as u64;

        if flags & (FLAG_LINE_END | FLAG_EOF) != 0 {
            if has_partial {
                if pending != 0 {
                    // The line closes on an unpaired lead byte: hash it raw.
                    partial_hash = (partial_hash ^ pending as u64).wrapping_mul(HASH_PRIME);
                    partial.push(pending);
                }
                let canon = canonicalize(&partial);
                self.push_token(hash_finalize(partial_hash), key, partial_start, canon);
                partial.clear();
                has_partial = false;
                partial_hash = HASH_SEED;
            }
            if flags & FLAG_LINE_END != 0 {
                pos += 1; // the newline byte, stripped from the payload
                completed += 1;
                self.lines += 1;
            }
            pending = 0;
        }

        self.state[id] = FileState {
            pos,
            line_no,
            completed,
            partial_start,
            partial_hash,
            has_partial,
            pending_lead: pending,
        };
        self.partial_bytes[id] = partial;
    }

    #[inline]
    fn push_token(&mut self, h: u64, key: u64, offset: u64, canon: Vec<u8>) {
        let candidate = self.lists.len() as u32;
        let (lid, fresh) = self.map.place(h, candidate);
        if fresh {
            self.lists.push(Vec::new());
            self.term_of_list.push(canon);
        }
        // A repeat token already has its canonical bytes in `term_of_list`; the
        // freshly canonicalized copy is simply dropped.
        self.lists[lid as usize].push(Posting { key, offset });
        self.occurrences += 1;
    }

    #[inline]
    pub fn hash_postings(&self, h: u64) -> &[Posting] {
        match self.map.get(h) {
            Some(lid) => &self.lists[lid as usize],
            None => &[],
        }
    }

    #[inline]
    pub fn term_postings(&self, term: &str) -> &[Posting] {
        self.hash_postings(term_hash(term.as_bytes()))
    }

    pub fn search(&self, query: &str) -> Vec<SearchResult> {
        let toks = tokenize_bytes(query);
        if toks.is_empty() {
            return Vec::new();
        }
        let mut lists: Vec<Vec<Posting>> = Vec::new();
        for t in &toks {
            let merged = self.prefix_postings(t);
            if merged.is_empty() {
                return Vec::new();
            }
            lists.push(merged);
        }
        search_owned(lists, usize::MAX)
    }

    /// Same as [`IndexShard::search`], stopping as soon as `limit` hits are
    /// collected.
    pub fn search_limited(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let toks = tokenize_bytes(query);
        if toks.is_empty() {
            return Vec::new();
        }
        let mut lists: Vec<Vec<Posting>> = Vec::new();
        for t in &toks {
            let merged = self.prefix_postings(t);
            if merged.is_empty() {
                return Vec::new();
            }
            lists.push(merged);
        }
        search_owned(lists, limit)
    }

    /// Merge every posting list whose canonical term starts with `prefix`.
    ///
    /// One matching term: the list is returned as it stands, already sorted by
    /// key, with every occurrence kept - a line where the prefix appears twice
    /// is two hits for that one token.
    ///
    /// Several matching terms: merged and deduped by `key` in sorted order. A
    /// line containing several matching spellings appears once: the union means
    /// "some token on this line starts with the prefix".
    fn prefix_postings(&self, prefix: &[u8]) -> Vec<Posting> {
        let mut matched: Vec<usize> = Vec::new();
        for (lid, term) in self.term_of_list.iter().enumerate() {
            if term.starts_with(prefix) {
                matched.push(lid);
            }
        }
        if matched.len() == 1 {
            return self.lists[matched[0]].clone();
        }
        let mut out: Vec<Posting> = Vec::new();
        for lid in matched {
            out.extend_from_slice(&self.lists[lid]);
        }
        out.sort_unstable_by_key(|p| p.key);
        out.dedup_by_key(|p| p.key);
        out
    }

    /// Every shard posting list is sorted by key (per lane, chunks arrive in
    /// ascending file and line order).
    pub fn lists_sorted(&self) -> bool {
        self.lists
            .iter()
            .all(|l| l.windows(2).all(|w| w[0].key <= w[1].key))
    }
}

// ---------------------------------------------------------------------------
// Global index (query side)
// ---------------------------------------------------------------------------

/// Merged, immutable inverted index: sorted term keys, list descriptors, and
/// one packed posting arena.
pub struct GlobalIndex {
    keys: Vec<u64>,
    lists: Vec<PostList>,
    arena: Vec<Posting>,
    /// Canonical term bytes concatenated; term `i` lives at
    /// `term_bytes[canon_off[i]..canon_off[i]+canon_len[i]]`, parallel to `keys`.
    term_bytes: Vec<u8>,
    canon_off: Vec<u32>,
    canon_len: Vec<u32>,
    /// `keys` indices sorted by canonical term bytes, so every term sharing a
    /// prefix is a contiguous run: prefix search binary-searches this order.
    byte_key_idx: Vec<u32>,
    pub files: u64,
    pub chunks: u64,
    pub payload_bytes: u64,
    pub lines: u64,
    pub occurrences: u64,
    pub entries: u64,
    pub terms: u64,
}

impl GlobalIndex {
    /// Posting list for a term hash; empty when the term is absent.
    #[inline]
    pub fn posting(&self, h: u64) -> &[Posting] {
        match self.keys.binary_search(&h) {
            Ok(i) => {
                let l = self.lists[i];
                &self.arena[l.start as usize..(l.start + l.len) as usize]
            }
            Err(_) => &[],
        }
    }

    #[inline]
    pub fn term_postings(&self, term: &str) -> &[Posting] {
        self.posting(term_hash(term.as_bytes()))
    }

    /// Keyword AND, widened to prefixes: every query token expands to the union
    /// of postings for all canonical terms starting with that token, and the
    /// per-token unions are then intersected line-by-line. Because canonical
    /// bytes agree with the FNV hash, an exact spelling is always inside its
    /// own prefix union, so exact lookups stay correct while `"play"` now also
    /// reaches `"player"` and `"абсурд"` reaches `"абсурдные"`.
    pub fn search(&self, query: &str) -> Vec<SearchResult> {
        let toks = tokenize_bytes(query);
        if toks.is_empty() {
            return Vec::new();
        }
        let mut lists: Vec<Vec<Posting>> = Vec::new();
        for t in &toks {
            let merged = self.prefix_postings(t);
            if merged.is_empty() {
                return Vec::new();
            }
            lists.push(merged);
        }
        search_owned(lists, usize::MAX)
    }

    /// Same as [`GlobalIndex::search`], stopping as soon as `limit` hits are
    /// collected.
    pub fn search_limited(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let toks = tokenize_bytes(query);
        if toks.is_empty() {
            return Vec::new();
        }
        let mut lists: Vec<Vec<Posting>> = Vec::new();
        for t in &toks {
            let merged = self.prefix_postings(t);
            if merged.is_empty() {
                return Vec::new();
            }
            lists.push(merged);
        }
        search_owned(lists, limit)
    }

    /// Query by precomputed term hashes (no tokenizing cost on the hot path).
    pub fn search_hashes(&self, hashes: &[u64]) -> Vec<SearchResult> {
        let mut lists: Vec<Vec<Posting>> = Vec::new();
        for &h in hashes {
            let p = self.posting(h);
            if p.is_empty() {
                return Vec::new();
            }
            lists.push(p.to_vec());
        }
        search_owned(lists, usize::MAX)
    }

    /// The byte-sorted run of canonical terms that start with `prefix`, as
    /// positions in `byte_key_idx`. Terms are lexicographic, so prefix matches
    /// are contiguous: two binary searches bound the run.
    fn prefix_range(&self, prefix: &[u8]) -> (usize, usize) {
        let n = self.byte_key_idx.len();
        let term = |pos: usize| -> &[u8] {
            let ki = self.byte_key_idx[pos] as usize;
            let off = self.canon_off[ki] as usize;
            &self.term_bytes[off..off + self.canon_len[ki] as usize]
        };
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if prefix_cmp(term(mid), prefix) == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let start = lo;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if prefix_cmp(term(mid), prefix) == std::cmp::Ordering::Greater {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        (start, lo)
    }

    /// Merge every posting list whose canonical term starts with `prefix`,
    /// deduped by `key` in sorted order: "some token on this line begins with
    /// the prefix", each matching line counted once. A run of exactly one term
    /// keeps every occurrence, since that term is the only spelling matched and
    /// repeated occurrences on one line are repeated hits for that token.
    fn prefix_postings(&self, prefix: &[u8]) -> Vec<Posting> {
        let (lo, hi) = self.prefix_range(prefix);
        if hi - lo == 1 {
            let ki = self.byte_key_idx[lo] as usize;
            let l = self.lists[ki];
            return self.arena[l.start as usize..(l.start + l.len) as usize].to_vec();
        }
        let mut out: Vec<Posting> = Vec::new();
        for pos in lo..hi {
            let ki = self.byte_key_idx[pos] as usize;
            let l = self.lists[ki];
            out.extend_from_slice(&self.arena[l.start as usize..(l.start + l.len) as usize]);
        }
        out.sort_unstable_by_key(|p| p.key);
        out.dedup_by_key(|p| p.key);
        out
    }

    /// Hottest `n` terms by posting length.
    pub fn hot_terms(&self, n: usize) -> Vec<u64> {
        let mut idx: Vec<usize> = (0..self.keys.len()).collect();
        idx.sort_by_key(|i| std::cmp::Reverse(self.lists[*i].len as usize));
        idx.into_iter().take(n).map(|i| self.keys[i]).collect()
    }

    /// Coldest `n` terms by posting length: cheapest queries.
    pub fn cold_terms(&self, n: usize) -> Vec<u64> {
        let mut idx: Vec<usize> = (0..self.keys.len()).collect();
        idx.sort_by_key(|i| self.lists[*i].len as usize);
        idx.into_iter().take(n).map(|i| self.keys[i]).collect()
    }

    /// A term hash guaranteed to be absent, for early-termination tests.
    pub fn absent_hash(&self) -> u64 {
        let mut h = 1u64;
        while self.keys.binary_search(&h).is_ok() {
            h += 2;
        }
        h
    }

    /// Resident index footprint in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.keys.len() * std::mem::size_of::<u64>()
            + self.lists.len() * std::mem::size_of::<PostList>()
            + self.arena.len() * std::mem::size_of::<Posting>()
            + self.term_bytes.len()
            + self.canon_off.len() * std::mem::size_of::<u32>()
            + self.canon_len.len() * std::mem::size_of::<u32>()
            + self.byte_key_idx.len() * std::mem::size_of::<u32>()
    }

    /// Every merged posting list is sorted by key.
    pub fn lists_sorted(&self) -> bool {
        self.lists.iter().all(|l| {
            let s = &self.arena[l.start as usize..(l.start + l.len) as usize];
            s.windows(2).all(|w| w[0].key <= w[1].key)
        })
    }
}

/// Split `n` items into `t` contiguous ranges.
fn split_ranges(n: usize, t: usize) -> Vec<(usize, usize)> {
    let t = t.min(n).max(1);
    let base = n / t;
    let rem = n % t;
    let mut out = Vec::with_capacity(t);
    let mut cur = 0usize;
    for r in 0..t {
        let len = base + if r < rem { 1 } else { 0 };
        out.push((cur, cur + len));
        cur += len;
    }
    out
}

/// Merge shards in parallel: partition the sorted key space, write disjoint
/// arena slices from scoped threads, keep every merged list sorted.
pub fn merge_shards(shards: Vec<IndexShard>, threads: usize) -> GlobalIndex {
    let mut keys: Vec<u64> = Vec::new();
    for s in &shards {
        keys.extend(s.map.live().map(|(k, _)| k));
    }
    keys.sort_unstable();
    keys.dedup();

    // Entries per global key = sum of the per-shard list lengths.
    let mut counts = vec![0u32; keys.len()];
    for s in &shards {
        for (k, lid) in s.map.live() {
            let i = keys.binary_search(&k).expect("key set built from shards");
            counts[i] += s.lists[lid as usize].len() as u32;
        }
    }

    let total: usize = counts.iter().map(|&c| c as usize).sum();
    if total > u32::MAX as usize {
        panic!("posting arena exceeds the u32 address space");
    }
    let mut arena = vec![Posting { key: 0, offset: 0 }; total];
    let mut lists = vec![PostList { start: 0, len: 0 }; keys.len()];

    let ranges = split_ranges(keys.len(), threads.max(1));
    let mut starts: Vec<usize> = Vec::with_capacity(ranges.len());
    let mut acc = 0usize;
    for &(a, b) in &ranges {
        starts.push(acc);
        acc += counts[a..b].iter().map(|&c| c as usize).sum::<usize>();
    }

    let shards_ref: &[IndexShard] = &shards;

    thread::scope(|scope| {
        let mut arena_rest = &mut arena[..];
        let mut lists_rest = &mut lists[..];
        let mut handles = Vec::with_capacity(ranges.len());
        for (r, &base) in ranges.iter().zip(starts.iter()) {
            let span = counts[r.0..r.1].iter().sum::<u32>() as usize;
            let (a_arena, rest_a) = arena_rest.split_at_mut(span);
            let (a_lists, rest_l) = lists_rest.split_at_mut(r.1 - r.0);
            arena_rest = rest_a;
            lists_rest = rest_l;

            let keys_slice = &keys[r.0..r.1];
            let counts_slice = &counts[r.0..r.1];

            handles.push(scope.spawn(move || {
                let mut cur = 0usize;
                for (i, &k) in keys_slice.iter().enumerate() {
                    let span = counts_slice[i] as usize;
                    let dst = &mut a_arena[cur..cur + span];
                    let mut w = 0usize;
                    for s in shards_ref {
                        if let Some(lid) = s.map.get(k) {
                            let src = &s.lists[lid as usize];
                            dst[w..w + src.len()].copy_from_slice(src);
                            w += src.len();
                        }
                    }
                    dst.sort_unstable_by_key(|p| p.key);
                    a_lists[i] = PostList {
                        start: (base + cur) as u32,
                        len: span as u32,
                    };
                    cur += span;
                }
            }));
        }
        for h in handles {
            if h.join().is_err() {
                panic!("merge thread panicked");
            }
        }
    });

    // Canonical term bytes: every key maps to one canonical spelling (the bytes
    // consumed by its FNV hash), identical across shards. Keep the spelling for
    // each global key from the first shard that holds it.
    let mut hash_canon: HashMap<u64, &Vec<u8>> = HashMap::new();
    for s in &shards {
        for (k, lid) in s.map.live() {
            hash_canon.entry(k).or_insert(&s.term_of_list[lid as usize]);
        }
    }
    let mut term_bytes: Vec<u8> = Vec::new();
    let mut canon_off: Vec<u32> = Vec::with_capacity(keys.len());
    let mut canon_len: Vec<u32> = Vec::with_capacity(keys.len());
    for &k in &keys {
        canon_off.push(term_bytes.len() as u32);
        let t = hash_canon[&k];
        canon_len.push(t.len() as u32);
        term_bytes.extend_from_slice(t);
    }
    // Sort key indices by canonical bytes so prefix matches form a contiguous
    // run for the byte-sorted binary search in `prefix_range`.
    let mut byte_key_idx: Vec<u32> = (0..keys.len() as u32).collect();
    byte_key_idx.sort_unstable_by_key(|&ki| {
        let off = canon_off[ki as usize] as usize;
        let len = canon_len[ki as usize] as usize;
        &term_bytes[off..off + len]
    });

    let terms = keys.len() as u64;
    GlobalIndex {
        keys,
        lists,
        arena,
        term_bytes,
        canon_off,
        canon_len,
        byte_key_idx,
        files: shards
            .iter()
            .map(|s| s.state.len() as u64)
            .max()
            .unwrap_or(0),
        chunks: shards.iter().map(|s| s.chunks).sum(),
        payload_bytes: shards.iter().map(|s| s.payload_bytes).sum(),
        lines: shards.iter().map(|s| s.lines).sum(),
        occurrences: shards.iter().map(|s| s.occurrences).sum(),
        entries: total as u64,
        terms,
    }
}

// ---------------------------------------------------------------------------
// Intersection and query execution
// ---------------------------------------------------------------------------

/// Lower bound: first index in `[lo, hi)` with `key` <= element key.
#[inline]
fn lower_bound(slice: &[Posting], key: u64, lo: usize, hi: usize) -> usize {
    let mut lo = lo;
    let mut hi = hi;
    while lo < hi {
        let mid = lo + ((hi - lo) >> 1);
        if slice[mid].key < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Galloping search: first index >= `key` starting from `from`, exponential
/// probe then a binary search inside the found window.
#[inline]
fn gallop(slice: &[Posting], from: usize, key: u64) -> usize {
    if from >= slice.len() {
        return slice.len();
    }
    if slice[from].key >= key {
        return from;
    }
    let mut lo = from;
    let mut step = 1usize;
    loop {
        let hi = lo + step;
        if hi >= slice.len() {
            return lower_bound(slice, key, lo + 1, slice.len());
        }
        if slice[hi].key >= key {
            return lower_bound(slice, key, lo + 1, hi);
        }
        lo = hi;
        step = step.saturating_mul(2);
    }
}

/// Sorted intersection of two posting lists with early termination.
/// The shorter list drives; position in the longer list is monotone.
fn intersect(a: &[Posting], b: &[Posting], out: &mut Vec<Posting>, limit: usize) {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut start = 0usize;
    for p in short {
        let key = p.key;
        let pos = gallop(long, start, key);
        if pos >= long.len() {
            break; // the driver cannot match any remaining element
        }
        if long[pos].key == key {
            out.push(*p);
            start = pos + 1;
            if out.len() >= limit {
                break;
            }
        } else {
            start = pos;
        }
    }
}

/// AND-intersect a set of owned, key-sorted posting lists. `lists[i]` holds the
/// merged postings for one query token, key-sorted by the prefix-union rule.
/// The shortest list seeds the accumulator and each longer list narrows it, so
/// the tightest token does the least work. This is the shared query tail for
/// exact, prefix, and cache-decoded hits: a token expands to its prefix union,
/// and the tokens are intersected line-by-line.
fn search_owned(mut lists: Vec<Vec<Posting>>, limit: usize) -> Vec<SearchResult> {
    if lists.is_empty() || limit == 0 {
        return Vec::new();
    }
    lists.sort_by_key(|p| p.len());
    // Every list arrives key-sorted, so `acc` needs no re-sorting: one term's
    // own list is appended in key order, and a merged prefix union is sorted
    // before it is returned. Dropping the sort keeps repeated occurrences of a
    // single term, which is what one token on a line with several of them means.
    let mut acc = std::mem::take(&mut lists[0]);
    let mut buf = Vec::new();
    for next in &lists[1..] {
        buf.clear();
        intersect(acc.as_slice(), next, &mut buf, limit);
        std::mem::swap(&mut acc, &mut buf);
        if acc.is_empty() {
            return Vec::new();
        }
        if acc.len() >= limit {
            break;
        }
    }
    acc.truncate(limit);
    acc.iter().map(SearchResult::from_posting).collect()
}

// ---------------------------------------------------------------------------
// Serialization: write and read a `GlobalIndex` to/from the on-disk cache
// ---------------------------------------------------------------------------
//
// The cache is a raw little-endian byte stream: a `u32` count for every
// variable-length field, then the field's fixed-size records. The reader
// validates every length against the bytes that are left, so a corrupt file
// returns `None` instead of allocating on a bogus count.

pub(crate) fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_f64(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_bits().to_le_bytes());
}

pub(crate) fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

pub(crate) fn put_path(out: &mut Vec<u8>, p: &Path) {
    put_str(out, &p.display().to_string());
}

pub(crate) fn put_paths(out: &mut Vec<u8>, paths: &[PathBuf]) {
    put_u32(out, paths.len() as u32);
    for p in paths {
        put_path(out, p);
    }
}

pub(crate) struct Reader<'b> {
    b: &'b [u8],
    i: usize,
}

impl<'b> Reader<'b> {
    pub(crate) fn new(b: &'b [u8]) -> Self {
        Reader { b, i: 0 }
    }

    /// Bytes left to read.
    pub(crate) fn remaining(&self) -> usize {
        self.b.len() - self.i
    }

    pub(crate) fn take(&mut self, n: usize) -> Option<&'b [u8]> {
        if self.i + n > self.b.len() {
            return None;
        }
        let s = &self.b[self.i..self.i + n];
        self.i += n;
        Some(s)
    }

    pub(crate) fn u32(&mut self) -> Option<u32> {
        let s = self.take(4)?;
        Some(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    pub(crate) fn u64(&mut self) -> Option<u64> {
        let s = self.take(8)?;
        Some(u64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }

    pub(crate) fn f64(&mut self) -> Option<f64> {
        Some(f64::from_bits(self.u64()?))
    }

    pub(crate) fn string(&mut self) -> Option<String> {
        let len = self.u32()? as usize;
        let s = self.take(len)?;
        String::from_utf8(s.to_vec()).ok()
    }

    pub(crate) fn path(&mut self) -> Option<PathBuf> {
        Some(PathBuf::from(self.string()?))
    }
}

impl GlobalIndex {
    /// Append the whole index, in field order, to `out`.
    pub(crate) fn serialize_into(&self, out: &mut Vec<u8>) {
        put_u32(out, self.keys.len() as u32);
        for k in &self.keys {
            put_u64(out, *k);
        }
        put_u32(out, self.lists.len() as u32);
        for l in &self.lists {
            put_u32(out, l.start);
            put_u32(out, l.len);
        }
        put_u32(out, self.arena.len() as u32);
        for p in &self.arena {
            put_u64(out, p.key);
            put_u64(out, p.offset);
        }
        put_u64(out, self.files);
        put_u64(out, self.chunks);
        put_u64(out, self.payload_bytes);
        put_u64(out, self.lines);
        put_u64(out, self.occurrences);
        put_u64(out, self.entries);
        put_u64(out, self.terms);
        // Canonical term bytes: the concatenated spellings, their offsets/lens
        // parallel to `keys`, and the byte-sorted key order for prefix search.
        put_u32(out, self.term_bytes.len() as u32);
        out.extend_from_slice(&self.term_bytes);
        for v in &self.canon_off {
            put_u32(out, *v);
        }
        for v in &self.canon_len {
            put_u32(out, *v);
        }
        for v in &self.byte_key_idx {
            put_u32(out, *v);
        }
    }

    /// Full structural equality with another index: every posting list, the
    /// posting arena, the canonical term bytes, and every counter. The cache
    /// round-trip test uses this to prove a decoded index is byte-for-byte the
    /// index that was built.
    pub fn same_index(&self, other: &GlobalIndex) -> bool {
        self.keys == other.keys
            && self.lists == other.lists
            && self.arena == other.arena
            && self.term_bytes == other.term_bytes
            && self.canon_off == other.canon_off
            && self.canon_len == other.canon_len
            && self.byte_key_idx == other.byte_key_idx
            && self.files == other.files
            && self.chunks == other.chunks
            && self.payload_bytes == other.payload_bytes
            && self.lines == other.lines
            && self.occurrences == other.occurrences
            && self.entries == other.entries
            && self.terms == other.terms
    }

    /// Read an index from `r`, returning `None` on any truncation.
    pub(crate) fn deserialize(r: &mut Reader<'_>) -> Option<GlobalIndex> {
        let nkeys = r.u32()? as usize;
        if r.remaining() < nkeys.checked_mul(8)? {
            return None;
        }
        let mut keys = Vec::with_capacity(nkeys);
        for _ in 0..nkeys {
            keys.push(r.u64()?);
        }
        let nlists = r.u32()? as usize;
        if r.remaining() < nlists.checked_mul(8)? {
            return None;
        }
        let mut lists = Vec::with_capacity(nlists);
        for _ in 0..nlists {
            lists.push(PostList {
                start: r.u32()?,
                len: r.u32()?,
            });
        }
        let narena = r.u32()? as usize;
        if r.remaining() < narena.checked_mul(16)? {
            return None;
        }
        let mut arena = Vec::with_capacity(narena);
        for _ in 0..narena {
            arena.push(Posting {
                key: r.u64()?,
                offset: r.u64()?,
            });
        }
        let files = r.u64()?;
        let chunks = r.u64()?;
        let payload_bytes = r.u64()?;
        let lines = r.u64()?;
        let occurrences = r.u64()?;
        let entries = r.u64()?;
        let terms = r.u64()?;

        // Canonical term bytes. A corrupt cache must fail closed: every spelling
        // must fit inside the byte blob, and the byte-sorted key order must stay
        // in range. The blob length is a byte count, not a key count, so it is
        // read as-is; the offset/len/index tables carry one entry per key.
        let nterm = r.u32()? as usize;
        let term_bytes = r.take(nterm)?.to_vec();
        let mut canon_off = Vec::with_capacity(nkeys);
        for _ in 0..nkeys {
            canon_off.push(r.u32()?);
        }
        let mut canon_len = Vec::with_capacity(nkeys);
        for _ in 0..nkeys {
            canon_len.push(r.u32()?);
        }
        for i in 0..nkeys {
            let end = canon_off[i] as usize + canon_len[i] as usize;
            if end > term_bytes.len() {
                return None;
            }
        }
        let mut byte_key_idx = Vec::with_capacity(nkeys);
        for _ in 0..nkeys {
            let ki = r.u32()?;
            if ki as usize >= nkeys {
                return None;
            }
            byte_key_idx.push(ki);
        }

        Some(GlobalIndex {
            keys,
            lists,
            arena,
            term_bytes,
            canon_off,
            canon_len,
            byte_key_idx,
            files,
            chunks,
            payload_bytes,
            lines,
            occurrences,
            entries,
            terms,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{CHUNK_HEADER, FLAG_CONTINUED};

    /// Build the wire bytes for one chunk record, exactly as a producer encodes
    /// a lane slot payload.
    fn wire(file_id: u32, seq: u32, flags: u8, text: &str) -> Vec<u8> {
        let mut c = TextChunk::new(file_id, seq, flags);
        c.fill(text.as_bytes());
        let mut out = vec![0u8; CHUNK_HEADER + c.data_len as usize];
        let n = c.encode(&mut out);
        out.truncate(n);
        out
    }

    fn feed(shard: &mut IndexShard, file_id: u32, parts: &[(&str, u8)]) {
        for (i, (text, flags)) in parts.iter().enumerate() {
            shard.ingest_wire(&wire(file_id, i as u32, *flags, text));
        }
    }

    /// Wire bytes for a raw payload, so a chunk boundary can cut a multi-byte
    /// UTF-8 sequence in half.
    fn wire_bytes(file_id: u32, seq: u32, flags: u8, payload: &[u8]) -> Vec<u8> {
        let mut c = TextChunk::new(file_id, seq, flags);
        c.fill(payload);
        let mut out = vec![0u8; CHUNK_HEADER + payload.len()];
        let n = c.encode(&mut out);
        out.truncate(n);
        out
    }

    fn feed_bytes(shard: &mut IndexShard, file_id: u32, parts: &[(&[u8], u8)]) {
        for (i, (payload, flags)) in parts.iter().enumerate() {
            shard.ingest_wire(&wire_bytes(file_id, i as u32, *flags, payload));
        }
    }

    #[test]
    fn tokens_split_on_whitespace_and_punctuation() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            0,
            &[("alpha, beta;gamma!", FLAG_LINE_START | FLAG_LINE_END)],
        );
        assert_eq!(s.occurrences, 3);
        // Offsets: alpha=0, beta=7, gamma=12.
        assert_eq!(s.term_postings("alpha")[0].offset, 0);
        assert_eq!(s.term_postings("beta")[0].offset, 7);
        assert_eq!(s.term_postings("gamma")[0].offset, 12);
        assert_eq!(s.term_postings("beta")[0].key, pack_key(0, 1));
        assert_eq!(s.lines, 1);
    }

    #[test]
    fn case_insensitive_exact_matching() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            0,
            &[("Hello World", FLAG_LINE_START | FLAG_LINE_END)],
        );
        let hits = s.search("HELLO world");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].line, 1);
        assert_eq!(hits[0].file_id, 0);
    }

    #[test]
    fn cyrillic_uppercase_matches_lowercase_queries() {
        let text = "Глава Первая: Ёжик";
        let mut s = IndexShard::new();
        feed(&mut s, 0, &[(text, FLAG_LINE_START | FLAG_LINE_END)]);
        assert_eq!(s.occurrences, 3);
        // Every case variant lands on one term hash.
        for q in ["глава", "ГЛАВА", "Глава", "гЛаВа"] {
            assert_eq!(s.term_postings(q).len(), 1, "query '{q}'");
        }
        for q in ["первая", "ПЕРВАЯ", "Первая"] {
            assert_eq!(s.term_postings(q).len(), 1, "query '{q}'");
        }
        assert_eq!(s.search("глава первая ёжик").len(), 1);
        assert_eq!(s.search("ГЛАВА").len(), 1);
        assert_eq!(s.search("ГЛАВА ПЕРВАЯ").len(), 1);
        assert_eq!(s.search("глава gamma").len(), 0);
        // Postings keep the exact byte offsets of the original text.
        let p = s.term_postings("глава");
        assert_eq!(p[0].offset, 0);
        assert_eq!(p[0].key, pack_key(0, 1));
        let y = s.term_postings("ёжик");
        assert_eq!(y.len(), 1);
        assert_eq!(y[0].offset, text.find("Ёжик").unwrap() as u64);
        assert_eq!(s.lines, 1);
    }

    #[test]
    fn cyrillic_yo_normalizes_to_ye() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            0,
            &[("Ёжик ежик ЁЖИК", FLAG_LINE_START | FLAG_LINE_END)],
        );
        // Ё, е, and ё all canonicalize to the same term.
        assert_eq!(s.occurrences, 3);
        for q in ["ёжик", "ежик", "ЁЖИК", "ЕЖИК"] {
            assert_eq!(s.term_postings(q).len(), 3, "query '{q}'");
        }
        // One line matches, three times: the AND is a single term.
        assert_eq!(s.search("ёжик").len(), 3);
    }

    #[test]
    fn fold_pair_canonicalizes_cyrillic_ranges() {
        assert_eq!(fold_cyrillic_pair(0xD0, 0x90), (0xD0, 0xB0)); // А -> а
        assert_eq!(fold_cyrillic_pair(0xD0, 0x9F), (0xD0, 0xBF)); // П -> п
        assert_eq!(fold_cyrillic_pair(0xD0, 0xA0), (0xD1, 0x80)); // Р -> р
        assert_eq!(fold_cyrillic_pair(0xD0, 0xAF), (0xD1, 0x8F)); // Я -> я
        assert_eq!(fold_cyrillic_pair(0xD0, 0x81), (0xD0, 0xB5)); // Ё -> е
        assert_eq!(fold_cyrillic_pair(0xD1, 0x91), (0xD0, 0xB5)); // ё -> е
                                                                  // Lowercase and non-Cyrillic pairs pass through byte for byte.
        assert_eq!(fold_cyrillic_pair(0xD0, 0xB0), (0xD0, 0xB0)); // а
        assert_eq!(fold_cyrillic_pair(0xD1, 0x80), (0xD1, 0x80)); // р
        assert_eq!(fold_cyrillic_pair(0xD0, 0xB5), (0xD0, 0xB5)); // е
        assert_eq!(fold_cyrillic_pair(0xE0, 0xA0), (0xE0, 0xA0)); // 3-byte lead
    }

    #[test]
    fn tokenize_uses_the_same_cyrillic_fold() {
        assert_eq!(tokenize("ГЛАВА"), tokenize("глава"));
        assert_eq!(tokenize("ГЛАВА ПЕРВАЯ"), tokenize("глава первая"));
        assert_eq!(tokenize("Рот"), tokenize("рот"));
        assert_eq!(tokenize("Якорь"), tokenize("якорь"));
        assert_eq!(tokenize("Ёжик"), tokenize("ёжик"));
        assert_eq!(tokenize("ёжик"), tokenize("ежик"));
        assert_eq!(tokenize("GLAVA"), tokenize("glava")); // ASCII fold intact
    }

    #[test]
    fn split_two_byte_cyrillic_pair_hashes_identically() {
        let text = "Рот Аист";
        let bytes = text.as_bytes();
        // Reference: the whole line delivered as one chunk.
        let mut whole = IndexShard::new();
        feed_bytes(&mut whole, 0, &[(bytes, FLAG_LINE_START | FLAG_LINE_END)]);

        // Cut every chunk boundary right after a lead byte: `Р` = D0 A0 and
        // `А` = D0 90, so each pair is split between two `ChunkRef` payloads.
        let mut split = IndexShard::new();
        feed_bytes(
            &mut split,
            0,
            &[
                (&bytes[0..1], FLAG_LINE_START | FLAG_CONTINUED), // D0
                (&bytes[1..7], FLAG_CONTINUED),                   // A0 + "от "
                (&bytes[7..8], FLAG_CONTINUED),                   // D0
                (&bytes[8..], FLAG_LINE_END),                     // 90 + "ист"
            ],
        );

        assert_eq!(term_hash("Рот".as_bytes()), term_hash("рот".as_bytes()));
        assert_eq!(term_hash("Аист".as_bytes()), term_hash("аист".as_bytes()));
        assert_eq!(split.occurrences, 2);
        assert_eq!(split.occurrences, whole.occurrences);
        assert_eq!(split.terms(), whole.terms());
        assert_eq!(split.lines, 1);
        // The split stream folds to exactly the same postings as the whole one.
        assert_eq!(split.term_postings("рот"), whole.term_postings("рот"));
        assert_eq!(split.term_postings("аист"), whole.term_postings("аист"));
        assert_eq!(split.term_postings("Рот").len(), 1);
        assert_eq!(split.term_postings("Аист").len(), 1);
        assert_eq!(split.term_postings("рот")[0].offset, 0);
        assert_eq!(split.term_postings("аист")[0].offset, 7);
        assert_eq!(split.search("Рот Аист").len(), 1);
        assert_eq!(split.search("рот аист").len(), 1);
        assert_eq!(split.search("РОТ АИСТ").len(), 1);
        assert!(split.lists_sorted());
    }

    #[test]
    fn split_yo_pair_across_chunks_hashes_identically() {
        let bytes = "Ёжик".as_bytes(); // D0 81 D0 B6 D0 B8 D0 9A
        let mut whole = IndexShard::new();
        feed_bytes(&mut whole, 0, &[(bytes, FLAG_LINE_START | FLAG_LINE_END)]);
        let mut split = IndexShard::new();
        feed_bytes(
            &mut split,
            0,
            &[
                (&bytes[0..1], FLAG_LINE_START | FLAG_CONTINUED), // D0
                (&bytes[1..3], FLAG_CONTINUED),                   // 81 + D0
                (&bytes[3..], FLAG_EOF),                          // B6 D0 B8 D0 9A
            ],
        );
        assert_eq!(whole.occurrences, 1);
        assert_eq!(split.occurrences, 1);
        assert_eq!(split.terms(), whole.terms());
        assert_eq!(split.term_postings("ежик"), whole.term_postings("ежик"));
        assert_eq!(split.term_postings("ёжик").len(), 1);
        assert_eq!(split.term_postings("ЕЖИК").len(), 1);
        assert_eq!(split.term_postings("ежик")[0].offset, 0);
    }

    #[test]
    fn unpaired_lead_byte_at_line_end_is_flushed_raw() {
        // The stream truncates on a lone D0 lead byte, so the token keeps that
        // byte raw, exactly as an unsplit stream of the same bytes.
        let mut whole = IndexShard::new();
        feed_bytes(
            &mut whole,
            0,
            &[(b"ab\xD0" as &[u8], FLAG_LINE_START | FLAG_LINE_END)],
        );
        let mut split = IndexShard::new();
        feed_bytes(
            &mut split,
            0,
            &[
                (b"ab" as &[u8], FLAG_LINE_START | FLAG_CONTINUED),
                (b"\xD0" as &[u8], FLAG_LINE_END),
            ],
        );
        assert_eq!(whole.occurrences, 1);
        assert_eq!(split.occurrences, 1);
        let h = term_hash(b"ab\xD0");
        assert_eq!(whole.hash_postings(h).len(), 1);
        assert_eq!(split.hash_postings(h), whole.hash_postings(h));
        assert_eq!(split.lines, 1);
        assert_eq!(whole.lines, 1);
    }

    #[test]
    fn deferred_lead_byte_before_a_separator_is_flushed_raw() {
        // Chunk 1 ends on a lead byte, chunk 2 opens on a separator: the token
        // that ended at the boundary hashes the raw lead byte, as unsplit text.
        let mut whole = IndexShard::new();
        feed_bytes(
            &mut whole,
            0,
            &[(b"\xD0 ok" as &[u8], FLAG_LINE_START | FLAG_LINE_END)],
        );
        let mut split = IndexShard::new();
        feed_bytes(
            &mut split,
            0,
            &[
                (b"\xD0" as &[u8], FLAG_LINE_START | FLAG_CONTINUED),
                (b" ok" as &[u8], FLAG_LINE_END),
            ],
        );
        assert_eq!(split.occurrences, 2);
        assert_eq!(whole.occurrences, 2);
        assert_eq!(split.terms(), whole.terms());
        assert_eq!(
            split.hash_postings(term_hash(b"\xD0")),
            whole.hash_postings(term_hash(b"\xD0"))
        );
        assert_eq!(split.term_postings("ok"), whole.term_postings("ok"));
        assert_eq!(split.term_postings("ok")[0].offset, 2);
    }

    #[test]
    fn pending_lead_never_crosses_a_line_boundary() {
        let mut s = IndexShard::new();
        feed_bytes(
            &mut s,
            0,
            &[
                (b"ab\xD0" as &[u8], FLAG_LINE_START | FLAG_LINE_END),
                (b"\x90cd" as &[u8], FLAG_LINE_START | FLAG_LINE_END),
            ],
        );
        // "ab\xD0" flushes raw at the line end; the next line starts a new
        // token with a stray continuation byte, with no folded pair.
        assert_eq!(s.occurrences, 2);
        assert_eq!(s.lines, 2);
        assert_eq!(s.hash_postings(term_hash(b"ab\xD0")).len(), 1);
        assert_eq!(s.hash_postings(term_hash(b"\x90cd")).len(), 1);
        assert_eq!(s.hash_postings(term_hash(b"\xD0\x90")).len(), 0);
    }

    #[test]
    fn utf8_tokens_are_kept_whole() {
        let mut s = IndexShard::new();
        feed(&mut s, 0, &[("мир, труд", FLAG_LINE_START | FLAG_LINE_END)]);
        assert_eq!(s.occurrences, 2);
        assert_eq!(s.term_postings("мир").len(), 1);
        assert_eq!(s.term_postings("труд").len(), 1);
    }

    #[test]
    fn token_spanning_chunk_boundary_is_hashed_incrementally() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            0,
            &[
                ("hello wor", FLAG_LINE_START | FLAG_CONTINUED),
                ("ld and rust", FLAG_LINE_END),
            ],
        );
        // "world" starts at byte 6 and was split across two chunks.
        let p = s.term_postings("world");
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].offset, 6);
        assert_eq!(p[0].key, pack_key(0, 1));
        assert_eq!(s.term_postings("hello").len(), 1);
        assert_eq!(s.term_postings("and").len(), 1);
        assert_eq!(s.term_postings("rust").len(), 1);
        assert_eq!(s.lines, 1);
    }

    #[test]
    fn blank_lines_keep_line_numbers_exact() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            0,
            &[
                ("alpha", FLAG_LINE_START | FLAG_LINE_END),
                ("", FLAG_LINE_START | FLAG_LINE_END),
                ("beta", FLAG_LINE_START | FLAG_LINE_END),
            ],
        );
        assert_eq!(s.lines, 3);
        assert_eq!(s.term_postings("alpha")[0].key, pack_key(0, 1));
        assert_eq!(s.term_postings("beta")[0].key, pack_key(0, 3));
    }

    #[test]
    fn eof_tail_flushes_partial_token() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            0,
            &[
                ("alph", FLAG_LINE_START | FLAG_CONTINUED),
                ("abeta", FLAG_EOF),
            ],
        );
        let p = s.term_postings("alphabeta");
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].offset, 0);
        assert_eq!(s.lines, 0); // EOF tail is not a completed line
    }

    #[test]
    fn and_intersection_across_terms() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            0,
            &[
                ("alpha beta", FLAG_LINE_START | FLAG_LINE_END),
                ("beta gamma", FLAG_LINE_START | FLAG_LINE_END),
            ],
        );
        let r = s.search("alpha beta");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].line, 1);
        let r = s.search("beta gamma");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].line, 2);
        assert!(s.search("alpha gamma").is_empty());
    }

    #[test]
    fn shard_rejects_broken_wire_records() {
        let mut s = IndexShard::new();
        s.ingest_wire(&[1, 2, 3]);
        assert_eq!(s.bad_chunks, 1);
        assert_eq!(s.occurrences, 0);
    }

    #[test]
    fn merge_shards_keeps_global_lists_sorted() {
        let mut a = IndexShard::new();
        feed(
            &mut a,
            0,
            &[("alpha beta", FLAG_LINE_START | FLAG_LINE_END)],
        );
        let mut b = IndexShard::new();
        feed(
            &mut b,
            1,
            &[("beta gamma", FLAG_LINE_START | FLAG_LINE_END)],
        );
        let g = merge_shards(vec![a, b], 2);
        assert!(g.lists_sorted());
        assert_eq!(g.terms, 3);
        assert_eq!(g.occurrences, 4);
        assert_eq!(g.entries, 4);
        let r = g.search("beta");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].file_id, 0);
        assert_eq!(r[1].file_id, 1);
        let r = g.search("beta gamma");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].file_id, 1);
        assert_eq!(r[0].line, 1);
    }

    #[test]
    fn merge_accumulates_repeated_rounds() {
        let mut a = IndexShard::new();
        feed(
            &mut a,
            0,
            &[("alpha beta", FLAG_LINE_START | FLAG_LINE_END)],
        );
        let mut b = IndexShard::new();
        feed(
            &mut b,
            0,
            &[("alpha beta", FLAG_LINE_START | FLAG_LINE_END)],
        );
        let g = merge_shards(vec![a, b], 2);
        assert_eq!(g.occurrences, 4);
        assert_eq!(g.terms, 2);
        assert_eq!(g.search("alpha beta").len(), 2);
        assert!(g.lists_sorted());
    }

    #[test]
    fn limited_search_stops_early() {
        let mut s = IndexShard::new();
        for i in 0..64 {
            let text = format!("term{i} shared");
            feed(
                &mut s,
                i,
                &[(text.as_str(), FLAG_LINE_START | FLAG_LINE_END)],
            );
        }
        let all = s.search("shared");
        assert_eq!(all.len(), 64);
        let few = s.search_limited("shared", 8);
        assert_eq!(few.len(), 8);
    }

    #[test]
    fn absent_term_and_hot_cold_sampling() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            0,
            &[
                ("shared shared rare", FLAG_LINE_START | FLAG_LINE_END),
                ("shared rare2", FLAG_LINE_START | FLAG_LINE_END),
            ],
        );
        let g = merge_shards(vec![s], 1);
        assert!(g.search_hashes(&[g.absent_hash()]).is_empty());
        let hot = g.hot_terms(1);
        assert_eq!(term_text_hits(&g, hot[0]), 3);
        let cold = g.cold_terms(1);
        assert_eq!(term_text_hits(&g, cold[0]), 1);
    }

    fn term_text_hits(g: &GlobalIndex, h: u64) -> usize {
        g.posting(h).len()
    }

    #[test]
    fn posting_key_packing_round_trips() {
        let k = pack_key(1234, 56);
        assert_eq!(key_file(k), 1234);
        assert_eq!(key_line(k), 56);
        let big = pack_key(u32::MAX, u32::MAX);
        assert_eq!(key_file(big), u32::MAX);
        assert_eq!(key_line(big), u32::MAX);
    }

    #[test]
    fn shard_lists_stay_sorted_across_files() {
        let mut s = IndexShard::new();
        for fid in [3u32, 7, 11] {
            feed(
                &mut s,
                fid,
                &[("alpha shared", FLAG_LINE_START | FLAG_LINE_END)],
            );
            feed(
                &mut s,
                fid,
                &[("beta shared", FLAG_LINE_START | FLAG_LINE_END)],
            );
        }
        assert!(s.lists_sorted());
        let p = s.term_postings("shared");
        assert_eq!(p.len(), 6);
        assert_eq!(key_file(p[0].key), 3);
        assert_eq!(key_file(p[5].key), 11);
    }

    #[test]
    fn lanes_ingest_matches_producer_stream() {
        use crate::parser::{crawl, partition, run_producer, ConsumerStats, FileRecord, WalkStats};
        use crate::queue::Lanes;
        use std::path::PathBuf;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join("index_lanes");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("a.txt"), "alpha beta\nbeta gamma\nalpha only\n").unwrap();
        std::fs::write(
            root.join("nested").join("b.rs"),
            "seeksage parser\nseeksage queue lane\n",
        )
        .unwrap();

        let mut records: Vec<FileRecord> = Vec::new();
        let mut walk = WalkStats::default();
        crawl(&root, &mut records, &mut walk).unwrap();
        assert_eq!(records.len(), 2);

        let parts = partition(&records, 2);
        let lanes = Lanes::new(2, 4096);
        let mut token = lanes.consumer().expect("consumer token");
        let active = AtomicUsize::new(2);

        let (published, cons, shard) = std::thread::scope(|s| {
            let handles: Vec<_> = (0..2)
                .map(|i| {
                    let lanes_ref = &lanes;
                    let active_ref = &active;
                    let part = &parts[i];
                    s.spawn(move || -> std::io::Result<u64> {
                        let st = run_producer(lanes_ref, i, part);
                        active_ref.fetch_sub(1, Ordering::Release);
                        st.map(|ps| ps.published)
                    })
                })
                .collect();
            let consumer_ref = &mut token;
            let active_ref = &active;
            let ch = s.spawn(move || {
                let mut c = ConsumerStats::default();
                let mut shard = IndexShard::new();
                crate::parser::drain_until_done(consumer_ref, active_ref, |bytes| {
                    c.record(bytes);
                    shard.ingest_wire(bytes);
                });
                (c, shard)
            });
            let mut published = 0u64;
            for h in handles {
                published += h.join().expect("producer thread").expect("producer lane");
            }
            let (c, sh) = ch.join().expect("consumer thread");
            (published, c, sh)
        });

        assert_eq!(cons.chunks, published);
        assert_eq!(shard.chunks, cons.chunks);
        assert_eq!(shard.payload_bytes, cons.payload_bytes);
        assert!(shard.lists_sorted());

        let g = merge_shards(vec![shard], 2);
        let r = g.search("seeksage");
        assert_eq!(r.len(), 2);
        let r = g.search("seeksage queue");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].line, 2);
        let r = g.search("beta gamma");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].line, 2);
        assert!(g.search("alpha gamma").is_empty());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn prefix_query_reaches_longer_words() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            1,
            &[
                ("the player plays piano", FLAG_LINE_START | FLAG_LINE_END),
                ("play only", FLAG_LINE_START | FLAG_LINE_END),
            ],
        );
        let g = merge_shards(vec![s], 1);
        // "play" expands to {play, plays, player}: lines 1 and 2, each once.
        let r = g.search("play");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].line, 1);
        assert_eq!(r[1].line, 2);
        // Exact spellings still resolve inside their own prefix union.
        assert_eq!(g.search("player").len(), 1);
        assert_eq!(g.search("the").len(), 1);
        assert_eq!(g.search("piano").len(), 1);
        // AND of a prefix and an exact term: both must land on one line.
        assert_eq!(g.search("play piano").len(), 1);
        assert_eq!(g.search("play piano")[0].line, 1);
        // A prefix that matches no term returns empty, even under AND.
        assert!(g.search("quart").is_empty());
        assert!(g.search("play quart").is_empty());
    }

    #[test]
    fn cyrillic_prefix_query_reaches_longer_words() {
        let mut s = IndexShard::new();
        feed(
            &mut s,
            1,
            &[
                ("абсурдные коровы", FLAG_LINE_START | FLAG_LINE_END),
                ("обычные строки", FLAG_LINE_START | FLAG_LINE_END),
            ],
        );
        let g = merge_shards(vec![s], 1);
        // "абсурд" is a canonical prefix of "абсурдные" on line 1.
        let r = g.search("абсурд");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].line, 1);
        // An uppercase prefix folds to the same canonical bytes.
        assert_eq!(g.search("АБСУРД").len(), 1);
        assert_eq!(g.search("коро").len(), 1);
        // The exact spelling is still found.
        assert_eq!(g.search("абсурдные").len(), 1);
        // Prefix + exact AND, both on line 1.
        assert_eq!(g.search("абсурд коровы").len(), 1);
        assert!(g.search("абсурд строки").is_empty());
    }
}
