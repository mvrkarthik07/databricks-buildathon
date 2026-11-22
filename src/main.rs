mod input;
mod output;
mod prof;
#[cfg(target_os = "linux")]
mod huge;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: huge::Alloc = huge::Alloc;

use std::cmp::{Reverse, min};
use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;

use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use rustc_hash::{FxHashMap, FxHasher};

use input::{
    DAY, Dates, GAP, Mapped, WEEK, chunk_batches, csv_files, decimal, fields, lines, rows, timestamp,
    ingest_within_day, timestamp_cached, will_need,
};
use output::{CampaignRow, UserRow};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Text<'a> = &'a [u8];
/// Rows per dedup shard that keep a shard's hash table (about 24 bytes a row) inside a 2 MiB L2.
const SHARD_ROWS: usize = 40_000;

/// Dedup shards for `event_bytes` of input (about 88 bytes a row): enough that a shard's table
/// stays cache-resident, between 32 and 4096. A power of two, as the shard index is a remainder.
fn dedup_shards(event_bytes: usize) -> usize {
    let rows = env::var("SS_SHARD_ROWS").ok().and_then(|s| s.parse().ok()).filter(|&n| n > 0).unwrap_or(SHARD_ROWS);
    (event_bytes / 88 / rows).next_power_of_two().clamp(32, 4096)
}

/// A second hash, independent of `hash`, built from folded 128-bit multiplies.
fn hash2(text: &[u8]) -> u64 {
    const S0: u64 = 0xa076_1d64_78bd_642f;
    const S1: u64 = 0xe703_7ed1_a0b4_28db;
    const S2: u64 = 0x8ebc_6af0_9c88_c6e3;
    #[inline]
    fn mix(a: u64, b: u64) -> u64 {
        let product = u128::from(a) * u128::from(b);
        (product as u64) ^ ((product >> 64) as u64)
    }
    #[inline]
    fn word(bytes: &[u8]) -> u64 {
        u64::from_le_bytes(bytes[..8].try_into().unwrap())
    }
    #[inline]
    fn half(bytes: &[u8]) -> u64 {
        u64::from(u32::from_le_bytes(bytes[..4].try_into().unwrap()))
    }
    let len = text.len() as u64;
    let mut seed = S0 ^ len;
    let mut rest = text;
    while rest.len() > 16 {
        seed = mix(word(rest) ^ S1, word(&rest[8..]) ^ seed);
        rest = &rest[16..];
    }
    let (a, b) = if rest.len() >= 8 {
        (word(rest), word(&rest[rest.len() - 8..]))
    } else if rest.len() >= 4 {
        (half(rest), half(&rest[rest.len() - 4..]))
    } else if !rest.is_empty() {
        let n = rest.len();
        (
            u64::from(rest[0]) << 16 | u64::from(rest[n / 2]) << 8 | u64::from(rest[n - 1]),
            0,
        )
    } else {
        (0, 0)
    };
    mix(S1 ^ len, mix(a ^ S2, b ^ seed))
}

fn hash(text: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = FxHasher::default();
    hasher.write(text);
    hasher.finish()
}

/// Distinct people, identified by two independent 64-bit hashes of their smallest ID. Equal
/// pairs are treated as equal names: a false match needs a 128-bit collision, so the name bytes
/// (which sit cold in the input mapping) are never read back to confirm.
#[derive(Default)]
struct People {
    table: HashTable<u32>,
    keys: Vec<(u64, u64)>,
    /// Where each person's name is. The bytes are read only when the person is processed, with the
    /// next few requested ahead, because they sit cold in the input mapping.
    names: Vec<NameRef>,
}

impl People {
    #[inline]
    fn find(&self, hash: u64, hash2: u64) -> Option<u32> {
        self.table
            .find(hash, |&id| self.keys[id as usize] == (hash, hash2))
            .copied()
    }

    /// `name` is kept only for a person seen for the first time.
    #[inline]
    fn intern(&mut self, hash: u64, hash2: u64, name: NameRef) -> u32 {
        let keys = &mut self.keys;
        match self.table.entry(
            hash,
            |&id| keys[id as usize] == (hash, hash2),
            |&id| keys[id as usize].0,
        ) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let id = u32::try_from(keys.len()).expect("too many distinct people");
                keys.push((hash, hash2));
                self.names.push(name);
                entry.insert(id);
                id
            }
        }
    }
}

#[derive(Default)]
struct Ids<'a> {
    ids: FxHashMap<Text<'a>, u32>,
    names: Vec<Text<'a>>,
}

impl<'a> Ids<'a> {
    fn intern(&mut self, text: Text<'a>) -> u32 {
        *self.ids.entry(text).or_insert_with(|| {
            let id = u32::try_from(self.names.len()).expect("too many distinct identifiers");
            self.names.push(text);
            id
        })
    }
}

struct Catalog<'a> {
    products: Ids<'a>,
    product_category: Vec<u32>,
    categories: Ids<'a>,
    unknown: u32,
    campaigns: Ids<'a>,
    channels: Vec<Text<'a>>,
    organic: u32,
}

fn catalog<'a>(products: &'a Mapped, campaigns: &'a Mapped) -> Catalog<'a> {
    let mut product_ids = Ids::default();
    let mut categories = Ids::default();
    let mut product_category = Vec::new();
    let unknown = categories.intern(b"unknown");
    for [product, category] in lines(products.body()).filter_map(fields::<2>) {
        product_ids.intern(product);
        product_category.push(categories.intern(category));
    }
    let mut campaign_ids = Ids::default();
    let mut channels = Vec::new();
    for [campaign, channel] in lines(campaigns.body()).filter_map(fields::<2>) {
        campaign_ids.intern(campaign);
        channels.push(channel);
    }
    let organic = campaign_ids.intern(b"organic");
    channels.push(b"organic");
    Catalog {
        products: product_ids,
        product_category,
        categories,
        unknown,
        campaigns: campaign_ids,
        channels,
        organic,
    }
}

fn root(parents: &mut [u32], mut id: u32) -> u32 {
    while parents[id as usize] != id {
        let parent = parents[id as usize];
        parents[id as usize] = parents[parent as usize];
        id = parent;
    }
    id
}

/// One link row: two IDs of the same person, with their hashes computed while the bytes are hot.
#[derive(Clone, Copy)]
struct LinkRow<'a> {
    a: Text<'a>,
    b: Text<'a>,
    hashes: [u64; 4],
}

fn parse_links<'a>(chunks: Vec<Text<'a>>) -> Vec<LinkRow<'a>> {
    let mut out = Vec::new();
    for chunk in chunks {
        rows::<2>(chunk, |[a, b]| {
            if a.is_empty() || b.is_empty() {
                return;
            }
            out.push(LinkRow {
                a,
                b,
                hashes: [hash(a), hash2(a), hash(b), hash2(b)],
            });
        });
    }
    out
}

/// Builds the ID to person mapping. IDs are identified by their two hashes (see `People`); the
/// person is the byte-wise smallest ID of the connected component.
fn build_links<'a>(link_rows: Vec<Vec<LinkRow<'a>>>) -> Links<'a> {
    let total: usize = link_rows.iter().map(Vec::len).sum();
    // Every row can introduce two new IDs.
    let ids = total * 2;
    let mut table: HashTable<u32> = HashTable::with_capacity(ids);
    let mut keys: Vec<(u64, u64)> = Vec::with_capacity(ids);
    let mut names: Vec<Text<'a>> = Vec::with_capacity(ids);
    let mut parents: Vec<u32> = Vec::with_capacity(ids);
    let mut sizes: Vec<u32> = Vec::with_capacity(ids);
    let mut intern = |name: Text<'a>, h: u64, h2: u64| -> u32 {
        match table.entry(h, |&id| keys[id as usize] == (h, h2), |&id| keys[id as usize].0) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let id = u32::try_from(keys.len()).expect("too many distinct identifiers");
                keys.push((h, h2));
                names.push(name);
                parents.push(id);
                sizes.push(1);
                entry.insert(id);
                id
            }
        }
    };
    let mut pairs: Vec<(u32, u32)> = Vec::with_capacity(total);
    for row in link_rows.iter().flatten() {
        let a = intern(row.a, row.hashes[0], row.hashes[1]);
        let b = intern(row.b, row.hashes[2], row.hashes[3]);
        pairs.push((a, b));
    }
    drop(link_rows);
    for (a, b) in pairs {
        let (mut a, mut b) = (root(&mut parents, a), root(&mut parents, b));
        if a != b {
            if sizes[a as usize] < sizes[b as usize] {
                std::mem::swap(&mut a, &mut b);
            }
            parents[b as usize] = a;
            sizes[a as usize] += sizes[b as usize];
        }
    }
    let count = names.len();
    let mut smallest: Vec<u32> = (0..count as u32).collect();
    for id in 0..count {
        let component = root(&mut parents, id as u32) as usize;
        if names[id] < names[smallest[component] as usize] {
            smallest[component] = id as u32;
        }
    }
    let mut links = Links::with_capacity(count);
    // Dense index of each person that has another ID, assigned in order of first use.
    let mut person_of = vec![u32::MAX; count];
    for id in 0..count {
        let target = smallest[root(&mut parents, id as u32) as usize] as usize;
        if target != id {
            if person_of[target] == u32::MAX {
                person_of[target] = u32::try_from(links.persons.len()).expect("too many people");
                links.persons.push(LinkTarget {
                    name: names[target],
                    hash: keys[target].0,
                    hash2: keys[target].1,
                });
            }
            links.insert(keys[id], person_of[target]);
        }
    }
    links
}

#[derive(Clone, Copy)]
struct LinkTarget<'a> {
    name: Text<'a>,
    hash: u64,
    hash2: u64,
}

#[derive(Clone, Copy, Default)]
struct LinkSlot {
    key: (u64, u64),
    /// The person's own two hashes, so routing an event needs no second memory access.
    target: (u64, u64),
    /// Dense person index plus one; zero marks an empty slot.
    person_plus_one: u32,
}

impl LinkSlot {
    #[inline]
    fn person(&self) -> u32 {
        self.person_plus_one - 1
    }
}

/// Lookups per batch. The table is far bigger than any cache, so a lookup is a DRAM miss; a batch
/// issues all of its misses before waiting for the first.
const BATCH: usize = 16;

#[inline(always)]
fn prefetch<T>(ptr: *const T) {
    // SAFETY: a prefetch is a hint; it never faults and reads nothing the program can observe.
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr.cast());
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) ptr, options(nostack, preserves_flags));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

/// Maps a linked ID to its person, found by the ID's two hashes. IDs that are their own person
/// are absent. A one-hash bloom filter answers the common "not linked" case without touching the
/// table. The table is flat with linear probing, so a lookup's first slot address is known from
/// the hash and can be prefetched; `persons` holds each person's name.
struct Links<'a> {
    slots: Vec<LinkSlot>,
    persons: Vec<LinkTarget<'a>>,
    bloom: Vec<u64>,
    shift: u32,
}

impl<'a> Links<'a> {
    fn with_capacity(count: usize) -> Self {
        let bits = (count.max(1) * 32).next_power_of_two().max(64);
        Links {
            // Load factor at most 0.6. Zeroed memory is the empty table.
            slots: vec![LinkSlot::default(); count * 5 / 3 + 16],
            persons: Vec::new(),
            bloom: vec![0; bits / 64],
            shift: 64 - bits.trailing_zeros(),
        }
    }

    #[inline(always)]
    fn first_slot(&self, hash: u64) -> usize {
        ((u128::from(hash) * self.slots.len() as u128) >> 64) as usize
    }

    fn insert(&mut self, key: (u64, u64), person: u32) {
        let bit = (key.0 >> self.shift) as usize;
        self.bloom[bit / 64] |= 1 << (bit % 64);
        let target = &self.persons[person as usize];
        let slot = LinkSlot { key, target: (target.hash, target.hash2), person_plus_one: person + 1 };
        let mut i = self.first_slot(key.0);
        while self.slots[i].person_plus_one != 0 {
            i = if i + 1 == self.slots.len() { 0 } else { i + 1 };
        }
        self.slots[i] = slot;
    }

    #[inline(always)]
    fn bloom_hit(&self, hash: u64) -> bool {
        let bit = (hash >> self.shift) as usize;
        self.bloom[bit / 64] >> (bit % 64) & 1 != 0
    }

    #[inline(always)]
    fn probe(&self, hash: u64, hash2: u64) -> Option<&LinkSlot> {
        let mut i = self.first_slot(hash);
        loop {
            let slot = &self.slots[i];
            if slot.person_plus_one == 0 {
                return None;
            }
            if slot.key == (hash, hash2) {
                return Some(slot);
            }
            i = if i + 1 == self.slots.len() { 0 } else { i + 1 };
        }
    }

    /// The person each ID belongs to, if the ID is linked to another, for up to `BATCH` keys at once: all bloom words are requested, then all table
    /// slots that pass the bloom filter, then the lookups run on cache lines already on the way.
    #[inline]
    fn resolve_batch<'s>(&'s self, keys: &[(u64, u64)], out: &mut [Option<&'s LinkSlot>; BATCH]) {
        debug_assert!(keys.len() <= BATCH);
        for &(hash, _) in keys {
            prefetch(&self.bloom[(hash >> self.shift) as usize / 64]);
        }
        let mut maybe = [false; BATCH];
        for (i, &(hash, _)) in keys.iter().enumerate() {
            if self.bloom_hit(hash) {
                maybe[i] = true;
                let slot: *const LinkSlot = &self.slots[self.first_slot(hash)];
                // A slot can straddle two cache lines.
                prefetch(slot);
                prefetch(slot.wrapping_byte_add(std::mem::size_of::<LinkSlot>() - 1));
            }
        }
        for (i, &(hash, hash2)) in keys.iter().enumerate() {
            out[i] = if maybe[i] { self.probe(hash, hash2) } else { None };
        }
    }
}

/// Numbers every chunk, so a record can name its line as (chunk, offset) in 8 bytes. Fails only
/// for a line longer than 4 GiB (chunks are about 4 MiB plus one line), which the 32-bit offsets
/// cannot address.
fn index_chunks<'a>(
    batches: Vec<Vec<Text<'a>>>,
) -> Result<(Vec<Text<'a>>, Vec<Vec<(u32, Text<'a>)>>)> {
    let mut table = Vec::new();
    let mut indexed = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut out = Vec::with_capacity(batch.len());
        for chunk in batch {
            if chunk.len() > u32::MAX as usize || table.len() >= 1 << 31 {
                return Err(io::Error::other("an events line longer than 4 GiB is not supported").into());
            }
            out.push((table.len() as u32, chunk));
            table.push(chunk);
        }
        indexed.push(out);
    }
    Ok((table, indexed))
}

/// Worker `w` takes buckets `w * per_worker ..` of `by_bucket`.
fn split_by_worker<T>(by_bucket: Vec<Vec<Vec<T>>>, buckets: Buckets) -> Vec<Vec<Vec<Vec<T>>>> {
    let mut by_bucket = by_bucket.into_iter();
    (0..buckets.workers).map(|_| by_bucket.by_ref().take(buckets.per_worker).collect()).collect()
}

/// Like `parallel`, but each worker gets its whole lane of jobs at once, so it can keep state
/// across them.
fn parallel_lanes<T: Send, U: Send>(jobs: Vec<T>, workers: usize, run: impl Fn(Vec<T>) -> U + Sync) -> Vec<U> {
    let mut lanes: Vec<Vec<T>> = (0..workers).map(|_| Vec::new()).collect();
    for (i, job) in jobs.into_iter().enumerate() {
        lanes[i % workers].push(job);
    }
    thread::scope(|scope| {
        let run = &run;
        let mut lanes = lanes.into_iter();
        let local = lanes.next().unwrap();
        let handles: Vec<_> = lanes.filter(|lane| !lane.is_empty()).map(|lane| scope.spawn(move || run(lane))).collect();
        let mut results = vec![run(local)];
        for handle in handles {
            results.push(handle.join().unwrap());
        }
        results
    })
}

fn parallel<T: Send, U: Send>(jobs: Vec<T>, workers: usize, run: impl Fn(T) -> U + Sync) -> Vec<U> {
    let mut lanes: Vec<Vec<T>> = (0..workers).map(|_| Vec::new()).collect();
    for (i, job) in jobs.into_iter().enumerate() {
        lanes[i % workers].push(job);
    }
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers - 1);
        let run = &run;
        let mut lanes = lanes.into_iter();
        let local = lanes.next().unwrap();
        for lane in lanes.filter(|lane| !lane.is_empty()) {
            handles.push(scope.spawn(move || lane.into_iter().map(run).collect::<Vec<_>>()));
        }
        let mut results: Vec<_> = local.into_iter().map(run).collect();
        for handle in handles {
            results.extend(handle.join().unwrap());
        }
        results
    })
}

fn transpose<T>(sources: Vec<Vec<Vec<T>>>, shards: usize) -> Vec<Vec<Vec<T>>> {
    let mut destinations: Vec<Vec<Vec<T>>> = (0..shards).map(|_| Vec::new()).collect();
    for source in sources {
        for (destination, rows) in destinations.iter_mut().zip(source) {
            if !rows.is_empty() {
                destination.push(rows);
            }
        }
    }
    destinations
}

#[derive(Clone, Copy, Default)]
struct Event {
    ts: i64,
    price: u32,
    category: u32,
}

/// Where an input line starts: its chunk's index in the chunk table and the byte offset in it.
/// Fields that few rows ever need (the event ID bytes, the ingest time, the user name) are read
/// again from here instead of being carried by every record.
#[derive(Clone, Copy)]
struct Line {
    chunk: u32,
    offset: u32,
}

/// Field `index` of an event line, cleaned of its quotes. Scans only up to that field.
fn cold_field<'a>(chunks: &[Text<'a>], line: Line, index: usize) -> Text<'a> {
    let chunk = chunks[line.chunk as usize];
    let mut start = line.offset as usize;
    for _ in 0..index {
        // The line was accepted by `rows`, so it has all seven fields: these commas exist.
        start += memchr::memchr(b',', &chunk[start..]).expect("line was already parsed") + 1;
    }
    let end = start + memchr::memchr(b',', &chunk[start..]).expect("line was already parsed");
    match &chunk[start..end] {
        [b'"', inner @ .., b'"'] => inner,
        value => value,
    }
}

/// One parsed copy of an event row. Validity is settled per copy, but only the winning copy of
/// each event ID (the earliest ingest) is allowed to count.
#[derive(Clone, Copy)]
struct Row {
    hash: u64,
    user_hash: u64,
    user_hash2: u64,
    ts: i64,
    price: u32,
    /// `INVALID` when the copy fails validation. Real categories are dense indexes into the
    /// products file, far below `u32::MAX`.
    category: u32,
    line: Line,
}

const INVALID: u32 = u32::MAX;

/// Where a person's name bytes are: in a linked person's record (top bit set), or on the line of
/// one of their events (a person with a single ID is named by that ID).
#[derive(Clone, Copy)]
struct NameRef(u64);

impl NameRef {
    const LINKED: u64 = 1 << 63;

    fn linked(person: u32) -> Self {
        Self(Self::LINKED | u64::from(person))
    }

    fn line(line: Line) -> Self {
        Self(u64::from(line.chunk) << 32 | u64::from(line.offset))
    }

    /// Requests the bytes `text` will read.
    #[inline(always)]
    fn prefetch(self, chunks: &[Text<'_>], links: &Links<'_>) {
        if self.0 & Self::LINKED != 0 {
            prefetch(links.persons[(self.0 & !Self::LINKED) as usize].name.as_ptr());
        } else {
            let line = Line { chunk: (self.0 >> 32) as u32, offset: self.0 as u32 };
            prefetch(chunks[line.chunk as usize].as_ptr().wrapping_add(line.offset as usize));
        }
    }

    fn text<'a>(self, chunks: &[Text<'a>], links: &Links<'a>) -> Text<'a> {
        if self.0 & Self::LINKED != 0 {
            links.persons[(self.0 & !Self::LINKED) as usize].name
        } else {
            let line = Line { chunk: (self.0 >> 32) as u32, offset: self.0 as u32 };
            cold_field(chunks, line, 1)
        }
    }
}

#[derive(Clone, Copy)]
struct NamedEvent {
    name: NameRef,
    hash: u64,
    hash2: u64,
    event: Event,
}

#[derive(Clone, Copy, Default)]
struct Click<'a> {
    ts: i64,
    id: Text<'a>,
    campaign: u32,
}

#[derive(Clone, Copy)]
struct NamedClick<'a> {
    hash: u64,
    hash2: u64,
    click: Click<'a>,
}

#[inline]
fn worker_of(hash: u64, workers: usize) -> usize {
    (((hash >> 32) * workers as u64) >> 32) as usize
}

/// Events per bucket the aggregation aims for: small enough that a bucket's people table, grouped
/// events and sort all stay in cache. `SS_BUCKET_EVENTS` overrides it (tests use tiny buckets).
fn bucket_events() -> usize {
    env::var("SS_BUCKET_EVENTS").ok().and_then(|s| s.parse().ok()).filter(|&n| n > 0).unwrap_or(131_072)
}

/// Where a person's events and clicks go: each worker owns `per_worker` consecutive buckets, and a
/// person's identity hashes pick the worker and then the bucket. A worker aggregates its buckets one
/// after another, so interning, grouping and sorting work on a cache-sized slice of its people.
#[derive(Clone, Copy)]
struct Buckets {
    workers: usize,
    per_worker: usize,
}

impl Buckets {
    fn new(events: usize, workers: usize) -> Self {
        let per_worker = (events / workers).div_ceil(bucket_events()).clamp(1, 4096 / workers);
        Self { workers, per_worker }
    }

    fn count(self) -> usize {
        self.workers * self.per_worker
    }

    #[inline]
    fn of(self, hash: u64, hash2: u64) -> usize {
        worker_of(hash, self.workers) * self.per_worker + (((hash2 >> 32) * self.per_worker as u64) >> 32) as usize
    }
}

/// `L` is a profiling ladder: each level stops after one more operation per row (0 only counts
/// lines, 1 splits, 2 adds the event-ID hash, 3 the user hashes, 4 both timestamps, 5 validation,
/// 6 and above the full row push). Levels below 6 return empty shards and exist only to measure
/// what each operation costs; the scored build always uses 99.
fn route_rows<'a, const L: u32>(
    chunks: Vec<(u32, Text<'a>)>,
    catalog: &Catalog<'a>,
    start: i64,
    end: i64,
    shard_count: usize,
) -> Vec<Vec<Row>> {
    let t0 = prof::now();
    let sys0 = prof::thread_sys();
    let lines: usize = chunks.iter().map(|(_, chunk)| memchr::memchr_iter(b'\n', chunk).count()).sum();
    if L == 0 {
        std::hint::black_box(lines);
        prof::add(prof::ROUTE, t0);
        return Vec::new();
    }
    let mut sink = 0u64;
    let share = lines / shard_count + lines / shard_count / 16 + 64;
    let mut shards: Vec<Vec<Row>> = (0..shard_count).map(|_| Vec::with_capacity(share)).collect();
    let mut dates = Dates::new();
    for (_, chunk) in chunks.iter().take(2) {
        will_need(chunk);
    }
    for (index, &(chunk_index, chunk)) in chunks.iter().enumerate() {
        if let Some((_, ahead)) = chunks.get(index + 2) {
            will_need(ahead);
        }
        rows::<7>(chunk, |[id, user, ts, ingest, kind, product, price]| {
        if L == 1 {
            sink = sink.wrapping_add(
                (id.len() + user.len() + ts.len() + ingest.len() + kind.len() + product.len() + price.len())
                    as u64,
            );
            return;
        }
        if id.is_empty() || ingest.len() != 20 {
            return;
        }
        // A quoted first field is preceded by its quote; an unquoted one by the previous newline.
        let at = id.as_ptr().addr() - chunk.as_ptr().addr();
        let offset = at - usize::from(at > 0 && chunk[at - 1] == b'"');
        let hash = hash(id);
        // Keep shard bits separate from hashbrown's low bucket bits (below 2^32 buckets) and its
        // top seven tag bits.
        let shard = ((hash >> 40) as usize) % shard_count;
        if L == 2 {
            sink = sink.wrapping_add(hash ^ shard as u64 ^ offset as u64);
            return;
        }
        let (user_hash, user_hash2) = (crate::hash(user), hash2(user));
        if L == 3 {
            sink = sink.wrapping_add(user_hash ^ user_hash2);
            return;
        }
        if L == 4 {
            let a = timestamp_cached(ts, &mut dates).unwrap_or(0);
            let b = timestamp_cached(ingest, &mut dates).unwrap_or(0);
            sink = sink.wrapping_add((a ^ b) as u64);
            return;
        }
        let parsed = (|| {
            if user.is_empty() || product.is_empty() {
                return None;
            }
            let (price, category) = match kind {
                b"view" | b"cart" => (0, 0),
                b"purchase" => {
                    let price = decimal(price, 10_000_000).filter(|&n| n != 0)?;
                    let category = catalog
                        .products
                        .ids
                        .get(product)
                        .map_or(catalog.unknown, |&id| catalog.product_category[id as usize]);
                    (price as u32, category)
                }
                _ => return None,
            };
            let raw_ts = ts;
            let ts = timestamp_cached(ts, &mut dates)?;
            if ts < start || ts >= end {
                return None;
            }
            // A malformed ingest time fails validation just like one more than a day late.
            if !ingest_within_day(ingest, raw_ts, ts, &mut dates) {
                return None;
            }
            Some((ts, price, category))
        })();
        let (ts, price, category) = parsed.unwrap_or((0, 0, INVALID));
        if L == 5 {
            sink = sink.wrapping_add(ts as u64 ^ u64::from(price) ^ u64::from(category));
            return;
        }
        shards[shard].push(Row {
            hash,
            user_hash,
            user_hash2,
            ts,
            price,
            category,
            // `chunk_index` and every chunk length were checked by `index_chunks`.
            line: Line { chunk: chunk_index, offset: offset as u32 },
        });
        });
    }
    std::hint::black_box(sink);
    prof::add(prof::ROUTE, t0);
    prof::add_sys(prof::S_ROUTE, sys0);
    shards
}

#[derive(Clone, Copy)]
struct Slot {
    hash: u64,
    batch: u32,
    pos: u32,
}

/// Requests the first cache lines of an input line, which sit cold in the input mapping.
#[inline(always)]
fn prefetch_line(chunks: &[Text<'_>], line: Line) {
    let at = chunks[line.chunk as usize].as_ptr().wrapping_add(line.offset as usize);
    prefetch(at);
    prefetch(at.wrapping_add(64));
}

/// True for the copy that counts in each group of rows with one event ID: the earliest ingest time,
/// and among equal times the copy read first (smallest batch, then position). The answer does not
/// depend on the order copies are examined in, so copies of an already-seen hash are only collected
/// first and resolved later with their input lines prefetched: reading those lines back to compare
/// IDs and ingest times is a DRAM miss each, and used to be most of this phase.
fn winners<'a>(batches: &[Vec<Row>], chunks: &[Text<'a>], total: usize) -> Vec<Vec<bool>> {
    // `SS_DEFER=0` resolves repeats on first sight (for comparing the two).
    static DEFER: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *DEFER.get_or_init(|| env::var_os("SS_DEFER").is_none_or(|v| v != "0")) {
        winners_deferred(batches, chunks, total).unwrap_or_else(|| winners_exact(batches, chunks, total))
    } else {
        winners_exact(batches, chunks, total)
    }
}

/// `None` when two different IDs share a hash (a 64-bit collision), which the caller handles by
/// comparing IDs on first sight instead.
fn winners_deferred<'a>(batches: &[Vec<Row>], chunks: &[Text<'a>], total: usize) -> Option<Vec<Vec<bool>>> {
    const AHEAD: usize = 8;
    let mut kept: HashTable<Slot> = HashTable::with_capacity(total);
    // Copies whose hash was already in the table, as (batch, position).
    let mut later: Vec<(u32, u32)> = Vec::new();
    for (b, batch) in batches.iter().enumerate() {
        for (p, row) in batch.iter().enumerate() {
            match kept.entry(row.hash, |old| old.hash == row.hash, |slot| slot.hash) {
                Entry::Vacant(entry) => {
                    entry.insert(Slot { hash: row.hash, batch: b as u32, pos: p as u32 });
                }
                Entry::Occupied(_) => later.push((b as u32, p as u32)),
            }
        }
    }
    prof::add_ns(prof::C_DUP_PAIRS, later.len() as u64);
    for i in 0..later.len() {
        if let Some(&(b, p)) = later.get(i + AHEAD) {
            let row = &batches[b as usize][p as usize];
            prefetch_line(chunks, row.line);
            if let Some(slot) = kept.find(row.hash, |slot| slot.hash == row.hash) {
                prefetch_line(chunks, batches[slot.batch as usize][slot.pos as usize].line);
            }
        }
        let (b, p) = later[i];
        let row = &batches[b as usize][p as usize];
        let slot = kept
            .find_mut(row.hash, |slot| slot.hash == row.hash)
            .expect("the first pass inserted every hash");
        let old = &batches[slot.batch as usize][slot.pos as usize];
        if cold_field(chunks, old.line, 0) != cold_field(chunks, row.line, 0) {
            return None;
        }
        let earlier = match cold_field(chunks, row.line, 3).cmp(&cold_field(chunks, old.line, 3)) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => (b, p) < (slot.batch, slot.pos),
            std::cmp::Ordering::Greater => false,
        };
        if earlier {
            *slot = Slot { hash: row.hash, batch: b, pos: p };
        }
    }
    let mut winner: Vec<Vec<bool>> = batches.iter().map(|batch| vec![false; batch.len()]).collect();
    for slot in &kept {
        winner[slot.batch as usize][slot.pos as usize] = true;
    }
    Some(winner)
}

/// Same result as `winners_deferred`, but compares IDs the moment a hash repeats, so distinct IDs
/// with one hash stay apart.
fn winners_exact<'a>(batches: &[Vec<Row>], chunks: &[Text<'a>], total: usize) -> Vec<Vec<bool>> {
    let mut kept: HashTable<Slot> = HashTable::with_capacity(total);
    for (b, batch) in batches.iter().enumerate() {
        for (p, row) in batch.iter().enumerate() {
            let slot = Slot { hash: row.hash, batch: b as u32, pos: p as u32 };
            match kept.entry(
                row.hash,
                |old| {
                    old.hash == row.hash
                        && cold_field(chunks, batches[old.batch as usize][old.pos as usize].line, 0)
                            == cold_field(chunks, row.line, 0)
                },
                |slot| slot.hash,
            ) {
                Entry::Vacant(entry) => {
                    entry.insert(slot);
                }
                Entry::Occupied(mut entry) => {
                    let old = &batches[entry.get().batch as usize][entry.get().pos as usize];
                    if cold_field(chunks, row.line, 3) < cold_field(chunks, old.line, 3) {
                        *entry.get_mut() = slot;
                    }
                }
            }
        }
    }
    let mut winner: Vec<Vec<bool>> = batches.iter().map(|batch| vec![false; batch.len()]).collect();
    for slot in &kept {
        winner[slot.batch as usize][slot.pos as usize] = true;
    }
    winner
}

fn dedup_events<'a>(
    batches: Vec<Vec<Row>>,
    people: &Links<'a>,
    chunks: &[Text<'a>],
    buckets: Buckets,
    routed: &mut Vec<Vec<NamedEvent>>,
) {
    let total: usize = batches.iter().map(Vec::len).sum();
    let mut t = prof::now();
    let sys0 = prof::thread_sys();
    let winner = winners(&batches, chunks, total);
    let mut flagged = 0u64;
    prof::split(&mut flagged, &mut t);
    prof::add_ns(prof::DEDUP_INSERT, flagged);
    prof::add_ns(prof::C_ROWS, total as u64);

    let survivors_before: usize = routed.iter().map(Vec::len).sum();
    let mut linked_events = 0u64;
    let mut pending: Vec<&Row> = Vec::with_capacity(BATCH);
    let mut route_pending = |pending: &mut Vec<&Row>, routed: &mut Vec<Vec<NamedEvent>>| {
        let mut keys = [(0u64, 0u64); BATCH];
        for (key, row) in keys.iter_mut().zip(pending.iter()) {
            *key = (row.user_hash, row.user_hash2);
        }
        let mut found = [None; BATCH];
        people.resolve_batch(&keys[..pending.len()], &mut found);
        for (row, slot) in pending.drain(..).zip(found) {
            let (name, hash, hash2) = match slot {
                Some(slot) => {
                    linked_events += 1;
                    (NameRef::linked(slot.person()), slot.target.0, slot.target.1)
                }
                None => (NameRef::line(row.line), row.user_hash, row.user_hash2),
            };
            routed[buckets.of(hash, hash2)].push(NamedEvent {
                name,
                hash,
                hash2,
                event: Event { ts: row.ts, price: row.price, category: row.category },
            });
        }
    };
    for (batch, winner) in batches.iter().zip(&winner) {
        for (row, _) in batch.iter().zip(winner).filter(|&(row, &win)| win && row.category != INVALID) {
            pending.push(row);
            if pending.len() == BATCH {
                route_pending(&mut pending, routed);
            }
        }
    }
    route_pending(&mut pending, routed);
    let mut routed_ns = 0u64;
    prof::split(&mut routed_ns, &mut t);
    prof::add_ns(prof::DEDUP_ROUTE, routed_ns);
    prof::add_sys(prof::S_DEDUP, sys0);
    prof::add_ns(prof::C_LINKED_EVENTS, linked_events);
    prof::add_ns(prof::C_SURVIVORS, (routed.iter().map(Vec::len).sum::<usize>() - survivors_before) as u64);
}

struct PendingClick<'a> {
    user: (u64, u64),
    click: Click<'a>,
}

fn route_clicks<'a>(
    chunks: Vec<Text<'a>>,
    people: &Links<'a>,
    catalog: &Catalog<'a>,
    start: i64,
    end: i64,
    buckets: Buckets,
) -> Vec<Vec<NamedClick<'a>>> {
    let t0 = prof::now();
    let sys0 = prof::thread_sys();
    // Counting the lines first is cheap next to parsing them, and sizes every bucket once so none
    // has to grow (and be copied) as clicks arrive.
    let lines: usize = chunks.iter().map(|chunk| memchr::memchr_iter(b'\n', chunk).count()).sum();
    let capacity = lines / buckets.count() + lines / buckets.count() / 8 + 16;
    let mut routed: Vec<Vec<NamedClick>> = (0..buckets.count()).map(|_| Vec::with_capacity(capacity)).collect();
    let mut dates = Dates::new();
    let mut pending: Vec<PendingClick> = Vec::with_capacity(BATCH);
    let route_pending = |pending: &mut Vec<PendingClick<'a>>, routed: &mut Vec<Vec<NamedClick<'a>>>| {
        let mut keys = [(0u64, 0u64); BATCH];
        for (key, p) in keys.iter_mut().zip(pending.iter()) {
            *key = p.user;
        }
        let mut found = [None; BATCH];
        people.resolve_batch(&keys[..pending.len()], &mut found);
        for (p, slot) in pending.drain(..).zip(found) {
            let (hash, hash2) = slot.map_or(p.user, |slot| slot.target);
            routed[buckets.of(hash, hash2)].push(NamedClick { hash, hash2, click: p.click });
        }
    };
    for chunk in chunks.iter().take(2) {
        will_need(chunk);
    }
    for (index, &chunk) in chunks.iter().enumerate() {
        if let Some(ahead) = chunks.get(index + 2) {
            will_need(ahead);
        }
        rows::<4>(chunk, |[id, user, campaign, ts]| {
        if id.is_empty() || user.is_empty() {
            return;
        }
        let Some(&campaign) = catalog.campaigns.ids.get(campaign) else {
            return;
        };
        if campaign == catalog.organic {
            return;
        }
        let Some(ts) = timestamp_cached(ts, &mut dates) else { return };
        if ts < start - WEEK || ts >= end {
            return;
        }
        pending.push(PendingClick {
            user: (crate::hash(user), self::hash2(user)),
            click: Click { ts, id, campaign },
        });
        if pending.len() == BATCH {
            route_pending(&mut pending, &mut routed);
        }
        });
    }
    route_pending(&mut pending, &mut routed);
    prof::add(prof::CLICKS, t0);
    prof::add_sys(prof::S_CLICKS, sys0);
    routed
}

#[derive(Clone, Copy, Default)]
struct CampaignTotals {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assists: i64,
}

type CampaignDaily = FxHashMap<(u32, i32), CampaignTotals>;

/// Largest dense (campaign x day) table, in cells. A longer window or more campaigns than this
/// falls back to hashing, so any manifest and any `campaigns.csv` still work.
const DENSE_CELLS: usize = 1 << 18;

/// Per-(campaign, day) totals, indexed directly while the table is small (the normal case: a
/// couple of weeks of days and a few thousand campaigns).
enum DailyTotals {
    Dense { first_day: i32, campaigns: usize, cells: Vec<CampaignTotals> },
    Sparse(CampaignDaily),
}

impl DailyTotals {
    /// Session days lie in `first_day..=last_day`: every event is inside the manifest window.
    fn new(first_day: i32, last_day: i32, campaigns: usize) -> Self {
        let days = (i64::from(last_day) - i64::from(first_day) + 1) as usize;
        match days.checked_mul(campaigns) {
            Some(cells) if cells <= DENSE_CELLS => {
                Self::Dense { first_day, campaigns, cells: vec![CampaignTotals::default(); cells] }
            }
            _ => Self::Sparse(CampaignDaily::default()),
        }
    }

    #[inline]
    fn at(&mut self, campaign: u32, day: i32) -> &mut CampaignTotals {
        match self {
            Self::Dense { first_day, campaigns, cells } => {
                &mut cells[(day - *first_day) as usize * *campaigns + campaign as usize]
            }
            Self::Sparse(map) => map.entry((campaign, day)).or_default(),
        }
    }

    /// Adds another worker's totals. Every worker builds the same kind of table.
    fn add(&mut self, other: Self) {
        fn sum(total: &mut CampaignTotals, part: CampaignTotals) {
            total.sessions += part.sessions;
            total.purchasing += part.purchasing;
            total.revenue += part.revenue;
            total.assists += part.assists;
        }
        match (self, other) {
            (Self::Dense { cells, .. }, Self::Dense { cells: part, .. }) => {
                cells.iter_mut().zip(part).for_each(|(total, part)| sum(total, part));
            }
            (Self::Sparse(map), Self::Sparse(part)) => {
                for (key, part) in part {
                    sum(map.entry(key).or_default(), part);
                }
            }
            _ => unreachable!("workers build the same kind of table"),
        }
    }

    /// Every (campaign, day) that received a session or an assist.
    fn entries(self) -> Vec<((u32, i32), CampaignTotals)> {
        match self {
            Self::Dense { first_day, campaigns, cells } => cells
                .into_iter()
                .enumerate()
                .filter(|(_, part)| part.sessions != 0 || part.assists != 0)
                .map(|(i, part)| (((i % campaigns) as u32, first_day + (i / campaigns) as i32), part))
                .collect(),
            Self::Sparse(map) => map.into_iter().collect(),
        }
    }
}

/// The campaigns with a click in the current 7-day look-back, with how many clicks each has.
/// Direct-indexed by campaign; `list` holds the live campaigns so a session visits only those.
struct Active {
    count: Vec<u32>,
    slot: Vec<u32>,
    list: Vec<u32>,
}

impl Active {
    fn new(campaigns: usize) -> Self {
        Self { count: vec![0; campaigns], slot: vec![0; campaigns], list: Vec::new() }
    }

    #[inline]
    fn add(&mut self, campaign: u32) {
        let c = campaign as usize;
        if self.count[c] == 0 {
            self.slot[c] = self.list.len() as u32;
            self.list.push(campaign);
        }
        self.count[c] += 1;
    }

    #[inline]
    fn remove(&mut self, campaign: u32) {
        let c = campaign as usize;
        self.count[c] -= 1;
        if self.count[c] == 0 {
            let slot = self.slot[c] as usize;
            let moved = self.list[self.list.len() - 1];
            self.list.swap_remove(slot);
            self.slot[moved as usize] = slot as u32;
        }
    }

    fn clear(&mut self) {
        for &campaign in &self.list {
            self.count[campaign as usize] = 0;
        }
        self.list.clear();
    }
}

/// Groups `items` by person with a counting sort. Returns the grouped items and, for each
/// person, the start of their range (one extra entry marks the end).
fn group_by_person<T: Copy + Default>(
    items: impl Iterator<Item = (u32, T)> + Clone,
    count: usize,
    people: usize,
) -> (Vec<T>, Vec<u32>) {
    let mut start = vec![0u32; people + 1];
    for (person, _) in items.clone() {
        start[person as usize + 1] += 1;
    }
    for i in 0..people {
        start[i + 1] += start[i];
    }
    let mut next = start[..people].to_vec();
    let mut grouped = vec![T::default(); count];
    for (person, item) in items {
        let slot = &mut next[person as usize];
        grouped[*slot as usize] = item;
        *slot += 1;
    }
    (grouped, start)
}

/// What a worker's people add up to, across all the buckets it processes.
struct Accum<'a> {
    users: Vec<UserRow<'a>>,
    campaigns: DailyTotals,
    spend: Vec<i64>,
    touched: Vec<usize>,
    active: Active,
}

/// Aggregates the buckets of one worker, one after another.
fn aggregate<'a>(
    event_buckets: Vec<Vec<Vec<NamedEvent>>>,
    click_buckets: Vec<Vec<Vec<NamedClick<'a>>>>,
    catalog: &Catalog<'a>,
    (first_day, last_day): (i32, i32),
    chunks: &[Text<'a>],
    links: &Links<'a>,
) -> (Vec<UserRow<'a>>, DailyTotals) {
    let campaign_count = catalog.campaigns.names.len();
    let mut acc = Accum {
        users: Vec::new(),
        campaigns: DailyTotals::new(first_day, last_day, campaign_count),
        spend: vec![0i64; catalog.categories.names.len()],
        touched: Vec::new(),
        active: Active::new(campaign_count),
    };
    for (events, clicks) in event_buckets.into_iter().zip(click_buckets) {
        aggregate_part(events, clicks, catalog, chunks, links, &mut acc);
    }
    (acc.users, acc.campaigns)
}

fn aggregate_part<'a>(
    event_batches: Vec<Vec<NamedEvent>>,
    click_batches: Vec<Vec<NamedClick<'a>>>,
    catalog: &Catalog<'a>,
    chunks: &[Text<'a>],
    links: &Links<'a>,
    acc: &mut Accum<'a>,
) {
    let mut t = prof::now();
    let mut people = People::default();
    let event_count: usize = event_batches.iter().map(Vec::len).sum();
    let mut event_person = Vec::with_capacity(event_count);
    for event in event_batches.iter().flatten() {
        event_person.push(people.intern(event.hash, event.hash2, event.name));
    }
    let mut ns = 0u64;
    prof::split(&mut ns, &mut t);
    prof::add_ns(prof::AGG_INTERN_EVENTS, ns);
    let mut click_person = Vec::with_capacity(click_batches.iter().map(Vec::len).sum());
    for click in click_batches.iter().flatten() {
        click_person.push(people.find(click.hash, click.hash2));
    }
    let mut ns = 0u64;
    prof::split(&mut ns, &mut t);
    prof::add_ns(prof::AGG_FIND_CLICKS, ns);
    let person_count = people.names.len();
    prof::add_ns(prof::C_PEOPLE, person_count as u64);
    let (mut events, event_start) = group_by_person(
        event_batches
            .iter()
            .flatten()
            .zip(&event_person)
            .map(|(event, &person)| (person, event.event)),
        event_count,
        person_count,
    );
    let mut ns = 0u64;
    prof::split(&mut ns, &mut t);
    prof::add_ns(prof::AGG_GROUP_EVENTS, ns);
    let click_count = click_person.iter().flatten().count();
    let (mut clicks, click_start) = group_by_person(
        click_batches
            .iter()
            .flatten()
            .zip(&click_person)
            .filter_map(|(click, person)| person.map(|person| (person, click.click))),
        click_count,
        person_count,
    );
    drop((event_batches, click_batches, event_person, click_person));
    let mut ns = 0u64;
    prof::split(&mut ns, &mut t);
    prof::add_ns(prof::AGG_GROUP_CLICKS, ns);
    prof::add_ns(prof::C_CLICKS_KEPT, click_count as u64);
    let (mut sort_events, mut sort_clicks, mut sessionize, mut flush) = (0u64, 0u64, 0u64, 0u64);
    let mut sessions_total = 0u64;

    let Accum { users, campaigns, spend, touched, active } = acc;

    for person in 0..person_count {
        if let Some(&ahead) = people.names.get(person + 8) {
            ahead.prefetch(chunks, links);
        }
        let name = people.names[person].text(chunks, links);
        prof::split(&mut sessionize, &mut t);
        let person_events = &mut events[event_start[person] as usize..event_start[person + 1] as usize];
        person_events.sort_unstable_by_key(|event| event.ts);
        prof::split(&mut sort_events, &mut t);
        // At a tied timestamp the last click must have the smallest byte ID.
        let person_clicks = &mut clicks[click_start[person] as usize..click_start[person + 1] as usize];
        person_clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| b.id.cmp(a.id)));
        // Identical click copies have the same validity and always reach the same person.
        let mut kept = 0;
        for i in 0..person_clicks.len() {
            if kept == 0
                || person_clicks[kept - 1].ts != person_clicks[i].ts
                || person_clicks[kept - 1].id != person_clicks[i].id
            {
                person_clicks[kept] = person_clicks[i];
                kept += 1;
            }
        }
        let person_clicks = &person_clicks[..kept];
        prof::split(&mut sort_clicks, &mut t);

        let mut daily = UserRow::new(name);
        let mut first = 0;
        let mut last = 0;
        active.clear();
        for session in person_events.chunk_by(|a, b| b.ts - a.ts <= GAP) {
            let start = session[0].ts;
            let day = start.div_euclid(DAY) as i32;
            sessions_total += 1;
            if daily.sessions != 0 && daily.day != day {
                prof::split(&mut sessionize, &mut t);
                flush_daily(users, daily, touched, spend, catalog);
                prof::split(&mut flush, &mut t);
                daily = UserRow::new(name);
            }
            daily.day = day;
            daily.sessions += 1;
            daily.events += session.len() as i64;

            let mut revenue = 0;
            for event in session.iter().filter(|event| event.price != 0) {
                let category = event.category as usize;
                if spend[category] == 0 {
                    touched.push(category);
                }
                spend[category] += i64::from(event.price);
                revenue += i64::from(event.price);
            }
            daily.revenue += revenue;

            let next_first =
                first + person_clicks[first..].partition_point(|click| click.ts < start - WEEK);
            let next_last = last + person_clicks[last..].partition_point(|click| click.ts <= start);
            for click in &person_clicks[first..min(last, next_first)] {
                active.remove(click.campaign);
            }
            for click in &person_clicks[last.max(next_first)..next_last] {
                active.add(click.campaign);
            }
            first = next_first;
            last = next_last;
            for &campaign in &active.list {
                campaigns.at(campaign, day).assists += 1;
            }
            let winner = if first == last {
                catalog.organic
            } else {
                person_clicks[last - 1].campaign
            };
            let total = campaigns.at(winner, day);
            total.sessions += 1;
            total.purchasing += i64::from(revenue != 0);
            total.revenue += revenue;
        }
        prof::split(&mut sessionize, &mut t);
        flush_daily(users, daily, touched, spend, catalog);
        prof::split(&mut flush, &mut t);
    }
    prof::add_ns(prof::AGG_SORT_EVENTS, sort_events);
    prof::add_ns(prof::AGG_SORT_CLICKS, sort_clicks);
    prof::add_ns(prof::AGG_SESSIONIZE, sessionize);
    prof::add_ns(prof::AGG_FLUSH, flush);
    prof::add_ns(prof::C_SESSIONS, sessions_total);
}

fn flush_daily<'a>(
    out: &mut Vec<UserRow<'a>>,
    mut row: UserRow<'a>,
    touched: &mut Vec<usize>,
    spend: &mut [i64],
    catalog: &Catalog<'a>,
) {
    row.top = touched
        .drain(..)
        .map(|category| {
            (
                std::mem::take(&mut spend[category]),
                catalog.categories.names[category],
            )
        })
        .max_by_key(|&(revenue, name)| (revenue, Reverse(name)))
        .map(|(_, name)| name);
    out.push(row);
}

/// Profiling ladder level from `SS_LADDER` (only with the `prof` feature), else the full pipeline.
fn ladder_level(key: &str) -> u32 {
    if cfg!(feature = "prof") {
        env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(99)
    } else {
        99
    }
}

fn run(data: &Path, out: &Path, workers: usize) -> Result<()> {
    let t = std::time::Instant::now();
    let timing = env::var_os("SS_TIMING").is_some();
    let lap = |name: &str| {
        if timing {
            // Process CPU time so far (all threads), to see CPU per stage as well as wall.
            let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
            let ms = |t: libc::timeval| t.tv_sec as f64 * 1e3 + t.tv_usec as f64 / 1e3;
            eprintln!(
                "{name}: wall {:.0} ms, cpu user {:.0} sys {:.0} ms, minor faults {}",
                t.elapsed().as_secs_f64() * 1e3,
                ms(usage.ru_utime),
                ms(usage.ru_stime),
                usage.ru_minflt
            );
        }
    };
    let manifest = Mapped::open(&data.join("manifest.json"))?;
    let manifest: serde_json::Value = serde_json::from_slice(manifest.bytes())?;
    let parse_boundary = |key: &str| {
        manifest[key]
            .as_str()
            .and_then(|s| timestamp(s.as_bytes()))
            .ok_or_else(|| io::Error::other(format!("invalid manifest {key}")))
    };
    let (start, end) = (
        parse_boundary("window_start")?,
        parse_boundary("window_end")?,
    );
    if start >= end {
        return Err(io::Error::other("manifest window must be increasing").into());
    }

    let products = Mapped::open(&data.join("products.csv"))?;
    let campaigns = Mapped::open(&data.join("campaigns.csv"))?;
    // The catalog is only needed once events are parsed, so build it while the inputs are mapped.
    let (catalog, files) = thread::scope(|scope| {
        let catalog = scope.spawn(|| catalog(&products, &campaigns));
        let files = (|| -> Result<_> {
            Ok((
                csv_files(&data.join("links"), workers)?,
                csv_files(&data.join("events"), workers)?,
                csv_files(&data.join("clicks"), workers)?,
            ))
        })();
        (catalog.join().unwrap(), files)
    });
    let (links, event_files, click_files) = files?;
    lap("catalog + files");
    let shards = dedup_shards(event_files.iter().map(|file| file.bytes().len()).sum());
    let (event_chunks, event_batches) = index_chunks(chunk_batches(&event_files, workers))?;
    let t0 = prof::now();
    let link_rows = parallel(chunk_batches(&links, workers), workers, parse_links);
    prof::add(prof::LINK_PARSE, t0);
    lap("link parse");
    let (people, copies) = thread::scope(|scope| {
        let people = scope.spawn(|| {
            let t = std::time::Instant::now();
            let t0 = prof::now();
            let people = build_links(link_rows);
            prof::add(prof::BUILD_LINKS, t0);
            if timing {
                eprintln!("  build_links: {:?}", t.elapsed());
            }
            people
        });
        let ladder = ladder_level("SS_LADDER");
        let copies = parallel(event_batches, workers, |chunks| match ladder {
            0 => route_rows::<0>(chunks, &catalog, start, end, shards),
            1 => route_rows::<1>(chunks, &catalog, start, end, shards),
            2 => route_rows::<2>(chunks, &catalog, start, end, shards),
            3 => route_rows::<3>(chunks, &catalog, start, end, shards),
            4 => route_rows::<4>(chunks, &catalog, start, end, shards),
            5 => route_rows::<5>(chunks, &catalog, start, end, shards),
            _ => route_rows::<99>(chunks, &catalog, start, end, shards),
        });
        if ladder < 6 {
            prof::report();
            std::process::exit(0);
        }
        (people.join().unwrap(), copies)
    });
    lap("links+route");
    let rows: usize = copies.iter().flatten().map(Vec::len).sum();
    let buckets = Buckets::new(rows, workers);
    // Each worker keeps its own bucket vectors across all the shards it dedups.
    let capacity = rows / workers / buckets.count() * 5 / 4 + 16;
    let events = parallel_lanes(transpose(copies, shards), workers, |lane| {
        let mut routed: Vec<Vec<NamedEvent>> = (0..buckets.count()).map(|_| Vec::with_capacity(capacity)).collect();
        for batches in lane {
            dedup_events(batches, &people, &event_chunks, buckets, &mut routed);
        }
        routed
    });
    lap("dedup");
    let clicks = parallel(chunk_batches(&click_files, workers), workers, |chunks| {
        route_clicks(chunks, &people, &catalog, start, end, buckets)
    });
    lap("clicks");

    let user_folder = out.join("user_daily");
    let campaign_folder = out.join("campaign_daily");
    fs::create_dir_all(&user_folder)?;
    fs::create_dir_all(&campaign_folder)?;
    let jobs = split_by_worker(transpose(events, buckets.count()), buckets)
        .into_iter()
        .zip(split_by_worker(transpose(clicks, buckets.count()), buckets))
        .enumerate()
        .collect();
    // Event timestamps are inside the window, so session days are too.
    let days = (start.div_euclid(DAY) as i32, (end - 1).div_euclid(DAY) as i32);
    let results = parallel(
        jobs,
        workers,
        |(shard, (events, clicks))| -> Result<DailyTotals> {
            let sys0 = prof::thread_sys();
            let (users, campaigns) = aggregate(events, clicks, &catalog, days, &event_chunks, &people);
            prof::add_sys(prof::S_AGG, sys0);
            if shard == 0 || !users.is_empty() {
                let t0 = prof::now();
                let sys0 = prof::thread_sys();
                output::users(&user_folder.join(format!("part-{shard}.parquet")), &users)?;
                prof::add(prof::WRITE_USERS, t0);
                prof::add_sys(prof::S_WRITE, sys0);
            }
            Ok(campaigns)
        },
    );

    lap("aggregate+users");
    let mut merged: Option<DailyTotals> = None;
    for result in results {
        let totals = result?;
        match &mut merged {
            Some(merged) => merged.add(totals),
            None => merged = Some(totals),
        }
    }
    let rows: Vec<_> = merged
        .map(DailyTotals::entries)
        .unwrap_or_default()
        .into_iter()
        .map(|((campaign, day), totals)| CampaignRow {
            day,
            id: catalog.campaigns.names[campaign as usize],
            channel: catalog.channels[campaign as usize],
            totals,
        })
        .collect();
    let t0 = prof::now();
    output::campaigns(&campaign_folder.join("part-0.parquet"), &rows)?;
    prof::add(prof::WRITE_CAMPAIGNS, t0);
    lap("campaigns");
    prof::report();
    Ok(())
}

fn main() -> Result<()> {
    #[cfg(target_os = "linux")]
    huge::init();
    let mut args = env::args_os().skip(1);
    let mut path =
        |key, default| PathBuf::from(args.next().or_else(|| env::var_os(key)).unwrap_or(default));
    let data = path("DATA_DIR", "/data".into());
    let output = path("OUTPUT_DIR", "/output".into());
    if args.next().is_some() {
        return Err(io::Error::other("usage: shopping-sessions [DATA_DIR OUTPUT_DIR]").into());
    }
    let workers = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8)
        .clamp(1, 8);
    run(&data, &output, workers)?;
    // Everything is written and closed; skip tearing down gigabytes of buffers.
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny deterministic generator, so the test needs no dependency.
    fn next(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    }

    #[test]
    fn batched_lookup_matches_a_map() {
        let mut state = 99;
        let mut links = Links::with_capacity(5_000);
        let mut model = std::collections::HashMap::new();
        for person in 0..300u32 {
            links.persons.push(LinkTarget { name: b"p", hash: u64::from(person) * 7, hash2: u64::from(person) * 13 });
        }
        for _ in 0..5_000 {
            let key = (next(&mut state) << 20 | next(&mut state), next(&mut state));
            let person = (next(&mut state) % 300) as u32;
            if model.insert(key, person).is_none() {
                links.insert(key, person);
            }
        }
        // Present keys, absent keys, and keys sharing a first hash with a present one.
        let mut keys: Vec<(u64, u64)> = model.keys().copied().collect();
        keys.extend((0..3_000).map(|_| (next(&mut state) << 20 | next(&mut state), next(&mut state))));
        keys.extend(model.keys().take(500).map(|&(a, b)| (a, b ^ 1)));
        for chunk in keys.chunks(BATCH) {
            let mut found = [None; BATCH];
            links.resolve_batch(chunk, &mut found);
            for (key, slot) in chunk.iter().zip(found) {
                let expected = model.get(key).copied();
                assert_eq!(slot.map(|slot| slot.person()), expected);
                if let Some(slot) = slot {
                    assert_eq!(slot.target, (u64::from(slot.person()) * 7, u64::from(slot.person()) * 13));
                }
            }
        }
    }

    #[test]
    fn cold_fields_read_back_each_line() {
        let chunk: &[u8] = b"e1,\"u1\",ts,2026-10-01T00:00:00Z,view,p,0\n\"e2\",u2,ts,\"2026-10-01T00:00:01Z\",view,p,0\ne3,u3,ts,2026-10-01T00:00:02Z,view,p,0";
        let chunks = [b"ignored".as_slice(), chunk];
        let mut seen = Vec::new();
        // Line starts are what `route_rows` derives from the first field, quoted or not.
        crate::input::rows::<7>(chunk, |fields| {
            let at = fields[0].as_ptr().addr() - chunk.as_ptr().addr();
            let offset = at - usize::from(at > 0 && chunk[at - 1] == b'"');
            seen.push((fields, Line { chunk: 1, offset: offset as u32 }));
        });
        assert_eq!(seen.len(), 3);
        for (fields, line) in seen {
            assert_eq!(cold_field(&chunks, line, 0), fields[0]);
            assert_eq!(cold_field(&chunks, line, 1), fields[1]);
            assert_eq!(cold_field(&chunks, line, 3), fields[3]);
        }
    }

    #[test]
    fn winners_pick_the_earliest_ingest_then_the_first_copy() {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for collide in [false, true] {
            for _ in 0..200 {
                let mut text = Vec::new();
                let mut rows = vec![Vec::new(), Vec::new(), Vec::new()];
                let mut copies = Vec::new(); // (id, ingest, batch, position)
                for _ in 0..(next() % 60) {
                    let id = next() % 7;
                    let ingest = next() % 4;
                    let offset = text.len() as u32;
                    text.extend_from_slice(format!("e{id},u,ts,2026-10-01T00:00:0{ingest}Z,view,p,0\n").as_bytes());
                    let b = (next() % 3) as usize;
                    let hash = if collide { id % 2 } else { id * 7919 + 1 };
                    rows[b].push(Row {
                        hash,
                        user_hash: 0,
                        user_hash2: 0,
                        ts: 0,
                        price: 0,
                        category: 0,
                        line: Line { chunk: 0, offset },
                    });
                    copies.push((id, ingest, b, rows[b].len() - 1));
                }
                let chunks = [text.as_slice()];
                let total = copies.len();
                let got = winners(&rows, &chunks, total);
                for &(id, ingest, b, p) in &copies {
                    let best = copies.iter().filter(|c| c.0 == id).map(|c| (c.1, c.2, c.3)).min().unwrap();
                    assert_eq!(got[b][p], best == (ingest, b, p), "collide {collide} copy {:?}", (id, ingest, b, p));
                }
            }
        }
    }

    #[test]
    fn active_matches_a_counting_map() {
        let mut state = 7;
        let (mut active, mut model) = (Active::new(8), FxHashMap::<u32, u32>::default());
        for _ in 0..10_000 {
            let campaign = (next(&mut state) % 8) as u32;
            if next(&mut state) % 2 == 0 || !model.contains_key(&campaign) {
                active.add(campaign);
                *model.entry(campaign).or_default() += 1;
            } else {
                active.remove(campaign);
                let count = model.get_mut(&campaign).unwrap();
                *count -= 1;
                if *count == 0 {
                    model.remove(&campaign);
                }
            }
            let mut live = active.list.clone();
            live.sort_unstable();
            let mut expected: Vec<_> = model.keys().copied().collect();
            expected.sort_unstable();
            assert_eq!(live, expected);
        }
        active.clear();
        assert!(active.list.is_empty() && active.count.iter().all(|&n| n == 0));
    }

    #[test]
    fn dense_and_sparse_totals_agree() {
        // 10 days x 3 campaigns is dense; 10 days x 100_000 campaigns exceeds DENSE_CELLS.
        let (mut dense, mut sparse) = (DailyTotals::new(100, 109, 3), DailyTotals::new(100, 109, 100_000));
        assert!(matches!(dense, DailyTotals::Dense { .. }));
        assert!(matches!(sparse, DailyTotals::Sparse(_)));
        let mut state = 11;
        for _ in 0..1_000 {
            let (campaign, day) = ((next(&mut state) % 3) as u32, 100 + (next(&mut state) % 10) as i32);
            for totals in [&mut dense, &mut sparse] {
                let total = totals.at(campaign, day);
                total.sessions += 1;
                total.assists += 2;
            }
        }
        let mut other = DailyTotals::new(100, 109, 3);
        other.at(2, 105).sessions += 5;
        dense.add(other);
        sparse.at(2, 105).sessions += 5;
        let sorted = |totals: DailyTotals| {
            let mut entries: Vec<_> =
                totals.entries().into_iter().map(|(k, t)| (k, t.sessions, t.assists)).collect();
            entries.sort_unstable();
            entries
        };
        assert_eq!(sorted(dense), sorted(sparse));
    }
}
