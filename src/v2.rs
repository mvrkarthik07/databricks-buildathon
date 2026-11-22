//! Engine v2: the event path as three passes that each touch a record once.
//!
//! A. `parse_pack` (parallel over input chunks): one fused pass splits, validates and packs a
//!    40-byte row into a lane-local sequential array, and scatters a 24-byte dedup record
//!    `(event hash, ingest rank, row)` into one of `dedup_shards()` radix partitions. Full rows are
//!    never shuffled.
//! B. `dedup_shard` (parallel over partitions): an L2-resident open-addressing table keyed by the
//!    event hash. A duplicate pair is settled on the spot: the later ingest is marked a loser in a
//!    1-byte-per-row flag array. No winner list, no second table, no per-row bookkeeping for the
//!    94% of events that are unique.
//! C. `resolve_route` (parallel over lanes): reads each lane's rows sequentially, skips losers and
//!    invalid copies, resolves the person and writes the routed record.
//!
//! Dedup semantics are unchanged: it happens before validation (an invalid winner suppresses
//! later valid copies) and the earliest ingest wins. Ingest order is compared as a number built
//! from the digits of a canonical `YYYY-MM-DDTHH:MM:SSZ` field, which orders exactly as the bytes
//! do. Any other 20-byte ingest field makes the whole run fall back to the generic engine.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering::Relaxed};

use crate::input::{Dates, rows, timestamp_cached, decimal, will_need};
use crate::{
    Catalog, DAY, Event, INVALID, Line, Links, NameRef, NamedEvent, Text, cold_field, hash, hash2,
    stage, worker_of,
};

#[derive(Clone, Copy)]
struct PRow {
    user_hash: u64,
    user_hash2: u64,
    ts: i64,
    price: u32,
    category: u32,
    line: Line,
}

#[derive(Clone, Copy)]
struct DRec {
    hash: u64,
    rank: u64,
    row: u32,
    lane: u32,
}

pub struct Lane {
    rows: Vec<PRow>,
    shards: Vec<Vec<DRec>>,
}

/// The ingest field as a number that orders like its bytes, if it is the canonical 20-byte form.
#[inline]
fn ingest_rank(b: &[u8]) -> Option<u64> {
    let b: &[u8; 20] = b.try_into().ok()?;
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' || b[19] != b'Z' {
        return None;
    }
    let mut v = 0u64;
    let mut bad = 0u8;
    for i in [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
        let d = b[i].wrapping_sub(b'0');
        bad |= (d > 9) as u8;
        v = v * 10 + u64::from(d);
    }
    (bad == 0).then_some(v)
}

pub fn parse_pack<'a>(
    lane_index: u32,
    chunks: Vec<(u32, Text<'a>)>,
    catalog: &Catalog<'a>,
    (start, end): (i64, i64),
    shard_count: usize,
    irregular: &AtomicBool,
) -> Lane {
    let lines: usize = chunks.iter().map(|(_, chunk)| memchr::memchr_iter(b'\n', chunk).count()).sum();
    let share = lines / shard_count + lines / shard_count / 16 + 64;
    let mut shards: Vec<Vec<DRec>> = (0..shard_count).map(|_| Vec::with_capacity(share)).collect();
    let mut rows_out: Vec<PRow> = Vec::with_capacity(lines);
    let mut dates = Dates::new();
    for (_, chunk) in chunks.iter().take(2) {
        will_need(chunk);
    }
    for (index, &(chunk_index, chunk)) in chunks.iter().enumerate() {
        if let Some((_, ahead)) = chunks.get(index + 2) {
            will_need(ahead);
        }
        rows::<7>(chunk, |[id, user, ts, ingest, kind, product, price]| {
            if id.is_empty() || ingest.len() != 20 {
                return;
            }
            let Some(rank) = ingest_rank(ingest) else {
                irregular.store(true, Relaxed);
                return;
            };
            let at = id.as_ptr().addr() - chunk.as_ptr().addr();
            let offset = at - usize::from(at > 0 && chunk[at - 1] == b'"');
            let event_hash = hash(id);
            let shard = ((event_hash >> 40) as usize) % shard_count;
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
                let ts = timestamp_cached(ts, &mut dates)?;
                if ts < start || ts >= end {
                    return None;
                }
                if timestamp_cached(ingest, &mut dates)? - ts > DAY {
                    return None;
                }
                Some((ts, price, category))
            })();
            let (ts, price, category) = parsed.unwrap_or((0, 0, INVALID));
            let row = rows_out.len() as u32;
            // A valid copy needs its user resolved later; an invalid one never does.
            let (user_hash, user_hash2) =
                if category == INVALID { (0, 0) } else { (hash(user), hash2(user)) };
            rows_out.push(PRow {
                user_hash,
                user_hash2,
                ts,
                price,
                category,
                line: Line { chunk: chunk_index, offset: offset as u32 },
            });
            shards[shard].push(DRec { hash: event_hash, rank, row, lane: lane_index });
        });
    }
    Lane { rows: rows_out, shards }
}

/// Settles every duplicate group of one partition. Marks each non-winning copy in `flags`.
fn dedup_shard(shard: usize, lanes: &[Lane], flags: &[&[AtomicU8]], chunks: &[Text<'_>]) {
    let n: usize = lanes.iter().map(|lane| lane.shards[shard].len()).sum();
    let mut recs: Vec<DRec> = Vec::with_capacity(n);
    for lane in lanes {
        recs.extend_from_slice(&lane.shards[shard]);
    }
    let capacity = (n * 2).next_power_of_two().max(16);
    let mask = capacity - 1;
    let mut table = vec![0u32; capacity];
    let id_of = |r: &DRec| cold_field(chunks, lanes[r.lane as usize].rows[r.row as usize].line, 0);
    for i in 0..n {
        let r = recs[i];
        let mut pos = r.hash as usize & mask;
        loop {
            let t = table[pos];
            if t == 0 {
                table[pos] = i as u32 + 1;
                break;
            }
            let o = recs[(t - 1) as usize];
            if o.hash == r.hash && id_of(&o) == id_of(&r) {
                let loser = if r.rank < o.rank {
                    table[pos] = i as u32 + 1;
                    o
                } else {
                    r
                };
                flags[loser.lane as usize][loser.row as usize].store(1, Relaxed);
                break;
            }
            pos = (pos + 1) & mask;
        }
    }
}

pub fn dedup_all(lanes: &[Lane], chunks: &[Text<'_>], workers: usize) -> Vec<Vec<u8>> {
    let mut flags: Vec<Vec<u8>> = lanes.iter().map(|lane| vec![0u8; lane.rows.len()]).collect();
    let atomic: Vec<&[AtomicU8]> = flags
        .iter_mut()
        .map(|f| {
            // SAFETY: `AtomicU8` has the layout of `u8`, and `f` is not used as plain bytes until
            // the scope below has joined every writer.
            unsafe { &*(f.as_mut_slice() as *mut [u8] as *const [AtomicU8]) }
        })
        .collect();
    let shard_count = lanes.first().map_or(0, |lane| lane.shards.len());
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let t = std::time::Instant::now();
                loop {
                    let shard = next.fetch_add(1, Relaxed);
                    if shard >= shard_count {
                        break;
                    }
                    dedup_shard(shard, lanes, &atomic, chunks);
                }
                stage(0, t);
            });
        }
    });
    flags
}

pub fn resolve_route<'a>(
    lane: &Lane,
    flags: &[u8],
    links: &Links<'a>,
    workers: usize,
) -> Vec<Vec<NamedEvent>> {
    let t = std::time::Instant::now();
    let share = lane.rows.len() / workers + lane.rows.len() / workers / 8 + 16;
    let mut routed: Vec<Vec<NamedEvent>> = (0..workers).map(|_| Vec::with_capacity(share)).collect();
    for (row, &lost) in lane.rows.iter().zip(flags) {
        if lost != 0 || row.category == INVALID {
            continue;
        }
        let (name, hash, hash2) = match links.resolve(row.user_hash, row.user_hash2) {
            Some(slot) => (NameRef::linked(slot.person), slot.target.0, slot.target.1),
            None => (NameRef::line(row.line), row.user_hash, row.user_hash2),
        };
        routed[worker_of(hash, workers)].push(NamedEvent {
            name,
            hash,
            hash2,
            event: Event { ts: row.ts, price: row.price, category: row.category },
        });
    }
    stage(2, t);
    routed
}
