mod input;
mod output;

use std::cmp::{Reverse, min};
use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;

use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHasher};

use input::{
    DAY, Dates, GAP, Mapped, WEEK, chunk_batches, csv_files, decimal, fields, lines, rows, timestamp,
    timestamp_cached, will_need,
};
use output::{CampaignRow, UserRow};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Text<'a> = &'a [u8];
const DEDUP_SHARDS: usize = 32;

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
struct People<'a> {
    table: HashTable<u32>,
    keys: Vec<(u64, u64)>,
    names: Vec<Text<'a>>,
}

impl<'a> People<'a> {
    #[inline]
    fn find(&self, hash: u64, hash2: u64) -> Option<u32> {
        self.table
            .find(hash, |&id| self.keys[id as usize] == (hash, hash2))
            .copied()
    }

    #[inline]
    fn intern(&mut self, name: Text<'a>, hash: u64, hash2: u64) -> u32 {
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
    for id in 0..count {
        let target = smallest[root(&mut parents, id as u32) as usize] as usize;
        if target != id {
            links.insert(
                keys[id],
                LinkTarget {
                    name: names[target],
                    hash: keys[target].0,
                    hash2: keys[target].1,
                },
            );
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

/// Maps a linked ID to the person's smallest ID, found by the ID's two hashes. IDs that are
/// their own person are absent. A one-hash bloom filter answers the common "not linked" case
/// without touching the table.
struct Links<'a> {
    table: HashTable<((u64, u64), LinkTarget<'a>)>,
    bloom: Vec<u64>,
    shift: u32,
}

impl<'a> Links<'a> {
    fn with_capacity(count: usize) -> Self {
        let bits = (count.max(1) * 32).next_power_of_two().max(64);
        Links {
            table: HashTable::with_capacity(count),
            bloom: vec![0; bits / 64],
            shift: 64 - bits.trailing_zeros(),
        }
    }

    fn insert(&mut self, key: (u64, u64), target: LinkTarget<'a>) {
        let bit = (key.0 >> self.shift) as usize;
        self.bloom[bit / 64] |= 1 << (bit % 64);
        self.table.insert_unique(key.0, (key, target), |e| e.0.0);
    }

    #[inline]
    fn resolve(&self, hash: u64, hash2: u64) -> Option<LinkTarget<'a>> {
        let bit = (hash >> self.shift) as usize;
        if self.bloom[bit / 64] >> (bit % 64) & 1 == 0 {
            return None;
        }
        self.table
            .find(hash, |e| e.0 == (hash, hash2))
            .map(|e| e.1)
    }
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

/// One parsed copy of an event row. Validity is settled per copy, but only the winning copy of
/// each event ID (the earliest ingest) is allowed to count.
struct Row<'a> {
    hash: u64,
    id: Text<'a>,
    user: Text<'a>,
    user_hash: u64,
    user_hash2: u64,
    ts: i64,
    price: u32,
    category: u32,
    ingest: [u8; 20],
    valid: bool,
}

struct NamedEvent<'a> {
    name: Text<'a>,
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

struct NamedClick<'a> {
    hash: u64,
    hash2: u64,
    click: Click<'a>,
}

#[inline]
fn worker_of(hash: u64, workers: usize) -> usize {
    (((hash >> 32) * workers as u64) >> 32) as usize
}

fn route_rows<'a>(
    chunks: Vec<&'a [u8]>,
    catalog: &Catalog<'a>,
    start: i64,
    end: i64,
) -> Vec<Vec<Row<'a>>> {
    let lines: usize = chunks.iter().map(|chunk| memchr::memchr_iter(b'\n', chunk).count()).sum();
    let share = lines / DEDUP_SHARDS + lines / DEDUP_SHARDS / 16 + 64;
    let mut shards: Vec<Vec<Row>> = (0..DEDUP_SHARDS).map(|_| Vec::with_capacity(share)).collect();
    let mut dates = Dates::new();
    for chunk in chunks.iter().take(2) {
        will_need(chunk);
    }
    for (index, &chunk) in chunks.iter().enumerate() {
        if let Some(ahead) = chunks.get(index + 2) {
            will_need(ahead);
        }
        rows::<7>(chunk, |[id, user, ts, ingest, kind, product, price]| {
        if id.is_empty() || ingest.len() != 20 {
            return;
        }
        let hash = hash(id);
        // Keep shard bits separate from hashbrown's low bucket bits and top seven tag bits.
        let shard = ((hash >> 52) as usize) % DEDUP_SHARDS;
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
        let (ts, price, category) = parsed.unwrap_or((0, 0, 0));
        shards[shard].push(Row {
            hash,
            id,
            user,
            user_hash: crate::hash(user),
            user_hash2: hash2(user),
            ts,
            price,
            category,
            ingest: ingest.try_into().unwrap(),
            valid: parsed.is_some(),
        });
        });
    }
    shards
}

#[derive(Clone, Copy)]
struct Slot {
    hash: u64,
    batch: u32,
    pos: u32,
}

fn dedup_events<'a>(
    batches: Vec<Vec<Row<'a>>>,
    people: &Links<'a>,
    workers: usize,
) -> Vec<Vec<NamedEvent<'a>>> {
    let total: usize = batches.iter().map(Vec::len).sum();
    let mut kept: HashTable<Slot> = HashTable::with_capacity(total);
    for (b, batch) in batches.iter().enumerate() {
        for (p, row) in batch.iter().enumerate() {
            let slot = Slot {
                hash: row.hash,
                batch: b as u32,
                pos: p as u32,
            };
            match kept.entry(
                row.hash,
                |old| {
                    old.hash == row.hash
                        && batches[old.batch as usize][old.pos as usize].id == row.id
                },
                |slot| slot.hash,
            ) {
                Entry::Vacant(entry) => {
                    entry.insert(slot);
                }
                Entry::Occupied(mut entry) => {
                    let old = &batches[entry.get().batch as usize][entry.get().pos as usize];
                    if row.ingest < old.ingest {
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
    drop(kept);

    let share = total / workers + total / workers / 8 + 16;
    let mut routed: Vec<Vec<NamedEvent>> = (0..workers).map(|_| Vec::with_capacity(share)).collect();
    for (batch, winner) in batches.iter().zip(&winner) {
        for (row, _) in batch.iter().zip(winner).filter(|&(row, &win)| win && row.valid) {
            let (name, hash, hash2) = match people.resolve(row.user_hash, row.user_hash2) {
                Some(target) => (target.name, target.hash, target.hash2),
                None => (row.user, row.user_hash, row.user_hash2),
            };
            routed[worker_of(hash, workers)].push(NamedEvent {
                name,
                hash,
                hash2,
                event: Event {
                    ts: row.ts,
                    price: row.price,
                    category: row.category,
                },
            });
        }
    }
    routed
}

fn route_clicks<'a>(
    chunks: Vec<Text<'a>>,
    people: &Links<'a>,
    catalog: &Catalog<'a>,
    start: i64,
    end: i64,
    workers: usize,
) -> Vec<Vec<NamedClick<'a>>> {
    let mut routed: Vec<Vec<NamedClick>> = (0..workers).map(|_| Vec::new()).collect();
    let mut dates = Dates::new();
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
        let (user_hash, user_hash2) = (crate::hash(user), self::hash2(user));
        let (hash, hash2) = match people.resolve(user_hash, user_hash2) {
            Some(target) => (target.hash, target.hash2),
            None => (user_hash, user_hash2),
        };
        routed[worker_of(hash, workers)].push(NamedClick {
            hash,
            hash2,
            click: Click {
                ts,
                id,
                campaign,
            },
        });
        });
    }
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

fn aggregate<'a>(
    event_batches: Vec<Vec<NamedEvent<'a>>>,
    click_batches: Vec<Vec<NamedClick<'a>>>,
    catalog: &Catalog<'a>,
) -> (Vec<UserRow<'a>>, CampaignDaily) {
    let mut people = People::default();
    let event_count: usize = event_batches.iter().map(Vec::len).sum();
    let mut event_person = Vec::with_capacity(event_count);
    for event in event_batches.iter().flatten() {
        event_person.push(people.intern(event.name, event.hash, event.hash2));
    }
    let mut click_person = Vec::with_capacity(click_batches.iter().map(Vec::len).sum());
    for click in click_batches.iter().flatten() {
        click_person.push(people.find(click.hash, click.hash2));
    }
    let person_count = people.names.len();
    let (mut events, event_start) = group_by_person(
        event_batches
            .iter()
            .flatten()
            .zip(&event_person)
            .map(|(event, &person)| (person, event.event)),
        event_count,
        person_count,
    );
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

    let mut users = Vec::new();
    let mut campaigns = CampaignDaily::default();
    let mut spend = vec![0i64; catalog.categories.names.len()];
    let mut touched = Vec::new();
    let mut active: FxHashMap<u32, usize> = FxHashMap::with_hasher(FxBuildHasher);

    for person in 0..person_count {
        let name = people.names[person];
        let person_events = &mut events[event_start[person] as usize..event_start[person + 1] as usize];
        person_events.sort_unstable_by_key(|event| event.ts);
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

        let mut daily = UserRow::new(name);
        let mut first = 0;
        let mut last = 0;
        active.clear();
        for session in person_events.chunk_by(|a, b| b.ts - a.ts <= GAP) {
            let start = session[0].ts;
            let day = start.div_euclid(DAY) as i32;
            if daily.sessions != 0 && daily.day != day {
                flush_daily(&mut users, daily, &mut touched, &mut spend, catalog);
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
                let count = active.get_mut(&click.campaign).unwrap();
                *count -= 1;
                if *count == 0 {
                    active.remove(&click.campaign);
                }
            }
            for click in &person_clicks[last.max(next_first)..next_last] {
                *active.entry(click.campaign).or_default() += 1;
            }
            first = next_first;
            last = next_last;
            for &campaign in active.keys() {
                campaigns.entry((campaign, day)).or_default().assists += 1;
            }
            let winner = if first == last {
                catalog.organic
            } else {
                person_clicks[last - 1].campaign
            };
            let total = campaigns.entry((winner, day)).or_default();
            total.sessions += 1;
            total.purchasing += i64::from(revenue != 0);
            total.revenue += revenue;
        }
        flush_daily(&mut users, daily, &mut touched, &mut spend, catalog);
    }

    (users, campaigns)
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

fn run(data: &Path, out: &Path, workers: usize) -> Result<()> {
    let t = std::time::Instant::now();
    let timing = env::var_os("SS_TIMING").is_some();
    let lap = |name: &str| {
        if timing {
            eprintln!("{name}: {:?}", t.elapsed());
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
    let catalog = catalog(&products, &campaigns);
    lap("catalog");
    let links = csv_files(&data.join("links"))?;
    let event_files = csv_files(&data.join("events"))?;
    let click_files = csv_files(&data.join("clicks"))?;
    let link_rows = parallel(chunk_batches(&links, workers), workers, parse_links);
    lap("link parse");
    let (people, copies) = thread::scope(|scope| {
        let people = scope.spawn(|| {
            let t = std::time::Instant::now();
            let people = build_links(link_rows);
            if timing {
                eprintln!("  build_links: {:?}", t.elapsed());
            }
            people
        });
        let copies = parallel(chunk_batches(&event_files, workers), workers, |chunks| {
            route_rows(chunks, &catalog, start, end)
        });
        (people.join().unwrap(), copies)
    });
    lap("links+route");
    let events = parallel(transpose(copies, DEDUP_SHARDS), workers, |batches| {
        dedup_events(batches, &people, workers)
    });
    lap("dedup");
    let clicks = parallel(chunk_batches(&click_files, workers), workers, |chunks| {
        route_clicks(chunks, &people, &catalog, start, end, workers)
    });
    lap("clicks");

    let user_folder = out.join("user_daily");
    let campaign_folder = out.join("campaign_daily");
    fs::create_dir_all(&user_folder)?;
    fs::create_dir_all(&campaign_folder)?;
    let jobs = transpose(events, workers)
        .into_iter()
        .zip(transpose(clicks, workers))
        .enumerate()
        .collect();
    let results = parallel(
        jobs,
        workers,
        |(shard, (events, clicks))| -> Result<CampaignDaily> {
            let (users, campaigns) = aggregate(events, clicks, &catalog);
            if shard == 0 || !users.is_empty() {
                output::users(&user_folder.join(format!("part-{shard}.parquet")), &users)?;
            }
            Ok(campaigns)
        },
    );

    lap("aggregate+users");
    let mut merged = CampaignDaily::default();
    for result in results {
        for (key, part) in result? {
            let total = merged.entry(key).or_default();
            total.sessions += part.sessions;
            total.purchasing += part.purchasing;
            total.revenue += part.revenue;
            total.assists += part.assists;
        }
    }
    let rows: Vec<_> = merged
        .into_iter()
        .map(|((campaign, day), totals)| CampaignRow {
            day,
            id: catalog.campaigns.names[campaign as usize],
            channel: catalog.channels[campaign as usize],
            totals,
        })
        .collect();
    output::campaigns(&campaign_folder.join("part-0.parquet"), &rows)?;
    lap("campaigns");
    Ok(())
}

fn main() -> Result<()> {
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
