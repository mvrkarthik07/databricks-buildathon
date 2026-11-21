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

use input::{DAY, GAP, Mapped, WEEK, chunk_batches, csv_files, decimal, fields, lines, rows, timestamp};
use output::{CampaignRow, UserRow};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Text<'a> = &'a [u8];
const DEDUP_SHARDS: usize = 32;

fn hash(text: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = FxHasher::default();
    hasher.write(text);
    hasher.finish()
}

#[derive(Default)]
struct People<'a> {
    table: HashTable<u32>,
    names: Vec<Text<'a>>,
}

impl<'a> People<'a> {
    fn find(&self, name: Text<'a>, hash: u64) -> Option<u32> {
        self.table
            .find(hash, |&id| self.names[id as usize] == name)
            .copied()
    }

    fn intern(&mut self, name: Text<'a>, hash: u64) -> u32 {
        let names = &mut self.names;
        match self.table.entry(
            hash,
            |&id| names[id as usize] == name,
            |&id| crate::hash(names[id as usize]),
        ) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let id = u32::try_from(names.len()).expect("too many distinct people");
                names.push(name);
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

fn root(parents: &mut [u32], mut id: u32) -> usize {
    while parents[id as usize] != id {
        let parent = parents[id as usize];
        parents[id as usize] = parents[parent as usize];
        id = parent;
    }
    id as usize
}

fn link_names(links: &[Mapped]) -> FxHashMap<Text<'_>, Text<'_>> {
    let mut users = Ids::default();
    let mut parents = Vec::new();
    let mut sizes = Vec::new();
    for [left, right] in links
        .iter()
        .flat_map(|file| lines(file.body()))
        .filter_map(fields::<2>)
    {
        if left.is_empty() || right.is_empty() {
            continue;
        }
        let (a, b) = (users.intern(left), users.intern(right));
        while parents.len() < users.names.len() {
            parents.push(parents.len() as u32);
            sizes.push(1usize);
        }
        let (mut a, mut b) = (root(&mut parents, a), root(&mut parents, b));
        if a != b {
            if sizes[a] < sizes[b] {
                std::mem::swap(&mut a, &mut b);
            }
            parents[b] = a as u32;
            sizes[a] += sizes[b];
        }
    }
    let mut smallest = users.names.clone();
    for (id, &name) in users.names.iter().enumerate() {
        let component = root(&mut parents, id as u32);
        smallest[component] = min(smallest[component], name);
    }
    users
        .names
        .into_iter()
        .enumerate()
        .map(|(id, name)| (name, smallest[root(&mut parents, id as u32)]))
        .collect()
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
    ts: i64,
    price: u32,
    category: u32,
    ingest: [u8; 20],
    valid: bool,
}

struct NamedEvent<'a> {
    name: Text<'a>,
    hash: u64,
    event: Event,
}

#[derive(Clone, Copy, Default)]
struct Click<'a> {
    ts: i64,
    id: Text<'a>,
    campaign: u32,
}

struct NamedClick<'a> {
    name: Text<'a>,
    hash: u64,
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
    let mut shards: Vec<Vec<Row>> = (0..DEDUP_SHARDS).map(|_| Vec::new()).collect();
    for chunk in chunks {
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
            let ts = timestamp(ts)?;
            if ts < start || ts >= end {
                return None;
            }
            if timestamp(ingest)? - ts > DAY {
                return None;
            }
            Some((ts, price, category))
        })();
        let (ts, price, category) = parsed.unwrap_or((0, 0, 0));
        shards[shard].push(Row {
            hash,
            id,
            user,
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
    people: &FxHashMap<Text<'a>, Text<'a>>,
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

    let mut routed: Vec<Vec<NamedEvent>> = (0..workers).map(|_| Vec::new()).collect();
    for (batch, winner) in batches.iter().zip(&winner) {
        for (row, _) in batch.iter().zip(winner).filter(|&(row, &win)| win && row.valid) {
            let name = if people.is_empty() {
                row.user
            } else {
                people.get(row.user).copied().unwrap_or(row.user)
            };
            let hash = hash(name);
            routed[worker_of(hash, workers)].push(NamedEvent {
                name,
                hash,
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
    people: &FxHashMap<Text<'a>, Text<'a>>,
    catalog: &Catalog<'a>,
    start: i64,
    end: i64,
    workers: usize,
) -> Vec<Vec<NamedClick<'a>>> {
    let mut routed: Vec<Vec<NamedClick>> = (0..workers).map(|_| Vec::new()).collect();
    for chunk in chunks {
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
        let Some(ts) = timestamp(ts) else { return };
        if ts < start - WEEK || ts >= end {
            return;
        }
        let name = if people.is_empty() {
            user
        } else {
            people.get(user).copied().unwrap_or(user)
        };
        let hash = hash(name);
        routed[worker_of(hash, workers)].push(NamedClick {
            name,
            hash,
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
        event_person.push(people.intern(event.name, event.hash));
    }
    let mut click_person = Vec::with_capacity(click_batches.iter().map(Vec::len).sum());
    for click in click_batches.iter().flatten() {
        click_person.push(people.find(click.name, click.hash));
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
    let (people, copies) = thread::scope(|scope| {
        let people = scope.spawn(|| link_names(&links));
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
    run(&data, &output, workers)
}
