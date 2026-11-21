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

use input::{DAY, GAP, Mapped, WEEK, chunk_batches, csv_files, decimal, fields, lines, timestamp};
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

#[derive(Clone, Copy)]
struct EventCopy<'a> {
    line: Text<'a>,
    hash: u64,
    id_end: usize,
    ingest_start: usize,
}

impl<'a> EventCopy<'a> {
    fn id(self) -> Text<'a> {
        &self.line[usize::from(self.line[0] == b'"')..self.id_end]
    }

    fn ingest(self) -> Text<'a> {
        &self.line[self.ingest_start..self.ingest_start + 20]
    }
}

#[derive(Clone, Copy)]
struct Event {
    ts: i64,
    person: u32,
    price: u32,
    category: u32,
}

struct NamedEvent<'a> {
    name: Text<'a>,
    event: Event,
}

#[derive(Clone, Copy)]
struct Click<'a> {
    ts: i64,
    id: Text<'a>,
    person: u32,
    campaign: u32,
}

struct NamedClick<'a> {
    name: Text<'a>,
    click: Click<'a>,
}

fn route_copies(chunks: Vec<&[u8]>) -> Vec<Vec<EventCopy<'_>>> {
    let mut shards = vec![Vec::new(); DEDUP_SHARDS];
    for line in chunks.into_iter().flat_map(lines) {
        let Some([id, _, _, ingest, _, _, _]) = fields(line) else {
            continue;
        };
        if id.is_empty() || ingest.len() != 20 {
            continue;
        }
        let hash = hash(id);
        // Keep shard bits separate from hashbrown's low bucket bits and top seven tag bits.
        let shard = ((hash >> 52) as usize) % DEDUP_SHARDS;
        shards[shard].push(EventCopy {
            line,
            hash,
            id_end: id.as_ptr().addr() + id.len() - line.as_ptr().addr(),
            ingest_start: ingest.as_ptr().addr() - line.as_ptr().addr(),
        });
    }
    shards
}

fn dedup_events<'a>(
    batches: Vec<Vec<EventCopy<'a>>>,
    people: &FxHashMap<Text<'a>, Text<'a>>,
    catalog: &Catalog<'a>,
    start: i64,
    end: i64,
    workers: usize,
) -> Vec<Vec<NamedEvent<'a>>> {
    let mut kept = HashTable::with_capacity(batches.iter().map(Vec::len).sum());
    for event in batches.into_iter().flatten() {
        match kept.entry(
            event.hash,
            |old: &EventCopy| old.id() == event.id(),
            |row| row.hash,
        ) {
            Entry::Vacant(entry) => {
                entry.insert(event);
            }
            Entry::Occupied(mut entry) if event.ingest() < entry.get().ingest() => {
                *entry.get_mut() = event;
            }
            Entry::Occupied(_) => {}
        }
    }

    let mut routed: Vec<Vec<NamedEvent>> = (0..workers).map(|_| Vec::new()).collect();
    for copy in kept {
        let [_, user, ts, _, kind, product, price] =
            fields(copy.line).expect("dedup only receives parsed rows");
        if user.is_empty() || product.is_empty() {
            continue;
        }
        let (price, category) = match kind {
            b"view" | b"cart" => (0, 0),
            b"purchase" => {
                let Some(price) = decimal(price, 10_000_000).filter(|&n| n != 0) else {
                    continue;
                };
                let category = catalog
                    .products
                    .ids
                    .get(product)
                    .map_or(catalog.unknown, |&id| catalog.product_category[id as usize]);
                (price as u32, category)
            }
            _ => continue,
        };
        let Some(ts) = timestamp(ts) else { continue };
        if ts < start || ts >= end {
            continue;
        }
        let Some(ingest) = timestamp(copy.ingest()) else {
            continue;
        };
        if ingest - ts > DAY {
            continue;
        }
        let name = people.get(user).copied().unwrap_or(user);
        routed[hash(name) as usize % workers].push(NamedEvent {
            name,
            event: Event {
                ts,
                price,
                category,
                person: 0,
            },
        });
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
    for [id, user, campaign, ts] in chunks.into_iter().flat_map(lines).filter_map(fields::<4>) {
        if id.is_empty() || user.is_empty() {
            continue;
        }
        let Some(&campaign) = catalog.campaigns.ids.get(campaign) else {
            continue;
        };
        if campaign == catalog.organic {
            continue;
        }
        let Some(ts) = timestamp(ts) else { continue };
        if ts < start - WEEK || ts >= end {
            continue;
        }
        let name = people.get(user).copied().unwrap_or(user);
        routed[hash(name) as usize % workers].push(NamedClick {
            name,
            click: Click {
                ts,
                id,
                campaign,
                person: 0,
            },
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

fn aggregate<'a>(
    event_batches: Vec<Vec<NamedEvent<'a>>>,
    click_batches: Vec<Vec<NamedClick<'a>>>,
    catalog: &Catalog<'a>,
) -> (Vec<UserRow<'a>>, CampaignDaily) {
    let mut people = Ids::default();
    let mut events = Vec::with_capacity(event_batches.iter().map(Vec::len).sum());
    for NamedEvent { name, mut event } in event_batches.into_iter().flatten() {
        event.person = people.intern(name);
        events.push(event);
    }
    let mut clicks = Vec::with_capacity(click_batches.iter().map(Vec::len).sum());
    for NamedClick { name, mut click } in click_batches.into_iter().flatten() {
        if let Some(&person) = people.ids.get(name) {
            click.person = person;
            clicks.push(click);
        }
    }
    events.sort_unstable_by_key(|event| (event.person, event.ts));
    // At a tied timestamp the last click must have the smallest byte ID.
    clicks.sort_unstable_by_key(|click| (click.person, click.ts, Reverse(click.id)));
    // Identical click copies have the same validity and always reach the same person.
    clicks.dedup_by_key(|click| (click.person, click.ts, click.id));

    let mut users = Vec::new();
    let mut campaigns = CampaignDaily::default();
    let mut remaining_clicks = clicks.as_slice();
    let mut spend = vec![0i64; catalog.categories.names.len()];
    let mut touched = Vec::new();
    let mut active: FxHashMap<u32, usize> = FxHashMap::with_hasher(FxBuildHasher);

    for person_events in events.chunk_by(|a, b| a.person == b.person) {
        let person = person_events[0].person;
        let name = people.names[person as usize];
        let begin = remaining_clicks.partition_point(|click| click.person < person);
        remaining_clicks = &remaining_clicks[begin..];
        let end = remaining_clicks.partition_point(|click| click.person == person);
        let (person_clicks, rest) = remaining_clicks.split_at(end);
        remaining_clicks = rest;

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
    let links = csv_files(&data.join("links"))?;
    let people = link_names(&links);
    let event_files = csv_files(&data.join("events"))?;
    let click_files = csv_files(&data.join("clicks"))?;

    let copies = parallel(chunk_batches(&event_files, workers), workers, route_copies);
    let events = parallel(transpose(copies, DEDUP_SHARDS), workers, |batches| {
        dedup_events(batches, &people, &catalog, start, end, workers)
    });
    let clicks = parallel(chunk_batches(&click_files, workers), workers, |chunks| {
        route_clicks(chunks, &people, &catalog, start, end, workers)
    });

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
