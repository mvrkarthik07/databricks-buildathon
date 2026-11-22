//! Sub-phase CPU timers for profiling runs. Every call compiles to nothing unless the `prof`
//! feature is on, so the scored build is unchanged. Timers are wall time of one thread's work,
//! so run with `NCPU` no larger than the core count to read them as CPU time.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

pub const ROUTE: usize = 0;
pub const DEDUP_INSERT: usize = 1;
pub const DEDUP_WINNER: usize = 2;
pub const DEDUP_ROUTE: usize = 3;
pub const CLICKS: usize = 4;
pub const AGG_INTERN_EVENTS: usize = 5;
pub const AGG_FIND_CLICKS: usize = 6;
pub const AGG_GROUP_EVENTS: usize = 7;
pub const AGG_GROUP_CLICKS: usize = 8;
pub const AGG_SORT_EVENTS: usize = 9;
pub const AGG_SORT_CLICKS: usize = 10;
pub const AGG_SESSIONIZE: usize = 11;
pub const AGG_FLUSH: usize = 12;
pub const WRITE_USERS: usize = 13;
pub const WRITE_CAMPAIGNS: usize = 14;
pub const LINK_PARSE: usize = 15;
pub const BUILD_LINKS: usize = 16;
// Counters (not times).
pub const C_ROWS: usize = 17;
pub const C_DUP_PAIRS: usize = 18;
pub const C_KEY_COMPARES: usize = 19;
pub const C_SURVIVORS: usize = 20;
pub const C_PEOPLE: usize = 21;
pub const C_SESSIONS: usize = 22;
pub const C_CLICKS_KEPT: usize = 23;
pub const C_LINKED_EVENTS: usize = 24;

const NAMES: [&str; 25] = [
    "route_rows (parse+hash+validate+push)",
    "dedup: table insert + compares",
    "dedup: winner flags",
    "dedup: survivor resolve + route",
    "route_clicks",
    "aggregate: intern event people",
    "aggregate: find click people",
    "aggregate: group events",
    "aggregate: group clicks",
    "aggregate: sort events",
    "aggregate: sort+dedup clicks",
    "aggregate: sessionize+attribute",
    "aggregate: flush_daily",
    "parquet: user_daily",
    "parquet: campaign_daily",
    "link parse",
    "build_links (own thread)",
    "count: event rows parsed",
    "count: dedup pairs (same id seen)",
    "count: id byte compares",
    "count: surviving events",
    "count: people",
    "count: sessions",
    "count: clicks kept",
    "count: events routed to linked person",
];

static TOTAL: [AtomicU64; 25] = [const { AtomicU64::new(0) }; 25];

#[inline(always)]
pub fn now() -> Option<Instant> {
    if cfg!(feature = "prof") { Some(Instant::now()) } else { None }
}

#[inline(always)]
pub fn add(id: usize, start: Option<Instant>) {
    if let Some(start) = start {
        TOTAL[id].fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
    }
}

/// Adds the time since `*start` to `*acc` and restarts the clock.
#[inline(always)]
pub fn split(acc: &mut u64, start: &mut Option<Instant>) {
    if let Some(begin) = start {
        let now = Instant::now();
        *acc += now.duration_since(*begin).as_nanos() as u64;
        *begin = now;
    }
}

#[inline(always)]
pub fn add_ns(id: usize, ns: u64) {
    if cfg!(feature = "prof") {
        TOTAL[id].fetch_add(ns, Relaxed);
    }
}

pub fn report() {
    if !cfg!(feature = "prof") {
        return;
    }
    for (id, name) in NAMES.iter().enumerate() {
        let value = TOTAL[id].load(Relaxed);
        if id < C_ROWS {
            eprintln!("PROF {name:<42} {:>9.1} thread-ms", value as f64 / 1e6);
        } else {
            eprintln!("PROF {name:<42} {value:>12}");
        }
    }
}
