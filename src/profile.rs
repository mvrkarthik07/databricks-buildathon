#[cfg(feature = "phase-timing")]
use Phase::*;

#[derive(Clone, Copy)]
pub enum Phase {
    ReadParse,
    Dedup,
    Links,
    SortSessionize,
    Attribution,
    Aggregation,
    Parquet,
}

#[cfg(feature = "phase-timing")]
type Clock = std::time::Instant;
#[cfg(not(feature = "phase-timing"))]
type Clock = ();

#[cfg(feature = "phase-timing")]
type Times = [u64; Parquet as usize + 1];
#[cfg(not(feature = "phase-timing"))]
type Times = ();

#[cfg(feature = "phase-timing")]
thread_local! {
    static TIMES: std::cell::Cell<Times> = const { std::cell::Cell::new([0; Parquet as usize + 1]) };
}

pub struct Timer {
    #[cfg(feature = "phase-timing")]
    phase: Phase,
    #[cfg(feature = "phase-timing")]
    start: Clock,
}

impl Drop for Timer {
    fn drop(&mut self) {
        #[cfg(feature = "phase-timing")]
        TIMES.with(|times| {
            let mut counts = times.get();
            counts[self.phase as usize] += self.start.elapsed().as_nanos() as u64;
            times.set(counts);
        });
    }
}

#[inline]
pub fn clock() -> Clock {
    #[cfg(feature = "phase-timing")]
    return Clock::now();
}

#[inline]
pub fn measure(_phase: Phase) -> Timer {
    Timer {
        #[cfg(feature = "phase-timing")]
        phase: _phase,
        #[cfg(feature = "phase-timing")]
        start: clock(),
    }
}

#[inline]
pub fn in_phase<T>(phase: Phase, run: impl FnOnce() -> T) -> T {
    let _timer = measure(phase);
    run()
}

#[inline]
pub fn isolated<T>(run: impl FnOnce() -> T) -> (T, Times) {
    #[cfg(feature = "phase-timing")]
    {
        let previous = TIMES.take();
        let result = run();
        (result, TIMES.replace(previous))
    }
    #[cfg(not(feature = "phase-timing"))]
    (run(), ())
}

#[inline]
pub fn merge(_into: &mut Times, _other: Times) {
    #[cfg(feature = "phase-timing")]
    for (total, time) in _into.iter_mut().zip(_other) {
        *total += time;
    }
}

#[inline]
pub fn account(_start: Clock, _workers: Times) {
    #[cfg(feature = "phase-timing")]
    {
        let total: u64 = _workers.iter().sum();
        if total != 0 {
            // Each phase gets its share of the parallel stage's wall time.
            let scale = _start.elapsed().as_nanos() as f64 / total as f64;
            let scaled = _workers.map(|ns| (ns as f64 * scale) as u64);
            TIMES.with(|times| {
                let mut counts = times.get();
                merge(&mut counts, scaled);
                times.set(counts);
            });
        }
    }
}

pub fn report() {
    #[cfg(feature = "phase-timing")]
    for (phase, name) in [
        (ReadParse, "read_parse"),
        (Dedup, "dedup"),
        (Links, "links"),
        (SortSessionize, "sort_sessionize"),
        (Attribution, "attribution"),
        (Aggregation, "aggregation"),
        (Parquet, "parquet"),
    ] {
        let seconds = TIMES.get()[phase as usize] as f64 / 1e9;
        eprintln!("phase {name} {seconds:.6}");
    }
}
