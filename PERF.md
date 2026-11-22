# Shopping Sessions performance log

## Phase 2 Rust baseline

The release build uses mmap, eight workers, sharded event deduplication,
parallel person sorts, and the low-level Parquet writer. The published
36 fixtures, seven adversarial fixtures, and 1M-event dev answer matched.
The 200k-event runs for seeds 15847, 63911, 98057, 220307, and 503387
matched both tables from the Python oracle row for row.

Native measurements use macOS arm64, Rust 1.93.1 release, and `NCPU=8`.
Each dataset had one warm-up followed by five runs with fresh output
directories. Wall time includes process launch, Parquet writes, and
process exit. Peak RSS is the largest of the five measured runs.

| Requested events | Median wall time | Peak RSS |
| --- | ---: | ---: |
| 20M | 4.2775 s | 3.227 GiB |
| 60M | 17.7434 s | 9.197 GiB |

Datasets used the release generator. The 20M-event data has 5M requested
clicks and seed 20261003. The 60M-event data has 15M requested clicks and
seed 20261015. Both datasets and outputs were written outside the repo.
Both full-scale outputs passed the session, revenue, and assist invariants
from the rules.

Docker socket access was denied. The Linux amd64 image build, the dev
check inside the container, cold-cache runs, and measurements under
scorer limits are pending.

## Reproduce the correctness gate

Install the Python dependency and prepare the published bundles as described
in [ref/README.md](ref/README.md). Use a fresh output directory.

```sh
cargo build --release --locked
make check-fixtures check-adversarial REF=target/release/shopping-sessions
target/release/shopping-sessions dev out
make check-dev
```

Run the same dev dataset in the scorer-style container with a fresh output
directory:

```sh
docker build --platform linux/amd64 -t shopping-sessions:latest .
starter-duckdb/scripts/docker_run.sh dev out-container
make check-dev OUT=out-container
```

## Phase 3: profile-driven speedups

Machine: Apple M5 (arm64, 10 cores), `NCPU=8` default, Rust 1.93, release build. This machine
cannot run the AVX2 row splitter (Rosetta reports `avx2=false`), so those paths were neither
changed nor measured; everything below is on the portable code. Local times are not scorer
times (the scorer is x86 Xeon).

The Phase 2 numbers above are out of date: HEAD before this phase ran 20M events in about 1.2 s
and 5M in about 0.29 s on this machine.

Datasets (generated outside the repo): 5M events / 1.25M clicks (seed 20261003) and 20M events /
5M clicks (seed 20261015). Times are medians of interleaved runs (`tools/ab.py`), so drift cancels;
they include about 10-15 ms of harness process start, the same for both builds.

| Experiment | Change | 5M | 20M | Correctness | Decision |
| --- | --- | ---: | ---: | --- | --- |
| Baseline | HEAD 6c87438 | 287.8 ms | 1200.3 ms | pass | |
| 1 | Dense (campaign, day) totals and a direct-indexed active-campaign set instead of two hash maps (36M assist increments at 20M) | 271.0 ms (-16) | 1106.5 ms (-96) | pass | kept |
| 2 | 64 dedup shards, shard bits from `hash >> 40` | 257.4 ms (-14 more) | 1111.8 ms (flat) | pass | kept |
| 3 | Map input files on several threads; build the catalog while they map | -6.7 ms (front matter 14.1 -> 5.6 ms) | flat | pass | kept |
| 4 | Merge dense campaign tables cell by cell; write `campaign_daily` as parallel parts | tail 6.4 -> 3.8 ms, not resolvable in wall time | flat | pass | kept |
| Best | 1 to 4 combined | 249.0 ms (-35, -12%) | 1064.4 ms (-139, -12%) | pass | |

1M dev set: 71.7 -> 60.6 ms. With `NCPU=4`, 5M: 459.4 -> 418.4 ms.

Rejected, and why:

- 128 and 256 dedup shards: no better than 64 at 5M and worse at 20M (128: 1142 ms).
- Replace the pre-parse `memchr` line count with a sampled estimate: 295 vs 298 ms at 5M, no
  effect locally. It may matter more with a cold page cache; not measurable here.
- Touch the bloom word 16 rows ahead in the dedup route loop: no change.
- Flat linear-probing link table. With a `persons` array behind it: no gain. With the target
  stored in the slot: 303 ms (5M) and 1128 ms (20M), worse. Without the bloom filter: 327 ms and
  1374 ms. The bloom filter is what keeps unlinked IDs cheap; hashbrown stays.

Where the time goes now (5M, thread-ms summed over 8 threads, SS_TIMING plus ad-hoc timers):
parse about 900, dedup insert 138, link resolve and route 149, per-person sort and sessionize 173,
user parquet 81, group by person 70, intern 60. Link resolve is the costliest single operation:
at 20M, 946 thread-ms with the bloom check about 184 and the table probe about 548.

Next opportunities, in rough order:

1. Parse. On this machine the scalar splitter is about 100 of 140 ns per row; the AVX2 splitter
   and a possible AVX-512 one need x86 hardware to measure and to check against the fixtures.
   Building with `-C target-cpu=x86-64-v3` is untested for the same reason.
2. Shrink `Row` (96 B) and `NamedEvent` (48 B). Memory traffic and first-touch page faults grow
   with these (peak RSS is about 270 B per event), and 4 KiB pages on Linux fault 4x as often.
3. Give linked people dense IDs at link build, so dedup routing and the aggregate intern step
   can skip a hash lookup per event.
4. A direct PLAIN-page parquet writer for `user_daily`.
5. Work-stealing for the parse phase: threads finish within about 10% of each other, so at most
   about 2%.

## Reproduce Phase 3

```sh
cargo build --release --locked
cargo test --release --locked
(cd generator && cargo build --release)
generator/target/release/generate --seed 20261003 --events 5000000 --clicks 1250000 --output /tmp/d5m
generator/target/release/generate --seed 20261015 --events 20000000 --clicks 5000000 --output /tmp/d20m

# correctness: published fixtures, adversarial set, 1M dev answer (see ref/README.md for setup)
make check-fixtures check-adversarial REF=target/release/shopping-sessions
target/release/shopping-sessions dev out && make check-dev
# independent check on other seeds, and against an older build
python3 ref/reference.py DATA /tmp/ref-out
target/release/shopping-sessions DATA /tmp/new-out
python3 tools/diff.py /tmp/new-out /tmp/ref-out

# benchmark: interleaved runs of an older build and this one
python3 tools/ab.py /tmp/d5m 15 ./shopping-sessions-old target/release/shopping-sessions
SS_TIMING=1 target/release/shopping-sessions /tmp/d5m /tmp/out   # per-phase laps
```

## Phase 4: record layout, link resolution, reassessment

Same machine, datasets and method as Phase 3, plus 10M-event sets for seeds 101, 202 and 303.
Medians of interleaved rounds (31 to 41), `tools/ab.py` now also reports child CPU time. The
machine had background load (a VM and a browser), so IQRs were 10-50 ms; differences under about
10 ms are noise. Phase 3 stays above, unchanged.

**Result of this phase: no further wall-clock gain; peak memory down 20%.** x86 has not been
tested (see below), so none of this is a statement about the scorer.

### Record layout

| Experiment | Change | Peak RSS 20M | 20M wall (vs Phase 3) | 20M CPU | Decision |
| --- | --- | ---: | ---: | ---: | --- |
| Phase 3 | `Row` 96 B, `NamedEvent` 48 B | 4702 MiB | | 8092 ms | |
| A | `Row` 80 B: 20-byte ingest copy -> reference into the mapping, `valid` folded into `category` | 4412 MiB | -4 ms | 7949 ms (-1.8%) | superseded by B |
| B1 | `Row` 48 B: `id`, `user`, `ingest` -> 8-byte (chunk, offset) line; names re-read on demand; `NamedEvent` 40 B; full `fields::<7>` re-parse for cold fields | | +115 ms | 8925 ms | rejected as is |
| B2 | B1 with the link slot carrying the target hashes | | +47 ms | 8462 ms | rejected as is |
| B3 | B2 with a one-field reader (`cold_field`) instead of `fields::<7>` | 3757 MiB (-20%) | -6 ms (noise) | 7927 ms | kept |

Why B1 lost: dedup insert went from 642 to 1265 thread-ms (about 600k duplicate pairs, four
full-line parses each), and moving the target out of the link slot made every linked event do two
dependent random loads. B3 fixes both. Remaining cost of B3 versus A, from per-phase timers at 20M:
dedup insert +160 thread-ms and intern +55, offset by group -100 and sort -55. Memory traffic is
not what limits this machine, so the smaller records do not show up as time here. They do cut
peak memory, which matters for the 24 GB limit at larger sizes, and may matter more on x86 with
4 KiB pages and less bandwidth; that is unmeasured.

Value ranges that make the narrower types safe: a line offset is below 2^32 because chunks are
about 4 MiB plus one line and `index_chunks` rejects any chunk over `u32::MAX` bytes (a single
line over 4 GiB), and chunk indexes are checked below 2^31 for the `NameRef` tag bit. Prices and
categories were not narrowed (a category count is not bounded below 2^32 by the input format), and
`ts` stays `i64`.

### Link resolution

Flat open-addressing table with 40-byte slots, and the same table with a software prefetch 12 rows
ahead in the resolve loop (x86 `prefetch`, aarch64 `prfm`): 20M wall +5.4 ms without prefetch, -1.3
ms with, 5M -2.6 and +3.6 ms. No gain, so hashbrown with a dense person index stays, and the bloom
filter stays. The earlier ablation that put link resolution at about 9% of wall time was removal of
the whole lookup, not of its memory latency, and latency hiding changed nothing.

### Reassessment of Phase 3 changes

| Change | Test | Result | Decision |
| --- | --- | --- | --- |
| 64 dedup shards | 32 / 64 / 128 on three 10M seeds | 32: +0.7, +2.0, -1.6 ms; 128: -11.1, +4.3, -4.9 ms; CPU lowest at 64 on 2 of 3 seeds | keep 64 |
| Parallel file mapping | revert to sequential at 1M, 5M, 10M, 20M | +0.2, +3.2, -1.2, +8.3 ms | keep (tiny, plausibly more with many small files) |
| Parallel `campaign_daily` writer | single writer at 1M, 5M, 10M, 20M | +2.3, +6.1, -9.1, +1.4 ms | removed: no measurable benefit |
| Presize click routing vectors | A/B | 5M -2.5, 20M -8.7 ms, CPU -0.5% | reverted: within noise |

### Cumulative against the original (6c87438), interleaved, 41 rounds

| Events | Original | Now | Change | CPU, original -> now |
| --- | ---: | ---: | ---: | ---: |
| 1M (dev) | 70.2 ms | 61.9 ms | -11.7% | 353 -> 336 ms |
| 5M | 288.0 ms | 243.5 ms | -15.5% | 1860 -> 1671 ms |
| 10M (seed 101) | 575.2 ms | 502.8 ms | -12.6% | 3919 -> 3534 ms |
| 20M | 1192.5 ms | 1083.2 ms | -9.2% | 8499 -> 7941 ms |

Against Phase 3 alone the current build is equal within noise (1M +1.5, 5M -1.6, 10M -25.8, 20M +14.2
ms). All numbers are Apple M5, 8 workers, not the scorer.

### x86

Not tested. This Mac cannot run the AVX2 splitter (Rosetta reports `avx2=false`, `avx512f=false`),
and Docker is denied. `cargo check --target x86_64-unknown-linux-musl` passes, so `rows_avx2`
compiles against every change here, but it has not been executed since Phase 2 and nothing about
it has been measured. `.github/workflows/x86-bench.yml` (manual `workflow_dispatch`, not yet run)
prints the runner's AVX2 and AVX-512 flags first, runs the fixtures, compares output with a base
commit, and A/B-times both builds. GitHub's runners are 4 vCPU, so run it with `NCPU` equal to the
core count, and treat its times as a relative comparison only.

### Where the CPU goes now (20M, about 7900 CPU-ms, 8 workers)

Parse and route about 3500 (44%), dedup insert about 650 to 800, link resolve and route about 850,
aggregate intern about 250 to 300, group by person about 400 to 500, per-person sort and sessionize
about 700, click routing about 880, parquet about 300, kernel time about 840 (page faults). The
parse share is the largest and is the part that differs most between this machine and the scorer.

Ideas not tried because they cannot be measured here: transparent huge pages for the large vectors
(about 10% of CPU is page-fault time), an AVX-512 splitter, and `target-cpu=x86-64-v3`.

## Reproduce Phase 4

```sh
cargo test --release --locked
python3 tools/ab.py /tmp/d20m 41 ./shopping-sessions-6c87438 target/release/shopping-sessions
for s in 101 202 303; do generator/target/release/generate --seed $s --events 10000000 --clicks 2500000 --output /tmp/d10_$s; done
/usr/bin/time -l target/release/shopping-sessions /tmp/d20m /tmp/out   # peak RSS
```

## Phase 5: x86 measurements and the 1.99 s scored build

Scored commit `0141e3d` (official 1.99 s). Experiment branch: `exp/compact-ids`; `main` and the
branch `scored-1.99s` still point at the scored build. x86 numbers come from GitHub-hosted runners
(`.github/workflows/x86-bench.yml`, manual dispatch): 4 vCPU, 16 GB, and whichever CPU the pool
hands out (AMD EPYC 9V45, EPYC 7763 and Intel Xeon 6973P-C in these runs). They are not the
scorer's 8-vCPU Cascade Lake. Use them to compare builds and stages, never as a score.
Dataset: seed 20261003, 20M events, 5M clicks. The 36 published fixtures are in a git-ignored
folder, so CI runs the 7 adversarial fixtures and the Python oracle on seeds 11, 22, 33 instead;
the 36 run locally.

### Scored build, per stage (EPYC 9V45, 4 vCPU, 3.27 s total, peak RSS 3.8 GB)

| Stage | Wall | Share |
| --- | ---: | ---: |
| catalog, mapping, link parse | 30 ms | 1% |
| parse + route events | 1064 ms | 32% |
| dedup (insert, winners, resolve + route) | 904 ms | 28% |
| click routing | 300 ms | 9% |
| aggregate + user parquet | 896 ms | 27% |
| merge + campaign parquet + exit | 80 ms | 2% |

Sampled profile, 4 threads, 22k samples (share of all CPU): dedup 19.1%, aggregate 16.1%, AVX2
row splitter 12.0%, `route_rows` field parse 7.5%, `memchr` 4.5% (the line-count pre-pass),
`Links::resolve` 4.5%, timestamp parse 3.9%, kernel page faults and zeroing about 6%, event
quicksort 2.0%. Single-thread microbenchmark (`SS_BENCH=1`), ns per event row: AVX2 split 37
(scalar `memchr2` split 84), +event-id hash 4, +user hashes 1, +both timestamps 23, full
`route_rows` 113 to 118 whatever the shard count from 1 to 512 (4096: 140).

### Results

| Experiment | x86 result | Output | Decision |
| --- | --- | --- | --- |
| Phases 3 and 4 against the original `6c87438` | 3709 -> 3284 ms (-10.8%) | matches | already in the scored build |
| Shard count sweep 32/64/128/256/512/1024/2048 | 2943, 2919, 2904, 2869, **2771**, 2817, 2831 ms | all 7 match | tables of about 40k rows fit a 2 MiB L2 |
| Adaptive shards (`SHARD_ROWS` = 40 000, so 512 at 20M) vs fixed 64 | -119.7 ms (-3.8%, Xeon 6973P-C), -177.6 ms (-5.2%, EPYC 7763) | match | kept |
| Adaptive shards build vs scored build, Intel Xeon 6973P-C | 3215.8 -> 3026.9 ms (-5.9%), CPU 11903 -> 11185 ms | match | |
| Sampled row-count estimate in place of the line-count pre-pass | -29 ms in one run, +29 ms by medians, CPU unchanged | match | reverted: the cold-memory cost moves to the splitter |

On the M5 the shard count showed nothing (Phase 4); its L2 is far larger, which is why it only
appeared on x86.

### What was not built, and why

- **Compact-ID decoding and direct-index dedup.** The generator writes `base36(scramble(index, key))`
  with a per-dataset key. A decoded ID is already a uniform 40-bit number, so decoding replaces
  hashing at about the same cost. Measured hashing is 4 to 5 of about 113 ns per row, and the
  dedup insert is a cache-miss cost, which the L2-sized shards already cut. Recovering the dense
  index needs the generator's fixed multipliers and a search over about 2^25 key candidates, and
  correctness needs a fallback for noncanonical IDs (leading zeros, uppercase, over 40 bits). The
  most it can save is the dedup insert, about 9% of CPU (987 thread-ms of about 11 s). Not
  implemented; it would also tie the fast path to this generator.
- **Why 7.65x is out of reach for any single change.** The target is 0.26 s on 8 vCPUs, about 2.1
  thread-seconds for 20M rows, or about 100 ns per row for all stages together. The measured total
  here is about 11 to 12 thread-seconds, 550 to 600 ns per row, and `route_rows` alone is 113 ns.
  The largest stage is 19% of CPU. Removing dedup entirely would give 1.24x. Reaching the target
  needs every stage at least five times cheaper: AVX-512 splitting and timestamp parsing,
  records written once instead of three times, and a person-keyed layout that skips the second
  redistribution. That is a rewrite, not an experiment, and nothing here shows it would get to 0.26 s.

### Reproduce

```sh
gh workflow run x86-bench.yml --ref exp/compact-ids -f base=0141e3d -f events=20000000
gh workflow run x86-bench.yml --ref exp/compact-ids -f only_micro=true   # microbenchmark + perf profile
SS_SHARDS=512 SS_TIMING=1 target/release/shopping-sessions DATA OUT        # knob and stage timers
SS_BENCH=1 target/release/shopping-sessions DATA OUT                       # parse microbenchmark
```

## Phase 6: rewrite prototypes (engine v2, v3) and huge pages

Branch `exp/rewrite-v2` (from `exp/compact-ids` = adaptive shards). `main` and `scored-1.99s` stay at
`0141e3d`. All three engines live in one binary, selected by `SS_ENGINE=v1|v2|v3` (default v1, the
generic engine), so every comparison is the same build on the same runner and data.

- **v2** (`src/v2.rs`): fused parse+pack writes a 40 B row to a lane-local sequential array and a 24 B
  dedup record to a radix partition; an open-addressing table per partition marks losers in a
  1 B/row flag array (no winner list, no second table); a lane pass skips losers, resolves the person
  and routes. Ingest is compared as a number from a canonical 20-byte field; anything else sends the
  whole run to the generic engine.
- **v3**: links are built first, so the parse pass resolves the person inline and writes the final
  routed 40 B record once. The dedup record holds only (hash, line, slot); a loser is tombstoned in
  place (`category = INVALID`), found with a byte-wise ingest comparison (no canonical-form
  assumption, no fallback). No resolve+route pass.
- **THP** (`src/thp.rs`, `SS_THP=1`, Linux only): allocations of 8 MiB or more are mapped directly,
  2 MiB-aligned, with `MADV_HUGEPAGE`. Everything else uses the system allocator.

Correctness (x86 CI, run 44978377251): v2, v3, v1+THP and v3+THP each pass the 7 adversarial fixtures and
the Python oracle on seeds 11/22/33/44 at NCPU 1, 3 and 8; v2/v3/THP output equals the generic
engine on 10M-event seeds 7 and 99 and equals the scored build on the 20M dataset. Locally (M5): all
36 fixtures, 7 adversarial, the 1M official answer, d5m/d20m vs baseline, 5 oracle seeds, for v1 and v3.

### Interleaved A/B, EPYC 9V45, 4 vCPU, 13 rounds, 20M events (wall median, ms; CPU ms)

| Build | Wall | vs scored | CPU |
| --- | ---: | ---: | ---: |
| scored `0141e3d` | 2698 | | 9801 |
| v1 (adaptive shards) | 2661 | -1.5% | 9635 |
| v2 | 2665 | -1.2% | 9409 |
| v3 | 2737 | +1.4% | 9489 |
| **v1 + THP** | **2381** | **-11.8%** | 8760 |
| v3 + THP | 2382 | -11.7% | 8621 |

Extra seeds, 10M events, 9 rounds, wall vs v1: seed 7: THP -78 ms (-6.1%), v2 +39, v3 +37; seed 99:
THP -84 ms (-7.2%), v2 +109, v3 +77. An earlier run on an EPYC 7763 had v2 +56 ms at 20M and
+127/+172 ms on the 10M seeds, so the result does not depend on the CPU.

### Why v2 and v3 were rejected

- v2's dedup is genuinely cheaper (668 vs 874 thread-ms on the 9V45; 823 vs 983 on the 7763), but the
  lane-local row array is a new 40 B/row allocation: minor page faults went 616k -> 902k and the
  parse pass got slower by about the amount dedup saved. Resolve+route is unchanged (1351 vs 1456
  thread-ms) because it still reads every row once.
- v3 removes that pass, but moves person resolution (link lookup, user hashes) in front of dedup, so
  it runs for the 2 to 4% duplicate rows too and writes 40 B per event in the parse pass. The parse
  pass grew more than the removed stage shrank.
- Neither changes the unavoidable work: parse (113 ns/row), group, sort, sessionize, Parquet.
  Moving records between stages differently only moves cost between stages.
- Hardware counters (cycles, cache misses) are not exposed on these runners (`<not supported>`), so
  cycles/row and miss rates could not be measured; task-clock and page faults are what is reported.
  Task-clock: v1 9969 ms, v2 9818, v3 9895 (20M, 4 vCPU), i.e. 498, 491, 495 ns/row of CPU.

### The one real lever: kernel memory cost

Peak RSS is about 3.9 GB for 20M events; page faulting and zeroing was already about 6% of samples
in Phase 5 and the first-touch cost is spread across every stage. 2 MiB pages cut CPU by 9 to 10%
and wall by 6 to 12%, on both engines, on every dataset. That is larger than anything the data-layout
rewrites bought, and it needs no change to semantics. It is opt-in (`SS_THP=1`); a Linux host with
THP set to `never` gets nothing from `madvise` and loses nothing.

### Is full integration justified?

No. At 2381 ms on a 4-vCPU shared runner against a 2698 ms scored-build baseline on the same runner,
the best verified build is 1.13x faster. The target is 7.65x. Nothing measured here is on a path to
it: the remaining cost is spread over parse, group/sort and Parquet, each of which would need to be
several times cheaper at once, and the three layouts tried so far trade cost between stages without
removing it.

### Reproduce

```sh
gh workflow run x86-bench.yml --ref exp/rewrite-v2 -f base=0141e3d -f only_micro=engine
SS_ENGINE=v3 SS_THP=1 SS_TIMING=1 target/release/shopping-sessions DATA OUT
```
