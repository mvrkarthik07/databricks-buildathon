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
