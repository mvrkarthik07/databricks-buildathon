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
