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
