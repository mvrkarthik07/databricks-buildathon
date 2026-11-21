# Shopping Sessions reference

Install the reference and checker dependency in a Python 3.10+ environment.

```sh
python3 -m pip install -r ref/requirements.txt
unzip -n starter-duckdb.zip
unzip -n dev-answer.zip
cargo build --release --locked --manifest-path generator/Cargo.toml
generator/target/release/generate --seed 1 --events 1000000 --clicks 250000 --output dev
make check-fixtures check-adversarial
make run-ref-dev check-dev
```

For another dataset, run `python3 ref/reference.py DATA_DIR OUTPUT_DIR`.
Run `python3 tools/diff.py CANDIDATE_DIR REFERENCE_DIR` to compare Parquet
outputs or to compare against the published flat CSV answers. The checker
reports missing keys, extra keys, and changed rows.

The adversarial fixtures contain handwritten expected rows. They cover:

| Fixture | Rules exercised |
| --- | --- |
| `csv-dialect` | Whole-field quotes, quoted empties, literal carriage returns, spaces, ASCII digits, malformed field counts and quotes, multi-part input, header-only files, and a final row without a newline |
| `timestamps` | F1/F2/F3, signed offsets and UTC date crossings, leap day, invalid forms, window boundaries, and 5,000-digit F3 values and padding |
| `century-leap` | Gregorian century leap-year exceptions |
| `dedup-and-validation` | Earliest ingest across files, identical ties, invalid earliest copies, exact 24-hour lateness vs. 24 hours plus 1 ms, price limits, and unknown products |
| `sessions-and-categories` | Exact 30-minute gaps and gaps 1 ms longer, equal timestamps, UTC midnight, daily category sums and byte-wise ties |
| `link-components` | A chain of 300 links spread across files, shared device, transitivity, unused minimum ID, case variants, empty sides, self-links, and repeated links |
| `click-windows-and-assists` | Exact and just-outside 7-day and session-start bounds, byte-wise click-ID ties, omitted and unknown campaigns, click reuse, attribution through a linked ID, and organic and assist-only rows |
