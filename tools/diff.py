#!/usr/bin/env python3
"""Compare two Shopping Sessions output folders by key, in any row or file order."""
import argparse
import csv
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Both tables start with their two key fields here, regardless of file column order.
COLUMNS = {
    "user_daily": {
        "user_id": str, "day": date, "sessions": int, "events": int,
        "revenue_cents": int, "top_category": str,
    },
    "campaign_daily": {
        "campaign_id": str, "day": date, "channel": str, "sessions": int,
        "purchasing_sessions": int, "revenue_cents": int, "assisted_sessions": int,
    },
}


def check_columns(columns, expected, path):
    if len(columns) != len(expected) or set(columns) != set(expected):
        raise ValueError(f"{path}: expected columns {list(expected)}, got {columns}")


def read_csv(path, columns):
    with path.open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source, strict=True)
        check_columns(reader.fieldnames or [], columns, path)
        for row in reader:
            if None in row or None in row.values():
                raise ValueError(f"{path}:{reader.line_num}: wrong number of fields")
            values = {}
            for name, kind in columns.items():
                raw = row[name]
                if name == "top_category" and raw == "":
                    values[name] = None
                else:
                    values[name] = date.fromisoformat(raw) if kind is date else kind(raw)
            yield values


def read_parquet(folder, columns):
    if folder.is_symlink() or not folder.is_dir():
        raise ValueError(f"expected an output directory: {folder}")
    files = [
        path for path in sorted(folder.glob("*.parquet"))
        if not path.name.startswith((".", "_")) and not path.is_symlink() and path.is_file()
    ]
    if not files:
        raise ValueError(f"no Parquet files in {folder}")

    for path in files:
        with pq.ParquetFile(path) as parquet:
            schema = parquet.schema_arrow
            check_columns(schema.names, columns, path)
            for name, kind in columns.items():
                actual = schema.field(name).type
                if pa.types.is_dictionary(actual):
                    actual = actual.value_type
                if kind is str:
                    valid = pa.types.is_string(actual) or pa.types.is_large_string(actual) or pa.types.is_string_view(actual)
                elif kind is int:
                    valid = pa.types.is_integer(actual)
                else:
                    day_column = parquet.schema.column(parquet.schema.names.index(name))
                    valid = day_column.logical_type.type == "DATE"
                if not valid:
                    raise ValueError(f"{path}: {name} has type {actual}, expected {kind.__name__}")
            for batch in parquet.iter_batches(columns=list(columns)):
                yield from batch.to_pylist()


def load(folder, table, columns):
    # The published expected answers use flat CSV files.
    expected = folder / f"{table}.csv"
    rows = read_csv(expected, columns) if expected.is_file() else read_parquet(folder / table, columns)
    indexed = {}
    for row in rows:
        values = tuple(row[name] for name in columns)
        if any(value is None for name, value in zip(columns, values) if name != "top_category"):
            raise ValueError(f"{folder}/{table}: unexpected null in {values}")
        key = values[:2]
        if key in indexed:
            raise ValueError(f"{folder}/{table}: duplicate key {key}")
        indexed[key] = values
    return indexed


def compare(left, right):
    matched = True
    for table, columns in COLUMNS.items():
        a, b = (load(folder, table, columns) for folder in (left, right))
        only_a = a.keys() - b.keys()
        only_b = b.keys() - a.keys()
        differing = [key for key in a.keys() & b.keys() if a[key] != b[key]]
        print(f"{table}: rows {len(a)} vs {len(b)}; extra in A {len(only_a)}; "
              f"missing from A {len(only_b)}; differing {len(differing)}")
        for key in sorted(only_a)[:5]:
            print(f"  only A: {a[key]}")
        for key in sorted(only_b)[:5]:
            print(f"  only B: {b[key]}")
        for key in sorted(differing)[:5]:
            print(f"  A: {a[key]}\n  B: {b[key]}")
        matched &= not (only_a or only_b or differing)
    print("MATCH" if matched else "DIFFER")
    return matched


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    args = parser.parse_args()
    try:
        return 0 if compare(args.left, args.right) else 1
    except (ValueError, OSError, csv.Error) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
