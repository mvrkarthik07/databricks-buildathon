#!/usr/bin/env python3
"""Read the Shopping Sessions inputs and write the two Parquet tables."""
from __future__ import annotations

import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
GAP_MS = 1_800_000
EPOCH_DAY = date(1970, 1, 1)
EPOCH = datetime(1970, 1, 1)
CIVIL_TIMESTAMP = re.compile(
    rb"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
    rb"(?:Z|([+-])([0-9]{2}):([0-9]{2}))"
)
SCHEMAS = {
    "user_daily": pa.schema([
        ("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()),
        ("events", pa.int64()), ("revenue_cents", pa.int64()), ("top_category", pa.string()),
    ]),
    "campaign_daily": pa.schema([
        ("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()),
        ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()),
        ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64()),
    ]),
}


def csv_rows(paths, columns):
    for path in paths:
        with path.open("rb") as source:
            next(source, None)
            for line in source:
                fields = line.removesuffix(b"\n").split(b",")
                if len(fields) != columns:
                    continue
                values = []
                for field in fields:
                    if len(field) >= 2 and field.startswith(b'"') and field.endswith(b'"'):
                        field = field[1:-1]
                    if b'"' in field:
                        break
                    values.append(field or None)
                else:
                    yield values


def timestamp(value):
    if value is None:
        return None
    if value.isdigit():
        digits = value.lstrip(b"0") or b"0"
        # Decimal handles integers beyond Python's decimal-string conversion limit.
        return int(digits) if len(digits) <= 640 else int(Decimal(digits.decode("ascii")))
    match = CIVIL_TIMESTAMP.fullmatch(value)
    if match is None:
        return None
    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    try:
        delta = datetime(year, month, day, hour, minute, second) - EPOCH
    except ValueError:
        return None
    ms = delta.days * DAY_MS + delta.seconds * 1_000
    sign, offset_hour, offset_minute = match.groups()[6:]
    if sign is None:
        return ms
    hours, minutes = int(offset_hour), int(offset_minute)
    if hours > 23 or minutes > 59:
        return None
    offset = (hours * 60 + minutes) * 60_000
    return ms - offset if sign == b"+" else ms + offset


def people(paths):
    neighbors = defaultdict(set)
    for left, right in csv_rows(paths, 2):
        if left is not None and right is not None:
            neighbors[left].add(right)
            neighbors[right].add(left)

    names = {}
    for first in neighbors:
        if first in names:
            continue
        component = {first}
        pending = [first]
        while pending:
            for other in neighbors[pending.pop()] - component:
                component.add(other)
                pending.append(other)
        name = min(component)
        for member in component:
            names[member] = name
    return names


def events_by_person(paths, names, products, start, end):
    kept = {}
    for row in csv_rows(paths, 7):
        event_id, _, _, ingest, _, _, _ = row
        # Fixed-width UTC F1 sorts in timestamp order.
        if event_id is not None and (event_id not in kept or ingest < kept[event_id][3]):
            kept[event_id] = row

    events = defaultdict(list)
    for _, user, raw_ts, ingest, kind, product, raw_price in kept.values():
        ts = timestamp(raw_ts)
        if user is None or ts is None or not start <= ts < end:
            continue
        if kind not in (b"view", b"cart", b"purchase") or product is None:
            continue

        price = 0
        category = None
        if kind == b"purchase":
            if raw_price is None or not raw_price.isdigit():
                continue
            digits = raw_price.lstrip(b"0")
            if not digits or len(digits) > 8 or (price := int(digits)) > 10_000_000:
                continue
            category = products.get(product, b"unknown")

        if timestamp(ingest) - ts > DAY_MS:
            continue
        events[names.get(user, user)].append((ts, price, category))
    return events


def clicks_by_person(paths, names, campaigns, start, end):
    seen = set()
    clicks = defaultdict(list)
    for click_id, user, raw_campaign, raw_ts in csv_rows(paths, 4):
        if click_id is None or click_id in seen:
            continue
        seen.add(click_id)
        ts = timestamp(raw_ts)
        if user is None or raw_campaign not in campaigns or ts is None or not start - WEEK_MS <= ts < end:
            continue
        clicks[names.get(user, user)].append((ts, click_id, raw_campaign))
    return clicks


def sessions(events):
    session = []
    for event in sorted(events, key=lambda event: event[0]):
        if session and event[0] - session[-1][0] > GAP_MS:
            yield session
            session = []
        session.append(event)
    if session:
        yield session


def summarize(events, clicks, campaigns):
    user_daily = defaultdict(Counter)
    categories = defaultdict(Counter)
    campaign_daily = defaultdict(Counter)
    for person, person_events in events.items():
        person_clicks = sorted(clicks.get(person, ()))
        click_times = [ts for ts, _, _ in person_clicks]
        for session in sessions(person_events):
            start = session[0][0]
            day = date.fromordinal(EPOCH_DAY.toordinal() + start // DAY_MS)
            key = person, day
            user_daily[key]["sessions"] += 1
            user_daily[key]["events"] += len(session)
            revenue = 0
            for _, price, category in session:
                if price:
                    revenue += price
                    categories[key][category] += price
            user_daily[key]["revenue_cents"] += revenue

            first = bisect_left(click_times, start - WEEK_MS)
            last = bisect_right(click_times, start)
            qualifying_clicks = person_clicks[first:last]
            campaign = b"organic"
            if qualifying_clicks:
                _, _, campaign = min(qualifying_clicks, key=lambda click: (-click[0], click[1]))
                for assisted in {click[2] for click in qualifying_clicks}:
                    campaign_daily[assisted, day]["assisted_sessions"] += 1
            credited = campaign_daily[campaign, day]
            credited["sessions"] += 1
            credited["purchasing_sessions"] += int(revenue > 0)
            credited["revenue_cents"] += revenue

    for column in ("sessions", "revenue_cents"):
        assert sum(row[column] for row in user_daily.values()) == sum(
            row[column] for row in campaign_daily.values()
        )
    assert sum(row["assisted_sessions"] for row in campaign_daily.values()) >= sum(
        row["sessions"] for (campaign, _), row in campaign_daily.items() if campaign != b"organic"
    )

    user_rows = []
    for (person, day), totals in user_daily.items():
        spend = categories.get((person, day))
        top = min(spend, key=lambda category: (-spend[category], category)) if spend else None
        user_rows.append(dict(user_id=person, day=day, top_category=top, **totals))

    campaign_rows = []
    for (campaign, day), totals in campaign_daily.items():
        campaign_rows.append(dict(
            campaign_id=campaign,
            channel=b"organic" if campaign == b"organic" else campaigns[campaign],
            day=day,
            sessions=totals["sessions"],
            purchasing_sessions=totals["purchasing_sessions"],
            revenue_cents=totals["revenue_cents"],
            assisted_sessions=totals["assisted_sessions"],
        ))
    return {"user_daily": user_rows, "campaign_daily": campaign_rows}


def run(data, output):
    manifest = json.loads((data / "manifest.json").read_bytes())
    start, end = (timestamp(manifest[key].encode("ascii")) for key in ("window_start", "window_end"))
    names = people(sorted((data / "links").glob("*.csv")))
    products = dict(csv_rows([data / "products.csv"], 2))
    campaigns = dict(csv_rows([data / "campaigns.csv"], 2))
    events = events_by_person(sorted((data / "events").glob("*.csv")), names, products, start, end)
    clicks = clicks_by_person(sorted((data / "clicks").glob("*.csv")), names, campaigns, start, end)
    for table, rows in summarize(events, clicks, campaigns).items():
        folder = output / table
        folder.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMAS[table]), folder / "part-0.parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", nargs="?", type=Path, default=Path(os.environ.get("DATA_DIR", "/data")))
    parser.add_argument("output", nargs="?", type=Path, default=Path(os.environ.get("OUTPUT_DIR", "/output")))
    args = parser.parse_args()
    run(args.data, args.output)
