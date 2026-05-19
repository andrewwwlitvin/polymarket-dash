#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Derive Top-12 HOT and Top-12 OVERLOOKED from a single enriched CSV and
write a compact selector CSV: polymarket_top12_<YYYYmmdd_HHMMSS>.csv

- The output CSV contains the SAME columns as the enriched CSV, plus:
    bucket   -> "HOT" or "OVERLOOKED"
    rank     -> 1..12 per bucket

- This lets the builder render cards directly from the top-12 file
  with no extra joins or heuristics drift.

Usage:
  python scripts/derive_top12_from_csv.py /path/to/polymarket_enriched_fast_*.csv
"""

import sys, csv
from pathlib import Path
from datetime import datetime, timezone

def utc_now():
    return datetime.now(timezone.utc)

def read_rows(p: Path):
    with p.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return r.fieldnames, [dict(row) for row in r]

def fnum(row, key, default=0.0):
    try:
        v = row.get(key)
        return float(v) if v not in (None, "", "—") else default
    except Exception:
        return default

def ttr_days(row):
    # prefer explicit numeric if present
    td = row.get("timeToResolveDays")
    try:
        if td not in (None, "", "—"):
            return float(td)
    except Exception:
        pass
    # fallback: treat missing as large
    return 9e9

def vol24(row):
    v = row.get("volume24h") or row.get("vol24h") or row.get("volume")
    try:
        return float(v) if v not in (None, "", "—") else 0.0
    except Exception:
        return 0.0

MAX_PER_EVENT = 3  # max markets from a single event in any one bucket

def event_key(row):
    """Grouping key for per-event cap. Uses eventId if present, else category."""
    eid = (row.get("eventId") or "").strip()
    if eid:
        return eid
    # fallback for older CSVs without eventId: use category
    return row.get("category") or "unknown"

def pick_capped(scored_tuples, n, already_ids=None):
    """
    Select up to n rows from scored_tuples (pre-sorted, each tuple ends with the row dict),
    skipping rows whose event_key already hit MAX_PER_EVENT, and optionally skipping
    rows whose market-id is in already_ids.
    Returns list of row dicts.
    """
    already_ids = already_ids or set()
    event_counts = {}
    result = []
    for tup in scored_tuples:
        if len(result) >= n:
            break
        r = tup[-1]
        rid = str(r.get("id") or r.get("slug") or r.get("url") or r.get("question") or id(r))
        if rid in already_ids:
            continue
        ek = event_key(r)
        if event_counts.get(ek, 0) >= MAX_PER_EVENT:
            continue
        event_counts[ek] = event_counts.get(ek, 0) + 1
        result.append(r)
    return result

def main():
    if len(sys.argv) < 2:
        print("ERROR: need enriched CSV path")
        return 1
    src = Path(sys.argv[1])
    if not src.exists():
        print(f"ERROR: enriched CSV not found: {src}")
        return 1

    headers, rows = read_rows(src)
    if not rows:
        print("ERROR: CSV empty")
        return 1

    # HOT = sort by volume24h desc, then TTR asc.
    # Exclude markets with TTR > 365 days (far-future markets shouldn't appear in HOT).
    # If fewer than 12 near-term markets exist, backfill from the far-future pool.
    # Cap at MAX_PER_EVENT markets from any single event.
    MAX_TTR_HOT = 365.0
    hot_scored = [(vol24(r), ttr_days(r), r) for r in rows]
    hot_scored.sort(key=lambda x: (-x[0], x[1]))
    hot_near = [t for t in hot_scored if t[1] is None or t[1] <= MAX_TTR_HOT]
    hot_far  = [t for t in hot_scored if t[1] is not None and t[1] > MAX_TTR_HOT]
    hot_rows = pick_capped(hot_near + hot_far, 12)

    # OVERLOOKED pool = exclude HOT, prefer near-50 / negative underround, earlier TTR
    # Also cap at MAX_PER_EVENT per event.
    hot_ids = set(str(r.get("id") or r.get("slug") or r.get("url") or r.get("question") or id(r)) for r in hot_rows)
    pool = []
    for r in rows:
        rid = str(r.get("id") or r.get("slug") or r.get("url") or r.get("question") or id(r))
        if rid in hot_ids:
            continue
        near50 = fnum(r, "near50Flag", 0.0)
        under = fnum(r, "underround", 0.0)
        pool.append((near50, under, ttr_days(r), r))
    pool.sort(key=lambda x: (-x[0], x[1], x[2]))
    gems_rows = pick_capped(pool, 12, already_ids=hot_ids)

    # write output with bucket/rank
    out_headers = list(headers)
    for extra in ("bucket", "rank"):
        if extra not in out_headers:
            out_headers.append(extra)

    stamp = utc_now().strftime("%Y%m%d_%H%M%S")
    out_name = f"polymarket_top12_{stamp}.csv"
    out_path = Path(out_name)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_headers)
        w.writeheader()
        for i, r in enumerate(hot_rows, 1):
            rr = dict(r)
            rr["bucket"] = "HOT"
            rr["rank"] = str(i)
            w.writerow(rr)
        for i, r in enumerate(gems_rows, 1):
            rr = dict(r)
            rr["bucket"] = "OVERLOOKED"
            rr["rank"] = str(i)
            w.writerow(rr)

    print(f"[ok] Wrote {out_name} (Top-12 HOT + Top-12 OVERLOOKED)")
    return 0

if __name__ == "__main__":
    sys.exit(main())
