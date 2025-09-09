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

    # HOT = sort by volume24h desc, then TTR asc
    hot_scored = [(vol24(r), ttr_days(r), r) for r in rows]
    hot_scored.sort(key=lambda x: (-x[0], x[1]))
    hot_rows = [r for _, __, r in hot_scored[:12]]

    # OVERLOOKED pool = exclude HOT, prefer near-50 / negative underround, earlier TTR
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
    gems_rows = [r for _, __, ___, r in pool[:12]]

    # backfill if fewer than 12
    if len(gems_rows) < 12:
        for _, __, ___, r in pool[12:]:
            rid = str(r.get("id") or r.get("slug") or r.get("url") or r.get("question") or id(r))
            if rid in hot_ids or r in gems_rows:
                continue
            gems_rows.append(r)
            if len(gems_rows) >= 12:
                break

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
