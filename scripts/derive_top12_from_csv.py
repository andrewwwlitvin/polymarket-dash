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

import sys, csv, math
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

MAX_PER_EVENT = 3        # max markets from a single event in any one bucket
MIN_VOL_OVERLOOKED = 5_000   # minimum $5k/24h — illiquid markets excluded from OVERLOOKED
MAX_TTR_HOT = 365.0          # hard ceiling for HOT — far-future markets excluded entirely

def hot_score(row):
    """
    Composite score for HOT section.
    Volume is the primary signal, but penalises near-certain markets and
    rewards near-term resolution and active price movement.

    Hard filters (return -1e9 to exclude):
      - TTR <= 0 or TTR > MAX_TTR_HOT  → resolved or far-future
      - volume24h < 1 000              → noise

    Multipliers (applied to log10(volume)):
      uncertainty (0.4–1.0) — peaks at 50% mid; near-certain markets
                               get 0.4× (still can rank if volume is huge)
      timing      (0.8–1.5) — today=1.5, this week=1.2, month=1.1,
                               quarter=1.0, up to 365d=0.8
      momentum    (+0–30%)  — absolute 24h price move; rewards live markets
    """
    v24 = vol24(row)
    if v24 < 1_000:
        return -1e9

    ttr = ttr_days(row)
    if ttr <= 0 or ttr > MAX_TTR_HOT:
        return -1e9

    # Base: log-scale volume
    vol_s = math.log10(max(v24, 1))

    # Uncertainty multiplier: smooth 0.4 at extremes → 1.0 at 50%
    mid = fnum(row, "binaryMidYes", -1.0)
    if mid < 0:
        unc = 0.75  # multi-outcome: neutral
    else:
        unc = 0.4 + 0.6 * (1.0 - abs(mid - 0.5) * 2.0)

    # Timing multiplier: near-term resolution is more actionable
    if ttr <= 1:
        timing = 1.5   # resolves today
    elif ttr <= 7:
        timing = 1.2   # this week
    elif ttr <= 30:
        timing = 1.1   # this month
    elif ttr <= 90:
        timing = 1.0   # this quarter
    else:
        timing = 0.8   # up to 365d

    # Momentum bonus: big price move = something is happening
    mom_pct = fnum(row, "momentumPct24h", 0.0)
    momentum = min(0.30, abs(mom_pct) / 100.0)

    return vol_s * unc * timing * (1.0 + momentum)

def overlooked_score(row):
    """
    Composite score for OVERLOOKED section.
    Surfaces opportunistic markets: real volume, genuine uncertainty,
    near-term resolution, and bettor value.

    Hard filters (return -1e9 to exclude):
      - volume24h < MIN_VOL_OVERLOOKED  → not tradeable
      - binaryMidYes outside 10-90%    → near-certain, not a real bet

    Weighted components (each 0..1):
      vol_score   (35%) — log-scale liquidity, rewards activity
      uncertainty (30%) — price near 50% = genuine two-sided bet
      timing      (15%) — resolves sooner is more actionable
      value       (15%) — negative underround = bettor has edge
      momentum    ( 5%) — recent price movement = market is live
    """
    v24 = vol24(row)
    if v24 < MIN_VOL_OVERLOOKED:
        return -1e9

    # Volume: log10 scale, normalized $5k..$5M → 0..1
    vol_score = min(1.0, math.log10(max(v24, 1)) / math.log10(5_000_000))

    # Uncertainty: continuous, peaks at binaryMidYes=0.5, 0 at extremes
    mid = fnum(row, "binaryMidYes", -1.0)
    if mid < 0:
        # multi-outcome market: use near50Flag as a proxy
        uncertainty = fnum(row, "near50Flag", 0.0) * 0.6
    else:
        if mid < 0.10 or mid > 0.90:
            return -1e9  # near-certain outcome — not an interesting bet
        uncertainty = 1.0 - abs(mid - 0.5) * 2.0  # 1.0 at 0.5, 0.0 at 0 or 1

    # Timing: < 90d = full score; decays to 0.1 beyond 365d
    ttr = ttr_days(row)
    if ttr <= 0:
        timing = 0.0
    elif ttr <= 90:
        timing = 1.0
    elif ttr <= 365:
        timing = 0.4 + 0.6 * (1.0 - (ttr - 90) / 275.0)
    else:
        timing = 0.1

    # Value: negative underround → bettor has edge; cap at 1.0
    under = fnum(row, "underround", 0.0)
    value_score = min(1.0, max(0.0, -under * 15))  # -0.067 underround → 1.0

    # Momentum: recent price movement signals active, live market
    mom_pct = fnum(row, "momentumPct24h", 0.0)
    momentum = min(1.0, abs(mom_pct) / 30.0)  # 30% move → 1.0

    return (
        0.35 * vol_score
      + 0.30 * uncertainty
      + 0.15 * timing
      + 0.15 * value_score
      + 0.05 * momentum
    )

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

    # HOT = high volume × uncertainty × timing × momentum.
    # Far-future markets (TTR > MAX_TTR_HOT) are hard-excluded by hot_score().
    # Cap at MAX_PER_EVENT markets from any single event.
    hot_scored = []
    for r in rows:
        sc = hot_score(r)
        if sc > -1e8:
            hot_scored.append((sc, ttr_days(r), r))
    hot_scored.sort(key=lambda x: (-x[0], x[1]))
    hot_rows = pick_capped(hot_scored, 12)

    # OVERLOOKED: opportunistic markets — real volume, genuine uncertainty,
    # near-term resolution, bettor value. Scored by overlooked_score().
    # Cap at MAX_PER_EVENT per event. Backfill with lower-scored markets if < 12.
    hot_ids = set(str(r.get("id") or r.get("slug") or r.get("url") or r.get("question") or id(r)) for r in hot_rows)
    pool, pool_backfill = [], []
    for r in rows:
        rid = str(r.get("id") or r.get("slug") or r.get("url") or r.get("question") or id(r))
        if rid in hot_ids:
            continue
        sc = overlooked_score(r)
        if sc > -1e8:
            pool.append((sc, ttr_days(r), r))
        else:
            # Only backfill if filtered for low volume, NOT for near-certain price.
            # Near-certain markets (mid < 10% or > 90%) are excluded entirely.
            mid_val = fnum(r, "binaryMidYes", -1.0)
            if mid_val < 0 or (0.10 <= mid_val <= 0.90):
                pool_backfill.append((fnum(r, "volume24h", 0.0), ttr_days(r), r))
    pool.sort(key=lambda x: (-x[0], x[1]))
    pool_backfill.sort(key=lambda x: (-x[0], x[1]))
    gems_rows = pick_capped(pool + pool_backfill, 12, already_ids=hot_ids)

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
