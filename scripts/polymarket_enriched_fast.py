#!/usr/bin/env python3
"""
FAST Polymarket enrichment with resilient quotes + 24h trades (TOP-K only),
and Top 12 (HOT + HIDDEN_GEMS) outputs.

What it does
------------
1) Fetch ALL open markets from Gamma (cheap).
2) PRE-RANK cheaply (lifetime volume + time proximity).
3) Select TOP-K (default 120).
4) For TOP-K only (concurrent):
     - Fetch quotes via resilient endpoints (id-first + slug fallback).
       * Expanded parser handles orderBook/book, nested outcome quote/book, flat fields.
       * Two-pass retry if first attempt returns no quotes.
     - Fetch 24h trades (volume, trades, unique traders, momentum).
5) Compute features; write:
     - Full CSV  : polymarket_enriched_fast_<timestamp>.csv
     - Top CSV   : polymarket_top12_<timestamp>.csv  (HOT + HIDDEN_GEMS)
   Prints “Top 12 HOT” and “Top 12 Hidden Gems”.

No third-party dependencies (urllib, csv, json, etc).
"""

import csv, json, time, urllib.parse, urllib.request, argparse, math, threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Settings (can be overridden via CLI) ---
USE_INSECURE_SSL = True  # set False if your system CA bundle is ok
TIMEOUT = 18
RETRIES = 3
SLEEP = 0.08
PAGE_SIZE = 200

TOPK_DEFAULT = 120
CONCURRENCY_DEFAULT = 8

def timestamp_tag():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

TS = timestamp_tag()
OUT_CSV_FULL = f"polymarket_enriched_fast_{TS}.csv"
OUT_CSV_TOPS = f"polymarket_top12_{TS}.csv"

# --- Endpoints ---
GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"

# Quotes: try conditionId (0x hash) first — that's what CLOB API requires.
# {cid} = conditionId (0x hex), {id} = numeric Gamma id, {slug} = slug
PLANB_CID_FIRST = [
    "https://clob.polymarket.com/markets/{cid}",
    "https://clob.polymarket.com/markets/{cid}/summary",
    "https://clob.polymarket.com/markets/{cid}/orderbook",
]

PLANB_SLUG_FALLBACK = [
    "https://gamma-api.polymarket.com/markets?slug={slug}",
    "https://clob.polymarket.com/markets?slug={slug}",
]

if USE_INSECURE_SSL:
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context

# --- Helpers ---
def _f(x):
    try:
        return float(x) if x is not None and x != "" else None
    except Exception:
        return None

def parse_dt(dt):
    if not dt: return None
    if isinstance(dt, str):
        try:
            if dt.endswith("Z"): dt = dt.replace("Z","+00:00")
            return datetime.fromisoformat(dt)
        except Exception:
            return None
    if isinstance(dt, (int,float)):
        if dt > 1_000_000_000_000: dt/=1000.0
        return datetime.fromtimestamp(dt, tz=timezone.utc)
    return None

def days_to_resolve(end_dt):
    if not end_dt: return None
    now = datetime.now(timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    return (end_dt - now).total_seconds()/86400.0

def http_get_json(url, params=None, retries=RETRIES, timeout=TIMEOUT):
    last_err = None
    full = url + ("?" + urllib.parse.urlencode(params) if params else "")
    for attempt in range(1, retries+1):
        try:
            req = urllib.request.Request(full, headers={
                "User-Agent":"python-urllib",
                "Accept":"application/json",
                "Connection":"close",
            })
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            last_err = e
            time.sleep(min(0.25*attempt, 1.0))
    raise last_err

# --- Expanded parser: supports orderBook/book, nested quote/book in outcomes, flat best fields, arrays ---
def outcome_quotes_from_obj(obj):
    """
    Normalize various market payload shapes into:
      [{'name': <str>, 'bestBid': <float|None>, 'bestAsk': <float|None>}, ...]
    """
    out = []
    if not isinstance(obj, dict):
        return out

    core = obj.get("market") if isinstance(obj.get("market"), dict) else obj

    def top_from_book(book):
        bb = None; ba = None
        try:
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            # first level is top-of-book
            if bids:
                b0 = bids[0]
                bb = _f(b0.get("price") or b0.get("p") or (b0 if isinstance(b0,(int,float)) else None))
            if asks:
                a0 = asks[0]
                ba = _f(a0.get("price") or a0.get("p") or (a0 if isinstance(a0,(int,float)) else None))
        except Exception:
            pass
        return bb, ba

    # 1) Direct top-of-book under orderBook/orderbook/book
    for k in ("orderBook", "orderbook", "book"):
        if isinstance(core.get(k), dict):
            bb, ba = top_from_book(core[k])
            if bb is not None or ba is not None:
                out.append({"name": "Top", "bestBid": bb, "bestAsk": ba})

    # 2) Known flat best fields
    flat_bid = (core.get("bestBid") or core.get("best_bid") or
                core.get("bestBidPrice"))
    flat_ask = (core.get("bestAsk") or core.get("best_ask") or
                core.get("bestAskPrice"))
    if flat_bid is not None or flat_ask is not None:
        out.append({"name": "Top", "bestBid": _f(flat_bid), "bestAsk": _f(flat_ask)})

    # 3) Outcomes list with nested book/quote or direct fields
    outcomes = core.get("outcomes") or core.get("contracts") or core.get("options") or core.get("choices")
    if isinstance(outcomes, list) and outcomes:
        for i, o in enumerate(outcomes):
            if isinstance(o, dict):
                name = o.get("name") or o.get("shortName") or o.get("outcome") or o.get("symbol") or f"Outcome {i}"
                bb = _f(o.get("bestBid") or o.get("best_bid") or o.get("bestBidPrice"))
                ba = _f(o.get("bestAsk") or o.get("best_ask") or o.get("bestAskPrice"))

                # nested quote object
                if (bb is None or ba is None) and isinstance(o.get("quote"), dict):
                    if bb is None: bb = _f(o["quote"].get("bid"))
                    if ba is None: ba = _f(o["quote"].get("ask"))

                # nested per-outcome book
                if (bb is None or ba is None) and isinstance(o.get("book"), dict):
                    tbb, tba = top_from_book(o["book"])
                    if bb is None: bb = tbb
                    if ba is None: ba = tba

                out.append({"name": name, "bestBid": bb, "bestAsk": ba})
            else:
                # string labels, try core-level bid/ask maps
                bids = core.get("bids") or core.get("bestBids") or {}
                asks = core.get("asks") or core.get("bestAsks") or {}
                name = str(o)
                out.append({"name": name, "bestBid": _f(bids.get(name)), "bestAsk": _f(asks.get(name))})

    # 4) Array fields by index
    bid_arr = core.get("bestBids") or []
    ask_arr = core.get("bestAsks") or []
    if bid_arr or ask_arr:
        n = max(len(bid_arr), len(ask_arr))
        for i in range(n):
            out.append({
                "name": f"Outcome {i}",
                "bestBid": _f(bid_arr[i] if i < len(bid_arr) else None),
                "bestAsk": _f(ask_arr[i] if i < len(ask_arr) else None),
            })

    # Remove empties
    cleaned = []
    for q in out:
        if q.get("bestBid") is None and q.get("bestAsk") is None:
            continue
        cleaned.append(q)
    return cleaned

def compute_spread_avg(quotes):
    spreads = []
    for q in quotes:
        bb, ba = q.get("bestBid"), q.get("bestAsk")
        if isinstance(bb, (int,float)) and isinstance(ba, (int,float)) and ba >= bb:
            spreads.append(ba - bb)
    return sum(spreads)/len(spreads) if spreads else None

def compute_underround(quotes):
    asks = [q.get("bestAsk") for q in quotes if isinstance(q.get("bestAsk"), (int,float))]
    if not asks: return None
    return sum(asks) - 1.0

def midpoint(bid,ask):
    if bid is None or ask is None: return None
    return (bid+ask)/2.0

def is_binary(quotes):
    usable = [q for q in quotes if (q.get("bestBid") is not None or q.get("bestAsk") is not None)]
    return len(usable) <= 2 and len(quotes) <= 3

# --- Fetchers ---
def fetch_gamma_open_markets():
    print("[1/4] Fetching open markets from Gamma…")
    out, offset = [], 0
    while True:
        params = {"closed":"false","active":"true","limit":PAGE_SIZE,"offset":offset,"order":"volumeNum","ascending":"false"}
        batch = http_get_json(GAMMA_MARKETS, params=params)
        if not batch: break
        out.extend(batch)
        offset += PAGE_SIZE
        time.sleep(SLEEP)
    print(f"  Got {len(out)} markets")
    return out

def gamma_quotes_from_market(m):
    """
    Extract YES/NO quotes from Gamma market outcomePrices/outcomes.
    Gamma returns midpoint prices; we approximate a small spread around them.
    Returns list of quote dicts, or [] if data is absent/malformed.
    """
    outcomes_raw = m.get("outcomes")
    prices_raw = m.get("outcomePrices")
    if not outcomes_raw or not prices_raw:
        return []
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    except Exception:
        return []
    if not isinstance(outcomes, list) or not isinstance(prices, list):
        return []

    # Try to get spread from Gamma's spreadClob / spread field
    spread_raw = m.get("spreadClob") or m.get("spread")
    half = _f(spread_raw)
    half = (half / 2.0) if (isinstance(half, float) and half > 0) else 0.005

    quotes = []
    for name, price in zip(outcomes, prices):
        p = _f(price)
        if p is None:
            continue
        bid = max(0.0, round(p - half, 6))
        ask = min(1.0, round(p + half, 6))
        quotes.append({"name": str(name), "bestBid": bid, "bestAsk": ask})
    return quotes


def fetch_quotes_resilient(condition_id: str, slug: str | None = None):
    """
    Try CLOB endpoints using conditionId (0x hash) first, then slug fallbacks.
    Returns (quotes, source_url_or_none).
    """
    if condition_id:
        for tpl in PLANB_CID_FIRST:
            url = tpl.format(cid=condition_id)
            try:
                data = http_get_json(url)
                q = outcome_quotes_from_obj(data)
                if q:
                    return q, url
            except Exception:
                time.sleep(0.05)

    if slug:
        for tpl in PLANB_SLUG_FALLBACK:
            url = tpl.format(slug=slug)
            try:
                data = http_get_json(url)
                if isinstance(data, list) and data:
                    sel = next((m for m in data if (m.get("slug") == slug or m.get("marketSlug") == slug)), data[0])
                    q = outcome_quotes_from_obj(sel)
                else:
                    q = outcome_quotes_from_obj(data)
                if q:
                    return q, url
            except Exception:
                time.sleep(0.05)

    return [], None

def fetch_momentum_clob(condition_id: str):
    """
    Fetch 24h price momentum from CLOB prices-history endpoint (no auth required).
    Returns (momentumDelta24h, momentumPct24h) or (None, None) on failure.
    """
    if not condition_id:
        return None, None
    try:
        url = (f"https://clob.polymarket.com/prices-history"
               f"?market={urllib.parse.quote(condition_id)}&fidelity=60&interval=1d")
        data = http_get_json(url, retries=2, timeout=12)
        history = data.get("history") or []
        if len(history) >= 2:
            first_p = _f((history[0].get("p") if isinstance(history[0], dict) else None))
            last_p  = _f((history[-1].get("p") if isinstance(history[-1], dict) else None))
            if first_p and last_p and first_p > 0:
                delta = last_p - first_p
                pct   = delta / first_p * 100.0
                return round(delta, 6), round(pct, 4)
    except Exception:
        pass
    return None, None

# --- Pre-score (no book) to pick TOP-K ---
def prelim_score(m):
    vol = m.get("volumeNum") if m.get("volumeNum") is not None else m.get("volume")
    try: vol = float(vol) if vol is not None else 0.0
    except Exception: vol = 0.0

    end_dt = parse_dt(m.get("endDate") or m.get("endDateIso"))
    ttr_days = days_to_resolve(end_dt)
    # Hard-exclude markets that have already ended/resolved
    if ttr_days is not None and ttr_days <= 0:
        return 0.0
    ttr_factor = 0.0
    if ttr_days is not None:
        ttr_factor = max(0.0, 1.0/(1.0 + ttr_days/30.0))
    return math.log1p(max(vol,0.0)) * 1.0 + ttr_factor * 2.0

# --- FAST enrichment path (quotes + 24h trades) ---
def fast_enrich(top_markets, concurrency, use_proxy_spread=True):
    print(f"[2/4] Fetching quotes + 24h trades for top {len(top_markets)} markets (concurrent)…")
    results = {}
    lock = threading.Lock()

    def task(m):
        gamma_id = str(m.get("id") or m.get("_id") or "")
        condition_id = str(m.get("conditionId") or "")
        slug = m.get("slug") or ""

        # Skip already-ended markets (safety net on top of prelim_score filter)
        end_dt = parse_dt(m.get("endDate") or m.get("endDateIso"))
        if end_dt:
            ttr = days_to_resolve(end_dt)
            if ttr is not None and ttr <= 0:
                with lock:
                    results[gamma_id] = {"quotes": [], "vol24h": None,
                                         "momentumDelta": None, "momentumPct": None,
                                         "quoteSource": None}
                return

        # Quotes: CLOB conditionId → slug fallback → Gamma outcomePrices
        quotes, quote_src = fetch_quotes_resilient(condition_id, slug)
        if not quotes:
            time.sleep(0.4)
            quotes, quote_src = fetch_quotes_resilient(condition_id, slug)
        if not quotes:
            quotes = gamma_quotes_from_market(m)
            quote_src = "gamma_outcome_prices" if quotes else None

        # Volume: use Gamma's pre-computed per-market 24h volume (reliable, no extra call)
        vol24h = _f(m.get("volume24hr") or m.get("volume24hrClob") or m.get("oneDayVolume"))

        # Momentum: 24h price change from CLOB prices-history (no auth needed)
        mom_delta, mom_pct = fetch_momentum_clob(condition_id) if condition_id else (None, None)

        with lock:
            results[gamma_id] = {"quotes": quotes, "vol24h": vol24h,
                                  "momentumDelta": mom_delta, "momentumPct": mom_pct,
                                  "quoteSource": quote_src}

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(task, m) for m in top_markets]
        for _ in as_completed(futures):
            pass

    print("[3/4] Computing features…")
    enriched = []
    for m in top_markets:
        gamma_id = str(m.get("id") or m.get("_id") or "")
        condition_id = str(m.get("conditionId") or "")
        slug = m.get("slug") or ""
        url = f"https://polymarket.com/event/{slug}" if slug else ""
        embedSrc = f"https://embed.polymarket.com/market.html?market={slug}&features=volume&theme=light" if slug else ""

        question = m.get("question") or m.get("title") or m.get("name") or ""
        category = m.get("category") or ""
        vol_lifetime = _f(m.get("volumeNum") if m.get("volumeNum") is not None else m.get("volume"))
        end_dt = parse_dt(m.get("endDate") or m.get("endDateIso"))
        ttr_val = days_to_resolve(end_dt)

        # Skip ended markets in output too
        if ttr_val is not None and ttr_val <= 0:
            continue

        bundle = results.get(gamma_id) or {}
        quotes = bundle.get("quotes") or []
        vol24h = bundle.get("vol24h")          # from Gamma volume24hr (per-market, reliable)
        mom_delta = bundle.get("momentumDelta") # from CLOB prices-history
        mom_pct   = bundle.get("momentumPct")

        avg_spread = compute_spread_avg(quotes)
        under = compute_underround(quotes)

        binary_mid_yes = None
        if quotes and is_binary(quotes):
            yes = None
            for q in quotes:
                nm = (q.get("name") or "").strip().lower()
                if nm in ("yes","y","true"): yes = q; break
            tgt = yes or (quotes[0] if quotes else None)
            if tgt and tgt.get("bestBid") is not None and tgt.get("bestAsk") is not None:
                binary_mid_yes = midpoint(tgt["bestBid"], tgt["bestAsk"])

        ttr_days = round(ttr_val, 1) if ttr_val is not None else None
        why_bits = []
        if isinstance(vol24h, (int, float)) and vol24h > 0:
            why_bits.append(f"24h ${int(vol24h):,}")
        if avg_spread is not None:
            why_bits.append(f"spread {avg_spread:.3f}")
        if ttr_days is not None:
            why_bits.append(f"TTR {ttr_days}d")
        if binary_mid_yes is not None and 0.40 <= binary_mid_yes <= 0.60:
            why_bits.append("~50% mid")
        why = " • ".join(why_bits) if why_bits else ""

        enriched.append({
            "id": gamma_id,
            "conditionId": condition_id,
            "slug": slug,
            "url": url,
            "embedSrc": embedSrc,
            "question": question,
            "category": category,
            "why": why,

            "volume": vol_lifetime,
            "volume24h": round(vol24h, 2) if isinstance(vol24h, float) else vol24h,
            "momentumDelta24h": mom_delta,
            "momentumPct24h": mom_pct,

            "endDateISO": end_dt.isoformat() if end_dt else "",
            "timeToResolveDays": round(ttr_val, 3) if ttr_val is not None else None,
            "outcomeCount": len(quotes),
            "avgSpread": round(avg_spread,6) if avg_spread is not None else None,
            "underround": round(under,6) if under is not None else None,
            "binaryMidYes": round(binary_mid_yes,6) if binary_mid_yes is not None else None,
            "near50Flag": 1 if (binary_mid_yes is not None and 0.40 <= binary_mid_yes <= 0.60) else 0,
            "bestQuotesJSON": json.dumps(quotes, ensure_ascii=False) if quotes else "",
        })
    print("[4/4] Done.")
    return enriched

# --- Ranking to produce Top 12 + Top 12 ---
def rank_hot(rows):
    # Prefer 24h volume; fallback to lifetime. Reward tight spreads & time proximity.
    def key(r):
        vol24 = r.get("volume24h")
        volL  = r.get("volume") or 0.0
        vol = vol24 if isinstance(vol24,(int,float)) and vol24 else volL
        spread = r.get("avgSpread")
        ttr = r.get("timeToResolveDays")
        spread_component = (1.0/(1.0+spread*100)) if (isinstance(spread,(int,float)) and spread is not None and spread>=0) else 0.5
        ttr_component = (1.0/(1.0+(ttr or 365)/30.0))
        return (math.log1p(vol)*1.3) + (spread_component*2.0) + (ttr_component*1.2)
    return sorted(rows, key=key, reverse=True)[:12]

def rank_gems(rows):
    # Hidden gems: near 50, underround < 0, moderate 24h vol, soonish
    def key(r):
        vol24 = r.get("volume24h") or 0.0
        near50 = r.get("near50Flag") or 0
        under = r.get("underround")
        ttr = r.get("timeToResolveDays") or 365
        vol_penalty = 0.0
        if vol24>200000: vol_penalty = -1.0
        elif vol24<1500: vol_penalty = -0.4
        under_bonus = 0.0
        if isinstance(under,(int,float)):
            under_bonus = min(0.6, max(-1.0, -under*2.2))  # more negative -> more bonus
        ttr_bonus = 1.0/(1.0+ttr/45.0)
        return (near50*2.2) + under_bonus + ttr_bonus + vol_penalty

    candidates = [r for r in rows if (r.get("volume24h") or 0) >= 0]
    with_quotes = [r for r in candidates if (r.get("outcomeCount") or 0) > 0]
    pool = with_quotes if with_quotes else candidates
    pool_mod = [r for r in pool if 1500 <= (r.get("volume24h") or 0) <= 200000]
    pool = pool_mod if pool_mod else pool

    return sorted(pool, key=key, reverse=True)[:12]

# --- Main ---
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fast", action="store_true", help="Only enrich TOP-K prelim markets")
    ap.add_argument("--topk", type=int, default=TOPK_DEFAULT, help="How many markets to enrich in fast mode")
    ap.add_argument("--concurrency", type=int, default=CONCURRENCY_DEFAULT, help="Parallel workers for per-market fetches")
    ap.add_argument("--no-proxy-spread", action="store_true", help="Do not use trade high/low fallback when quotes missing")
    args = ap.parse_args()

    gamma = fetch_gamma_open_markets()
    ranked = sorted(gamma, key=prelim_score, reverse=True)
    topk = ranked[: args.topk] if args.fast else gamma
    print(f"[info] Selected {len(topk)} markets for enrichment (quotes + 24h trades).")

    enriched = fast_enrich(topk, concurrency=args.concurrency, use_proxy_spread=not args.no_proxy_spread)

    # Write full list (dashboard-friendly fields included)
    fields = ["id","conditionId","slug","url","embedSrc","question","category","why",
              "volume","volume24h","momentumDelta24h","momentumPct24h",
              "endDateISO","timeToResolveDays","outcomeCount","avgSpread","underround",
              "binaryMidYes","near50Flag","bestQuotesJSON"]
    with open(OUT_CSV_FULL, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in enriched:
            w.writerow(r)
    print(f"[ok] Wrote {OUT_CSV_FULL}")

    # Build Top 12 lists
    hot = rank_hot(enriched)
    gems = rank_gems(enriched)

    # Print Top 12
    def fmt(r):
        ttr = r.get("timeToResolveDays")
        ttr_str = ("{:.1f}d".format(ttr) if isinstance(ttr,(int,float)) else "—")
        v24 = r.get("volume24h") or 0
        return f"- {r['question'][:80]} | 24h=${v24:,.0f} | spread={r.get('avgSpread')} | ttr={ttr_str}"

    print("\nTop 12 HOT:")
    for r in hot: print(fmt(r))

    print("\nTop 12 Hidden Gems:")
    for r in gems: print(fmt(r))

    # Write combined Top12 CSV (two buckets; include bestQuotesJSON + proxy field for debugging)
    top_fields = ["list","rank","id","conditionId","slug","url","embedSrc","question","category",
                  "why","volume24h","volume","avgSpread","underround",
                  "near50Flag","timeToResolveDays","outcomeCount","momentumPct24h","endDateISO",
                  "bestQuotesJSON"]
    with open(OUT_CSV_TOPS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=top_fields)
        w.writeheader()
        for i, r in enumerate(hot, start=1):
            w.writerow({
                "list":"HOT","rank":i,"id":r["id"],"conditionId":r.get("conditionId"),
                "slug":r.get("slug"),"url":r.get("url"),"embedSrc":r.get("embedSrc"),
                "question":r["question"],"category":r["category"],"why":r.get("why"),
                "volume24h":r.get("volume24h"),"volume":r.get("volume"),
                "avgSpread":r.get("avgSpread"),"underround":r.get("underround"),
                "near50Flag":r.get("near50Flag"),"timeToResolveDays":r.get("timeToResolveDays"),
                "outcomeCount":r.get("outcomeCount"),"momentumPct24h":r.get("momentumPct24h"),
                "endDateISO":r.get("endDateISO"),"bestQuotesJSON":r.get("bestQuotesJSON"),
            })
        for i, r in enumerate(gems, start=1):
            w.writerow({
                "list":"HIDDEN_GEMS","rank":i,"id":r["id"],"conditionId":r.get("conditionId"),
                "slug":r.get("slug"),"url":r.get("url"),"embedSrc":r.get("embedSrc"),
                "question":r["question"],"category":r["category"],"why":r.get("why"),
                "volume24h":r.get("volume24h"),"volume":r.get("volume"),
                "avgSpread":r.get("avgSpread"),"underround":r.get("underround"),
                "near50Flag":r.get("near50Flag"),"timeToResolveDays":r.get("timeToResolveDays"),
                "outcomeCount":r.get("outcomeCount"),"momentumPct24h":r.get("momentumPct24h"),
                "endDateISO":r.get("endDateISO"),"bestQuotesJSON":r.get("bestQuotesJSON"),
            })
    print(f"[ok] Wrote {OUT_CSV_TOPS} (HOT + HIDDEN_GEMS)")

if __name__ == "__main__":
    main()
