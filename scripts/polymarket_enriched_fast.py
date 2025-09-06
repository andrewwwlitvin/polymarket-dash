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
from datetime import datetime, timezone, timedelta
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
DATA_TRADES = "https://data-api.polymarket.com/trades"

# Quotes: try ID-based first (more likely to match), then slug-based fallbacks
PLANB_ID_FIRST = [
    "https://clob.polymarket.com/markets/{id}/summary",
    "https://clob.polymarket.com/markets/{id}/orderbook",
    "https://clob.polymarket.com/markets/{id}",
    "https://clob.polymarket.com/market?market_id={id}",
    "https://gamma-api.polymarket.com/markets/{id}",
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
        params = {"closed":"false","limit":PAGE_SIZE,"offset":offset,"order":"volumeNum","ascending":"false"}
        batch = http_get_json(GAMMA_MARKETS, params=params)
        if not batch: break
        out.extend(batch)
        offset += PAGE_SIZE
        time.sleep(SLEEP)
    print(f"  Got {len(out)} markets")
    return out

def fetch_quotes_resilient(market_id: str, slug: str | None = None):
    """
    Try several endpoints by market_id first; if that fails, try by slug.
    Returns (quotes, source_url_or_none).
    """
    for tpl in PLANB_ID_FIRST:
        url = tpl.format(id=market_id)
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

def fetch_trades_24h(market_id):
    """
    Pull up to ~5000 trades in the last 24h (paged), best effort.
    Returns dict with: volume24h, trades24h, uniqueTraders24h, momentumDelta24h, momentumPct24h,
    and min/max trade price (for proxy spread fallback).
    """
    start_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    total_trades = 0
    total_volume = 0.0
    first_price = None
    last_price = None
    min_price = None
    max_price = None
    traders = set()

    limit = 1000
    page = 0
    while page < 5:
        params = {"market_id": market_id, "start": start_iso, "limit": str(limit)}
        try:
            arr = http_get_json(DATA_TRADES, params=params)
        except Exception:
            break
        if not isinstance(arr, list) or not arr:
            break

        for t in arr:
            sz = _f(t.get("size") or t.get("amount") or t.get("qty"))
            px = _f(t.get("price") or t.get("fill_price") or t.get("avg_price"))
            who = t.get("trader") or t.get("taker") or t.get("maker")
            if isinstance(who, str) and who:
                traders.add(who)
            if isinstance(sz, float):
                total_volume += sz
            if isinstance(px, float):
                if first_price is None:
                    first_price = px
                last_price = px
                min_price = px if min_price is None else min(min_price, px)
                max_price = px if max_price is None else max(max_price, px)
            total_trades += 1

        if len(arr) < limit:
            break
        page += 1
        time.sleep(0.03)

    delta = None
    pct = None
    if first_price is not None and last_price is not None:
        delta = last_price - first_price
        pct = (delta / first_price * 100.0) if first_price else None

    return {
        "volume24h": round(total_volume, 6) if total_volume else 0.0,
        "trades24h": total_trades,
        "uniqueTraders24h": len(traders),
        "momentumDelta24h": round(delta, 6) if delta is not None else None,
        "momentumPct24h": round(pct, 4) if pct is not None else None,
        "minTradePrice24h": min_price,
        "maxTradePrice24h": max_price,
    }

# --- Pre-score (no book) to pick TOP-K ---
def prelim_score(m):
    vol = m.get("volumeNum") if m.get("volumeNum") is not None else m.get("volume")
    try: vol = float(vol) if vol is not None else 0.0
    except Exception: vol = 0.0

    end_dt = parse_dt(m.get("endDate") or m.get("endDateIso"))
    ttr_days = days_to_resolve(end_dt)
    ttr_factor = 0.0
    if ttr_days is not None:
        if ttr_days <= 0: ttr_factor = 0.2
        else: ttr_factor = max(0.0, 1.0/(1.0 + ttr_days/30.0))
    return math.log1p(max(vol,0.0)) * 1.0 + ttr_factor * 2.0

# --- FAST enrichment path (quotes + 24h trades) ---
def fast_enrich(top_markets, concurrency, use_proxy_spread=True):
    print(f"[2/4] Fetching quotes + 24h trades for top {len(top_markets)} markets (concurrent)…")
    results = {}
    lock = threading.Lock()

    def task(m):
        mid = str(m.get("id") or m.get("_id") or m.get("slug") or "")
        s = m.get("slug") or ""
        # pass 1
        quotes, quote_src = fetch_quotes_resilient(mid, s) if mid else ([], None)
        # pass 2 (thin books can blink)
        if not quotes:
            time.sleep(0.6)
            quotes, quote_src = fetch_quotes_resilient(mid, s) if mid else ([], None)
        stats24 = fetch_trades_24h(mid) if mid else {}
        with lock:
            results[mid] = {"quotes": quotes, "stats24": stats24, "quoteSource": quote_src}

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(task, m) for m in top_markets]
        for _ in as_completed(futures):
            pass

    print("[3/4] Computing features…")
    enriched = []
    for m in top_markets:
        mid = str(m.get("id") or m.get("_id") or m.get("slug") or "")
        slug = m.get("slug") or ""
        url = f"https://polymarket.com/event/{slug}" if slug else ""
        embedSrc = f"https://embed.polymarket.com/market.html?market={slug}&features=volume&theme=light" if slug else ""

        question = m.get("question") or m.get("title") or m.get("name") or ""
        category = m.get("category") or ""
        vol_lifetime = _f(m.get("volumeNum") if m.get("volumeNum") is not None else m.get("volume"))
        end_dt = parse_dt(m.get("endDate") or m.get("endDateIso"))
        bundle = results.get(mid) or {}
        quotes = bundle.get("quotes") or []
        stats24 = bundle.get("stats24") or {}

        avg_spread = compute_spread_avg(quotes)
        under = compute_underround(quotes)

        # Optional proxy spread from trades high/low
        proxy_spread = None
        mn = stats24.get("minTradePrice24h")
        mx = stats24.get("maxTradePrice24h")
        if avg_spread is None and use_proxy_spread and isinstance(mn, (int,float)) and isinstance(mx, (int,float)) and mx >= mn:
            proxy_spread = mx - mn
            avg_spread = proxy_spread  # fill as fallback

        binary_mid_yes = None
        if quotes and is_binary(quotes):
            yes = None
            for q in quotes:
                nm = (q.get("name") or "").strip().lower()
                if nm in ("yes","y","true"): yes = q; break
            tgt = yes or (quotes[0] if quotes else None)
            if tgt and tgt.get("bestBid") is not None and tgt.get("bestAsk") is not None:
                binary_mid_yes = midpoint(tgt["bestBid"], tgt["bestAsk"])

        # One-liner reason
        ttr_days = round(days_to_resolve(end_dt), 1) if end_dt else None
        why_bits = []
        v24 = stats24.get("volume24h")
        if isinstance(v24, (int, float)) and v24 > 0:
            why_bits.append(f"24h ${int(v24):,}")
        if avg_spread is not None:
            why_bits.append(f"spread {avg_spread:.3f}")
        if ttr_days is not None:
            why_bits.append(f"TTR {ttr_days}d")
        if binary_mid_yes is not None and 0.40 <= binary_mid_yes <= 0.60:
            why_bits.append("~50% mid")
        why = " • ".join(why_bits) if why_bits else ""

        enriched.append({
            "id": mid,
            "slug": slug,
            "url": url,
            "embedSrc": embedSrc,
            "question": question,
            "category": category,
            "why": why,

            "volume": vol_lifetime,
            "volume24h": stats24.get("volume24h"),
            "trades24h": stats24.get("trades24h"),
            "uniqueTraders24h": stats24.get("uniqueTraders24h"),
            "momentumDelta24h": stats24.get("momentumDelta24h"),
            "momentumPct24h": stats24.get("momentumPct24h"),

            "endDateISO": end_dt.isoformat() if end_dt else "",
            "timeToResolveDays": round(days_to_resolve(end_dt),3) if end_dt else None,
            "outcomeCount": len(quotes),
            "avgSpread": round(avg_spread,6) if avg_spread is not None else None,
            "underround": round(under,6) if under is not None else None,
            "binaryMidYes": round(binary_mid_yes,6) if binary_mid_yes is not None else None,
            "near50Flag": 1 if (binary_mid_yes is not None and 0.40 <= binary_mid_yes <= 0.60) else 0,
            "bestQuotesJSON": json.dumps(quotes, ensure_ascii=False) if quotes else "",
            "avgSpreadProxy": round(proxy_spread,6) if proxy_spread is not None else None,
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
    fields = ["id","slug","url","embedSrc","question","category","why",
              "volume","volume24h","trades24h","uniqueTraders24h",
              "momentumDelta24h","momentumPct24h",
              "endDateISO","timeToResolveDays","outcomeCount","avgSpread","underround",
              "binaryMidYes","near50Flag","bestQuotesJSON","avgSpreadProxy"]
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
    top_fields = ["list","rank","id","slug","url","embedSrc","question","category",
                  "why","volume24h","volume","avgSpread","underround",
                  "near50Flag","timeToResolveDays","outcomeCount","momentumPct24h","endDateISO",
                  "bestQuotesJSON","avgSpreadProxy"]
    with open(OUT_CSV_TOPS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=top_fields)
        w.writeheader()
        for i, r in enumerate(hot, start=1):
            w.writerow({
                "list":"HOT","rank":i,"id":r["id"],"slug":r.get("slug"),"url":r.get("url"),"embedSrc":r.get("embedSrc"),
                "question":r["question"],"category":r["category"],"why":r.get("why"),
                "volume24h":r.get("volume24h"),"volume":r.get("volume"),
                "avgSpread":r.get("avgSpread"),"underround":r.get("underround"),
                "near50Flag":r.get("near50Flag"),"timeToResolveDays":r.get("timeToResolveDays"),
                "outcomeCount":r.get("outcomeCount"),"momentumPct24h":r.get("momentumPct24h"),
                "endDateISO":r.get("endDateISO"), "bestQuotesJSON": r.get("bestQuotesJSON"),
                "avgSpreadProxy": r.get("avgSpreadProxy"),
            })
        for i, r in enumerate(gems, start=1):
            w.writerow({
                "list":"HIDDEN_GEMS","rank":i,"id":r["id"],"slug":r.get("slug"),"url":r.get("url"),"embedSrc":r.get("embedSrc"),
                "question":r["question"],"category":r["category"],"why":r.get("why"),
                "volume24h":r.get("volume24h"),"volume":r.get("volume"),
                "avgSpread":r.get("avgSpread"),"underround":r.get("underround"),
                "near50Flag":r.get("near50Flag"),"timeToResolveDays":r.get("timeToResolveDays"),
                "outcomeCount":r.get("outcomeCount"),"momentumPct24h":r.get("momentumPct24h"),
                "endDateISO":r.get("endDateISO"), "bestQuotesJSON": r.get("bestQuotesJSON"),
                "avgSpreadProxy": r.get("avgSpreadProxy"),
            })
    print(f"[ok] Wrote {OUT_CSV_TOPS} (HOT + HIDDEN_GEMS)")

if __name__ == "__main__":
    main()
