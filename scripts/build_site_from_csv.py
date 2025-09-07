#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, csv, re, json, random, html as html_lib
from pathlib import Path
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Optional

# =========================================================
# Config
# =========================================================
SITE_DIR        = Path("site")
DATA_DIR        = Path("data")
META_DIR        = Path("content/meta")   # short, plain-text (<=160 chars)
LONG_DIR        = Path("content/long")   # long HTML (visible)
DESC_HISTORY    = Path("data/desc_history.json")
RECENT_WINDOW   = 30                     # do not reuse same meta/long within last N runs
SITE_BASE       = "https://urbanpoly.com"  # change if needed
TIMESTAMP_FMT   = "%Y-%m-%d_%H%M"        # dashboard_YYYY-MM-DD_HHMM.html

# =========================================================
# CSV resolution
# =========================================================
def resolve_csv_path() -> Path:
    """Use CLI arg if provided, else newest CSV in data/, else create a tiny sample."""
    if len(sys.argv) > 1:
        p = Path(sys.argv[1]).resolve()
        print(f"[builder] CLI CSV arg detected: {p}")
        if not p.exists():
            print(f"[error] CSV not found: {p}", file=sys.stderr)
            sys.exit(2)
        return p

    DATA_DIR.mkdir(exist_ok=True)
    csvs = sorted(DATA_DIR.glob("*.csv"))
    if csvs:
        chosen = csvs[-1].resolve()
        print(f"[builder] Using newest in data/: {chosen}")
        return chosen

    # Write a minimal sample so site always builds
    sample = DATA_DIR / "sample_enriched_20250101_000000.csv"
    if not sample.exists():
        with sample.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "id","slug","url","embedSrc","question","category","why",
                "volume","volume24h","trades24h","uniqueTraders24h",
                "momentumDelta24h","momentumPct24h","endDateISO","timeToResolveDays",
                "outcomeCount","avgSpread","underround","binaryMidYes","near50Flag",
                "bestQuotesJSON","avgSpreadProxy"
            ])
            w.writerow([
                "1","bitcoin-100k","https://polymarket.com/event/bitcoin-100k",
                "https://embed.polymarket.com/market.html?market=bitcoin-100k&features=volume&theme=light",
                "Will Bitcoin hit $100k by 2025?","Crypto",
                "Strong 24h activity; near resolution.","1000000","250000","1200","800",
                "0.15","15.0","2025-12-31T00:00:00Z","120",
                "2","0.020","-0.010","0.48","1",
                '[{"name":"Top","bestBid":0.48,"bestAsk":0.52}]',"0.02"
            ])
    print(f"[builder] No CSVs found. Wrote sample: {sample.resolve()}")
    return sample.resolve()

# =========================================================
# Data helpers
# =========================================================
def read_rows(csv_path: Path) -> List[Dict]:
    rows: List[Dict] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k: v for k, v in r.items()})
    return rows

def num(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default

def fmt_money(n) -> str:
    if n is None:
        return "—"
    try:
        n = float(n)
        return f"${int(round(n)):,.0f}"
    except Exception:
        return "—"

def ttr_badge(days_str: Optional[str]) -> str:
    if not days_str:
        return "TTR —"
    d = num(days_str)
    if d is None:
        return "TTR —"
    if d <= 0:
        return "TTR Ended"
    return f"TTR {d:.1f}d"

def pick_hot_and_gems(rows: List[Dict], k: int = 12):
    """HOT: highest 24h volume then nearest resolve; GEMS: near50 & underround with moderate volume."""
    enriched = []
    for r in rows:
        vol = num(r.get("volume24h")) or num(r.get("volume")) or 0.0
        ttr = num(r.get("timeToResolveDays"), 99999.0)
        under = num(r.get("underround"), 0.0)
        near50 = num(r.get("near50Flag"), 0.0)
        enriched.append((vol, ttr, -under, near50, r))
    hot_rows = [r for _, __, ___, ____, r in sorted(enriched, key=lambda x: (-x[0], x[1]))[:k]]
    pool = [e for e in enriched if 1_000 <= e[0] <= 100_000]
    gem_rows = [r for _, __, ___, ____, r in sorted(pool, key=lambda x: (-x[3], x[2], x[1]))[:k]]
    return hot_rows, gem_rows

# =========================================================
# Rollover previous index into snapshot
# =========================================================
def rollover_previous_index_to_snapshot(site_dir: Path) -> Optional[str]:
    """
    If site/index.html exists, extract hidden build marker <!-- build_ts: YYYY-MM-DD_HHMM -->
    and write it as site/dashboard_<ts>.html (only if not exists). This snapshots the PREVIOUS run.
    """
    idx = site_dir / "index.html"
    if not idx.exists():
        return None
    html = idx.read_text(encoding="utf-8", errors="ignore")
    m = re.search(r"<!--\s*build_ts:\s*([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{4})\s*-->", html)
    if not m:
        return None
    ts = m.group(1)
    snap = site_dir / f"dashboard_{ts}.html"
    if snap.exists():
        return None
    snap.write_text(html, encoding="utf-8")
    return snap.name

# =========================================================
# Description rotation (meta + long)
# =========================================================
def _list_files(dirpath: Path, exts: Tuple[str, ...]) -> List[Path]:
    if not dirpath.exists():
        return []
    out = []
    for p in dirpath.iterdir():
        if p.is_file() and p.suffix.lower() in exts:
            out.append(p)
    return sorted(out)

def _load_history() -> dict:
    if DESC_HISTORY.exists():
        try:
            return json.loads(DESC_HISTORY.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"meta_recent": [], "long_recent": []}

def _save_history(h: dict):
    DESC_HISTORY.parent.mkdir(parents=True, exist_ok=True)
    DESC_HISTORY.write_text(json.dumps(h, ensure_ascii=False, indent=2), encoding="utf-8")

def _pick_without_recent(paths: List[Path], recent: List[str], keep_last: int) -> Optional[Path]:
    if not paths:
        return None
    pool = [p for p in paths if p.name not in recent]
    if not pool:
        pool = paths[:]  # allow reuse if exhausted
    return random.choice(pool)

def _strip_html(s: str) -> str:
    return re.sub(r"<[^>]*>", "", s or "").strip()

def choose_descriptions() -> tuple[str, str, str, str]:
    """
    Returns: (meta_text, long_html, meta_filename, long_filename)
    - meta_text: <= 160 chars (used in <meta name="description">)
    - long_html: injected visibly into Description section (HTML allowed)
    """
    META_DIR.mkdir(parents=True, exist_ok=True)
    LONG_DIR.mkdir(parents=True, exist_ok=True)

    meta_files = _list_files(META_DIR, (".txt",))
    long_files = _list_files(LONG_DIR, (".html", ".htm", ".txt"))

    hist = _load_history()

    # meta
    meta_pick = _pick_without_recent(meta_files, hist.get("meta_recent", []), RECENT_WINDOW)
    if meta_pick:
        raw = meta_pick.read_text(encoding="utf-8")
        meta_text = _strip_html(" ".join(raw.split()))[:160]
        meta_name = meta_pick.name
    else:
        meta_text = "Live Polymarket dashboards—hottest markets & overlooked chances. Updated every 6 hours."
        meta_name = "(default)"

    # long
    long_pick = _pick_without_recent(long_files, hist.get("long_recent", []), RECENT_WINDOW)
    if long_pick:
        long_html = long_pick.read_text(encoding="utf-8")
        long_name = long_pick.name
    else:
        long_html = (
            "<p>This dashboard highlights activity, spreads, and time-to-resolve. "
            "Updated every six hours. Not financial advice; do your own research.</p>"
        )
        long_name = "(default)"

    # update history (prepend newest, keep N)
    def upd(key: str, name: str):
        arr = list(hist.get(key, []))
        if name != "(default)":
            arr = [name] + [n for n in arr if n != name]
            hist[key] = arr[:RECENT_WINDOW]
    upd("meta_recent", meta_name)
    upd("long_recent", long_name)
    _save_history(hist)

    return meta_text, long_html, meta_name, long_name

# =========================================================
# SEO head & assets
# =========================================================
def html_head(title: str, page_url: str, desc: str, iso_now: str, build_ts: str) -> str:
    return f"""<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{html_lib.escape(title)}</title>
<!-- build_ts: {build_ts} -->
<meta name="description" content="{html_lib.escape(desc)}" />
<meta name="keywords" content="polymarket, election odds, prediction markets, betting markets, hidden gems, dashboard" />
<link rel="canonical" href="{html_lib.escape(page_url)}" />

<!-- Open Graph -->
<meta property="og:title" content="{html_lib.escape(title)}" />
<meta property="og:description" content="{html_lib.escape(desc)}" />
<meta property="og:type" content="website" />
<meta property="og:url" content="{html_lib.escape(page_url)}" />
<meta property="og:image" content="{SITE_BASE}/og-preview.png" />
<meta property="og:updated_time" content="{iso_now}" />

<!-- Twitter -->
<meta name="twitter:card" content="summary_large_image" />
<meta name="twitter:title" content="{html_lib.escape(title)}" />
<meta name="twitter:description" content="{html_lib.escape(desc)}" />
<meta name="twitter:image" content="{SITE_BASE}/og-preview.png" />

<style>
:root {{ --bg:#0b0b10; --fg:#eaeaf0; --muted:#a3a8b4; --card:#141420; --border:#232336; --accent:#6ea8fe; --accent-2:#a879fe; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background: var(--bg); color: var(--fg); }}
.container {{ max-width: 1240px; margin: 0 auto; padding: 32px 24px 44px; }}
.header {{ margin-bottom: 18px; }}
.header h1 {{ margin:0; font-size: 34px; font-variant: small-caps; letter-spacing: .5px; }}
.header .date {{ color: var(--fg); font-size: 20px; font-weight: 600; margin-top: 6px; }}
.header .source {{ color: var(--muted); font-size: 14px; margin-top: 6px; }}
.navrow {{ display:flex; align-items:center; gap:12px; margin: 8px 0 18px; }}
.btn {{ display:inline-flex; align-items:center; gap:8px; padding:10px 14px; border:1px solid var(--border); border-radius: 14px; background:transparent; color:var(--fg); text-decoration:none; font-weight:700; }}
.btn:hover {{ background:#111322; }}
.tabs {{ display:grid; grid-template-columns: 1fr 1fr; border:1px solid var(--border); border-radius: 14px; overflow: hidden; margin: 12px 0 22px; }}
.tabs button {{ background: transparent; color: var(--fg); padding: 14px 12px; border:0; cursor:pointer; font-weight:700; font-size: 14px; }}
.tabs button.active {{ background: var(--accent); color: #0b0b10; }}
.grid {{ display:grid; gap: 18px; grid-template-columns: repeat(1, minmax(0,1fr)); }}
@media (min-width:720px) {{ .grid {{ grid-template-columns: repeat(2, minmax(0,1fr)); }} }}
@media (min-width:1024px) {{ .grid {{ grid-template-columns: repeat(3, minmax(0,1fr)); }} }}
.card {{ background: var(--card); border:1px solid var(--border); border-radius: 16px; overflow:hidden; }}
.card figure {{ margin:0; }}
.card .embed {{ width:100%; aspect-ratio: 20/9; background:#151522; }}
.card h3 {{ margin:14px 16px 6px; font-size: 16px; line-height: 1.2; }}
.card p {{ margin:0 16px 10px; color:var(--muted); font-size: 13px; }}
.meta {{ margin: 0 16px 16px; font-size: 13px; color: var(--fg); opacity:.95; }}
.footer {{ margin-top: 28px; color: var(--muted); text-align:center; font-size: 12px; }}
.section {{ margin-top: 22px; }}
.section h2 {{ margin: 0 0 10px; font-size: 18px; }}
.disclaimer {{ color:#ff9a9a; }}
.hr {{ height:1px; background:var(--border); margin: 26px 0; }}
.figcap {{ margin: 6px 16px 0; color: var(--muted); font-size: 12px; }}
</style>
</head>"""

# =========================================================
# Market card with accessibility + fallback
# =========================================================
def render_market_card(row: Dict) -> str:
    title = row.get("question") or "(Untitled market)"
    why   = row.get("why") or (row.get("category") or "—")
    embed = row.get("embedSrc") or ""
    vol   = fmt_money(num(row.get("volume24h")) or num(row.get("volume")))
    ttr   = ttr_badge(row.get("timeToResolveDays"))
    meta_line = f"24h {vol} • {ttr}"

    # Provide descriptive text for assistive tech and noscript
    aria_label = f"Polymarket embed for: {title}"
    noscript_text = (
        f"Embedded market card unavailable without JavaScript. "
        f"Visit the market page to view details."
    )
    market_url = row.get("url") or ""
    caption = f"{title}" + (f" — View on Polymarket: {html_lib.escape(market_url)}" if market_url else "")

    return f"""
<div class="card">
  <figure>
    <div class="embed">
      <iframe
        title="{html_lib.escape(title)}"
        aria-label="{html_lib.escape(aria_label)}"
        src="{html_lib.escape(embed)}"
        width="100%" height="100%" frameborder="0"
        loading="lazy" referrerpolicy="no-referrer-when-downgrade">
      </iframe>
    </div>
    <figcaption class="figcap">{html_lib.escape(caption)}</figcaption>
    <noscript>{html_lib.escape(noscript_text)}</noscript>
  </figure>
  <h3>{html_lib.escape(title)}</h3>
  <p>{html_lib.escape(why)}</p>
  <div class="meta">{html_lib.escape(meta_line)}</div>
</div>
""".strip()

def render_section(title: str, rows: List[Dict]) -> str:
    cards = "\n".join(render_market_card(r) for r in rows)
    return f"""
<div class="section">
  <h2>{html_lib.escape(title)}</h2>
  <div class="grid">
    {cards}
  </div>
</div>
""".strip()

# =========================================================
# Pages (navigation rules as specified)
# =========================================================
def render_index_html(now_local_str: str, build_ts: str, hot: List[Dict], gems: List[Dict],
                      meta_desc: str, long_desc_html: str, newest_snapshot: Optional[str]) -> str:
    """
    Home: only Back → to previous snapshot (newest existing). If no snapshots yet, hide nav.
    """
    title = f"Hottest Markets & Overlooked Chances on Polymarket Today — {now_local_str}"
    desc = meta_desc
    page_url = f"{SITE_BASE}/index.html"
    iso_now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    head = html_head(title, page_url, desc, iso_now, build_ts)

    hot_sec = render_section("HOT (Top 12)", hot[:12])
    gem_sec = render_section("Overlooked (Top 12)", gems[:12])

    if newest_snapshot:
        nav = f"""
<!-- NAV_START -->
<div class="navrow">
  <a class="btn" href="{html_lib.escape(newest_snapshot)}">Back →</a>
</div>
<!-- NAV_END -->
"""
    else:
        nav = """
<!-- NAV_START -->
<div class="navrow" aria-hidden="true" style="visibility:hidden;height:0;margin:0;padding:0;"></div>
<!-- NAV_END -->
"""

    methodology = f"""
<div class="hr"></div>
<div class="section">
  <h2>Methodology</h2>
  <p><strong>Hottest:</strong> Prioritizes 24h volume, tighter spreads, and sooner time-to-resolve.</p>
  <p><strong>Overlooked:</strong> Prefers near-50% binary midpoints, negative underround, moderate 24h volume, and sooner resolution.</p>
  <p><strong>Why line:</strong> Displays quick cues like “24h $X • TTR Yd”.</p>
</div>
<div class="section">
  <h2>Description</h2>
  {long_desc_html}
</div>
<div class="hr"></div>
<div class="footer">
  <div><span class="disclaimer"><strong>Not financial advice.</strong> DYOR.</span></div>
  <div>Source: Polymarket API data • Updated {now}</div>
</div>
""".format(now=now_local_str)

    body = f"""<body>
<div class="container">
  <div class="header">
    <h1>Hottest Markets &amp; Overlooked Chances on Polymarket Today</h1>
    <div class="date">{now_local_str}</div>
    <div class="source">Source: Polymarket API data.</div>
  </div>

  {nav}

  <div class="tabs" aria-hidden="true">
    <button class="active" type="button">HOT</button>
    <button type="button">Hidden Gems</button>
  </div>

  {hot_sec}
  {gem_sec}

  {methodology}
</div>
</body>"""
    return f"<!doctype html><html lang=\"en\">{head}{body}</html>"

def render_archive_html(now_local_str: str, build_ts: str, snapshots: List[str]) -> str:
    """
    Archive: only ← Forward to oldest snapshot (if exists).
    """
    title = "Polymarket Dashboards — Archive"
    desc = "Browse historical snapshots; updated every 6 hours. Hottest markets & overlooked chances."
    page_url = f"{SITE_BASE}/archive.html"
    iso_now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    head = html_head(title, page_url, desc, iso_now, build_ts)

    # snapshots is newest→oldest from caller
    # For labels and list:
    def label_for(fn: str) -> str:
        m = re.search(r"dashboard_([0-9]{4}-[0-9]{2}-[0-9]{2})_([0-9]{4})\.html", fn)
        if not m: return fn
        d, hm = m.group(1), m.group(2)
        return f"{d} • {hm[:2]}:{hm[2:]}"

    lis = "\n".join(
        f'<li><a class="btn" href="{html_lib.escape(fn)}">{html_lib.escape(label_for(fn))}</a></li>'
        for fn in snapshots
    ) or "<li>No snapshots yet.</li>"

    oldest = snapshots[-1] if snapshots else None
    if oldest:
        nav_html = f'''
<div class="navrow">
  <a class="btn" href="{html_lib.escape(oldest)}">← Forward</a>
</div>
'''
    else:
        nav_html = '''
<div class="navrow" aria-hidden="true" style="visibility:hidden;height:0;margin:0;padding:0;"></div>
'''

    body = f"""<body>
<div class="container">
  <div class="header">
    <h1>Archive</h1>
    <div class="date">{now_local_str}</div>
    <div class="source">Source: Polymarket API data.</div>
  </div>

  {nav_html}

  <div class="section">
    <h2>Snapshots</h2>
    <ul style="list-style:none; padding:0; display:grid; grid-template-columns:repeat(1,minmax(0,1fr)); gap:10px;">
      {lis}
    </ul>
  </div>

  <div class="hr"></div>
  <div class="footer">
    <div><span class="disclaimer"><strong>Not financial advice.</strong> DYOR.</span></div>
    <div>Updated {now_local_str}</div>
  </div>
</div>
</body>"""
    return f"<!doctype html><html lang=\"en\">{head}{body}</html>"

# =========================================================
# Sitemap & robots
# =========================================================
def write_sitemap_xml(site_dir: Path, site_base: str, pages: List[Dict]):
    out = site_dir / "sitemap.xml"
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
    ]
    for p in pages:
        loc = p["loc"]
        lastmod = p.get("lastmod")
        lines.append("  <url>")
        lines.append(f"    <loc>{loc}</loc>")
        if lastmod:
            lines.append(f"    <lastmod>{lastmod}</lastmod>")
        lines.append("  </url>")
    lines.append("</urlset>")
    out.write_text("\n".join(lines), encoding="utf-8")

def write_robots_txt(site_dir: Path, site_base: str):
    (site_dir / "robots.txt").write_text(
        f"User-agent: *\nAllow: /\nSitemap: {site_base}/sitemap.xml\n",
        encoding="utf-8"
    )

# =========================================================
# Snapshot NAV rewriter (post-build)
# =========================================================
def update_snapshot_navs(site_dir: Path):
    """
    Snapshot pages: Forward -> newer snapshot (or index.html if none)
                    Back    -> older snapshot (or archive.html if none)
    """
    snaps = sorted(site_dir.glob("dashboard_*.html"), key=lambda p: p.name, reverse=True)  # newest → oldest
    if not snaps:
        return

    for idx, snap_path in enumerate(snaps):
        html = snap_path.read_text(encoding="utf-8", errors="ignore")

        newer = snaps[idx - 1].name if idx - 1 >= 0 else None         # toward newer
        older = snaps[idx + 1].name if idx + 1 < len(snaps) else None # toward older

        back_href = older if older else "archive.html"
        fwd_href  = newer if newer else "index.html"

        new_nav = f"""
<!-- NAV_START -->
<div class="navrow">
  <a class="btn" href="{html_lib.escape(fwd_href)}">← Forward</a>
  <a class="btn" href="{html_lib.escape(back_href)}">Back →</a>
</div>
<!-- NAV_END -->
""".strip()

        html_new = re.sub(
            r"<!--\s*NAV_START\s*-->.*?<!--\s*NAV_END\s*-->",
            new_nav,
            html,
            flags=re.DOTALL
        )
        if html_new != html:
            snap_path.write_text(html_new, encoding="utf-8")

# =========================================================
# Main
# =========================================================
def main():
    random.seed()  # ensure varied description selection
    csv_path = resolve_csv_path()
    print(f"[builder] Using CSV: {csv_path}")

    rows = read_rows(csv_path)
    hot, gems = pick_hot_and_gems(rows, k=12)

    SITE_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Rollover previous index to snapshot (previous run only)
    rolled = rollover_previous_index_to_snapshot(SITE_DIR)
    if rolled:
        print(f"[builder] Rolled previous index into snapshot: {rolled}")

    # Gather current snapshot list (newest → oldest)
    snapshots = sorted([p.name for p in SITE_DIR.glob("dashboard_*.html")], reverse=True)
    newest_snap = snapshots[0] if snapshots else None

    # 2) Choose SEO descriptions
    meta_desc, long_desc_html, meta_name, long_name = choose_descriptions()
    print(f"[builder] Using meta: {meta_name} | long: {long_name}")

    # 3) Build current pages (index + archive) — no current snapshot duplication
    now = datetime.now(timezone.utc)
    build_ts = now.strftime(TIMESTAMP_FMT)
    now_local_str = now.astimezone().strftime("%d %B %Y • %H:%M")

    index_html = render_index_html(now_local_str, build_ts, hot, gems, meta_desc, long_desc_html, newest_snap)
    (SITE_DIR / "index.html").write_text(index_html, encoding="utf-8")

    # refresh snapshot list in case none existed before (index written regardless)
    snapshots = sorted([p.name for p in SITE_DIR.glob("dashboard_*.html")], reverse=True)

    archive_html = render_archive_html(now_local_str, build_ts, snapshots)
    (SITE_DIR / "archive.html").write_text(archive_html, encoding="utf-8")

    # 4) SEO assets (always write)
    iso_now = now.isoformat(timespec="seconds")
    pages = [
        {"loc": f"{SITE_BASE}/index.html", "lastmod": iso_now},
        {"loc": f"{SITE_BASE}/archive.html", "lastmod": iso_now},
    ] + [{"loc": f"{SITE_BASE}/{name}", "lastmod": iso_now} for name in snapshots]
    write_sitemap_xml(SITE_DIR, SITE_BASE, pages)
    write_robots_txt(SITE_DIR, SITE_BASE)

    # 5) After all pages exist, rewrite snapshot navs
    update_snapshot_navs(SITE_DIR)

    print("[ok] Wrote site/index.html, site/archive.html, site/sitemap.xml, site/robots.txt, and updated snapshot navs")

if __name__ == "__main__":
    main()