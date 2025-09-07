#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds static HTML pages from the latest enriched CSV.

Implements UX/SEO fixes (Steps 1–10):
- Consistent header/footer nav, per-page rules (index/archive/snapshot)
- Tabs label = "Overlooked" and switching works
- Remove duplicate plain "why" line; keep stat boxes only
- Muted, small embed captions; link not blue
- Remove "HOT (Top 12) / Overlooked (Top 12)" headings on index
- Archive gains rotating description (like index). Snapshots freeze description at publish time
- All times show "UTC"; og:updated_time in ISO UTC
- Snapshot titles drop "Today"; index keeps it

Inputs:
- CSV path as argv[1], or auto-pick newest in data/
- content/meta/*.txt (short description pool)
- content/long/*.html (long description pool)
- data/desc_history.json (to avoid repeats)

Outputs:
- site/index.html
- site/dashboard_YYYY-MM-DD_HHMM.html (snapshot)
- site/archive.html
- site/robots.txt
- site/sitemap.xml
- data/desc_history.json (updated)
"""

import sys, csv, re, html as html_lib, json, random
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CONTENT_DIR = ROOT / "content"
SITE_DIR = ROOT / "site"

META_DIR = CONTENT_DIR / "meta"
LONG_DIR = CONTENT_DIR / "long"

HISTORY_PATH = DATA_DIR / "desc_history.json"

# -----------------------
# Helpers
# -----------------------
def utc_now():
    return datetime.now(timezone.utc)

def ts_for_snapshot(dt: datetime) -> str:
    # dashboard_YYYY-MM-DD_HHMM.html
    return dt.strftime("dashboard_%Y-%m-%d_%H%M.html")

def human_date(dt: datetime) -> str:
    return dt.strftime("%d %B %Y • %H:%M UTC")

def iso_og_time(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def newest_csv_in_data() -> Path | None:
    csvs = list(DATA_DIR.glob("polymarket_enriched_fast_*.csv"))
    if not csvs:
        return None
    return sorted(csvs, key=lambda p: p.stat().st_mtime, reverse=True)[0]

def read_csv_rows(csv_path: Path) -> list[dict]:
    with csv_path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]

def escape(s: str) -> str:
    return html_lib.escape(s, quote=True)

def load_history() -> dict:
    """
    Always return a dict with lists:
      { "recent_meta": [...], "recent_long": [...] }
    even if file is missing, empty, malformed, or wrong shape.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if HISTORY_PATH.exists():
        try:
            obj = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            obj = {}
    else:
        obj = {}
    if not isinstance(obj, dict):
        obj = {}
    if "recent_meta" not in obj or not isinstance(obj.get("recent_meta"), list):
        obj["recent_meta"] = []
    if "recent_long" not in obj or not isinstance(obj.get("recent_long"), list):
        obj["recent_long"] = []
    return obj

def save_history(hist: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_PATH.write_text(json.dumps(hist, ensure_ascii=False, indent=2), encoding="utf-8")

def choose_rotating_file(pool: list[Path], recent: list[str], keep: int = 30) -> Path | None:
    if not pool:
        return None
    # Prefer those not in recent
    candidates = [p for p in pool if p.name not in recent]
    pick = random.choice(candidates or pool)
    # update recent (append and cap)
    recent.append(pick.name)
    while len(recent) > keep:
        recent.pop(0)
    return pick

def parse_money(n: str | None) -> str:
    try:
        x = float(n) if n not in (None, "") else 0.0
        return f"${int(round(x)):,}"
    except Exception:
        return "—"

def caption_text(question: str, url: str) -> str:
    # Smaller muted caption; link inherits color; not blue
    return (
        f"<p class='embed-caption'>"
        f"{escape(question)} — "
        f"<a href='{escape(url)}' target='_blank' rel='noopener'>View on Polymarket</a>"
        f"</p>"
    )

def ttr_from_iso(end_iso: str | None) -> str:
    if not end_iso:
        return "—"
    try:
        dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        sec = (dt - utc_now()).total_seconds()
        if sec <= 0: return "Ended"
        days = sec / 86400.0
        return f"{days:.1f}d"
    except Exception:
        return "—"

# -----------------------
# Templating
# -----------------------
BASE_CSS = """
:root { --bg:#0b0b10; --fg:#eaeaf0; --muted:#a3a8b4; --card:#141420; --border:#232336; --accent:#6ea8fe; --accent-2:#a879fe; }
* { box-sizing: border-box; }
body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background: var(--bg); color: var(--fg); }
.container { max-width: 1240px; margin: 0 auto; padding: 32px 24px 44px; }
.header { margin-bottom: 18px; }
.header h1 { margin:0; font-size: 34px; font-variant: small-caps; letter-spacing: .5px; }
.header .date { color: var(--fg); font-size: 20px; font-weight: 600; margin-top: 6px; }
.header .source { color: var(--muted); font-size: 14px; margin-top: 6px; }

.navrow { display:flex; align-items:center; gap:12px; margin: 8px 0 18px; flex-wrap: wrap; }
.btn { display:inline-flex; align-items:center; justify-content:center; gap:8px; padding:12px 14px; border:1px solid var(--border); border-radius: 14px; background:transparent; color:var(--fg); text-decoration:none; font-weight:700; }
.btn:hover { background:#141827; }
.btn[hidden] { display:none !important; }
.btn .ico { font-size: 14px; line-height: 1; opacity:.9; }

/* Tabs */
.tabs { display:grid; grid-template-columns:1fr 1fr; border:1px solid var(--border); border-radius:14px; overflow:hidden; margin: 12px 0 22px; }
.tabs button { background: transparent; color: var(--fg); padding: 14px 12px; border:0; cursor:pointer; font-weight:700; font-size: 14px; }
.tabs button.active { background: var(--accent); color: #0b0b10; }

/* Grid & Cards */
.grid { display:grid; gap:18px; grid-template-columns: 1fr; }
@media (min-width: 680px) { .grid { grid-template-columns: 1fr 1fr; } }
@media (min-width: 1024px) { .grid { grid-template-columns: 1fr 1fr 1fr; } }
.card { border:1px solid var(--border); background: var(--card); border-radius: 16px; overflow:hidden; }
.card-body { padding:14px; }
.card h3 { margin:0; font-size:16px; font-weight:700; line-height:1.3; }
.stats { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:10px; }
.stat { border:1px solid var(--border); border-radius: 12px; padding:8px; font-size:12px; }
.stat .lab { display:block; font-weight:700; margin-bottom:4px; font-size:12px; }
.stat .val { font-size:12px; }

.embed-wrap { width:100%; aspect-ratio: 20/9; background:#0f1120; }
.embed { width:100%; height:100%; border:0; }
.embed-caption { margin: 6px 14px 12px; font-size:12px; color: var(--muted); opacity:.9; }
.embed-caption a { color: inherit; text-decoration: none; }
.embed-caption a:hover { text-decoration: underline; }

/* Sections */
.section { margin-top: 18px; }
.section h2 { margin:0 0 10px 0; font-size: 16px; }
.meta-block { margin-top: 18px; color:var(--muted); font-size: 14px; }

/* Footer */
.footer { margin-top: 26px; border-top:1px solid var(--border); padding-top:18px; color:var(--muted); font-size:12px; text-align:center; }
.footer .navrow { justify-content:center; margin-top: 10px; }

/* System info */
.sys { margin-top:8px; font-size: 11px; color: var(--muted); opacity:.85; }
"""

TABS_JS = """
<script>
(function(){
  const tabHot = document.getElementById('tab-hot');
  const tabGem = document.getElementById('tab-overlooked');
  const secHot = document.getElementById('sec-hot');
  const secGem = document.getElementById('sec-overlooked');

  function activate(which){
    if(which==='hot'){
      tabHot.classList.add('active');
      tabGem.classList.remove('active');
      secHot.style.display = '';
      secGem.style.display = 'none';
    }else{
      tabGem.classList.add('active');
      tabHot.classList.remove('active');
      secGem.style.display = '';
      secHot.style.display = 'none';
    }
  }

  tabHot?.addEventListener('click', ()=>activate('hot'));
  tabGem?.addEventListener('click', ()=>activate('overlooked'));
  // default to HOT visible
  activate('hot');
})();
</script>
"""

# -----------------------
# HTML builders
# -----------------------
def build_nav_top(page_type: str) -> str:
    """
    Top row: Home (left) and Archive (right), hidden on their own pages
    page_type: 'index' | 'archive' | 'snapshot'
    """
    home_hidden = " hidden" if page_type == "index" else ""
    arch_hidden = " hidden" if page_type == "archive" else ""
    return (
        "<div class='navrow' role='navigation' aria-label='Top utility navigation'>"
        f"<a class='btn{home_hidden}' href='index.html' aria-label='Home'><span class='ico'>&larr;</span> Home</a>"
        f"<span style='flex:1 1 auto'></span>"
        f"<a class='btn{arch_hidden}' href='archive.html' aria-label='Archive'>Archive <span class='ico'>&rarr;</span></a>"
        "</div>"
    )

def build_nav_back_forward(page_type: str, back_href: str | None, fwd_href: str | None) -> str:
    """
    Second row: Back / Forward according to rules.
    If a link is not available, hide its button.
    """
    back_hidden = " hidden" if not back_href else ""
    fwd_hidden  = " hidden" if not fwd_href else ""
    back_btn = f"<a class='btn{back_hidden}' href='{escape(back_href or '')}' aria-label='Back'>Back <span class='ico'>&rarr;</span></a>"
    fwd_btn  = f"<a class='btn{fwd_hidden}'  href='{escape(fwd_href or '')}' aria-label='Forward'><span class='ico'>&larr;</span> Forward</a>"
    return f"<div class='navrow' role='navigation' aria-label='Snapshot navigation'>{fwd_btn}<span style='flex:1 1 auto'></span>{back_btn}</div>"

def build_card(row: dict) -> str:
    title = row.get("question") or "(Untitled)"
    url = row.get("url") or ""
    embed = row.get("embedSrc") or ""
    vol24 = parse_money(row.get("volume24h"))
    spread = (row.get("avgSpread") or "—")
    ttr = ttr_from_iso(row.get("endDateISO"))
    momentum = row.get("momentumPct24h") or row.get("momentumDelta24h") or "—"
    return f"""
<article class="card">
  <div class="embed-wrap">
    <iframe class="embed" title="{escape(title)}" src="{escape(embed)}" loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>
  </div>
  {caption_text(title, url)}
  <div class="card-body">
    <h3>{escape(title)}</h3>
    <div class="stats">
      <div class="stat"><span class="lab">24h Vol</span><span class="val">{escape(vol24)}</span></div>
      <div class="stat"><span class="lab">Avg Spread</span><span class="val">{escape(str(spread))}</span></div>
      <div class="stat"><span class="lab">Time to Resolve</span><span class="val">{escape(ttr)}</span></div>
      <div class="stat"><span class="lab">Momentum</span><span class="val">{escape(str(momentum))}</span></div>
    </div>
  </div>
</article>""".strip()

def render_grid(rows: list[dict]) -> str:
    return "<section class='grid'>" + "\n".join(build_card(r) for r in rows) + "</section>"

def page_head(title: str, description: str, canonical: str, og_updated: datetime, build_ts_tag: str, build_nonce: str) -> str:
    return f"""<!doctype html><html lang='en'><head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{escape(title)}</title>
<!-- build_ts: {escape(build_ts_tag)} -->
<!-- build_nonce: {escape(build_nonce)} -->
<meta name="description" content="{escape(description)}" />
<meta name="keywords" content="polymarket, election odds, prediction markets, betting markets, hidden gems, dashboard" />
<link rel="canonical" href="{escape(canonical)}" />
<meta property="og:title" content="{escape(title)}" />
<meta property="og:description" content="{escape(description)}" />
<meta property="og:type" content="website" />
<meta property="og:url" content="{escape(canonical)}" />
<meta property="og:image" content="https://urbanpoly.com/og-preview.png" />
<meta property="og:updated_time" content="{escape(iso_og_time(og_updated))}" />
<meta name="twitter:card" content="summary_large_image" />
<meta name="twitter:title" content="{escape(title)}" />
<meta name="twitter:description" content="{escape(description)}" />
<meta name="twitter:image" content="https://urbanpoly.com/og-preview.png" />
<style>{BASE_CSS}</style>
</head><body>
"""

def page_footer(build_dt: datetime, page_type: str, back_href: str | None, fwd_href: str | None) -> str:
    # Footer nav mirrors header nav rows
    return f"""
<div class="footer">
  <div class="navrow" role="navigation" aria-label="Footer utility nav">
    {'<a class="btn" href="index.html"><span class="ico">&larr;</span> Home</a>' if page_type!='index' else ''}
    <span style="flex:1 1 auto"></span>
    {'<a class="btn" href="archive.html">Archive <span class="ico">&rarr;</span></a>' if page_type!='archive' else ''}
  </div>
  {build_nav_back_forward(page_type, back_href, fwd_href)}
  <div class="sys">Last updated: {escape(human_date(build_dt))}</div>
  <div class="sys">Not financial advice. DYOR.</div>
</div>
</body></html>"""

def description_html(short: str, long_html: str) -> str:
    return f"""
<div class="section">
  <h2>Description</h2>
  <p class="meta-block">{escape(short)}</p>
  {long_html}
</div>
""".strip()

def methodology_html() -> str:
    return """
<div class="section">
  <h2>Methodology</h2>
  <ul class="meta-block">
    <li><strong>Hottest</strong>: Prioritizes 24h volume, tighter spreads, and sooner time-to-resolve.</li>
    <li><strong>Overlooked</strong>: Prefers near-50% binary midpoints, negative underround, moderate 24h volume, and sooner resolution.</li>
    <li><strong>Why line</strong>: Displays quick cues like “24h $X • TTR Yd” pulled from the Polymarket API.</li>
  </ul>
</div>
""".strip()

# -----------------------
# Main build
# -----------------------
def main():
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Pick CSV
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    else:
        csv_path = newest_csv_in_data()
        if not csv_path:
            print("ERROR: No CSV found in data/, and none provided")
            sys.exit(1)

    print(f"[builder] CLI CSV arg detected: {csv_path}" if len(sys.argv)>1 else f"[builder] Auto-picked latest CSV: {csv_path}")
    rows = read_csv_rows(csv_path)
    if not rows:
        print("ERROR: CSV has no rows")
        sys.exit(1)

    # Slice top 12 lists by the CSV order (assume already ranked), or fallback safe slice
    top = rows[:12]
    # derive a "gems" set by filtering where near50Flag==1 or underround<0 then take first 12
    gems_pool = []
    for r in rows:
        try:
            near50 = float(r.get("near50Flag") or 0)
        except Exception:
            near50 = 0
        try:
            under = float(r.get("underround") or 0)
        except Exception:
            under = 0
        if near50 >= 1 or under < 0:
            gems_pool.append(r)
    gems = (gems_pool[:12]) if gems_pool else rows[12:24]

    # ROTATING descriptions (index & archive); snapshots freeze
    hist = load_history()
    meta_files = sorted(META_DIR.glob("*.txt"))
    long_files = sorted(LONG_DIR.glob("*.html"))

    meta_pick = choose_rotating_file(meta_files, hist["recent_meta"])
    long_pick = choose_rotating_file(long_files, hist["recent_long"])
    short_desc = (meta_pick.read_text(encoding="utf-8").strip() if meta_pick and meta_pick.exists() else "Daily dashboard of Polymarket heat & overlooked opportunities.")
    long_desc_html = (long_pick.read_text(encoding="utf-8") if long_pick and long_pick.exists() else "<p>Insightful commentary rotates here.</p>")

    print(f"[builder] Using meta={meta_pick.name if meta_pick else '—'}, long={long_pick.name if long_pick else '—'}")

    # Times / titles
    now = utc_now()
    human = human_date(now)
    build_ts_tag = now.strftime("%Y-%m-%d_%H%M%S")
    nonce = str(random.getrandbits(31))

    # ---------- Build INDEX (with Today) ----------
    index_title = f"Hottest Markets & Overlooked Chances on Polymarket Today — {human}"
    head = page_head(
        title=index_title,
        description=short_desc[:160],
        canonical="https://urbanpoly.com/index.html",
        og_updated=now,
        build_ts_tag=build_ts_tag,
        build_nonce=nonce,
    )
    # top nav (Archive only on index)
    top_nav = build_nav_top("index")
    # back link on index: previous snapshot by filename order (if exists)
    snapshots = sorted(SITE_DIR.glob("dashboard_*.html"))
    back_href_index = snapshots[-1].name if snapshots else None
    nav_row = build_nav_back_forward("index", back_href_index, None)

    tabs = f"""
<div class="tabs">
  <button id="tab-hot" class="active">HOT</button>
  <button id="tab-overlooked">Overlooked</button>
</div>
"""

    html_index = [
        head,
        f"<div class='container'>",
        "<header class='header'>",
        f"<h1>Hottest Markets &amp; Overlooked Chances on Polymarket Today</h1>",
        f"<div class='date'>{escape(human)}</div>",
        "<div class='source'>Source: Polymarket API data.</div>",
        "</header>",
        top_nav,
        nav_row,
        tabs,
        # No extra headings "HOT (Top 12)" etc. — removed per STEP 5
        "<section id='sec-hot'>",
        render_grid(top),
        "</section>",
        "<section id='sec-overlooked' style='display:none'>",
        render_grid(gems),
        "</section>",
        description_html(short_desc, long_desc_html),
        methodology_html(),
        "</div>",
        TABS_JS,
        page_footer(now, "index", back_href_index, None),
    ]
    (SITE_DIR / "index.html").write_text("\n".join(html_index), encoding="utf-8")

    # ---------- Build SNAPSHOT (drop Today) ----------
    snap_name = ts_for_snapshot(now)
    snap_title = f"Hottest Markets & Overlooked Chances on Polymarket — {human}"
    head_snap = page_head(
        title=snap_title,
        description=short_desc[:160],  # freeze description at publish time
        canonical=f"https://urbanpoly.com/{snap_name}",
        og_updated=now,
        build_ts_tag=build_ts_tag,
        build_nonce=nonce,
    )
    # snapshot nav: Home+Archive top; back+forward second row
    # compute neighbors from existing snapshots + this one
    snapshots = sorted(SITE_DIR.glob("dashboard_*.html"))
    older = snapshots  # existing ones already on disk
    older_names = [p.name for p in older]
    # back href: last older (or archive)
    snap_back = older_names[-1] if older_names else "archive.html"
    # forward href: none yet (to be index)
    snap_fwd = "index.html"
    top_nav_snap = build_nav_top("snapshot")
    nav_row_snap = build_nav_back_forward("snapshot", snap_back, snap_fwd)

    html_snap = [
        head_snap,
        "<div class='container'>",
        "<header class='header'>",
        f"<h1>Hottest Markets &amp; Overlooked Chances on Polymarket</h1>",
        f"<div class='date'>{escape(human)}</div>",
        "<div class='source'>Source: Polymarket API data.</div>",
        "</header>",
        top_nav_snap,
        nav_row_snap,
        """
<div class="tabs">
  <button id="tab-hot" class="active">HOT</button>
  <button id="tab-overlooked">Overlooked</button>
</div>
""",
        "<section id='sec-hot'>",
        render_grid(top),
        "</section>",
        "<section id='sec-overlooked' style='display:none'>",
        render_grid(gems),
        "</section>",
        # Snapshot keeps the description frozen at publish time
        description_html(short_desc, long_desc_html),
        methodology_html(),
        "</div>",
        TABS_JS,
        page_footer(now, "snapshot", snap_back, snap_fwd),
    ]
    (SITE_DIR / snap_name).write_text("\n".join(html_snap), encoding="utf-8")

    # ---------- Build ARCHIVE (rotating description like index) ----------
    snaps_after_write = sorted(SITE_DIR.glob("dashboard_*.html"))
    oldest = snaps_after_write[0].name if snaps_after_write else None
    head_arch = page_head(
        title="Polymarket Dashboards — Archive",
        description=short_desc[:160],  # rotates every run
        canonical="https://urbanpoly.com/archive.html",
        og_updated=now,
        build_ts_tag=build_ts_tag,
        build_nonce=nonce,
    )
    top_nav_arch = build_nav_top("archive")
    nav_row_arch = build_nav_back_forward("archive", None, oldest)

    # Simple list of snapshots newest->oldest
    items = []
    for p in sorted(snaps_after_write, key=lambda x: x.stat().st_mtime, reverse=True):
        label = p.name.replace("dashboard_", "").replace(".html", "")
        items.append(f"<li><a href='{p.name}'>{escape(label)}</a></li>")
    list_html = "<ul>" + "\n".join(items) + "</ul>" if items else "<p>No snapshots yet.</p>"

    html_arch = [
        head_arch,
        "<div class='container'>",
        "<header class='header'>",
        "<h1>Archive</h1>",
        f"<div class='date'>{escape(human)}</div>",
        "<div class='source'>All published snapshots, newest first.</div>",
        "</header>",
        top_nav_arch,
        nav_row_arch,
        "<div class='section'><h2>All Snapshots</h2>",
        list_html,
        "</div>",
        # Archive gets rotating description every run (STEP 7)
        description_html(short_desc, long_desc_html),
        methodology_html(),
        "</div>",
        page_footer(now, "archive", None, oldest),
    ]
    (SITE_DIR / "archive.html").write_text("\n".join(html_arch), encoding="utf-8")

    # ---------- robots.txt & sitemap.xml ----------
    robots = "User-agent: *\nAllow: /\nSitemap: https://urbanpoly.com/sitemap.xml\n"
    (SITE_DIR / "robots.txt").write_text(robots, encoding="utf-8")

    # Build a sitemap of index + archive + all snapshots
    snap_names = [p.name for p in snaps_after_write]
    urls = ["index.html", "archive.html"] + snap_names
    locs = [f"https://urbanpoly.com/{u}" for u in urls]
    lastmod = iso_og_time(now)
    sm = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
    ]
    for loc in locs:
        sm += [
            "<url>",
            f"<loc>{loc}</loc>",
            f"<lastmod>{lastmod}</lastmod>",
            "</url>"
        ]
    sm.append("</urlset>")
    (SITE_DIR / "sitemap.xml").write_text("\n".join(sm), encoding="utf-8")

    # Save desc history (for rotation) — robust file
    save_history(hist)

    print("[ok] Wrote site/index.html, site/archive.html, site/sitemap.xml, site/robots.txt; updated snapshot navs")

if __name__ == "__main__":
    sys.exit(main())
