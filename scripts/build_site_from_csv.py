#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds static HTML pages from the latest enriched CSV.

New in this version:
- ARCHIVE: snapshot list items are rendered as big .btn buttons (not tiny links).
- ARCHIVE: the newest snapshot entry points to index.html (live) instead of its own snapshot file.
  (We still write the newest snapshot file for history; the archive list just treats it as "live".)

Also retained:
- Tabs show ONE section at a time; Overlooked distinct from HOT.
- Navigation consistency (header & footer); index shows Back only; archive shows Forward-only; snapshots show Back+Forward.
- UTC time everywhere; rotating descriptions for index+archive; frozen for snapshots.
- robots.txt + sitemap.xml emitted.
- desc_history.json robust load/save.

Added now:
- Google Tag Manager snippets (head + noscript) with your container ID.

Critical fix:
- Use the newest polymarket_top12_*.csv (if present) to pick the exact HOT and Hidden Gems sets,
  matching the fetch step output. Fallback to prior heuristic if top12 file is missing.
"""

import sys, csv, html as html_lib, json, random, re
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

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
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def ts_for_snapshot(dt: datetime) -> str:
    return dt.strftime("dashboard_%Y-%m-%d_%H%M.html")

def human_date(dt: datetime) -> str:
    return dt.strftime("%d %B %Y • %H:%M UTC")

def iso_og_time(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def newest_csv_in_data() -> Optional[Path]:
    csvs = list(DATA_DIR.glob("polymarket_enriched_fast_*.csv"))
    if not csvs:
        return None
    return sorted(csvs, key=lambda p: p.stat().st_mtime, reverse=True)[0]

def newest_top12_csv() -> Optional[Path]:
    """
    Look for the newest polymarket_top12_*.csv. The fetch step often leaves it
    at repo root; sometimes you may move it into data/. We check both.
    """
    candidates = list(ROOT.glob("polymarket_top12_*.csv")) + list(DATA_DIR.glob("polymarket_top12_*.csv"))
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]

def read_csv_rows(csv_path: Path) -> List[Dict[str, Any]]:
    with csv_path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]

def escape(s: str) -> str:
    return html_lib.escape(s, quote=True)

def load_history() -> Dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if HISTORY_PATH.exists():
        try:
            obj = json.loads(HISTORY_PATH.read_text(encoding="utf-8") or "{}")
        except Exception:
            obj = {}
    else:
        obj = {}
    if not isinstance(obj, dict): obj = {}
    obj.setdefault("recent_meta", [])
    obj.setdefault("recent_long", [])
    return obj

def save_history(hist: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_PATH.write_text(json.dumps(hist, ensure_ascii=False, indent=2), encoding="utf-8")

def choose_rotating_file(pool: List[Path], recent: List[str], keep: int = 30) -> Optional[Path]:
    if not pool:
        return None
    candidates = [p for p in pool if p.name not in recent]
    pick = random.choice(candidates or pool)
    recent.append(pick.name)
    while len(recent) > keep:
        recent.pop(0)
    return pick

def parse_money(n: Optional[str]) -> str:
    try:
        x = float(n) if n not in (None, "") else 0.0
        return f"${int(round(x)):,}"
    except Exception:
        return "—"

def caption_text(question: str, url: str) -> str:
    return (
        f"<p class='embed-caption'>"
        f"{escape(question)} — "
        f"<a href='{escape(url)}' target='_blank' rel='noopener'>View on Polymarket</a>"
        f"</p>"
    )

def ttr_from_iso(end_iso: Optional[str]) -> str:
    if not end_iso:
        return "—"
    try:
        dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00")) if end_iso.endswith("Z") else datetime.fromisoformat(end_iso)
        sec = (dt - utc_now()).total_seconds()
        if sec <= 0: return "Ended"
        return f"{sec/86400.0:.1f}d"
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

/* Archive list as buttons */
.archive-list { display:grid; gap:12px; grid-template-columns: 1fr; }
@media (min-width: 680px) { .archive-list { grid-template-columns: 1fr 1fr; } }
@media (min-width: 1024px) { .archive-list { grid-template-columns: 1fr 1fr 1fr; } }
.archive-item { display:block; }

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

# ------------- Google Tag Manager snippets (safe-string, brace-escaped) -------------
GTM_ID = "GTM-WJ2H3V7F"

GTM_HEAD = """<!-- Google Tag Manager -->
<script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{'gtm.start':
new Date().getTime(),event:'gtm.js'}});var f=d.getElementsByTagName(s)[0],
j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
}})(window,document,'script','dataLayer','{id}');</script>
<!-- End Google Tag Manager -->""".format(id=GTM_ID)

GTM_NOSCRIPT = """<!-- Google Tag Manager (noscript) -->
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id={id}"
height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
<!-- End Google Tag Manager (noscript) -->""".format(id=GTM_ID)

# -----------------------
# HTML builders
# -----------------------
def build_nav_top(page_type: str) -> str:
    # Home hidden on index; Archive hidden on archive.
    return (
        "<div class='navrow' role='navigation' aria-label='Top utility navigation'>"
        f"<a class='btn' href='index.html' aria-label='Home' {'hidden' if page_type=='index' else ''}><span class='ico'>&larr;</span> Home</a>"
        "<span style='flex:1 1 auto'></span>"
        f"<a class='btn' href='archive.html' aria-label='Archive' {'hidden' if page_type=='archive' else ''}>Archive <span class='ico'>&rarr;</span></a>"
        "</div>"
    )

def build_nav_back_forward(page_type: str, back_href: Optional[str], fwd_href: Optional[str]) -> str:
    # On index: force no Forward
    if page_type == "index":
        fwd_href = None
    back_btn = f"<a class='btn' href='{escape(back_href or '')}' aria-label='Back' {'hidden' if not back_href else ''}>Back <span class='ico'>&rarr;</span></a>"
    fwd_btn  = f"<a class='btn' href='{escape(fwd_href or '')}' aria-label='Forward' {'hidden' if not fwd_href else ''}><span class='ico'>&larr;</span> Forward</a>"
    return f"<div class='navrow' role='navigation' aria-label='Snapshot navigation'>{fwd_btn}<span style='flex:1 1 auto'></span>{back_btn}</div>"

def page_footer(build_dt: datetime, page_type: str, back_href: Optional[str], fwd_href: Optional[str]) -> str:
    """
    Footer with two rows:
      1) Utility nav (Home / Archive) — Home hidden on index, Archive hidden on archive.
      2) Snapshot nav (Forward | Back) — Forward hidden on index.
    Also shows last-updated timestamp and disclaimer.
    """
    # Utility row: Home/Archive
    util_left  = "" if page_type == "index"   else '<a class="btn" href="index.html"><span class="ico">&larr;</span> Home</a>'
    util_right = "" if page_type == "archive" else '<a class="btn" href="archive.html">Archive <span class="ico">&rarr;</span></a>'

    # Snapshot row: no Forward on index
    if page_type == "index":
        fwd_href = None

    back_btn = f'<a class="btn" href="{escape(back_href or "")}" {"hidden" if not back_href else ""}>Back <span class="ico">&rarr;</span></a>'
    fwd_btn  = f'<a class="btn" href="{escape(fwd_href or "")}" {"hidden" if not fwd_href else ""}><span class="ico">&larr;</span> Forward</a>'

    return f"""
<div class="footer">
  <div class="navrow" role="navigation" aria-label="Footer utility nav">
    {util_left}<span style="flex:1 1 auto"></span>{util_right}
  </div>
  <div class="navrow" role="navigation" aria-label="Footer snapshot nav">
    {fwd_btn}<span style="flex:1 1 auto"></span>{back_btn}
  </div>
  <div class="sys">Last updated: {escape(human_date(build_dt))}</div>
  <div class="sys">Not financial advice. DYOR.</div>
</div>
</body></html>"""

def render_grid(rows: List[Dict[str, Any]]) -> str:
    return "<section class='grid'>" + "\n".join(build_card(r) for r in rows) + "</section>"

def build_card(row: Dict[str, Any]) -> str:
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
  <noscript>This market embed requires JavaScript. View it on Polymarket: <a href="{escape(url)}">{escape(url)}</a></noscript>
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

# ---------- SEO HEAD (versioned OG/Twitter image) ----------
def page_head(title: str, description: str, canonical: str, og_updated: datetime) -> str:
    """
    Adds:
    - unique <title>, <meta name="description">, and page-specific keywords
    - favicons
    - JSON-LD (WebSite + WebPage)
    - versioned OG/Twitter image URL to bust social caches
    - Google Tag Manager (head)
    """
    if canonical.endswith("archive.html"):
        keywords = "polymarket archive, prediction markets archive, polymarket snapshots, dashboard history"
    elif canonical.endswith("index.html"):
        keywords = "polymarket, prediction markets, polymarket odds, election odds, betting markets, dashboard"
    else:
        keywords = "polymarket snapshot, prediction markets snapshot, polymarket odds, election odds, dashboard"

    ver = og_updated.strftime("%Y%m%d%H%M")  # cache-buster
    og_img = f"https://urbanpoly.com/og-preview.png?v={ver}"

    # Structured data
    website_ld = {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "@id": "https://urbanpoly.com/#website",
        "url": "https://urbanpoly.com/",
        "name": "UrbanPoly — Polymarket Dashboard",
        "description": "Automated Polymarket dashboard highlighting hottest and overlooked markets, refreshed ~6h."
    }
    webpage_ld = {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "isPartOf": {"@id": "https://urbanpoly.com/#website"},
        "url": canonical,
        "name": title,
        "description": description,
        "dateModified": iso_og_time(og_updated)
    }
    if canonical.startswith("https://urbanpoly.com/dashboard_"):
        webpage_ld["datePublished"] = iso_og_time(og_updated)

    json_ld = json.dumps(website_ld, separators=(",", ":")) + "\n" + json.dumps(webpage_ld, separators=(",", ":"))

    return (
        "<!doctype html><html lang='en'><head>\n"
        "<meta charset=\"utf-8\" />\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        f"<title>{escape(title)}</title>\n"
        f"<meta name=\"description\" content=\"{escape(description)}\" />\n"
        f"<meta name=\"keywords\" content=\"{escape(keywords)}\" />\n"
        f"<link rel=\"canonical\" href=\"{escape(canonical)}\" />\n"
        "<link rel=\"icon\" href=\"/favicon.ico\" />\n"
        "<link rel=\"shortcut icon\" href=\"/favicon.ico\" />\n"
        "<link rel=\"apple-touch-icon\" href=\"/apple-touch-icon.png\" />\n"
        f"{GTM_HEAD}\n"
        f"<meta property=\"og:title\" content=\"{escape(title)}\" />\n"
        f"<meta property=\"og:description\" content=\"{escape(description)}\" />\n"
        "<meta property=\"og:type\" content=\"website\" />\n"
        f"<meta property=\"og:url\" content=\"{escape(canonical)}\" />\n"
        f"<meta property=\"og:image\" content=\"{escape(og_img)}\" />\n"
        f"<meta property=\"og:updated_time\" content=\"{escape(iso_og_time(og_updated))}\" />\n"
        "<meta name=\"twitter:card\" content=\"summary_large_image\" />\n"
        f"<meta name=\"twitter:title\" content=\"{escape(title)}\" />\n"
        f"<meta name=\"twitter:description\" content=\"{escape(description)}\" />\n"
        f"<meta name=\"twitter:image\" content=\"{escape(og_img)}\" />\n"
        "<script type=\"application/ld+json\">\n"
        f"{json_ld}\n"
        "</script>\n"
        f"<style>{BASE_CSS}</style>\n"
        "</head><body>\n"
        f"{GTM_NOSCRIPT}\n"
    )

# ---------- Top-12 resolution helpers ----------
def _row_key(r: Dict[str, Any]) -> Optional[str]:
    """Return a stable key preference: id > slug > url > question."""
    for k in ("id", "slug", "url", "question"):
        v = r.get(k)
        if v not in (None, "", "0"):
            return str(v)
    return None

def _build_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        k = _row_key(r)
        if k:
            idx[k] = r
    return idx

def _resolve_top12_rows(top12_rows: List[Dict[str, Any]], enriched_idx: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Map rows from top12 CSV to full enriched rows using id/slug/url/question.
    If a row can't be mapped, we keep the top12 row as-is (best-effort).
    """
    out: List[Dict[str, Any]] = []
    for r in top12_rows:
        k = _row_key(r)
        if k and k in enriched_idx:
            out.append(enriched_idx[k])
        else:
            out.append(r)
    return out

# -----------------------
# Main build
# -----------------------
def main() -> int:
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else newest_csv_in_data()
    if not csv_path or not csv_path.exists():
        print("ERROR: No CSV found or provided.")
        return 1
    rows = read_csv_rows(csv_path)
    if not rows:
        print("ERROR: CSV has no rows")
        return 1

    # Prefer using the explicit Top-12 file (matches fetch output exactly).
    hot: List[Dict[str, Any]]
    gems: List[Dict[str, Any]]

    top12_path = newest_top12_csv()
    if top12_path and top12_path.exists():
        try:
            top12_rows = read_csv_rows(top12_path)
            enriched_idx = _build_index(rows)

            # Assume file order: first 12 HOT, next 12 Hidden Gems.
            hot_candidates = top12_rows[:12]
            gem_candidates = top12_rows[12:24]

            hot = _resolve_top12_rows(hot_candidates, enriched_idx)
            gems = _resolve_top12_rows(gem_candidates, enriched_idx)

            # If the file is shorter or malformed, fall back for any missing part.
            need_hot = len(hot) < 12
            need_gems = len(gems) < 12

            if need_hot or need_gems:
                # Fall back gracefully to previous heuristic for missing slots
                # (keep already-resolved items in front).
                taken_keys = { _row_key(r) for r in (hot + gems) if _row_key(r) }
                # Fill hot from remaining enriched rows
                for r in rows:
                    if len(hot) >= 12: break
                    k = _row_key(r)
                    if k not in taken_keys:
                        hot.append(r); taken_keys.add(k)
                # Fill gems from non-hot rows that meet the near-50/underround idea
                if len(gems) < 12:
                    # seed
                    seed: List[Dict[str, Any]] = []
                    for r in rows:
                        try:
                            near50 = float(r.get("near50Flag") or 0)
                        except Exception:
                            near50 = 0
                        try:
                            under = float(r.get("underround") or 0)
                        except Exception:
                            under = 0
                        rk = _row_key(r)
                        if (near50 >= 1 or under < 0) and rk not in taken_keys:
                            seed.append(r); taken_keys.add(rk)
                    for r in rows:
                        if len(seed) >= 12: break
                        rk = _row_key(r)
                        if rk not in taken_keys:
                            seed.append(r); taken_keys.add(rk)
                    gems = seed[:12]
        except Exception as e:
            print(f"[warn] Failed to use top12 file {top12_path.name}: {e}. Falling back to heuristic.")
            top12_path = None

    if not top12_path:
        # Previous behavior (heuristic): first 12 rows = HOT; pick gems by near-50 or underround (excluding HOT).
        hot = rows[:12]
        def rid(r: Dict[str, Any]) -> str:
            return str(r.get("id") or r.get("slug") or r.get("url") or r.get("question") or id(r))
        hot_ids = {rid(r) for r in hot}
        seed: List[Dict[str, Any]] = []
        for r in rows:
            try:
                near50 = float(r.get("near50Flag") or 0)
            except Exception:
                near50 = 0
            try:
                under = float(r.get("underround") or 0)
            except Exception:
                under = 0
            if (near50 >= 1 or under < 0) and rid(r) not in hot_ids:
                seed.append(r)
        gems = seed[:]
        if len(gems) < 12:
            for r in rows:
                if rid(r) not in hot_ids and r not in gems:
                    gems.append(r)
                    if len(gems) >= 12:
                        break
        gems = gems[:12]

    # descriptions (rotate on index/archive; snapshots freeze)
    hist = load_history()
    meta_files = sorted(META_DIR.glob("*.txt"))
    long_files = sorted(LONG_DIR.glob("*.html"))
    meta_pick = choose_rotating_file(meta_files, hist["recent_meta"])
    long_pick = choose_rotating_file(long_files, hist["recent_long"])
    short_desc = (meta_pick.read_text(encoding="utf-8").strip() if meta_pick else "Daily dashboard of Polymarket heat & overlooked opportunities.")
    long_desc_html = (long_pick.read_text(encoding="utf-8") if long_pick else "<p>Insightful commentary rotates here.</p>")
    save_history(hist)

    now = utc_now()
    human = human_date(now)

    # ---------- INDEX ----------
    head = page_head(
        title=f"Hottest Markets & Overlooked Chances on Polymarket Today — {human}",
        description=short_desc[:160],
        canonical="https://urbanpoly.com/index.html",
        og_updated=now,
    )
    top_nav = build_nav_top("index")
    snaps = sorted(SITE_DIR.glob("dashboard_*.html"))
    back_href_index = snaps[-1].name if snaps else None
    row_nav = build_nav_back_forward("index", back_href_index, None)

    tabs = """
<div class="tabs">
  <button id="tab-hot" class="active">HOT</button>
  <button id="tab-overlooked">Overlooked</button>
</div>
"""

    html_index = [
        head,
        "<div class='container'>",
        "<header class='header'>",
        "<h1>Hottest Markets &amp; Overlooked Chances on Polymarket Today</h1>",
        f"<div class='date'>{escape(human)}</div>",
        "<div class='source'>Source: Polymarket API data.</div>",
        "</header>",
        top_nav,
        row_nav,
        tabs,
        "<section id='sec-hot'>", render_grid(hot), "</section>",
        "<section id='sec-overlooked' style='display:none'>", render_grid(gems), "</section>",
        description_html(short_desc, long_desc_html),
        methodology_html(),
        "</div>",
        TABS_JS,
        page_footer(now, "index", back_href_index, None),
    ]
    (SITE_DIR / "index.html").write_text("\n".join(html_index), encoding="utf-8")

    # ---------- SNAPSHOT ----------
    snap_name = ts_for_snapshot(now)
    head_snap = page_head(
        title=f"Hottest Markets & Overlooked Chances on Polymarket — {human}",
        description=short_desc[:160],  # frozen copy
        canonical=f"https://urbanpoly.com/{snap_name}",
        og_updated=now,
    )
    top_nav_snap = build_nav_top("snapshot")
    prev_snaps = sorted(SITE_DIR.glob("dashboard_*.html"))
    back_href_snap = (prev_snaps[-1].name if prev_snaps else "archive.html")
    fwd_href_snap = "index.html"

    tabs_snap = """
<div class="tabs">
  <button id="tab-hot" class="active">HOT</button>
  <button id="tab-overlooked">Overlooked</button>
</div>
"""

    html_snap = [
        head_snap,
        "<div class='container'>",
        "<header class='header'>",
        "<h1>Hottest Markets &amp; Overlooked Chances on Polymarket</h1>",
        f"<div class='date'>{escape(human)}</div>",
        "<div class='source'>Source: Polymarket API data.</div>",
        "</header>",
        top_nav_snap,
        build_nav_back_forward("snapshot", back_href_snap, fwd_href_snap),
        tabs_snap,
        "<section id='sec-hot'>", render_grid(hot), "</section>",
        "<section id='sec-overlooked' style='display:none'>", render_grid(gems), "</section>",
        # snapshot freezes the picked descriptions
        description_html(short_desc, long_desc_html),
        methodology_html(),
        "</div>",
        TABS_JS,
        page_footer(now, "snapshot", back_href_snap, fwd_href_snap),
    ]
    (SITE_DIR / snap_name).write_text("\n".join(html_snap), encoding="utf-8")

    # ---------- ARCHIVE ----------
    head_arch = page_head(
        title="Polymarket Dashboards — Archive",
        description=short_desc[:160],  # rotates
        canonical="https://urbanpoly.com/archive.html",
        og_updated=now,
    )
    snaps_after = sorted(SITE_DIR.glob("dashboard_*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    newest_snap = snaps_after[0].name if snaps_after else None
    oldest_snap = snaps_after[-1].name if snaps_after else None

    if snaps_after:
        items_btns: List[str] = []
        for i, p in enumerate(snaps_after):
            label = p.name.replace("dashboard_", "").replace(".html", "")
            if i == 0:
                items_btns.append(
                    f"<a class='btn archive-item' href='index.html' aria-label='Open latest snapshot (live)'>"
                    f"<span class='ico'>&#128336;</span> {escape(label)} (live)"
                    f"</a>"
                )
            else:
                items_btns.append(
                    f"<a class='btn archive-item' href='{p.name}' aria-label='Open snapshot {escape(label)}'>"
                    f"{escape(label)}"
                    f"</a>"
                )
        list_html = "<div class='archive-list'>" + "\n".join(items_btns) + "</div>"
    else:
        list_html = "<p>No snapshots yet.</p>"

    html_arch = [
        head_arch,
        "<div class='container'>",
        "<header class='header'>",
        "<h1>Archive</h1>",
        f"<div class='date'>{escape(human)}</div>",
        "<div class='source'>All published snapshots, newest first.</div>",
        "</header>",
        build_nav_top("archive"),
        build_nav_back_forward("archive", None, oldest_snap),
        "<div class='section'><h2>All Snapshots</h2>",
        list_html,
        "</div>",
        description_html(short_desc, long_desc_html),
        methodology_html(),
        "</div>",
        page_footer(now, "archive", None, oldest_snap),
    ]
    (SITE_DIR / "archive.html").write_text("\n".join(html_arch), encoding="utf-8")

    # ---------- robots + sitemap ----------
    (SITE_DIR / "robots.txt").write_text("User-agent: *\nAllow: /\nSitemap: https://urbanpoly.com/sitemap.xml\n", encoding="utf-8")
    urls = ["index.html", "archive.html"] + [p.name for p in snaps_after]
    sm = ['<?xml version="1.0" encoding="UTF-8"?>','<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for u in urls:
        sm += [ "<url>", f"<loc>https://urbanpoly.com/{u}</loc>", f"<lastmod>{iso_og_time(now)}</lastmod>", "</url>" ]
    sm.append("</urlset>")
    (SITE_DIR / "sitemap.xml").write_text("\n".join(sm), encoding="utf-8")

    print("[ok] Wrote index, snapshot, archive; archive buttons + newest->index mapping applied.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
