#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds the static site (index + archive; snapshots are existing files) from an enriched CSV,
and generates a fresh OpenGraph preview image on EVERY run.

What this script does:
- Loads enriched CSV and derives "Hottest" & "Overlooked" lists
- Rotates meta description + long description from /content/meta and /content/long (no-repeat window)
- Writes index.html (home) and archive.html
- Keeps existing snapshot pages and builds archive links (latest -> index)
- Copies favicons from /assets to /site
- Generates DYNAMIC OG image (1200x630) into /site/og-preview.png each run
- Writes robots.txt and sitemap.xml

Usage:
  python scripts/build_site_from_csv.py data/polymarket_enriched_fast_YYYYMMDD_HHMMSS.csv

Requires:
  Pillow (for dynamic OG image). If missing, OG generation is skipped gracefully.
"""

from __future__ import annotations

import csv
import html as html_lib
import json
import os
import random
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# -------------------------
# Config
# -------------------------
ROOT = Path(os.getenv("GITHUB_WORKSPACE", ".")).resolve()
SITE_DIR = ROOT / "site"
DATA_DIR = ROOT / "data"
CONTENT_META_DIR = ROOT / "content" / "meta"
CONTENT_LONG_DIR = ROOT / "content" / "long"
HISTORY_PATH = DATA_DIR / "desc_history.json"
ASSETS_DIR = ROOT / "assets"  # favicons live here (optional)

NO_REPEAT_WINDOW = 30

SITE_HOST = os.getenv("PUBLIC_SITE_HOST", "https://urbanpoly.com")
SITE_NAME = "UrbanPoly"
HOME_TITLE = "Hottest Markets & Overlooked Chances on Polymarket Today"

# output files
ARCHIVE_HTML = SITE_DIR / "archive.html"
INDEX_HTML = SITE_DIR / "index.html"
SITEMAP_XML = SITE_DIR / "sitemap.xml"
ROBOTS_TXT = SITE_DIR / "robots.txt"

# favicon & OG assets (copied if present)
FAVICON_FILES = [
    "favicon-16x16.png",
    "favicon-32x32.png",
    "favicon-180x180.png",
    "favicon-512x512.png",
    "favicon.ico",
]

# We always write site/og-preview.png dynamically each run.
OG_IMAGE_BASENAME = "og-preview.png"

# -------------------------
# Data types
# -------------------------
@dataclass
class Market:
    title: str
    url: str
    embed_src: str
    why: str
    vol24: Optional[float]
    spread: Optional[float]
    ttr_days: Optional[float]
    momentum_pct24: Optional[float]

# -------------------------
# Helpers
# -------------------------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def ts_label_from_csv_name(csv_path: Path) -> str:
    m = re.search(r"(\d{8})_(\d{6})", csv_path.name)
    if not m:
        dt = datetime.now(timezone.utc)
    else:
        d, t = m.group(1), m.group(2)
        dt = datetime.strptime(d + t, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d_%H%M")

def human_date_time(dt: datetime) -> Tuple[str, str]:
    return dt.strftime("%d %B %Y"), dt.strftime("%H:%M")

def load_csv(path: Path) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out.append(row)
    return out

def to_float(s: Optional[str]) -> Optional[float]:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None

def parse_markets(rows: List[Dict[str,str]]) -> List[Market]:
    markets: List[Market] = []
    for r in rows:
        title = r.get("question") or r.get("title") or "(Untitled market)"
        url = r.get("url") or ""
        embed_src = r.get("embedSrc") or ""
        why = r.get("why") or ""
        vol24 = to_float(r.get("volume24h")) or to_float(r.get("vol24h")) or to_float(r.get("volume"))
        spread = to_float(r.get("avgSpread")) or to_float(r.get("avgSpreadProxy"))
        ttr_days = to_float(r.get("timeToResolveDays"))
        momentum = to_float(r.get("momentumPct24h"))
        markets.append(Market(title, url, embed_src, why, vol24, spread, ttr_days, momentum))
    return markets

def fmt_money(x: Optional[float]) -> str:
    if x is None: return "—"
    return f"${int(round(x)):,.0f}"

def fmt_num(x: Optional[float], nd: int = 3) -> str:
    if x is None: return "—"
    return f"{x:.{nd}f}".rstrip("0").rstrip(".")

def fmt_ttr(x: Optional[float]) -> str:
    if x is None: return "—"
    return f"{x:.1f}d".rstrip("0").rstrip(".")

def ensure_dirs():
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def read_history() -> Dict:
    if HISTORY_PATH.exists():
        try:
            return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"recent_meta": [], "recent_long": []}

def write_history(hist: Dict):
    HISTORY_PATH.write_text(json.dumps(hist, ensure_ascii=False, indent=2), encoding="utf-8")

def list_files_sorted(folder: Path, suffix: str) -> List[Path]:
    if not folder.exists():
        return []
    return sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == suffix.lower()])

def choose_rotating_file(candidates: List[Path], recent_ids: List[str]) -> Path:
    ids = [p.stem for p in candidates]
    window = recent_ids[-NO_REPEAT_WINDOW:]
    pool = [p for p in candidates if p.stem not in window]
    if not pool:
        pool = candidates[:]
    return random.choice(pool)

def copy_if_exists(src: Path, dst: Path):
    if src.exists():
        shutil.copy2(src, dst)

def copy_favicons():
    # Copy to site/ so Vercel serves them
    for name in FAVICON_FILES:
        copy_if_exists(ASSETS_DIR / name, SITE_DIR / name)

def favicon_links_html() -> str:
    # Pages expect icons at site root (we copy them there each build)
    return """
<link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png">
<link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png">
<link rel="apple-touch-icon" sizes="180x180" href="/favicon-180x180.png">
<link rel="icon" type="image/png" sizes="512x512" href="/favicon-512x512.png">
<link rel="shortcut icon" href="/favicon.ico">
""".strip()

def json_ld_website() -> Dict:
    return {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": SITE_NAME,
        "url": SITE_HOST + "/",
        "potentialAction": {
            "@type": "SearchAction",
            "target": SITE_HOST + "/archive.html?q={search_term_string}",
            "query-input": "required name=search_term_string"
        }
    }

def json_ld_webpage(title: str, canonical: str, description: str, is_collection: bool=False) -> Dict:
    return {
        "@context": "https://schema.org",
        "@type": "CollectionPage" if is_collection else "WebPage",
        "name": title,
        "url": canonical,
        "inLanguage": "en",
        "isPartOf": {
            "@type": "WebSite",
            "name": SITE_NAME,
            "url": SITE_HOST + "/"
        },
        "description": description
    }

def build_meta_block(page_title: str, description: str, canonical_path: str) -> str:
    canonical = f"{SITE_HOST}{canonical_path}"
    og_img = f"{SITE_HOST}/{OG_IMAGE_BASENAME}"  # always the freshly generated one
    return f"""
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{html_lib.escape(page_title)}</title>
<!-- build_ts: {datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M%S')} -->
<meta name="description" content="{html_lib.escape(description[:300])}" />
<meta name="keywords" content="polymarket, election odds, prediction markets, betting markets, hidden gems, dashboard" />
<link rel="canonical" href="{canonical}" />
<meta property="og:title" content="{html_lib.escape(page_title)}" />
<meta property="og:description" content="{html_lib.escape(description[:300])}" />
<meta property="og:type" content="website" />
<meta property="og:url" content="{canonical}" />
<meta property="og:image" content="{og_img}" />
<meta name="twitter:card" content="summary_large_image" />
<meta name="twitter:title" content="{html_lib.escape(page_title)}" />
<meta name="twitter:description" content="{html_lib.escape(description[:300])}" />
<meta name="twitter:image" content="{og_img}" />
{favicon_links_html()}
""".strip()

# -------------------------
# Card / grid rendering
# -------------------------
def embed_card_html(m: Market) -> str:
    caption = (
        f'<p class="cap"><a href="{html_lib.escape(m.url)}" target="_blank" rel="noopener noreferrer">'
        f'{html_lib.escape(m.title)}</a> — '
        f'<a href="{html_lib.escape(m.url)}" target="_blank" rel="noopener noreferrer">'
        f'{html_lib.escape(m.url)}</a></p>'
    )
    stat_boxes = f"""
<div class="stats">
  <div class="stat"><div class="label">24h Vol</div><div class="val">{fmt_money(m.vol24)}</div></div>
  <div class="stat"><div class="label">Avg Spread</div><div class="val">{fmt_num(m.spread,3)}</div></div>
  <div class="stat"><div class="label">Time to Resolve</div><div class="val">{fmt_ttr(m.ttr_days)}</div></div>
  <div class="stat"><div class="label">Momentum</div><div class="val">{fmt_num(m.momentum_pct24,2)}%</div></div>
</div>""".strip()
    return f"""
<div class="card">
  <div class="embedwrap" role="region" aria-label="{html_lib.escape(m.title)}">
    <iframe title="{html_lib.escape(m.title)}" aria-label="{html_lib.escape(m.title)}"
            src="{html_lib.escape(m.embed_src)}" loading="lazy"
            referrerpolicy="no-referrer-when-downgrade"></iframe>
    <noscript>{caption}</noscript>
    {caption}
  </div>
  <h3 class="mt">{html_lib.escape(m.title)}</h3>
  {stat_boxes}
</div>""".strip()

def grid_of_cards(markets: List[Market]) -> str:
    return "\n".join(embed_card_html(m) for m in markets)

def base_css() -> str:
    return """
:root { --bg:#0b0b10; --fg:#eaeaf0; --muted:#a3a8b4; --card:#141420; --border:#232336; --accent:#6ea8fe; --accent-2:#a879fe; }
* { box-sizing: border-box; }
body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background: var(--bg); color: var(--fg); }
.container { max-width: 1240px; margin: 0 auto; padding: 32px 24px 44px; }
.header { margin-bottom: 18px; }
.header h1 { margin:0; font-size: 34px; font-variant: small-caps; letter-spacing: .5px; }
.header .date { color: var(--fg); font-size: 20px; font-weight: 600; margin-top: 6px; }
.header .source { color: var(--muted); font-size: 14px; margin-top: 6px; }
.navrow { display:flex; align-items:center; gap:12px; margin: 8px 0 18px; flex-wrap: wrap; }
.btn { display:inline-flex; align-items:center; gap:8px; padding:10px 14px; border:1px solid var(--border); border-radius: 14px; background:transparent; color:var(--fg); text-decoration:none; font-weight:700; }
.btn:hover { background:#151526; }
.spacer { flex:1; }
.tabs { display:grid; grid-template-columns: 1fr 1fr; border:1px solid var(--border); border-radius: 14px; overflow:hidden; }
.tabs a { text-align:center; text-decoration:none; padding:12px; font-weight:700; color:var(--fg); }
.tabs a.active { background: var(--accent); color:#0b0b10; }
.grid { display:grid; grid-template-columns: repeat(3,minmax(0,1fr)); gap:16px; }
@media (max-width: 980px){ .grid{ grid-template-columns: repeat(2,minmax(0,1fr)); } }
@media (max-width: 640px){ .grid{ grid-template-columns: 1fr; } }

.card { border:1px solid var(--border); border-radius:16px; padding:16px; background:var(--card); }
.embedwrap { border-radius:12px; overflow: hidden; border:1px solid var(--border); background:#0f0f18; }
.embedwrap iframe { width:100%; height:240px; border:0; display:block; }
.cap { font-size: 12px; color: #a3a8b4; margin: 10px 12px; }
.cap a { color: inherit; text-decoration: underline; text-underline-offset: 2px; }
.mt { margin-top: 10px; }
.stats { display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:10px; margin-top:12px; }
.stat { border:1px solid var(--border); border-radius:12px; padding:10px; }
.stat .label { font-size: 12px; color: var(--muted); }
.stat .val { font-size: 16px; font-weight: 700; }

.section { border:1px dashed #6ea8fe55; border-radius:18px; padding:18px; margin-top:12px; background:#121223; }
.section h3 { margin:0 0 8px 0; font-size:20px; }
.section p { margin:8px 0; color: var(--fg); }
.footer { margin-top: 22px; color: var(--muted); font-size: 13px; text-align:center; }
.small { font-size: 12px; color: var(--muted); }
""".strip()

def render_nav(home: bool, archive: bool, back_href: Optional[str], fwd_href: Optional[str]) -> str:
    home_btn = "" if home else '<a class="btn" href="/index.html">← Home</a>'
    archive_btn = "" if archive else '<a class="btn" href="/archive.html">Archive →</a>'
    forward_btn = f'<a class="btn" href="{html_lib.escape(fwd_href)}">← Forward</a>' if fwd_href else ""
    back_btn = f'<a class="btn" href="{html_lib.escape(back_href)}">Back →</a>' if back_href else ""
    return f"""
<div class="navrow">
  {home_btn}
  {forward_btn}
  <span class="spacer"></span>
  {back_btn}
  {archive_btn}
</div>""".strip()

def render_methodology_and_desc(methodology_html: str, long_html: str, updated_txt: str) -> str:
    return f"""
<div class="section">
  <h3>Methodology</h3>
  {methodology_html}
</div>
<div class="section">
  <h3>Description</h3>
  {long_html}
</div>
<div class="footer">updated {updated_txt} UTC • Source: Polymarket API data • Not financial advice. DYOR.</div>
""".strip()

def build_tabs(hot_active: bool) -> str:
    return f"""
<div class="tabs">
  <a href="#hot" class="{ 'active' if hot_active else '' }">Hottest</a>
  <a href="#overlooked" class="{ '' if hot_active else 'active' }">Overlooked</a>
</div>""".strip()

def list_existing_snapshots() -> List[Path]:
    return sorted(SITE_DIR.glob("dashboard_*.html"))

def pick_descriptions() -> Tuple[str, str, str, str]:
    meta_files = list_files_sorted(CONTENT_META_DIR, ".txt")
    long_files = list_files_sorted(CONTENT_LONG_DIR, ".html")
    hist = read_history()

    if not meta_files:
        meta_desc = "Live Polymarket dashboards: hottest & overlooked markets, updated every 6 hours."
        meta_name = "builtin"
    else:
        meta_pick = choose_rotating_file(meta_files, hist.get("recent_meta", []))
        meta_desc = meta_pick.read_text(encoding="utf-8").strip()
        meta_name = meta_pick.stem
        hist.setdefault("recent_meta", [])
        hist["recent_meta"].append(meta_name)
        hist["recent_meta"] = hist["recent_meta"][-(NO_REPEAT_WINDOW*2):]

    if not long_files:
        long_html = "<p>Polymarket overview dashboards updated every 6 hours.</p>"
        long_name = "builtin"
    else:
        long_pick = choose_rotating_file(long_files, hist.get("recent_long", []))
        long_html = long_pick.read_text(encoding="utf-8")
        long_name = long_pick.stem
        hist.setdefault("recent_long", [])
        hist["recent_long"].append(long_name)
        hist["recent_long"] = hist["recent_long"][-(NO_REPEAT_WINDOW*2):]

    write_history(hist)
    return meta_desc, long_html, meta_name, long_name

def split_hot_overlooked(markets: List[Market]) -> Tuple[List[Market], List[Market]]:
    hot = sorted(markets, key=lambda m: (-(m.vol24 or 0), (m.ttr_days if m.ttr_days is not None else 9999)))[:12]
    pool = [m for m in markets if (m.vol24 or 0) > 1000 and (m.vol24 or 0) < 100000]
    gems = sorted(pool, key=lambda m: (m.spread if m.spread is not None else 999, (m.ttr_days if m.ttr_days is not None else 9999)))[:12]
    return hot, gems

# -------------------------
# Dynamic OG image generator
# -------------------------
def truncate(text: str, max_chars: int) -> str:
    text = text.strip()
    return text if len(text) <= max_chars else text[:max_chars-1].rstrip() + "…"

def generate_og_image(hot: List[Market], date_str: str, time_str: str):
    """
    Writes /site/og-preview.png (1200x630) with:
    - Title, subtitle
    - Timestamp (UTC)
    - Top 2 HOT market titles + small chips for 24h, spread, TTR
    """
    try:
        from PIL import Image, ImageDraw, ImageFont  # type: ignore
    except Exception as e:
        print(f"[og] Pillow not available ({e}); skipping OG image generation.")
        return

    W, H = 1200, 630
    img = Image.new("RGB", (W, H), color=(11, 11, 16))
    d = ImageDraw.Draw(img)

    # Gradient bar
    for i in range(W):
        r = int(110 + (168-110) * (i/W))
        g = int(168 + (121-168) * (i/W))
        b = 254
        d.line([(i, 0), (i, 14)], fill=(r, g, b))

    # Fonts (fallback to default)
    def load_font(name: str, size: int):
        try:
            return ImageFont.truetype(name, size)
        except:
            try:
                return ImageFont.truetype("DejaVuSans-Bold.ttf" if "Bold" in name else "DejaVuSans.ttf", size)
            except:
                return ImageFont.load_default()

    font_title = load_font("DejaVuSans-Bold.ttf", 60)
    font_sub   = load_font("DejaVuSans.ttf", 38)
    font_time  = load_font("DejaVuSans.ttf", 30)
    font_row   = load_font("DejaVuSans.ttf", 28)
    font_chip  = load_font("DejaVuSans.ttf", 22)
    font_brand = load_font("DejaVuSans.ttf", 24)

    # Header
    d.text((W//2, 110), "Polymarket Dashboard", fill=(234,234,240), font=font_title, anchor="mm")
    d.text((W//2, 170), "Hottest & Overlooked Markets", fill=(168,121,254), font=font_sub, anchor="mm")
    d.text((W//2, 215), f"{date_str} • {time_str} UTC", fill=(163,168,180), font=font_time, anchor="mm")

    # Rows for top 2
    y = 280
    x_pad = 80
    row_gap = 90
    chip_gap = 10

    for i, m in enumerate(hot[:2]):
        title = truncate(m.title, 70)
        d.text((x_pad, y + i*row_gap), f"• {title}", fill=(234,234,240), font=font_row, anchor="ls")

        # chips
        cx = x_pad
        cy = y + i*row_gap + 34
        chips = [f"24h {fmt_money(m.vol24)}", f"spread {fmt_num(m.spread,3)}", f"TTR {fmt_ttr(m.ttr_days)}"]
        for chip in chips:
            w = d.textlength(chip, font=font_chip)
            h = 26
            box_w = int(w) + 20
            d.rounded_rectangle([cx, cy, cx+box_w, cy+h+10], radius=8, outline=(100,100,120), width=2, fill=None)
            d.text((cx+10, cy+5), chip, fill=(180,184,196), font=font_chip, anchor="ls")
            cx += box_w + chip_gap

    # Footer brand
    d.text((W//2, H-40), "urbanpoly.com", fill=(120,120,130), font=font_brand, anchor="mm")

    out = SITE_DIR / OG_IMAGE_BASENAME
    try:
        img.save(out, format="PNG")
        print(f"[og] Wrote {out}")
    except Exception as e:
        print(f"[og] Failed to write OG image: {e}")

# -------------------------
# HTML builders
# -------------------------
def build_index_html(hot: List[Market], gems: List[Market], date_str: str, time_str: str,
                     meta_desc: str, long_html: str) -> str:
    page_title = f"{HOME_TITLE} — {date_str} • {time_str}"
    head = build_meta_block(page_title, meta_desc, "/index.html")
    ld = [json_ld_website(), json_ld_webpage(page_title, f"{SITE_HOST}/index.html", meta_desc, is_collection=True)]
    ld_json = json.dumps(ld, ensure_ascii=False, indent=2)

    # NOTE: escape curly braces in JS within an f-string using doubled {{ }}
    return f"""<!doctype html><html lang='en'>
<head>
{head}
<script type="application/ld+json">
{ld_json}
</script>
<style>{base_css()}</style>
</head>
<body>
  <div class="container">
    <header class="header">
      <h1>{html_lib.escape(HOME_TITLE)}</h1>
      <div class="date">{date_str} • {time_str} UTC</div>
      <div class="source small">Source: Polymarket API data.</div>
    </header>

    {render_nav(home=True, archive=False, back_href="#", fwd_href=None)}

    {build_tabs(hot_active=True)}

    <section id="hot" class="grid" style="margin-top:12px">
      {grid_of_cards(hot)}
    </section>

    <section id="overlooked" class="grid" style="display:none; margin-top:12px">
      {grid_of_cards(gems)}
    </section>

    {render_methodology_and_desc(
        methodology_html="<ul><li><b>Hottest:</b> Prioritizes 24h volume, tighter spreads, and sooner time-to-resolve.</li><li><b>Overlooked:</b> Prefers moderate volume with tighter spreads and sooner resolution.</li><li><b>Why line:</b> we summarize each card with 24h volume, spread, and TTR metrics.</li></ul>",
        long_html=long_html,
        updated_txt=f"{date_str} • {time_str}"
    )}
  </div>

<script>
(function(){{
  function show(id){{ 
    document.querySelector('#hot').style.display = (id==='hot')?'grid':'none';
    document.querySelector('#overlooked').style.display = (id==='overlooked')?'grid':'none';
    var tabs = document.querySelectorAll('.tabs a');
    tabs[0].classList.toggle('active', id==='hot');
    tabs[1].classList.toggle('active', id==='overlooked');
  }}
  document.querySelectorAll('.tabs a')[0].addEventListener('click', function(e){{ e.preventDefault(); show('hot'); }});
  document.querySelectorAll('.tabs a')[1].addEventListener('click', function(e){{ e.preventDefault(); show('overlooked'); }});
  show('hot');
}})();
</script>

</body></html>
""".strip()

def build_snapshot_html(hot: List[Market], gems: List[Market], date_str: str, time_str: str,
                        ts_label: str, meta_desc: str, long_html: str,
                        prev_href: Optional[str], next_href: Optional[str]) -> str:
    title = f"Hottest Markets & Overlooked Chances on Polymarket — {date_str} • {time_str}"
    path = f"/dashboard_{ts_label}.html"
    head = build_meta_block(title, meta_desc, path)
    ld = [json_ld_website(), json_ld_webpage(title, f"{SITE_HOST}{path}", meta_desc, is_collection=True)]
    ld_json = json.dumps(ld, ensure_ascii=False, indent=2)

    return f"""<!doctype html><html lang='en'>
<head>
{head}
<script type="application/ld+json">
{ld_json}
</script>
<style>{base_css()}</style>
</head>
<body>
  <div class="container">
    <header class="header">
      <h1>Hottest Markets & Overlooked Chances on Polymarket</h1>
      <div class="date">{date_str} • {time_str} UTC</div>
      <div class="source small">Source: Polymarket API data.</div>
    </header>

    {render_nav(home=False, archive=False, back_href=prev_href, fwd_href=next_href)}

    {build_tabs(hot_active=True)}

    <section id="hot" class="grid" style="margin-top:12px">
      {grid_of_cards(hot)}
    </section>

    <section id="overlooked" class="grid" style="display:none; margin-top:12px">
      {grid_of_cards(gems)}
    </section>

    {render_methodology_and_desc(
        methodology_html="<ul><li><b>Hottest:</b> Prioritizes 24h volume, tighter spreads, and sooner time-to-resolve.</li><li><b>Overlooked:</b> Prefers moderate volume with tighter spreads and sooner resolution.</li><li><b>Why line:</b> we summarize each card with 24h volume, spread, and TTR metrics.</li></ul>",
        long_html=long_html,
        updated_txt=f"{date_str} • {time_str}"
    )}
  </div>

<script>
(function(){{
  function show(id){{ 
    document.querySelector('#hot').style.display = (id==='hot')?'grid':'none';
    document.querySelector('#overlooked').style.display = (id==='overlooked')?'grid':'none';
    var tabs = document.querySelectorAll('.tabs a');
    tabs[0].classList.toggle('active', id==='hot');
    tabs[1].classList.toggle('active', id==='overlooked');
  }}
  document.querySelectorAll('.tabs a')[0].addEventListener('click', function(e){{ e.preventDefault(); show('hot'); }});
  document.querySelectorAll('.tabs a')[1].addEventListener('click', function(e){{ e.preventDefault(); show('overlooked'); }});
  show('hot');
}})();
</script>

</body></html>
""".strip()

def build_archive_html(entries: List[Tuple[str, str]]) -> str:
    page_title = "Polymarket Dashboards — Archive"
    meta_desc = "Browse all UrbanPoly snapshots (hottest & overlooked Polymarket markets) updated every 6 hours."
    head = build_meta_block(page_title, meta_desc, "/archive.html")
    ld = [json_ld_website(), json_ld_webpage(page_title, f"{SITE_HOST}/archive.html", meta_desc, is_collection=True)]
    ld_json = json.dumps(ld, ensure_ascii=False, indent=2)

    links_html = "\n".join(
        f'<p><a class="btn" style="display:block; text-align:center" href="{html_lib.escape(href)}">{html_lib.escape(label)}</a></p>'
        for href, label in entries
    )

    # rotate a long description here too
    _, long_html, _, _ = pick_descriptions()

    return f"""<!doctype html><html lang='en'>
<head>
{head}
<script type="application/ld+json">
{ld_json}
</script>
<style>{base_css()}</style>
</head>
<body>
  <div class="container">
    <header class="header">
      <h1>Archive</h1>
      <div class="date">All recent snapshots</div>
    </header>

    {render_nav(home=False, archive=True, back_href=None, fwd_href="/index.html")}

    <div class="section">
      <h3>Description</h3>
      {long_html}
    </div>

    <div style="margin-top:16px">{links_html}</div>

    <div class="footer">Not financial advice. DYOR.</div>
  </div>
</body></html>
""".strip()

def write_robots_and_sitemap(snapshot_paths: List[Path]):
    ROBOTS_TXT.write_text(
        "User-agent: *\n"
        "Allow: /\n"
        f"Sitemap: {SITE_HOST}/sitemap.xml\n",
        encoding="utf-8"
    )
    urls = [f"{SITE_HOST}/index.html", f"{SITE_HOST}/archive.html"] + [
        f"{SITE_HOST}/{p.name}" for p in snapshot_paths
    ]
    items = "\n".join(
        f"<url><loc>{html_lib.escape(u)}</loc><lastmod>{utcnow_iso()}</lastmod></url>"
        for u in urls
    )
    SITEMAP_XML.write_text(
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n{items}\n</urlset>\n',
        encoding="utf-8"
    )

# -------------------------
# Main
# -------------------------
def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: build_site_from_csv.py path/to/enriched.csv", file=sys.stderr)
        return 2

    csv_path = Path(sys.argv[1]).resolve()
    print(f"[builder] CLI CSV arg detected: {csv_path}")
    if not csv_path.exists():
        print(f"ERROR: CSV path does not exist: {csv_path}", file=sys.stderr)
        return 2

    ensure_dirs()
    copy_favicons()

    rows = load_csv(csv_path)
    markets = parse_markets(rows)
    hot, gems = split_hot_overlooked(markets)

    # Descriptions
    meta_desc, long_html, meta_name, long_name = pick_descriptions()
    print(f"[builder] Using meta={meta_name}, long={long_name}")

    # Timestamps (from CSV name if possible)
    m = re.search(r"(\d{8})_(\d{6})", csv_path.name)
    if m:
        d, t = m.group(1), m.group(2)
        dt = datetime.strptime(d + t, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    date_str, time_str = human_date_time(dt)
    ts_label = ts_label_from_csv_name(csv_path)

    # Existing snapshots for archive + nav context
    existing_snaps = list_existing_snapshots()
    label_to_href = {re.search(r"dashboard_(\d{4}-\d{2}-\d{2}_\d{4})\.html", p.name).group(1): f"/{p.name}"
                     for p in existing_snaps
                     if re.search(r"dashboard_\d{4}-\d{2}-\d{2}_\d{4}\.html", p.name)}

    # 1) Generate DYNAMIC OG image for this run
    generate_og_image(hot, date_str, time_str)  # writes site/og-preview.png

    # 2) Write index.html
    INDEX_HTML.write_text(build_index_html(hot, gems, date_str, time_str, meta_desc, long_html), encoding="utf-8")

    # 3) Write archive.html (latest entry points to index)
    all_labels_sorted = sorted(label_to_href.keys(), reverse=True)
    entries: List[Tuple[str,str]] = []
    if all_labels_sorted:
        newest_label = all_labels_sorted[0]
        latest_dt = datetime.strptime(newest_label, "%Y-%m-%d_%H%M")
        entries.append(("/index.html", f"Latest — {latest_dt.strftime('%d %B %Y • %H:%M')} UTC"))
        for lbl in all_labels_sorted[1:]:
            dt_lbl = datetime.strptime(lbl, "%Y-%m-%d_%H%M")
            entries.append((f"/dashboard_{lbl}.html", f"{dt_lbl.strftime('%d %B %Y • %H:%M')} UTC"))
    ARCHIVE_HTML.write_text(build_archive_html(entries), encoding="utf-8")

    # 4) Robots + sitemap
    write_robots_and_sitemap(existing_snaps)

    print("[ok] Wrote site/index.html, site/archive.html, site/sitemap.xml, dynamic site/og-preview.png")
    return 0

if __name__ == "__main__":
    sys.exit(main())
