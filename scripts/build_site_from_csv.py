#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build static HTML pages (Home, Snapshot, Archive) from an enriched Polymarket CSV.

Behavior:
- Picks ONE description .txt per run from content/descriptions/ (private, not published),
  avoiding repeats in the last 30 runs via data/desc_history.json.
- Uses that description to:
  * write a NEW snapshot (frozen with this run’s description),
  * write index.html (Home) with the same latest description,
  * write archive.html with the same latest description.
- Older snapshots are NOT rewritten, so their description stays fixed.

Extras:
- Google Tag Manager (GTM) on every page (script in <head>, noscript right after <body>).
- Navigation order:
  LEFT  = "← Forward" (to newer / Home)
  RIGHT = "Back →"    (to older / Archive)
- Home: Back only (right) → latest snapshot
- Snapshot: Forward (left) + Back (right)
- Archive: Back only (right) → latest snapshot
- Footer includes subtle system info (CSV filename + timestamp).
"""

import sys, csv, math, re, html as html_lib, json, random
from pathlib import Path
from datetime import datetime

import os
# hard-stop in CI if no CSV path is provided
if os.environ.get("GITHUB_ACTIONS") and len(sys.argv) <= 1:
    print("[builder] ERROR: In CI you must pass a CSV path, e.g. build_site_from_csv.py /path/to.csv", flush=True)
    raise SystemExit(2)

# --- CLI CSV resolver ---
def resolve_csv_path_from_cli_or_latest() -> Path:
    if len(sys.argv) > 1:
        p = Path(sys.argv[1]).resolve()
        print(f"[builder] CLI CSV arg detected: {p}")
        if not p.exists():
            print(f"[error] CSV not found at CLI path: {p}", file=sys.stderr)
            sys.exit(2)
        return p
    data_dir = Path("data"); data_dir.mkdir(exist_ok=True)
    csvs = sorted(data_dir.glob("*.csv"))
    if csvs:
        chosen = csvs[-1].resolve()
        print(f"[builder] No CLI arg. Using newest in data/: {chosen}")
        return chosen
    # (optional) write sample if nothing exists
    sample = data_dir / "sample_enriched_20250101_000000.csv"
    if not sample.exists():
        with sample.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["question","why","embedSrc","volume24h","avgSpread","timeToResolveDays","momentumPct24h","near50Flag","underround"])
            w.writerow(["Will Bitcoin hit $100k by 2025?","Crypto trend","https://embed.polymarket.com/market.html?market=bitcoin-100k&features=volume&theme=light",250000,0.04,120,2.5,1,-0.02])
            w.writerow(["Will Trump win 2024 election?","Politics","https://embed.polymarket.com/market.html?market=trump-2024&features=volume&theme=light",450000,0.03,60,1.2,0.8,-0.01])
    print(f"[builder] No CSVs found. Wrote sample: {sample.resolve()}")
    return sample.resolve()

def main():
    csv_path = resolve_csv_path_from_cli_or_latest()  # uses sys.argv[1] if provided
    print(f"[builder] Using CSV: {csv_path}")

  
# =========================
# Config
# =========================
GTM_ID = "GTM-WJ2H3V7F"                      # Your GTM container ID
DESCRIPTIONS_DIR = Path("content/descriptions")  # Not served; keep outside 'site/'
DESC_HISTORY_PATH = Path("data/desc_history.json")
DESC_HISTORY_WINDOW = 30                     # Avoid reusing same desc for last N runs

# =========================
# CSV + numeric helpers
# =========================
def read_csv_rows(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def fnum(x, default=None):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def fmt_money(v):
    try:
        return f"${int(float(v)):,}"
    except Exception:
        return "—"

def choose_spread(row):
    s = fnum(row.get("avgSpread"))
    if s is None or s <= 0:
        s = fnum(row.get("avgSpreadProxy"))
    return s

# =========================
# Ranking
# =========================
def score_hot(row):
    vol24 = fnum(row.get("volume24h"), 0.0)
    volL  = fnum(row.get("volume"), 0.0)
    vol = vol24 if vol24 and vol24 > 0 else volL
    spread = fnum(row.get("avgSpread"))
    ttr = fnum(row.get("timeToResolveDays"), 365.0)
    spread_component = (1.0/(1.0+(spread*100.0))) if (spread is not None and spread >= 0) else 0.5
    ttr_component = (1.0/(1.0+(ttr/30.0)))
    return (math.log1p(vol)*1.3) + (spread_component*2.0) + (ttr_component*1.2)

def score_gems(row):
    vol24 = fnum(row.get("volume24h"), 0.0)
    near50 = fnum(row.get("near50Flag"), 0.0)
    under = fnum(row.get("underround"))
    ttr = fnum(row.get("timeToResolveDays"), 365.0)
    vol_penalty = -1.0 if vol24 > 200000 else (-0.4 if vol24 < 1500 else 0.0)
    under_bonus = 0.0
    if under is not None:
        under_bonus = min(0.6, max(-1.0, -under*2.2))  # more negative underround → more bonus
    ttr_bonus = 1.0/(1.0 + ttr/45.0)
    return (near50*2.2) + under_bonus + ttr_bonus + vol_penalty

def top_n(rows, n, keyfn):
    scored = [(keyfn(r), r) for r in rows]
    scored.sort(key=lambda t: t[0], reverse=True)
    return [r for _, r in scored[:n]]

# =========================
# Timestamps from CSV name
# =========================
def ts_from_filename(name: str):
    """
    Expect: ..._YYYYMMDD_HHMMSS.csv
    Return: (datetime, "YYYY-MM-DD_HHMM")
    """
    m = re.search(r"(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})", name)
    if not m:
        dt = datetime.utcnow()
        return dt, dt.strftime("%Y-%m-%d_%H%M")
    dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                  int(m.group(4)), int(m.group(5)), int(m.group(6)))
    return dt, dt.strftime("%Y-%m-%d_%H%M")

def human_dt(dt: datetime):
    return dt.strftime("%d %B %Y"), dt.strftime("%H:%M")

# =========================
# Description sourcing
# =========================
def load_desc_history(path: Path) -> list:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

def save_desc_history(path: Path, hist: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    # store only last DESC_HISTORY_WINDOW entries
    path.write_text(json.dumps(hist[-DESC_HISTORY_WINDOW:], ensure_ascii=False), encoding="utf-8")

def pick_description_file() -> Path | None:
    DESCRIPTIONS_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted([p for p in DESCRIPTIONS_DIR.glob("*.txt") if p.is_file()])
    if not files:
        return None
    history = load_desc_history(DESC_HISTORY_PATH)
    recent = set(history[-DESC_HISTORY_WINDOW:]) if history else set()
    eligible = [p for p in files if p.name not in recent]
    choices = eligible if eligible else files
    chosen = random.choice(choices)
    history.append(chosen.name)
    save_desc_history(DESC_HISTORY_PATH, history)
    return chosen

def render_description_html(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    paras = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    parts = [f"<p>{html_lib.escape(p)}</p>" for p in paras]
    body = ''.join(parts) if parts else '<p>(No description provided.)</p>'
    return f"""
  <div class="section description">
    <h3>Description</h3>
    {body}
  </div>
""".strip()

# =========================
# HTML building
# =========================
BASE_CSS = """
:root { --bg:#0b0b10; --fg:#eaeaf0; --muted:#a3a8b4; --card:#141420; --border:#232336; --accent:#6ea8fe; --accent-2:#a879fe; }
* { box-sizing: border-box; }
body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background: var(--bg); color: var(--fg); }
.container { max-width: 1240px; margin: 0 auto; padding: 32px 24px 40px; }
.header { margin-bottom: 14px; }
.header h1 { margin:0; font-size: 34px; font-variant: small-caps; letter-spacing: 0.5px; }
.header .date { color: var(--fg); font-size: 20px; font-weight: 600; margin-top: 6px; }
.header .source { color: var(--muted); font-size: 14px; margin-top: 6px; }
.tabs { display: grid; grid-template-columns: 1fr 1fr; border:1px solid var(--border); border-radius: 14px; overflow: hidden; margin: 12px 0 22px; }
.tabs button { background: transparent; color: var(--fg); padding: 14px 12px; border:0; cursor:pointer; font-weight:700; font-size: 14px; }
.tabs button.active { background: var(--accent); color: #0b0b10; }
.tab-panel { display: none; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 16px; align-items: stretch; }
.tab-panel.active { display: grid; }
.card { background: var(--card); border:1px solid var(--border); border-radius: 16px; overflow: hidden; display:flex; flex-direction: column; height: 100%; }
.iframe-wrap { position: relative; width: 100%; padding-top: 56.25%; background: #101018; }
.iframe-wrap iframe { position: absolute; top:0; left:0; width:100%; height:100%; border:0; display:block; }
.card-body { padding: 14px; display:flex; flex-direction: column; gap: 8px; flex:1 1 auto; }
.title { margin: 0; font-size: 16px; line-height: 1.3; min-height: 2.6rem; }
.why { margin: 0; color: var(--muted); font-size: 13px; min-height: 1.6rem; display:flex; align-items:center; }
.metrics { display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 8px; font-size: 12px; margin-top: auto; }
.metrics div { background:#0f0f18; border:1px solid var(--border); border-radius: 10px; padding: 8px; }
.metrics span { display:block; color: var(--muted); font-size: 11px; margin-bottom: 4px; }
.section { margin-top: 40px; padding-top: 16px; }
.section h3 { margin: 0 0 10px; font-size: 20px; color: var(--fg); }
.section p, .section li { color: var(--muted); font-size: 15px; line-height: 1.7; }
.methodology { border: 1px dashed var(--accent-2); background: rgba(168,121,254,0.08); border-radius: 16px; padding: 18px; margin-bottom: 36px; }
.nav-block { text-align:center; margin: 12px 0; }
.nav-buttons.tabs-like { display: grid; grid-template-columns: 1fr 1fr; border:1px solid var(--border); border-radius: 14px; overflow: hidden; }
.nav-buttons.tabs-like a { display:block; padding:14px 12px; color: var(--fg); text-decoration:none; font-weight:700; font-size:14px; }
.nav-buttons.tabs-like a:hover { background:#232336; }
.footer { color: var(--fg); font-size: 13px; text-align:center; margin-top: 28px; padding-top: 12px; border-top: 1px solid var(--border); font-weight: 700; }
.footer .system { margin-top: 6px; color: var(--muted); font-size: 12px; }
.archive-list { list-style: none; padding: 0; margin: 0; }
.archive-list li { padding: 10px 12px; border:1px solid var(--border); border-radius: 12px; margin-bottom: 10px; background:#141420; }
.archive-list a { color: var(--fg); text-decoration: none; font-weight: 600; }
.archive-list a:hover { text-decoration: underline; }
"""

BASE_JS = """
function showTab(name) {
  document.getElementById('tab-hot').classList.remove('active');
  document.getElementById('tab-gems').classList.remove('active');
  document.getElementById('hot').classList.remove('active');
  document.getElementById('gems').classList.remove('active');
  if (name==='hot') { document.getElementById('tab-hot').classList.add('active'); document.getElementById('hot').classList.add('active'); }
  else { document.getElementById('tab-gems').classList.add('active'); document.getElementById('gems').classList.add('active'); }
}
showTab('hot');
"""

METHODOLOGY_HTML = """
  <div class="section methodology">
    <h3>Methodology</h3>
    <ul>
      <li><strong>Hottest:</strong> Prioritizes 24h volume, tighter spreads, and sooner time-to-resolve.</li>
      <li><strong>Overlooked:</strong> Prefers near-50% binary midpoints, negative underround, moderate 24h volume, and sooner resolution.</li>
      <li><strong>Why line:</strong> Displays quick cues like “24h $X • TTR Yd” pulled from the POLYMARKET API.</li>
    </ul>
  </div>
"""

def card_html(row):
    title = html_lib.escape(row.get("question","(Untitled)"))
    why = html_lib.escape((row.get("why") or "").strip())
    embed = row.get("embedSrc","")
    vol_txt = fmt_money(fnum(row.get("volume24h")))
    spread = choose_spread(row)
    spread_txt = f"{spread:.3f}" if spread is not None else "—"
    ttr = fnum(row.get("timeToResolveDays"))
    ttr_txt = f"{ttr:.1f}d" if ttr is not None else "—"
    mom = fnum(row.get("momentumPct24h"))
    mom_txt = f"{mom:.2f}%" if mom is not None else "—"
    iframe = (f'<div class="iframe-wrap"><iframe title="{title}" src="{html_lib.escape(embed)}" frameborder="0"></iframe></div>'
              if embed else '<div class="iframe-wrap">No embedSrc</div>')
    return f"""
<article class="card">
  {iframe}
  <div class="card-body">
    <h3 class="title">{title}</h3>
    <p class="why">{why}</p>
    <div class="metrics">
      <div><span>24h Vol</span><strong>{vol_txt}</strong></div>
      <div><span>Avg Spread</span><strong>{spread_txt}</strong></div>
      <div><span>Time to Resolve</span><strong>{ttr_txt}</strong></div>
      <div><span>Momentum</span><strong>{mom_txt}</strong></div>
    </div>
  </div>
</article>"""

def tab_html(name, rows):
    return f'<section id="{name}" class="tab-panel">{"".join(card_html(r) for r in rows)}</section>'

def nav_block(forward_href=None, back_href=None):
    """
    Two columns:
      LEFT  = "← Forward" (to newer/Home)
      RIGHT = "Back →"    (to older/Archive)
    """
    left = (f'<a href="{html_lib.escape(forward_href)}">← Forward</a>'
            if forward_href else '<a style="visibility:hidden">—</a>')
    right = (f'<a href="{html_lib.escape(back_href)}">Back →</a>'
             if back_href else '<a style="visibility:hidden">—</a>')
    return f'<div class="nav-block"><div class="nav-buttons tabs-like">{left}{right}</div></div>'

def gtm_head(gtm_id: str) -> str:
    return f"""
<!-- Google Tag Manager -->
<script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{\'gtm.start\':
new Date().getTime(),event:\'gtm.js\'}});var f=d.getElementsByTagName(s)[0],
j=d.createElement(s),dl=l!=\'dataLayer\'?\'&l=\'+l:\'\';j.async=true;j.src=
'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
}})(window,document,'script','dataLayer','{gtm_id}');</script>
<!-- End Google Tag Manager -->
""".strip()

def gtm_body(gtm_id: str) -> str:
    return f"""
<!-- Google Tag Manager (noscript) -->
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id={gtm_id}"
height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
<!-- End Google Tag Manager (noscript) -->
""".strip()

def build_dashboard_html(title, subtitle_line, hot_rows, gem_rows,
                         header_nav_html, footer_nav_html, sysline,
                         description_html, gtm_id: str):
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{html_lib.escape(title)} — {subtitle_line}</title>
{gtm_head(gtm_id)}
<style>{BASE_CSS}</style>
</head>
<body>
{gtm_body(gtm_id)}
<div class="container">
  <div class="header">
    <h1>{html_lib.escape(title)}</h1>
    <div class="date">{subtitle_line}</div>
    <div class="source">Source: Polymarket API data.</div>
    {header_nav_html}
    <div class="tabs">
      <button id="tab-hot" class="active" onclick="showTab('hot')">HOT</button>
      <button id="tab-gems" onclick="showTab('gems')">Overlooked</button>
    </div>
  </div>
  {tab_html('hot', hot_rows)}
  {tab_html('gems', gem_rows)}
  {METHODOLOGY_HTML}
  {description_html}
  {footer_nav_html}
  <div class="footer">Not financial advice. DYOR.<div class="system">{sysline}</div></div>
</div>
<script>{BASE_JS}</script>
</body>
</html>"""

# =========================
# Main
# =========================
def main():
    csv_path = resolve_csv_path_from_cli_or_latest()
    print(f"[builder] Using CSV: {csv_path}")

    rows = read_csv_rows(csv_path)
    hot_rows = top_n(rows, 12, score_hot)
    gem_rows = top_n(rows, 12, score_gems)

    # Timestamp from CSV filename
    run_dt, ts_label = ts_from_filename(csv_path.name)
    d_human, t_human = human_dt(run_dt)

    # Choose description for THIS run (used by snapshot + index + archive for this run)
    chosen_path = pick_description_file()
    if chosen_path and chosen_path.exists():
        desc_text = chosen_path.read_text(encoding="utf-8")
    else:
        # Fallback text if no files present
        desc_text = (
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus. "
            "Suspendisse lectus tortor, dignissim sit amet, adipiscing nec, ultricies sed, dolor."
        )
    description_html = render_description_html(desc_text)

    # Output directory
    out_dir = Path("site"); out_dir.mkdir(exist_ok=True)
    snapshot_name = f"dashboard_{ts_label}.html"

    # Build snapshot list (include the one we’re about to write for link-chaining)
    existing = sorted(out_dir.glob("dashboard_*.html"))

    if (out_dir / snapshot_name) not in existing:
        existing.append(out_dir / snapshot_name)

    def snap_key(p: Path):
        m = re.search(r"dashboard_(\d{4}-\d{2}-\d{2})_(\d{4})", p.name)
        if not m:
            return p.name
        return datetime.strptime(m.group(1)+m.group(2), "%Y-%m-%d%H%M")

    existing.sort(key=snap_key)
    names = [p.name for p in existing]
    idx = names.index(snapshot_name)

    older = names[idx-1] if idx-1 >= 0 else None      # Back → older (or Archive)
    newer = names[idx+1] if idx+1 < len(names) else None  # Forward → newer (or Home)
    latest_snapshot = names[-1] if names else snapshot_name

    # Links for snapshot page
    forward_href = newer or "index.html"
    back_href    = older or "archive.html"

    sysline = f"Build: {csv_path.name} • Generated from run timestamp {ts_label}"

    # 1) Write snapshot (frozen with THIS run's description)
    snap_title = "Hottest Markets & Overlooked Chances on Polymarket"
    snap_sub = f"{d_human} • {t_human}"
    snap_header_nav = nav_block(forward_href=forward_href, back_href=back_href)
    snap_footer_nav = nav_block(forward_href=forward_href, back_href=back_href)
    snapshot_html = build_dashboard_html(
        snap_title, snap_sub, hot_rows, gem_rows,
        snap_header_nav, snap_footer_nav, sysline,
        description_html, GTM_ID
    )
    (out_dir / snapshot_name).write_text(snapshot_html, encoding="utf-8")

    # 2) Write Home (Back only → latest snapshot), with THIS run's (latest) description
    home_title = "Hottest Markets & Overlooked Chances on Polymarket Today"
    home_sub   = f"{d_human} • {t_human}"
    home_header_nav = nav_block(forward_href=None, back_href=latest_snapshot)
    home_footer_nav = nav_block(forward_href=None, back_href=latest_snapshot)
    home_html = build_dashboard_html(
        home_title, home_sub, hot_rows, gem_rows,
        home_header_nav, home_footer_nav, sysline,
        description_html, GTM_ID
    )
    (out_dir / "index.html").write_text(home_html, encoding="utf-8")

    # 3) Write Archive (Forward only → latest snapshot), with THIS run's (latest) description
    snaps = sorted(out_dir.glob("dashboard_*.html"), key=snap_key, reverse=True)
    items = []
    for p in snaps:
        m = re.search(r"dashboard_(\d{4}-\d{2}-\d{2})_(\d{4})\.html", p.name)
        label = p.name
        if m:
            label = f"{m.group(1)} {m.group(2)[:2]}:{m.group(2)[2:]}"
        items.append(f'<li><a href="{p.name}">{html_lib.escape(label)}</a></li>')
    archive_list = "<ul class='archive-list'>" + ("\n".join(items) if items else "<li>No snapshots yet.</li>") + "</ul>"

    archive_header_nav = nav_block(forward_href=latest_snapshot, back_href=None)
    archive_footer_nav = nav_block(forward_href=latest_snapshot, back_href=None)
    archive_html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Polymarket Dashboards — Archive</title>
{gtm_head(GTM_ID)}
<style>{BASE_CSS}</style>
</head>
<body>
{gtm_body(GTM_ID)}
<div class="container">
  <div class="header">
    <h1>Polymarket Dashboards — Archive</h1>
    <div class="date">All snapshots</div>
    <div class="source">Source: Polymarket API data.</div>
    {archive_header_nav}
  </div>
  {archive_list}
  {METHODOLOGY_HTML}
  {description_html}
  {archive_footer_nav}
  <div class="footer">Not financial advice. DYOR.<div class="system">{sysline}</div></div>
</div>
</body>
</html>"""
    (out_dir / "archive.html").write_text(archive_html, encoding="utf-8")

    print(f"[ok] Wrote site/index.html, site/{snapshot_name}, site/archive.html")

if __name__ == "__main__":
    main()
