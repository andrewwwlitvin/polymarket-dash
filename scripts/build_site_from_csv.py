#!/usr/bin/env python3
# scripts/build_site_from_csv.py
#
# Targeted fixes only:
# - Remove duplicate inline metrics line (no "why" paragraph)
# - Correct Home header back button + footer back target
# - Snapshot neighbors computed including the fresh snapshot
# - Archive kept simple; Live button -> index
# - Favicon tags added
# - OG preview always refreshed and visibly different per run

import sys, os, csv, json, re, html as html_lib
from pathlib import Path
from datetime import datetime, timezone

SITE_DIR = Path("site")
DATA_DIR = Path("data")
CONTENT_DIR = Path("content")
DESC_LONG_DIR = CONTENT_DIR / "long"

HOMEPAGE_CANONICAL = "https://urbanpoly.com/index.html"
FAVICON_HREF = "/favicon.ico"
OG_IMAGE_PATH = SITE_DIR / "og-preview.png"
OG_IMAGE_URL  = "/og-preview.png"

def now_utc():
    return datetime.now(timezone.utc)

def fmt_date_title(dt: datetime):
    return dt.strftime("%d %B %Y • %H:%M UTC")

def safe(s): return html_lib.escape(s or "")

def ensure_dirs():
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONTENT_DIR.mkdir(parents=True, exist_ok=True)

def read_csv(path: Path):
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def list_snapshots():
    snaps = []
    for p in SITE_DIR.glob("dashboard_????-??-??_????.html"):
        snaps.append(p)
    def key(p):
        m = re.search(r"dashboard_(\d{4}-\d{2}-\d{2})_(\d{4})\.html$", p.name)
        if not m: return ("0000-00-00","0000")
        return (m.group(1), m.group(2))
    return sorted(snaps, key=key)

def latest_snapshot():
    snaps = list_snapshots()
    return snaps[-1] if snaps else None

# ---------- OG image (Pillow optional)
def generate_og_preview(rows, ts):
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception as e:
        print(f"[og] Pillow not available ({e}); skipping OG.")
        return
    try:
        W,H = 1200,630
        img = Image.new("RGB",(W,H),(11,11,16))
        d = ImageDraw.Draw(img)
        def font(sz,b=False):
            path = ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                    if b else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
            return ImageFont.truetype(path,sz)
        # subtle header stripe for visible change each run
        d.rectangle([0,0,W,10], fill=(110,168,254))
        d.rectangle([0,10,W,18], fill=(168,121,254))
        title = "Polymarket Dashboard"
        sub   = "Hottest & Overlooked Markets"
        tsline= ts.strftime("%d %B %Y • %H:%M UTC")
        d.text((48,48), title, fill=(234,234,240), font=font(64,True))
        d.text((48,120), sub,   fill=(168,121,254), font=font(40,True))
        d.text((48,168), tsline,fill=(163,168,180), font=font(28))
        y=220
        def fit(text,maxw,f):
            t=text
            while d.textlength(t, font=f) > maxw and len(t)>4:
                t=t[:-1]
            return t + ("…" if t!=text else "")
        for r in rows[:2]:
            q=(r.get("question") or r.get("title") or "—").strip()
            d.text((48,y), "• "+fit(q, W-96, font(36,True)), fill=(234,234,240), font=font(36,True)); y+=44
        OG_IMAGE_PATH.parent.mkdir(parents=True,exist_ok=True)
        img.save(OG_IMAGE_PATH, "PNG", optimize=True)
        print(f"[og] wrote {OG_IMAGE_PATH}")
    except Exception as e:
        print(f"[og] failed: {e}")

# ---------- Meta / CSS / JS
BASE_CSS = """:root { --bg:#0b0b10; --fg:#eaeaf0; --muted:#a3a8b4; --card:#141420; --border:#232336; --accent:#6ea8fe; --accent-2:#a879fe; }
*{box-sizing:border-box}body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--fg)}
.container{max-width:1240px;margin:0 auto;padding:32px 24px 44px}
.header{margin-bottom:18px}.header h1{margin:0;font-size:34px;font-variant:small-caps;letter-spacing:.5px}
.header .date{color:var(--fg);font-size:20px;font-weight:600;margin-top:6px}
.header .source{color:var(--muted);font-size:14px;margin-top:6px}
.navrow{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:8px 0 18px;flex-wrap:wrap}
.navleft,.navright{display:flex;gap:12px}
.btn{display:inline-flex;align-items:center;gap:8px;padding:10px 14px;border:1px solid var(--border);border-radius:14px;background:transparent;color:var(--fg);text-decoration:none;font-weight:700}
.tabs{display:grid;grid-template-columns:1fr 1fr;border:1px solid var(--border);border-radius:14px;overflow:hidden;margin:12px 0 22px}
.tabs button{background:transparent;color:var(--fg);padding:14px 12px;border:0;cursor:pointer;font-weight:700;font-size:14px}
.tabs button.active{background:var(--accent);color:#0b0b10}
.grid{display:grid;gap:20px}.cards{grid-template-columns:1fr 1fr 1fr}
.card{border:1px solid var(--border);border-radius:16px;background:var(--card);overflow:hidden}
.iframe{width:100%;aspect-ratio:16/9;border:0}
.card .p{padding:16px}.muted{color:var(--muted)}
.kpi{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px}
.kpi .box{border:1px solid var(--border);border-radius:12px;padding:10px}
.footer{margin-top:26px;color:var(--muted);font-size:12px;text-align:center}
a.link{color:inherit;text-decoration:underline dotted}
.small{font-size:12px;color:var(--muted)}
.section{border:1px dashed #6e6aff66;border-radius:14px;padding:16px 18px;margin-top:14px}
.section h2{margin:0 0 8px 0}
"""

JS_TOGGLE = """<script>
function show(id){
  document.getElementById('hot').style.display  = (id==='hot')  ? 'grid' : 'none';
  document.getElementById('gems').style.display = (id==='gems') ? 'grid' : 'none';
  document.getElementById('tabHot').classList.toggle('active', id==='hot');
  document.getElementById('tabGem').classList.toggle('active', id==='gems');
}
</script>"""

DEFAULT_METHOD = (
    "<ul>"
    "<li><b>Hottest:</b> Prioritizes 24h volume, tighter spreads, and sooner time-to-resolve.</li>"
    "<li><b>Overlooked:</b> Prefers near-50% binary midpoints, negative underround, moderate 24h volume, and sooner resolution.</li>"
    "<li><b>Why line:</b> Quick cues like “24h $X • TTR Yd”.</li>"
    "</ul>"
)

DEFAULT_DESC = (
    "<p>Daily dashboard surfacing active and overlooked Polymarket markets. "
    "Signals emphasize 24h activity, spreads, and time-to-resolution. "
    "Updated every ~6 hours. Not financial advice.</p>"
)

def pick_long_description():
    try:
        if DESC_LONG_DIR.exists():
            files = sorted([p for p in DESC_LONG_DIR.glob("*.html") if p.is_file()])
            if files:
                i = now_utc().minute % len(files)
                return files[i].read_text(encoding="utf-8")
    except Exception as e:
        print(f"[desc] read failed: {e}")
    return DEFAULT_DESC

def inject_og_meta(head_html: str, page_title: str, page_desc: str, canonical_url: str) -> str:
    def meta(name, content, prop=False):
        c = safe(content)
        return f'<meta property="{name}" content="{c}" />' if prop else f'<meta name="{name}" content="{c}" />'
    head_html = re.sub(r'<meta\s+(?:name|property)="og:[^"]+"\s+content="[^"]*"\s*/?>', '', head_html)
    head_html = re.sub(r'<meta\s+name="twitter:[^"]+"\s+content="[^"]*"\s*/?>', '', head_html)
    extras = "\n".join([
        meta("og:title", page_title, True),
        meta("og:description", page_desc, True),
        meta("og:type", "website", True),
        meta("og:url", canonical_url, True),
        meta("og:image", OG_IMAGE_URL, True),
        meta("twitter:card", "summary_large_image"),
        meta("twitter:title", page_title),
        meta("twitter:description", page_desc),
        meta("twitter:image", OG_IMAGE_URL),
    ])
    return head_html.replace("</title>", "</title>\n" + extras, 1)

def head_html(page_title, page_desc, canonical):
    h = (
f"""<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{safe(page_title)}</title>
<link rel="canonical" href="{safe(canonical)}" />
<link rel="icon" type="image/x-icon" href="{safe(FAVICON_HREF)}" />
<link rel="shortcut icon" href="{safe(FAVICON_HREF)}" />
<style>{BASE_CSS}</style>"""
    )
    h = inject_og_meta(h, page_title, page_desc, canonical)
    ld = {
        "@context":"https://schema.org",
        "@type":"WebPage",
        "name": page_title,
        "description": page_desc,
        "url": canonical,
        "dateModified": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    h += f'\n<script type="application/ld+json">{json.dumps(ld,separators=(",",":"))}</script>'
    return h

# ---------- KPIs / cards (NO "why" line)
def kpi_boxes(r):
    def fnum(x):
        try: return f"{int(float(x)): ,}".replace(",", ",")
        except: return "—"
    vol = r.get("volume24h") or r.get("volume") or ""
    spr = r.get("avgSpread") or r.get("avgSpreadProxy") or ""
    ttr = r.get("timeToResolveDays") or r.get("ttr") or ""
    mom = r.get("momentumPct24h") or "—"
    try: spr_show = f"{float(spr):.3f}" if spr != "" else "—"
    except: spr_show = "—"
    try: ttr_show = f"{float(ttr):.1f}d" if ttr != "" else "—"
    except: ttr_show = f"{ttr}d" if ttr else "—"
    return (
f"""<div class="kpi">
  <div class="box"><div class="small">24h Vol</div><div>${fnum(vol)}</div></div>
  <div class="box"><div class="small">Avg Spread</div><div>{spr_show}</div></div>
  <div class="box"><div class="small">Time to Resolve</div><div>{ttr_show}</div></div>
  <div class="box"><div class="small">Momentum</div><div>{safe(mom)}</div></div>
</div>"""
    )

def market_cards(rows):
    out=[]
    for r in rows:
        title = r.get("question") or r.get("title") or "(Untitled)"
        url   = r.get("url") or ""
        embed = r.get("embedSrc") or ""
        out.append(
f"""<article class="card">
  <iframe class="iframe" src="{safe(embed)}" loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>
  <div class="p">
    <h3>{safe(title)}</h3>
    {kpi_boxes(r)}
    <p class="small muted">{safe(title)} — <a class="link" href="{safe(url)}" target="_blank" rel="noopener">View on Polymarket</a></p>
  </div>
</article>"""
        )
    return "\n".join(out)

# ---------- rotate previous index to snapshot and return its snapshot name
def rotate_previous_index_to_snapshot():
    idx = SITE_DIR / "index.html"
    if not idx.exists(): return None
    text = idx.read_text(encoding="utf-8")
    m = re.search(r"build_ts:\s*([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{4})", text)
    if not m: return None
    stamp = m.group(1)
    snap_name = f"dashboard_{stamp}.html"
    dst = SITE_DIR / snap_name
    if not dst.exists():
        dst.write_text(text, encoding="utf-8")
        print(f"[rotate] previous index saved as {dst.name}")
    return dst.name

# ---------- neighbors including the current snapshot
def snapshot_neighbors_including(current_name):
    snaps = list_snapshots()
    # add current_name into the ordered list
    pseudo = SITE_DIR / current_name
    snaps_with = snaps + [pseudo]
    # sort
    def key(p):
        m = re.search(r"dashboard_(\d{4}-\d{2}-\d{2})_(\d{4})\.html$", p.name)
        if not m: return ("0000-00-00","0000")
        return (m.group(1), m.group(2))
    snaps_with = sorted(snaps_with, key=key)
    names = [p.name for p in snaps_with]
    i = names.index(current_name)
    back = names[i-1] if i-1 >= 0 else None
    fwd  = names[i+1] if i+1 < len(names) else None
    back_href = f"/{back}" if back else "/archive.html"
    fwd_href  = f"/{fwd}" if fwd else "/index.html"
    return back_href, fwd_href

# ---------- pages
def write_index(rows, ts, back_target, long_html):
    title = "Hottest Markets & Overlooked Chances on Polymarket Today"
    date_line = fmt_date_title(ts)
    desc = "Daily Polymarket dashboard: hottest markets by 24h volume and overlooked near-50% opportunities. Updated every 6 hours."
    head = head_html(f"{title} — {date_line}", desc, HOMEPAGE_CANONICAL)

    hot  = rows[:12]
    gems = rows[12:24] if len(rows)>12 else rows[:12]

    header = (
f"""<header class="header">
  <h1>{safe(title)}</h1>
  <div class="date">{safe(date_line)}</div>
  <div class="source small">Source: Polymarket API data.</div>
  <div class="navrow">
    <div class="navleft"><a class="btn" href="{safe(back_target)}">Back</a></div>
    <div class="navright"><a class="btn" href="/archive.html">Archive</a></div>
  </div>
</header>"""
    )
    tabs = """<div class="tabs">
  <button id="tabHot" class="active" onclick="show('hot')">HOT</button>
  <button id="tabGem" onclick="show('gems')">Overlooked</button>
</div>"""

    body = (
f"""<div class="container">
  <!-- build_ts: {ts.strftime("%Y-%m-%d_%H%M")} -->
  {header}
  {tabs}
  <section id="hot" class="grid cards">{market_cards(hot)}</section>
  <section id="gems" class="grid cards" style="display:none">{market_cards(gems)}</section>

  <section class="section">
    <h2>Description</h2>
    {long_html}
  </section>
  <section class="section">
    <h2>Methodology</h2>
    {DEFAULT_METHOD}
  </section>

  <footer class="footer">
    <div class="navrow" style="justify-content:center">
      <div class="navleft"><a class="btn" href="{safe(back_target)}">Back</a></div>
      <div class="navright"><a class="btn" href="/archive.html">Archive</a></div>
    </div>
    <div class="small" style="margin-top:8px">updated {ts.strftime('%H:%M')} UTC</div>
  </footer>
</div>"""
    )
    html = "<!doctype html><html lang='en'><head>" + head + "</head><body>" + body + JS_TOGGLE + "</body></html>"
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")
    print("[ok] wrote site/index.html")

def write_snapshot(rows, ts, long_html):
    stamp = ts.strftime("%Y-%m-%d_%H%M")
    name = f"dashboard_{stamp}.html"
    title = "Hottest Markets & Overlooked Chances on Polymarket"
    date_line = fmt_date_title(ts)
    desc = "Snapshot of Polymarket markets: top volume and overlooked near-50% opportunities."
    head = head_html(f"{title} — {date_line}", desc, f"https://urbanpoly.com/{name}")

    hot  = rows[:12]
    gems = rows[12:24] if len(rows)>12 else rows[:12]

    # neighbors INCLUDING this new snapshot
    back_href, fwd_href = snapshot_neighbors_including(name)

    header = (
f"""<header class="header">
  <h1>{safe(title)}</h1>
  <div class="date">{safe(date_line)}</div>
  <div class="source small">Source: Polymarket API data.</div>
  <div class="navrow">
    <div class="navleft"><a class="btn" href="/index.html">Home</a></div>
    <div class="navright"><a class="btn" href="/archive.html">Archive</a></div>
  </div>
</header>"""
    )
    tabs = """<div class="tabs">
  <button id="tabHot" class="active" onclick="show('hot')">HOT</button>
  <button id="tabGem" onclick="show('gems')">Overlooked</button>
</div>"""

    body = (
f"""<div class="container">
  {header}
  {tabs}
  <section id="hot" class="grid cards">{market_cards(hot)}</section>
  <section id="gems" class="grid cards" style="display:none">{market_cards(gems)}</section>

  <section class="section">
    <h2>Description</h2>
    {long_html}
  </section>
  <section class="section">
    <h2>Methodology</h2>
    {DEFAULT_METHOD}
  </section>

  <footer class="footer">
    <div class="navrow" style="justify-content:center">
      <div class="navleft"><a class="btn" href="{safe(back_href)}">Back</a></div>
      <div class="navright"><a class="btn" href="{safe(fwd_href)}">Forward</a></div>
    </div>
    <div class="small" style="margin-top:8px">updated {ts.strftime('%H:%M')} UTC</div>
  </footer>
</div>"""
    )
    html = "<!doctype html><html lang='en'><head>" + head + "</head><body>" + body + JS_TOGGLE + "</body></html>"
    (SITE_DIR / name).write_text(html, encoding="utf-8")
    print(f"[ok] wrote site/{name}")
    return name

def write_archive(ts):
    snaps = list_snapshots()
    items=[]
    for p in reversed(snaps):
        label = p.stem.replace("dashboard_","").replace("_"," ")
        items.append(f'<p><a class="btn" href="/{p.name}">{safe(label)}</a></p>')
    title="Polymarket Dashboards — Archive"
    date_line=fmt_date_title(ts)
    desc="Chronological archive of snapshots generated roughly every six hours."
    head=head_html(f"{title} — {date_line}", desc, "https://urbanpoly.com/archive.html")
    body=(f"""
<div class="container">
  <header class="header">
    <h1>{safe(title)}</h1>
    <div class="date">{safe(date_line)}</div>
    <div class="navrow">
      <div class="navleft"><a class="btn" href="/index.html">Home</a></div>
      <div class="navright"></div>
    </div>
  </header>

  <section class="section"><h2>Description</h2>{DEFAULT_DESC}</section>
  <section class="section"><h2>Methodology</h2>{DEFAULT_METHOD}</section>

  <div style="margin:16px 0"><a class="btn" href="/index.html">Latest (Live)</a></div>

  {''.join(items) if items else '<p class="small muted">No snapshots yet.</p>'}

  <footer class="footer">
    <div class="navrow" style="justify-content:center">
      <div class="navleft"><a class="btn" href="/index.html">Home</a></div>
    </div>
    <div class="small" style="margin-top:8px">updated {ts.strftime('%H:%M')} UTC</div>
  </footer>
</div>
""")
    html="<!doctype html><html lang='en'><head>"+head+"</head><body>"+body+"</body></html>"
    (SITE_DIR/"archive.html").write_text(html,encoding="utf-8")
    print("[ok] wrote site/archive.html")

def write_robots_and_sitemap(ts):
    (SITE_DIR/"robots.txt").write_text(
        "User-agent: *\nAllow: /\nSitemap: https://urbanpoly.com/sitemap.xml\n",
        encoding="utf-8",
    )
    urls=sorted([f"https://urbanpoly.com/{p.name}" for p in SITE_DIR.glob("*.html")])
    nowiso=ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    xml=['<?xml version="1.0" encoding="UTF-8"?>',
         '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for u in urls:
        xml.append(f"<url><loc>{u}</loc><lastmod>{nowiso}</lastmod><changefreq>hourly</changefreq><priority>0.6</priority></url>")
    xml.append("</urlset>")
    (SITE_DIR/"sitemap.xml").write_text("\n".join(xml),encoding="utf-8")
    print("[ok] wrote site/robots.txt, site/sitemap.xml")

def main():
    if len(sys.argv)<2:
        print("usage: build_site_from_csv.py <csv_path>"); return 2
    csv_path=Path(sys.argv[1]).resolve()
    ensure_dirs()
    rows=read_csv(csv_path)
    ts=now_utc()

    # OG image first (so it always reflects this run)
    generate_og_preview(rows, ts)

    # Rotate previous index -> snapshot, for Home Back target
    prev_snapshot = rotate_previous_index_to_snapshot()
    back_target = f"/{prev_snapshot}" if prev_snapshot else "/archive.html"

    long_html = pick_long_description()

    # Write pages
    write_index(rows, ts, back_target, long_html)
    write_snapshot(rows, ts, long_html)
    write_archive(ts)
    write_robots_and_sitemap(ts)

    return 0

if __name__=="__main__":
    sys.exit(main())
