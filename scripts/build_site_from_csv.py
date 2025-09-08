#!/usr/bin/env python3
# scripts/build_site_from_csv.py
#
# Builder + inline OG-image generation.
# Fix: no JavaScript inside f-strings to avoid brace parsing errors.

import sys, os, csv, json, re, math, textwrap, html as html_lib
from pathlib import Path
from datetime import datetime, timezone

# ---- Paths / constants (tweak if needed) ----
SITE_DIR = Path("site")
DATA_DIR = Path("data")
CONTENT_DIR = Path("content")
DESC_META_DIR = CONTENT_DIR / "meta"
DESC_LONG_DIR = CONTENT_DIR / "long"

HOMEPAGE_CANONICAL = "https://urbanpoly.com/index.html"
OG_IMAGE_PATH = SITE_DIR / "og-preview.png"
OG_IMAGE_URL = "/og-preview.png"   # served from Vercel (root = site)
FAVICON_HREF = "/favicon.ico"      # expected in /site

# ---- Small helpers ----
def now_utc():
    return datetime.now(timezone.utc)

def fmt_date_title(dt: datetime):
    return dt.strftime("%d %B %Y • %H:%M UTC")

def read_csv(path: Path):
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def to_float(v, default=None):
    try:
        if v is None or v == "": return default
        return float(v)
    except:
        return default

def safe(s):
    return html_lib.escape(s or "")

def ensure_dirs():
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONTENT_DIR.mkdir(parents=True, exist_ok=True)

# ===============================
# OG image generation (pure Pillow)
# ===============================
def generate_og_preview(csv_path: Path, out_png: Path):
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageFilter
    except Exception as e:
        print(f"[og] Pillow not available ({e}); skipping OG image.")
        return
    try:
        rows = read_csv(csv_path)
        top = rows[:2] if rows else []
        W, H = 1200, 630
        PAD = 48

        img = Image.new("RGB", (W, H), (11, 11, 16))
        draw = ImageDraw.Draw(img)

        # Gradient rim
        glow = Image.new("RGB", (W+40, H+40), (0,0,0))
        gd = ImageDraw.Draw(glow)
        for i, c in enumerate([(110,168,254), (168,121,254)]):
            gd.rounded_rectangle([20-i,20-i,W+20+i,H+20+i], radius=28+i*2, outline=c, width=6)
        glow = glow.filter(ImageFilter.GaussianBlur(16)).crop((20,20,W+20,H+20))
        img = Image.blend(glow, img, 0.85)
        draw = ImageDraw.Draw(img)

        def font(sz, bold=False):
            path = ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                    if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
            return ImageFont.truetype(path, sz)

        y = PAD
        title = "Polymarket Dashboard"
        sub = "Hottest & Overlooked Markets"
        ts = now_utc().strftime("%d %B %Y • %H:%M UTC")

        draw.text((PAD, y), title, fill=(234,234,240), font=font(64, True)); y += 72
        draw.text((PAD, y), sub,   fill=(158,160,200), font=font(40, True)); y += 54
        draw.text((PAD, y), ts,    fill=(163,168,180), font=font(28));      y += 44

        def fit_lines(text, fnt, max_width, max_lines=2):
            tmp = []
            for para in text.splitlines():
                for line in textwrap.wrap(para, width=60):
                    if draw.textlength(line, font=fnt) <= max_width:
                        tmp.append(line)
                    else:
                        t = line
                        while draw.textlength(t + "…", font=fnt) > max_width and len(t) > 4:
                            t = t[:-1]
                        tmp.append(t + "…")
            return tmp[:max_lines]

        maxw = W - PAD*2
        gap = 22
        for row in top:
            q = (row.get("question") or row.get("title") or "").strip() or "—"
            vol = row.get("volume24h") or row.get("volume") or ""
            ttr = row.get("timeToResolveDays") or row.get("ttr") or ""
            spr = row.get("avgSpread") or row.get("avgSpreadProxy") or ""

            for ln in fit_lines(f"• {q}", font(34, True), maxw, 2):
                draw.text((PAD, y), ln, fill=(234,234,240), font=font(34, True)); y += 40

            stat = []
            if vol:
                try: stat.append(f"24h ${int(float(vol)):,.0f}")
                except: pass
            if spr:
                try: stat.append(f"spread {float(spr):.3f}")
                except: pass
            if ttr:
                try: stat.append(f"TTR {float(ttr):.1f}d")
                except: stat.append(f"TTR {ttr}")
            draw.text((PAD, y), "  •  ".join(stat) if stat else " ", fill=(163,168,180), font=font(24)); y += 36 + gap

        badge = "UP"
        bw = draw.textlength(badge, font=font(36, True))
        bx, by = W - PAD - bw - 16, PAD
        draw.rounded_rectangle([bx-14,by-10,bx+bw+14,by+36+10], radius=12, fill=(29,29,40))
        draw.text((bx,by), badge, fill=(234,234,240), font=font(36, True))

        out_png.parent.mkdir(parents=True, exist_ok=True)
        img.save(out_png, "PNG", optimize=True)
        print(f"[og] wrote {out_png}")
    except Exception as e:
        print(f"[og] generation failed: {e}; continuing without.")

# ---- Meta injector (ensures OG/Twitter tags) ----
def inject_og_meta(head_html: str, page_title: str, page_desc: str, canonical_url: str) -> str:
    def meta(name, content, prop=False):
        if prop:
            return f'<meta property="{name}" content="{safe(content)}" />'
        return f'<meta name="{name}" content="{safe(content)}" />'

    head_html = re.sub(r'<meta\s+(?:name|property)="og:[^"]+"\s+content="[^"]*"\s*/?>', '', head_html)
    head_html = re.sub(r'<meta\s+name="twitter:[^"]+"\s+content="[^"]*"\s*/?>', '', head_html)

    extras = "\n".join([
        meta("og:title", page_title, prop=True),
        meta("og:description", page_desc, prop=True),
        meta("og:type", "website", prop=True),
        meta("og:url", canonical_url, prop=True),
        meta("og:image", OG_IMAGE_URL, prop=True),
        meta("twitter:card", "summary_large_image"),
        meta("twitter:title", page_title),
        meta("twitter:description", page_desc),
        meta("twitter:image", OG_IMAGE_URL),
    ])
    if "</title>" in head_html:
        return head_html.replace("</title>", "</title>\n" + extras, 1)
    return head_html + "\n" + extras

# ---- CSS (as before) ----
BASE_CSS = """:root { --bg:#0b0b10; --fg:#eaeaf0; --muted:#a3a8b4; --card:#141420; --border:#232336; --accent:#6ea8fe; --accent-2:#a879fe; }
*{box-sizing:border-box}body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--fg)}
.container{max-width:1240px;margin:0 auto;padding:32px 24px 44px}
.header{margin-bottom:18px}.header h1{margin:0;font-size:34px;font-variant:small-caps;letter-spacing:.5px}
.header .date{color:var(--fg);font-size:20px;font-weight:600;margin-top:6px}
.header .source{color:var(--muted);font-size:14px;margin-top:6px}
.navrow{display:flex;align-items:center;gap:12px;margin:8px 0 18px;flex-wrap:wrap}
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
.small{font-size:12px;color:var(--muted)}"""

# ---- JS toggle as a PLAIN string (no f-string!) ----
JS_TOGGLE = """<script>
function show(id){
  document.getElementById('hot').style.display  = (id==='hot')  ? 'grid' : 'none';
  document.getElementById('gems').style.display = (id==='gems') ? 'grid' : 'none';
  document.getElementById('tabHot').classList.toggle('active', id==='hot');
  document.getElementById('tabGem').classList.toggle('active', id==='gems');
}
</script>"""

def head_html(page_title, page_desc, canonical):
    head = (
f"""<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{safe(page_title)}</title>
<link rel="canonical" href="{safe(canonical)}" />
<link rel="icon" href="{safe(FAVICON_HREF)}" />
<style>{BASE_CSS}</style>"""
    )
    head = inject_og_meta(head, page_title, page_desc, canonical)
    ts_iso = now_utc().isoformat()
    ld = {
        "@context":"https://schema.org",
        "@type":"WebPage",
        "name": page_title,
        "description": page_desc,
        "url": canonical,
        "dateModified": ts_iso
    }
    head += f'\n<script type="application/ld+json">{json.dumps(ld, separators=(",",":"))}</script>'
    return head

def market_cards(rows):
    parts = []
    for r in rows:
        title = r.get("question") or r.get("title") or "(Untitled)"
        why = r.get("why") or ""
        url = r.get("url") or ""
        embed = r.get("embedSrc") or ""
        vol24 = to_float(r.get("volume24h")) or to_float(r.get("volume")) or 0
        spread = r.get("avgSpread") or r.get("avgSpreadProxy") or ""
        ttr = r.get("timeToResolveDays") or r.get("ttr") or ""
        kpi = (
f"""<div class="kpi">
  <div class="box"><div class="small">24h Vol</div><div>${int(vol24):,}</div></div>
  <div class="box"><div class="small">Avg Spread</div><div>{safe(str(spread) if spread!='' else '—')}</div></div>
  <div class="box"><div class="small">Time to Resolve</div><div>{safe(str(ttr) if ttr!='' else '—')}d</div></div>
  <div class="box"><div class="small">Momentum</div><div>{safe(r.get("momentumPct24h") or "—")}</div></div>
</div>"""
        )
        parts.append(
f"""<article class="card">
  <iframe class="iframe" src="{safe(embed)}" loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>
  <div class="p">
    <h3>{safe(title)}</h3>
    <p class="small">{safe(why)}</p>
    {kpi}
    <p class="small muted">View: <a class="link" href="{safe(url)}" target="_blank" rel="noopener">{safe(url)}</a></p>
  </div>
</article>"""
        )
    return "\n".join(parts)

def write_index(rows):
    dt = now_utc()
    title = "Hottest Markets & Overlooked Chances on Polymarket Today"
    date_line = fmt_date_title(dt)
    desc = "Daily Polymarket dashboard: hottest markets by 24h volume and overlooked near-50% opportunities. Updated every 6 hours. Not financial advice."
    head = head_html(f"{title} — {date_line}", desc, HOMEPAGE_CANONICAL)

    hot = rows[:12]
    gems = rows[12:24] if len(rows) > 12 else rows[:12]

    header = (
f"""<header class="header">
  <h1>{safe(title)}</h1>
  <div class="date">{safe(date_line)}</div>
  <div class="source small">Source: Polymarket API data.</div>
  <div class="navrow">
    <a class="btn" href="/archive.html">Archive</a>
    <a class="btn" href="/dashboard_#BACK#.html">Back</a>
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
  <footer class="footer">
    <div class="navrow" style="justify-content:center">
      <a class="btn" href="/archive.html">Archive</a>
      <a class="btn" href="/dashboard_#BACK#.html">Back</a>
    </div>
    <div class="small" style="margin-top:8px">updated {dt.strftime('%H:%M')} UTC</div>
    <div class="small">Not financial advice. DYOR.</div>
  </footer>
</div>"""
    )

    html = "<!doctype html><html lang='en'><head>" + head + "</head><body>" + body + JS_TOGGLE + "</body></html>"
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")
    print("[ok] wrote site/index.html")

def write_snapshot(rows):
    dt = now_utc()
    stamp = dt.strftime("%Y-%m-%d_%H%M")
    name = f"dashboard_{stamp}.html"
    title = "Hottest Markets & Overlooked Chances on Polymarket"
    date_line = fmt_date_title(dt)
    desc = "Snapshot of Polymarket markets: top volume and overlooked near-50% opportunities."
    head = head_html(f"{title} — {date_line}", desc, f"https://urbanpoly.com/{name}")

    hot = rows[:12]
    gems = rows[12:24] if len(rows) > 12 else rows[:12]

    header = (
f"""<header class="header">
  <h1>{safe(title)}</h1>
  <div class="date">{safe(date_line)}</div>
  <div class="source small">Source: Polymarket API data.</div>
  <div class="navrow">
    <a class="btn" href="/archive.html">Archive</a>
    <a class="btn" href="/index.html">Home</a>
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
  <footer class="footer">
    <div class="navrow" style="justify-content:center">
      <a class="btn" href="/archive.html">Archive</a>
      <a class="btn" href="/index.html">Home</a>
    </div>
    <div class="small" style="margin-top:8px">updated {dt.strftime('%H:%M')} UTC</div>
    <div class="small">Not financial advice. DYOR.</div>
  </footer>
</div>"""
    )
    html = "<!doctype html><html lang='en'><head>" + head + "</head><body>" + body + JS_TOGGLE + "</body></html>"
    (SITE_DIR / name).write_text(html, encoding="utf-8")
    print(f"[ok] wrote site/{name}")
    return name

def write_archive():
    snaps = sorted(SITE_DIR.glob("dashboard_*.html"))
    items = []
    for p in snaps:
        label = p.stem.replace("dashboard_", "").replace("_", " ")
        items.append(f'<p><a class="btn" href="/{p.name}">{safe(label)}</a></p>')
    head = head_html("Polymarket Dashboards — Archive",
                     "Chronological archive of snapshots generated every ~6 hours.",
                     "https://urbanpoly.com/archive.html")
    body = (
f"""<div class="container">
  <header class="header">
    <h1>Archive</h1>
    <div class="navrow"><a class="btn" href="/index.html">Home</a></div>
  </header>
  {''.join(items) if items else '<p class="small muted">No snapshots yet.</p>'}
  <footer class="footer small">Not financial advice. DYOR.</footer>
</div>"""
    )
    html = "<!doctype html><html lang='en'><head>" + head + "</head><body>" + body + "</body></html>"
    (SITE_DIR / "archive.html").write_text(html, encoding="utf-8")
    print("[ok] wrote site/archive.html")

def write_robots_and_sitemap():
    (SITE_DIR / "robots.txt").write_text("User-agent: *\nAllow: /\nSitemap: https://urbanpoly.com/sitemap.xml\n", encoding="utf-8")
    urls = [f"https://urbanpoly.com/{p.name}" for p in SITE_DIR.glob("*.html")]
    nowiso = now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")
    xml = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for u in sorted(urls):
        xml.append(f"<url><loc>{u}</loc><lastmod>{nowiso}</lastmod><changefreq>hourly</changefreq><priority>0.6</priority></url>")
    xml.append("</urlset>")
    (SITE_DIR / "sitemap.xml").write_text("\n".join(xml), encoding="utf-8")
    print("[ok] wrote site/robots.txt, site/sitemap.xml")

# ---- main ----
def main():
    if len(sys.argv) < 2:
        print("usage: build_site_from_csv.py <csv_path>")
        return 2

    csv_path = Path(sys.argv[1]).resolve()
    ensure_dirs()

    rows = read_csv(csv_path)

    # Create fresh OG image first (non-fatal if Pillow is missing)
    generate_og_preview(csv_path, OG_IMAGE_PATH)

    # Write pages
    write_index(rows)
    write_snapshot(rows)
    write_archive()
    write_robots_and_sitemap()
    return 0

if __name__ == "__main__":
    sys.exit(main())
