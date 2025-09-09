#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds static HTML pages from the latest enriched CSV.

New in this version:
- ARCHIVE: snapshot list items are rendered as big .btn buttons (not tiny links).
- ARCHIVE: the newest snapshot entry points to index.html (live page) instead of its own snapshot file.
  (We still write the newest snapshot file for history; the archive list just treats it as "live".)

Also retained:
- Tabs show ONE section at a time; Overlooked distinct from HOT.
- Navigation consistency (header & footer); index shows Back only; archive shows Forward-only; snapshots show Back+Forward.
- UTC time everywhere; rotating descriptions for index+archive; frozen for snapshots.
- robots.txt + sitemap.xml emitted.
- desc_history.json robust load/save.
"""

import sys, csv, html as html_lib, json, random, re  # <-- added re
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
.embed-caption { margin: 6px 14