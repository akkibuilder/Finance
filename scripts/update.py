"""
TTWO Investment Dashboard — Hourly Data Updater
================================================

Runs in GitHub Actions on an hourly cron. Pulls:
  - TTWO stock data via yfinance (price, history, day change, 52w range)
  - GTA VI news from 8 reliable feeds, fetched in parallel:
      Yahoo Finance, Reuters (via Google News site filter), Google News,
      IGN, GamesRadar, PlayStation Blog, Xbox Wire, Reddit r/GTA6
  - Per-feed health stats (status, item count, latency) for the dashboard's
    data-quality strip
  - Computes portfolio P/L and the strategy verdict
  - Writes everything to data/snapshot.json

Also supports `python update.py --sample` to generate a realistic-looking
demo snapshot for local testing without needing yfinance/network access.
"""

import json
import sys
import os
import re
import time
import argparse
import datetime as dt
import urllib.request
import urllib.parse
import random
import math
import html
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ── Portfolio constants (Akki's actual holdings) ─────────────────────────────
SHARES = 10.17859464
AVG_PRICE_EUR = 213.49
INVESTED_EUR = 2172.90
EXTRA_CASH_EUR = 500.00
MAX_LOSS_EUR = 400.00
PROFIT_TARGET_EUR = 600.00

TICKER = "TKE.DE"
USD_TO_EUR_FALLBACK = 0.93  # If FX fetch fails

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "snapshot.json"


# ── GTA VI canonical events (manually curated, hand-checked) ─────────────────
GTA_EVENTS = [
    {
        "date": "2023-12-04",
        "title": "Trailer 1",
        "short": "TRAILER 1",
        "icon": "▶",
        "type": "trailer",
        "color": "#a855f7",
        "impact": "Set franchise hype record; TTWO closed +1.8% next session.",
        "source": "Rockstar Newswire",
    },
    {
        "date": "2025-05-06",
        "title": "Trailer 2",
        "short": "TRAILER 2",
        "icon": "▶",
        "type": "trailer",
        "color": "#a855f7",
        "impact": "Confirmed Vice City setting; TTWO +3.2% week-over-week.",
        "source": "Rockstar Newswire",
    },
    {
        "date": "2026-06-18",
        "title": "Cover Art",
        "short": "COVER ART",
        "icon": "🖼",
        "type": "media",
        "color": "#16a34a",
        "impact": "Marketing kickoff. Generally bullish for pre-release positioning.",
        "source": "Rockstar Newswire",
    },
    {
        "date": "2026-06-25",
        "title": "Preorders Open",
        "short": "PREORDERS OPEN",
        "icon": "🛒",
        "type": "commerce",
        "color": "#f97316",
        "impact": "First hard demand signal. Historically high-volatility event.",
        "source": "Take-Two IR",
    },
    {
        "date": "2026-11-19",
        "title": "Launch",
        "short": "LAUNCH",
        "icon": "🚀",
        "type": "release",
        "color": "#a855f7",
        "impact": "Global release. Classic 'sell the news' risk window.",
        "source": "Take-Two IR",
    },
]


# ── News feeds ───────────────────────────────────────────────────────────────
# Eight feeds across four categories. The weight column biases the impact score:
# financial/platform sources score higher than community sources because they
# matter more for an investment decision.
NEWS_FEEDS = [
    # ── Financial ────────────────────────────────────────────────────────────
    {
        "url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=TTWO&region=US&lang=en-US",
        "label": "Yahoo Finance", "weight": 1.4, "category": "financial",
    },
    {
        # Reuters dropped public RSS in 2020 — use Google News with site filter
        "url": "https://news.google.com/rss/search?q=site%3Areuters.com+%22Take-Two%22+OR+%22Grand+Theft+Auto%22&hl=en-US&gl=US&ceid=US:en",
        "label": "Reuters", "weight": 1.3, "category": "financial",
    },
    # ── Aggregator ───────────────────────────────────────────────────────────
    {
        "url": "https://news.google.com/rss/search?q=%22GTA+6%22+OR+%22Grand+Theft+Auto+VI%22+OR+%22Take-Two%22&hl=en-US&gl=US&ceid=US:en",
        "label": "Google News", "weight": 1.0, "category": "aggregator",
    },
    # ── Platform-official ────────────────────────────────────────────────────
    {
        "url": "https://blog.playstation.com/feed/",
        "label": "PlayStation Blog", "weight": 1.1, "category": "platform",
    },
    {
        "url": "https://news.xbox.com/en-us/feed/",
        "label": "Xbox Wire", "weight": 1.1, "category": "platform",
    },
    # ── Gaming press ─────────────────────────────────────────────────────────
    {
        "url": "https://www.ign.com/rss/v2/articles/feed",
        "label": "IGN", "weight": 1.0, "category": "gaming",
    },
    {
        "url": "https://www.gamesradar.com/feeds.xml",
        "label": "GamesRadar", "weight": 0.9, "category": "gaming",
    },
    # ── Community ────────────────────────────────────────────────────────────
    {
        "url": "https://www.reddit.com/r/GTA6/.rss",
        "label": "r/GTA6", "weight": 0.7, "category": "community",
    },
]

# Populated each fetch_news() run; consumed by the data-quality block in
# build_snapshot(). Module-level so a single fetch run owns the snapshot.
FEED_STATS = {}

# Same idea for the stock fetch — populated when build_snapshot() calls
# fetch_yfinance_stock() (or generate_sample_stock()).
STOCK_STATUS = {}


# ── Dynamic event detection ──────────────────────────────────────────────────
# Each detector scans news headlines for a specific event class. When a pattern
# matches, the event is added to the chart, persisted in snapshot.detected_events
# so it survives future runs, and surfaced as an alert.
#
# Design rules:
#   · `key` is the unique de-dup ID — re-detection of an already-known event is
#     suppressed even if news keeps mentioning it.
#   · `patterns` are case-insensitive regex; ANY match triggers.
#   · `exclude` patterns disqualify matches (e.g., fan-made trailers, denials,
#     references to old GTA V content).
#   · Detectors should be high-signal — false positives clutter the chart.
EVENT_DETECTORS = [
    {
        "key": "trailer-3",
        "patterns": [
            r"\btrailer\s*3\b",
            r"\bthird\s+trailer\b",
            r"\btrailer\s+iii\b",
            r"\bnew\s+gta\s*(?:vi|6)\s+trailer\b",
        ],
        "exclude": [
            r"\btrailer\s*[12]\b",
            r"\bfan[- ]?made\b",
            r"\brumou?r",
            r"\bspeculation\b",
            r"\bleak",
        ],
        "title": "Trailer 3", "short": "TRAILER 3",
        "icon": "🎥", "color": "#a855f7", "type": "trailer",
    },
    {
        "key": "gameplay-reveal",
        "patterns": [
            r"\bgameplay\s+(?:reveal|showcase|deep[- ]?dive|footage|preview|walkthrough)\b",
            r"\bfirst\s+gameplay\b",
            r"\bofficial\s+gameplay\b",
        ],
        "exclude": [r"\bleak", r"\bfake\b", r"\bfan[- ]?made\b"],
        "title": "Gameplay Reveal", "short": "GAMEPLAY",
        "icon": "🎮", "color": "#3b82f6", "type": "media",
    },
    {
        "key": "delay-announced",
        "patterns": [
            r"\b(?:gta\s*(?:vi|6)|grand\s+theft\s+auto\s+(?:vi|6))\s+(?:has\s+been\s+)?delayed\b",
            r"\bdelays?\s+(?:gta\s*(?:vi|6)|launch|release)\b",
            r"\bpushed\s+(?:back|to)\s+\d{4}\b",
            r"\bpostpon",
            r"\brelease\s+date\s+pushed\b",
        ],
        "exclude": [r"\bdenied\b", r"\bdoes\s+not\b", r"\bnot\s+delayed\b", r"\bdenies\b"],
        "title": "Delay Announced", "short": "DELAY",
        "icon": "⚠", "color": "#dc2626", "type": "negative",
    },
    {
        "key": "pc-version",
        "patterns": [
            r"\bpc\s+(?:version|release|port)\b",
            r"\bsteam\s+page\b",
            r"\bcomes\s+to\s+(?:pc|steam)\b",
            r"\b(?:gta\s*(?:vi|6))\s+on\s+pc\b",
        ],
        "title": "PC Version", "short": "PC ANNOUNCE",
        "icon": "🖥", "color": "#16a34a", "type": "platform",
    },
    {
        "key": "collector-edition",
        "patterns": [
            r"\bcollector(?:'s|s)?\s+edition\b",
            r"\bdeluxe\s+edition\b",
            r"\bspecial\s+edition\b",
            r"\bultimate\s+edition\b",
        ],
        "exclude": [r"\bgta\s*v\s+collector", r"\brdr2?\b"],
        "title": "Collector Edition", "short": "COLLECTOR ED",
        "icon": "🎁", "color": "#9333ea", "type": "commerce",
    },
    {
        "key": "review-embargo",
        "patterns": [
            r"\breview\s+embargo\b",
            r"\breviews?\s+go\s+live\b",
            r"\breviews?\s+(?:are\s+)?(?:in|out|drop)\b",
            r"\bfirst\s+reviews?\b",
        ],
        "title": "Review Embargo Lifted", "short": "REVIEWS LIVE",
        "icon": "📰", "color": "#f97316", "type": "media",
    },
    {
        "key": "metacritic",
        "patterns": [
            r"\bmetacritic\s+score\b",
            r"\bmetascore\b",
            r"\bscores?\s+\d{2,3}\s+on\s+metacritic\b",
        ],
        "title": "Metacritic Score", "short": "METASCORE",
        "icon": "⭐", "color": "#eab308", "type": "media",
    },
    {
        "key": "launch-trailer",
        "patterns": [
            r"\blaunch\s+trailer\b",
            r"\brelease\s+trailer\b",
        ],
        "exclude": [r"\bgta\s+v\b", r"\bgta\s*5\b"],
        "title": "Launch Trailer", "short": "LAUNCH TRAILER",
        "icon": "🎬", "color": "#a855f7", "type": "trailer",
    },
    {
        "key": "online-mode",
        "patterns": [
            r"\bgta\s+online\s+(?:vi|6)\b",
            r"\b(?:gta\s*(?:vi|6))\s+online\b",
            r"\bmultiplayer\s+(?:reveal|announce|detail)",
            r"\bonline\s+mode\s+(?:reveal|announced|detail)",
        ],
        "exclude": [r"\bgta\s+online\s+(?:v|5)\b", r"\bgta\s*v\s+online\b"],
        "title": "Online Mode", "short": "ONLINE",
        "icon": "🌐", "color": "#3b82f6", "type": "feature",
    },
    {
        "key": "sales-milestone",
        "patterns": [
            r"\b\d+\s*million\s+(?:copies\s+)?sold\b",
            r"\bsells\s+\d+\s*million\b",
            r"\bsales\s+milestone\b",
            r"\bfastest[- ]?selling\b",
            r"\bbest[- ]?selling\b",
        ],
        "title": "Sales Milestone", "short": "SALES",
        "icon": "🏆", "color": "#16a34a", "type": "milestone",
    },
    {
        "key": "preorder-record",
        "patterns": [
            r"\bpreorder\s+(?:record|numbers|figures)\b",
            r"\bpre[- ]?order\s+(?:record|numbers|figures)\b",
            r"\bmost\s+preorder",
            r"\brecord\s+preorder",
        ],
        "title": "Preorder Numbers", "short": "PREORDERS",
        "icon": "📊", "color": "#16a34a", "type": "commerce",
    },
    {
        "key": "wishlist-rank",
        "patterns": [
            r"\bmost\s+wishlisted\b",
            r"\btop\s+(?:of\s+)?wishlist\b",
            r"\bsteam\s+wishlist\s+(?:king|record|leader)",
        ],
        "title": "Wishlist Ranking", "short": "WISHLIST",
        "icon": "📌", "color": "#3b82f6", "type": "interest",
    },
    {
        "key": "award-nomination",
        "patterns": [
            r"\bgame\s+of\s+the\s+year\b",
            r"\bgoty\b",
            r"\bthe\s+game\s+awards?\b",
            r"\bbafta\s+(?:nomination|wins?)",
        ],
        "exclude": [r"\bgta\s*v\s+game\s+of"],
        "title": "Award / Nomination", "short": "AWARD",
        "icon": "🏅", "color": "#eab308", "type": "recognition",
    },
    {
        "key": "price-reveal",
        "patterns": [
            r"\bgta\s*(?:vi|6)\s+price\b",
            r"\b\$(?:69|70|79|89|99)\.\d{2}\b.*(?:gta|grand\s+theft)",
            r"\bpricing\s+(?:announced|confirmed|revealed)\b",
        ],
        "title": "Price Revealed", "short": "PRICING",
        "icon": "💰", "color": "#16a34a", "type": "commerce",
    },
]


# ── Live data fetchers ───────────────────────────────────────────────────────

def fetch_yfinance_stock():
    """Pull TTWO data using yfinance. Returns dict matching snapshot.stock shape."""
    import yfinance as yf

    t = yf.Ticker(TICKER)
    hist = t.history(
        period="2y",
        interval="1d",
        auto_adjust=False,
        repair=True
    )
    print(hist.tail())
    if hist.empty:
        raise RuntimeError("yfinance returned empty history")

    info = {}
    try:
        info = t.fast_info or {}
    except Exception:
        pass

    

    last_close_usd = float(hist["Close"].iloc[-1])
    prev_close_usd = float(hist["Close"].iloc[-2]) if len(hist) > 1 else last_close_usd

    last_close_eur = float(hist["Close"].iloc[-1])
    prev_close_eur = float(hist["Close"].iloc[-2]) if len(hist) > 1 else last_close_eur

    day_change_abs = last_close_eur - prev_close_eur
    day_change_pct = (day_change_abs / prev_close_eur) * 100 if prev_close_eur else 0

    last_year = hist.tail(252)
    w52_high = float(last_year["High"].max())
    w52_low = float(last_year["Low"].min())

    # Compact daily history for chart
    history = []
    for idx, row in hist.iterrows():
        history.append({
            "date": idx.strftime("%Y-%m-%d"),
            "close": round(float(row["Close"]), 2),
        })

    return {
        "ticker": TICKER,
        "currency": "EUR",
        "price_eur": round(last_close_eur, 2),
        "price_usd": None,
        "usd_to_eur": 1.0,
        "day_change_abs": round(day_change_abs, 2),
        "day_change_pct": round(day_change_pct, 2),
        "week52_high": round(w52_high, 2),
        "week52_low": round(w52_low, 2),
        "history": history,
        "data_source": "yfinance",
    }


def fetch_rss(url, source_label, timeout=15):
    """Fetch and parse an RSS/Atom feed. Returns list of {headline, date, url, source}."""
    items = []
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; TTWODashboard/1.0)",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            xml = resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  [warn] {source_label}: fetch failed — {e}", file=sys.stderr)
        return items

    # Quick-and-dirty RSS/Atom parser — robust enough for our 3 feeds
    entry_pattern = re.compile(r"<(item|entry)\b[^>]*>(.*?)</\1>", re.DOTALL | re.IGNORECASE)
    title_pattern = re.compile(r"<title[^>]*>(.*?)</title>", re.DOTALL | re.IGNORECASE)
    link_pattern = re.compile(r"<link[^>]*>(.*?)</link>", re.DOTALL | re.IGNORECASE)
    link_atom_pattern = re.compile(r'<link[^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)
    date_pattern = re.compile(
        r"<(pubDate|published|updated|dc:date)[^>]*>(.*?)</\1>",
        re.DOTALL | re.IGNORECASE,
    )

    for match in entry_pattern.finditer(xml):
        block = match.group(2)
        t_match = title_pattern.search(block)
        title = clean_html(t_match.group(1)) if t_match else ""
        if not title:
            continue

        l_match = link_pattern.search(block)
        link_text = ""
        if l_match:
            inner = l_match.group(1).strip()
            link_text = inner if inner.startswith("http") else ""
        if not link_text:
            l_atom = link_atom_pattern.search(block)
            if l_atom:
                link_text = l_atom.group(1)

        d_match = date_pattern.search(block)
        date_str = clean_html(d_match.group(2)) if d_match else ""

        items.append({
            "headline": title,
            "date": date_str,
            "url": link_text,
            "source": source_label,
        })

    return items


def clean_html(s):
    """Strip CDATA, tags, and decode HTML entities."""
    s = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", s, flags=re.DOTALL)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s).strip()
    return re.sub(r"\s+", " ", s)


def score_news_item(item):
    """Return (importance 1-10, sentiment 'bullish'/'bearish'/'neutral')."""
    h = item["headline"].lower()

    # Bearish signals
    bearish_words = ["delay", "delayed", "postpone", "lawsuit", "leak", "leaked",
                     "downgrade", "miss", "weak", "concern", "investigation", "fired"]
    # Bullish signals
    bullish_words = ["preorder", "record", "trailer", "release date", "launch",
                     "upgrade", "beat", "strong", "milestone", "unveil", "reveal",
                     "confirmed", "announced", "cover art"]
    # Importance keywords
    high_impact = ["gta vi", "gta 6", "grand theft auto", "take-two", "rockstar",
                   "release date", "delay", "trailer 3", "preorder", "earnings"]

    sentiment = "neutral"
    if any(w in h for w in bearish_words):
        sentiment = "bearish"
    elif any(w in h for w in bullish_words):
        sentiment = "bullish"

    importance = 5
    matches = sum(1 for w in high_impact if w in h)
    importance = min(10, 5 + matches * 2)

    return importance, sentiment


def fetch_news():
    """Pull all configured feeds in parallel, then dedupe / filter / score.

    Wall-clock is bounded by the single slowest feed rather than the sum.
    Per-feed health (status, latency, item count) is written to FEED_STATS
    for the data-quality strip on the dashboard.
    """
    global FEED_STATS
    FEED_STATS = {}

    now_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _fetch_one(feed):
        start = time.time()
        try:
            items = fetch_rss(feed["url"], feed["label"])
            elapsed = round(time.time() - start, 2)
            FEED_STATS[feed["label"]] = {
                "status": "ok" if items else "empty",
                "items_raw": len(items),
                "elapsed_sec": elapsed,
                "category": feed["category"],
                "last_fetched": now_iso,
            }
            # Tag items with source metadata for downstream scoring
            for it in items:
                it["_weight"] = feed["weight"]
                it["_category"] = feed["category"]
            return items
        except Exception as e:
            FEED_STATS[feed["label"]] = {
                "status": "error",
                "error": str(e)[:120],
                "items_raw": 0,
                "elapsed_sec": round(time.time() - start, 2),
                "category": feed["category"],
                "last_fetched": now_iso,
            }
            return []

    # Parallel fetch — 8 feeds in roughly the time of the slowest one
    all_items = []
    with ThreadPoolExecutor(max_workers=len(NEWS_FEEDS)) as ex:
        futures = {ex.submit(_fetch_one, feed): feed for feed in NEWS_FEEDS}
        for fut in as_completed(futures):
            all_items.extend(fut.result())

    # Per-feed summary line for the Action log
    for label, st in FEED_STATS.items():
        marker = "✓" if st["status"] == "ok" else ("·" if st["status"] == "empty" else "✗")
        msg = f"  {marker} {label:<18} {st['items_raw']:>3} raw items   {st['elapsed_sec']}s"
        if st["status"] == "error":
            msg += f"   [{st.get('error', '')}]"
        print(msg, file=sys.stderr)

    # Dedupe by first-80-chars of headline (case-insensitive)
    seen = set()
    unique = []
    for item in all_items:
        key = item["headline"].lower()[:80]
        if key not in seen and len(item["headline"]) > 8:
            seen.add(key)
            unique.append(item)

    # Filter to GTA VI / Take-Two relevant content. The non-gaming-press feeds
    # already filter at the URL level, but IGN / GamesRadar / PlayStation Blog
    # / Xbox Wire are broad gaming feeds, so we filter aggressively here.
    keywords = [
        "gta", "grand theft", "rockstar", "take-two", "take two", "ttwo",
        "vice city", "preorder", "pre-order",
    ]
    relevant = [
        it for it in unique
        if any(k in it["headline"].lower() for k in keywords)
    ]

    # Score with source-weighted importance. Financial sources (weight 1.4)
    # naturally outrank community noise (weight 0.7) for an investment view.
    scored = []
    for item in relevant:
        base_imp, sentiment = score_news_item(item)
        weighted = min(10, base_imp * item.get("_weight", 1.0))
        item["importance"] = max(1, round(weighted))
        item["sentiment"] = sentiment
        scored.append(item)

    # Sort: most important first, ties broken by recency
    scored.sort(key=lambda x: (x["importance"], x.get("date", "")), reverse=True)

    # Strip internal fields before returning — they're not for the dashboard
    for it in scored:
        it.pop("_weight", None)
        it.pop("_category", None)

    return scored[:12]


def _parse_news_date(s):
    """Best-effort parse of an RSS pubDate into ISO YYYY-MM-DD.

    RSS dates come in wildly inconsistent formats — RFC 822, RFC 3339, ISO 8601,
    "Mon, 23 Jun 2026 14:32:10 +0000", sometimes just a date. We try a few
    strategies and fall back to today's date if nothing parses.
    """
    if not s:
        return dt.date.today().isoformat()
    s = s.strip()
    # Try common RSS / Atom formats
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S GMT",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return dt.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    # Last resort: regex out a YYYY-MM-DD or DD/MM/YYYY
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return dt.date.today().isoformat()


def detect_dynamic_events(news_items, existing_detected):
    """Scan news for event patterns; return list of NEWLY-detected events.

    Already-detected events (by `key`) are skipped — once we've seen "Trailer 3",
    further headlines about it just feed the news panel, not the chart.

    Returns a list of event dicts ready to be appended to detected_events.json.
    Each event matches the canonical GTA_EVENTS shape so the dashboard can treat
    them uniformly on the chart.
    """
    new_events = []
    existing_keys = {e.get("key") for e in (existing_detected or [])}
    now_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for det in EVENT_DETECTORS:
        if det["key"] in existing_keys:
            continue  # already known — don't re-detect

        # Find ALL news items matching this detector
        matches = []
        for item in news_items or []:
            text = item.get("headline", "").lower()
            if not any(re.search(p, text, re.IGNORECASE) for p in det["patterns"]):
                continue
            # Apply exclusions (false-positive suppressors)
            excludes = det.get("exclude", [])
            if any(re.search(p, text, re.IGNORECASE) for p in excludes):
                continue
            matches.append(item)

        if not matches:
            continue

        # Confidence boost when multiple sources confirm the same event
        confirmation_count = len(matches)
        # Use the earliest matching news item as the event date
        matches.sort(key=lambda x: x.get("date", ""))
        anchor = matches[0]
        event_date = _parse_news_date(anchor.get("date"))

        event = {
            "key": det["key"],
            "date": event_date,
            "title": det["title"],
            "short": det["short"],
            "icon": det["icon"],
            "color": det["color"],
            "type": det["type"],
            "auto_detected": True,
            "detected_at": now_iso,
            "confirmations": confirmation_count,
            "source": anchor.get("source", "—"),
            "trigger_headline": anchor.get("headline", "")[:140],
            "trigger_url": anchor.get("url", ""),
            "impact": (
                f"Auto-detected from {confirmation_count} source"
                f"{'s' if confirmation_count > 1 else ''}: "
                f"{anchor.get('headline', '')[:90]}…"
            ),
        }
        new_events.append(event)

    return new_events


# ── Strategy engine ──────────────────────────────────────────────────────────

def compute_strategy(stock, news, today):
    """
    Determines BUY / HOLD / WAIT / SELL verdict for:
    - existing shares (hold/sell)
    - extra 500€ cash (buy/wait)
    - whole position (sell/hold)

    Based on: P/L, days-to-preorder, days-to-launch, recent news sentiment.
    """
    price = stock["price_eur"]
    current_value = SHARES * price
    pl_abs = current_value - INVESTED_EUR
    pl_pct = (pl_abs / INVESTED_EUR) * 100

    preorder_date = dt.date(2026, 6, 25)
    launch_date = dt.date(2026, 11, 19)
    days_to_preorder = (preorder_date - today).days
    days_to_launch = (launch_date - today).days

    bearish_count = sum(1 for n in news[:6] if n.get("sentiment") == "bearish")
    bullish_count = sum(1 for n in news[:6] if n.get("sentiment") == "bullish")

    # Distance to avg price (as % above/below)
    above_avg = ((price - AVG_PRICE_EUR) / AVG_PRICE_EUR) * 100

    # — Decision logic —

    # Sell-all verdict (almost always NO unless huge loss or huge win)
    if pl_pct < -22:
        sell_verdict, sell_conf = "REVIEW", 70
    elif pl_abs >= PROFIT_TARGET_EUR:
        sell_verdict, sell_conf = "TAKE PROFIT", 85
    else:
        sell_verdict, sell_conf = "NO", 90

    # Hold existing shares
    if pl_pct < -20:
        hold_verdict, hold_conf = "REVIEW", 60
    elif pl_pct > 25:
        hold_verdict, hold_conf = "TRIM", 70
    else:
        hold_verdict, hold_conf = "HOLD", 85

    # Invest extra 500€
    reasons = []
    if days_to_preorder >= 0 and days_to_preorder <= 7:
        invest_verdict, invest_conf = "WAIT", 80
        reasons.append(f"Preorder event in {days_to_preorder} days — high volatility window.")
    elif days_to_launch >= 0 and days_to_launch <= 21:
        invest_verdict, invest_conf = "WAIT", 75
        reasons.append("Launch window — 'sell the news' risk elevated.")
    elif above_avg > 5:
        invest_verdict, invest_conf = "WAIT", 70
        reasons.append(f"Price is {above_avg:.1f}% above your avg cost.")
    elif above_avg < -10:
        invest_verdict, invest_conf = "BUY 500€", 75
        reasons.append(f"Price is {abs(above_avg):.1f}% below your avg — better entry.")
    elif bearish_count > bullish_count + 1:
        invest_verdict, invest_conf = "WAIT", 65
        reasons.append("Recent news sentiment skews bearish.")
    else:
        invest_verdict, invest_conf = "HOLD CASH", 60
        reasons.append("No clear edge right now — keep dry powder.")

    if not reasons or len(reasons) < 4:
        if abs(above_avg) <= 1:
            reasons.append(f"Price is near your average cost ({AVG_PRICE_EUR} €).")
        if pl_abs < PROFIT_TARGET_EUR * 0.1:
            reasons.append("Risk/reward is not attractive right now.")
        if days_to_launch > 30:
            reasons.append("High 'buy the rumor, sell the news' risk before launch.")

    # Primary verdict (the big WAIT/BUY/HOLD shown at top)
    primary = invest_verdict
    primary_conf = invest_conf

    return {
        "primary": primary,
        "primary_confidence": primary_conf,
        "reasoning": reasons[:4],
        "actions": {
            "hold_shares": {"verdict": hold_verdict, "confidence": hold_conf},
            "invest_extra": {"verdict": invest_verdict, "confidence": invest_conf},
            "sell_all": {"verdict": sell_verdict, "confidence": sell_conf},
        },
        "metrics": {
            "days_to_preorder": days_to_preorder,
            "days_to_launch": days_to_launch,
            "above_avg_pct": round(above_avg, 2),
            "bearish_signals": bearish_count,
            "bullish_signals": bullish_count,
        },
    }


# ── Sample-mode generator (for local dev / first deploy) ─────────────────────

def generate_sample_history():
    """Realistic-looking TTWO daily price series from May 2024 to today."""
    random.seed(42)
    today = dt.date.today()
    start = today - dt.timedelta(days=400)

    history = []
    price = 148.0
    target_end = 215.0
    days_total = (today - start).days

    # Define a few "event" jumps for realism
    events = {
        (today - dt.timedelta(days=395)): -0.02,
        (today - dt.timedelta(days=300)): 0.04,
        (today - dt.timedelta(days=200)): 0.03,
        (today - dt.timedelta(days=120)): -0.05,  # mid-year dip
        (today - dt.timedelta(days=60)): 0.06,
        (today - dt.timedelta(days=14)): 0.02,
    }

    current = start
    day_idx = 0
    while current <= today:
        # Drift toward target
        drift = (target_end - price) / max(days_total - day_idx, 1) * 0.4
        noise = random.gauss(0, 0.015) * price
        shock = events.get(current, 0) * price
        price = max(50, price + drift + noise + shock)

        # Skip weekends
        if current.weekday() < 5:
            history.append({
                "date": current.strftime("%Y-%m-%d"),
                "close": round(price, 2),
            })

        current += dt.timedelta(days=1)
        day_idx += 1

    return history


def generate_sample_stock():
    """Realistic-looking stock data for demo mode."""
    history = generate_sample_history()
    last = history[-1]["close"]
    prev = history[-2]["close"] if len(history) > 1 else last

    closes = [h["close"] for h in history[-252:]]
    return {
        "ticker": TICKER,
        "currency": "EUR",
        "price_eur": last,
        "price_usd": round(last / 0.93, 2),
        "usd_to_eur": 0.93,
        "day_change_abs": round(last - prev, 2),
        "day_change_pct": round((last - prev) / prev * 100, 2),
        "week52_high": round(max(closes), 2),
        "week52_low": round(min(closes), 2),
        "history": history,
        "data_source": "sample",
    }


def generate_sample_news():
    """Plausible-looking demo headlines."""
    today = dt.date.today()
    return [
        {
            "headline": "Take-Two Interactive shares hit fresh 52-week high ahead of GTA VI preorder window",
            "date": (today - dt.timedelta(days=1)).isoformat(),
            "url": "https://example.com/1",
            "source": "Yahoo Finance",
            "importance": 9,
            "sentiment": "bullish",
        },
        {
            "headline": "Rockstar confirms GTA VI preorders open June 25 across PS5 and Xbox Series X|S",
            "date": (today - dt.timedelta(days=2)).isoformat(),
            "url": "https://example.com/2",
            "source": "Google News",
            "importance": 10,
            "sentiment": "bullish",
        },
        {
            "headline": "Analyst raises Take-Two price target to $300 citing GTA VI launch momentum",
            "date": (today - dt.timedelta(days=3)).isoformat(),
            "url": "https://example.com/3",
            "source": "Yahoo Finance",
            "importance": 8,
            "sentiment": "bullish",
        },
        {
            "headline": "Cover art reveal drives r/GTA6 subscriber growth past 3 million milestone",
            "date": (today - dt.timedelta(days=4)).isoformat(),
            "url": "https://example.com/4",
            "source": "r/GTA6",
            "importance": 6,
            "sentiment": "bullish",
        },
        {
            "headline": "Take-Two CFO signals 'measured' guidance update at next earnings call",
            "date": (today - dt.timedelta(days=5)).isoformat(),
            "url": "https://example.com/5",
            "source": "Yahoo Finance",
            "importance": 6,
            "sentiment": "neutral",
        },
        {
            "headline": "Industry watchers warn of typical 'sell the news' pattern around AAA launches",
            "date": (today - dt.timedelta(days=6)).isoformat(),
            "url": "https://example.com/6",
            "source": "Google News",
            "importance": 7,
            "sentiment": "bearish",
        },
    ]


# ── Sentiment + game-metric blocks ───────────────────────────────────────────

def compute_sentiment(news, days_to_launch):
    """Derive the 4 gauges (excitement, confidence, short-risk, long-upside)."""
    bullish = sum(1 for n in news if n.get("sentiment") == "bullish")
    bearish = sum(1 for n in news if n.get("sentiment") == "bearish")

    fan_excitement = min(10, 7 + bullish // 2)
    release_confidence = max(1, min(10, 9 - bearish))
    short_term_risk = min(10, 5 + (1 if days_to_launch < 60 else 0) + bearish)
    long_term_upside = min(10, 7 + bullish // 3)

    return {
        "fan_excitement": fan_excitement,
        "release_confidence": release_confidence,
        "short_term_risk": short_term_risk,
        "long_term_upside": long_term_upside,
    }


def game_metrics():
    """Static-ish game metrics block — overridden by news when concrete numbers appear."""
    return {
        "franchise_sales": "400M+",
        "copies_sold": "N/A",
        "copies_sold_note": "Not Released",
        "launch_estimate": "45M+",
        "launch_estimate_note": "First Weeks (Est.)",
        "preorder_numbers": "N/A",
        "preorder_note": "Waiting",
        "platforms": "PS5, XSX|S",
    }


def analyst_outlook():
    return {
        "price_target_low": 280,
        "price_target_high": 300,
        "currency": "USD",
        "rating": "STRONG BUY",
        "analysts_count": 32,
        "upside_low": 15,
        "upside_high": 25,
    }


def compute_countdowns(today, detected_events):
    """Return the five countdown chips shown above the catalyst row.

    Two are known dates (preorders, launch).
    Three are dynamic — date stays None until the matching detector fires:
      · Trailer 3       → trailer-3 detector
      · Reviews         → review-embargo detector
      · Online Mode     → online-mode detector

    The JS side handles live-ticking. We just publish the target dates.
    """
    detected_by_key = {e.get("key"): e for e in (detected_events or [])}

    def chip(key, label, icon, fixed_date=None, source_key=None):
        date = fixed_date
        if not date and source_key:
            ev = detected_by_key.get(source_key)
            if ev:
                date = ev.get("date")
        return {
            "key": key,
            "label": label,
            "icon": icon,
            "date": date,
            "known": bool(date),
        }

    return [
        chip("preorders",      "Preorders",   "🛒", fixed_date="2026-06-25"),
        chip("trailer-3",      "Trailer 3",   "🎥", source_key="trailer-3"),
        chip("review-embargo", "Reviews",     "📰", source_key="review-embargo"),
        chip("launch",         "Launch",      "🚀", fixed_date="2026-11-19"),
        chip("online-mode",    "Online Mode", "🌐", source_key="online-mode"),
    ]


# ── Main ─────────────────────────────────────────────────────────────────────

def build_snapshot(sample=False):
    today = dt.date.today()
    global STOCK_STATUS, FEED_STATS
    now_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if sample:
        print("Generating sample snapshot…", file=sys.stderr)
        stock = generate_sample_stock()
        news = generate_sample_news()
        # Stub status — clearly labeled so the dashboard shows "sample mode"
        STOCK_STATUS = {
            "source": "yfinance", "status": "sample", "elapsed_sec": 0.0,
            "category": "stock", "last_fetched": now_iso,
        }
        FEED_STATS = {
            feed["label"]: {
                "status": "sample", "items_raw": 0, "elapsed_sec": 0.0,
                "category": feed["category"], "last_fetched": now_iso,
            }
            for feed in NEWS_FEEDS
        }
    else:
        # Stock fetch — time it, record status for the data-quality strip
        print("Fetching live stock data…", file=sys.stderr)
        _start = time.time()
        try:
            stock = fetch_yfinance_stock()
            STOCK_STATUS = {
                "source": "yfinance", "status": "ok",
                "elapsed_sec": round(time.time() - _start, 2),
                "category": "stock", "last_fetched": now_iso,
            }
        except Exception as e:
            print(f"  ✗ yfinance failed: {e} — using sample stock", file=sys.stderr)
            stock = generate_sample_stock()
            STOCK_STATUS = {
                "source": "yfinance", "status": "error",
                "error": str(e)[:120],
                "elapsed_sec": round(time.time() - _start, 2),
                "category": "stock", "last_fetched": now_iso,
            }

        print("Fetching news feeds…", file=sys.stderr)
        news = fetch_news()    # FEED_STATS populated inside
        if not news:
            print("  No relevant news found — falling back to sample news.", file=sys.stderr)
            news = generate_sample_news()

    # Portfolio
    price = stock["price_eur"]
    current_value = SHARES * price
    pl_abs = current_value - INVESTED_EUR
    pl_pct = (pl_abs / INVESTED_EUR) * 100
    target_value = INVESTED_EUR + PROFIT_TARGET_EUR
    distance_to_target = target_value - current_value
    progress_pct = max(0, min(100, (pl_abs / PROFIT_TARGET_EUR) * 100))

    portfolio = {
        "shares": SHARES,
        "avg_price": AVG_PRICE_EUR,
        "invested": INVESTED_EUR,
        "extra_cash": EXTRA_CASH_EUR,
        "max_loss": MAX_LOSS_EUR,
        "profit_target": PROFIT_TARGET_EUR,
        "current_value": round(current_value, 2),
        "pl_abs": round(pl_abs, 2),
        "pl_pct": round(pl_pct, 2),
        "target_value": round(target_value, 2),
        "distance_to_target": round(distance_to_target, 2),
        "progress_pct": round(progress_pct, 2),
        "break_even_price": AVG_PRICE_EUR,
    }

    # Strategy
    strategy = compute_strategy(stock, news, today)

    # Sentiment
    days_to_launch = (dt.date(2026, 11, 19) - today).days
    sentiment = compute_sentiment(news, days_to_launch)

    # ── Dynamic event detection ────────────────────────────────────────────
    # Load previously-detected events from the last snapshot so they persist
    # across runs (a fresh "Trailer 3" detection should stay on the chart even
    # after the triggering news rolls off the feed).
    previously_detected = []
    if OUTPUT_PATH.exists():
        try:
            previous = json.loads(OUTPUT_PATH.read_text())
            previously_detected = previous.get("detected_events", []) or []
        except (json.JSONDecodeError, OSError):
            pass

    # Run detection on current news
    fresh_detections = detect_dynamic_events(news, previously_detected)
    if fresh_detections:
        print(f"  ✨ Detected {len(fresh_detections)} new event(s):", file=sys.stderr)
        for ev in fresh_detections:
            print(f"    + {ev['title']} ({ev['date']}) — {ev['confirmations']}× confirmed", file=sys.stderr)

    detected_events = previously_detected + fresh_detections

    # Merge canonical + detected events for the chart, sorted chronologically
    all_events = list(GTA_EVENTS) + detected_events
    all_events.sort(key=lambda e: e.get("date", "9999-12-31"))

    # Next catalyst is now computed from the merged list (a detected Trailer 3
    # may surface as the next catalyst if it's between now and the launch).
    upcoming = [e for e in all_events if e.get("date", "9999") >= today.isoformat()]
    next_catalyst = upcoming[0] if upcoming else None

    snapshot = {
        "updated_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_at_local": dt.datetime.now().strftime("%d %b %Y, %H:%M"),
        "stock": stock,
        "portfolio": portfolio,
        "strategy": strategy,
        "events": all_events,
        "detected_events": detected_events,
        "fresh_detections": [d["key"] for d in fresh_detections],
        "next_catalyst": next_catalyst,
        "news": news,
        "sentiment": sentiment,
        "game_metrics": game_metrics(),
        "analyst": analyst_outlook(),
        "countdowns": compute_countdowns(today, detected_events),
        "data_quality": {
            "stock": STOCK_STATUS,
            "feeds": [
                {"label": label, **stats}
                # Preserve the NEWS_FEEDS order so the UI shows them consistently
                for label, stats in (
                    (f["label"], FEED_STATS.get(f["label"], {
                        "status": "unknown", "items_raw": 0, "elapsed_sec": 0.0,
                        "category": f["category"], "last_fetched": now_iso,
                    }))
                    for f in NEWS_FEEDS
                )
            ],
            "summary": {
                "events_detected_total": len(detected_events),
                "fresh_this_run": len(fresh_detections),
                "news_items": len(news),
                "archive_count": (
                    # Count daily archives so the dashboard can show "47 days"
                    len(list((OUTPUT_PATH.parent / "archives").glob("snapshot_*.json")))
                    if (OUTPUT_PATH.parent / "archives").exists() else 0
                ),
                "last_run_utc": now_iso,
            },
        },
        "alerts": build_alerts(
            portfolio, strategy, news,
            detected_events=detected_events,
            fresh_keys={d["key"] for d in fresh_detections},
        ),
    }

    return snapshot


def build_alerts(portfolio, strategy, news, detected_events=None, fresh_keys=None):
    """Construct the alerts list.

    Two alert families:
      · Portfolio thresholds — profit, stop-loss, price-action triggers.
        Active when the underlying numeric condition is true.
      · Event watchers — one per dynamic-event detector key. Active when the
        detector has fired (i.e. the event appears in detected_events). The
        `fresh` flag marks alerts triggered on THIS run, so the UI can pulse.

    Returns a list sorted active-first, portfolio family before event family.
    """
    detected_events = detected_events or []
    fresh_keys = set(fresh_keys or [])
    detected_by_key = {e.get("key"): e for e in detected_events if e.get("key")}

    def event_alert(key, label, when_active, watching_label="Watching",
                    tone_active="orange", tone_watching="blue"):
        ev = detected_by_key.get(key)
        active = ev is not None
        out = {
            "label": label,
            "status": when_active if active else watching_label,
            "active": active,
            "tone": tone_active if active else tone_watching,
            "category": "event",
            "key": key,
            "fresh": key in fresh_keys,
        }
        if ev:
            out["source"] = ev.get("source", "")
            out["headline"] = (ev.get("trigger_headline") or "")[:90]
            out["triggered_at"] = ev.get("detected_at")
            out["url"] = ev.get("trigger_url", "")
        return out

    pl = portfolio.get("pl_abs", 0)

    alerts = [
        # ── Portfolio thresholds ─────────────────────────────────────────
        {
            "label": "Profit exceeds +500 €",
            "status": "Consider SELL" if pl >= 500 else "Watching",
            "active": pl >= 500,
            "tone": "purple" if pl >= 500 else "blue",
            "category": "portfolio",
            "key": "profit-target",
            "fresh": False,
        },
        {
            "label": "Stop Loss (−400 € from total capital)",
            "status": "REVIEW POSITION" if pl <= -400 else "Watching",
            "active": pl <= -400,
            "tone": "red" if pl <= -400 else "blue",
            "category": "portfolio",
            "key": "stop-loss",
            "fresh": False,
        },
        {
            "label": "TTWO falls 10% after preorder event",
            "status": "BUY 500 €",
            "active": False,    # requires price-window logic — left as watcher
            "tone": "blue",
            "category": "portfolio",
            "key": "post-preorder-dip",
            "fresh": False,
        },

        # ── Event watchers (one per dynamic-event detector) ──────────────
        event_alert("trailer-3",         "Trailer 3 released",          "RE-EVALUATE",      tone_active="purple"),
        event_alert("delay-announced",   "GTA VI delayed",              "REDUCE EXPOSURE",  tone_active="red"),
        event_alert("gameplay-reveal",   "Gameplay reveal",             "MOMENTUM SIGNAL",  tone_active="blue"),
        event_alert("collector-edition", "Collector Edition announced", "BULLISH SIGNAL",   tone_active="purple"),
        event_alert("review-embargo",    "Review embargo lifted",       "MONITOR REACTION", tone_active="orange"),
        event_alert("metacritic",        "Metacritic score available",  "ASSESS",           tone_active="orange"),
        event_alert("pc-version",        "PC version announced",        "BULLISH",          tone_active="green"),
        event_alert("online-mode",       "Online mode announced",       "BULLISH",          tone_active="green"),
        event_alert("launch-trailer",    "Launch trailer dropped",      "PEAK HYPE",        tone_active="purple"),
        event_alert("sales-milestone",   "Copies sold milestone",       "MILESTONE",        tone_active="green"),
        event_alert("preorder-record",   "Preorder record set",         "BULLISH",          tone_active="green"),
        event_alert("wishlist-rank",     "Wishlist ranking spike",      "INTEREST SIGNAL",  tone_active="blue"),
        event_alert("award-nomination",  "Award / nomination",          "RECOGNITION",      tone_active="orange"),
        event_alert("price-reveal",      "Price revealed",              "ASSESS",           tone_active="green"),
    ]

    # Sort: active first, then portfolio family before event family, then
    # fresh-this-run pulled up within the active group.
    alerts.sort(key=lambda a: (
        not a["active"],
        a.get("category") != "portfolio",
        not a.get("fresh", False),
    ))

    return alerts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true",
                        help="Generate a sample snapshot (no live fetching)")
    parser.add_argument("--out", default=str(OUTPUT_PATH),
                        help="Output path for snapshot.json")
    args = parser.parse_args()

    try:
        snap = build_snapshot(sample=args.sample)
    except Exception as e:
        if not args.sample:
            print(f"[error] Live fetch failed: {e}", file=sys.stderr)
            print("[fallback] Generating sample snapshot instead.", file=sys.stderr)
            snap = build_snapshot(sample=True)
        else:
            raise

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(snap, indent=2, ensure_ascii=False))

    print(f"\n✓ Wrote {out_path}")
    print(f"  Updated: {snap['updated_at']}")
    print(f"  Price:   {snap['stock']['price_eur']} EUR ({snap['stock']['data_source']})")
    print(f"  P/L:     {snap['portfolio']['pl_abs']:+.2f} EUR ({snap['portfolio']['pl_pct']:+.2f}%)")
    print(f"  Strategy: {snap['strategy']['primary']} ({snap['strategy']['primary_confidence']}%)")
    print(f"  News:    {len(snap['news'])} items")


if __name__ == "__main__":
    main()
