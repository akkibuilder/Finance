# TTWO Investment Dashboard

Personal investment-tracker for Take-Two Interactive (TTWO) with respect to the
GTA VI release. Auto-updates hourly via GitHub Actions, renders as a single-file
HTML dashboard served from GitHub Pages.

```
┌─ index.html ─────────┐    ┌─ scripts/update.py ──┐
│ Renders the          │    │ Fetches yfinance +   │
│ dashboard. Loads     │◄───│ Google News / Reddit │
│ data/snapshot.json.  │    │ / Yahoo Finance RSS. │
└──────────────────────┘    └──────────────────────┘
                                       ▲
                              ┌────────┴───────────┐
                              │ .github/workflows/ │
                              │ update.yml         │
                              │ (hourly cron)      │
                              └────────────────────┘
```

## What it shows

- Real-time TTWO price (USD → EUR converted at current FX)
- P/L on a 7.52397999-share position bought at €214.65 avg
- Distance to your +€500 profit target / progress ring
- 13-month price chart with vertical markers at GTA VI events:
  Trailer 1 · Trailer 2 · Cover Art · Preorders Open · Launch
- Filtered GTA VI news from 3 reliable feeds, scored for impact and sentiment
- A decision panel that recomputes BUY / HOLD / WAIT / SELL each refresh based
  on price, time-to-launch, and recent news sentiment
- Light (Apple Stocks) and dark (Bloomberg Terminal) themes · mobile responsive

## Quick local test (no GitHub needed)

Just open `index.html` in Chrome — it falls back to the embedded sample data
when `data/snapshot.json` isn't reachable, so the whole dashboard renders
correctly offline. This is also what you get on your office laptop before
deploying.

```
# If you want to regenerate the sample snapshot:
python3 scripts/update.py --sample
```

---

## Deploy to GitHub Pages (one-time setup, ~5 min)

### 1. Create the repo

Either upload these files to a new repo via the GitHub web UI, or:

```bash
cd path/to/ttwo-dashboard
git init -b main
git add .
git commit -m "Initial dashboard"
git remote add origin git@github.com:YOUR_USERNAME/ttwo-dashboard.git
git push -u origin main
```

Public or private — both work. Public is simpler (no Pages plan needed).

### 2. Enable GitHub Pages

In the repo: **Settings → Pages**

- Source: **Deploy from a branch**
- Branch: **main** · folder: **/ (root)**
- Save

Wait ~60 seconds. Your dashboard appears at:

```
https://YOUR_USERNAME.github.io/ttwo-dashboard/
```

### 3. Give Actions write permission

**Settings → Actions → General → Workflow permissions**

- Select **Read and write permissions**
- Save

This is required so the hourly job can commit the updated `snapshot.json` back
to the repo.

### 4. Trigger the first run

**Actions tab → "Update TTWO snapshot" → Run workflow → Run workflow**

Wait ~1 min. When it finishes you'll see a new commit `chore(data): hourly
snapshot · ...` in your repo, and refreshing the dashboard URL will show live
data (instead of the embedded sample).

The job then runs automatically every hour at xx:05 UTC.

---

## Customize your position

Edit `scripts/update.py`, lines near the top:

```python
SHARES         = 7.52397999
AVG_PRICE_EUR  = 214.65
INVESTED_EUR   = 1615.00
EXTRA_CASH_EUR = 500.00
MAX_LOSS_EUR   = 400.00
PROFIT_TARGET_EUR = 500.00
```

Commit the change — the next hourly run picks up new values automatically.
The dashboard's footer shows whether you're seeing live or demo data so you
know if it's working.

## Customize event dates

The five GTA VI events live in `scripts/update.py` as `GTA_EVENTS`. Add, remove,
or shift dates there. They'll show up automatically as vertical markers on
the chart and feed into the Next Major Catalyst / decision logic.

---

## Historical archives

The Action saves a dated copy of `snapshot.json` once per UTC day under
`data/archives/snapshot_YYYY-MM-DD.json`. Only the first run of each new
day creates an archive — subsequent runs the same day skip silently, so
the archive captures the early-morning state (which holds the previous
trading day's closing data).

```
data/
├── snapshot.json                     ← rewritten every hour
└── archives/
    ├── snapshot_2026-06-23.json      ← daily snapshots, never overwritten
    ├── snapshot_2026-06-24.json
    └── snapshot_2026-06-25.json
```

The dashboard's data-quality strip surfaces the running count as a small
"N days archived" pill once at least one archive exists.

**Storage cost:** each snapshot is ~30 KB. A year of daily archives is
~11 MB — well under any GitHub limit. No pruning needed for years.

**Future use:** a playback UI that scrubs through historical snapshots
(Bloomberg-style "rewind") could be built on top of these files. The
schema is stable, so older archives stay readable as the dashboard evolves.

---

## Email snapshot

Two ways:

**From the dashboard:** click the ✉ button in the header. This generates a
high-res PNG of the whole dashboard, downloads it, then opens your default
mail client pre-addressed to `akshaythakur1604@gmail.com` with current stats
in the body. (Browsers can't attach files to `mailto:` automatically, so you'll
attach the PNG manually from your Downloads folder.)

**Fully automatic** (optional, ~5 min more setup): Add a `send_email.py` step
to the Action that uses SMTP or the Resend API to email you the snapshot daily
or on big events. Ask me to wire that up when you're ready.

---

## Troubleshooting

**Dashboard shows "Demo data (no snapshot.json yet)"**
- The Action hasn't run successfully yet. Check the Actions tab for errors.
- Most common cause: Settings → Actions → Workflow permissions still on
  read-only. Switch to read-write (step 3 above).

**Stale data**
- The Action runs hourly, but if you opened the dashboard at 13:01 it might
  still show 12:05 data. Click the **Update** button to fetch the latest.

**yfinance occasionally fails**
- Yahoo throttles ~once per 100 requests. The script catches this and falls
  back to sample data so the dashboard never breaks; the next hourly run
  usually succeeds.

**Adjust update frequency**
- `.github/workflows/update.yml` line 5: change `'5 * * * *'` to your cron.
  GitHub's minimum is ~5 min, but they batch-schedule so anything under 15 min
  isn't reliable.

---

## Architecture summary

- **Frontend:** Single-file `index.html` (~95 KB) — vanilla JS, Plotly via CDN,
  html2canvas for email export. No build step. Embedded fallback data inside
  the file means it works offline / before first deploy.
- **Data layer:** `data/snapshot.json` (~25 KB) — the only thing that changes
  hourly. Single source of truth for the UI.
- **Fetcher:** `scripts/update.py` — pulls TTWO from yfinance, news from
  Google News RSS + Reddit r/GTA6 + Yahoo Finance RSS, computes portfolio
  P/L and strategy, writes `data/snapshot.json`.
- **Automation:** `.github/workflows/update.yml` — hourly cron, runs the
  fetcher, commits the new snapshot if it changed.

That's the whole system.
