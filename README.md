# Amateur Radio Data Dashboard

A self-updating dashboard that analyzes the [ISED amateur radio callsign database](https://apc-cap.ic.gc.ca/datafiles/amateur_delim.zip) for Canada, tracks daily changes, and serves an interactive web UI.

## Features

- Downloads fresh data daily from ISED and re-analyzes it automatically
- Tracks new callsigns, removed callsigns, and qualification upgrades/downgrades over time
- Stores change history in a SQLite database
- Dashboard with charts, tables, and a "Recent Changes" panel showing what shifted since the last run

## Project structure

```
analyze_amateur.py   Core analysis: reads amateur_delim.txt, writes 7 summary CSVs
db.py                SQLite layer: snapshots, records, change log
run_analysis.py      Pipeline: download ZIP → extract → analyze → diff → write JSON
server.py            Flask server + APScheduler (daily at 03:00 UTC)
dashboard/
  index.html         Dashboard shell
  main.js            Data loading, Chart.js charts, tables
  styles.css         Dark theme
requirements.txt     Python dependencies
runtime.txt          Python version hint for Nixpacks / Railway
railway.toml         Railway deployment config
Procfile             Fallback start command
```

Output files (written to `OUTPUT_DIR` after each run):

| File | Description |
|------|-------------|
| `province_summary.csv` | Licensee counts by province |
| `city_summary.csv` | Top 500 cities by licensee count |
| `qualification_combo_summary.csv` | Qualification letter combinations |
| `qualification_by_province.csv` | Qual breakdown per province |
| `club_summary.csv` | Aggregate club metrics |
| `top_clubs.csv` | Top 300 club names by record count |
| `data_quality_summary.csv` | Missing-field and duplicate stats |
| `recent_changes.json` | New / removed / upgraded callsigns (last 30 days) |
| `snapshot_history.json` | Daily row counts for the trend chart |
| `last_updated.json` | Timestamp and row count of the last run |

---

## Deploying to Railway (step by step)

### Step 1 — Push this repo to GitHub

Railway deploys from a Git remote. Commit your work, create a GitHub repository if needed, and push `main` (or your default branch).

### Step 2 — Create a Railway project

1. Open [railway.app](https://railway.app) and sign in.
2. **New project** → **Deploy from GitHub** → authorize Railway and pick this repository.
3. Railway reads **`railway.toml`**: build uses Nixpacks, production start is **Gunicorn** (`server:app`, **1 worker** so the daily scheduler runs once), health check is **`GET /status`**. Locally you can still run `python server.py` (Flask’s dev server).

### Step 3 — Add a persistent volume

The SQLite DB and generated CSV/JSON files must live on a volume or they disappear on every redeploy.

1. Open your **service** (not the project root) → **Settings** → **Volumes**.
2. **Add volume** → **Mount path**: `/data` → Add.
3. Everything under `/data` persists across deploys.

### Step 4 — Set environment variables

Open the service → **Variables** → **New variable**.

| Variable | Suggested value | Required? |
|----------|-----------------|-----------|
| `OUTPUT_DIR` | `/data/output` | **Yes** — must point inside the volume. |
| `DB_PATH` | *(omit)* | Optional. If omitted, the app uses **`$OUTPUT_DIR/ham.db`** (e.g. `/data/output/ham.db`). Set explicitly only if you want the DB elsewhere, e.g. `/data/ham.db`. |
| `DATA_URL` | `https://apc-cap.ic.gc.ca/datafiles/amateur_delim.zip` | Optional (same default in code). |

Do **not** set `PORT` — Railway injects it; `server.py` already binds `0.0.0.0` and reads `PORT`.

After saving variables, trigger a **Redeploy** so the service picks them up.

### Step 5 — Deploy and wait for data

1. Railway builds from `requirements.txt` and `runtime.txt`, then runs `python server.py`.
2. The first boot may take **several minutes**: the app starts Flask (health check passes), then a **background thread** downloads the ISED ZIP and runs the full analysis if output is missing or older than 25 hours.
3. Watch **Deployments** → **View logs** for `[run_analysis]` and `[db]` lines.
4. In the browser, open the **public URL** (service → **Settings** → **Networking** → **Generate domain**). The dashboard shows “Preparing dashboard data” until `/status` reports `"ready": true`.

### Step 6 — Daily updates

`server.py` schedules **`run_analysis.download_and_analyze`** every day at **03:00 UTC**. No extra Railway cron is required.

### Troubleshooting

- **`output/` directory not found` / 503 on `/output/...`**: Analysis not finished or failed — check logs and `last_error` in `/status`.
- **Empty dashboard after redeploy without a volume**: Add the `/data` volume and set `OUTPUT_DIR=/data/output` as above, then run a fresh analysis (or restore files from backup).

Example `/status` response after a successful run:

```json
{
  "updated_at": "2026-04-08T03:00:01+00:00",
  "row_count": 91500,
  "source_url": "https://apc-cap.ic.gc.ca/datafiles/amateur_delim.zip",
  "output_dir": "/data/output",
  "ready": true,
  "analysis_running": false,
  "last_started_at": "2026-04-08T03:00:00+00:00",
  "last_completed_at": "2026-04-08T03:00:01+00:00",
  "last_error": null
}
```

During the first deploy, `ready` will be `false` until the core dashboard CSV files have been generated. The web UI shows a "Preparing dashboard data" panel and polls `/status` until those core files are ready. Historical JSON files such as `snapshot_history.json` and `recent_changes.json` are treated as optional by the UI.

---

## Running locally

### First-time setup

```bash
pip install -r requirements.txt
```

### Run the full pipeline (download + analyze + DB diff)

```bash
OUTPUT_DIR=output DB_PATH=output/ham.db python run_analysis.py
```

This downloads the live ZIP, analyzes it, updates the SQLite DB, and writes all output files to `output/`.

### Start the dashboard server

```bash
OUTPUT_DIR=output DB_PATH=output/ham.db python server.py
```

Open [http://localhost:8080](http://localhost:8080).

The server skips the startup download if `output/last_updated.json` exists and is less than 25 hours old. If the output is missing, the UI will stay on a waiting state until the first analysis run finishes.

### Run analysis only (no download, uses local file)

```bash
python analyze_amateur.py --input amateur_delim.txt --output-dir output
```

---

## Environment variable reference

| Variable | Default | Description |
|----------|---------|-------------|
| `OUTPUT_DIR` | `output` | Directory for CSV and JSON output files |
| `DB_PATH` | `<OUTPUT_DIR>/ham.db` | SQLite database path |
| `DATA_URL` | ISED ZIP URL | Source data URL |
| `PORT` | `8080` | HTTP port (Railway sets this automatically) |
