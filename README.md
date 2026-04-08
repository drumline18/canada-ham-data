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

## Deploying to Railway

### 1. Create the project

1. Go to [railway.app](https://railway.app) and create a new project.
2. Choose **Deploy from GitHub repo** and select this repository.
3. Railway will detect `railway.toml` and use it automatically.

### 2. Add a persistent volume

The database and output files must survive service restarts.

1. In the Railway project, open your service → **Settings → Volumes**.
2. Click **Add Volume**, set the **Mount Path** to `/data`, and save.

### 3. Set environment variables

In the Railway service → **Variables**, add:

| Variable | Value | Notes |
|----------|-------|-------|
| `OUTPUT_DIR` | `/data/output` | Where CSVs and JSON files are written |
| `DB_PATH` | `/data/ham.db` | SQLite database file |
| `DATA_URL` | `https://apc-cap.ic.gc.ca/datafiles/amateur_delim.zip` | Optional — this is the default |

`PORT` is set automatically by Railway; you do not need to add it.

### 4. Deploy

Click **Deploy** (or push a commit). Railway will:

1. Install dependencies from `requirements.txt`.
2. Start `python server.py` (via `railway.toml`).
3. On first boot, detect that the output is missing and immediately run the full download + analysis pipeline in a background thread.
4. Start the APScheduler cron job that repeats the pipeline daily at **03:00 UTC**.

The `/status` endpoint (used as the Railway health check) returns JSON with the last run timestamp and row count:

```json
{
  "updated_at": "2026-04-08T03:00:01Z",
  "row_count": 91500,
  "source_url": "https://apc-cap.ic.gc.ca/datafiles/amateur_delim.zip",
  "output_dir": "/data/output"
}
```

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

The server skips the startup download if `output/last_updated.json` exists and is less than 25 hours old.

### Run analysis only (no download, uses local file)

```bash
python analyze_amateur.py --input amateur_delim.txt --output-dir output
```

---

## Environment variable reference

| Variable | Default | Description |
|----------|---------|-------------|
| `OUTPUT_DIR` | `output` | Directory for CSV and JSON output files |
| `DB_PATH` | `<OUTPUT_DIR>/../ham.db` | SQLite database path |
| `DATA_URL` | ISED ZIP URL | Source data URL |
| `PORT` | `8080` | HTTP port (Railway sets this automatically) |
