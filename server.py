#!/usr/bin/env python3
"""
Flask web server for the Amateur Radio Data Dashboard.

Serves:
  /              → dashboard/index.html
  /output/<file> → output CSV files
  /status        → JSON with last_updated info

Optional manual calls (all use ?token=TRIGGER_TOKEN if TRIGGER_TOKEN is set in env):
  /trigger         → full ISED download + diff + output files
  /rebuild-output  → refresh CSV/JSON from SQLite only (no download; good for quick tests)
  /cloudflare-purge → purge Cloudflare edge cache (needs CLOUDFLARE_ZONE_ID + CLOUDFLARE_API_TOKEN)

APScheduler runs run_analysis.download_and_analyze() daily at 03:00 UTC.
On startup the analysis runs automatically if the output is missing or older
than 25 hours.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, abort, jsonify, request, send_from_directory

import db as db_module
import run_analysis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "output"))
# Default: database next to generated CSVs (same folder as OUTPUT_DIR).
DB_PATH = Path(os.environ.get("DB_PATH", str(OUTPUT_DIR / "ham.db")))
PORT = int(os.environ.get("PORT", 8080))
TRIGGER_TOKEN = os.environ.get("TRIGGER_TOKEN", "")
STALE_HOURS = 25  # re-run if last update is older than this
READY_OUTPUT_FILES = (
    "province_summary.csv",
    "qualification_combo_summary.csv",
    "qualification_by_province.csv",
    "data_quality_summary.csv",
    "city_summary.csv",
    "top_clubs.csv",
    "club_summary.csv",
)

BASE_DIR = Path(__file__).parent
DASHBOARD_DIR = BASE_DIR / "dashboard"

app = Flask(__name__, static_folder=None)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory(DASHBOARD_DIR, "index.html")


@app.route("/<path:filename>")
def dashboard_static(filename: str):
    """Serve dashboard static assets (main.js, styles.css, etc.)."""
    safe = DASHBOARD_DIR / filename
    if not safe.resolve().is_relative_to(DASHBOARD_DIR.resolve()):
        abort(403)
    return send_from_directory(DASHBOARD_DIR, filename)


@app.route("/output/<path:filename>")
def output_file(filename: str):
    """Serve generated CSV / JSON files."""
    if not OUTPUT_DIR.exists():
        abort(503, description="Output not yet available — analysis pending.")
    response = send_from_directory(OUTPUT_DIR.resolve(), filename, max_age=0)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


def _check_trigger_token() -> bool:
    if not TRIGGER_TOKEN:
        return False
    if request.args.get("token") != TRIGGER_TOKEN:
        return False
    return True


@app.route("/trigger")
def trigger():
    """Manually kick off a full download + diff + output write. Requires ?token=<TRIGGER_TOKEN>."""
    if not TRIGGER_TOKEN:
        abort(403, description="TRIGGER_TOKEN env var not set — endpoint disabled.")
    if not _check_trigger_token():
        abort(403, description="Invalid token.")
    state = _get_analysis_state()
    if state["running"]:
        return jsonify({"queued": False, "reason": "Analysis already running."}), 409
    thread = threading.Thread(target=_run_analysis_safe, daemon=True)
    thread.start()
    log.info("Manual trigger: analysis started.")
    return jsonify({"queued": True})


@app.route("/rebuild-output")
def rebuild_output():
    """
    Regenerate dashboard CSV/JSON from the local SQLite DB (no ISED download).
    For testing KPI/output logic or refreshing files after a deploy. Same token as /trigger.
    """
    if not TRIGGER_TOKEN:
        abort(403, description="TRIGGER_TOKEN env var not set — endpoint disabled.")
    if not _check_trigger_token():
        abort(403, description="Invalid token.")
    if not DB_PATH.exists():
        abort(503, description="No database at DB_PATH — run analysis at least once.")
    if not _analysis_lock.acquire(blocking=False):
        return jsonify({"ok": False, "reason": "Analysis in progress; try again shortly."}), 409
    try:
        _sync_outputs_from_db()
    finally:
        _analysis_lock.release()
    log.info("Manual rebuild-output: JSON/CSVs refreshed from DB.")
    return jsonify({"ok": True, "rebuilt": True})


@app.route("/cloudflare-purge")
def cloudflare_purge():
    """
    Optional: purge Cloudflare edge cache for the whole zone. Does not re-run analysis.
    Set CLOUDFLARE_ZONE_ID and CLOUDFLARE_API_TOKEN (Cache Purge: Edit). Same ?token= as /trigger.
    """
    if not TRIGGER_TOKEN:
        abort(403, description="TRIGGER_TOKEN env var not set — endpoint disabled.")
    if not _check_trigger_token():
        abort(403, description="Invalid token.")
    zone_id = os.environ.get("CLOUDFLARE_ZONE_ID", "").strip()
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN", "").strip()
    if not zone_id or not api_token:
        return jsonify(
            {
                "ok": False,
                "error": "Set CLOUDFLARE_ZONE_ID and CLOUDFLARE_API_TOKEN in the app environment, "
                "or purge the cache in the Cloudflare dashboard (Caching → Purge).",
            }
        ), 400
    try:
        r = requests.post(
            f"https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache",
            headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
            json={"purge_everything": True},
            timeout=30,
        )
    except OSError as exc:
        log.warning("cloudflare-purge: request failed: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 502
    if not r.ok:
        log.warning("cloudflare-purge: HTTP %s: %s", r.status_code, r.text[:500])
        return jsonify({"ok": False, "error": r.text, "http_status": r.status_code}), 502
    body = r.json() if r.content else {}
    if not body.get("success", True):
        return jsonify({"ok": False, "error": body}), 502
    log.info("Manual cloudflare-purge: edge cache purged (purge_everything).")
    return jsonify({"ok": True, "purged": True, "result": body.get("result")})


@app.route("/status")
def status():
    last_updated_path = OUTPUT_DIR / "last_updated.json"
    if last_updated_path.exists():
        data = json.loads(last_updated_path.read_text(encoding="utf-8"))
    else:
        data = {"updated_at": None, "row_count": None, "source_url": None}
    state = _get_analysis_state()
    data["ready"] = _is_output_ready()
    data["analysis_running"] = state["running"]
    data["last_started_at"] = state["last_started_at"]
    data["last_completed_at"] = state["last_completed_at"]
    data["last_error"] = "Analysis failed. Check server logs." if state["last_error"] else None
    return jsonify(data)


# ---------------------------------------------------------------------------
# Analysis runner
# ---------------------------------------------------------------------------

_analysis_lock = threading.Lock()
_analysis_state_lock = threading.Lock()
_analysis_state = {
    "running": False,
    "last_started_at": None,
    "last_completed_at": None,
    "last_error": None,
}


def _set_analysis_state(**changes: object) -> None:
    with _analysis_state_lock:
        _analysis_state.update(changes)


def _get_analysis_state() -> dict:
    with _analysis_state_lock:
        return dict(_analysis_state)


def _is_output_ready() -> bool:
    return all((OUTPUT_DIR / filename).exists() for filename in READY_OUTPUT_FILES)


def _run_analysis_safe() -> None:
    """Run analysis in a thread-safe way, logging any errors."""
    if not _analysis_lock.acquire(blocking=False):
        log.info("Analysis already running, skipping.")
        return
    started_at = datetime.now(timezone.utc).isoformat()
    _set_analysis_state(running=True, last_started_at=started_at, last_error=None)
    try:
        log.info("Starting analysis...")
        run_analysis.download_and_analyze(
            data_url=run_analysis.DATA_URL,
            output_dir=OUTPUT_DIR,
            db_path=DB_PATH,
        )
        _set_analysis_state(
            running=False,
            last_completed_at=datetime.now(timezone.utc).isoformat(),
            last_error=None,
        )
        log.info("Analysis complete.")
    except Exception as exc:
        _set_analysis_state(
            running=False,
            last_completed_at=datetime.now(timezone.utc).isoformat(),
            last_error=str(exc),
        )
        log.exception("Analysis failed.")
    finally:
        _analysis_lock.release()


def _is_output_stale() -> bool:
    last_updated_path = OUTPUT_DIR / "last_updated.json"
    if not last_updated_path.exists():
        return True
    try:
        data = json.loads(last_updated_path.read_text(encoding="utf-8"))
        updated_at = datetime.fromisoformat(data["updated_at"])
        age_hours = (datetime.now(timezone.utc) - updated_at).total_seconds() / 3600
        return age_hours > STALE_HOURS
    except Exception:
        return True


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def _sync_outputs_from_db() -> None:
    """Prune baseline noise in SQLite, then rewrite JSON/CSV outputs from DB."""
    if not DB_PATH.exists():
        return
    conn = db_module.init_db(DB_PATH)
    try:
        db_module.write_output_files(conn, OUTPUT_DIR)
    finally:
        conn.close()


def _start_scheduler() -> None:
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(_run_analysis_safe, "cron", hour=3, minute=0)
    scheduler.start()
    log.info("Scheduler started - analysis will run daily at 03:00 UTC.")


_startup_done = False


def init_background_tasks() -> None:
    """
    One-time boot: output dir, DB→JSON sync, optional analysis thread, cron.
    Runs on import so Gunicorn (production) and `python server.py` both see it.
    Use a single Gunicorn worker so APScheduler is not duplicated.
    """
    global _startup_done
    if _startup_done:
        return
    _startup_done = True

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _sync_outputs_from_db()

    if _is_output_stale():
        log.info("Output is missing or stale - starting background analysis.")
        thread = threading.Thread(target=_run_analysis_safe, daemon=True)
        thread.start()
    else:
        log.info("Output is fresh, skipping startup analysis.")

    _start_scheduler()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

init_background_tasks()


if __name__ == "__main__":
    log.info("Starting Flask development server on port %d (use gunicorn on Railway).", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True)
