#!/usr/bin/env python3
"""
SQLite persistence layer for the Amateur Radio Data Dashboard.

Tables:
  snapshots  — one row per daily run
  records    — current state of every callsign (upserted each run)
  changes    — append-only log of new / removed / qual_upgrade / qual_downgrade events
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


QUAL_FIELDS = ["qual_a", "qual_b", "qual_c", "qual_d", "qual_e"]
QUAL_LETTERS = ["A", "B", "C", "D", "E"]
RECENT_DAYS = 30


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS snapshots (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    taken_at   TEXT    NOT NULL,
    row_count  INTEGER NOT NULL,
    source_url TEXT
);

CREATE TABLE IF NOT EXISTS records (
    callsign    TEXT PRIMARY KEY,
    prov_cd     TEXT,
    city        TEXT,
    qual_a      TEXT,
    qual_b      TEXT,
    qual_c      TEXT,
    qual_d      TEXT,
    qual_e      TEXT,
    snapshot_id INTEGER REFERENCES snapshots(id)
);

CREATE TABLE IF NOT EXISTS changes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER REFERENCES snapshots(id),
    detected_at TEXT    NOT NULL,
    callsign    TEXT    NOT NULL,
    change_type TEXT    NOT NULL,
    old_quals   TEXT,
    new_quals   TEXT,
    prov_cd     TEXT
);

CREATE INDEX IF NOT EXISTS changes_detected_at ON changes(detected_at);
CREATE INDEX IF NOT EXISTS changes_callsign     ON changes(callsign);
"""


def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(_DDL)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _qual_string(row: dict) -> str:
    """Return a compact qual string like 'AD' or '' from a record dict."""
    return "".join(
        letter
        for field, letter in zip(QUAL_FIELDS, QUAL_LETTERS)
        if (row.get(field) or "").strip()
    )


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

def get_current_records(conn: sqlite3.Connection) -> Dict[str, sqlite3.Row]:
    """Return all rows in the records table keyed by callsign."""
    cur = conn.execute("SELECT * FROM records")
    return {row["callsign"]: row for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def insert_snapshot(
    conn: sqlite3.Connection,
    taken_at: str,
    row_count: int,
    source_url: str,
) -> int:
    cur = conn.execute(
        "INSERT INTO snapshots (taken_at, row_count, source_url) VALUES (?, ?, ?)",
        (taken_at, row_count, source_url),
    )
    conn.commit()
    return cur.lastrowid


def apply_diff(
    conn: sqlite3.Connection,
    snapshot_id: int,
    old_records: Dict[str, sqlite3.Row],
    new_rows: List[dict],
) -> None:
    """
    Diff new_rows against old_records, write changes, and upsert records table.

    new_rows is the raw list of dicts from analyze_amateur.read_rows().
    """
    detected_at = datetime.now(timezone.utc).isoformat()
    new_by_callsign: Dict[str, dict] = {
        r["callsign"].strip().upper(): r for r in new_rows if r.get("callsign")
    }

    changes_to_insert: List[tuple] = []

    # New and modified callsigns
    for callsign, new_row in new_by_callsign.items():
        new_quals = _qual_string(new_row)
        prov = (new_row.get("prov_cd") or "").strip().upper()

        if callsign not in old_records:
            changes_to_insert.append(
                (snapshot_id, detected_at, callsign, "new", None, new_quals or None, prov)
            )
        else:
            old_quals = _qual_string(old_records[callsign])
            if new_quals != old_quals:
                old_set = set(old_quals)
                new_set = set(new_quals)
                if new_set > old_set:
                    change_type = "qual_upgrade"
                elif new_set < old_set:
                    change_type = "qual_downgrade"
                else:
                    change_type = "qual_upgrade" if len(new_quals) > len(old_quals) else "qual_downgrade"
                changes_to_insert.append(
                    (snapshot_id, detected_at, callsign, change_type, old_quals or None, new_quals or None, prov)
                )

    # Removed callsigns
    for callsign in old_records:
        if callsign not in new_by_callsign:
            old = old_records[callsign]
            old_quals = _qual_string(old)
            prov = (old["prov_cd"] or "").strip().upper() if old["prov_cd"] else ""
            changes_to_insert.append(
                (snapshot_id, detected_at, callsign, "removed", old_quals or None, None, prov)
            )

    if changes_to_insert:
        conn.executemany(
            """INSERT INTO changes
               (snapshot_id, detected_at, callsign, change_type, old_quals, new_quals, prov_cd)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            changes_to_insert,
        )

    # Upsert current records
    conn.executemany(
        """INSERT INTO records (callsign, prov_cd, city, qual_a, qual_b, qual_c, qual_d, qual_e, snapshot_id)
           VALUES (:callsign, :prov_cd, :city, :qual_a, :qual_b, :qual_c, :qual_d, :qual_e, :snapshot_id)
           ON CONFLICT(callsign) DO UPDATE SET
               prov_cd     = excluded.prov_cd,
               city        = excluded.city,
               qual_a      = excluded.qual_a,
               qual_b      = excluded.qual_b,
               qual_c      = excluded.qual_c,
               qual_d      = excluded.qual_d,
               qual_e      = excluded.qual_e,
               snapshot_id = excluded.snapshot_id""",
        [
            {
                "callsign":    r.get("callsign", "").strip().upper(),
                "prov_cd":     (r.get("prov_cd") or "").strip().upper() or None,
                "city":        (r.get("city") or "").strip().upper() or None,
                "qual_a":      (r.get("qual_a") or "").strip() or None,
                "qual_b":      (r.get("qual_b") or "").strip() or None,
                "qual_c":      (r.get("qual_c") or "").strip() or None,
                "qual_d":      (r.get("qual_d") or "").strip() or None,
                "qual_e":      (r.get("qual_e") or "").strip() or None,
                "snapshot_id": snapshot_id,
            }
            for r in new_rows
            if r.get("callsign")
        ],
    )

    # Remove callsigns that have disappeared — use a temp table to avoid
    # SQLite's 999-variable limit when the dataset has tens of thousands of rows.
    if new_by_callsign:
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _keep_callsigns (callsign TEXT PRIMARY KEY)")
        conn.execute("DELETE FROM _keep_callsigns")
        conn.executemany(
            "INSERT OR IGNORE INTO _keep_callsigns VALUES (?)",
            [(c,) for c in new_by_callsign.keys()],
        )
        conn.execute("DELETE FROM records WHERE callsign NOT IN (SELECT callsign FROM _keep_callsigns)")
        conn.execute("DROP TABLE IF EXISTS _keep_callsigns")

    conn.commit()
    print(
        f"[db] Snapshot {snapshot_id}: "
        f"{sum(1 for c in changes_to_insert if c[3] == 'new')} new, "
        f"{sum(1 for c in changes_to_insert if c[3] == 'removed')} removed, "
        f"{sum(1 for c in changes_to_insert if 'qual' in c[3])} qual changes"
    )


# ---------------------------------------------------------------------------
# Output files
# ---------------------------------------------------------------------------

def write_output_files(conn: sqlite3.Connection, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_recent_changes(conn, output_dir)
    _write_snapshot_history(conn, output_dir)


def _write_recent_changes(conn: sqlite3.Connection, output_dir: Path) -> None:
    cur = conn.execute(
        """SELECT callsign, change_type, old_quals, new_quals, prov_cd, detected_at
           FROM changes
           WHERE detected_at >= datetime('now', ?)
           ORDER BY detected_at DESC""",
        (f"-{RECENT_DAYS} days",),
    )
    rows = cur.fetchall()

    grouped: dict = {"new": [], "removed": [], "qual_upgrade": [], "qual_downgrade": []}
    for r in rows:
        entry = {
            "callsign":    r["callsign"],
            "detected_at": r["detected_at"],
            "prov_cd":     r["prov_cd"] or "",
        }
        if r["old_quals"] is not None:
            entry["old_quals"] = r["old_quals"]
        if r["new_quals"] is not None:
            entry["new_quals"] = r["new_quals"]
        grouped.setdefault(r["change_type"], []).append(entry)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days":         RECENT_DAYS,
        **grouped,
    }
    path = output_dir / "recent_changes.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"[db] Wrote {path}")


def _write_snapshot_history(conn: sqlite3.Connection, output_dir: Path) -> None:
    cur = conn.execute("SELECT taken_at, row_count FROM snapshots ORDER BY taken_at")
    rows = [{"taken_at": r["taken_at"], "row_count": r["row_count"]} for r in cur.fetchall()]
    path = output_dir / "snapshot_history.json"
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"[db] Wrote {path}")
