#!/usr/bin/env python3
"""
Download the ISED amateur radio ZIP, extract the delimited file,
run the analysis, and write a last_updated.json timestamp.

Usage:
    python run_analysis.py
    DATA_URL=https://... OUTPUT_DIR=/data/output python run_analysis.py
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

import analyze_amateur

DATA_URL = os.environ.get(
    "DATA_URL",
    "https://apc-cap.ic.gc.ca/datafiles/amateur_delim.zip",
)
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "output"))

_EXTRACTED_NAME = "amateur_delim.txt"


def download_and_analyze(data_url: str = DATA_URL, output_dir: Path = OUTPUT_DIR) -> None:
    print(f"[run_analysis] Downloading {data_url} …")
    response = requests.get(data_url, timeout=120, stream=True)
    response.raise_for_status()

    with tempfile.TemporaryDirectory() as tmp:
        zip_path = Path(tmp) / "amateur_delim.zip"
        with zip_path.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1 << 16):
                fh.write(chunk)
        print(f"[run_analysis] Download complete ({zip_path.stat().st_size:,} bytes)")

        with zipfile.ZipFile(zip_path) as zf:
            members = zf.namelist()
            # Accept any .txt member, preferring the canonical name
            txt_member = next(
                (m for m in members if m.lower() == _EXTRACTED_NAME.lower()),
                next((m for m in members if m.lower().endswith(".txt")), None),
            )
            if txt_member is None:
                raise RuntimeError(f"No .txt file found in ZIP. Members: {members}")
            zf.extract(txt_member, tmp)
            txt_path = Path(tmp) / txt_member
            print(f"[run_analysis] Extracted {txt_member}")

        row_count = analyze_amateur.run(txt_path, output_dir)

    timestamp = datetime.now(timezone.utc).isoformat()
    last_updated = {
        "updated_at": timestamp,
        "row_count": row_count,
        "source_url": data_url,
    }
    last_updated_path = output_dir / "last_updated.json"
    last_updated_path.write_text(json.dumps(last_updated, indent=2), encoding="utf-8")
    print(f"[run_analysis] Wrote {last_updated_path} — {timestamp}")


if __name__ == "__main__":
    try:
        download_and_analyze()
    except Exception as exc:
        print(f"[run_analysis] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
