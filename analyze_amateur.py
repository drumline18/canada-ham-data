#!/usr/bin/env python3
"""
Quick analysis for the amateur_delim dataset.

Outputs CSV summaries to ./output:
- province_summary.csv
- city_summary.csv
- qualification_combo_summary.csv
- qualification_by_province.csv
- club_summary.csv
- data_quality_summary.csv
- top_clubs.csv
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List


QUAL_FIELDS = ["qual_a", "qual_b", "qual_c", "qual_d", "qual_e"]
CORE_FIELDS = [
    "callsign",
    "first_name",
    "surname",
    "address_line",
    "city",
    "prov_cd",
    "postal_code",
] + QUAL_FIELDS + [
    "club_name",
    "club_name_2",
    "club_address",
    "club_city",
    "club_prov_cd",
    "club_postal_code",
]


def norm_text(value: str) -> str:
    return (value or "").strip()


def norm_upper(value: str) -> str:
    return norm_text(value).upper()


def postal_fsa(postal_code: str) -> str:
    compact = norm_upper(postal_code).replace(" ", "")
    return compact[:3] if len(compact) >= 3 else ""


def qualification_combo(row: Dict[str, str]) -> str:
    flags: List[str] = []
    for field, letter in zip(QUAL_FIELDS, ["A", "B", "C", "D", "E"]):
        if norm_text(row.get(field, "")):
            flags.append(letter)
    return "".join(flags) if flags else "(none)"


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: Iterable[str], rows: Iterable[Dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_rows(input_path: Path) -> List[Dict[str, str]]:
    with input_path.open("r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter=";")
        missing = [c for c in CORE_FIELDS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"Missing expected columns: {missing}")
        return [dict(row) for row in reader]


def build_summaries(rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, object]]]:
    total = len(rows)

    province_counts: Counter[str] = Counter()
    city_counts: Counter[tuple[str, str]] = Counter()
    combo_counts: Counter[str] = Counter()
    qual_by_province: Dict[str, Counter[str]] = defaultdict(Counter)
    club_by_province: Counter[str] = Counter()
    club_counts: Counter[str] = Counter()
    fsa_counts: Counter[str] = Counter()

    missing_address = 0
    missing_city = 0
    missing_province = 0
    missing_postal = 0
    missing_any_name = 0
    duplicate_key_counts: Counter[tuple[str, str, str]] = Counter()

    for row in rows:
        province = norm_upper(row.get("prov_cd", ""))
        city = norm_upper(row.get("city", ""))
        combo = qualification_combo(row)
        club_name = norm_upper(row.get("club_name", ""))
        postal = norm_upper(row.get("postal_code", ""))
        first = norm_upper(row.get("first_name", ""))
        surname = norm_upper(row.get("surname", ""))

        province_counts[province] += 1
        city_counts[(province, city)] += 1
        combo_counts[combo] += 1
        duplicate_key_counts[(first, surname, province)] += 1

        if postal_fsa(postal):
            fsa_counts[postal_fsa(postal)] += 1

        for q_field, letter in zip(QUAL_FIELDS, ["A", "B", "C", "D", "E"]):
            if norm_text(row.get(q_field, "")):
                qual_by_province[province][letter] += 1

        if club_name:
            club_by_province[province] += 1
            club_counts[club_name] += 1

        if not norm_text(row.get("address_line", "")):
            missing_address += 1
        if not city:
            missing_city += 1
        if not province:
            missing_province += 1
        if not postal:
            missing_postal += 1
        if not first or not surname:
            missing_any_name += 1

    duplicated_name_rows = sum(v - 1 for v in duplicate_key_counts.values() if v > 1)
    duplicated_name_keys = sum(1 for v in duplicate_key_counts.values() if v > 1)

    province_summary = []
    for province, count in province_counts.most_common():
        club_count = club_by_province.get(province, 0)
        province_summary.append(
            {
                "province": province or "(blank)",
                "records": count,
                "share_pct": round((count / total) * 100, 3) if total else 0,
                "club_records": club_count,
                "club_share_pct": round((club_count / count) * 100, 3) if count else 0,
            }
        )

    city_summary = []
    for (province, city), count in city_counts.most_common(500):
        city_summary.append(
            {
                "province": province or "(blank)",
                "city": city or "(blank)",
                "records": count,
            }
        )

    combo_summary = []
    for combo, count in combo_counts.most_common():
        combo_summary.append(
            {
                "qualification_combo": combo,
                "records": count,
                "share_pct": round((count / total) * 100, 3) if total else 0,
            }
        )

    qual_prov_rows = []
    for province, counter in sorted(qual_by_province.items(), key=lambda item: province_counts[item[0]], reverse=True):
        base = {"province": province or "(blank)", "records": province_counts[province]}
        for letter in ["A", "B", "C", "D", "E"]:
            value = counter.get(letter, 0)
            base[f"qual_{letter}_count"] = value
            base[f"qual_{letter}_pct"] = round((value / province_counts[province]) * 100, 3) if province_counts[province] else 0
        qual_prov_rows.append(base)

    top_clubs = []
    for club, count in club_counts.most_common(300):
        top_clubs.append({"club_name": club, "records": count})

    club_summary = [
        {"metric": "total_records", "value": total},
        {"metric": "records_with_club_name", "value": sum(club_by_province.values())},
        {
            "metric": "club_record_share_pct",
            "value": round((sum(club_by_province.values()) / total) * 100, 3) if total else 0,
        },
        {"metric": "distinct_club_names", "value": len(club_counts)},
        {"metric": "distinct_fsa", "value": len(fsa_counts)},
    ]

    data_quality_summary = [
        {"metric": "missing_address_line", "count": missing_address, "share_pct": round((missing_address / total) * 100, 3) if total else 0},
        {"metric": "missing_city", "count": missing_city, "share_pct": round((missing_city / total) * 100, 3) if total else 0},
        {"metric": "missing_province", "count": missing_province, "share_pct": round((missing_province / total) * 100, 3) if total else 0},
        {"metric": "missing_postal_code", "count": missing_postal, "share_pct": round((missing_postal / total) * 100, 3) if total else 0},
        {"metric": "missing_first_or_surname", "count": missing_any_name, "share_pct": round((missing_any_name / total) * 100, 3) if total else 0},
        {"metric": "duplicate_name_province_rows", "count": duplicated_name_rows, "share_pct": round((duplicated_name_rows / total) * 100, 3) if total else 0},
        {"metric": "duplicate_name_province_keys", "count": duplicated_name_keys, "share_pct": round((duplicated_name_keys / total) * 100, 3) if total else 0},
    ]

    return {
        "province_summary": province_summary,
        "city_summary": city_summary,
        "qualification_combo_summary": combo_summary,
        "qualification_by_province": qual_prov_rows,
        "club_summary": club_summary,
        "top_clubs": top_clubs,
        "data_quality_summary": data_quality_summary,
    }


def run(input_path: Path, output_dir: Path) -> tuple[int, List[Dict[str, str]]]:
    """Analyze *input_path* and write CSVs to *output_dir*.

    Returns (row_count, rows) so callers can pass the raw rows to the DB layer.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    ensure_output_dir(output_dir)
    rows = read_rows(input_path)
    summaries = build_summaries(rows)

    for name, data_rows in summaries.items():
        out_path = output_dir / f"{name}.csv"
        if not data_rows:
            continue
        fieldnames = list(data_rows[0].keys())
        write_csv(out_path, fieldnames, data_rows)

    print(f"Analyzed {len(rows)} rows from {input_path}")
    print(f"Wrote {len(summaries)} summary files to {output_dir.resolve()}")
    return len(rows), rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze amateur_delim.txt and produce summary CSV files.")
    parser.add_argument(
        "--input",
        default="amateur_delim.txt",
        help="Input semicolon-delimited file path (default: amateur_delim.txt)",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory for summary CSVs (default: output)",
    )
    args = parser.parse_args()
    run(Path(args.input), Path(args.output_dir))


if __name__ == "__main__":
    main()
