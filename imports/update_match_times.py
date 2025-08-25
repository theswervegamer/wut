#!/usr/bin/env python3
# filepath: imports/update_match_times.py
"""
Bulk-update match times from a CSV using **match_id** so there are no duplicates.

Input CSV columns (header required):
  - match_id   (int)
  - time_mmss  (string "MM:SS"; empty to clear)

Any extra columns are ignored.

Usage (from project root):
  # First export a template you can edit
  python imports/export_matches_for_times.py data/matches_times_export.csv --season 4

  # Edit time_mmss column in a spreadsheet, then run:
  python imports/update_match_times.py data/matches_times_export.csv --dry-run
  python imports/update_match_times.py data/matches_times_export.csv

Options:
  --db data/wut.db   # custom DB path (default: data/wut.db)
  --dry-run          # show what would change without writing
"""
from __future__ import annotations
import argparse
import csv
import os
import sqlite3
from typing import Optional


def _parse_mmss(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    s = value.strip()
    if not s:
        return None
    parts = s.split(":")
    if len(parts) != 2:
        return None
    m_str, s_str = parts[0].strip(), parts[1].strip()
    if not (m_str.isdigit() and s_str.isdigit()):
        return None
    m = int(m_str)
    sec = int(s_str)
    if sec >= 60 or m < 0:
        return None
    total = m * 60 + sec
    return total if total < 3600 else None


def bulk_update(db_path: str, csv_path: str, dry_run: bool) -> tuple[int, int, int]:
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {db_path}")
    if not os.path.exists(csv_path):
        raise SystemExit(f"CSV not found: {csv_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        updated = 0
        skipped = 0
        missing = 0

        with open(csv_path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            need_cols = {"match_id", "time_mmss"}
            have_cols = {c.strip().lower() for c in r.fieldnames or []}
            if not need_cols.issubset(have_cols):
                raise SystemExit("CSV must include headers: match_id,time_mmss")

            for row in r:
                raw_id = row.get("match_id")
                try:
                    mid = int(raw_id) if raw_id is not None and raw_id != "" else None
                except Exception:
                    mid = None
                if mid is None:
                    skipped += 1
                    continue

                # parse time
                sec = _parse_mmss(row.get("time_mmss"))

                # confirm match exists
                exists = conn.execute("SELECT 1 FROM matches WHERE id = ?", (mid,)).fetchone()
                if not exists:
                    missing += 1
                    continue

                if dry_run:
                    print(f"DRY-RUN: UPDATE matches SET match_time_seconds={sec} WHERE id={mid}")
                else:
                    conn.execute(
                        "UPDATE matches SET match_time_seconds = ? WHERE id = ?",
                        (sec, mid),
                    )
                    updated += 1

        if not dry_run:
            conn.commit()
        return updated, skipped, missing
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path", help="CSV path with match_id,time_mmss")
    ap.add_argument("--db", default="data/wut.db", help="SQLite DB path (default: data/wut.db)")
    ap.add_argument("--dry-run", action="store_true", help="show updates without writing")
    args = ap.parse_args()

    updated, skipped, missing = bulk_update(args.db, args.csv_path, args.dry_run)
    mode = "DRY-RUN" if args.dry_run else "UPDATED"
    print(f"{mode}: {updated} rows; skipped {skipped} bad IDs; {missing} not found")


if __name__ == "__main__":
    main()
