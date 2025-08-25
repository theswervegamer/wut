#!/usr/bin/env python3
# filepath: imports/export_matches_for_times.py
"""
Export all matches (with stable match IDs) to a CSV you can edit to add times.

Output columns:
- match_id
- season
- tournament
- round
- side1
- side2
- time_mmss   (current time formatted as MM:SS, empty if none)

Usage (from project root):
  python imports/export_matches_for_times.py data/matches_times_export.csv

Optional flags:
  --db data/wut.db         # custom DB path (default: data/wut.db)
  --season 4               # only one season
  --season-range 1 4       # inclusive range (min max)

You can open the CSV, fill the time_mmss column (e.g., 12:34), save, then run
imports/update_match_times.py to bulk-update without creating duplicates.
"""

from __future__ import annotations
import argparse
import csv
import os
import sqlite3
from typing import Optional


def _fmt_time(seconds: Optional[int]) -> Optional[str]:
    if seconds is None:
        return None
    if seconds < 0:
        return None
    m, s = divmod(int(seconds), 60)
    if m >= 60:
        return None  # outside your universe rules
    return f"{m}:{s:02d}"


def export_csv(db_path: str, out_csv: str, season: Optional[int], season_range: Optional[tuple[int, int]]) -> int:
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        sql = [
            """
            SELECT m.id AS match_id,
                   m.season,
                   m.tournament,
                   m.round,
                   (SELECT GROUP_CONCAT(w.name, ' & ')
                      FROM match_participants mp
                      JOIN wrestlers w ON w.id = mp.wrestler_id
                     WHERE mp.match_id = m.id AND mp.side = 1
                     ORDER BY w.name) AS side1,
                   (SELECT GROUP_CONCAT(w.name, ' & ')
                      FROM match_participants mp
                      JOIN wrestlers w ON w.id = mp.wrestler_id
                     WHERE mp.match_id = m.id AND mp.side = 2
                     ORDER BY w.name) AS side2,
                   m.match_time_seconds AS time_seconds
              FROM matches m
            """
        ]
        params: list[object] = []
        where = []
        if season is not None:
            where.append("m.season = ?")
            params.append(season)
        elif season_range is not None:
            lo, hi = season_range
            where.append("m.season BETWEEN ? AND ?")
            params.extend([lo, hi])
        if where:
            sql.append("WHERE ")
            sql.append(" AND ".join(where))
        sql.append(" ORDER BY m.season, m.day_index, m.order_in_day, m.id")
        query = "".join(sql)

        cur = conn.execute(query, params)
        rows = cur.fetchall()

        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["match_id", "season", "tournament", "round", "side1", "side2", "time_mmss"])
            for r in rows:
                time_str = _fmt_time(r["time_seconds"]) or ""
                w.writerow([
                    r["match_id"], r["season"], r["tournament"], r["round"],
                    r["side1"] or "", r["side2"] or "", time_str,
                ])
        return len(rows)
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out_csv", help="path to write CSV, e.g. data/matches_times_export.csv")
    ap.add_argument("--db", default="data/wut.db", help="SQLite DB path (default: data/wut.db)")
    ap.add_argument("--season", type=int, help="only this season", default=None)
    ap.add_argument("--season-range", nargs=2, type=int, metavar=("MIN", "MAX"), default=None)
    args = ap.parse_args()

    sr = None
    if args.season_range is not None:
        lo, hi = args.season_range
        if lo > hi:
            lo, hi = hi, lo
        sr = (lo, hi)

    n = export_csv(args.db, args.out_csv, args.season, sr)
    print(f"Exported {n} matches → {args.out_csv}")


if __name__ == "__main__":
    main()
