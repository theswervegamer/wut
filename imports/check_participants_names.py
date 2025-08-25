# /imports/check_participants_names.py — v2
"""
Checks participants.csv names against DB. Accepts either:
  • Wrestler names (exact in roster, case-insensitive)
  • Team names (must be a tag team with EXACTLY 2 members)

Usage (from project root):
  python imports/check_participants_names.py data/participants.csv

Exit codes:
  0 = all names resolve (wrestler or 2-person team)
  1 = unknown names and/or invalid teams found
  2 = usage / file issues

Notes:
  • Optional column `Type` can be used per row to force resolution: `Wrestler` or `Team`.
  • No Freebird rules assumed: teams must have exactly 2 members.
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
from typing import Dict, List, Set

DB_PATH = os.path.join("data", "wut.db")


def load_db() -> tuple[Dict[str, int], Dict[str, List[int]]]:
    """Return (wrestlers_by_name_lower -> id, teams_by_name_lower -> [wrestler_ids])."""
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Wrestlers
    wrestlers: Dict[str, int] = {}
    for r in conn.execute("SELECT id, name FROM wrestlers").fetchall():
        name = (r["name"] or "").strip()
        if name:
            wrestlers[name.lower()] = int(r["id"])
    # Teams -> members (wrestler ids)
    teams: Dict[str, List[int]] = {}
    rows = conn.execute(
        """
        SELECT tt.name AS team_name, ttm.wrestler_id
        FROM tag_teams tt
        JOIN tag_team_members ttm ON ttm.team_id = tt.id
        ORDER BY tt.name
        """
    ).fetchall()
    for r in rows:
        tname = (r["team_name"] or "").strip()
        if tname:
            teams.setdefault(tname.lower(), []).append(int(r["wrestler_id"]))
    conn.close()
    return wrestlers, teams


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python imports/check_participants_names.py data/participants.csv")
        return 2
    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return 2

    wrestlers, teams = load_db()

    unknown: Set[str] = set()
    bad_teams: Dict[str, int] = {}  # name_lower -> member_count

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            print("participants.csv has no headers")
            return 2
        headers_low = {h.lower(): h for h in rdr.fieldnames}
        if "wrestler" not in headers_low:
            print("participants.csv must include a 'Wrestler' column header")
            return 2
        col_w = headers_low["wrestler"]
        col_t = headers_low.get("type")  # optional

        for row in rdr:
            name_raw = (row.get(col_w) or "").strip()
            if not name_raw:
                continue
            name_l = name_raw.lower()
            forced = (row.get(col_t) or "").strip().lower() if col_t else ""

            if forced == "wrestler":
                if name_l not in wrestlers:
                    unknown.add(name_raw)
                continue
            if forced == "team":
                members = teams.get(name_l)
                if not members:
                    unknown.add(name_raw)
                elif len(members) != 2:
                    bad_teams[name_raw] = len(members)
                continue

            # Auto-detect: try wrestler first, then team
            if name_l in wrestlers:
                continue
            members = teams.get(name_l)
            if members is None:
                unknown.add(name_raw)
            elif len(members) != 2:
                bad_teams[name_raw] = len(members)
            # else: valid 2-person team → OK

    if not unknown and not bad_teams:
        print("All participant names resolve to either a Wrestler or a 2-person Team. ✅")
        return 0

    if unknown:
        print("Unknown names (not found as Wrestler or Team):")
        for n in sorted(unknown, key=str.lower):
            print(f"  - {n}")
    if bad_teams:
        print("\nTeams that do not have exactly 2 members (update team membership or specify individuals):")
        for n in sorted(bad_teams, key=str.lower):
            print(f"  - {n} (has {bad_teams[n]} members)")

    print("\nFix: add missing wrestlers/teams in the app, or correct names/Type in CSV. Then re-run this check.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
