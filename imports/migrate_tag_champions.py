# /imports/migrate_tag_champions.py
"""
Adds team-aware champion columns so seasonal/ongoing titles can be held by a TAG TEAM.
- championship_seasons: + champion_team_id, + runner_up_team_id (both NULLable)
- championship_reigns:  + champion_team_id (NULLable)

Safe to run multiple times. Existing data untouched.

Usage (from project root):
  python imports/migrate_tag_champions.py
"""
from __future__ import annotations
import os, sqlite3
from pathlib import Path

DB = Path("data/wut.db")

SQLS = [
    # championship_seasons — add team columns if missing
    "ALTER TABLE championship_seasons ADD COLUMN champion_team_id INTEGER NULL;",
    "ALTER TABLE championship_seasons ADD COLUMN runner_up_team_id INTEGER NULL;",
    # helpful indexes
    "CREATE INDEX IF NOT EXISTS idx_cs_champion_team ON championship_seasons(champion_team_id);",
    "CREATE INDEX IF NOT EXISTS idx_cs_runner_team ON championship_seasons(runner_up_team_id);",
    # championship_reigns — add optional team champion column
    "ALTER TABLE championship_reigns ADD COLUMN champion_team_id INTEGER NULL;",
    "CREATE INDEX IF NOT EXISTS idx_cr_champion_team ON championship_reigns(champion_team_id);",
]


def column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(r[1].lower() == col.lower() for r in cur.fetchall())


def main() -> None:
    if not DB.exists():
        raise SystemExit(f"DB not found: {DB}")
    conn = sqlite3.connect(DB)
    try:
        # Apply only missing columns
        if not column_exists(conn, "championship_seasons", "champion_team_id"):
            conn.execute(SQLS[0])
        if not column_exists(conn, "championship_seasons", "runner_up_team_id"):
            conn.execute(SQLS[1])
        conn.execute(SQLS[2])
        conn.execute(SQLS[3])

        if not column_exists(conn, "championship_reigns", "champion_team_id"):
            conn.execute(SQLS[4])
        conn.execute(SQLS[5])
        conn.commit()
    finally:
        conn.close()
    print("Migration complete ✓  (team champion columns present)")


if __name__ == "__main__":
    main()
