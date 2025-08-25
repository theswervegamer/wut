# /imports/migrate_drop_legacy_match_cols.py
"""
One‑time SQLite migration that rebuilds `matches` WITHOUT legacy columns:
  comp1_kind, comp1_id, comp1_name, comp2_kind, comp2_id, comp2_name, order_in_day
and removes the old `match_wrestlers_view` if it exists.

Usage (from project root):
  python imports/migrate_drop_legacy_match_cols.py

Safety:
- Makes a backup `data/wut.db.bak` first.
- Uses a transaction; on error, nothing is changed.
- Keeps all existing rows and IDs.
"""
from __future__ import annotations
import os, shutil, sqlite3

DB = os.path.join("data", "wut.db")

NEW_SCHEMA_SQL = """
CREATE TABLE matches_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season INTEGER NOT NULL,
    tournament TEXT NOT NULL,
    round TEXT NOT NULL,
    winner_side INTEGER NULL CHECK (winner_side >= 1),
    result TEXT DEFAULT 'win' CHECK (result IN ('win','draw','nc')),
    stipulation TEXT NULL,
    match_time_seconds INTEGER NULL,
    day_index INTEGER NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

COPY_SQL = """
INSERT INTO matches_new (
    id, season, tournament, round, winner_side, result, stipulation, match_time_seconds, day_index, created_at
)
SELECT id, season, tournament, round, 
       winner_side,
       COALESCE(result, 'win') AS result,
       stipulation,
       match_time_seconds,
       day_index,
       created_at
FROM matches;
"""

INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_matches_season    ON matches(season);",
    "CREATE INDEX IF NOT EXISTS idx_matches_tournament ON matches(tournament);",
    "CREATE INDEX IF NOT EXISTS idx_matches_day       ON matches(day_index);",
]


def main() -> None:
    if not os.path.exists(DB):
        raise SystemExit(f"DB not found: {DB}")

    # Backup
    bak = DB + ".bak"
    shutil.copy2(DB, bak)
    print(f"Backup created: {bak}")

    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA foreign_keys=OFF;")
        con.execute("BEGIN;")

        # Drop the legacy view if present (it referenced comp1_*/comp2_*)
        con.execute("DROP VIEW IF EXISTS match_wrestlers_view;")

        # Build new table, copy, swap
        con.execute(NEW_SCHEMA_SQL)
        con.execute(COPY_SQL)
        con.execute("ALTER TABLE matches RENAME TO matches_old;")
        con.execute("ALTER TABLE matches_new RENAME TO matches;")

        # Recreate indexes
        for sql in INDEXES_SQL:
            con.execute(sql)

        # Verify row count
        old_count = con.execute("SELECT COUNT(*) FROM matches_old").fetchone()[0]
        new_count = con.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        if old_count != new_count:
            raise RuntimeError(f"Row count mismatch after copy: old={old_count} new={new_count}")

        # Keep old table around for safety; you can drop later if happy
        # con.execute("DROP TABLE matches_old;")

        con.execute("COMMIT;")
        print("Migration complete. Legacy columns removed. New schema in place.")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.execute("PRAGMA foreign_keys=ON;")
        con.close()

if __name__ == "__main__":
    main()
