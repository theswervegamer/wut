# /imports/recreate_match_participants.py
"""
Recreates the match_participants table to ensure foreign keys match the v2 schema.
- Makes a backup: data/wut.db.mp.bak
- Drops match_participants (only) and recreates it with correct FKs + indexes.
Usage:
  python imports/recreate_match_participants.py
"""
from __future__ import annotations
import os, shutil, sqlite3

DB = os.path.join("data", "wut.db")

SQL_CREATE = """
CREATE TABLE match_participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    side INTEGER NOT NULL,
    wrestler_id INTEGER NOT NULL,
    UNIQUE(match_id, side, wrestler_id),
    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
    FOREIGN KEY (wrestler_id) REFERENCES wrestlers(id)
);
"""

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_mp_match ON match_participants(match_id);",
    "CREATE INDEX IF NOT EXISTS idx_mp_wrestler ON match_participants(wrestler_id);",
]

def main() -> None:
    if not os.path.exists(DB):
        raise SystemExit(f"DB not found: {DB}")
    bak = DB + ".mp.bak"
    shutil.copy2(DB, bak)
    print(f"Backup created: {bak}")

    con = sqlite3.connect(DB)
    try:
        con.execute("PRAGMA foreign_keys=OFF;")
        con.execute("BEGIN;")
        con.execute("DROP TABLE IF EXISTS match_participants;")
        con.execute(SQL_CREATE)
        for s in INDEXES:
            con.execute(s)
        con.execute("COMMIT;")
        print("match_participants recreated.")
    except Exception as e:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.execute("PRAGMA foreign_keys=ON;")
        con.close()
        print("Done.")

if __name__ == "__main__":
    main()
