# ===== import_wrestlers_csv.py =====
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sqlite3
from typing import Tuple

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "data" / "wut.db"

def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_schema(conn: sqlite3.Connection) -> None:
    # Creates table if not present (matches app.py)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wrestlers (
          id      INTEGER PRIMARY KEY AUTOINCREMENT,
          name    TEXT NOT NULL,
          gender  TEXT CHECK(gender IN ('Male','Female')) NOT NULL,
          active  INTEGER NOT NULL  -- 1=yes, 0=no
        );
        """
    )
    conn.commit()

def norm_gender(val: str) -> str:
    v = (val or "").strip().lower()
    if v in {"male", "m"}:
        return "Male"
    if v in {"female", "f"}:
        return "Female"
    raise ValueError("Gender must be Male/Female (or M/F)")

def norm_active(val: str) -> int:
    v = (val or "").strip().lower()
    if v in {"yes", "y", "true", "1"}:
        return 1
    if v in {"no", "n", "false", "0"}:
        return 0
    raise ValueError("Active must be Yes/No (or Y/N/True/False/1/0)")

def upsert(conn: sqlite3.Connection, name: str, gender: str, active: int, update_existing: bool) -> Tuple[str,int]:
    # Case-insensitive match on name
    cur = conn.execute("SELECT id FROM wrestlers WHERE name = ? COLLATE NOCASE", (name,))
    row = cur.fetchone()
    if row and update_existing:
        conn.execute(
            "UPDATE wrestlers SET gender = ?, active = ? WHERE id = ?",
            (gender, active, row["id"]),
        )
        return ("updated", row["id"])
    if row:
        return ("skipped", row["id"])
    cur = conn.execute(
        "INSERT INTO wrestlers(name, gender, active) VALUES (?,?,?)",
        (name, gender, active),
    )
    return ("inserted", cur.lastrowid)

def sniff_dialect(sample: str, default_delim: str | None) -> csv.Dialect:
    if default_delim:
        class _D(csv.Dialect):
            delimiter = default_delim
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return _D
    try:
        return csv.Sniffer().sniff(sample, delimiters=[",",";","\t","|"])
    except Exception:
        return csv.get_dialect("excel")

def main() -> None:
    p = argparse.ArgumentParser(description="Bulk import wrestlers into data/wut.db")
    p.add_argument("csv_path", help="Path to CSV file (UTF-8). Columns: name, gender, active")
    p.add_argument("--db", default=str(DB_PATH), help="Override DB path (default: data/wut.db)")
    p.add_argument("--delimiter", default=None, help="Force delimiter (e.g., ',' ';' '\\t')")
    p.add_argument("--dry-run", action="store_true", help="Parse & validate only; no DB writes")
    p.add_argument("--update", action="store_true", help="Update existing names instead of skipping")
    args = p.parse_args()

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    init_schema(conn)

    csv_file = Path(args.csv_path)
    if not csv_file.exists():
        raise SystemExit(f"CSV not found: {csv_file}")

    text = csv_file.read_text(encoding="utf-8-sig")
    dialect = sniff_dialect(text[:4096], args.delimiter)

    reader = csv.DictReader(text.splitlines(), dialect=dialect)
    # If headers are missing, try position-based fallback
    positional = False
    if not reader.fieldnames or not any(h.lower() == "name" for h in reader.fieldnames):
        positional = True
        reader = csv.reader(text.splitlines(), dialect=dialect)

    total = 0
    inserted = updated = skipped = errors = 0
    to_commit = 0

    try:
        for i, row in enumerate(reader, start=1):
            total += 1
            try:
                if positional:
                    # Expect columns: name, gender, active
                    try:
                        name, gender, active = row[0], row[1], row[2]
                    except Exception:
                        raise ValueError("Row must have 3 columns: name, gender, active")
                else:
                    name = (row.get("name") or row.get("Name") or "").strip()
                    gender = (row.get("gender") or row.get("Gender") or "").strip()
                    active = (row.get("active") or row.get("Active") or "").strip()

                if not name:
                    raise ValueError("Name is required")

                gender_n = norm_gender(gender)
                active_n = norm_active(active)

                if args.dry_run:
                    continue

                status, _rowid = upsert(conn, name, gender_n, active_n, args.update)
                to_commit += 1
                if status == "inserted":
                    inserted += 1
                elif status == "updated":
                    updated += 1
                else:
                    skipped += 1

                # Commit every 500 rows for speed/atomicity
                if to_commit >= 500:
                    conn.commit()
                    to_commit = 0

            except Exception as e:
                errors += 1
                print(f"[row {i}] ERROR: {e}")

        if not args.dry_run and to_commit:
            conn.commit()

    finally:
        conn.close()

    print(f"Rows read: {total}")
    if args.dry_run:
        print("Dry-run only: no changes written.")
    else:
        print(f"Inserted: {inserted} | Updated: {updated} | Skipped: {skipped} | Errors: {errors}")

if __name__ == "__main__":
    main()
