from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sqlite3
from typing import List, Dict, Tuple

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "data" / "wut.db"

# ---------------- DB helpers ----------------

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS wrestlers (
          id      INTEGER PRIMARY KEY AUTOINCREMENT,
          name    TEXT NOT NULL,
          gender  TEXT CHECK(gender IN ('Male','Female')) NOT NULL,
          active  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tag_teams (
          id      INTEGER PRIMARY KEY AUTOINCREMENT,
          name    TEXT NOT NULL UNIQUE,
          active  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tag_team_members (
          team_id     INTEGER NOT NULL,
          wrestler_id INTEGER NOT NULL,
          PRIMARY KEY(team_id, wrestler_id),
          FOREIGN KEY(team_id) REFERENCES tag_teams(id) ON DELETE CASCADE,
          FOREIGN KEY(wrestler_id) REFERENCES wrestlers(id) ON DELETE RESTRICT
        );
        """
    )
    # Helpful indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_wrestlers_name   ON wrestlers(name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tag_teams_name   ON tag_teams(name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_team_members_team ON tag_team_members(team_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_team_members_wrestler ON tag_team_members(wrestler_id)")
    conn.commit()


def ensure_status_column(conn: sqlite3.Connection) -> None:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(tag_teams)")]  # cid,name,type,...
    if "status" not in cols:
        conn.execute("ALTER TABLE tag_teams ADD COLUMN status TEXT DEFAULT 'Active'")
        conn.execute(
            """
            UPDATE tag_teams
            SET status = CASE WHEN COALESCE(active,1)=1 THEN 'Active' ELSE 'Inactive' END
            WHERE status IS NULL
            """
        )
        conn.commit()

# ---------------- Normalizers ----------------

def norm_active(val: str) -> int:
    v = (val or "").strip().lower()
    if v in {"yes", "y", "true", "1"}: return 1
    if v in {"no", "n", "false", "0"}: return 0
    raise ValueError("Active must be Yes/No (or Y/N/True/False/1/0)")


def norm_status(val: str) -> str:
    v = (val or "").strip().lower()
    mapping = {
        "active": "Active", "yes": "Active", "y": "Active", "1": "Active", "true": "Active",
        "inactive": "Inactive", "no": "Inactive", "n": "Inactive", "0": "Inactive", "false": "Inactive",
        "disbanded": "Disbanded", "retired": "Disbanded", "split": "Disbanded",
    }
    if v in mapping:
        return mapping[v]
    raise ValueError("Status must be Active/Inactive/Disbanded (or Yes/No/True/False/Disbanded)")

# ---------------- CSV helpers ----------------

def sniff(text: str, forced_delim: str | None) -> csv.Dialect:
    if forced_delim:
        class D(csv.Dialect):
            delimiter = forced_delim
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return D
    try:
        return csv.Sniffer().sniff(text[:4096], delimiters=[",",";","\t","|"])
    except Exception:
        return csv.get_dialect("excel")


def parse_members(row: Dict[str, str], member_sep: str) -> List[str]:
    lower = {k.lower(): (v or "").strip() for k, v in row.items()}
    names: List[str] = []
    if "members" in lower and lower["members"]:
        names = [p.strip() for p in lower["members"].split(member_sep) if p.strip()]
    else:
        for k, v in lower.items():
            if k.startswith("member") and v:
                names.append(v)
    # de-dupe, keep order
    seen, out = set(), []
    for n in names:
        key = n.lower()
        if key not in seen:
            seen.add(key)
            out.append(n)
    return out

# ---------------- Import helpers ----------------

def load_wrestler_index(conn: sqlite3.Connection) -> Dict[str, Tuple[int, str]]:
    idx: Dict[str, Tuple[int, str]] = {}
    dupes: Dict[str, int] = {}
    for r in conn.execute("SELECT id, name, gender FROM wrestlers"):
        key = r["name"].strip().lower()
        if key in idx:
            dupes[key] = dupes.get(key, 1) + 1
        else:
            idx[key] = (r["id"], r["gender"])
    if dupes:
        print("[warning] Duplicate wrestler names detected (case-insensitive):")
        for k, c in dupes.items():
            print(f"  '{k}' appears {c} times — imports may error on ambiguous names")
    return idx


def ensure_wrestler(
    conn: sqlite3.Connection,
    name: str,
    team_status: str,
    wrestlers_idx: Dict[str, Tuple[int, str]],
    create: bool = True,
) -> int:
    key = name.strip().lower()
    if key in wrestlers_idx:
        wid, gender = wrestlers_idx[key]
        if gender != "Male":
            raise ValueError(f"Only male wrestlers allowed in teams (offending: {name})")
        return wid
    active_int = 1 if team_status == "Active" else 0
    if not create:
        return -1
    cur = conn.execute(
        "INSERT INTO wrestlers(name, gender, active) VALUES (?,?,?)",
        (name.strip(), "Male", active_int),
    )
    wid = cur.lastrowid
    wrestlers_idx[key] = (wid, "Male")
    print(f"[create] Added wrestler: {name} (Male, active={active_int})")
    return wid


def upsert_team(
    conn: sqlite3.Connection,
    name: str,
    status: str,
    member_ids: List[int],
    mode: str,
) -> str:
    active_int = 1 if status == "Active" else 0
    cur = conn.execute("SELECT id FROM tag_teams WHERE name = ? COLLATE NOCASE", (name,))
    row = cur.fetchone()
    if row is None:
        cur = conn.execute(
            "INSERT INTO tag_teams(name, active, status) VALUES (?,?,?)",
            (name, active_int, status),
        )
        team_id = cur.lastrowid
        if member_ids:
            conn.executemany(
                "INSERT INTO tag_team_members(team_id, wrestler_id) VALUES (?,?)",
                [(team_id, wid) for wid in member_ids],
            )
        return "inserted"

    team_id = row[0]
    if mode == "skip":
        return "skipped"

    conn.execute(
        "UPDATE tag_teams SET active = ?, status = ? WHERE id = ?",
        (active_int, status, team_id),
    )
    if mode == "update":
        conn.execute("DELETE FROM tag_team_members WHERE team_id = ?", (team_id,))
        add = member_ids
    else:  # merge
        existing = {r[0] for r in conn.execute(
            "SELECT wrestler_id FROM tag_team_members WHERE team_id = ?",
            (team_id,),
        )}
        add = [wid for wid in member_ids if wid not in existing]
    if add:
        conn.executemany(
            "INSERT INTO tag_team_members(team_id, wrestler_id) VALUES (?,?)",
            [(team_id, wid) for wid in add],
        )
    return "updated"

# ---------------- Main ----------------

def main() -> None:
    p = argparse.ArgumentParser(description="Bulk import tag teams into data/wut.db")
    p.add_argument("csv_path", help="Path to CSV (UTF-8). Columns: name, status|active, and members list")
    p.add_argument("--db", default=str(DB_PATH), help="Override DB path (default: data/wut.db)")
    p.add_argument("--delimiter", default=None, help="Force CSV delimiter (e.g., ',' ';' '\t' '|')")
    p.add_argument("--member-sep", default=";", help="Separator for the 'members' column (default: ';')")
    p.add_argument("--mode", choices=["skip","update","merge"], default="update",
                   help="When team exists: skip (no change), update (replace members), or merge (add new members)")
    p.add_argument("--dry-run", action="store_true", help="Validate only; no DB writes")

    args = p.parse_args()

    db_path = Path(args.db)
    conn = connect(db_path)
    ensure_schema(conn)
    ensure_status_column(conn)

    csv_file = Path(args.csv_path)
    if not csv_file.exists():
        raise SystemExit(f"CSV not found: {csv_file}")

    text = csv_file.read_text(encoding="utf-8-sig")
    dialect = sniff(text, args.delimiter)

    # Reader with or without headers
    reader = csv.DictReader(text.splitlines(), dialect=dialect)
    positional = False
    if not reader.fieldnames or not any(h and h.lower() == "name" for h in reader.fieldnames):
        positional = True
        reader = csv.reader(text.splitlines(), dialect=dialect)

    wrestlers_idx = load_wrestler_index(conn)

    total = inserted = updated = skipped = errors = 0
    to_commit = 0

    try:
        if positional:
            print("[info] No headers found — expecting columns: name, status(or active), members")
        for i, row in enumerate(reader, start=1):
            total += 1
            try:
                if positional:
                    try:
                        name = (row[0] or "").strip()
                        status_raw = (row[1] or "").strip()
                        members_raw = (row[2] or "").strip()
                        row_dict = {"name": name, "status": status_raw, "members": members_raw}
                    except Exception:
                        raise ValueError("Row must have 3 columns: name, status|active, members")
                else:
                    row_dict = {k: (v or "") for k, v in row.items()}

                name = (row_dict.get("name") or row_dict.get("Name") or "").strip()
                if not name:
                    raise ValueError("Team name is required")

                # Determine status (preferred) or map from active
                if row_dict.get("status") or row_dict.get("Status"):
                    status = norm_status(row_dict.get("status") or row_dict.get("Status"))
                else:
                    active_n = norm_active(row_dict.get("active") or row_dict.get("Active") or "")
                    status = "Active" if active_n else "Inactive"

                member_names = parse_members(row_dict, args.member_sep)
                if len(member_names) < 2:
                    raise ValueError("At least two member names are required")

                # Resolve/auto-create members (Male, active derived from team status)
                member_ids: List[int] = []
                for m in member_names:
                    wid = ensure_wrestler(conn, m, status, wrestlers_idx, create=(not args.dry_run))
                    member_ids.append(wid)

                if args.dry_run:
                    continue

                res = upsert_team(conn, name, status, member_ids, args.mode)
                to_commit += 1
                if res == "inserted":
                    inserted += 1
                elif res == "updated":
                    updated += 1
                elif res == "skipped":
                    skipped += 1

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
