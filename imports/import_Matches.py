# /imports/import_matches.py (v3)
"""
CSV importer for match history with **Day** timeline.

Usage (from project root):
  python imports/import_matches.py data/matches.csv --dry-run
  python imports/import_matches.py data/matches.csv

Required columns (case-insensitive):
  Season, Tournament, Round, Day, Wrestler/team 1, Wrestler/team 2, Winner, Match Time
Optional:
  Comp1 Type, Comp2 Type

Notes:
  - Time must be MM:SS (never past an hour).
  - Day = universe day number (1 = first ever show in S1). Use the same Day for matches on the same show.
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, Tuple, Dict

DB_PATH = os.path.join("data", "wut.db")


def connect_db() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_matches_schema(conn: sqlite3.Connection) -> None:
    # Keep in sync with app.py ensure_matches_schema. order_in_day column (if present) is ignored.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER NOT NULL,
            tournament TEXT NOT NULL,
            round TEXT NOT NULL,
            comp1_kind TEXT NOT NULL CHECK (comp1_kind IN ('Wrestler','Team')),
            comp1_id INTEGER NOT NULL,
            comp1_name TEXT NOT NULL,
            comp2_kind TEXT NOT NULL CHECK (comp2_kind IN ('Wrestler','Team')),
            comp2_id INTEGER NOT NULL,
            comp2_name TEXT NOT NULL,
            winner_side INTEGER NULL CHECK (winner_side IN (1,2)),
            match_time_seconds INTEGER NULL,
            day_index INTEGER NULL,
            order_in_day INTEGER NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cols = {row[1] for row in conn.execute("PRAGMA table_info('matches')").fetchall()}
    if 'day_index' not in cols:
        conn.execute("ALTER TABLE matches ADD COLUMN day_index INTEGER")
    if 'order_in_day' not in cols:
        conn.execute("ALTER TABLE matches ADD COLUMN order_in_day INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_season ON matches(season);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_tournament ON matches(tournament);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_comp_ids ON matches(comp1_id, comp2_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_day ON matches(day_index, order_in_day);")
    conn.commit()


@dataclass
class Competitor:
    kind: str  # "Wrestler" | "Team"
    id: int
    name: str


def parse_season(value: str) -> int:
    v = value.strip()
    if v.lower().startswith("s"):
        v = v[1:]
    return int(v)


def parse_time_mmss(value: str) -> Optional[int]:
    v = (value or "").strip()
    if not v:
        return None
    parts = v.split(":")
    if len(parts) != 2:
        raise ValueError(f"Time must be MM:SS, got {value!r}")
    m, s = parts
    m = int(m)
    s = int(s)
    if not (0 <= m <= 59 and 0 <= s <= 59):
        raise ValueError(f"Time out of range (MM and SS must be 0..59): {value!r}")
    return m * 60 + s


def lookup_competitor(conn: sqlite3.Connection, name: str, explicit_kind: Optional[str]) -> Competitor:
    name_norm = name.strip()
    if not name_norm:
        raise ValueError("Empty competitor name")

    wr = conn.execute(
        "SELECT id, name FROM wrestlers WHERE LOWER(name) = LOWER(?)",
        (name_norm,),
    ).fetchone()
    tr = conn.execute(
        "SELECT id, name FROM tag_teams WHERE LOWER(name) = LOWER(?)",
        (name_norm,),
    ).fetchone()

    if wr and tr:
        if not explicit_kind:
            raise ValueError(
                f"Ambiguous name {name!r}: exists as Wrestler and Team. Provide Comp Type column."
            )
        chosen = explicit_kind.capitalize()
        if chosen not in ("Wrestler", "Team"):
            raise ValueError(f"Invalid explicit kind {explicit_kind!r} for {name!r}")
        return Competitor(chosen, int((wr if chosen == 'Wrestler' else tr)["id"]), (wr if chosen == 'Wrestler' else tr)["name"])

    if wr:
        return Competitor("Wrestler", int(wr["id"]), wr["name"])
    if tr:
        return Competitor("Team", int(tr["id"]), tr["name"])
    raise ValueError(f"Unknown competitor name: {name!r}")


def resolve_winner(winner_value: str, c1: Competitor, c2: Competitor) -> Optional[int]:
    v = (winner_value or "").strip()
    if not v:
        return None
    if v in ("1", "2"):
        return int(v)
    v_low = v.lower()
    if v_low == c1.name.lower():
        return 1
    if v_low == c2.name.lower():
        return 2
    if v_low in ("comp1", "c1", "side1"):
        return 1
    if v_low in ("comp2", "c2", "side2"):
        return 2
    raise ValueError(f"Winner value {winner_value!r} doesn't match competitor names")


def detect_headers(header_row: list[str]) -> Dict[str, int]:
    norm = {h.strip().lower(): i for i, h in enumerate(header_row)}
    def idx(*aliases: str) -> int:
        for a in aliases:
            if a in norm:
                return norm[a]
        raise KeyError(f"Missing required column; tried aliases: {aliases}")

    return {
        "season": idx("season"),
        "tournament": idx("tournament"),
        "round": idx("round"),
        "comp1": idx("wrestler/team 1", "competitor 1", "comp1", "wrestler 1", "team 1"),
        "comp2": idx("wrestler/team 2", "competitor 2", "comp2", "wrestler 2", "team 2"),
        "winner": idx("winner", "result"),
        "time": idx("match time", "time"),
        "day": idx("day", "timeline day", "universe day"),
        "comp1_type": norm.get("comp1 type", norm.get("competitor 1 type", -1)),
        "comp2_type": norm.get("comp2 type", norm.get("competitor 2 type", -1)),
    }


def import_csv(path: str, dry_run: bool = False, delimiter: str = ",", encoding: str = "utf-8") -> Tuple[int, int]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    with open(path, "r", encoding=encoding, newline="") as f, connect_db() as conn:
        ensure_matches_schema(conn)
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)
        headers = detect_headers(header)

        inserted = 0
        rownum = 1
        for row in reader:
            rownum += 1
            if not row or all(not c.strip() for c in row):
                continue
            try:
                season = parse_season(row[headers["season"]])
                tournament = row[headers["tournament"]].strip()
                rnd = row[headers["round"]].strip()

                comp1_name_in = row[headers["comp1"]].strip()
                comp2_name_in = row[headers["comp2"]].strip()

                comp1_type_in = row[headers["comp1_type"]].strip() if headers["comp1_type"] != -1 else None
                comp2_type_in = row[headers["comp2_type"]].strip() if headers["comp2_type"] != -1 else None

                c1 = lookup_competitor(conn, comp1_name_in, comp1_type_in)
                c2 = lookup_competitor(conn, comp2_name_in, comp2_type_in)

                winner_val = row[headers["winner"]]
                winner_side = resolve_winner(winner_val, c1, c2)

                time_seconds = parse_time_mmss(row[headers["time"]])
                day_index = int(row[headers["day"]])
                if day_index < 1:
                    raise ValueError("Day must be >= 1")

                if dry_run:
                    print(
                        f"DRY-RUN row {rownum}: Day {day_index} | S{season} | {tournament} | {rnd} | "
                        f"{c1.kind}:{c1.name} vs {c2.kind}:{c2.name} | winner_side={winner_side} | time={time_seconds}"
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO matches (
                            season, tournament, round,
                            comp1_kind, comp1_id, comp1_name,
                            comp2_kind, comp2_id, comp2_name,
                            winner_side, match_time_seconds,
                            day_index
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            season, tournament, rnd,
                            c1.kind, c1.id, c1.name,
                            c2.kind, c2.id, c2.name,
                            winner_side, time_seconds,
                            day_index,
                        ),
                    )
                    inserted += 1
            except Exception as e:
                raise RuntimeError(f"Error on CSV row {rownum}: {e}") from e
        if not dry_run:
            conn.commit()
        return rownum - 1, inserted


def main() -> None:
    p = argparse.ArgumentParser(description="Import match history CSV into SQLite matches table")
    p.add_argument("csv_path", help="Path to CSV file")
    p.add_argument("--dry-run", action="store_true", help="Validate and show inserts without writing")
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    p.add_argument("--encoding", default="utf-8", help="CSV encoding (default: utf-8)")

    args = p.parse_args()
    total, inserted = import_csv(args.csv_path, dry_run=args.dry_run, delimiter=args.delimiter, encoding=args.encoding)
    if args.dry_run:
        print(f"Checked {total} rows. 0 inserted (dry-run).")
    else:
        print(f"Processed {total} rows. Inserted {inserted} matches.")


if __name__ == "__main__":
    main()
