# /imports/import_matches_v2.py
"""
Import v2: two CSVs linked by a human-friendly Key.

Usage (from project root):
  python imports/import_matches_v2.py data/matches.csv data/participants.csv --dry-run
  python imports/import_matches_v2.py data/matches.csv data/participants.csv

CSV A: matches.csv (headers, case-insensitive)
  Key,Season,Day,Tournament,Round,Stipulation,Result,Winner Side,Match Time
    - Result: win|draw|nc  (win requires Winner Side)
    - Match Time: MM:SS (stored as seconds)

CSV B: participants.csv (headers)
  Key,Side,Wrestler
    - Side: integer >=1; for tag, both team members use same Side number
    - Wrestler: exact name in roster (case-insensitive)

Import rules:
  - Validates that Winner Side exists among participants when Result=win
  - Validates that each participant wrestler name resolves to an id
  - Idempotency: this script does not dedupe previously inserted Keys; running twice will duplicate records
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional

DB_PATH = os.path.join("data", "wut.db")


def connect_db() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    # Keep aligned with app schema; minimal definitions to avoid import errors
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER NOT NULL,
            tournament TEXT NOT NULL,
            round TEXT NOT NULL,
            comp1_kind TEXT NULL CHECK (comp1_kind IN ('Wrestler','Team')),
            comp1_id INTEGER NULL,
            comp1_name TEXT NULL,
            comp2_kind TEXT NULL CHECK (comp2_kind IN ('Wrestler','Team')),
            comp2_id INTEGER NULL,
            comp2_name TEXT NULL,
            winner_side INTEGER NULL CHECK (winner_side >= 1),
            result TEXT DEFAULT 'win' CHECK (result IN ('win','draw','nc')),
            stipulation TEXT NULL,
            match_time_seconds INTEGER NULL,
            day_index INTEGER NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS match_participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            side INTEGER NOT NULL,
            wrestler_id INTEGER NOT NULL,
            UNIQUE(match_id, side, wrestler_id),
            FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
            FOREIGN KEY (wrestler_id) REFERENCES wrestlers(id)
        );
        """
    )
    conn.commit()


@dataclass
class MatchRow:
    key: str
    season: int
    day: int
    tournament: str
    round: str
    stipulation: Optional[str]
    result: str  # win|draw|nc
    winner_side: Optional[int]
    time_seconds: Optional[int]


def parse_season(v: str) -> int:
    v = v.strip()
    return int(v[1:]) if v.lower().startswith("s") else int(v)


def parse_time_mmss(v: str) -> Optional[int]:
    v = (v or "").strip()
    if not v:
        return None
    parts = v.split(":")
    if len(parts) != 2:
        raise ValueError(f"Time must be MM:SS, got {v!r}")
    m, s = int(parts[0]), int(parts[1])
    if not (0 <= m <= 59 and 0 <= s <= 59):
        raise ValueError(f"Time out of range: {v!r}")
    return m * 60 + s


def read_matches_csv(path: str) -> Dict[str, MatchRow]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"key", "season", "day", "tournament", "round", "stipulation", "result", "winner side", "match time"}
        low = [h.lower() for h in reader.fieldnames or []]
        missing = [h for h in required if h not in low]
        if missing:
            raise KeyError(f"Missing columns in matches.csv: {missing}")
        rows: Dict[str, MatchRow] = {}
        for raw in reader:
            d = {k.lower(): v for k, v in raw.items()}
            key = d["key"].strip()
            if not key:
                raise ValueError("Empty Key in matches.csv")
            season = parse_season(d["season"]) 
            day = int(d["day"]) 
            if day < 1:
                raise ValueError(f"Day must be >=1 for Key {key}")
            tournament = d["tournament"].strip()
            round_ = d["round"].strip()
            stip = d.get("stipulation", "").strip() or None
            result = d["result"].strip().lower()
            if result not in ("win", "draw", "nc"):
                raise ValueError(f"Invalid Result for Key {key}: {result!r}")
            ws = d.get("winner side", "").strip()
            winner_side = int(ws) if ws else None
            if result == "win" and not winner_side:
                raise ValueError(f"Winner Side required when Result=win (Key {key})")
            tsec = parse_time_mmss(d.get("match time", ""))
            rows[key] = MatchRow(key, season, day, tournament, round_, stip, result, winner_side, tsec)
        return rows


def lookup_wrestler_id(conn: sqlite3.Connection, name: str) -> int:
    r = conn.execute("SELECT id FROM wrestlers WHERE LOWER(name) = LOWER(?)", (name.strip(),)).fetchone()
    if not r:
        raise ValueError(f"Unknown wrestler name: {name!r}")
    return int(r[0])


def read_participants_csv(path: str) -> List[Tuple[str, int, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"key", "side", "wrestler"}
        low = [h.lower() for h in reader.fieldnames or []]
        missing = [h for h in required if h not in low]
        if missing:
            raise KeyError(f"Missing columns in participants.csv: {missing}")
        out: List[Tuple[str, int, str]] = []
        for raw in reader:
            d = {k.lower(): v for k, v in raw.items()}
            key = d["key"].strip()
            side = int(d["side"]) if d["side"].strip() else 0
            if side < 1:
                raise ValueError(f"Side must be >=1 for Key {key}")
            wrestler = d["wrestler"].strip()
            if not key or not wrestler:
                raise ValueError("Key/Wrestler cannot be empty in participants.csv")
            out.append((key, side, wrestler))
        return out


def import_all(matches_csv: str, participants_csv: str, dry_run: bool = False) -> Tuple[int, int]:
    matches = read_matches_csv(matches_csv)
    parts = read_participants_csv(participants_csv)

    with connect_db() as conn:
        ensure_schema(conn)

        # Insert matches first, map Key->id
        key_to_id: Dict[str, int] = {}
        inserted_matches = 0
        for key, mr in matches.items():
            if dry_run:
                print(f"DRY-RUN: MATCH {key}: S{mr.season} Day {mr.day} | {mr.tournament} / {mr.round} | {mr.stipulation or '-'} | {mr.result} ws={mr.winner_side} | t={mr.time_seconds}")
                continue
            cur = conn.execute(
                """
                INSERT INTO matches (season, tournament, round, winner_side, result, stipulation, match_time_seconds, day_index)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (mr.season, mr.tournament, mr.round, mr.winner_side, mr.result, mr.stipulation, mr.time_seconds, mr.day),
            )
            key_to_id[key] = int(cur.lastrowid)
            inserted_matches += 1

        # If dry-run, we still need fake IDs to validate participant refs
        if dry_run:
            fake_id = 1
            for key in matches.keys():
                key_to_id[key] = fake_id; fake_id += 1

        # Validate winner side existence (only if not dry-run with fake ids)
        # We'll validate after loading participants per key
        inserted_parts = 0
        # Group participants by key
        from collections import defaultdict
        by_key: Dict[str, Dict[int, List[int]]] = defaultdict(lambda: defaultdict(list))
        for key, side, wname in parts:
            wid = lookup_wrestler_id(conn, wname)
            by_key[key][side].append(wid)

        # Now write participants and validate winner side
        for key, sides in by_key.items():
            if key not in key_to_id:
                raise ValueError(f"participants.csv references unknown Key: {key}")
            mr = matches[key]
            if mr.result == 'win' and (mr.winner_side or 0) not in sides:
                raise ValueError(f"Key {key}: Winner Side {mr.winner_side} has no participants")

            match_id = key_to_id[key]
            for side, wids in sides.items():
                for wid in wids:
                    if dry_run:
                        print(f"DRY-RUN: PART {key}: match_id=? side={side} wrestler_id={wid}")
                    else:
                        conn.execute(
                            "INSERT OR IGNORE INTO match_participants (match_id, side, wrestler_id) VALUES (?, ?, ?)",
                            (match_id, side, wid),
                        )
                        inserted_parts += 1

        if not dry_run:
            conn.commit()

        return inserted_matches, inserted_parts


def main() -> None:
    p = argparse.ArgumentParser(description="Import matches v2 (matches.csv + participants.csv)")
    p.add_argument("matches_csv")
    p.add_argument("participants_csv")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    m, pcount = import_all(args.matches_csv, args.participants_csv, dry_run=args.dry_run)
    if args.dry_run:
        print(f"Checked matches and participants. 0 rows inserted (dry-run).")
    else:
        print(f"Inserted: {m} matches, {pcount} participants.")


if __name__ == "__main__":
    main()
