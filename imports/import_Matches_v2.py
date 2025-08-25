# /imports/import_matches_v2.py — FULL FILE (v2 + team expansion + ensure_schema)
"""
Import v2: two CSVs linked by a human-friendly Key (supports any number of sides).
Also supports listing *team names* in participants.csv; importer expands a 2‑person
team into the two wrestler IDs at import time.

Usage (from project root):
  python imports/import_matches_v2.py data/matches.csv data/participants.csv --dry-run
  python imports/import_matches_v2.py data/matches.csv data/participants.csv

CSV A: matches.csv (headers, case-insensitive)
  Key,Season,Day,Tournament,Round,Stipulation,Result,Winner Side,Match Time
    - Result: win|draw|nc  (win requires Winner Side)
    - Match Time: MM:SS (stored as seconds; blank allowed)

CSV B: participants.csv (headers)
  Key,Side,Wrestler[,Type]
    - Side: integer >=1; tag teams share the same Side; multi-way uses 1..N
    - Wrestler: roster name or team name
    - Type (optional): "Wrestler" or "Team" to force resolution

Validations:
  - Winner Side must exist among participants when Result=win
  - Every Wrestler/Team must resolve (team must have exactly 2 members)
  - Day >= 1; Time format MM:SS if provided
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
    """v2 schema: matches has NO comp1_*/comp2_*; participants carries the sides."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
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
        low = [h.lower() for h in (reader.fieldnames or [])]
        missing = [h for h in required if h not in low]
        if missing:
            raise KeyError(f"Missing columns in matches.csv: {missing}")
        rows: Dict[str, MatchRow] = {}
        for raw in reader:
            d = {k.lower(): (v or "") for k, v in raw.items()}
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
            if key in rows:
                raise ValueError(f"Duplicate Key in matches.csv: {key}")
            rows[key] = MatchRow(key, season, day, tournament, round_, stip, result, winner_side, tsec)
        return rows


def lookup_wrestler_id(conn: sqlite3.Connection, name: str) -> int:
    r = conn.execute("SELECT id FROM wrestlers WHERE LOWER(name) = LOWER(?)", (name.strip(),)).fetchone()
    if not r:
        raise ValueError(f"Unknown wrestler name: {name!r}")
    return int(r[0])


def resolve_name_to_ids(conn: sqlite3.Connection, name: str, forced_type: Optional[str] = None) -> List[int]:
    """Return a list of wrestler IDs for this participant name.
    - If it's a wrestler name → [id]
    - If it's a 2‑person team name → [id1, id2]
    - If forced_type is provided ("wrestler"|"team" - case-insensitive), only resolve that type.
    Raises ValueError if it cannot resolve.
    """
    nm = (name or "").strip()
    if not nm:
        raise ValueError("Empty participant name")
    forced = (forced_type or "").strip().lower()

    def find_wrestler(n: str) -> Optional[int]:
        r = conn.execute("SELECT id FROM wrestlers WHERE LOWER(name) = LOWER(?)", (n.strip(),)).fetchone()
        return int(r[0]) if r else None

    def find_team_members(n: str) -> Optional[List[int]]:
        t = conn.execute("SELECT id FROM tag_teams WHERE LOWER(name) = LOWER(?)", (n.strip(),)).fetchone()
        if not t:
            return None
        tid = int(t[0])
        rows = conn.execute("SELECT wrestler_id FROM tag_team_members WHERE team_id = ?", (tid,)).fetchall()
        ids = [int(r[0]) for r in rows]
        if len(ids) != 2:
            raise ValueError(f"Team {name!r} does not have exactly 2 members ({len(ids)})")
        return ids

    if forced == "wrestler":
        wid = find_wrestler(nm)
        if wid is None:
            raise ValueError(f"Unknown wrestler name: {name!r}")
        return [wid]
    if forced == "team":
        ids = find_team_members(nm)
        if ids is None:
            raise ValueError(f"Unknown team name: {name!r}")
        return ids

    # Auto-detect: wrestler first, then team
    wid = find_wrestler(nm)
    if wid is not None:
        return [wid]
    ids = find_team_members(nm)
    if ids is not None:
        return ids

    raise ValueError(f"Unknown participant name (not a wrestler or team): {name!r}")


def read_participants_csv(path: str) -> List[Tuple[str, int, str, Optional[str]]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"key", "side", "wrestler"}
        low = [h.lower() for h in (reader.fieldnames or [])]
        missing = [h for h in required if h not in low]
        if missing:
            raise KeyError(f"Missing columns in participants.csv: {missing}")
        out: List[Tuple[str, int, str, Optional[str]]] = []
        # map canonical header names
        header_map = {h.lower(): h for h in reader.fieldnames or []}
        col_key = header_map["key"]
        col_side = header_map["side"]
        col_wrestler = header_map["wrestler"]
        col_type = header_map.get("type")  # optional
        for raw in reader:
            key = (raw.get(col_key) or "").strip()
            side_txt = (raw.get(col_side) or "").strip()
            name = (raw.get(col_wrestler) or "").strip()
            forced = (raw.get(col_type) or "").strip() if col_type else None
            if not key or not name:
                raise ValueError("Key/Wrestler cannot be empty in participants.csv")
            if not side_txt:
                raise ValueError(f"Missing Side for Key {key}")
            try:
                side = int(side_txt)
            except ValueError:
                raise ValueError(f"Side must be an integer for Key {key}, got {side_txt!r}")
            if side < 1:
                raise ValueError(f"Side must be >=1 for Key {key}")
            out.append((key, side, name, forced))
        return out


def import_all(matches_csv: str, participants_csv: str, dry_run: bool = False) -> Tuple[int, int]:
    matches = read_matches_csv(matches_csv)
    parts = read_participants_csv(participants_csv)

    with connect_db() as conn:
        ensure_schema(conn)

        # Group participants by Key -> Side -> [wrestler_id], expanding 2‑person teams
        from collections import defaultdict
        by_key_ids: Dict[str, Dict[int, List[int]]] = defaultdict(lambda: defaultdict(list))
        for key, side, wname, forced_type in parts:
            if key not in matches:
                raise ValueError(f"participants.csv references unknown Key: {key}")
            wids = resolve_name_to_ids(conn, wname, forced_type)
            for wid in wids:
                by_key_ids[key][side].append(wid)

        # Validate winner side consistency
        for key, mr in matches.items():
            if mr.result == "win":
                sides = by_key_ids.get(key, {})
                if (mr.winner_side or 0) not in sides:
                    raise ValueError(f"Key {key}: Winner Side {mr.winner_side} has no participants")

        if dry_run:
            # Summary like previous output
            for key, mr in matches.items():
                print(
                    f"DRY-RUN: MATCH {key}: S{mr.season} Day {mr.day} | {mr.tournament} / {mr.round} | "
                    f"{mr.stipulation or '—'} | {mr.result} ws={mr.winner_side} | t={mr.time_seconds}"
                )
            # Also show participants briefly
            for key, sides in by_key_ids.items():
                for side, wids in sorted(sides.items()):
                    print(f"DRY-RUN: PART {key}: side={side} count={len(wids)}")
            return 0, 0

        # Insert matches
        key_to_id: Dict[str, int] = {}
        inserted_matches = 0
        for key, mr in matches.items():
            cur = conn.execute(
                """
                INSERT INTO matches (
                    season, tournament, round, winner_side, result, stipulation, match_time_seconds, day_index
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (mr.season, mr.tournament, mr.round, mr.winner_side, mr.result, mr.stipulation, mr.time_seconds, mr.day),
            )
            key_to_id[key] = int(cur.lastrowid)
            inserted_matches += 1

        # Insert participants
        inserted_parts = 0
        for key, sides in by_key_ids.items():
            match_id = key_to_id[key]
            for side, wids in sides.items():
                for wid in wids:
                    conn.execute(
                        "INSERT OR IGNORE INTO match_participants (match_id, side, wrestler_id) VALUES (?, ?, ?)",
                        (match_id, side, wid),
                    )
                    inserted_parts += 1

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
        print("Checked matches and participants. 0 rows inserted (dry-run).")
    else:
        print(f"Inserted: {m} matches, {pcount} participants.")


if __name__ == "__main__":
    main()
