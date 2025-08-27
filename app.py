from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import List, Optional
from datetime import date

from fastapi import FastAPI, Request, Form, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
import json

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "wut.db"
STATIC_DIR = APP_DIR / "static"
TEMPLATES_DIR = APP_DIR / "templates"


# Photos directory
PHOTOS_DIR = STATIC_DIR / "photos"
PHOTOS_DIR.mkdir(parents=True, exist_ok=True)

# Season setting
CURRENT_SEASON = 4

app = FastAPI(title="Wrestling Universe Tracker")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# === Register Jinja globals for 2kw Highlights ===============================

def _wrestler_highlights_jinja(wid: int, season: int = 1) -> list[str]:
    conn = get_conn()
    try:
        return get_wrestler_highlights(conn, wid, season)
    finally:
        conn.close()


def _team_highlights_jinja(tid: int, season: int = 1) -> list[str]:
    conn = get_conn()
    try:
        return get_team_highlights(conn, tid, season)
    finally:
        conn.close()

        # === DB-backed Highlights reader ============================================

# === US Title defense counts in DB‑backed highlights ========================
# Replace your existing wrestler_highlights_db with this version.

US_LABELS = {
    "Mens US Championship Winner": "Mens US Championship",
    "Womens US Championship Winner": "Womens US Championship",
}


def _us_defense_count(conn: sqlite3.Connection, wid: int, season: int, title_name: str) -> int | None:
    # Try reading from championship_seasons (preferred)
    cols_cs = {r[1] for r in conn.execute("PRAGMA table_info('championship_seasons')").fetchall()}
    if not {"championship_id", "season"}.issubset(cols_cs):
        return None
    # Need champion_wrestler_id to attribute to a person
    if "champion_wrestler_id" not in cols_cs:
        return None
    row = conn.execute(
        """
        SELECT cs.*
        FROM championship_seasons cs
        JOIN championships c ON c.id = cs.championship_id
        WHERE cs.season = ? AND c.name = ? AND cs.champion_wrestler_id = ?
        LIMIT 1
        """,
        (season, title_name, wid),
    ).fetchone()
    if not row:
        return None
    for col in ("defenses", "successful_defenses", "defense_count"):
        if col in cols_cs:
            val = row[col]
            if val is None:
                return None
            try:
                return int(val)
            except Exception:
                return None
    return None


def wrestler_highlights_db(wid: int, season: int | None = 1) -> list[str]:
    conn = get_conn()
    try:
        # Ensure tables exist before querying (avoids OperationalError on fresh DBs)
        ensure_highlights_schema(conn)

        if season is None:
            rows = conn.execute(
                """
                SELECT ht.label, wh.season
                FROM wrestler_highlights wh
                JOIN highlight_types ht ON ht.id = wh.highlight_id
                WHERE wh.wrestler_id = ?
                ORDER BY wh.season
                """,
                (wid,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT ht.label, wh.season
                FROM wrestler_highlights wh
                JOIN highlight_types ht ON ht.id = wh.highlight_id
                WHERE wh.wrestler_id = ? AND wh.season = ?
                ORDER BY wh.season
                """,
                (wid, season),
            ).fetchall()

        by_label: dict[str, list[int]] = {}
        for r in rows:
            by_label.setdefault(r["label"], []).append(int(r["season"]))

        out: list[str] = []
        for label, seasons in by_label.items():
            uniq = sorted(set(seasons))
            if label in US_LABELS:
                title_name = US_LABELS[label]
                for s in uniq:
                    d = _us_defense_count(conn, wid, s, title_name)
                    if d is None:
                        out.append(f"1 x {label} (Season {s})")
                    else:
                        out.append(f"1 x {label} (Season {s}, {d} defenses)")
            else:
                count = len(uniq)
                s_repr = f"Season {uniq[0]}" if count == 1 else f"Seasons {', '.join(str(x) for x in uniq)}"
                out.append(f"{count} x {label} ({s_repr})")
        return sorted(out)
    finally:
        conn.close()


def team_highlights_db(tid: int, season: int | None = None) -> list[str]:
    conn = get_conn()
    try:
        # Ensure tables exist before querying (avoids OperationalError on fresh DBs)
        ensure_highlights_schema(conn)

        if season is None:
            rows = conn.execute(
                """
                SELECT ht.label, th.season
                FROM team_highlights th
                JOIN highlight_types ht ON ht.id = th.highlight_id
                WHERE th.team_id = ?
                ORDER BY th.season
                """,
                (tid,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT ht.label, th.season
                FROM team_highlights th
                JOIN highlight_types ht ON ht.id = th.highlight_id
                WHERE th.team_id = ? AND th.season = ?
                ORDER BY th.season
                """,
                (tid, season),
            ).fetchall()

        by_label: dict[str, list[int]] = {}
        for r in rows:
            by_label.setdefault(r["label"], []).append(int(r["season"]))

        out: list[str] = []
        for label, seasons in by_label.items():
            uniq = sorted(set(seasons))
            count = len(uniq)
            s_repr = f"Season {uniq[0]}" if count == 1 else f"Seasons {', '.join(str(x) for x in uniq)}"
            out.append(f"{count} x {label} ({s_repr})")
        return sorted(out)
    finally:
        conn.close()


# Register for Jinja
try:
    templates.env.globals["team_highlights_db"] = team_highlights_db
except Exception:
    pass
# === /Team DB-backed reader ================================================



def register_template_globals() -> None:
    templates.env.globals["wrestler_highlights"] = _wrestler_highlights_jinja
    templates.env.globals["team_highlights"] = _team_highlights_jinja
    templates.env.globals["wrestler_highlights_db"] = wrestler_highlights_db

register_template_globals()
# === /Register Jinja globals =================================================

# ---------------- DB helpers ----------------



def load_champ_order() -> None:
    """Load optional config/championship_order.json and build lookup sets.
    Stores on app.state.champ_order = {featured, order, index, featured_set}
    """
    cfg_path = APP_DIR / "config" / "championship_order.json"
    featured: list[str] = []
    order: list[str] = []
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
        featured = [str(x).strip() for x in data.get("featured", []) if str(x).strip()]
        order = [str(x).strip() for x in data.get("order", []) if str(x).strip()]
    except FileNotFoundError:
        # optional file; skip if missing
        cfg_path.parent.mkdir(exist_ok=True)
    except Exception as e:
        print(f"[warning] Failed to read {cfg_path}: {e}")

    index = {name.lower(): i for i, name in enumerate(order)}
    featured_set = {name.lower() for name in featured}
    app.state.champ_order = {
        "featured": featured,
        "order": order,
        "index": index,
        "featured_set": featured_set,
    }



def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    return conn


# === Highlights schema (extend with team_highlights) =========================

def ensure_highlights_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS highlight_types (
            id INTEGER PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            label TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wrestler_highlights (
            wrestler_id INTEGER NOT NULL,
            highlight_id INTEGER NOT NULL,
            season INTEGER NOT NULL,
            PRIMARY KEY (wrestler_id, highlight_id, season)
        )
        """
    )

    # NEW: per-team cached highlights (e.g., Tag Team World Champion)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS team_highlights (
            team_id INTEGER NOT NULL,
            highlight_id INTEGER NOT NULL,
            season INTEGER NOT NULL,
            PRIMARY KEY (team_id, highlight_id, season)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS highlight_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at TEXT NOT NULL DEFAULT (datetime('now')),
            season INTEGER,
            last_day INTEGER,
            last_order INTEGER,
            last_match_id INTEGER
        )
        """
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_wh_wrestler ON wrestler_highlights(wrestler_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_wh_season   ON wrestler_highlights(season)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_th_team     ON team_highlights(team_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_th_season   ON team_highlights(season)")
    conn.commit()
# === /schema ================================================================


# === /Highlights schema ======================================================

# === Highlights seeding ======================================================

# === Highlights seeding ======================================================

def seed_highlight_types(conn: sqlite3.Connection) -> None:
    rows: list[tuple[str, str]] = [
        # World (Men)
        ("mens_world_champion", "Mens World Champion"),
        ("mens_world_runner_up", "Mens World Championship Runner-up"),
        ("mens_world_sf", "Mens World Championship Semi-Finalist"),
        ("mens_world_qf", "Mens World Championship Quarter-Finalist"),
        # World (Women)
        ("womens_world_champion", "Womens World Champion"),
        ("womens_world_runner_up", "Womens World Championship Runner-up"),
        ("womens_world_sf", "Womens World Championship Semi-Finalist"),
        ("womens_world_qf", "Womens World Championship Quarter-Finalist"),

        # Tag Team World (gender-neutral tournament name)
        ("tag_world_champion", "Tag Team World Champion"),
        ("tag_world_runner_up", "Tag Team World Championship Runner-up"),
        ("tag_world_sf", "Tag Team World Championship Semi-Finalist"),

        # NXT (Men)
        ("mens_nxt_champion", "Mens NXT Champion"),
        ("mens_nxt_runner_up", "Mens NXT Championship Runner-up"),
        ("mens_nxt_sf", "Mens NXT Championship Semi-Finalist"),
        # NXT (Women)
        ("womens_nxt_champion", "Womens NXT Champion"),
        ("womens_nxt_runner_up", "Womens NXT Championship Runner-up"),
        ("womens_nxt_sf", "Womens NXT Championship Semi-Finalist"),

        # Underground (Men)
        ("mens_underground_champion", "Mens Underground Champion"),
        ("mens_underground_runner_up", "Mens Underground Championship Runner-up"),
        # Underground (Women)
        ("womens_underground_champion", "Womens Underground Champion"),
        ("womens_underground_runner_up", "Womens Underground Championship Runner-up"),

        # Hardcore (Men)
        ("mens_hardcore_champion", "Mens Hardcore Champion"),
        ("mens_hardcore_runner_up", "Mens Hardcore Championship Runner-up"),
        # Hardcore (Women)
        ("womens_hardcore_champion", "Womens Hardcore Champion"),
        ("womens_hardcore_runner_up", "Womens Hardcore Championship Runner-up"),

        # Royal Rumble
        ("mens_rumble_winner", "Mens Royal Rumble Winner"),
        ("womens_rumble_winner", "Womens Royal Rumble Winner"),

        # US Championship — title match winner (from title page data)
        ("mens_us_championship_winner", "Mens US Championship Winner"),
        ("womens_us_championship_winner", "Womens US Championship Winner"),

        # Elimination Chamber — winner
        ("mens_elimination_chamber_winner", "Mens Elimination Chamber Winner"),
        ("womens_elimination_chamber_winner", "Womens Elimination Chamber Winner"),

        # Extra one-off tournaments
        ("mens_andre_battle_royal_winner", "Mens Andre Battle Royal Winner"),
        ("womens_chyna_battle_royal_winner", "Womens Chyna Battle Royal Winner"),
        ("mens_dusty_rhodes_gauntlet_winner", "Mens Dusty Rhodes Gauntlet Winner"),
        ("womens_mae_young_gauntlet_winner", "Womens Mae Young Gauntlet Winner"),
    ]
    for code, label in rows:
        conn.execute(
            "INSERT OR IGNORE INTO highlight_types(code, label) VALUES (?, ?)",
            (code, label),
        )
    conn.commit()

# === Highlights: tournament & round constants (exact spellings) ==============
# Round names (exact strings used in DB; comparisons will be case-insensitive)
ROUND_QF = "Quarter Final"
ROUND_SF = "Semi Final"
ROUND_F  = "Final"

# Mens / Womens singles championships
T_WORLD_MEN   = "Mens World Championship"
T_WORLD_WOMEN = "Womens World Championship"

T_NXT_MEN     = "Mens NXT Championship"
T_NXT_WOMEN   = "Womens NXT Championship"

T_UNDERGROUND_MEN   = "Mens Underground Championship"
T_UNDERGROUND_WOMEN = "Womens Underground Championship"

T_HARDCORE_MEN   = "Mens Hardcore Championship"
T_HARDCORE_WOMEN = "Womens Hardcore Championship"

T_US_MEN   = "Mens US Championship"
T_US_WOMEN = "Womens US Championship"

# One-off season matches (no round; one per season)
T_RUMBLE_MEN   = "Mens Royal Rumble"
T_RUMBLE_WOMEN = "Womens Royal Rumble"

T_ELIM_MEN   = "Mens Elimination Chamber"
T_ELIM_WOMEN = "Womens Elimination Chamber"

T_ANDRE_MEN = "Mens Andre Battle Royal"
T_CHYNA_WOMEN = "Womens Chyna Battle Royal"

T_DUSTY_MEN = "Mens Dusty Rhodes Gauntlet"
T_MAE_WOMEN = "Womens Mae Young Gauntlet"

# Tag team tournament (gender-neutral)
T_TAG_WORLD = "Tag Team World Championship"

# Convenience collections for future recompute logic
ROUND_NAME_SET = {ROUND_QF.lower(), ROUND_SF.lower(), ROUND_F.lower()}
ONE_OFF_TOURNAMENTS = {
    T_RUMBLE_MEN, T_RUMBLE_WOMEN,
    T_ELIM_MEN, T_ELIM_WOMEN,
    T_ANDRE_MEN, T_CHYNA_WOMEN,
    T_DUSTY_MEN, T_MAE_WOMEN,
}
CHAMPIONSHIP_TOURNAMENTS = {
    T_WORLD_MEN, T_WORLD_WOMEN,
    T_NXT_MEN, T_NXT_WOMEN,
    T_UNDERGROUND_MEN, T_UNDERGROUND_WOMEN,
    T_HARDCORE_MEN, T_HARDCORE_WOMEN,
    T_US_MEN, T_US_WOMEN,
    T_TAG_WORLD,
}
# === /constants =============================================================


def _finals_row_any(
    conn: sqlite3.Connection,
    season: int,
    aliases: set[str],
    rounds: set[str],
):
    """Return the most recent finals match for any alias (case-insensitive)."""
    a = sorted({x.strip().lower() for x in aliases if x})
    r = sorted({x.strip().lower() for x in rounds if x})
    sql = (
        "SELECT id, winner_side, tournament, round FROM matches "
        "WHERE season = ?"
    )
    params: list[object] = [season]
    if a:
        sql += f" AND lower(tournament) IN ({','.join(['?'] * len(a))})"
        params.extend(a)
    if r:
        sql += f" AND lower(COALESCE(round,'')) IN ({','.join(['?'] * len(r))})"
        params.extend(r)
    sql += " ORDER BY id DESC LIMIT 1"
    return conn.execute(sql, params).fetchone()


# Round name normalization (lowercased)
_ROUND_FINALS: set[str] = {"final"}
_ROUND_SF: set[str] = {"semi final"}
_ROUND_QF: set[str] = {"quarter final"}


def _match_row(conn: sqlite3.Connection, season: int, tournament: str, rounds: set[str]):
    sql = (
        "SELECT id, winner_side, tournament, round "
        "FROM matches "
        "WHERE season = ? AND lower(tournament) = lower(?)"
    )
    params: list[object] = [season, tournament]
    if rounds:
        placeholders = ",".join("?" for _ in rounds)
        sql += f" AND lower(COALESCE(round,'')) IN ({placeholders})"
        params.extend(list(rounds))
    sql += " ORDER BY id DESC LIMIT 1"
    return conn.execute(sql, params).fetchone()


def _side_members(conn: sqlite3.Connection, match_id: int) -> dict[int, set[int]]:
    rows = conn.execute(
        "SELECT side, wrestler_id FROM match_participants WHERE match_id = ?",
        (match_id,),
    ).fetchall()
    sides: dict[int, set[int]] = {}
    for r in rows:
        sides.setdefault(int(r["side"]), set()).add(int(r["wrestler_id"]))
    return sides





def get_wrestler_highlights(conn: sqlite3.Connection, wid: int, season: int | None = 1) -> list[str]:
    """Build highlight lines for a wrestler.
    - If `season` is an int, computes for that season only.
    - If `season` is None, aggregates across **all seasons**.
    Includes: World (Champion/Runner-up + SF/QF tiers), Hardcore (Champion/Runner-up), Underground (Champion/Runner-up), Rumble (Winner).
    """
    # Seasons to scan
    if season is None:
        rows = conn.execute(
            """
            SELECT DISTINCT m.season
            FROM matches m
            JOIN match_participants p ON p.match_id = m.id
            WHERE p.wrestler_id = ?
            """,
            (wid,),
        ).fetchall()
        seasons = [int(r["season"]) for r in rows]
    else:
        seasons = [int(season)]

    if not seasons:
        return []

    ach: dict[str, list[int]] = {}

    for s in seasons:
        # --- World Championship finals (Champion / Runner-up)
        final_world = _match_row(conn, s, HIGHLIGHTS_TOURNAMENT_WORLD, _ROUND_FINALS)
        champion_or_runner_world = False
        if final_world and final_world["winner_side"] is not None:
            sides = _side_members(conn, final_world["id"])
            winners = sides.get(int(final_world["winner_side"]), set())
            others = set().union(*[m for sid, m in sides.items() if sid != int(final_world["winner_side"])])
            if wid in winners:
                ach.setdefault("Mens World Champion", []).append(s)
                champion_or_runner_world = True
            elif wid in others:
                ach.setdefault("Mens World Championship Runner-up", []).append(s)
                champion_or_runner_world = True

        # --- World SF / QF tiers
        if not champion_or_runner_world:
            sf_hit = conn.execute(
                f"""
                SELECT 1 FROM matches m
                JOIN match_participants p ON p.match_id = m.id
                WHERE m.season = ? AND lower(m.tournament) = lower(?)
                  AND lower(COALESCE(m.round,'')) IN ({','.join(['?'] * len(_ROUND_SF))})
                  AND p.wrestler_id = ?
                LIMIT 1
                """,
                (s, HIGHLIGHTS_TOURNAMENT_WORLD, *list(_ROUND_SF), wid),
            ).fetchone()
            final_part = conn.execute(
                f"""
                SELECT 1 FROM matches m
                JOIN match_participants p ON p.match_id = m.id
                WHERE m.season = ? AND lower(m.tournament) = lower(?)
                  AND lower(COALESCE(m.round,'')) IN ({','.join(['?'] * len(_ROUND_FINALS))})
                  AND p.wrestler_id = ?
                LIMIT 1
                """,
                (s, HIGHLIGHTS_TOURNAMENT_WORLD, *list(_ROUND_FINALS), wid),
            ).fetchone()
            if sf_hit and not final_part:
                ach.setdefault("Mens World Championship Semi-Finalist", []).append(s)
            else:
                qf_hit = conn.execute(
                    f"""
                    SELECT 1 FROM matches m
                    JOIN match_participants p ON p.match_id = m.id
                    WHERE m.season = ? AND lower(m.tournament) = lower(?)
                      AND lower(COALESCE(m.round,'')) IN ({','.join(['?'] * len(_ROUND_QF))})
                      AND p.wrestler_id = ?
                    LIMIT 1
                    """,
                    (s, HIGHLIGHTS_TOURNAMENT_WORLD, *list(_ROUND_QF), wid),
                ).fetchone()
                progressed = conn.execute(
                    f"""
                    SELECT 1 FROM matches m
                    JOIN match_participants p ON p.match_id = m.id
                    WHERE m.season = ? AND lower(m.tournament) = lower(?)
                      AND lower(COALESCE(m.round,'')) IN ({','.join(['?'] * (len(_ROUND_SF) + len(_ROUND_FINALS)))})
                      AND p.wrestler_id = ?
                    LIMIT 1
                    """,
                    (s, HIGHLIGHTS_TOURNAMENT_WORLD, *list(_ROUND_SF | _ROUND_FINALS), wid),
                ).fetchone()
                if qf_hit and not progressed:
                    ach.setdefault("Mens World Championship Quarter-Finalist", []).append(s)

        # --- Hardcore (finals only)
        final_hardcore = _finals_row_any(conn, s, HIGHLIGHTS_TOURNAMENT_HARDCORE_ALIASES, _ROUND_FINALS)
        if final_hardcore and final_hardcore["winner_side"] is not None:
            sides = _side_members(conn, final_hardcore["id"])
            winners = sides.get(int(final_hardcore["winner_side"]), set())
            others = set().union(*[m for sid, m in sides.items() if sid != int(final_hardcore["winner_side"])])
            if wid in winners:
                ach.setdefault("Mens Hardcore Champion", []).append(s)
            elif wid in others:
                ach.setdefault("Mens Hardcore Championship Runner-up", []).append(s)

        # --- Underground (finals only)
        final_ug = _finals_row_any(conn, s, HIGHLIGHTS_TOURNAMENT_UNDERGROUND_ALIASES, _ROUND_FINALS)
        if final_ug and final_ug["winner_side"] is not None:
            sides = _side_members(conn, final_ug["id"])
            winners = sides.get(int(final_ug["winner_side"]), set())
            others = set().union(*[m for sid, m in sides.items() if sid != int(final_ug["winner_side"])])
            if wid in winners:
                ach.setdefault("Mens Underground Champion", []).append(s)
            elif wid in others:
                ach.setdefault("Mens Underground Championship Runner-up", []).append(s)

        # --- Rumble winner
        rumble_final = _match_row(conn, s, HIGHLIGHTS_TOURNAMENT_RUMBLE, _ROUND_FINALS | {''})
        if rumble_final and rumble_final["winner_side"] is not None:
            r_sides = _side_members(conn, rumble_final["id"])
            if wid in r_sides.get(int(rumble_final["winner_side"]), set()):
                ach.setdefault("Mens Royal Rumble Winner", []).append(s)

    ordered = [
        "Mens World Champion",
        "Mens World Championship Runner-up",
        "Mens World Championship Semi-Finalist",
        "Mens World Championship Quarter-Finalist",
        "Mens Hardcore Champion",
        "Mens Hardcore Championship Runner-up",
        "Mens Underground Champion",
        "Mens Underground Championship Runner-up",
        "Mens Royal Rumble Winner",
    ]

    lines: list[str] = []
    for label in ordered:
        seasons_list = ach.get(label, [])
        if not seasons_list:
            continue
        uniq = sorted(set(seasons_list))
        count = len(uniq)
        season_str = (
            f"Season {uniq[0]}" if count == 1 else f"Seasons {', '.join(str(x) for x in uniq)}"
        )
        lines.append(f"{count} x {label} ({season_str})")

    return lines




def get_team_highlights(conn: sqlite3.Connection, tid: int, season: int = 1) -> list[str]:
    # Team champions recorded via championship_seasons.champion_team_id
    cols = {row[1] for row in conn.execute("PRAGMA table_info('championship_seasons')").fetchall()}
    if "champion_team_id" not in cols:
        return []
    rows = conn.execute(
        """
        SELECT cs.season, c.name AS championship_name
        FROM championship_seasons cs
        JOIN championships c ON c.id = cs.championship_id
        WHERE cs.season = ? AND cs.champion_team_id = ?
        ORDER BY cs.season DESC
        """,
        (season, tid),
    ).fetchall()
    return [f"1 x {r['championship_name']} Champion (Season {r['season']})" for r in rows]
# === /2kw Highlights =========================================================



# === DB-backed Highlights reader ============================================

def wrestler_highlights_db(wid: int, season: int | None = 1) -> list[str]:
    conn = get_conn()
    try:
        if season is None:
            rows = conn.execute(
                """
                SELECT ht.label, wh.season
                FROM wrestler_highlights wh
                JOIN highlight_types ht ON ht.id = wh.highlight_id
                WHERE wh.wrestler_id = ?
                ORDER BY wh.season
                """,
                (wid,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT ht.label, wh.season
                FROM wrestler_highlights wh
                JOIN highlight_types ht ON ht.id = wh.highlight_id
                WHERE wh.wrestler_id = ? AND wh.season = ?
                ORDER BY wh.season
                """,
                (wid, season),
            ).fetchall()

        by_label: dict[str, list[int]] = {}
        for r in rows:
            by_label.setdefault(r["label"], []).append(int(r["season"]))

        out: list[str] = []
        for label, seasons in by_label.items():
            uniq = sorted(set(seasons))
            count = len(uniq)
            s_repr = f"Season {uniq[0]}" if count == 1 else f"Seasons {', '.join(str(x) for x in uniq)}"
            out.append(f"{count} x {label} ({s_repr})")
        return sorted(out)
    finally:
        conn.close()
# === /DB-backed reader ======================================================



def _column_exists(c: sqlite3.Connection, table: str, col: str) -> bool:
    return any(r[1] == col for r in c.execute(f"PRAGMA table_info({table})"))

# Paste this HELPER near your other DB helpers (e.g., under _column_exists). It rebuilds
# championship_seasons so champion_id is NULL-able and adds team columns.

def _rebuild_championship_seasons_nullable_team(conn: sqlite3.Connection) -> None:
    """If championship_seasons.champion_id is NOT NULL, rebuild the table so it becomes NULL-able
    and include champion_team_id / runner_up_team_id columns.
    Safe to run multiple times.
    """
    info = conn.execute("PRAGMA table_info(championship_seasons)").fetchall()
    if not info:
        return
    cols = {r[1].lower(): r for r in info}
    # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
    champion_notnull = cols.get("champion_id", [None, None, None, 0])[3] == 1
    has_team_cols = ("champion_team_id" in cols) and ("runner_up_team_id" in cols)

    if not champion_notnull:
        # Already nullable → nothing to do
        return

    # Rebuild with desired shape
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS championship_seasons_new (
            championship_id    INTEGER NOT NULL,
            season             INTEGER NOT NULL,
            champion_id        INTEGER NULL,
            runner_up_id       INTEGER NULL,
            champion_team_id   INTEGER NULL,
            runner_up_team_id  INTEGER NULL,
            PRIMARY KEY (championship_id, season)
        )
        """
    )

    if has_team_cols:
        conn.execute(
            """
            INSERT INTO championship_seasons_new (
                championship_id, season, champion_id, runner_up_id, champion_team_id, runner_up_team_id
            )
            SELECT championship_id, season, champion_id, runner_up_id, champion_team_id, runner_up_team_id
            FROM championship_seasons
            """
        )
    else:
        conn.execute(
            """
            INSERT INTO championship_seasons_new (
                championship_id, season, champion_id, runner_up_id
            )
            SELECT championship_id, season, champion_id, runner_up_id
            FROM championship_seasons
            """
        )

    conn.execute("DROP TABLE championship_seasons")
    conn.execute("ALTER TABLE championship_seasons_new RENAME TO championship_seasons")
    # Ensure unique index for ON CONFLICT (also covered by PRIMARY KEY)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_cs_ch_season ON championship_seasons(championship_id, season)"
    )
    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()

# === Highlights dry-run compute (World + Tag only) ===========================
from collections import defaultdict
from typing import Dict, List, Set

# Round names (exact per your DB; case-insensitive compare)
ROUND_QF = "Quarter Final"
ROUND_SF = "Semi Final"
ROUND_F  = "Final"

# Tournament names (exact per your DB)
T_WORLD_MEN   = "Mens World Championship"
T_WORLD_WOMEN = "Womens World Championship"
T_TAG_WORLD   = "Tag Team World Championship"


def _participants_by_side(conn: sqlite3.Connection, match_id: int) -> Dict[int, Set[int]]:
    rows = conn.execute(
        "SELECT side, wrestler_id FROM match_participants WHERE match_id = ?",
        (match_id,),
    ).fetchall()
    out: Dict[int, Set[int]] = {}
    for r in rows:
        out.setdefault(int(r["side"]), set()).add(int(r["wrestler_id"]))
    return out


def _winners_and_losers(conn: sqlite3.Connection, match_row) -> tuple[Set[int], Set[int]]:
    if match_row is None or match_row["winner_side"] is None:
        return set(), set()
    sides = _participants_by_side(conn, int(match_row["id"]))
    winners = set(sides.get(int(match_row["winner_side"]), set()))
    losers = set().union(*sides.values()) - winners if sides else set()
    return winners, losers


def _final_match(conn: sqlite3.Connection, season: int, tournament: str):
    return conn.execute(
        """
        SELECT id, winner_side
        FROM matches
        WHERE season = ? AND lower(tournament) = lower(?) AND lower(COALESCE(round,'')) = lower(?)
        ORDER BY id DESC
        LIMIT 1
        """,
        (season, tournament, ROUND_F),
    ).fetchone()


def _round_matches(conn: sqlite3.Connection, season: int, tournament: str, round_name: str):
    return conn.execute(
        """
        SELECT id, winner_side
        FROM matches
        WHERE season = ? AND lower(tournament) = lower(?) AND lower(COALESCE(round,'')) = lower(?)
        ORDER BY id ASC
        """,
        (season, tournament, round_name),
    ).fetchall()


def _label_for_world(tournament: str, kind: str) -> str:
    if tournament == T_WORLD_MEN:
        base = "Mens World Championship"
        if kind == "champ":
            return "Mens World Champion"
    elif tournament == T_WORLD_WOMEN:
        base = "Womens World Championship"
        if kind == "champ":
            return "Womens World Champion"
    elif tournament == T_TAG_WORLD:
        base = "Tag Team World Championship"
        if kind == "champ":
            return "Tag Team World Champion"
    else:
        base = tournament
    if kind == "runner":
        return f"{base} Runner-up"
    if kind == "sf":
        return f"{base} Semi-Finalist"
    if kind == "qf":
        return f"{base} Quarter-Finalist"
    return base


def dry_run_world_tag(conn: sqlite3.Connection, season: int | None = None) -> Dict[int, List[str]]:
    """Compute highlights for World (Men/Women) and Tag Team World for the given season(s).
    Returns a mapping wrestler_id -> list of label strings (not persisted).
    """
    # Seasons to process
    if season is None:
        seasons_rows = conn.execute("SELECT DISTINCT season FROM matches ORDER BY season").fetchall()
        seasons = [int(r["season"]) for r in seasons_rows]
    else:
        seasons = [int(season)]

    labels_by_wrestler: Dict[int, Set[str]] = defaultdict(set)

    for s in seasons:
        for T in (T_WORLD_MEN, T_WORLD_WOMEN, T_TAG_WORLD):
            # Finals: champion / runner-up
            fin = _final_match(conn, s, T)
            W, L = _winners_and_losers(conn, fin)
            for wid in W:
                labels_by_wrestler[wid].add(_label_for_world(T, "champ"))
            for wid in L:
                labels_by_wrestler[wid].add(_label_for_world(T, "runner"))

            # Collect participants who made SF or Final to suppress lower tier
            final_participants: Set[int] = set(W) | set(L)

            # Semi-Finalists: losers only, who did not appear in the Final
            for sf in _round_matches(conn, s, T, ROUND_SF):
                Ws, Ls = _winners_and_losers(conn, sf)
                for wid in Ls:
                    if wid not in final_participants:
                        labels_by_wrestler[wid].add(_label_for_world(T, "sf"))

            # Quarter-Finalists (World only; Tag has no QF): losers only, who did not make SF/Final
            if T in (T_WORLD_MEN, T_WORLD_WOMEN):
                # Build set of everyone who made SF or Final
                sf_participants: Set[int] = set()
                for sf in _round_matches(conn, s, T, ROUND_SF):
                    sides = _participants_by_side(conn, int(sf["id"]))
                    for group in sides.values():
                        sf_participants.update(group)
                higher = final_participants | sf_participants

                for qf in _round_matches(conn, s, T, ROUND_QF):
                    Ws, Ls = _winners_and_losers(conn, qf)
                    for wid in Ls:
                        if wid not in higher:
                            labels_by_wrestler[wid].add(_label_for_world(T, "qf"))

    # Convert sets to sorted lists
    return {wid: sorted(list(labels)) for wid, labels in labels_by_wrestler.items()}
# === /dry-run compute =======================================================


# === Highlights dry‑run: extend to all families except US =====================
# Requires helpers from Patch 018a: _participants_by_side, _winners_and_losers,
# _final_match, _round_matches, and constants ROUND_QF/ROUND_SF/ROUND_F plus
# T_WORLD_MEN/T_WORLD_WOMEN/T_TAG_WORLD.

# Additional tournaments (exact spellings, case-insensitive compares in SQL)
T_NXT_MEN     = "Mens NXT Championship"
T_NXT_WOMEN   = "Womens NXT Championship"
T_UNDERGROUND_MEN   = "Mens Underground Championship"
T_UNDERGROUND_WOMEN = "Womens Underground Championship"
T_HARDCORE_MEN   = "Mens Hardcore Championship"
T_HARDCORE_WOMEN = "Womens Hardcore Championship"

# One-off, no-round, one per season
T_RUMBLE_MEN   = "Mens Royal Rumble"
T_RUMBLE_WOMEN = "Womens Royal Rumble"
T_ELIM_MEN     = "Mens Elimination Chamber"
T_ELIM_WOMEN   = "Womens Elimination Chamber"
T_ANDRE_MEN    = "Mens Andre Battle Royal"
T_CHYNA_WOMEN  = "Womens Chyna Battle Royal"
T_DUSTY_MEN    = "Mens Dusty Rhodes Gauntlet"
T_MAE_WOMEN    = "Womens Mae Young Gauntlet"


def _single_match(conn: sqlite3.Connection, season: int, tournament: str):
    return conn.execute(
        """
        SELECT id, winner_side
        FROM matches
        WHERE season = ? AND lower(tournament) = lower(?)
        ORDER BY id DESC
        LIMIT 1
        """,
        (season, tournament),
    ).fetchone()


def _label_for_nxt(tournament: str, kind: str) -> str:
    if tournament == T_NXT_MEN:
        base = "Mens NXT Championship"
        if kind == "champ":
            return "Mens NXT Champion"
    elif tournament == T_NXT_WOMEN:
        base = "Womens NXT Championship"
        if kind == "champ":
            return "Womens NXT Champion"
    else:
        base = tournament
    if kind == "runner":
        return f"{base} Runner-up"
    if kind == "sf":
        return f"{base} Semi-Finalist"
    return base


def _label_for_final_only(tournament: str, kind: str) -> str:
    # Underground / Hardcore champion + runner-up only
    if tournament == T_UNDERGROUND_MEN:
        return "Mens Underground Champion" if kind == "champ" else "Mens Underground Championship Runner-up"
    if tournament == T_UNDERGROUND_WOMEN:
        return "Womens Underground Champion" if kind == "champ" else "Womens Underground Championship Runner-up"
    if tournament == T_HARDCORE_MEN:
        return "Mens Hardcore Champion" if kind == "champ" else "Mens Hardcore Championship Runner-up"
    if tournament == T_HARDCORE_WOMEN:
        return "Womens Hardcore Champion" if kind == "champ" else "Womens Hardcore Championship Runner-up"
    return tournament


_ONE_OFF_WINNER_LABEL: dict[str, str] = {
    T_RUMBLE_MEN: "Mens Royal Rumble Winner",
    T_RUMBLE_WOMEN: "Womens Royal Rumble Winner",
    T_ELIM_MEN: "Mens Elimination Chamber Winner",
    T_ELIM_WOMEN: "Womens Elimination Chamber Winner",
    T_ANDRE_MEN: "Mens Andre Battle Royal Winner",
    T_CHYNA_WOMEN: "Womens Chyna Battle Royal Winner",
    T_DUSTY_MEN: "Mens Dusty Rhodes Gauntlet Winner",
    T_MAE_WOMEN: "Womens Mae Young Gauntlet Winner",
}


def dry_run_all_except_us(conn: sqlite3.Connection, season: int | None = None) -> dict[int, list[str]]:
    """Compute highlights for World (Men/Women), Tag, NXT (M/W), Underground (M/W),
    Hardcore (M/W), and one-off tournaments (Rumble/Chamber/Battle Royals/Gauntlets).
    Does not persist; returns {wrestler_id: [labels...]}."""
    from collections import defaultdict

    # Seasons to process
    if season is None:
        seasons_rows = conn.execute("SELECT DISTINCT season FROM matches ORDER BY season").fetchall()
        seasons = [int(r["season"]) for r in seasons_rows]
    else:
        seasons = [int(season)]

    labels_by_wrestler: dict[int, set[str]] = defaultdict(set)

    for s in seasons:
        # 1) World + Tag (reuse existing helpers/labels)
        partial = dry_run_world_tag(conn, season=s)
        for wid, labels in partial.items():
            labels_by_wrestler[int(wid)].update(labels)

        # 2) NXT (finals + losing SF)
        for T in (T_NXT_MEN, T_NXT_WOMEN):
            fin = _final_match(conn, s, T)
            W, L = _winners_and_losers(conn, fin)
            for wid in W:
                labels_by_wrestler[wid].add(_label_for_nxt(T, "champ"))
            for wid in L:
                labels_by_wrestler[wid].add(_label_for_nxt(T, "runner"))

            finalists = set(W) | set(L)
            for sf in _round_matches(conn, s, T, ROUND_SF):
                Ws, Ls = _winners_and_losers(conn, sf)
                for wid in Ls:
                    if wid not in finalists:
                        labels_by_wrestler[wid].add(_label_for_nxt(T, "sf"))

        # 3) Underground / Hardcore (finals only)
        for T in (
            T_UNDERGROUND_MEN, T_UNDERGROUND_WOMEN,
            T_HARDCORE_MEN, T_HARDCORE_WOMEN,
        ):
            fin = _final_match(conn, s, T)
            W, L = _winners_and_losers(conn, fin)
            for wid in W:
                labels_by_wrestler[wid].add(_label_for_final_only(T, "champ"))
            for wid in L:
                labels_by_wrestler[wid].add(_label_for_final_only(T, "runner"))

        # 4) One-off winners (single match per season)
        for T, label in _ONE_OFF_WINNER_LABEL.items():
            m = _single_match(conn, s, T)
            W, _ = _winners_and_losers(conn, m)
            for wid in W:
                labels_by_wrestler[wid].add(label)

    # Convert sets to sorted lists
    return {wid: sorted(list(labels)) for wid, labels in labels_by_wrestler.items()}
# === /extend dry‑run =======================================================

# === Highlights dry‑run: add US Title and unify ==============================
# Depends on helpers from Patch 018a and 019a.

T_US_MEN   = "Mens US Championship"
T_US_WOMEN = "Womens US Championship"


def _us_winners_for_season(conn: sqlite3.Connection, season: int, tournament: str) -> set[int]:
    """Return wrestler_ids who WON a US title match in the given season.
    We consider any match with tournament name == US Title and empty/NULL round.
    """
    rows = conn.execute(
        """
        SELECT id, winner_side
        FROM matches
        WHERE season = ? AND lower(tournament) = lower(?) AND lower(COALESCE(round,'')) = ''
        ORDER BY id ASC
        """,
        (season, tournament),
    ).fetchall()
    winners: set[int] = set()
    for r in rows:
        W, _ = _winners_and_losers(conn, r)
        winners.update(W)
    return winners


def dry_run_all(conn: sqlite3.Connection, season: int | None = None) -> dict[int, list[str]]:
    """Compute ALL highlights including US Title winners.
    Returns {wrestler_id: [labels...]}."""
    # Start with everything except US
    base = dry_run_all_except_us(conn, season=season)
    labels_by_wrestler: dict[int, set[str]] = {int(w): set(v) for w, v in base.items()}

    # Seasons to process
    if season is None:
        seasons_rows = conn.execute("SELECT DISTINCT season FROM matches ORDER BY season").fetchall()
        seasons = [int(r["season"]) for r in seasons_rows]
    else:
        seasons = [int(season)]

    # Add US Title winners per season
    for s in seasons:
        for T, label in ((T_US_MEN, "Mens US Championship Winner"), (T_US_WOMEN, "Womens US Championship Winner")):
            wids = _us_winners_for_season(conn, s, T)
            for wid in wids:
                labels_by_wrestler.setdefault(int(wid), set()).add(label)

    # Back to sorted lists
    return {wid: sorted(list(labels)) for wid, labels in labels_by_wrestler.items()}
# === /unified dry‑run =======================================================


def init_db() -> None:
    conn = get_conn()
    try:
        # Wrestlers (singles)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wrestlers (
              id      INTEGER PRIMARY KEY AUTOINCREMENT,
              name    TEXT NOT NULL,
              gender  TEXT CHECK(gender IN ('Male','Female')) NOT NULL,
              active  INTEGER NOT NULL,
              photo   TEXT
            );
            """
        )
        if not _column_exists(conn, "wrestlers", "photo"):
            conn.execute("ALTER TABLE wrestlers ADD COLUMN photo TEXT")

        # Tag teams + membership
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tag_teams (
              id      INTEGER PRIMARY KEY AUTOINCREMENT,
              name    TEXT NOT NULL UNIQUE,
              active  INTEGER NOT NULL,
              status  TEXT DEFAULT 'Active'
            );
            """
        )
        if not _column_exists(conn, "tag_teams", "status"):
            conn.execute("ALTER TABLE tag_teams ADD COLUMN status TEXT DEFAULT 'Active'")
            conn.execute(
                """
                UPDATE tag_teams
                SET status = CASE WHEN COALESCE(active,1)=1 THEN 'Active' ELSE 'Inactive' END
                WHERE status IS NULL
                """
            )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tag_team_members (
              team_id     INTEGER NOT NULL,
              wrestler_id INTEGER NOT NULL,
              PRIMARY KEY(team_id, wrestler_id),
              FOREIGN KEY(team_id) REFERENCES tag_teams(id) ON DELETE CASCADE,
              FOREIGN KEY(wrestler_id) REFERENCES wrestlers(id) ON DELETE RESTRICT
            );
            """
        )

        # Factions + membership
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS factions (
              id      INTEGER PRIMARY KEY AUTOINCREMENT,
              name    TEXT NOT NULL UNIQUE,
              active  INTEGER NOT NULL,
              status  TEXT DEFAULT 'Active'
            );
            """
        )
        if not _column_exists(conn, "factions", "status"):
            conn.execute("ALTER TABLE factions ADD COLUMN status TEXT DEFAULT 'Active'")
            conn.execute(
                """
                UPDATE factions
                SET status = CASE WHEN COALESCE(active,1)=1 THEN 'Active' ELSE 'Inactive' END
                WHERE status IS NULL
                """
            )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS faction_members (
              faction_id  INTEGER NOT NULL,
              wrestler_id INTEGER NOT NULL,
              PRIMARY KEY(faction_id, wrestler_id),
              FOREIGN KEY(faction_id)  REFERENCES factions(id)   ON DELETE CASCADE,
              FOREIGN KEY(wrestler_id) REFERENCES wrestlers(id) ON DELETE RESTRICT
            );
            """
        )

        # Championships core
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS championships (
              id          INTEGER PRIMARY KEY AUTOINCREMENT,
              name        TEXT NOT NULL UNIQUE,
              gender      TEXT CHECK(gender IN ('Male','Female')) NOT NULL,
              stipulation TEXT,
              mode        TEXT CHECK(mode IN ('Seasonal','Ongoing')) NOT NULL DEFAULT 'Seasonal'
            );
            """
        )
        if not _column_exists(conn, "championships", "photo"):
            conn.execute("ALTER TABLE championships ADD COLUMN photo TEXT")

        # Seasonal champions by season
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS championship_seasons (
              championship_id INTEGER NOT NULL,
              season          INTEGER NOT NULL,
              champion_id     INTEGER NOT NULL,
              PRIMARY KEY (championship_id, season),
              FOREIGN KEY (championship_id) REFERENCES championships(id) ON DELETE CASCADE,
              FOREIGN KEY (champion_id)     REFERENCES wrestlers(id)     ON DELETE RESTRICT
            );
            """
        )

        # In app.py, inside init_db(), paste this RIGHT AFTER the CREATE TABLE for championship_seasons
# and BEFORE the block of CREATE INDEX statements.

        # --- Seasonal extras: optional runner-up per season ---
        if not _column_exists(conn, "championship_seasons", "runner_up_id"):
            conn.execute("ALTER TABLE championship_seasons ADD COLUMN runner_up_id INTEGER")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_champ_seasons_ru ON championship_seasons(runner_up_id)"
            )

# Paste this INSIDE init_db(), **right UNDER** the block that adds runner_up_id
# Anchor to find first:  "--- Seasonal extras: optional runner-up per season ---"
# Then paste this block directly **after** that runner_up_id/index code, before the Ongoing/reigns table.

        _rebuild_championship_seasons_nullable_team(conn)

        # --- Seasonal extras: allow TEAM champions per season ---
        if not _column_exists(conn, "championship_seasons", "champion_team_id"):
            conn.execute("ALTER TABLE championship_seasons ADD COLUMN champion_team_id INTEGER NULL")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cs_champion_team ON championship_seasons(champion_team_id)"
            )
        if not _column_exists(conn, "championship_seasons", "runner_up_team_id"):
            conn.execute("ALTER TABLE championship_seasons ADD COLUMN runner_up_team_id INTEGER NULL")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cs_runner_team ON championship_seasons(runner_up_team_id)"
            )

        # --- Ongoing extras: allow TEAM champions for reigns as well ---
        if not _column_exists(conn, "championship_reigns", "champion_team_id"):
            conn.execute("ALTER TABLE championship_reigns ADD COLUMN champion_team_id INTEGER NULL")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cr_champion_team ON championship_reigns(champion_team_id)"
            )


        # Ongoing title reigns (open-ended intervals)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS championship_reigns (
              id              INTEGER PRIMARY KEY AUTOINCREMENT,
              championship_id INTEGER NOT NULL,
              champion_id     INTEGER NOT NULL,
              won_on          TEXT NOT NULL,   -- YYYY-MM-DD
              lost_on         TEXT,            -- NULL = current reign
              defences        INTEGER NOT NULL DEFAULT 0,
              FOREIGN KEY (championship_id) REFERENCES championships(id) ON DELETE CASCADE,
              FOREIGN KEY (champion_id)     REFERENCES wrestlers(id)     ON DELETE RESTRICT
            );
            """
        )


        # --- Ongoing extras: season + champion number (no real dates required) ---
        if not _column_exists(conn, "championship_reigns", "season_won"):
            conn.execute("ALTER TABLE championship_reigns ADD COLUMN season_won INTEGER")
        if not _column_exists(conn, "championship_reigns", "champ_number"):
            conn.execute("ALTER TABLE championship_reigns ADD COLUMN champ_number INTEGER")
        # (We keep won_on/lost_on TEXT columns for open/closed state; you won't need real dates.)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reigns_champnum ON championship_reigns(championship_id, champ_number)")


        # Indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wrestlers_name          ON wrestlers(name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wrestlers_active        ON wrestlers(active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tag_teams_name          ON tag_teams(name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tag_teams_active        ON tag_teams(active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_team_members_team       ON tag_team_members(team_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_team_members_wrestler   ON tag_team_members(wrestler_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_factions_name           ON factions(name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_factions_active         ON factions(active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_faction_members_faction ON faction_members(faction_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_faction_members_wrestler ON faction_members(wrestler_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_championships_name      ON championships(name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_champ_seasons_chid      ON championship_seasons(championship_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_champ_reigns_current    ON championship_reigns(championship_id, lost_on)")

        

        conn.commit()
    finally:
        conn.close()



# REPLACE your current startup with this one so we also load the order config
@app.on_event("startup")
def on_startup() -> None:
    init_db()
    _refresh_wrestler_cache()
    load_champ_order()



# ---------------- Utilities ----------------

def norm_gender(val: str) -> str:
    v = (val or "").strip().capitalize()
    if v not in {"Male", "Female"}:
        raise ValueError("Gender must be Male or Female.")
    return v


def norm_active(val: str) -> int:
    v = (val or "").strip().lower()
    return 1 if v in {"yes", "y", "1", "true", "on"} else 0


def _next_champ_number(conn: sqlite3.Connection, cid: int) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(champ_number), 0) + 1 AS nxt FROM championship_reigns WHERE championship_id = ?",
        (cid,),
    ).fetchone()
    return int(row[0] or 1)



# ---------------- Common Routes ----------------

@app.get("/favicon.ico", include_in_schema=False)
async def favicon_redirect():
    return RedirectResponse(url="/static/favicon.svg", status_code=307)

# Paste this over your existing home() function in app.py.
# Find:
#   @app.get("/", response_class=HTMLResponse, include_in_schema=False)
#   async def home(request: Request):
# and replace the whole function body with this one.

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def home(request: Request):
    import os, json
    conn = get_conn()
    try:
        # Pull all championships
        rows = conn.execute(
            """
            SELECT id, name, gender, stipulation, mode, photo
            FROM championships
            ORDER BY id
            """
        ).fetchall()

        items = []
        for c in rows:
            champ_name: str | None = None
            champ_photo: str | None = None
            champ_photos: list[str] = []  # for team champs (two member photos)
            champ_href: str | None = None

            if c["mode"] == "Seasonal":
                # 1) Try current season
                srow = conn.execute(
                    """
                    SELECT season, champion_id, champion_team_id
                    FROM championship_seasons
                    WHERE championship_id = ? AND season = ?
                    """,
                    (c["id"], CURRENT_SEASON),
                ).fetchone()
                # 2) Fallback to latest season having any champion
                if (not srow) or (srow["champion_id"] is None and srow["champion_team_id"] is None):
                    srow = conn.execute(
                        """
                        SELECT season, champion_id, champion_team_id
                        FROM championship_seasons
                        WHERE championship_id = ?
                        ORDER BY season DESC
                        LIMIT 1
                        """,
                        (c["id"],),
                    ).fetchone()

                if srow:
                    if srow["champion_team_id"]:
                        # Team champion
                        t_id = int(srow["champion_team_id"])
                        t = conn.execute("SELECT name FROM tag_teams WHERE id = ?", (t_id,)).fetchone()
                        if t:
                            champ_name = t[0] if isinstance(t, tuple) else t["name"]
                        # Two member photos max, ordered by wrestler name
                        r2 = conn.execute(
                            """
                            SELECT w.photo
                            FROM tag_team_members ttm
                            JOIN wrestlers w ON w.id = ttm.wrestler_id
                            WHERE ttm.team_id = ?
                            ORDER BY w.name
                            """,
                            (t_id,),
                        ).fetchall()
                        for rr in r2:
                            ph = rr[0] if isinstance(rr, tuple) else rr["photo"]
                            if ph:
                                champ_photos.append(ph)
                        champ_photos = champ_photos[:2]
                        champ_href = f"/teams/edit/{t_id}"  # read-only team page not implemented yet
                    elif srow["champion_id"]:
                        # Singles champion
                        wid = int(srow["champion_id"])  # type: ignore[arg-type]
                        wrow = conn.execute(
                            "SELECT id, name, photo FROM wrestlers WHERE id = ?",
                            (wid,),
                        ).fetchone()
                        if wrow:
                            champ_name = wrow["name"] if not isinstance(wrow, tuple) else wrow[1]
                            champ_photo = wrow["photo"] if not isinstance(wrow, tuple) else wrow[2]
                            champ_href = f"/wrestler/{wid}"

            else:
                # Ongoing: current reign (lost_on IS NULL)
                r = conn.execute(
                    """
                    SELECT w.id AS wid, w.name AS name, w.photo AS photo
                    FROM championship_reigns r
                    JOIN wrestlers w ON w.id = r.champion_id
                    WHERE r.championship_id = ? AND r.lost_on IS NULL
                    ORDER BY r.id DESC LIMIT 1
                    """,
                    (c["id"],),
                ).fetchone()
                if r:
                    champ_name = r["name"]
                    champ_photo = r["photo"]
                    champ_href = f"/wrestler/{r['wid']}"

            items.append(
                {
                    "id": c["id"],
                    "name": c["name"],
                    "gender": c["gender"],
                    "stipulation": c["stipulation"] or "",
                    "mode": c["mode"],
                    "belt_photo": c["photo"],
                    "champ_photo": champ_photo,
                    "champ_photos": champ_photos,
                    "champion": champ_name or "Vacant",
                    "champ_href": champ_href,
                }
            )

        # Apply featured + order from config/championship_order.json (case-insensitive matches)
        featured = []
        champs = []
        try:
            cfg_path = os.path.join("config", "championship_order.json")
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {"featured": [], "order": []}

        # Map by lowercase name for quick lookup
        pool = {itm["name"].lower(): itm for itm in items}
        used = set()

        # Featured first
        for nm in (cfg.get("featured") or []):
            key = str(nm).lower().strip()
            if key in pool:
                featured.append(pool[key])
                used.add(key)

        # Ordered grid next
        for nm in (cfg.get("order") or []):
            key = str(nm).lower().strip()
            if key in pool and key not in used:
                champs.append(pool[key])
                used.add(key)

        # Any remaining titles not mentioned in config, append alphabetically
        for key in sorted(k for k in pool.keys() if k not in used):
            champs.append(pool[key])

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "active": "home",
                "featured": featured,
                "champs": champs,
                "season": CURRENT_SEASON,
            },
        )
    finally:
        conn.close()



# ---------------- Singles Roster ----------------

@app.get("/roster", response_class=HTMLResponse, include_in_schema=False)
async def roster(request: Request):
    q = (request.query_params.get("q") or "").strip()
    gender = (request.query_params.get("gender") or "All")
    active = (request.query_params.get("active") or "All")

    conditions = []
    params: List[object] = []

    if q:
        conditions.append("name LIKE ? COLLATE NOCASE")
        params.append(f"%{q}%")
    if gender in ("Male", "Female"):
        conditions.append("gender = ?")
        params.append(gender)
    if active in ("Yes", "No"):
        conditions.append("active = ?")
        params.append(1 if active == "Yes" else 0)

    sql = "SELECT id, name, gender, active, photo FROM wrestlers"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY name"

    conn = get_conn()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    wrestlers = [
        {"id": r["id"], "name": r["name"], "gender": r["gender"], "active": bool(r["active"]), "photo": r["photo"]}
        for r in rows
    ]

    return templates.TemplateResponse(
        "roster_list.html",
        {
            "request": request,
            "active": "roster",
            "wrestlers": wrestlers,
            "filters": {"q": q, "gender": gender, "active": active},
        },
    )


@app.get("/roster/add", response_class=HTMLResponse, include_in_schema=False)
async def roster_add_form(request: Request):
    return templates.TemplateResponse(
        "roster_form.html",
        {
            "request": request,
            "active": "roster",
            "heading": "Add Wrestler",
            "action_url": "/roster/add",
            "form": {"name": "", "gender": "Male", "active": "Yes", "photo": None},
            "allow_photo_upload": False,
            "error": "",
        },
    )


@app.post("/roster/add", response_class=HTMLResponse, include_in_schema=False)
async def roster_add_submit(
    request: Request,
    name: str = Form(...),
    gender: str = Form(...),
    active: str = Form(...),
):
    name = name.strip()
    try:
        gender_n = norm_gender(gender)
        active_n = norm_active(active)
    except ValueError as e:
        return templates.TemplateResponse(
            "roster_form.html",
            {
                "request": request,
                "active": "roster",
                "heading": "Add Wrestler",
                "action_url": "/roster/add",
                "form": {"name": name, "gender": gender, "active": active, "photo": None},
                "allow_photo_upload": False,
                "error": str(e),
            },
            status_code=400,
        )
    if not name:
        return templates.TemplateResponse(
            "roster_form.html",
            {
                "request": request,
                "active": "roster",
                "heading": "Add Wrestler",
                "action_url": "/roster/add",
                "form": {"name": name, "gender": gender, "active": active, "photo": None},
                "allow_photo_upload": False,
                "error": "Name is required.",
            },
            status_code=400,
        )

    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO wrestlers(name, gender, active) VALUES (?,?,?)",
            (name, gender_n, active_n),
        )
        conn.commit()
        _refresh_wrestler_cache()
    finally:
        conn.close()

    return RedirectResponse(url="/roster", status_code=303)


@app.get("/roster/edit/{wid}", response_class=HTMLResponse, include_in_schema=False)
async def roster_edit_form(request: Request, wid: int):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, name, gender, active, photo FROM wrestlers WHERE id = ?",
            (wid,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Wrestler not found")

    form = {
        "name": row["name"],
        "gender": row["gender"],
        "active": "Yes" if row["active"] else "No",
        "photo": row["photo"],
    }
    return templates.TemplateResponse(
        "roster_form.html",
        {
            "request": request,
            "active": "roster",
            "heading": "Edit Wrestler",
            "action_url": f"/roster/edit/{wid}",
            "form": form,
            "allow_photo_upload": True,
            "error": "",
        },
    )


@app.post("/roster/edit/{wid}", response_class=HTMLResponse, include_in_schema=False)
async def roster_edit_submit(
    request: Request,
    wid: int,
    name: str = Form(...),
    gender: str = Form(...),
    active: str = Form(...),
    photo: UploadFile | None = File(None),
):
    name = name.strip()
    try:
        gender_n = norm_gender(gender)
        active_n = norm_active(active)
    except ValueError as e:
        return templates.TemplateResponse(
            "roster_form.html",
            {
                "request": request,
                "active": "roster",
                "heading": "Edit Wrestler",
                "action_url": f"/roster/edit/{wid}",
                "form": {"name": name, "gender": gender, "active": active},
                "allow_photo_upload": True,
                "error": str(e),
            },
            status_code=400,
        )
    if not name:
        return templates.TemplateResponse(
            "roster_form.html",
            {
                "request": request,
                "active": "roster",
                "heading": "Edit Wrestler",
                "action_url": f"/roster/edit/{wid}",
                "form": {"name": name, "gender": gender, "active": active},
                "allow_photo_upload": True,
                "error": "Name is required.",
            },
            status_code=400,
        )

    conn = get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM wrestlers WHERE id = ?", (wid,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Wrestler not found")
        conn.execute(
            "UPDATE wrestlers SET name = ?, gender = ?, active = ? WHERE id = ?",
            (name, gender_n, active_n, wid),
        )

        if photo and (photo.filename or "").strip():
            ct = (photo.content_type or "").lower()
            allowed = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}
            if ct not in allowed:
                raise HTTPException(status_code=400, detail="Only JPEG/PNG/WebP images are allowed")
            ext = allowed[ct]
            filename = f"w{wid}{ext}"
            dest = PHOTOS_DIR / filename

            max_bytes = 8 * 1024 * 1024
            written = 0
            with dest.open("wb") as out:
                while True:
                    chunk = await photo.read(1_048_576)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > max_bytes:
                        out.close()
                        dest.unlink(missing_ok=True)
                        raise HTTPException(status_code=413, detail="Image too large (max 8 MB)")
                    out.write(chunk)

            for old_ext in (".jpg", ".jpeg", ".png", ".webp"):
                p = PHOTOS_DIR / f"w{wid}{old_ext}"
                if p.exists() and p.name != filename:
                    try:
                        p.unlink()
                    except Exception:
                        pass

            web_path = f"/static/photos/{filename}"
            conn.execute("UPDATE wrestlers SET photo = ? WHERE id = ?", (web_path, wid))

        conn.commit()
        _refresh_wrestler_cache()
    finally:
        conn.close()

    return RedirectResponse(url=f"/wrestler/{wid}", status_code=303)


@app.post("/roster/delete/{wid}", include_in_schema=False)
async def roster_delete(wid: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM wrestlers WHERE id = ?", (wid,))
        conn.commit()
        _refresh_wrestler_cache()
    finally:
        conn.close()
    return RedirectResponse(url="/roster", status_code=303)

# ---------------- Wrestler Profile ----------------

# Find this route in app.py and REPLACE the whole function.
# Anchor to find: @app.get("/wrestler/{wid}")

@app.get("/wrestler/{wid}", response_class=HTMLResponse)
def wrestler_profile(request: Request, wid: int):
    conn = get_db()
    try:
        w = conn.execute("SELECT * FROM wrestlers WHERE id = ?", (wid,)).fetchone()
        if not w:
            return HTMLResponse("Wrestler not found", status_code=404)

        # Tag teams this wrestler is/was in
        teams = conn.execute(
            """
            SELECT tt.* FROM tag_teams tt
            JOIN tag_team_members ttm ON ttm.team_id = tt.id
            WHERE ttm.wrestler_id = ?
            ORDER BY tt.name
            """,
            (wid,),
        ).fetchall()

        # Factions this wrestler is/was in
        factions = conn.execute(
            """
            SELECT f.* FROM factions f
            JOIN faction_members fm ON fm.faction_id = f.id
            WHERE fm.wrestler_id = ?
            ORDER BY f.name
            """,
            (wid,),
        ).fetchall()

        # NEW: All matches for this wrestler
        matches = _fetch_wrestler_matches(conn, wid)

    finally:
        conn.close()

    return templates.TemplateResponse(
        "wrestler_profile.html",
        {
            "request": request,
            "wrestler": w,
            "teams": teams,
            "factions": factions,
            "matches": matches,
        },
    )

# Paste this NEW route just BELOW the existing wrestler_profile route (@app.get("/wrestler/{wid}")).

@app.get("/team/{tid}", response_class=HTMLResponse)
def team_profile(request: Request, tid: int):
    conn = get_db()
    try:
        t = conn.execute("SELECT * FROM tag_teams WHERE id = ?", (tid,)).fetchone()
        if not t:
            return HTMLResponse("Team not found", status_code=404)

        members = conn.execute(
            """
            SELECT w.* FROM wrestlers w
            JOIN tag_team_members ttm ON ttm.wrestler_id = w.id
            WHERE ttm.team_id = ?
            ORDER BY w.name
            """,
            (tid,),
        ).fetchall()

        matches = _fetch_team_matches(conn, tid)
    finally:
        conn.close()

    return templates.TemplateResponse(
        "team_profile.html",
        {
            "request": request,
            "team": t,
            "members": members,
            "matches": matches,
        },
    )


# ---------------- Tag Teams ----------------

@app.get("/teams", response_class=HTMLResponse, include_in_schema=False)
async def teams_list(request: Request):
    q = (request.query_params.get("q") or "").strip()
    status = (request.query_params.get("status") or "All")

    conditions = []
    params: List[object] = []

    if q:
        conditions.append("t.name LIKE ? COLLATE NOCASE")
        params.append(f"%{q}%")
    if status in ("Active", "Inactive", "Disbanded"):
        conditions.append("t.status = ?")
        params.append(status)

    base_sql = (
        "SELECT t.id, t.name, t.active, t.status, "
        "GROUP_CONCAT(w.name, ', ') AS members "
        "FROM tag_teams t "
        "LEFT JOIN tag_team_members m ON m.team_id = t.id "
        "LEFT JOIN wrestlers w ON w.id = m.wrestler_id "
    )
    if conditions:
        base_sql += "WHERE " + " AND ".join(conditions) + " "
    base_sql += "GROUP BY t.id ORDER BY t.name"

    conn = get_conn()
    try:
        rows = conn.execute(base_sql, params).fetchall()
    finally:
        conn.close()

    teams = [
        {
            "id": r["id"],
            "name": r["name"],
            "status": (r["status"] or ("Active" if r["active"] else "Inactive")),
            "members": r["members"] or "",
        }
        for r in rows
    ]

    return templates.TemplateResponse(
        "teams_list.html",
        {
            "request": request,
            "active": "teams",
            "teams": teams,
            "filters": {"q": q, "status": status},
        },
    )


@app.get("/teams/add", response_class=HTMLResponse, include_in_schema=False)
async def teams_add_form(request: Request):
    conn = get_conn()
    try:
        wrestlers = conn.execute("SELECT id, name FROM wrestlers WHERE gender = 'Male' ORDER BY name").fetchall()
    finally:
        conn.close()

    return templates.TemplateResponse(
        "team_form.html",
        {
            "request": request,
            "active": "teams",
            "heading": "Add Tag Team",
            "action_url": "/teams/add",
            "form": {"name": "", "status": "Active"},
            "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
            "selected_ids": [],
            "error": "",
        },
    )


@app.post("/teams/add", response_class=HTMLResponse, include_in_schema=False)
async def teams_add_submit(
    request: Request,
    name: str = Form(...),
    status: str = Form(...),
    members: List[int] = Form([]),
):
    name = name.strip()
    status = (status or "").strip().title()
    if status not in {"Active", "Inactive", "Disbanded"}:
        err = "Status must be Active, Inactive or Disbanded."
    else:
        err = ""

    member_ids = list(dict.fromkeys(members))
    if not name:
        err = "Team name is required."
    elif len(member_ids) < 2:
        err = "Select at least two members for a tag team."

    conn = get_conn()
    try:
        if not err:
            cur = conn.execute("SELECT id FROM tag_teams WHERE name = ? COLLATE NOCASE", (name,))
            if cur.fetchone():
                err = "A tag team with that name already exists."

        if not err and member_ids:
            placeholders = ",".join(["?"] * len(member_ids))
            cur = conn.execute(
                f"SELECT COUNT(*) FROM wrestlers WHERE id IN ({placeholders}) AND gender = 'Male'",
                member_ids,
            )
            male_count = cur.fetchone()[0]
            if male_count != len(member_ids):
                err = "Only male wrestlers can be selected for tag teams."

        if err:
            wrestlers = conn.execute("SELECT id, name FROM wrestlers WHERE gender = 'Male' ORDER BY name").fetchall()
            return templates.TemplateResponse(
                "team_form.html",
                {
                    "request": request,
                    "active": "teams",
                    "heading": "Add Tag Team",
                    "action_url": "/teams/add",
                    "form": {"name": name, "status": status},
                    "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
                    "selected_ids": member_ids,
                    "error": err,
                },
                status_code=400,
            )

        active_int = 1 if status == "Active" else 0
        cur = conn.execute(
            "INSERT INTO tag_teams(name, active, status)) VALUES (?,?,?)",
            (name, active_int, status),
        )
        team_id = cur.lastrowid
        if member_ids:
            conn.executemany(
                "INSERT INTO tag_team_members(team_id, wrestler_id) VALUES (?, ?)",
                [(team_id, wid) for wid in member_ids],
            )
        conn.commit()
    finally:
        conn.close()

    return RedirectResponse(url="/teams", status_code=303)


@app.get("/teams/edit/{tid}", response_class=HTMLResponse, include_in_schema=False)
async def teams_edit_form(request: Request, tid: int):
    conn = get_conn()
    try:
        team = conn.execute(
            "SELECT id, name, active, status FROM tag_teams WHERE id = ?",
            (tid,),
        ).fetchone()
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")
        wrestlers = conn.execute("SELECT id, name FROM wrestlers WHERE gender = 'Male' ORDER BY name").fetchall()
        selected = conn.execute(
            "SELECT wrestler_id FROM tag_team_members WHERE team_id = ? ORDER BY wrestler_id",
            (tid,),
        ).fetchall()
    finally:
        conn.close()

    selected_ids = [row[0] for row in selected]
    status_val = team["status"] or ("Active" if team["active"] else "Inactive")

    return templates.TemplateResponse(
        "team_form.html",
        {
            "request": request,
            "active": "teams",
            "heading": "Edit Tag Team",
            "action_url": f"/teams/edit/{tid}",
            "form": {"name": team["name"], "status": status_val},
            "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
            "selected_ids": selected_ids,
            "error": "",
        },
    )


@app.post("/teams/edit/{tid}", response_class=HTMLResponse, include_in_schema=False)
async def teams_edit_submit(
    request: Request,
    tid: int,
    name: str = Form(...),
    status: str = Form(...),
    members: List[int] = Form([]),
):
    name = name.strip()
    status = (status or "").strip().title()
    member_ids = list(dict.fromkeys(members))

    err = ""
    if status not in {"Active", "Inactive", "Disbanded"}:
        err = "Status must be Active, Inactive or Disbanded."
    elif not name:
        err = "Team name is required."
    elif len(member_ids) < 2:
        err = "Select at least two members for a tag team."

    conn = get_conn()
    try:
        if not err:
            cur = conn.execute(
                "SELECT id FROM tag_teams WHERE name = ? COLLATE NOCASE AND id <> ?",
                (name, tid),
            )
            if cur.fetchone():
                err = "Another team with that name already exists."

        if not err and member_ids:
            placeholders = ",".join(["?"] * len(member_ids))
            cur = conn.execute(
                f"SELECT COUNT(*) FROM wrestlers WHERE id IN ({placeholders}) AND gender = 'Male'",
                member_ids,
            )
            male_count = cur.fetchone()[0]
            if male_count != len(member_ids):
                err = "Only male wrestlers can be selected for tag teams."

        if err:
            wrestlers = conn.execute("SELECT id, name FROM wrestlers WHERE gender = 'Male' ORDER BY name").fetchall()
            return templates.TemplateResponse(
                "team_form.html",
                {
                    "request": request,
                    "active": "teams",
                    "heading": "Edit Tag Team",
                    "action_url": f"/teams/edit/{tid}",
                    "form": {"name": name, "status": status},
                    "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
                    "selected_ids": member_ids,
                    "error": err,
                },
                status_code=400,
            )

        cur = conn.execute("SELECT 1 FROM tag_teams WHERE id = ?", (tid,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Team not found")

        active_int = 1 if status == "Active" else 0
        conn.execute(
            "UPDATE tag_teams SET name = ?, active = ?, status = ? WHERE id = ?",
            (name, active_int, status, tid),
        )
        conn.execute("DELETE FROM tag_team_members WHERE team_id = ?", (tid,))
        if member_ids:
            conn.executemany(
                "INSERT INTO tag_team_members(team_id, wrestler_id) VALUES (?, ?)",
                [(tid, wid) for wid in member_ids],
            )
        conn.commit()
    finally:
        conn.close()

    return RedirectResponse(url="/teams", status_code=303)


@app.post("/teams/delete/{tid}", include_in_schema=False)
async def teams_delete(tid: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM tag_team_members WHERE team_id = ?", (tid,))
        conn.execute("DELETE FROM tag_teams WHERE id = ?", (tid,))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/teams", status_code=303)

# ---------------- Factions ----------------

@app.get("/factions", response_class=HTMLResponse, include_in_schema=False)
async def factions_list(request: Request):
    q = (request.query_params.get("q") or "").strip()
    status = (request.query_params.get("status") or "All")

    conditions: List[str] = []
    params: List[object] = []
    if q:
        conditions.append("f.name LIKE ? COLLATE NOCASE")
        params.append(f"%{q}%")
    if status in ("Active", "Inactive", "Disbanded"):
        conditions.append("f.status = ?")
        params.append(status)

    sql = (
        "SELECT f.id, f.name, f.status, "
        "GROUP_CONCAT(w.name, ', ') AS members "
        "FROM factions f "
        "LEFT JOIN faction_members fm ON fm.faction_id = f.id "
        "LEFT JOIN wrestlers w       ON w.id = fm.wrestler_id "
    )
    if conditions:
        sql += "WHERE " + " AND ".join(conditions) + " "
    sql += "GROUP BY f.id ORDER BY f.name"

    conn = get_conn()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    factions = [{
        "id": r["id"],
        "name": r["name"],
        "status": r["status"] or "Inactive",
        "members": r["members"] or "",
    } for r in rows]

    return templates.TemplateResponse(
        "factions_list.html",
        {"request": request, "active": "factions", "factions": factions,
         "filters": {"q": q, "status": status}},
    )


@app.get("/factions/add", response_class=HTMLResponse, include_in_schema=False)
async def factions_add_form(request: Request):
    conn = get_conn()
    try:
        wrestlers = conn.execute("SELECT id, name FROM wrestlers ORDER BY name").fetchall()
    finally:
        conn.close()

    return templates.TemplateResponse(
        "faction_form.html",
        {"request": request, "active": "factions",
         "heading": "Add Faction", "action_url": "/factions/add",
         "form": {"name": "", "status": "Active"},
         "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
         "selected_ids": [], "error": ""},
    )


@app.post("/factions/add", response_class=HTMLResponse, include_in_schema=False)
async def factions_add_submit(
    request: Request,
    name: str = Form(...),
    status: str = Form(...),
    members: List[int] = Form([]),
):
    name = name.strip()
    status = (status or "").strip().title()
    member_ids = list(dict.fromkeys(members))

    err = ""
    if status not in {"Active", "Inactive", "Disbanded"}:
        err = "Status must be Active, Inactive or Disbanded."
    elif not name:
        err = "Faction name is required."
    elif len(member_ids) < 2 or len(member_ids) > 10:
        err = "Select between 2 and 10 members."

    conn = get_conn()
    try:
        if not err:
            cur = conn.execute("SELECT id FROM factions WHERE name = ? COLLATE NOCASE", (name,))
            if cur.fetchone():
                err = "A faction with that name already exists."

        if err:
            wrestlers = conn.execute("SELECT id, name FROM wrestlers ORDER BY name").fetchall()
            return templates.TemplateResponse(
                "faction_form.html",
                {"request": request, "active": "factions",
                 "heading": "Add Faction", "action_url": "/factions/add",
                 "form": {"name": name, "status": status},
                 "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
                 "selected_ids": member_ids, "error": err},
                status_code=400,
            )

        active_int = 1 if status == "Active" else 0
        cur = conn.execute(
            "INSERT INTO factions(name, active, status) VALUES (?,?,?)",
            (name, active_int, status),
        )
        fid = cur.lastrowid
        if member_ids:
            conn.executemany(
                "INSERT INTO faction_members(faction_id, wrestler_id) VALUES (?,?)",
                [(fid, wid) for wid in member_ids],
            )
        conn.commit()
    finally:
        conn.close()

    return RedirectResponse(url="/factions", status_code=303)


@app.get("/factions/edit/{fid}", response_class=HTMLResponse, include_in_schema=False)
async def factions_edit_form(request: Request, fid: int):
    conn = get_conn()
    try:
        faction = conn.execute(
            "SELECT id, name, active, status FROM factions WHERE id = ?",
            (fid,),
        ).fetchone()
        if not faction:
            raise HTTPException(status_code=404, detail="Faction not found")
        wrestlers = conn.execute("SELECT id, name FROM wrestlers ORDER BY name").fetchall()
        selected = conn.execute(
            "SELECT wrestler_id FROM faction_members WHERE faction_id = ? ORDER BY wrestler_id",
            (fid,),
        ).fetchall()
    finally:
        conn.close()

    selected_ids = [r[0] for r in selected]
    status_val = faction["status"] or ("Active" if faction["active"] else "Inactive")

    return templates.TemplateResponse(
        "faction_form.html",
        {"request": request, "active": "factions",
         "heading": "Edit Faction", "action_url": f"/factions/edit/{fid}",
         "form": {"name": faction["name"], "status": status_val},
         "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
         "selected_ids": selected_ids, "error": ""},
    )


@app.post("/factions/edit/{fid}", response_class=HTMLResponse, include_in_schema=False)
async def factions_edit_submit(
    request: Request,
    fid: int,
    name: str = Form(...),
    status: str = Form(...),
    members: List[int] = Form([]),
):
    name = name.strip()
    status = (status or "").strip().title()
    member_ids = list(dict.fromkeys(members))

    err = ""
    if status not in {"Active", "Inactive", "Disbanded"}:
        err = "Status must be Active, Inactive or Disbanded."
    elif not name:
        err = "Faction name is required."
    elif len(member_ids) < 2 or len(member_ids) > 10:
        err = "Select between 2 and 10 members."

    conn = get_conn()
    try:
        if not err:
            cur = conn.execute(
                "SELECT id FROM factions WHERE name = ? COLLATE NOCASE AND id <> ?",
                (name, fid),
            )
            if cur.fetchone():
                err = "Another faction with that name already exists."

        if err:
            wrestlers = conn.execute("SELECT id, name FROM wrestlers ORDER BY name").fetchall()
            return templates.TemplateResponse(
                "faction_form.html",
                {"request": request, "active": "factions",
                 "heading": "Edit Faction", "action_url": f"/factions/edit/{fid}",
                 "form": {"name": name, "status": status},
                 "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
                 "selected_ids": member_ids, "error": err},
                status_code=400,
            )

        cur = conn.execute("SELECT 1 FROM factions WHERE id = ?", (fid,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Faction not found")

        active_int = 1 if status == "Active" else 0
        conn.execute(
            "UPDATE factions SET name = ?, active = ?, status = ? WHERE id = ?",
            (name, active_int, status, fid),
        )
        conn.execute("DELETE FROM faction_members WHERE faction_id = ?", (fid,))
        if member_ids:
            conn.executemany(
                "INSERT INTO faction_members(faction_id, wrestler_id) VALUES (?,?)",
                [(fid, wid) for wid in member_ids],
            )
        conn.commit()
    finally:
        conn.close()

    return RedirectResponse(url="/factions", status_code=303)


@app.post("/factions/delete/{fid}", include_in_schema=False)
async def factions_delete(fid: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM faction_members WHERE faction_id = ?", (fid,))
        conn.execute("DELETE FROM factions WHERE id = ?", (fid,))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/factions", status_code=303)

# ---------------- Championships ----------------

# Add this route anywhere below your other routes (e.g., near the factions/teams list routes)

@app.get("/championships", response_class=HTMLResponse, include_in_schema=False)
async def championships_list(request: Request):
    q = (request.query_params.get("q") or "").strip()
    gender = (request.query_params.get("gender") or "All")
    mode = (request.query_params.get("mode") or "All")

    conditions: list[str] = []
    params: list[object] = []
    if q:
        conditions.append("c.name LIKE ? COLLATE NOCASE")
        params.append(f"%{q}%")
    if gender in ("Male", "Female"):
        conditions.append("c.gender = ?")
        params.append(gender)
    if mode in ("Seasonal", "Ongoing"):
        conditions.append("c.mode = ?")
        params.append(mode)

    sql = "SELECT c.id, c.name, c.gender, c.stipulation, c.mode FROM championships c "
    if conditions:
        sql += "WHERE " + " AND ".join(conditions) + " "
    sql += "ORDER BY c.name"

    conn = get_conn()
    try:
        rows = conn.execute(sql, params).fetchall()
        items: list[dict] = []
        for c in rows:
            current = "—"
            if c["mode"] == "Seasonal":
                r = conn.execute(
                    """
                    SELECT w.name FROM championship_seasons s
                    JOIN wrestlers w ON w.id = s.champion_id
                    WHERE s.championship_id = ? AND s.season = ?
                    """,
                    (c["id"], CURRENT_SEASON),
                ).fetchone()
                if r: current = r[0]
            else:
                r = conn.execute(
                    """
                    SELECT w.name FROM championship_reigns r
                    JOIN wrestlers w ON w.id = r.champion_id
                    WHERE r.championship_id = ? AND r.lost_on IS NULL
                    ORDER BY r.id DESC LIMIT 1
                    """,
                    (c["id"],),
                ).fetchone()
                if r: current = r[0]
            items.append({
                "id": c["id"],
                "name": c["name"],
                "gender": c["gender"],
                "stipulation": c["stipulation"] or "",
                "mode": c["mode"],
                "current": current,
            })
    finally:
        conn.close()

    return templates.TemplateResponse(
        "championships_list.html",
        {
            "request": request,
            "active": "champs",
            "items": items,
            "filters": {"q": q, "gender": gender, "mode": mode, "season": CURRENT_SEASON},
        },
    )


# REPLACE the entire championship_detail(...) function with this version.
# Anchor to find:
# @app.get("/championship/{cid}", response_class=HTMLResponse, include_in_schema=False)

@app.get("/championship/{cid}", response_class=HTMLResponse, include_in_schema=False)
async def championship_detail(request: Request, cid: int):
    conn = get_conn()
    try:
        c = conn.execute(
            "SELECT id, name, gender, stipulation, mode, photo FROM championships WHERE id = ?",
            (cid,),
        ).fetchone()
        if not c:
            return HTMLResponse("Not found", status_code=404)

        seasonal_rows = []
        current_reign = None
        reign_rows = []

        if c["mode"] == "Seasonal":
            # Pull rows with BOTH wrestler and team options; coalesce to a printable name
            seasonal_rows = conn.execute(
                """
                SELECT s.season,
                       COALESCE(tt.name, w.name)  AS champion,
                       COALESCE(tt2.name, ru.name) AS runner_up
                FROM championship_seasons s
                LEFT JOIN wrestlers w   ON w.id    = s.champion_id
                LEFT JOIN tag_teams tt  ON tt.id   = s.champion_team_id
                LEFT JOIN wrestlers ru  ON ru.id   = s.runner_up_id
                LEFT JOIN tag_teams tt2 ON tt2.id  = s.runner_up_team_id
                WHERE s.championship_id = ?
                ORDER BY s.season ASC
                """,
                (cid,),
            ).fetchall()

            # Provide picker data for the form (filtered by gender)
            wrestlers = conn.execute(
                "SELECT id, name FROM wrestlers WHERE gender = ? ORDER BY name",
                (c["gender"],),
            ).fetchall()
            teams = conn.execute(
                "SELECT id, name FROM tag_teams ORDER BY name",
            ).fetchall()

        else:
            # Ongoing: keep existing behaviour
            current_reign = conn.execute(
                """
                SELECT r.id, w.name AS champion, r.season_won, r.champ_number, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NULL
                ORDER BY r.id DESC LIMIT 1
                """,
                (cid,),
            ).fetchone()
            reign_rows = conn.execute(
                """
                SELECT w.name AS champion, r.season_won, r.champ_number, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NOT NULL
                ORDER BY r.champ_number DESC, r.id DESC
                """,
                (cid,),
            ).fetchall()
            wrestlers = []
            teams = []

        return templates.TemplateResponse(
            "championship_detail.html",
            {
                "request": request,
                "active": "champs",
                "champ": {
                    "id": c["id"],
                    "name": c["name"],
                    "gender": c["gender"],
                    "stipulation": c["stipulation"] or "",
                    "mode": c["mode"],
                    "photo": c["photo"],
                },
                "season": CURRENT_SEASON,
                "seasonal_rows": seasonal_rows,
                "current_reign": current_reign,
                "reign_rows": reign_rows,
                "wrestlers": wrestlers,
                "teams": teams,
            },
        )
    finally:
        conn.close()



# Add or replace this handler
@app.get("/championships/add", response_class=HTMLResponse, include_in_schema=False)
async def championships_add_form(request: Request):
    return templates.TemplateResponse(
        "championship_form.html",
        {
            "request": request,
            "active": "champs",
            "heading": "Add Championship",
            "action_url": "/championships/add",
            "form": {"name": "", "gender": "Male", "stipulation": "", "mode": "Seasonal", "photo": None},
            "allow_photo_upload": False,   # upload on Edit page for now
            # below are used by the template's management sections; keep empty on Add
            "champ": {"id": 0, "gender": "Male", "mode": "Seasonal"},
            "season": CURRENT_SEASON,
            "seasonal_rows": [],
            "current_reign": None,
            "reign_rows": [],
            "error": "",
        },
    )


@app.post("/championships/add", response_class=HTMLResponse, include_in_schema=False)
async def championships_add_submit(
    request: Request,
    name: str = Form(...),
    gender: str = Form(...),
    stipulation: str = Form(""),
    mode: str = Form(...),
):
    name = name.strip()
    try:
        gender_n = norm_gender(gender)
    except ValueError as e:
        err = str(e)
        return templates.TemplateResponse(
            "championship_form.html",
            {"request": request, "active": "champs", "heading": "Add Championship", "action_url": "/championships/add",
             "form": {"name": name, "gender": gender, "stipulation": stipulation, "mode": mode}, "error": err},
            status_code=400,
        )
    mode_n = (mode or "").strip().title()
    if mode_n not in {"Seasonal", "Ongoing"}:
        err = "Mode must be Seasonal or Ongoing."
        return templates.TemplateResponse(
            "championship_form.html",
            {"request": request, "active": "champs", "heading": "Add Championship", "action_url": "/championships/add",
             "form": {"name": name, "gender": gender, "stipulation": stipulation, "mode": mode}, "error": err},
            status_code=400,
        )
    if not name:
        err = "Name is required."
        return templates.TemplateResponse(
            "championship_form.html",
            {"request": request, "active": "champs", "heading": "Add Championship", "action_url": "/championships/add",
             "form": {"name": name, "gender": gender, "stipulation": stipulation, "mode": mode}, "error": err},
            status_code=400,
        )

    conn = get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM championships WHERE name = ? COLLATE NOCASE", (name,))
        if cur.fetchone():
            err = "A championship with that name already exists."
            return templates.TemplateResponse(
                "championship_form.html",
                {"request": request, "active": "champs", "heading": "Add Championship", "action_url": "/championships/add",
                 "form": {"name": name, "gender": gender_n, "stipulation": stipulation, "mode": mode_n}, "error": err},
                status_code=400,
            )
        conn.execute(
            "INSERT INTO championships(name, gender, stipulation, mode) VALUES (?,?,?,?)",
            (name, gender_n, stipulation.strip(), mode_n),
        )
        conn.commit()
    finally:
        conn.close()

    return RedirectResponse(url="/championships", status_code=303)


# REPLACE the whole /championships/edit/{cid} function with this
# Anchor to find:
# @app.get("/championships/edit/{cid}", response_class=HTMLResponse, include_in_schema=False)

@app.get("/championships/edit/{cid}", response_class=HTMLResponse, include_in_schema=False)
async def championships_edit_form(request: Request, cid: int):
    conn = get_conn()
    try:
        c = conn.execute(
            "SELECT id, name, gender, stipulation, mode, photo FROM championships WHERE id = ?",
            (cid,),
        ).fetchone()
        if not c:
            raise HTTPException(status_code=404, detail="Championship not found")

        seasonal_rows = []
        current_reign = None
        reign_rows = []
        next_champ_no = None

        wrestlers = []
        teams = []

        if c["mode"] == "Seasonal":
            # Team-aware season list (COALESCE team name, wrestler name)
            seasonal_rows = conn.execute(
                """
                SELECT s.season,
                       COALESCE(tt.name, w.name)   AS champion,
                       COALESCE(tt2.name, ru.name) AS runner_up
                FROM championship_seasons s
                LEFT JOIN wrestlers w   ON w.id   = s.champion_id
                LEFT JOIN tag_teams tt  ON tt.id  = s.champion_team_id
                LEFT JOIN wrestlers ru  ON ru.id  = s.runner_up_id
                LEFT JOIN tag_teams tt2 ON tt2.id = s.runner_up_team_id
                WHERE s.championship_id = ?
                ORDER BY s.season DESC
                """,
                (cid,),
            ).fetchall()

            # Pickers
            wrestlers = conn.execute(
                "SELECT id, name FROM wrestlers WHERE gender = ? ORDER BY name",
                (c["gender"],),
            ).fetchall()
            teams = conn.execute(
                "SELECT id, name FROM tag_teams ORDER BY name",
            ).fetchall()

        else:
            # Ongoing
            current_reign = conn.execute(
                """
                SELECT r.id, w.name AS champion, r.season_won, r.champ_number, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NULL
                ORDER BY r.id DESC LIMIT 1
                """,
                (cid,),
            ).fetchone()
            reign_rows = conn.execute(
                """
                SELECT w.name AS champion, r.season_won, r.champ_number, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NOT NULL
                ORDER BY r.champ_number DESC, r.id DESC
                """,
                (cid,),
            ).fetchall()
            # Eligible list reused by the ongoing panel
            cache = getattr(app.state, "_cache_wrestlers_by_gender", {})
            wrestlers = cache.get(c["gender"], [])

            # For the edit header (next champion # helper)
            def _next_champ_number(conn: sqlite3.Connection, championship_id: int) -> int:
                r = conn.execute(
                    "SELECT COALESCE(MAX(champ_number), 0) FROM championship_reigns WHERE championship_id = ?",
                    (championship_id,),
                ).fetchone()
                return int(r[0] or 0) + 1

            next_champ_no = _next_champ_number(conn, cid)

        return templates.TemplateResponse(
            "championship_form.html",
            {
                "request": request,
                "active": "champs",
                "heading": "Edit Championship",
                "action_url": f"/championships/edit/{cid}",
                "form": {
                    "name": c["name"],
                    "gender": c["gender"],
                    "stipulation": c["stipulation"] or "",
                    "mode": c["mode"],
                    "photo": c["photo"],
                },
                "allow_photo_upload": True,
                "champ": {"id": c["id"], "gender": c["gender"], "mode": c["mode"], "stipulation": c["stipulation"] or ""},
                "season": CURRENT_SEASON,
                "seasonal_rows": seasonal_rows,
                "current_reign": current_reign,
                "reign_rows": reign_rows,
                "wrestlers": wrestlers,
                "teams": teams,
                "next_champ_no": next_champ_no,
                "error": "",
            },
        )
    finally:
        conn.close()



# REPLACE your existing championships_edit_submit with this version
@app.post("/championships/edit/{cid}", response_class=HTMLResponse, include_in_schema=False)
async def championships_edit_submit(
    request: Request,
    cid: int,
    name: str = Form(...),
    gender: str = Form(...),
    stipulation: str = Form(""),
    mode: str = Form(...),
    photo: UploadFile | None = File(None),
):
    name = name.strip()
    try:
        gender_n = norm_gender(gender)
    except ValueError as e:
        err = str(e)
        return templates.TemplateResponse(
            "championship_form.html",
            {"request": request, "active": "champs", "heading": "Edit Championship", "action_url": f"/championships/edit/{cid}",
             "form": {"name": name, "gender": gender, "stipulation": stipulation, "mode": mode}, "allow_photo_upload": True, "error": err},
            status_code=400,
        )
    mode_n = (mode or "").strip().title()
    if mode_n not in {"Seasonal", "Ongoing"}:
        err = "Mode must be Seasonal or Ongoing."
        return templates.TemplateResponse(
            "championship_form.html",
            {"request": request, "active": "champs", "heading": "Edit Championship", "action_url": f"/championships/edit/{cid}",
             "form": {"name": name, "gender": gender, "stipulation": stipulation, "mode": mode}, "allow_photo_upload": True, "error": err},
            status_code=400,
        )
    if not name:
        err = "Name is required."
        return templates.TemplateResponse(
            "championship_form.html",
            {"request": request, "active": "champs", "heading": "Edit Championship", "action_url": f"/championships/edit/{cid}",
             "form": {"name": name, "gender": gender, "stipulation": stipulation, "mode": mode}, "allow_photo_upload": True, "error": err},
            status_code=400,
        )

    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT 1 FROM championships WHERE name = ? COLLATE NOCASE AND id <> ?",
            (name, cid),
        )
        if cur.fetchone():
            err = "Another championship with that name already exists."
            return templates.TemplateResponse(
                "championship_form.html",
                {"request": request, "active": "champs", "heading": "Edit Championship", "action_url": f"/championships/edit/{cid}",
                 "form": {"name": name, "gender": gender, "stipulation": stipulation, "mode": mode}, "allow_photo_upload": True, "error": err},
                status_code=400,
            )

        # Update base fields
        conn.execute(
            "UPDATE championships SET name = ?, gender = ?, stipulation = ?, mode = ? WHERE id = ?",
            (name, gender_n, stipulation.strip(), mode_n, cid),
        )

        # Optional photo upload
        if photo and (photo.filename or "").strip():
            ct = (photo.content_type or "").lower()
            allowed = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}
            if ct not in allowed:
                raise HTTPException(status_code=400, detail="Only JPEG/PNG/WebP images are allowed")
            ext = allowed[ct]
            filename = f"ch{cid}{ext}"
            dest = PHOTOS_DIR / filename

            # Write with simple size cap (~8 MB)
            max_bytes = 8 * 1024 * 1024
            written = 0
            with dest.open("wb") as out:
                while True:
                    chunk = await photo.read(1_048_576)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > max_bytes:
                        out.close()
                        dest.unlink(missing_ok=True)
                        raise HTTPException(status_code=413, detail="Image too large (max 8 MB)")
                    out.write(chunk)

            # Remove old files for this championship with other extensions
            for old_ext in (".jpg", ".jpeg", ".png", ".webp"):
                p = PHOTOS_DIR / f"ch{cid}{old_ext}"
                if p.exists() and p.name != filename:
                    try:
                        p.unlink()
                    except Exception:
                        pass

            web_path = f"/static/photos/{filename}"
            conn.execute("UPDATE championships SET photo = ? WHERE id = ?", (web_path, cid))

        conn.commit()
    finally:
        conn.close()

    return RedirectResponse(url=f"/championships/edit/{cid}", status_code=303)



@app.post("/championships/delete/{cid}", include_in_schema=False)
async def championships_delete(cid: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM championships WHERE id = ?", (cid,))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/championships", status_code=303)




# REPLACE the whole function for setting a seasonal champion with this version.
# Anchor to find:
# @app.post("/championship/{cid}/season/set", include_in_schema=False)

@app.post("/championship/{cid}/season/set", include_in_schema=False)
async def championship_set_season(
    cid: int,
    season: int = Form(...),
    champion_type: str = Form("wrestler"),  # "wrestler" | "team"
    champion_wrestler_id: Optional[int] = Form(None),
    champion_team_id: Optional[int] = Form(None),
    runner_up_wrestler_id: str = Form(""),  # optional (string so blank is allowed)
    runner_up_team_id: str = Form(""),      # optional
):
    conn = get_conn()
    try:
        # Load championship & validate seasonal
        c = conn.execute(
            "SELECT gender, mode, COALESCE(stipulation, '') AS stipulation FROM championships WHERE id = ?",
            (cid,),
        ).fetchone()
        if not c:
            raise HTTPException(status_code=404, detail="Championship not found")
        if c["mode"] != "Seasonal":
            raise HTTPException(status_code=400, detail="Not a Seasonal championship")

        # Runner-up only for non-Rumble/Chamber styles
        st = (c["stipulation"] or "").strip().lower()
        allow_runner_up = st not in {"royal rumble", "elimination chamber", "rumble", "elimination"}

        champ_id_val: Optional[int] = None
        champ_team_val: Optional[int] = None
        ru_id_val: Optional[int] = None
        ru_team_val: Optional[int] = None

        if champion_type == "team":
            # Validate TEAM champion
            if not champion_team_id:
                raise HTTPException(status_code=400, detail="Select a team as Champion")
            # Team must exist and (by your business rules) be 2 members of the right gender
            members = conn.execute(
                """
                SELECT w.id, w.gender
                FROM tag_team_members ttm
                JOIN wrestlers w ON w.id = ttm.wrestler_id
                WHERE ttm.team_id = ?
                ORDER BY w.name
                """,
                (champion_team_id,),
            ).fetchall()
            if len(members) != 2:
                raise HTTPException(status_code=400, detail="Team must have exactly two members")
            # Gender check (teams are male in your data model, but enforce anyway)
            valid_gender = all((m[1] == c["gender"]) for m in members)
            if not valid_gender:
                raise HTTPException(status_code=400, detail="Team members must match the championship gender")
            champ_team_val = int(champion_team_id)

            # Optional team runner-up
            if allow_runner_up and runner_up_team_id and runner_up_team_id.isdigit():
                ru_team = int(runner_up_team_id)
                if ru_team == champ_team_val:
                    raise HTTPException(status_code=400, detail="Runner-up must be different from the Champion team")
                # Basic existence check
                ok = conn.execute("SELECT 1 FROM tag_teams WHERE id=?", (ru_team,)).fetchone()
                if not ok:
                    raise HTTPException(status_code=400, detail="Runner-up team not found")
                ru_team_val = ru_team

        else:  # champion_type == "wrestler"
            if not champion_wrestler_id:
                raise HTTPException(status_code=400, detail="Select a wrestler as Champion")
            # Validate wrestler & gender
            w = conn.execute(
                "SELECT id FROM wrestlers WHERE id = ? AND gender = ?",
                (champion_wrestler_id, c["gender"]),
            ).fetchone()
            if not w:
                raise HTTPException(status_code=400, detail="Champion must be a wrestler of the correct gender")
            champ_id_val = int(champion_wrestler_id)

            # Optional wrestler runner-up
            if allow_runner_up and runner_up_wrestler_id and runner_up_wrestler_id.isdigit():
                ru_id = int(runner_up_wrestler_id)
                wr = conn.execute("SELECT id FROM wrestlers WHERE id = ? AND gender = ?", (ru_id, c["gender"])).fetchone()
                if not wr:
                    raise HTTPException(status_code=400, detail="Runner-up must be a wrestler of the correct gender")
                if ru_id == champ_id_val:
                    raise HTTPException(status_code=400, detail="Runner-up must be different from the Champion")
                ru_id_val = ru_id

        # Upsert champion (wrestler or team) + runner-up (matching type) for the season
        conn.execute(
            """
            INSERT INTO championship_seasons(
                championship_id, season,
                champion_id, champion_team_id,
                runner_up_id, runner_up_team_id
            )
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(championship_id, season)
            DO UPDATE SET
                champion_id       = excluded.champion_id,
                champion_team_id  = excluded.champion_team_id,
                runner_up_id      = excluded.runner_up_id,
                runner_up_team_id = excluded.runner_up_team_id
            """,
            (cid, season, champ_id_val, champ_team_val, ru_id_val, ru_team_val),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/championship/{cid}", status_code=303)



# Paste this EXACT block in app.py **right under** the existing
# `@app.post("/championship/{cid}/season/set", include_in_schema=False)` handler.
# If you already have a function with the SAME decorator, REPLACE it with this one.

@app.post("/championship/{cid}/season/delete", include_in_schema=False)
async def championship_delete_season_post(
    cid: int,
    season: int = Form(...),
):
    conn = get_conn()
    try:
        conn.execute(
            "DELETE FROM championship_seasons WHERE championship_id = ? AND season = ?",
            (cid, season),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/championship/{cid}", status_code=303)


# 2) OPTIONAL: add a GET alias for delete (helps if a browser or extension fires a GET)
# Find your existing POST delete:
# @app.post("/championship/{cid}/season/delete", include_in_schema=False)
# ...leave it AS-IS...
# Then add this exact GET handler BELOW it (uses the same SQL):

@app.get("/championship/{cid}/season/delete", include_in_schema=False)
async def championship_delete_season_get(cid: int, season: int):
    conn = get_conn()
    try:
        conn.execute(
            "DELETE FROM championship_seasons WHERE championship_id = ? AND season = ?",
            (cid, season),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/championship/{cid}", status_code=303)




# REPLACE these two handlers

@app.post("/championship/{cid}/reigns/start", include_in_schema=False)
async def championship_start_reign(
    cid: int,
    champion_id: int = Form(...),
    season_won: int = Form(...),
    champ_number: Optional[int] = Form(None),
):
    conn = get_conn()
    try:
        c = conn.execute("SELECT gender, mode FROM championships WHERE id = ?", (cid,)).fetchone()
        if not c:
            raise HTTPException(status_code=404, detail="Championship not found")
        if c["mode"] != "Ongoing":
            raise HTTPException(status_code=400, detail="Not an Ongoing championship")
        w = conn.execute("SELECT id FROM wrestlers WHERE id = ? AND gender = ?", (champion_id, c["gender"]))
        if not w.fetchone():
            raise HTTPException(status_code=400, detail="Champion must be a wrestler of the correct gender")
        # Ensure no open reign
        open_r = conn.execute(
            "SELECT 1 FROM championship_reigns WHERE championship_id = ? AND lost_on IS NULL",
            (cid,),
        ).fetchone()
        if open_r:
            raise HTTPException(status_code=400, detail="There is already an active reign")

        if not champ_number:
            champ_number = _next_champ_number(conn, cid)
        # We keep won_on as a text marker so the column is non-null; use season tag
        won_marker = f"S{season_won}"
        conn.execute(
            """
            INSERT INTO championship_reigns(championship_id, champion_id, won_on, lost_on, defences, season_won, champ_number)
            VALUES (?,?,?,?,0,?,?)
            """,
            (cid, champion_id, won_marker, None, season_won, champ_number),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/championship/{cid}", status_code=303)






@app.post("/championship/{cid}/reigns/increment", include_in_schema=False)
async def championship_increment_defences(cid: int):
    conn = get_conn()
    try:
        cur = conn.execute(
            "UPDATE championship_reigns SET defences = defences + 1 WHERE championship_id = ? AND lost_on IS NULL",
            (cid,),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=400, detail="No active reign to increment")
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/championship/{cid}", status_code=303)


@app.post("/championship/{cid}/reigns/end", include_in_schema=False)
async def championship_end_reign(cid: int, lost_season: int = Form(...)):
    # mark current reign closed; we store a season marker instead of a real date
    lost_marker = f"S{lost_season}"
    conn = get_conn()
    try:
        cur = conn.execute(
            "UPDATE championship_reigns SET lost_on = ? WHERE championship_id = ? AND lost_on IS NULL",
            (lost_marker, cid),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=400, detail="No active reign to end")
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/championship/{cid}", status_code=303)




def _refresh_wrestler_cache() -> None:
    conn = get_conn()
    try:
        males = conn.execute(
            "SELECT id, name FROM wrestlers WHERE gender='Male' ORDER BY name"
        ).fetchall()
        females = conn.execute(
            "SELECT id, name FROM wrestlers WHERE gender='Female' ORDER BY name"
        ).fetchall()
    finally:
        conn.close()
    app.state._cache_wrestlers_by_gender = {
        "Male": [{"id": r["id"], "name": r["name"]} for r in males],
        "Female": [{"id": r["id"], "name": r["name"]} for r in females],
    }


# === REPLACE your previous matches block in app.py with this entire block ===
# Supports: full participant model (any number of sides), Day ordering, MM:SS time, draw/NC.

from typing import Optional, List, Dict, Tuple
import os
import sqlite3
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
TEMPLATES = Jinja2Templates(directory="templates")


def get_db() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect("data/wut.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# In app.py, REPLACE the entire ensure_matches_schema() definition with this version.
# How to place: Ctrl+F for:   def ensure_matches_schema(conn: sqlite3.Connection)
# Select from that line down to the matching "conn.commit()" and replace with this.

def ensure_matches_schema(conn: sqlite3.Connection) -> None:
    """Create/upgrade schema for v2 matches + participants (no legacy comp1_*/comp2_*).
    Safe to call often.
    """
    # Core matches table (no comp1_*/comp2_*; result + stipulation + day_index kept)
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

    # Add columns if upgrading from a very old version
    cols = {row[1] for row in conn.execute("PRAGMA table_info('matches')").fetchall()}
    if 'day_index' not in cols:
        conn.execute("ALTER TABLE matches ADD COLUMN day_index INTEGER")
    if 'result' not in cols:
        conn.execute("ALTER TABLE matches ADD COLUMN result TEXT DEFAULT 'win'")
    if 'stipulation' not in cols:
        conn.execute("ALTER TABLE matches ADD COLUMN stipulation TEXT")

    # Participants table (one row per wrestler per side)
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

    # Helpful indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_season     ON matches(season);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_tournament ON matches(tournament);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_day        ON matches(day_index);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mp_match           ON match_participants(match_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mp_wrestler        ON match_participants(wrestler_id);")

    # NOTE: v2 does NOT create match_wrestlers_view (we read from match_participants directly)
    _ensure_match_timeline_cols(conn)
    conn.commit()


def _ensure_match_timeline_cols(conn) -> None:
    try:
        rows = conn.execute("PRAGMA table_info(matches)").fetchall()
        names = { (r[1] if isinstance(r, tuple) else r["name"]).lower() for r in rows }
        if "day_index" not in names:
            conn.execute("ALTER TABLE matches ADD COLUMN day_index INTEGER")
        if "order_in_day" not in names:
            conn.execute("ALTER TABLE matches ADD COLUMN order_in_day INTEGER")
        conn.commit()
    except Exception:
        # Safe no-op: if table doesn't exist here, caller will create it
        pass


def _fmt_time(seconds: Optional[int]) -> Optional[str]:
    if seconds is None:
        return None
    try:
        s = int(seconds)
    except Exception:
        return None
    m, sec = divmod(s, 60)
    if m > 59:
        # Clamp display; your data model targets MM:SS only
        m = m % 60
    return f"{m}:{sec:02d}"

# Place this helper **immediately under** def _fmt_time(...)

def _parse_mmss(value: str | None) -> Optional[int]:
    """Parse a clock string like "MM:SS" (or "M:SS"). Returns seconds or None.
    Empty/invalid input → None. Capped < 3600 per your universe rules.
    """
    if not value:
        return None
    s = value.strip()
    if not s:
        return None
    parts = s.split(":")
    if len(parts) != 2:
        return None
    m_str, s_str = parts[0].strip(), parts[1].strip()
    if not (m_str.isdigit() and s_str.isdigit()):
        return None
    m = int(m_str)
    sec = int(s_str)
    if sec >= 60 or m < 0:
        return None
    total = m * 60 + sec
    return total if total < 3600 else None



def _parse_mmss(value: str | None) -> Optional[int]:
    """Parse a clock string like "MM:SS" (or "M:SS"). Returns seconds or None.
    Empty/invalid input → None. We cap at < 3600 as your universe never exceeds an hour.
    """
    if not value:
        return None
    s = value.strip()
    if not s:
        return None
    parts = s.split(":")
    if len(parts) != 2:
        return None
    m_str, s_str = parts[0].strip(), parts[1].strip()
    if not (m_str.isdigit() and s_str.isdigit()):
        return None
    m = int(m_str)
    sec = int(s_str)
    if sec >= 60 or m < 0:
        return None
    total = m * 60 + sec
    return total if total < 3600 else None


def _collect_participants(conn: sqlite3.Connection, match_ids: List[int]) -> Dict[int, Dict[int, List[dict]]]:
    """Return {match_id: {side: [{"id": wid, "name": name}, ...]}}
    We include wrestler IDs so we can collapse pairs to a team label when possible.
    """
    if not match_ids:
        return {}
    q = (
        "SELECT mp.match_id, mp.side, mp.wrestler_id, w.name "
        "FROM match_participants mp JOIN wrestlers w ON w.id = mp.wrestler_id "
        "WHERE mp.match_id IN (%s) ORDER BY mp.side ASC, w.name ASC" % (",".join("?" * len(match_ids)))
    )
    out: Dict[int, Dict[int, List[dict]]] = {}
    for row in conn.execute(q, match_ids).fetchall():
        mid = int(row[0]); side = int(row[1]); wid = int(row[2]); name = row[3]
        out.setdefault(mid, {}).setdefault(side, []).append({"id": wid, "name": name})
    return out

def _load_team_pairs(conn: sqlite3.Connection) -> Dict[frozenset, str]:
    """Return {frozenset({w1, w2}): team_name} for teams that have exactly 2 members.
    Order‑independent using frozenset. Uses current tag_team_members.
    """
    pairs: Dict[frozenset, str] = {}
    rows = conn.execute(
        """
        SELECT tt.id, tt.name, ttm.wrestler_id
        FROM tag_teams tt
        JOIN tag_team_members ttm ON ttm.team_id = tt.id
        ORDER BY tt.id
        """
    ).fetchall()
    by_team: Dict[int, List[int]] = {}
    names: Dict[int, str] = {}
    for tid, tname, wid in rows:
        by_team.setdefault(int(tid), []).append(int(wid))
        names[int(tid)] = tname
    for tid, wids in by_team.items():
        if len(wids) == 2:
            key = frozenset(wids)
            pairs[key] = names[tid]
    return pairs

    # Paste these NEW helpers near the other helpers (e.g., directly under _load_team_pairs)

def _get_team_name(conn: sqlite3.Connection, team_id: int) -> Optional[str]:
    r = conn.execute("SELECT name FROM tag_teams WHERE id = ?", (team_id,)).fetchone()
    return r[0] if r else None


def _get_team_member_photos(conn: sqlite3.Connection, team_id: int) -> List[str]:
    """Return up to two photo paths (strings) for the given team members.
    Falls back to empty list if no photos on file.
    """
    rows = conn.execute(
        """
        SELECT w.photo FROM tag_team_members ttm
        JOIN wrestlers w ON w.id = ttm.wrestler_id
        WHERE ttm.team_id = ?
        ORDER BY w.name
        """,
        (team_id,),
    ).fetchall()
    photos = [r[0] for r in rows if r[0]]
    return photos[:2]

# === Highlights watermark: add season column + helper ========================

def _ensure_highlight_runs_season_col(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info('highlight_runs')").fetchall()}
    if "season" not in cols:
        conn.execute("ALTER TABLE highlight_runs ADD COLUMN season INTEGER")
        conn.commit()


def _record_highlight_run(conn: sqlite3.Connection, season: int) -> None:
    """Store the last processed position for a given season.
    Adds a new row to highlight_runs with (season, last_day, last_order, last_match_id)."""
    _ensure_highlight_runs_season_col(conn)
    row = conn.execute(
        """
        SELECT COALESCE(MAX(day_index), -1)  AS d,
               COALESCE(MAX(order_in_day), -1) AS o,
               MAX(id) AS mid
        FROM matches
        WHERE season = ?
        """,
        (season,),
    ).fetchone()
    conn.execute(
        """
        INSERT INTO highlight_runs(season, last_day, last_order, last_match_id)
        VALUES (?, ?, ?, ?)
        """,
        (
            int(season),
            int(row["d"]),
            int(row["o"]),
            int(row["mid"]) if row["mid"] is not None else None,
        ),
    )
# === /watermark =============================================================

# === Team highlights recompute (Tag Team World — champions) ==================

def _label_id(conn: sqlite3.Connection, label: str) -> int:
    row = conn.execute("SELECT id FROM highlight_types WHERE label = ?", (label,)).fetchone()
    if row:
        return int(row["id"])
    code = label.lower().replace(" ", "_")
    conn.execute("INSERT OR IGNORE INTO highlight_types(code, label) VALUES (?, ?)", (code, label))
    row = conn.execute("SELECT id FROM highlight_types WHERE label = ?", (label,)).fetchone()
    return int(row["id"]) if row else -1


# === Team highlights (Tag Team World: SF/Runner-up/Champion) ================
from typing import Set


def _side_to_team_id(conn: sqlite3.Connection, wrestler_ids: Set[int]) -> int | None:
    # Exact-match a side's members to a team by roster (2-person teams expected).
    if not wrestler_ids:
        return None
    placeholders = ",".join(["?"] * len(wrestler_ids))
    size = len(wrestler_ids)
    row = conn.execute(
        f"""
        SELECT tm.team_id
        FROM tag_team_members tm
        WHERE tm.wrestler_id IN ({placeholders})
        GROUP BY tm.team_id
        HAVING COUNT(DISTINCT tm.wrestler_id) = ?
           AND (SELECT COUNT(*) FROM tag_team_members tm2 WHERE tm2.team_id = tm.team_id) = ?
        LIMIT 1
        """,
        (*map(int, wrestler_ids), size, size),
    ).fetchone()
    return int(row["team_id"]) if row else None


def recompute_team_tag_highlights(conn: sqlite3.Connection, season: int) -> int:
    """Populate team_highlights for Tag Team World: Champion, Runner-up, Semi-Finalist.
    Uses matches to infer teams from sides; requires team_members(team_id, wrestler_id).
    """
    ensure_highlights_schema(conn)

    hid_champ = _label_id(conn, "Tag Team World Champion")
    hid_runner = _label_id(conn, "Tag Team World Championship Runner-up")
    hid_sf = _label_id(conn, "Tag Team World Championship Semi-Finalist")

    # Clear our slice for the season (idempotent)
    conn.execute(
        "DELETE FROM team_highlights WHERE season = ? AND highlight_id IN (?, ?, ?)",
        (season, hid_champ, hid_runner, hid_sf),
    )

    inserted = 0

    # Finals → champion + runner-up
    fin = _final_match(conn, season, T_TAG_WORLD)
    finals_team_ids: Set[int] = set()
    if fin is not None:
        sides = _participants_by_side(conn, int(fin["id"]))
        # collect team ids for all finals sides for later suppression of SF
        for members in sides.values():
            tid = _side_to_team_id(conn, set(int(x) for x in members))
            if tid is not None:
                finals_team_ids.add(tid)
        if fin["winner_side"] is not None:
            w_side = int(fin["winner_side"])
            win_tid = _side_to_team_id(conn, set(int(x) for x in sides.get(w_side, set())))
            if win_tid is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO team_highlights(team_id, highlight_id, season) VALUES (?, ?, ?)",
                    (win_tid, hid_champ, season),
                )
                inserted += 1
            for side, members in sides.items():
                if int(side) == w_side:
                    continue
                ru_tid = _side_to_team_id(conn, set(int(x) for x in members))
                if ru_tid is not None:
                    conn.execute(
                        "INSERT OR IGNORE INTO team_highlights(team_id, highlight_id, season) VALUES (?, ?, ?)",
                        (ru_tid, hid_runner, season),
                    )
                    inserted += 1

    # Semi Finals → losing teams that did not reach the Final
    for sf in _round_matches(conn, season, T_TAG_WORLD, ROUND_SF):
        if sf["winner_side"] is None:
            continue
        sides = _participants_by_side(conn, int(sf["id"]))
        w_side = int(sf["winner_side"]) if sf["winner_side"] is not None else None
        for side, members in sides.items():
            if w_side is not None and int(side) == w_side:
                continue  # loser only
            tid = _side_to_team_id(conn, set(int(x) for x in members))
            if tid is not None and tid not in finals_team_ids:
                conn.execute(
                    "INSERT OR IGNORE INTO team_highlights(team_id, highlight_id, season) VALUES (?, ?, ?)",
                    (tid, hid_sf, season),
                )
                inserted += 1

    return inserted
# === /Team highlights ========================================================


# Paste these NEW helpers directly UNDER `_load_team_pairs(...)` in app.py

from typing import Tuple  # at top of file you already import List, Dict, Optional


def _team_pairs_full(conn: sqlite3.Connection) -> Dict[frozenset, Dict[str, int | str]]:
    """Return {frozenset({w1,w2}): {"id": team_id, "name": team_name}} for 2‑person teams.
    Order‑independent key so {A,B} == {B,A}.
    """
    rows = conn.execute(
        """
        SELECT tt.id AS team_id, tt.name AS team_name, ttm.wrestler_id
        FROM tag_teams tt
        JOIN tag_team_members ttm ON ttm.team_id = tt.id
        ORDER BY tt.id
        """
    ).fetchall()
    by_team: Dict[int, Dict[str, object]] = {}
    for r in rows:
        tid = int(r["team_id"])
        tname = r["team_name"]
        by_team.setdefault(tid, {"name": tname, "wids": []})
        by_team[tid]["wids"].append(int(r["wrestler_id"]))  # type: ignore[index]

    out: Dict[frozenset, Dict[str, int | str]] = {}
    for tid, info in by_team.items():
        wids: List[int] = info["wids"]  # type: ignore[assignment]
        if len(wids) == 2:
            out[frozenset(wids)] = {"id": tid, "name": info["name"]}  # type: ignore[index]
    return out


# Replace the entire _render_side_label(...) with this version

def _render_side_label(people: List[Dict[str, object]], team_pairs: Dict[frozenset, Dict[str, int | str]], link: bool = True) -> str:
    """Return HTML label for a side.
    - If exactly 2 people match a known team → link to /team/{id}
    - Else list linked wrestler names to /wrestler/{id}
    - Links use class="plain-link" so they look like normal text (no blue/underline)
    """
    import html as _html

    if len(people) == 2:
        pair_ids = frozenset([int(people[0]["id"]), int(people[1]["id"])])
        info = team_pairs.get(pair_ids)
        if info:
            name = _html.escape(str(info["name"]))
            if link:
                return f'<a class="plain-link" href="/team/{int(info["id"])}">{name}</a>'
            return name

    parts: List[str] = []
    for p in people:
        pid = int(p["id"])  # type: ignore[index]
        pname = _html.escape(str(p["name"]))
        parts.append(f'<a class="plain-link" href="/wrestler/{pid}">{pname}</a>' if link else pname)
    return ", ".join(parts)


# Replace the entire _fetch_wrestler_matches(...) with this version

def _fetch_wrestler_matches(conn: sqlite3.Connection, wid: int) -> List[dict]:
    ensure_matches_schema(conn)
    rows = conn.execute(
        """
        SELECT DISTINCT m.*
        FROM matches m
        JOIN match_participants mp ON mp.match_id = m.id
        WHERE mp.wrestler_id = ?
        ORDER BY m.day_index IS NULL, m.day_index ASC, m.id ASC
        """,
        (wid,),
    ).fetchall()
    match_ids = [int(r["id"]) for r in rows]
    sides_map = _collect_participants(conn, match_ids)
    team_pairs = _team_pairs_full(conn)

    items: List[dict] = []
    for r in rows:
        d = dict(r)
        sides = sides_map.get(r["id"], {})

        # Build MATCH label with smart delimiter (comma when >4 total participants)
        parts_html: List[str] = []
        for side_num in sorted(sides.keys()):
            people = sides[side_num]
            parts_html.append(_render_side_label(people, team_pairs, link=True))
        total_participants = sum(len(v) for v in sides.values())
        delim = ", " if total_participants > 4 else " vs "
        d["match_display_html"] = delim.join(parts_html) if parts_html else "—"




        res = (d.get("result") or "win").lower()
        wn_val = d.get("winner_side")


        if res == "win" and wn_val:
            wn = int(wn_val)
            ppl = sides.get(wn, [])
            win_html = _render_side_label(ppl, team_pairs, link=True) if ppl else f"Side {wn}"
            d["result_display_html"] = win_html
        elif res == "draw":
            d["result_display_html"] = "Draw"
        elif res == "nc":
            d["result_display_html"] = "No contest"
        else:
            d["result_display_html"] = "—"


        d["match_time_display"] = _fmt_time(d.get("match_time_seconds")) or "—"
        items.append(d)

    return items


# Replace the entire _fetch_team_matches(...) with this version

def _fetch_team_matches(conn: sqlite3.Connection, tid: int) -> List[dict]:
    ensure_matches_schema(conn)

    members = conn.execute(
        "SELECT w.id, w.name FROM wrestlers w \n         JOIN tag_team_members ttm ON ttm.wrestler_id = w.id \n         WHERE ttm.team_id = ? ORDER BY w.name",
        (tid,),
    ).fetchall()
    if len(members) != 2:
        return []
    a_id, b_id = int(members[0][0]), int(members[1][0])

    rows = conn.execute(
        """
        SELECT DISTINCT m.*
        FROM matches m
        JOIN match_participants mp1 ON mp1.match_id = m.id AND mp1.wrestler_id = ?
        JOIN match_participants mp2 ON mp2.match_id = m.id AND mp2.wrestler_id = ? AND mp2.side = mp1.side
        ORDER BY m.day_index IS NULL, m.day_index ASC, m.id ASC
        """,
        (a_id, b_id),
    ).fetchall()

    match_ids = [int(r["id"]) for r in rows]
    sides_map = _collect_participants(conn, match_ids)
    team_pairs = _team_pairs_full(conn)

    items: List[dict] = []
    for r in rows:
        d = dict(r)
        sides = sides_map.get(r["id"], {})


        # Build MATCH label with smart delimiter (comma when >4 total participants)
        parts_html: List[str] = []
        for side_num in sorted(sides.keys()):
            people = sides[side_num]
            parts_html.append(_render_side_label(people, team_pairs, link=True))
        total_participants = sum(len(v) for v in sides.values())
        delim = ", " if total_participants > 4 else " vs "
        d["match_display_html"] = delim.join(parts_html) if parts_html else "—"


        res = (d.get("result") or "win").lower()
        wn_val = d.get("winner_side")



        if res == "win" and wn_val:
            wn = int(wn_val)
            ppl = sides.get(wn, [])
            win_html = _render_side_label(ppl, team_pairs, link=True) if ppl else f"Side {wn}"
            d["result_display_html"] = win_html
        elif res == "draw":
            d["result_display_html"] = "Draw"
        elif res == "nc":
            d["result_display_html"] = "No contest"
        else:
            d["result_display_html"] = "—"



        d["match_time_display"] = _fmt_time(d.get("match_time_seconds")) or "—"
        items.append(d)

    return items



def _fallback_sides(row: sqlite3.Row) -> Dict[int, List[str]]:
    sides: Dict[int, List[str]] = {}
    if row["comp1_name"]:
        sides[1] = [row["comp1_name"]]
    if row["comp2_name"]:
        sides[2] = [row["comp2_name"]]
    return sides



@router.get("/matches", response_class=HTMLResponse)
def matches_page(
    request: Request,
    season: Optional[str] = None,
    tournament: Optional[str] = None,
    competitor: Optional[str] = None,
    wid: Optional[int] = None,
):
    conn = get_db()
    try:
        ensure_matches_schema(conn)

        # Season can be blank string from the UI
        season_int: Optional[int] = None
        if season is not None and str(season).strip() != "":
            try:
                season_int = int(season)
            except ValueError:
                season_int = None

        params: List = []
        if wid is not None:
            sql = (
                "SELECT DISTINCT m.* FROM matches m "
                "JOIN match_participants mp ON mp.match_id = m.id "
                "WHERE mp.wrestler_id = ?"
            )
            params.append(wid)
        else:
            sql = "SELECT m.* FROM matches m WHERE 1=1"

        if season_int is not None:
            sql += " AND m.season = ?"; params.append(season_int)
        if tournament:
            tnorm = tournament.strip().lower()
            sql += (
                " AND LOWER(m.tournament) = ?" if wid is not None else " AND LOWER(tournament) = ?"
            )
            params.append(tnorm)
        if competitor:
            sql += (
                " AND EXISTS (SELECT 1 FROM match_participants mp JOIN wrestlers w ON w.id = mp.wrestler_id "
                " WHERE mp.match_id = m.id AND LOWER(w.name) LIKE ?)"
            )
            params.append(f"%{competitor.lower()}%")

        sql += " ORDER BY m.day_index IS NULL, m.day_index ASC, m.id ASC"

        rows = conn.execute(sql, params).fetchall()
        match_ids = [int(r["id"]) for r in rows]
        sides_map = _collect_participants(conn, match_ids)
        team_pairs = _team_pairs_full(conn)

        seasons = [r[0] for r in conn.execute(
            "SELECT DISTINCT season FROM matches ORDER BY season DESC"
        ).fetchall()]
        tournaments = [r[0] for r in conn.execute(
            "SELECT DISTINCT tournament FROM matches ORDER BY tournament ASC"
        ).fetchall()]
    finally:
        conn.close()

    items: List[Dict] = []
    for r in rows:
        d = dict(r)
        sides = sides_map.get(r["id"], {})

        # Build MATCH label with smart delimiter (comma when >4 total participants)
        parts_html: List[str] = []
        for side_num in sorted(sides.keys()):
            people = sides[side_num]
            parts_html.append(_render_side_label(people, team_pairs, link=True))
        total_participants = sum(len(v) for v in sides.values())
        delim = ", " if total_participants > 4 else " vs "
        d["match_display_html"] = delim.join(parts_html) if parts_html else "—"


        # Result display
        res = (d.get("result") or "win").lower()
        wn_val = d.get("winner_side")
        if res == "win" and wn_val:
            wn = int(wn_val)
            ppl = sides.get(wn, [])
            win_html = _render_side_label(ppl, team_pairs, link=True) if ppl else f"Side {wn}"
            d["result_display_html"] = f"Winner: {win_html}"
        elif res == "draw":
            d["result_display_html"] = "Draw"
        elif res == "nc":
            d["result_display_html"] = "No contest"
        else:
            d["result_display_html"] = "—"

        d["match_time_display"] = _fmt_time(d.get("match_time_seconds")) or "—"
        items.append(d)

    return TEMPLATES.TemplateResponse(
        "matches_list.html",
        {
            "request": request,
            "active": "matches",
            "matches": items,
            "seasons": seasons,
            "tournaments": tournaments,
            "selected": {
                "season": season_int,
                "tournament": tournament or "",
                "competitor": competitor or "",
                "wid": wid,
            },
        },
    )



# Paste both handlers **directly below** the existing matches_page(...) in app.py.

@router.get("/matches/edit/{mid}", response_class=HTMLResponse)
def match_edit_form(request: Request, mid: int):
    conn = get_db()
    try:
        ensure_matches_schema(conn)
        row = conn.execute(
            """
            SELECT id, season, tournament, round, winner_side,
                   match_time_seconds, day_index, order_in_day
            FROM matches WHERE id = ?
            """,
            (mid,),
        ).fetchone()
        if not row:
            return RedirectResponse(url="/matches", status_code=302)

        # Build human labels per side from participants (e.g., "A & B")
        sides = conn.execute(
            """
            SELECT mp.side, GROUP_CONCAT(w.name, ' & ') AS names
            FROM match_participants mp
            JOIN wrestlers w ON w.id = mp.wrestler_id
            WHERE mp.match_id = ?
            GROUP BY mp.side
            ORDER BY mp.side
            """,
            (mid,),
        ).fetchall()
        names_by_side: dict[int, str] = {}
        for r in sides:
            side_val = r[0] if isinstance(r, tuple) else r["side"]
            names_val = r[1] if isinstance(r, tuple) else r["names"]
            try:
                names_by_side[int(side_val)] = names_val
            except Exception:
                pass

        side1_label = f"Side 1 ({names_by_side.get(1, '—') or '—'})"
        side2_label = f"Side 2 ({names_by_side.get(2, '—') or '—'})"
        display_time = _fmt_time(row["match_time_seconds"]) or ""

        return TEMPLATES.TemplateResponse(
            "matches_edit.html",
            {
                "request": request,
                "active": "matches",
                "m": dict(row),
                "display_time": display_time,
                "side1_label": side1_label,
                "side2_label": side2_label,
            },
        )
    finally:
        conn.close()


@router.post("/matches/edit/{mid}")
async def match_edit_submit(request: Request, mid: int):
    form = await request.form()

    def _get_str(name: str) -> str:
        val = form.get(name)
        return val.strip() if isinstance(val, str) else ""

    def _get_int(name: str) -> Optional[int]:
        s = _get_str(name)
        if s == "":
            return None
        try:
            return int(s)
        except Exception:
            return None

    season = _get_int("season")
    tournament = _get_str("tournament")
    round_name = _get_str("round")
    winner_raw = _get_str("winner_side")
    time_raw = _get_str("time_mmss")
    day_index = _get_int("day_index")
    order_in_day = _get_int("order_in_day")

    winner_side: Optional[int]
    if winner_raw in ("1", "2"):
        winner_side = int(winner_raw)
    elif winner_raw == "":
        winner_side = None
    else:
        winner_side = None

    match_time_seconds = _parse_mmss(time_raw)

    conn = get_db()
    try:
        ensure_matches_schema(conn)
        conn.execute(
            """
            UPDATE matches
            SET season = COALESCE(?, season),
                tournament = CASE WHEN ? <> '' THEN ? ELSE tournament END,
                round = CASE WHEN ? <> '' THEN ? ELSE round END,
                winner_side = ?,
                match_time_seconds = ?,
                day_index = COALESCE(?, day_index),
                order_in_day = COALESCE(?, order_in_day)
            WHERE id = ?
            """,
            (
                season,
                tournament, tournament,
                round_name, round_name,
                winner_side,
                match_time_seconds,
                day_index,
                order_in_day,
                mid,
            ),
        )
        conn.commit()
        return RedirectResponse(url="/matches", status_code=302)
    finally:
        conn.close()







# Mount router into app
app.include_router(router)



from fastapi.responses import PlainTextResponse

@app.get("/admin/init-highlights", response_class=PlainTextResponse)
def admin_init_highlights():
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        return "ok: highlight tables ensured"
    finally:
        conn.close()





from fastapi.responses import PlainTextResponse

@app.get("/admin/seed-highlights", response_class=PlainTextResponse)
def admin_seed_highlights():
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        seed_highlight_types(conn)
        n = conn.execute("SELECT COUNT(*) AS c FROM highlight_types").fetchone()["c"]
        return f"ok: highlight_types seeded (total={n})"
    finally:
        conn.close()

from fastapi import Request
from fastapi.responses import HTMLResponse


@app.get("/admin/highlight-types", response_class=HTMLResponse)
def admin_highlight_types(request: Request):
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        rows = conn.execute(
            "SELECT id, code, label FROM highlight_types ORDER BY label"
        ).fetchall()
        total = len(rows)
        return templates.TemplateResponse(
            "admin_highlight_types.html",
            {"request": request, "rows": rows, "total": total},
        )
    finally:
        conn.close()

from fastapi import Request
from fastapi.responses import HTMLResponse


from collections import Counter
from fastapi import Request
from fastapi.responses import HTMLResponse


from collections import Counter
from fastapi import Request
from fastapi.responses import HTMLResponse


@app.get("/admin/highlights/dry-run", response_class=HTMLResponse)
def admin_highlights_dry_run(request: Request, season: int | None = None):
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        # Use full family including US Title now
        results = dry_run_all(conn, season=season)

        # Fetch names for display
        wids = sorted(results.keys())
        names = {}
        if wids:
            placeholders = ",".join(["?"] * len(wids))
            rows = conn.execute(
                f"SELECT id, name FROM wrestlers WHERE id IN ({placeholders})",
                wids,
            ).fetchall()
            names = {int(r["id"]): r["name"] for r in rows}

        # Totals per label
        label_counter: Counter[str] = Counter()
        for labels in results.values():
            label_counter.update(labels)
        totals = dict(sorted(label_counter.items()))

        # Seasons list for dropdown
        seasons = [r["season"] for r in conn.execute("SELECT DISTINCT season FROM matches ORDER BY season").fetchall()]

        # Optional persisted info
        persisted = request.query_params.get("persisted")
        added = request.query_params.get("added")
        unchanged = request.query_params.get("unchanged")

        return templates.TemplateResponse(
            "admin_highlights_dry_run.html",
            {
                "request": request,
                "results": results,
                "names": names,
                "season": season,
                "seasons": seasons,
                "totals": totals,
                "persisted": persisted,
                "added": added,
                "unchanged": unchanged,
            },
        )
    finally:
        conn.close()



from fastapi import Request, Form
from fastapi.responses import RedirectResponse


def _label_to_type_id(conn: sqlite3.Connection, label: str) -> int:
    row = conn.execute("SELECT id FROM highlight_types WHERE label = ?", (label,)).fetchone()
    if row:
        return int(row["id"])
    # Auto-register if missing (defensive; should be seeded already)
    code = label.lower().replace(" ", "_")
    conn.execute("INSERT OR IGNORE INTO highlight_types(code, label) VALUES (?, ?)", (code, label))
    return int(conn.execute("SELECT id FROM highlight_types WHERE label = ?", (label,)).fetchone()["id"])


@app.post("/admin/highlights/persist-world-tag")
def admin_highlights_persist_world_tag(season: int = Form(...)):
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        # Compute for exactly one season
        results = dry_run_world_tag(conn, season=season)
        # Clear slice for idempotency
        conn.execute("DELETE FROM wrestler_highlights WHERE season = ?", (season,))
        inserted = 0
        for wid, labels in results.items():
            for label in labels:
                hid = _label_to_type_id(conn, label)
                conn.execute(
                    "INSERT OR IGNORE INTO wrestler_highlights(wrestler_id, highlight_id, season) VALUES (?, ?, ?)",
                    (int(wid), int(hid), int(season)),
                )
                inserted += 1
        conn.commit()
    finally:
        conn.close()
    # Redirect back to dry-run with a success note
    return RedirectResponse(url=f"/admin/highlights/dry-run?season={season}&persisted=1&added={inserted}", status_code=303)

from fastapi import Form
from fastapi.responses import RedirectResponse


from fastapi import Form
from fastapi.responses import RedirectResponse

# In: /admin/highlights/persist-all-except-us
# After computing and before/around commit, call the watermark helper.

@app.post("/admin/highlights/persist-all-except-us")
def admin_highlights_persist_all_except_us(season: int = Form(...)):
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        results = dry_run_all_except_us(conn, season=season)
        # Clear slice for idempotency
        conn.execute("DELETE FROM wrestler_highlights WHERE season = ?", (season,))
        inserted = 0
        for wid, labels in results.items():
            for label in labels:
                row = conn.execute("SELECT id FROM highlight_types WHERE label = ?", (label,)).fetchone()
                if row is None:
                    code = label.lower().replace(" ", "_")
                    conn.execute(
                        "INSERT OR IGNORE INTO highlight_types(code, label) VALUES (?, ?)",
                        (code, label),
                    )
                    row = conn.execute("SELECT id FROM highlight_types WHERE label = ?", (label,)).fetchone()
                hid = int(row["id"]) if row else None
                if hid is not None:
                    conn.execute(
                        "INSERT OR IGNORE INTO wrestler_highlights(wrestler_id, highlight_id, season) VALUES (?, ?, ?)",
                        (int(wid), hid, int(season)),
                    )
                    inserted += 1
        # Record watermark for this season (last day/order/id seen in matches)
        _record_highlight_run(conn, int(season))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(
        url=f"/admin/highlights/dry-run?season={season}&persisted=1&added={inserted}",
        status_code=303,
    )





from fastapi import Request
from fastapi.responses import HTMLResponse


@app.get("/admin/highlights/status", response_class=HTMLResponse)
def admin_highlights_status(request: Request):
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        _ensure_highlight_runs_season_col(conn)
        # Counts by season currently stored
        rows = conn.execute(
            """
            SELECT season, COUNT(*) AS c
            FROM wrestler_highlights
            GROUP BY season
            ORDER BY season
            """
        ).fetchall()
        # Last 5 watermarks
        watermarks = conn.execute(
            """
            SELECT season, last_day, last_order, last_match_id, ran_at
            FROM highlight_runs
            ORDER BY id DESC
            LIMIT 5
            """
        ).fetchall()
        return templates.TemplateResponse(
            "admin_highlights_status.html",
            {
                "request": request,
                "rows": rows,
                "watermarks": watermarks,
            },
        )
    finally:
        conn.close()

from fastapi import Form
from fastapi.responses import RedirectResponse


# Replace the two persist endpoints so they also populate team_highlights

@app.post("/admin/highlights/persist-all")
def admin_highlights_persist_all(season: int = Form(...)):
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        # Wrestlers (all families incl. US)
        results = dry_run_all(conn, season=season)
        conn.execute("DELETE FROM wrestler_highlights WHERE season = ?", (season,))
        inserted = 0
        for wid, labels in results.items():
            for label in labels:
                hid = _label_id(conn, label)
                if hid != -1:
                    conn.execute(
                        "INSERT OR IGNORE INTO wrestler_highlights(wrestler_id, highlight_id, season) VALUES (?, ?, ?)",
                        (int(wid), hid, int(season)),
                    )
                    inserted += 1
        # Teams (Tag Team World — champions)
        t_inserted = recompute_team_tag_highlights(conn, int(season))
        _record_highlight_run(conn, int(season))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(
        url=f"/admin/highlights/dry-run?season={season}&persisted=1&added={inserted+t_inserted}",
        status_code=303,
    )


@app.post("/admin/highlights/persist-incremental")
def admin_highlights_persist_incremental(season: int = Form(...)):
    conn = get_conn()
    try:
        ensure_highlights_schema(conn)
        _ensure_highlight_runs_season_col(conn)
        wm = conn.execute(
            "SELECT last_day, last_order, last_match_id FROM highlight_runs WHERE season = ? ORDER BY id DESC LIMIT 1",
            (season,),
        ).fetchone()
        cur = conn.execute(
            """
            SELECT COALESCE(MAX(day_index), -1) AS d,
                   COALESCE(MAX(order_in_day), -1) AS o,
                   MAX(id) AS mid
            FROM matches WHERE season = ?
            """,
            (season,),
        ).fetchone()
        if wm and int(wm["last_day"]) == int(cur["d"]) and int(wm["last_order"]) == int(cur["o"]) and int(wm["last_match_id"] or -1) == int(cur["mid"] or -1):
            return RedirectResponse(url=f"/admin/highlights/dry-run?season={season}&unchanged=1", status_code=303)

        # Wrestlers (recompute full slice — idempotent)
        results = dry_run_all(conn, season=season)
        conn.execute("DELETE FROM wrestler_highlights WHERE season = ?", (season,))
        inserted = 0
        for wid, labels in results.items():
            for label in labels:
                hid = _label_id(conn, label)
                if hid != -1:
                    conn.execute(
                        "INSERT OR IGNORE INTO wrestler_highlights(wrestler_id, highlight_id, season) VALUES (?, ?, ?)",
                        (int(wid), hid, int(season)),
                    )
                    inserted += 1
        # Teams (Tag Team World — champions)
        t_inserted = recompute_team_tag_highlights(conn, int(season))
        _record_highlight_run(conn, int(season))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/admin/highlights/dry-run?season={season}&persisted=1&added={inserted+t_inserted}", status_code=303)



from fastapi import Form
from fastapi.responses import RedirectResponse





@app.get("/admin", response_class=HTMLResponse)
def admin_home(request: Request):
    return templates.TemplateResponse(
        "admin_index.html",
        {"request": request},
    )


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
