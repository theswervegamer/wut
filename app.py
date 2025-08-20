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

# ---------------- DB helpers ----------------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    return conn


def _column_exists(c: sqlite3.Connection, table: str, col: str) -> bool:
    return any(r[1] == col for r in c.execute(f"PRAGMA table_info({table})"))


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



@app.on_event("startup")
def on_startup() -> None:
    init_db()                 # make sure tables exist
    _refresh_wrestler_cache() # build cache for templates


# ---------------- Utilities ----------------

def norm_gender(val: str) -> str:
    v = (val or "").strip().capitalize()
    if v not in {"Male", "Female"}:
        raise ValueError("Gender must be Male or Female.")
    return v


def norm_active(val: str) -> int:
    v = (val or "").strip().lower()
    return 1 if v in {"yes", "y", "1", "true", "on"} else 0


# ---------------- Common Routes ----------------

@app.get("/favicon.ico", include_in_schema=False)
async def favicon_redirect():
    return RedirectResponse(url="/static/favicon.svg", status_code=307)

# REPLACE your existing home() route with this version (adds separate champ_photo and larger tile intent)
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def home(request: Request):
    conn = get_conn()
    try:
        champs = conn.execute(
            "SELECT id, name, gender, stipulation, mode, photo FROM championships ORDER BY name"
        ).fetchall()
        items: list[dict] = []
        for c in champs:
            champ_name = None
            champ_photo = None
            if c["mode"] == "Seasonal":
                r = conn.execute(
                    """
                    SELECT w.name AS name, w.photo AS photo
                    FROM championship_seasons s
                    JOIN wrestlers w ON w.id = s.champion_id
                    WHERE s.championship_id = ? AND s.season = ?
                    """,
                    (c["id"], CURRENT_SEASON),
                ).fetchone()
                if not r:
                    r = conn.execute(
                        """
                        SELECT w.name AS name, w.photo AS photo
                        FROM championship_seasons s
                        JOIN wrestlers w ON w.id = s.champion_id
                        WHERE s.championship_id = ?
                        ORDER BY s.season DESC LIMIT 1
                        """,
                        (c["id"],),
                    ).fetchone()
                if r:
                    champ_name, champ_photo = r["name"], r["photo"]
            else:
                r = conn.execute(
                    """
                    SELECT w.name AS name, w.photo AS photo
                    FROM championship_reigns r
                    JOIN wrestlers w ON w.id = r.champion_id
                    WHERE r.championship_id = ? AND r.lost_on IS NULL
                    ORDER BY r.id DESC LIMIT 1
                    """,
                    (c["id"],),
                ).fetchone()
                if r:
                    champ_name, champ_photo = r["name"], r["photo"]

            items.append({
                "id": c["id"],
                "name": c["name"],
                "gender": c["gender"],
                "stipulation": c["stipulation"] or "",
                "mode": c["mode"],
                "belt_photo": c["photo"],
                "champ_photo": champ_photo,
                "champion": champ_name or "Vacant",
            })
    finally:
        conn.close()

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "active": "home", "champs": items, "season": CURRENT_SEASON},
    )


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

@app.get("/wrestler/{wid}", response_class=HTMLResponse, include_in_schema=False)
async def wrestler_profile(request: Request, wid: int):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, name, gender, active, photo FROM wrestlers WHERE id = ?",
            (wid,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Wrestler not found")
        team_rows = conn.execute(
            """
            SELECT t.id, t.name,
                   COALESCE(t.status, CASE WHEN t.active=1 THEN 'Active' ELSE 'Inactive' END) AS status
            FROM tag_teams t
            JOIN tag_team_members m ON m.team_id = t.id
            WHERE m.wrestler_id = ?
            ORDER BY t.name
            """,
            (wid,),
        ).fetchall()
        faction_rows = conn.execute(
            """
            SELECT f.id, f.name,
                   COALESCE(f.status, CASE WHEN f.active=1 THEN 'Active' ELSE 'Inactive' END) AS status
            FROM factions f
            JOIN faction_members fm ON fm.faction_id = f.id
            WHERE fm.wrestler_id = ?
            ORDER BY f.name
            """,
            (wid,),
        ).fetchall()
    finally:
        conn.close()

    wrestler = {
        "id": row["id"],
        "name": row["name"],
        "gender": row["gender"],
        "active": bool(row["active"]),
        "photo": row["photo"],
    }
    teams = [{"id": r[0], "name": r[1], "status": r[2]} for r in team_rows]
    factions = [{"id": r[0], "name": r[1], "status": r[2]} for r in faction_rows]

    return templates.TemplateResponse(
        "wrestler_profile.html",
        {
            "request": request,
            "active": "roster",
            "wrestler": wrestler,
            "teams": teams,
            "factions": factions,
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


# REPLACE your existing championship_detail() route with this version
# REPLACE your existing championship_detail() with this version (adds photo)
@app.get("/championship/{cid}", response_class=HTMLResponse, include_in_schema=False)
async def championship_detail(request: Request, cid: int):
    conn = get_conn()
    try:
        c = conn.execute(
            "SELECT id, name, gender, stipulation, mode, photo FROM championships WHERE id = ?",
            (cid,),
        ).fetchone()
        if not c:
            raise HTTPException(status_code=404, detail="Championship not found")

        seasonal_rows = []
        reign_rows = []
        current_reign = None

        if c["mode"] == "Seasonal":
            seasonal_rows = conn.execute(
                """
                SELECT s.season, w.name as champion
                FROM championship_seasons s
                JOIN wrestlers w ON w.id = s.champion_id
                WHERE s.championship_id = ?
                ORDER BY s.season ASC
                """,
                (cid,),
            ).fetchall()
        else:
            current_reign = conn.execute(
                """
                SELECT r.id, w.name as champion, r.won_on, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NULL
                ORDER BY r.id DESC LIMIT 1
                """,
                (cid,),
            ).fetchone()
            reign_rows = conn.execute(
                """
                SELECT w.name as champion, r.won_on, r.lost_on, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NOT NULL
                ORDER BY r.id DESC
                """,
                (cid,),
            ).fetchall()
    finally:
        conn.close()

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


# REPLACE your existing championships_edit_form with this version
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
        if c["mode"] == "Seasonal":
            seasonal_rows = conn.execute(
                """
                SELECT s.season, w.name as champion
                FROM championship_seasons s
                JOIN wrestlers w ON w.id = s.champion_id
                WHERE s.championship_id = ?
                ORDER BY s.season DESC
                """,
                (cid,),
            ).fetchall()
        else:
            current_reign = conn.execute(
                """
                SELECT r.id, w.name as champion, r.won_on, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NULL
                ORDER BY r.id DESC LIMIT 1
                """,
                (cid,),
            ).fetchone()
            reign_rows = conn.execute(
                """
                SELECT w.name as champion, r.won_on, r.lost_on, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NOT NULL
                ORDER BY r.id DESC
                """,
                (cid,),
            ).fetchall()
    finally:
        conn.close()

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
            "champ": {"id": c["id"], "gender": c["gender"], "mode": c["mode"]},
            "season": CURRENT_SEASON,
            "seasonal_rows": seasonal_rows,
            "current_reign": current_reign,
            "reign_rows": reign_rows,
            "error": "",
        },
    )



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

# Championship detail + actions

@app.get("/championship/{cid}", response_class=HTMLResponse, include_in_schema=False)
async def championship_detail(request: Request, cid: int):
    conn = get_conn()
    try:
        c = conn.execute(
            "SELECT id, name, gender, stipulation, mode FROM championships WHERE id = ?",
            (cid,),
        ).fetchone()
        if not c:
            raise HTTPException(status_code=404, detail="Championship not found")

        seasonal_rows = []
        reign_rows = []
        current_reign: Optional[sqlite3.Row] = None

        if c["mode"] == "Seasonal":
            seasonal_rows = conn.execute(
                """
                SELECT s.season, w.name as champion
                FROM championship_seasons s
                JOIN wrestlers w ON w.id = s.champion_id
                WHERE s.championship_id = ?
                ORDER BY s.season DESC
                """,
                (cid,),
            ).fetchall()
        else:
            # current reign
            current_reign = conn.execute(
                """
                SELECT r.id, w.name as champion, r.won_on, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NULL
                ORDER BY r.id DESC LIMIT 1
                """,
                (cid,),
            ).fetchone()
            # past reigns
            reign_rows = conn.execute(
                """
                SELECT w.name as champion, r.won_on, r.lost_on, r.defences
                FROM championship_reigns r
                JOIN wrestlers w ON w.id = r.champion_id
                WHERE r.championship_id = ? AND r.lost_on IS NOT NULL
                ORDER BY r.id DESC
                """,
                (cid,),
            ).fetchall()
    finally:
        conn.close()

    return templates.TemplateResponse(
        "championship_detail.html",
        {
            "request": request,
            "active": "champs",
            "champ": {"id": c["id"], "name": c["name"], "gender": c["gender"], "stipulation": c["stipulation"] or "", "mode": c["mode"]},
            "season": CURRENT_SEASON,
            "seasonal_rows": seasonal_rows,
            "current_reign": current_reign,
            "reign_rows": reign_rows,
        },
    )


@app.post("/championship/{cid}/season/set", include_in_schema=False)
async def championship_set_season(cid: int, champion_id: int = Form(...), season: int = Form(...)):
    conn = get_conn()
    try:
        # Validate
        c = conn.execute("SELECT gender, mode FROM championships WHERE id = ?", (cid,)).fetchone()
        if not c:
            raise HTTPException(status_code=404, detail="Championship not found")
        if c["mode"] != "Seasonal":
            raise HTTPException(status_code=400, detail="Not a Seasonal championship")
        w = conn.execute("SELECT id FROM wrestlers WHERE id = ? AND gender = ?", (champion_id, c["gender"]))
        if not w.fetchone():
            raise HTTPException(status_code=400, detail="Champion must be a wrestler of the correct gender")

        conn.execute(
            "REPLACE INTO championship_seasons(championship_id, season, champion_id) VALUES (?,?,?)",
            (cid, season, champion_id),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/championship/{cid}", status_code=303)


@app.post("/championship/{cid}/season/delete", include_in_schema=False)
async def championship_delete_season(cid: int, season: int = Form(...)):
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


@app.post("/championship/{cid}/reigns/start", include_in_schema=False)
async def championship_start_reign(
    cid: int,
    champion_id: int = Form(...),
    won_on: str = Form(""),
):
    won_on = (won_on or "").strip() or date.today().isoformat()
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

        conn.execute(
            "INSERT INTO championship_reigns(championship_id, champion_id, won_on, defences) VALUES (?,?,?,0)",
            (cid, champion_id, won_on),
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
async def championship_end_reign(cid: int, lost_on: str = Form("")):
    lost_on = (lost_on or "").strip() or date.today().isoformat()
    conn = get_conn()
    try:
        cur = conn.execute(
            "UPDATE championship_reigns SET lost_on = ? WHERE championship_id = ? AND lost_on IS NULL",
            (lost_on, cid),
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


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
