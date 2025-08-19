from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import List

from fastapi import FastAPI, Request, Form, HTTPException
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

app = FastAPI(title="Wrestling Universe Tracker")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ---------------- DB helpers ----------------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Enforce FK constraints in SQLite
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        # Singles roster
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
        # Tag teams
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tag_teams (
              id      INTEGER PRIMARY KEY AUTOINCREMENT,
              name    TEXT NOT NULL UNIQUE,
              active  INTEGER NOT NULL  -- 1=yes, 0=no
            );
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
        # Helpful indexes for speed
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wrestlers_name   ON wrestlers(name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wrestlers_active ON wrestlers(active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tag_teams_name   ON tag_teams(name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tag_teams_active ON tag_teams(active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_team_members_team    ON tag_team_members(team_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_team_members_wrestler ON tag_team_members(wrestler_id)")
        conn.commit()
    finally:
        conn.close()


@app.on_event("startup")
def on_startup() -> None:
    init_db()


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


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "active": "home"},
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

    sql = "SELECT id, name, gender, active FROM wrestlers"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY name"

    conn = get_conn()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    wrestlers = [
        {"id": r["id"], "name": r["name"], "gender": r["gender"], "active": bool(r["active"]) }
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
            "form": {"name": "", "gender": "Male", "active": "Yes"},
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
                "form": {"name": name, "gender": gender, "active": active},
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
                "form": {"name": name, "gender": gender, "active": active},
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
    finally:
        conn.close()

    return RedirectResponse(url="/roster", status_code=303)


@app.get("/roster/edit/{wid}", response_class=HTMLResponse, include_in_schema=False)
async def roster_edit_form(request: Request, wid: int):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, name, gender, active FROM wrestlers WHERE id = ?",
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
    }
    return templates.TemplateResponse(
        "roster_form.html",
        {
            "request": request,
            "active": "roster",
            "heading": "Edit Wrestler",
            "action_url": f"/roster/edit/{wid}",
            "form": form,
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
        conn.commit()
    finally:
        conn.close()

    return RedirectResponse(url="/roster", status_code=303)


@app.post("/roster/delete/{wid}", include_in_schema=False)
async def roster_delete(wid: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM wrestlers WHERE id = ?", (wid,))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/roster", status_code=303)


# ---------------- Tag Teams ----------------

@app.get("/teams", response_class=HTMLResponse, include_in_schema=False)
async def teams_list(request: Request):
    q = (request.query_params.get("q") or "").strip()
    active = (request.query_params.get("active") or "All")

    conditions = []
    params: List[object] = []

    if q:
        conditions.append("t.name LIKE ? COLLATE NOCASE")
        params.append(f"%{q}%")
    if active in ("Yes", "No"):
        conditions.append("t.active = ?")
        params.append(1 if active == "Yes" else 0)

    base_sql = (
        "SELECT t.id, t.name, t.active, "
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
            "active": bool(r["active"]),
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
            "filters": {"q": q, "active": active},
        },
    )


@app.get("/teams/add", response_class=HTMLResponse, include_in_schema=False)
async def teams_add_form(request: Request):
    conn = get_conn()
    try:
        # Only male wrestlers are eligible for tag teams
        wrestlers = conn.execute(
            "SELECT id, name FROM wrestlers WHERE gender = 'Male' ORDER BY name"
        ).fetchall()
    finally:
        conn.close()

    return templates.TemplateResponse(
        "team_form.html",
        {
            "request": request,
            "active": "teams",
            "heading": "Add Tag Team",
            "action_url": "/teams/add",
            "form": {"name": "", "active": "Yes"},
            "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
            "selected_ids": [],
            "error": "",
        },
    )


@app.post("/teams/add", response_class=HTMLResponse, include_in_schema=False)
async def teams_add_submit(
    request: Request,
    name: str = Form(...),
    active: str = Form(...),
    members: List[int] = Form([]),
):
    name = name.strip()
    active_n = norm_active(active)
    member_ids = list(dict.fromkeys(members))

    # Basic checks
    err = ""
    if not name:
        err = "Team name is required."
    elif len(member_ids) < 2:
        err = "Select at least two members for a tag team."

    conn = get_conn()
    try:
        # Unique name (case-insensitive)
        if not err:
            cur = conn.execute("SELECT id FROM tag_teams WHERE name = ? COLLATE NOCASE", (name,))
            if cur.fetchone():
                err = "A tag team with that name already exists."

        # Male-only membership validation
        if not err and member_ids:
            placeholders = ",".join(["?"] * len(member_ids))
            # Count how many of the selected members are Male
            cur = conn.execute(
                f"SELECT COUNT(*) AS c FROM wrestlers WHERE id IN ({placeholders}) AND gender = 'Male'",
                member_ids,
            )
            male_count = cur.fetchone()[0]
            if male_count != len(member_ids):
                err = "Only male wrestlers can be selected for tag teams."
    finally:
        conn.close()

    if err:
        # Re-render form with selections
        conn = get_conn()
        try:
            wrestlers = conn.execute(
                "SELECT id, name FROM wrestlers WHERE gender = 'Male' ORDER BY name"
            ).fetchall()
        finally:
            conn.close()
        return templates.TemplateResponse(
            "team_form.html",
            {
                "request": request,
                "active": "teams",
                "heading": "Add Tag Team",
                "action_url": "/teams/add",
                "form": {"name": name, "active": ("Yes" if active_n else "No")},
                "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
                "selected_ids": member_ids,
                "error": err,
            },
            status_code=400,
        )

    # Insert
    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO tag_teams(name, active) VALUES (?, ?)",
            (name, active_n),
        )
        team_id = cur.lastrowid
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
            "SELECT id, name, active FROM tag_teams WHERE id = ?",
            (tid,),
        ).fetchone()
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")
        wrestlers = conn.execute(
            "SELECT id, name FROM wrestlers WHERE gender = 'Male' ORDER BY name"
        ).fetchall()
        selected = conn.execute(
            "SELECT wrestler_id FROM tag_team_members WHERE team_id = ? ORDER BY wrestler_id",
            (tid,),
        ).fetchall()
    finally:
        conn.close()

    selected_ids = [row[0] for row in selected]

    return templates.TemplateResponse(
        "team_form.html",
        {
            "request": request,
            "active": "teams",
            "heading": "Edit Tag Team",
            "action_url": f"/teams/edit/{tid}",
            "form": {"name": team["name"], "active": ("Yes" if team["active"] else "No")},
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
    active: str = Form(...),
    members: List[int] = Form([]),
):
    name = name.strip()
    active_n = norm_active(active)
    member_ids = list(dict.fromkeys(members))

    # Basic checks
    err = ""
    if not name:
        err = "Team name is required."
    elif len(member_ids) < 2:
        err = "Select at least two members for a tag team."

    conn = get_conn()
    try:
        # Unique name (case-insensitive) for other teams
        if not err:
            cur = conn.execute(
                "SELECT id FROM tag_teams WHERE name = ? COLLATE NOCASE AND id <> ?",
                (name, tid),
            )
            if cur.fetchone():
                err = "Another team with that name already exists."

        # Male-only membership validation
        if not err and member_ids:
            placeholders = ",".join(["?"] * len(member_ids))
            cur = conn.execute(
                f"SELECT COUNT(*) AS c FROM wrestlers WHERE id IN ({placeholders}) AND gender = 'Male'",
                member_ids,
            )
            male_count = cur.fetchone()[0]
            if male_count != len(member_ids):
                err = "Only male wrestlers can be selected for tag teams."

        if err:
            # Re-render form with data
            wrestlers = conn.execute(
                "SELECT id, name FROM wrestlers WHERE gender = 'Male' ORDER BY name"
            ).fetchall()
            return templates.TemplateResponse(
                "team_form.html",
                {
                    "request": request,
                    "active": "teams",
                    "heading": "Edit Tag Team",
                    "action_url": f"/teams/edit/{tid}",
                    "form": {"name": name, "active": ("Yes" if active_n else "No")},
                    "all_wrestlers": [{"id": w["id"], "name": w["name"]} for w in wrestlers],
                    "selected_ids": member_ids,
                    "error": err,
                },
                status_code=400,
            )

        # Update
        cur = conn.execute("SELECT 1 FROM tag_teams WHERE id = ?", (tid,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Team not found")
        conn.execute(
            "UPDATE tag_teams SET name = ?, active = ? WHERE id = ?",
            (name, active_n, tid),
        )
        conn.execute("DELETE FROM tag_team_members WHERE team_id = ?", (tid,))
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


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
