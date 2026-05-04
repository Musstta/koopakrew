from flask import session


def fetch_players(db) -> list[dict]:
    rows = db.execute("SELECT id, name FROM players WHERE active = 1 ORDER BY name").fetchall()
    return [dict(id=r["id"], name=r["name"]) for r in rows]


def fetch_all_players(db) -> list[dict]:
    rows = db.execute("SELECT id, name, active FROM players ORDER BY name").fetchall()
    return [dict(id=r["id"], name=r["name"], active=bool(r["active"])) for r in rows]


def get_default_player(db) -> dict | None:
    player_id = session.get("default_player_id")
    if not player_id:
        return None
    row = db.execute(
        "SELECT id, name FROM players WHERE id = ? AND active = 1",
        (player_id,),
    ).fetchone()
    if not row:
        session.pop("default_player_id", None)
        return None
    return {"id": row["id"], "name": row["name"]}
