from koopakrew.queries.filters import FilterBuilder


def state_label(val: int) -> str:
    return {1: "Locked", 0: "Default", -1: "At Risk"}.get(val, "Unknown")


def fetch_cups_for_season(db, season_id: int) -> list[dict]:
    sql = """
    SELECT DISTINCT c.id, c.code, c.en, c.es, c.[order], c.is_dlc
    FROM cups c
    JOIN tracks t ON t.cup_id = c.id
    WHERE t.season = ?
    ORDER BY c.[order] ASC
    """
    rows = db.execute(sql, (season_id,)).fetchall()
    return [
        dict(
            id=r["id"],
            code=r["code"],
            en=r["en"],
            es=r["es"],
            order=r["order"],
            is_dlc=bool(r["is_dlc"]),
        )
        for r in rows
    ]


def fetch_totals_overall(db, season_id: int) -> list[tuple[str, int]]:
    sql = """
    SELECT p.name AS owner, COUNT(*) AS n
    FROM tracks t
    JOIN players p ON p.id = t.owner_id
    WHERE t.season = ?
    GROUP BY p.name
    ORDER BY n DESC, p.name ASC
    """
    rows = db.execute(sql, (season_id,)).fetchall()
    return [(r["owner"], r["n"]) for r in rows]


def fetch_standings(
    db, season_id: int, *, owner_names=None, cup_code=None, state_filters=None
) -> list[dict]:
    where_str, args = (
        FilterBuilder(season_id)
        .with_owners(owner_names, alias="po")
        .with_cup(cup_code)
        .with_states(state_filters)
        .build()
    )
    sql = f"""
    SELECT
        t.id,
        t.code AS track_code,
        t.en AS track_en,
        t.es AS track_es,
        t.state,
        c.en AS cup_en,
        c.es AS cup_es,
        c.code AS cup_code,
        c.[order] AS cup_order,
        c.is_dlc AS is_dlc,
        po.name AS owner,
        pt.name AS threatened_by
    FROM tracks t
    JOIN cups c ON c.id = t.cup_id
    LEFT JOIN players po ON po.id = t.owner_id
    LEFT JOIN players pt ON pt.id = t.threatened_by_id
    WHERE {where_str}
    ORDER BY c.[order] ASC, t.order_in_cup ASC
    """
    rows = db.execute(sql, args).fetchall()
    grouped: dict[str, dict] = {}
    for r in rows:
        key = r["cup_code"]
        if key not in grouped:
            grouped[key] = {
                "cup_code": r["cup_code"],
                "cup_en": r["cup_en"],
                "cup_es": r["cup_es"],
                "cup_order": r["cup_order"],
                "is_dlc": bool(r["is_dlc"]),
                "tracks": [],
            }
        grouped[key]["tracks"].append(dict(r))
    return sorted(grouped.values(), key=lambda item: item["cup_order"])


def fetch_totals_filtered(
    db, season_id: int, *, owner_names=None, cup_code=None, state_filters=None
) -> list[tuple[str, int]]:
    where_str, args = (
        FilterBuilder(season_id)
        .with_owners(owner_names, alias="p")
        .with_cup(cup_code)
        .with_states(state_filters)
        .build()
    )
    sql = f"""
    SELECT p.name AS owner, COUNT(*) AS n
    FROM tracks t
    JOIN players p ON p.id = t.owner_id
    JOIN cups c ON c.id = t.cup_id
    WHERE {where_str} AND t.owner_id IS NOT NULL
    GROUP BY p.name
    ORDER BY n DESC, p.name ASC
    """
    rows = db.execute(sql, args).fetchall()
    return [(r["owner"], r["n"]) for r in rows]


def fetch_track_detail(db, track_id: int):
    sql = """
    SELECT
        t.*,
        c.en AS cup_en,
        c.es AS cup_es,
        po.name AS owner,
        pt.name AS threatened_by
    FROM tracks t
    JOIN cups c ON c.id = t.cup_id
    LEFT JOIN players po ON po.id = t.owner_id
    LEFT JOIN players pt ON pt.id = t.threatened_by_id
    WHERE t.id = ?
    """
    return db.execute(sql, (track_id,)).fetchone()
