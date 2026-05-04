import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from flask import current_app

logger = logging.getLogger(__name__)


def current_local_date() -> date:
    tz_name = current_app.config.get("LOCAL_TIMEZONE", "America/Costa_Rica")
    return datetime.now(ZoneInfo(tz_name)).date()


def get_current_season_row(db):
    today_date = current_local_date()
    today = today_date.isoformat()
    row = db.execute(
        "SELECT * FROM season_meta WHERE start_date <= ? AND end_date > ? ORDER BY start_date DESC LIMIT 1",
        (today, today),
    ).fetchone()
    if row:
        return row
    try:
        return create_season_for_today(db, today_date)
    except RuntimeError as exc:
        logger.warning("Auto-season creation skipped: %s", exc)
        return db.execute(
            "SELECT * FROM season_meta ORDER BY start_date DESC LIMIT 1"
        ).fetchone()


def get_season_row(db, season_id=None):
    if season_id:
        row = db.execute("SELECT * FROM season_meta WHERE id = ?", (season_id,)).fetchone()
        if row:
            return row
    return get_current_season_row(db)


def _quarter_info_for(day: date):
    quarter = ((day.month - 1) // 3) + 1
    quarter_start_month = (quarter - 1) * 3 + 1
    next_q_month = quarter_start_month + 3
    next_q_year = day.year
    if next_q_month > 12:
        next_q_month -= 12
        next_q_year += 1
    return day.year, quarter, date(next_q_year, next_q_month, 1)


def seed_tracks_for_new_season(db, new_season_id: int):
    template_row = db.execute(
        """
        SELECT sm.id AS season_id
        FROM season_meta sm
        JOIN tracks t ON t.season = sm.id
        GROUP BY sm.id
        ORDER BY sm.start_date DESC, sm.id DESC
        LIMIT 1
        """
    ).fetchone()
    if not template_row:
        raise RuntimeError(
            "Cannot auto-create a season because no existing tracks are available to copy from. Seed the database first."
        )
    template_season_id = template_row["season_id"]
    track_defs = db.execute(
        """
        SELECT code, cup_id, en, es, order_in_cup
        FROM tracks
        WHERE season = ?
        ORDER BY cup_id ASC, order_in_cup ASC
        """,
        (template_season_id,),
    ).fetchall()
    if not track_defs:
        raise RuntimeError("Template season has no tracks to clone; aborting auto-season creation.")
    for r in track_defs:
        db.execute(
            """
            INSERT INTO tracks
                (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
            VALUES (?, ?, ?, ?, ?, NULL, 0, NULL, ?)
            """,
            (r["code"], r["cup_id"], r["en"], r["es"], r["order_in_cup"], new_season_id),
        )


def create_season_for_today(db, day: date):
    year, quarter, next_quarter_start = _quarter_info_for(day)
    label_rows = db.execute(
        "SELECT label FROM season_meta WHERE label LIKE 'Season %'"
    ).fetchall()
    existing_nums = []
    for r in label_rows:
        try:
            existing_nums.append(int(r["label"].split()[1]))
        except (IndexError, ValueError):
            pass
    season_number = (max(existing_nums) if existing_nums else 0) + 1
    label = f"Season {season_number} — {year} Q{quarter}"
    cur = db.execute(
        "INSERT INTO season_meta (label, start_date, end_date) VALUES (?, ?, ?)",
        (label, day.isoformat(), next_quarter_start.isoformat()),
    )
    season_id = cur.lastrowid
    seed_tracks_for_new_season(db, season_id)
    db.commit()
    return db.execute("SELECT * FROM season_meta WHERE id = ?", (season_id,)).fetchone()
