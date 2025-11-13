import os
import json
import sqlite3
import csv
import io
from collections import defaultdict
from datetime import datetime, date
from zoneinfo import ZoneInfo
from flask import Flask, g, render_template, request, redirect, url_for, abort, flash, make_response, session

DB_PATH = os.environ.get("KOOPAKREW_DB", "koopakrew.db")
LOCAL_TZ = os.environ.get("KOOPAKREW_TZ", "America/Costa_Rica")

app = Flask(__name__)
app.secret_key = os.environ.get("KOOPAKREW_SECRET", "koopakrew-dev-secret")  # replace in prod
STATIC_IMAGE_EXTS = ("png", "jpg", "jpeg", "webp", "gif", "svg")


def resolve_asset_path(subdir: str, code: str | None) -> str | None:
    if not code or not app.static_folder:
        return None
    slug = code.lower()
    for ext in STATIC_IMAGE_EXTS:
        rel = os.path.join("images", subdir, f"{slug}.{ext}")
        abs_path = os.path.join(app.static_folder, rel)
        if os.path.exists(abs_path):
            return rel.replace("\\", "/")
    return None


def cup_image_path(code: str | None) -> str | None:
    return resolve_asset_path("cups", code)


def track_image_path(code: str | None) -> str | None:
    return resolve_asset_path("tracks", code)


def cup_image_url(cup_dict):
    local = cup_image_path(cup_dict.get("cup_code"))
    if local:
        return url_for("static", filename=local)
    return None


def track_image_url(track_dict):
    local = track_image_path(track_dict.get("track_code"))
    if local:
        return url_for("static", filename=local)
    return None


METRIC_DEFS = [
    {"id": "tracks_owned", "label": "Tracks", "type": "value", "value_key": "tracks_owned", "group": "control", "sort_mode": "value", "help": "Tracks currently controlled."},
    {"id": "locked_tracks", "label": "Locked", "type": "value", "value_key": "locked_tracks", "group": "control", "sort_mode": "value", "help": "Owned tracks that are locked."},
    {"id": "races_played", "label": "Races", "type": "value", "value_key": "races_played", "group": "performance", "sort_mode": "value", "help": "Total races involved in (as owner or winner)."},
    {"id": "wins", "label": "Wins", "type": "value", "value_key": "wins", "group": "performance", "sort_mode": "value", "help": "Total race wins."},
    {"id": "win_rate", "label": "Win %", "type": "percent", "value_key": "win_rate", "group": "performance", "sort_mode": "value", "help": "Wins divided by races."},
    {"id": "wins_as_owner", "label": "Owner wins", "type": "value", "value_key": "wins_as_owner", "group": "performance", "sort_mode": "value", "help": "Wins while already owning the track."},
    {"id": "wins_as_non_owner", "label": "Challenger wins", "type": "value", "value_key": "wins_as_non_owner", "group": "performance", "sort_mode": "value", "help": "Wins taken as challenger."},
    {"id": "challenge_win_rate", "label": "Challenger %", "type": "percent", "value_key": "challenge_win_rate", "group": "performance", "sort_mode": "value", "help": "Challenger wins divided by challenger attempts."},
    {"id": "sweeps", "label": "Cup sweeps", "type": "value", "value_key": "sweeps", "group": "performance", "sort_mode": "value", "help": "Total cup sweeps triggered."},
    {"id": "tracks_taken", "label": "Tracks taken", "type": "value", "value_key": "tracks_taken", "group": "performance", "sort_mode": "value", "help": "Tracks gained from others."},
    {"id": "tracks_lost", "label": "Tracks lost", "type": "value", "value_key": "tracks_lost", "group": "performance", "sort_mode": "value", "help": "Tracks lost to challengers."},
    {"id": "net_tracks", "label": "Net gain", "type": "value", "value_key": "net_tracks", "group": "performance", "sort_mode": "value", "help": "Tracks taken minus lost."},
    {"id": "wins_on_risk", "label": "Risk wins", "type": "value", "value_key": "wins_on_risk", "group": "risk", "sort_mode": "value", "help": "Wins on tracks that began the race at risk."},
    {"id": "steals_from_risk", "label": "Risk steals", "type": "value", "value_key": "steals_from_risk", "group": "risk", "sort_mode": "value", "help": "At-risk wins that stole the track."},
    {"id": "hunter_marks", "label": "Hunter tags", "type": "value", "value_key": "hunter_marks", "group": "risk", "sort_mode": "value", "help": "Times a challenger marked someone at risk."},
    {"id": "wins_with_hunter_mark", "label": "Hunter closes", "type": "value", "value_key": "wins_with_hunter_mark", "group": "risk", "sort_mode": "value", "help": "Wins when already tagged as hunter."},
    {
        "id": "defense_succ_att",
        "label": "Defense saves",
        "type": "pair",
        "num_key": "defense_successes",
        "den_key": "races_as_owner",
        "group": "defense",
        "sort_mode": "ratio",
        "help": "Successful defenses vs. owner races.",
    },
    {"id": "defense_success_rate", "label": "Defense %", "type": "percent", "value_key": "defense_success_rate", "group": "defense", "sort_mode": "value", "help": "Defense success rate on owner races."},
    {
        "id": "risk_defense_pair",
        "label": "At-risk saves",
        "type": "pair",
        "num_key": "defense_at_risk_successes",
        "den_key": "defense_at_risk_attempts",
        "group": "defense",
        "sort_mode": "ratio",
        "help": "Successful saves when the track was already at risk.",
    },
    {
        "id": "defense_at_risk_rate",
        "label": "At-risk %",
        "type": "percent",
        "value_key": "defense_at_risk_rate",
        "group": "defense",
        "sort_mode": "value",
        "help": "Defense success rate on at-risk tracks.",
    },
]


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# --- Season helpers ----------------------------------------------------------

def current_local_date():
    tz = ZoneInfo(LOCAL_TZ)
    return datetime.now(tz).date()


def _quarter_info_for(day: date):
    """Return (year, quarter_number, next_quarter_start_date)."""
    quarter = ((day.month - 1) // 3) + 1
    quarter_start_month = (quarter - 1) * 3 + 1
    next_q_month = quarter_start_month + 3
    next_q_year = day.year
    if next_q_month > 12:
        next_q_month -= 12
        next_q_year += 1
    next_quarter_start = date(next_q_year, next_q_month, 1)
    return day.year, quarter, next_quarter_start


def seed_tracks_for_new_season(db, new_season_id: int):
    """
    Copy track metadata (code/cup/order) from the most recent season that already has tracks,
    resetting ownership/state so the new season starts clean.
    """
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
        raise RuntimeError("Cannot auto-create a season because no existing tracks are available to copy from. Seed the database first.")

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
    today_iso = day.isoformat()
    next_q_iso = next_quarter_start.isoformat()

    count_row = db.execute("SELECT COUNT(*) AS n FROM season_meta").fetchone()
    season_number = (count_row["n"] or 0) + 1
    label = f"Season {season_number} — {year} Q{quarter}"

    cur = db.execute(
        "INSERT INTO season_meta (label, start_date, end_date) VALUES (?, ?, ?)",
        (label, today_iso, next_q_iso),
    )
    season_id = cur.lastrowid
    seed_tracks_for_new_season(db, season_id)
    db.commit()

    return db.execute("SELECT * FROM season_meta WHERE id = ?", (season_id,)).fetchone()


def get_current_season_row(db):
    """Return (and auto-create if necessary) the active season row for today's local date."""
    today_date = current_local_date()
    today = today_date.isoformat()
    cur = db.execute(
        "SELECT * FROM season_meta WHERE start_date <= ? AND end_date > ? ORDER BY start_date DESC LIMIT 1",
        (today, today)
    )
    row = cur.fetchone()
    if row:
        return row

    try:
        return create_season_for_today(db, today_date)
    except RuntimeError as exc:
        # If automatic creation fails (e.g., no template tracks), fall back to the latest season.
        print(f"Auto-season creation skipped: {exc}")
        cur = db.execute("SELECT * FROM season_meta ORDER BY start_date DESC LIMIT 1")
        return cur.fetchone()


def get_season_row(db, season_id=None):
    if season_id:
        cur = db.execute("SELECT * FROM season_meta WHERE id = ?", (season_id,))
        row = cur.fetchone()
        if row:
            return row
    return get_current_season_row(db)


# --- Lookups & queries -------------------------------------------------------

def fetch_players(db):
    rows = db.execute("SELECT id, name FROM players WHERE active = 1 ORDER BY name").fetchall()
    return [dict(id=r["id"], name=r["name"]) for r in rows]


def fetch_all_players(db):
    rows = db.execute("SELECT id, name, active FROM players ORDER BY name").fetchall()
    return [dict(id=r["id"], name=r["name"], active=bool(r["active"])) for r in rows]


def get_default_player(db):
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


def fetch_cups_for_season(db, season_id):
    sql = """
    SELECT DISTINCT c.id, c.code, c.en, c.es, c.[order]
    FROM cups c
    JOIN tracks t ON t.cup_id = c.id
    WHERE t.season = ?
    ORDER BY c.[order] ASC
    """
    rows = db.execute(sql, (season_id,)).fetchall()
    return [dict(id=r["id"], code=r["code"], en=r["en"], es=r["es"], order=r["order"]) for r in rows]


def fetch_totals_overall(db, season_id):
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


def fetch_standings(db, season_id, *, owner_name=None, cup_code=None, state_filter=None):
    # Build WHERE predicates
    where = ["t.season = ?"]
    args = [season_id]

    if owner_name and owner_name.lower() != "all":
        where.append("po.name = ?")
        args.append(owner_name)

    if cup_code and cup_code.lower() != "all":
        where.append("c.code = ?")
        args.append(cup_code)

    if state_filter and state_filter.lower() != "any":
        # map labels to integers
        mapping = {"locked": 1, "default": 0, "at-risk": -1, "atrisk": -1, "at_risk": -1}
        val = mapping.get(state_filter.lower())
        if val is not None:
            where.append("t.state = ?")
            args.append(val)

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
        po.name AS owner,
        pt.name AS threatened_by
    FROM tracks t
    JOIN cups c ON c.id = t.cup_id
    LEFT JOIN players po ON po.id = t.owner_id
    LEFT JOIN players pt ON pt.id = t.threatened_by_id
    WHERE {' AND '.join(where)}
    ORDER BY c.[order] ASC, t.order_in_cup ASC
    """
    rows = db.execute(sql, args).fetchall()
    grouped = {}
    for r in rows:
        key = r["cup_code"]
        if key not in grouped:
            grouped[key] = {
                "cup_code": r["cup_code"],
                "cup_en": r["cup_en"],
                "cup_es": r["cup_es"],
                "cup_order": r["cup_order"],
                "tracks": [],
            }
        grouped[key]["tracks"].append(dict(r))
    cups = sorted(grouped.values(), key=lambda item: item["cup_order"])
    return cups


def fetch_track_detail(db, track_id):
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
    row = db.execute(sql, (track_id,)).fetchone()
    return row


# --- State machine -----------------------------------------------------------

def apply_result(db, season_id, track_id, winner_id):
    # Load pre-state
    row = db.execute("SELECT * FROM tracks WHERE id = ? AND season = ?", (track_id, season_id)).fetchone()
    if not row:
        raise ValueError("Track not found for this season")

    pre_owner_id = row["owner_id"]
    pre_state = row["state"]
    pre_threat_id = row["threatened_by_id"]
    cup_id = row["cup_id"]

    # --- Pre-counts BEFORE the update (for sweep detection) ---
    pre_count_winner = db.execute(
        "SELECT COUNT(*) AS n FROM tracks WHERE season = ? AND cup_id = ? AND owner_id = ?",
        (season_id, cup_id, winner_id)
    ).fetchone()["n"] if winner_id else 0

    pre_count_prev_owner = db.execute(
        "SELECT COUNT(*) AS n FROM tracks WHERE season = ? AND cup_id = ? AND owner_id = ?",
        (season_id, cup_id, pre_owner_id)
    ).fetchone()["n"] if pre_owner_id else 0

    # --- Race transition (pre -> post) ---
    post_owner_id = pre_owner_id
    post_state = pre_state
    post_threat_id = pre_threat_id

    if pre_owner_id is None:
        # Season start: immediate claim
        post_owner_id = winner_id
        post_state = 0
        post_threat_id = None
    else:
        if pre_owner_id == winner_id:
            # Owner won
            if pre_state == -1:
                post_state = 0
                post_threat_id = None
            else:
                post_state = 1
                post_threat_id = None
        else:
            # Challenger won
            if pre_state == 1:
                post_state = 0
                post_threat_id = None
            elif pre_state == 0:
                post_state = -1
                post_threat_id = winner_id  # cosmetic mark
            elif pre_state == -1:
                post_owner_id = winner_id   # free-for-all claim
                post_state = 0
                post_threat_id = None

    occurred_at = datetime.now(ZoneInfo(LOCAL_TZ)).isoformat(timespec="seconds")

    # --- Write normal race event (is_sweep=0) ---
    cur = db.execute(
        """
        INSERT INTO events
          (track_id, winner_id, occurred_at,
           pre_owner_id, pre_state, pre_threatened_by_id,
           post_owner_id, post_state, post_threatened_by_id,
           side_effects_json, is_sweep, sweep_cup_id, sweep_owner_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, NULL, NULL)
        """,
        (track_id, winner_id, occurred_at,
         pre_owner_id, pre_state, pre_threat_id,
         post_owner_id, post_state, post_threat_id)
    )
    event_id = cur.lastrowid

    # --- Apply the track change once ---
    db.execute(
        "UPDATE tracks SET owner_id = ?, state = ?, threatened_by_id = ? WHERE id = ?",
        (post_owner_id, post_state, post_threat_id, track_id)
    )

    # --- Sweep detection: did the post-owner cross from <4 to 4? ---
    post_owner_now = post_owner_id
    if post_owner_now:
        post_count_for_post_owner = db.execute(
            "SELECT COUNT(*) AS n FROM tracks WHERE season = ? AND cup_id = ? AND owner_id = ?",
            (season_id, cup_id, post_owner_now)
        ).fetchone()["n"]

        pre_count_for_post_owner = (
            pre_count_winner if post_owner_now == winner_id else pre_count_prev_owner
        )

        if pre_count_for_post_owner < 4 and post_count_for_post_owner == 4:
            # Lock any of that owner's 4 that aren't locked yet, record side-effects
            affected = db.execute(
                """
                SELECT id, state, threatened_by_id
                FROM tracks
                WHERE season = ? AND cup_id = ? AND owner_id = ?
                """,
                (season_id, cup_id, post_owner_now)
            ).fetchall()

            to_lock_ids = [r["id"] for r in affected if r["state"] != 1]
            if to_lock_ids:
                side_effects = []
                for r in affected:
                    if r["id"] in to_lock_ids:
                        side_effects.append({
                            "track_id": r["id"],
                            "pre_state": r["state"],
                            "pre_threatened_by_id": r["threatened_by_id"],
                            "post_state": 1,
                            "post_threatened_by_id": None
                        })
                qmarks = ",".join("?" for _ in to_lock_ids)
                db.execute(
                    f"UPDATE tracks SET state = 1, threatened_by_id = NULL WHERE id IN ({qmarks})",
                    to_lock_ids
                )
                # Separate sweep event (is_sweep=1) carrying the locks we applied
                db.execute(
                    """
                    INSERT INTO events
                      (track_id, winner_id, occurred_at,
                       pre_owner_id, pre_state, pre_threatened_by_id,
                       post_owner_id, post_state, post_threatened_by_id,
                       side_effects_json, is_sweep, sweep_cup_id, sweep_owner_id)
                    VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, 1, ?, ?)
                    """,
                    (track_id, post_owner_now, occurred_at, json.dumps(side_effects), cup_id, post_owner_now)
                )

    db.commit()
    return event_id


def undo_last_event(db):
    ev = db.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
    if not ev:
        return False

    # Restore side effects first (reverse cup sweep locks, etc.)
    side_json = ev["side_effects_json"]
    if side_json:
        try:
            effects = json.loads(side_json)
            for eff in effects:
                db.execute(
                    """
                    UPDATE tracks
                    SET state = COALESCE(?, 0),
                        threatened_by_id = ?
                    WHERE id = ?
                    """,
                    (
                        eff.get("pre_state", 0),
                        eff.get("pre_threatened_by_id"),
                        eff.get("track_id"),
                    ),
                )
        except Exception as e:
            print(f"Undo side effects failed: {e}")

    # If this was a sweep event, remove it and also undo the race that caused it.
    if ev["is_sweep"]:
        db.execute("DELETE FROM events WHERE id = ?", (ev["id"],))
        db.commit()
        # Recursively undo the previous event (the race that created the sweep)
        return undo_last_event(db)

    # Otherwise, restore main track pre-state for a normal race
    db.execute(
        "UPDATE tracks SET owner_id = ?, state = COALESCE(?, 0), threatened_by_id = ? WHERE id = ?",
        (ev["pre_owner_id"], ev["pre_state"], ev["pre_threatened_by_id"], ev["track_id"]),
    )

    # Delete the event
    db.execute("DELETE FROM events WHERE id = ?", (ev["id"],))
    db.commit()
    return True



# --- Helpers for labels & filtered totals -----------------------------------

def state_label(val: int) -> str:
    return {1: "Locked", 0: "Default", -1: "At Risk"}.get(val, "Unknown")

def fetch_totals_filtered(db, season_id, *, owner_name=None, cup_code=None, state_filter=None):
    where = ["t.season = ?"]
    args = [season_id]

    if owner_name and owner_name.lower() != "all":
        where.append("p.name = ?")
        args.append(owner_name)

    if cup_code and cup_code.lower() != "all":
        where.append("c.code = ?")
        args.append(cup_code)

    if state_filter and state_filter.lower() != "any":
        mapping = {"locked": 1, "default": 0, "at-risk": -1, "atrisk": -1, "at_risk": -1}
        val = mapping.get(state_filter.lower())
        if val is not None:
            where.append("t.state = ?")
            args.append(val)

    sql = f'''
    SELECT p.name AS owner, COUNT(*) AS n
    FROM tracks t
    JOIN players p ON p.id = t.owner_id
    JOIN cups c ON c.id = t.cup_id
    WHERE {' AND '.join(where)} AND t.owner_id IS NOT NULL
    GROUP BY p.name
    ORDER BY n DESC, p.name ASC
    '''
    rows = db.execute(sql, args).fetchall()
    return [(r["owner"], r["n"]) for r in rows]


# --- Export helpers ----------------------------------------------------------

def csv_response(filename: str, header: list[str], rows: list[list]):
    sio = io.StringIO()
    writer = csv.writer(sio)
    writer.writerow(header)
    for r in rows:
        writer.writerow(r)
    resp = make_response(sio.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp

# --- Routes ------------------------------------------------------------------

@app.route("/")
def index():
    db = get_db()
    season_param = request.args.get("season", type=int)
    season_row = get_season_row(db, season_param)
    if not season_row:
        abort(500, "No season configured. Seed the database.")
    season_id = season_row["id"]
    season_label = season_row["label"]

    # Filters
    owner_f = request.args.get("owner", default="all")
    cup_f = request.args.get("cup", default="all")
    state_f = request.args.get("state", default="any")

    standings = fetch_standings(db, season_id,
                                owner_name=owner_f,
                                cup_code=cup_f,
                                state_filter=state_f)
    for cup in standings:
        cup["logo_src"] = cup_image_url(cup)
        for track in cup["tracks"]:
            track["image_src"] = track_image_url(track)

    # Totals (overall, not filtered) and sort for medals
    totals_overall = fetch_totals_overall(db, season_id)  # list of (owner, n)
    totals_filtered = fetch_totals_filtered(db, season_id,
                                        owner_name=owner_f,
                                        cup_code=cup_f,
                                        state_filter=state_f)
    # medals mapping by index
    medals = ["🥇", "🥈", "🥉"]

    players = [p["name"] for p in fetch_players(db)]
    cups = fetch_cups_for_season(db, season_id)

    default_player = get_default_player(db)

    return render_template(
        "index.html",
        standings=standings,
        totals_overall=totals_overall,
        totals_filtered=totals_filtered,
        medals=medals,
        players=players,
        cups=cups,
        selected_filters={"owner": owner_f, "cup": cup_f, "state": state_f},
        season_label=season_label,
        default_player=default_player
    )


@app.route("/update/<int:track_id>", methods=["GET", "POST"])
def update_result(track_id):
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured. Seed the database.")
    season_id = season_row["id"]
    season_label = season_row["label"]

    track = fetch_track_detail(db, track_id)
    if not track or track["season"] != season_id:
        abort(404)

    players = fetch_players(db)
    default_player = get_default_player(db)
    prefill_winner = None
    if request.args.get("quick", default=0, type=int) == 1 and default_player:
        prefill_winner = default_player["name"]
    elif request.args.get("winner"):
        prefill_winner = request.args.get("winner")

    recent_events = db.execute(
        """
        SELECT
            e.occurred_at,
            pw.name AS winner_name,
            preo.name AS pre_owner_name,
            posto.name AS post_owner_name,
            e.pre_state,
            e.post_state
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        LEFT JOIN players pw   ON pw.id   = e.winner_id
        LEFT JOIN players preo ON preo.id = e.pre_owner_id
        LEFT JOIN players posto ON posto.id = e.post_owner_id
        WHERE t.id = ? AND e.is_sweep = 0
        ORDER BY e.occurred_at DESC, e.id DESC
        LIMIT 5
        """,
        (track_id,),
    ).fetchall()

    if request.method == "POST":
        winner_name = request.form.get("winner")
        # lookup winner_id
        row = db.execute("SELECT id FROM players WHERE name = ? AND active = 1", (winner_name,)).fetchone()
        if not row:
            flash("Unknown player", "error")
            return redirect(url_for("update_result", track_id=track_id))

        try:
            apply_result(db, season_id, track_id, row["id"])
            flash("Result saved.", "success")
        except Exception as e:
            flash(f"Error saving result: {e}", "error")
        return redirect(url_for("index"))

    # GET
    track_dict = dict(track)
    track_dict["owner"] = track["owner"]
    track_dict["threatened_by"] = track["threatened_by"]
    return render_template(
        "update.html",
        track=track_dict,
        players=[p["name"] for p in players],
        season_label=season_label,
        prefill_winner=prefill_winner,
        default_player=default_player,
        recent_events=recent_events
    )


@app.route("/undo", methods=["POST"])
def undo():
    db = get_db()
    ok = undo_last_event(db)
    if ok:
        flash("Last change undone.", "success")
    else:
        flash("Nothing to undo.", "info")
    return redirect(url_for("index"))

@app.route("/events")
def events_log():
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured.")
    season_id = season_row["id"]
    season_label = season_row["label"]

    player_filter = request.args.get("player", type=int)
    cup_filter = request.args.get("cup", type=int)
    track_filter = request.args.get("track", type=int)
    event_type_filter = request.args.get("event_type", default="all")
    me_filter = request.args.get("me", default=0, type=int)

    default_player = get_default_player(db)
    if me_filter == 1 and default_player and not player_filter:
        player_filter = default_player["id"]

    player_rows = db.execute("SELECT id, name FROM players ORDER BY name").fetchall()
    players = [dict(id=r["id"], name=r["name"]) for r in player_rows]
    cups = fetch_cups_for_season(db, season_id)
    track_rows = db.execute(
        """
        SELECT t.id, t.en AS track_en, t.es AS track_es,
               c.en AS cup_en, c.es AS cup_es
        FROM tracks t
        JOIN cups c ON c.id = t.cup_id
        WHERE t.season = ?
        ORDER BY c.[order] ASC, t.order_in_cup ASC
        """,
        (season_id,),
    ).fetchall()
    tracks = [
        {
            "id": r["id"],
            "track_en": r["track_en"],
            "track_es": r["track_es"],
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
        }
        for r in track_rows
    ]

    # Re-use the export_events query, but select only what we need
    where_clauses = [
        """
        (
            (t.season = ?)
            OR (
                t.id IS NULL
                AND e.is_sweep = 1
                AND e.sweep_cup_id IN (
                    SELECT DISTINCT ts.cup_id
                    FROM tracks ts
                    WHERE ts.season = ?
                )
            )
        )
        """
    ]
    sql_params = [season_id, season_id]

    if event_type_filter == "race":
        where_clauses.append("e.is_sweep = 0")
    elif event_type_filter == "sweep":
        where_clauses.append("e.is_sweep = 1")

    if player_filter:
        where_clauses.append(
            """
            (
                pw.id = ?
                OR preo.id = ?
                OR posto.id = ?
                OR (e.is_sweep = 1 AND so.id = ?)
            )
            """
        )
        sql_params.extend([player_filter, player_filter, player_filter, player_filter])

    if cup_filter:
        where_clauses.append(
            """
            (
                (e.is_sweep = 0 AND c.id = ?)
                OR
                (e.is_sweep = 1 AND c2.id = ?)
            )
            """
        )
        sql_params.extend([cup_filter, cup_filter])

    if track_filter:
        where_clauses.append("t.id = ?")
        sql_params.append(track_filter)

    sql = f"""
    SELECT
        e.id AS event_id,
        e.occurred_at,
        e.is_sweep,

        c2.code AS sweep_cup_code,
        c2.en   AS sweep_cup_en,
        c2.es   AS sweep_cup_es,
        so.name AS sweep_owner,

        c.code  AS cup_code,
        c.en    AS cup_en,
        c.es    AS cup_es,
        t.code  AS track_code,
        t.en    AS track_en,
        t.es    AS track_es,

        pw.name   AS winner,
        preo.name AS pre_owner,
        e.pre_state,
        pret.name AS pre_mark,
        posto.name AS post_owner,
        e.post_state,
        postt.name AS post_mark
    FROM events e
    LEFT JOIN tracks  t   ON t.id  = e.track_id
    LEFT JOIN cups    c   ON c.id  = t.cup_id
    LEFT JOIN cups    c2  ON c2.id = e.sweep_cup_id
    LEFT JOIN players so  ON so.id = e.sweep_owner_id
    LEFT JOIN players pw  ON pw.id = e.winner_id
    LEFT JOIN players preo ON preo.id = e.pre_owner_id
    LEFT JOIN players pret ON pret.id = e.pre_threatened_by_id
    LEFT JOIN players posto ON posto.id = e.post_owner_id
    LEFT JOIN players postt ON postt.id = e.post_threatened_by_id
    WHERE {" AND ".join(where_clauses)}
    ORDER BY e.occurred_at DESC, e.id DESC
    """
    rows = db.execute(sql, sql_params).fetchall()

    events = []
    for r in rows:
        events.append({
            "id": r["event_id"],
            "occurred_at": r["occurred_at"],
            "is_sweep": bool(r["is_sweep"]),
            "sweep_cup_en": r["sweep_cup_en"],
            "sweep_cup_es": r["sweep_cup_es"],
            "sweep_owner": r["sweep_owner"],
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
            "track_en": r["track_en"],
            "track_es": r["track_es"],
            "winner": r["winner"],
            "pre_owner": r["pre_owner"],
            "pre_state": r["pre_state"],
            "pre_mark": r["pre_mark"],
            "post_owner": r["post_owner"],
            "post_state": r["post_state"],
            "post_mark": r["post_mark"],
        })

    return render_template(
        "events.html",
        events=events,
        season_label=season_label,
        players=players,
        cups=cups,
        tracks=tracks,
        default_player=default_player,
        selected_filters={
            "player": player_filter,
            "cup": cup_filter,
            "track": track_filter,
            "event_type": event_type_filter,
            "me": me_filter
        }
    )

@app.route("/export/events.csv")
def export_events():
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured.")
    season_id = season_row["id"]
    season_label = season_row["label"]
    label_safe = season_label.replace(" — ", " ").replace("—", "-").replace(" ", "_")

    # Include both normal race events and sweep events.
    # For sweep events, track_id may or may not be meaningful in the future, so we LEFT JOIN tracks/cups.
    sql = """
    SELECT
        e.id AS event_id,
        e.occurred_at,
        e.is_sweep,

        -- sweep metadata (present only if is_sweep = 1)
        c2.code AS sweep_cup_code,
        c2.en   AS sweep_cup_en,
        c2.es   AS sweep_cup_es,
        so.name AS sweep_owner,

        -- race context (for is_sweep=0 these are the actual race's track/cup;
        -- for is_sweep=1 they’ll reflect the triggering track, which is fine)
        c.code  AS cup_code,
        c.en    AS cup_en,
        c.es    AS cup_es,
        t.code  AS track_code,
        t.en    AS track_en,
        t.es    AS track_es,

        -- race participants and state snapshots
        pw.name   AS winner,
        preo.name AS pre_owner,
        e.pre_state,
        pret.name AS pre_mark,
        posto.name AS post_owner,
        e.post_state,
        postt.name AS post_mark
    FROM events e
    LEFT JOIN tracks  t   ON t.id  = e.track_id
    LEFT JOIN cups    c   ON c.id  = t.cup_id
    LEFT JOIN cups    c2  ON c2.id = e.sweep_cup_id
    LEFT JOIN players so  ON so.id = e.sweep_owner_id
    LEFT JOIN players pw  ON pw.id = e.winner_id
    LEFT JOIN players preo ON preo.id = e.pre_owner_id
    LEFT JOIN players pret ON pret.id = e.pre_threatened_by_id
    LEFT JOIN players posto ON posto.id = e.post_owner_id
    LEFT JOIN players postt ON postt.id = e.post_threatened_by_id
    WHERE (t.season = ? OR e.is_sweep = 1)
    ORDER BY e.occurred_at ASC, e.id ASC
    """
    rows = db.execute(sql, (season_id,)).fetchall()

    header = [
        "event_id", "occurred_at", "is_sweep",
        "sweep_cup_code", "sweep_cup_en", "sweep_cup_es", "sweep_owner",
        "cup_code", "cup_en", "cup_es",
        "track_code", "track_en", "track_es",
        "winner",
        "pre_owner", "pre_state", "pre_state_label", "pre_mark",
        "post_owner", "post_state", "post_state_label", "post_mark",
    ]

    out = []
    for r in rows:
        out.append([
            r["event_id"],
            r["occurred_at"],
            r["is_sweep"],

            r["sweep_cup_code"], r["sweep_cup_en"], r["sweep_cup_es"],
            r["sweep_owner"],

            r["cup_code"], r["cup_en"], r["cup_es"],
            r["track_code"], r["track_en"], r["track_es"],

            r["winner"],

            r["pre_owner"], r["pre_state"], state_label(r["pre_state"]) if r["pre_state"] is not None else "",
            r["pre_mark"],

            r["post_owner"], r["post_state"], state_label(r["post_state"]) if r["post_state"] is not None else "",
            r["post_mark"],
        ])

    filename = f"events_{label_safe}.csv"
    return csv_response(filename, header, out)


@app.route("/export/standings.csv")
def export_standings():
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured.")
    season_id = season_row["id"]
    season_label = season_row["label"]
    label_safe = season_label.replace(" — ", " ").replace("—", "-").replace(" ", "_")

    owner_f = request.args.get("owner", default="all")
    cup_f = request.args.get("cup", default="all")
    state_f = request.args.get("state", default="any")

    where = ["t.season = ?"]
    args = [season_id]

    if owner_f and owner_f.lower() != "all":
        where.append("po.name = ?")
        args.append(owner_f)

    if cup_f and cup_f.lower() != "all":
        where.append("c.code = ?")
        args.append(cup_f)

    if state_f and state_f.lower() != "any":
        mapping = {"locked": 1, "default": 0, "at-risk": -1, "atrisk": -1, "at_risk": -1}
        val = mapping.get(state_f.lower())
        if val is not None:
            where.append("t.state = ?")
            args.append(val)

    sql = f"""
    SELECT
        c.code AS cup_code, c.en AS cup_en, c.es AS cup_es, c.[order] AS cup_order,
        t.code AS track_code, t.en AS track_en, t.es AS track_es, t.order_in_cup,
        po.name AS owner, t.state, pt.name AS mark
    FROM tracks t
    JOIN cups c ON c.id = t.cup_id
    LEFT JOIN players po ON po.id = t.owner_id
    LEFT JOIN players pt ON pt.id = t.threatened_by_id
    WHERE {' AND '.join(where)}
    ORDER BY c.[order] ASC, t.order_in_cup ASC
    """
    rows = db.execute(sql, args).fetchall()

    header = ["cup_code","cup_en","cup_es","track_code","track_en","track_es","order_in_cup","owner","state","state_label","mark"]
    out = []
    for r in rows:
        out.append([
            r["cup_code"], r["cup_en"], r["cup_es"],
            r["track_code"], r["track_en"], r["track_es"],
            r["order_in_cup"],
            r["owner"], r["state"], state_label(r["state"]), r["mark"]
        ])
    fbits = []
    if owner_f and owner_f.lower() != "all": fbits.append(f"owner-{owner_f}")
    if cup_f and cup_f.lower() != "all": fbits.append(f"cup-{cup_f}")
    if state_f and state_f.lower() != "any": fbits.append(f"state-{state_f}")
    suffix = ("_" + "_".join(fbits)) if fbits else ""
    filename = f"standings_{label_safe}{suffix}.csv"
    return csv_response(filename, header, out)

@app.route("/stats")
def stats_page():
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured.")
    season_id = season_row["id"]
    season_label = season_row["label"]

    # --- Player base data ---
    player_rows = db.execute(
        "SELECT id, name FROM players WHERE active = 1 ORDER BY name"
    ).fetchall()
    player_names = {r["id"]: r["name"] for r in player_rows}

    def make_empty_player_stats(name: str):
        return {
            "name": name,
            "tracks_owned": 0,
            "locked_tracks": 0,
            "wins": 0,
            "wins_as_owner": 0,
            "wins_as_non_owner": 0,
            "sweeps": 0,
            "tracks_taken": 0,
            "tracks_lost": 0,
            "net_tracks": 0,
            "races_as_owner": 0,
            "defense_successes": 0,
            "defense_success_rate": None,
            "defense_at_risk_attempts": 0,
            "defense_at_risk_successes": 0,
            "defense_at_risk_rate": None,
            "wins_on_risk": 0,
            "steals_from_risk": 0,
            "hunter_marks": 0,
            "wins_with_hunter_mark": 0,
            "races_played": 0,
        }

    stats = {pid: make_empty_player_stats(name) for pid, name in player_names.items()}
    for pid, entry in stats.items():
        entry["id"] = pid

    def ensure_player(pid):
        if pid is None:
            return None
        if pid not in stats:
            stats[pid] = make_empty_player_stats(player_names.get(pid, f"Player {pid}"))
        return stats[pid]

    # --- Track metadata for this season ---
    track_rows = db.execute(
        """
        SELECT t.id, t.en AS track_en, t.es AS track_es,
               c.en AS cup_en, c.es AS cup_es,
               t.owner_id AS final_owner_id,
               t.state AS final_state
        FROM tracks t
        JOIN cups c ON c.id = t.cup_id
        WHERE t.season = ?
        """,
        (season_id,),
    ).fetchall()

    track_info = {}
    for r in track_rows:
        tid = r["id"]
        track_info[tid] = {
            "track_en": r["track_en"],
            "track_es": r["track_es"],
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
        }

        owner_id = r["final_owner_id"]
        state = r["final_state"]
        if owner_id is not None:
            ps = ensure_player(owner_id)
            ps["tracks_owned"] += 1
            if state == 1:
                ps["locked_tracks"] += 1

    # --- Per-track counters used for highlights ---
    track_race_count = defaultdict(int)
    track_ownership_changes = defaultdict(int)
    track_defenses = defaultdict(int)  # successful defenses on that track

    # --- Sweeps per player ---
    sweep_rows = db.execute(
        """
        SELECT e.sweep_owner_id
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        WHERE t.season = ? AND e.is_sweep = 1
        """,
        (season_id,),
    ).fetchall()
    for r in sweep_rows:
        sweeper_id = r["sweep_owner_id"]
        ps = ensure_player(sweeper_id)
        if ps:
            ps["sweeps"] += 1

    # --- Normal race events for this season ---
    event_rows = db.execute(
        """
        SELECT e.*, t.id AS track_id
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        WHERE t.season = ? AND e.is_sweep = 0
        ORDER BY e.occurred_at ASC, e.id ASC
        """,
        (season_id,),
    ).fetchall()

    for e in event_rows:
        track_id = e["track_id"]
        winner_id = e["winner_id"]
        pre_owner_id = e["pre_owner_id"]
        post_owner_id = e["post_owner_id"]
        pre_state = e["pre_state"]
        post_state = e["post_state"]
        pre_threat = e["pre_threatened_by_id"]

        track_race_count[track_id] += 1

        winner_stats = ensure_player(winner_id)
        pre_owner_stats = ensure_player(pre_owner_id)
        post_owner_stats = ensure_player(post_owner_id)

        # Basic wins / races
        if winner_stats:
            winner_stats["wins"] += 1
            winner_stats["races_played"] += 1
            if pre_owner_id == winner_id:
                winner_stats["wins_as_owner"] += 1
            else:
                winner_stats["wins_as_non_owner"] += 1

        if pre_owner_stats:
            pre_owner_stats["races_played"] += 1
            pre_owner_stats["races_as_owner"] += 1

        # Ownership changes
        if pre_owner_id and post_owner_id and pre_owner_id != post_owner_id:
            track_ownership_changes[track_id] += 1
            if pre_owner_stats:
                pre_owner_stats["tracks_lost"] += 1
            if post_owner_stats:
                post_owner_stats["tracks_taken"] += 1

        # General defense: owner keeps track
        if pre_owner_id and winner_id == pre_owner_id and post_owner_id == pre_owner_id:
            pre_owner_stats["defense_successes"] += 1
            track_defenses[track_id] += 1

        # Defense on at-risk tracks
        if pre_owner_id and pre_state == -1:
            pre_owner_stats["defense_at_risk_attempts"] += 1
            if winner_id == pre_owner_id and post_owner_id == pre_owner_id:
                pre_owner_stats["defense_at_risk_successes"] += 1

        # Wins on at-risk tracks (anyone)
        if winner_id and pre_state == -1:
            winner_stats["wins_on_risk"] += 1
            if pre_owner_id and winner_id != pre_owner_id:
                winner_stats["steals_from_risk"] += 1

        # Hunter marks: winner puts someone else at risk from default
        if (
            winner_id
            and pre_state == 0
            and pre_owner_id
            and winner_id != pre_owner_id
            and post_state == -1
        ):
            winner_stats["hunter_marks"] += 1

        # Wins when you are the existing hunter mark
        if winner_id and pre_state == -1 and pre_threat == winner_id:
            winner_stats["wins_with_hunter_mark"] += 1

    # --- Derived per-player stats ---
    for ps in stats.values():
        ps["net_tracks"] = ps["tracks_taken"] - ps["tracks_lost"]

        if ps["races_as_owner"] > 0:
            ps["defense_success_rate"] = (
                ps["defense_successes"] / ps["races_as_owner"] * 100.0
            )
        else:
            ps["defense_success_rate"] = None

        if ps["defense_at_risk_attempts"] > 0:
            ps["defense_at_risk_rate"] = (
                ps["defense_at_risk_successes"] / ps["defense_at_risk_attempts"] * 100.0
            )
        else:
            ps["defense_at_risk_rate"] = None
        challenger_attempts = max(ps["races_played"] - ps["races_as_owner"], 0)
        ps["challenge_attempts"] = challenger_attempts
        if challenger_attempts > 0:
            ps["challenge_win_rate"] = (
                ps["wins_as_non_owner"] / challenger_attempts * 100.0
            )
        else:
            ps["challenge_win_rate"] = None

        if ps["races_played"] > 0:
            ps["win_rate"] = ps["wins"] / ps["races_played"] * 100.0
        else:
            ps["win_rate"] = None

    def metric_sort_value(player, descriptor):
        if descriptor["type"] in ("value", "percent"):
            val = player.get(descriptor.get("value_key"))
            return val if val is not None else 0
        if descriptor["type"] == "pair":
            num = player.get(descriptor.get("num_key"), 0) or 0
            den = player.get(descriptor.get("den_key"), 0) or 0
            return (num / den) if den else 0
        return 0

    metric_def_map = {d["id"]: d for d in METRIC_DEFS}
    sort_metric_id = request.args.get("sort", default="wins")
    sort_descriptor = metric_def_map.get(sort_metric_id, metric_def_map.get("wins"))

    player_stats = sorted(
        stats.values(),
        key=lambda s: (
            -metric_sort_value(s, sort_descriptor),
            -s["tracks_owned"],
            s["name"].lower(),
        ),
    )

    # --- Track highlights (season-wide) ---

    # Most active tracks
    track_activity = []
    for tid, count in track_race_count.items():
        info = track_info.get(tid)
        if not info:
            continue
        track_activity.append(
            {
                "track_en": info["track_en"],
                "track_es": info["track_es"],
                "cup_en": info["cup_en"],
                "cup_es": info["cup_es"],
                "races": count,
            }
        )
    track_activity.sort(key=lambda x: (-x["races"], x["track_en"]))
    track_activity = track_activity[:10]

    # Most defended track
    most_defended = None
    if track_defenses:
        tid = max(track_defenses, key=lambda t: track_defenses[t])
        info = track_info.get(tid)
        most_defended = {
            "track_en": info["track_en"],
            "track_es": info["track_es"],
            "cup_en": info["cup_en"],
            "cup_es": info["cup_es"],
            "defenses": track_defenses[tid],
        }

    # Most contested track (ownership changes)
    most_contested = None
    if track_ownership_changes:
        tid = max(track_ownership_changes, key=lambda t: track_ownership_changes[t])
        info = track_info.get(tid)
        most_contested = {
            "track_en": info["track_en"],
            "track_es": info["track_es"],
            "cup_en": info["cup_en"],
            "cup_es": info["cup_es"],
            "changes": track_ownership_changes[tid],
        }

    # --- Track selector + optional track-specific stats ---

    track_selector = []
    for tid, info in track_info.items():
        track_selector.append(
            {
                "id": tid,
                "track_en": info["track_en"],
                "track_es": info["track_es"],
                "cup_en": info["cup_en"],
                "cup_es": info["cup_es"],
            }
        )
    track_selector.sort(key=lambda x: (x["cup_en"], x["track_en"]))

    selected_track_id = request.args.get("track_id", type=int)
    selected_track = None
    track_player_stats = []
    track_events = []

    if selected_track_id:
        selected_track = db.execute(
            """
            SELECT t.*, c.en AS cup_en, c.es AS cup_es
            FROM tracks t
            JOIN cups c ON c.id = t.cup_id
            WHERE t.id = ? AND t.season = ?
            """,
            (selected_track_id, season_id),
        ).fetchone()

        if selected_track:
            events = db.execute(
                """
                SELECT
                    e.*,
                    pw.name   AS winner_name,
                    preo.name AS pre_owner_name,
                    posto.name AS post_owner_name
                FROM events e
                JOIN tracks t ON t.id = e.track_id
                LEFT JOIN players pw   ON pw.id   = e.winner_id
                LEFT JOIN players preo ON preo.id = e.pre_owner_id
                LEFT JOIN players posto ON posto.id = e.post_owner_id
                WHERE t.season = ? AND t.id = ? AND e.is_sweep = 0
                ORDER BY e.occurred_at ASC, e.id ASC
                """,
                (season_id, selected_track_id),
            ).fetchall()

            per_player_track = defaultdict(
                lambda: {
                    "name": "",
                    "wins": 0,
                    "defenses": 0,
                    "takes": 0,
                    "losses": 0,
                }
            )

            for ev in events:
                winner_id = ev["winner_id"]
                pre_owner_id = ev["pre_owner_id"]
                post_owner_id = ev["post_owner_id"]

                if winner_id:
                    ps = per_player_track[winner_id]
                    ps["name"] = player_names.get(winner_id, f"Player {winner_id}")
                    ps["wins"] += 1

                if pre_owner_id and post_owner_id and pre_owner_id != post_owner_id:
                    ps_old = per_player_track[pre_owner_id]
                    ps_old["name"] = player_names.get(pre_owner_id, f"Player {pre_owner_id}")
                    ps_old["losses"] += 1

                    ps_new = per_player_track[post_owner_id]
                    ps_new["name"] = player_names.get(post_owner_id, f"Player {post_owner_id}")
                    ps_new["takes"] += 1

                if (
                    pre_owner_id
                    and winner_id == pre_owner_id
                    and post_owner_id == pre_owner_id
                ):
                    ps_def = per_player_track[pre_owner_id]
                    ps_def["name"] = player_names.get(pre_owner_id, f"Player {pre_owner_id}")
                    ps_def["defenses"] += 1

            track_player_stats = sorted(
                per_player_track.values(),
                key=lambda s: (-s["wins"], -s["defenses"], s["name"].lower()),
            )
            track_events = events

    def build_cell(player_row, descriptor):
        if descriptor["type"] == "value":
            val = player_row.get(descriptor["value_key"], 0) or 0
            return {"value": val, "highlight": val}
        if descriptor["type"] == "percent":
            val = player_row.get(descriptor["value_key"])
            return {"percent": val, "highlight": val if val is not None else None}
        if descriptor["type"] == "pair":
            num = player_row.get(descriptor["num_key"], 0) or 0
            den = player_row.get(descriptor["den_key"], 0) or 0
            ratio = (num / den) if den else None
            return {"num": num, "den": den, "highlight": ratio}
        return {"value": 0, "highlight": 0}

    metric_rows = []
    for descriptor in METRIC_DEFS:
        row = {
            "id": descriptor["id"],
            "label": descriptor["label"],
            "type": descriptor["type"],
            "group": descriptor["group"],
            "cells": [],
            "help": descriptor.get("help"),
        }
        max_highlight = None
        for ps in player_stats:
            cell = build_cell(ps, descriptor)
            row["cells"].append(cell)
            highlight = cell.get("highlight")
            if highlight is not None and (max_highlight is None or highlight > max_highlight):
                max_highlight = highlight
        row["max_highlight"] = max_highlight
        metric_rows.append(row)

    return render_template(
        "stats.html",
        season_label=season_label,
        player_stats=player_stats,
        metric_rows=metric_rows,
        metric_sort_options=METRIC_DEFS,
        selected_sort=sort_metric_id,
        track_activity=track_activity,
        most_defended=most_defended,
        most_contested=most_contested,
        track_selector=track_selector,
        selected_track=selected_track,
        track_player_stats=track_player_stats,
        track_events=track_events,
    )


@app.route("/admin/players", methods=["GET", "POST"])
def admin_players():
    db = get_db()
    season_row = get_current_season_row(db)
    season_label = season_row["label"] if season_row else "Koopa Krew"
    show_mode = request.args.get("show", default="active")
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = (request.form.get("name") or "").strip()
            if not name:
                flash("Name is required.", "error")
            else:
                try:
                    db.execute("INSERT INTO players (name, active) VALUES (?, 1)", (name,))
                    db.commit()
                    flash(f"Added player {name}.", "success")
                except sqlite3.IntegrityError:
                    flash("That name already exists.", "error")
        elif action == "rename":
            try:
                player_id = int(request.form.get("player_id"))
            except (TypeError, ValueError):
                flash("Invalid player id.", "error")
                return redirect(url_for("admin_players"))
            name = (request.form.get("name") or "").strip()
            if not name:
                flash("Name is required.", "error")
            else:
                try:
                    db.execute("UPDATE players SET name = ? WHERE id = ?", (name, player_id))
                    db.commit()
                    flash("Name updated.", "success")
                except sqlite3.IntegrityError:
                    flash("That name already exists.", "error")
        elif action == "toggle":
            try:
                player_id = int(request.form.get("player_id"))
            except (TypeError, ValueError):
                flash("Invalid player id.", "error")
            else:
                db.execute(
                    "UPDATE players SET active = CASE active WHEN 1 THEN 0 ELSE 1 END WHERE id = ?",
                    (player_id,),
                )
                db.commit()
                flash("Toggled player status.", "success")
        else:
            flash("Unknown action.", "error")
        return redirect(url_for("admin_players"))

    players = fetch_all_players(db)
    filtered_players = players if show_mode == "all" else [p for p in players if p["active"]]
    active_count = sum(1 for p in players if p["active"])
    inactive_count = len(players) - active_count
    return render_template(
        "admin_players.html",
        season_label=season_label,
        players=filtered_players,
        counts={"active": active_count, "inactive": inactive_count},
        show_mode=show_mode,
        default_player=get_default_player(db),
    )


@app.route("/admin/players/set-default", methods=["POST"])
def set_default_player():
    db = get_db()
    player_id = request.form.get("player_id", type=int)
    row = db.execute(
        "SELECT id, name, active FROM players WHERE id = ?",
        (player_id,),
    ).fetchone()
    if not row:
        flash("Player not found.", "error")
    elif not row["active"]:
        flash("Activate the player before setting as default.", "error")
    else:
        session["default_player_id"] = row["id"]
        flash(f"{row['name']} is now your default player.", "success")
    show_mode = request.form.get("show_mode")
    return redirect(url_for("admin_players", show=show_mode) if show_mode else url_for("admin_players"))


@app.route("/admin/players/clear-default", methods=["POST"])
def clear_default_player():
    session.pop("default_player_id", None)
    flash("Cleared your default player.", "info")
    show_mode = request.form.get("show_mode")
    return redirect(url_for("admin_players", show=show_mode) if show_mode else url_for("admin_players"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
