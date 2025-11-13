import os
import json
import sqlite3
import csv
import io
from collections import defaultdict
from datetime import datetime, date
from zoneinfo import ZoneInfo
from flask import Flask, g, render_template, request, redirect, url_for, abort, flash, make_response

DB_PATH = os.environ.get("KOOPAKREW_DB", "koopakrew.db")
LOCAL_TZ = os.environ.get("KOOPAKREW_TZ", "America/Costa_Rica")

app = Flask(__name__)
app.secret_key = os.environ.get("KOOPAKREW_SECRET", "koopakrew-dev-secret")  # replace in prod


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


def get_current_season_row(db):
    """Return the active season row based on local date; if none, return latest by start_date."""
    today = current_local_date().isoformat()
    cur = db.execute(
        "SELECT * FROM season_meta WHERE start_date <= ? AND end_date > ? ORDER BY start_date DESC LIMIT 1",
        (today, today)
    )
    row = cur.fetchone()
    if row:
        return row
    # Fallback: show the most recent season if none matches (e.g., data not seeded for this quarter yet)
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
    # Group by cup
    grouped = {}
    for r in rows:
        key = (r["cup_en"], r["cup_es"])
        grouped.setdefault(key, []).append(dict(r))
    return grouped


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

    return render_template(
        "index.html",
        standings=standings,
        totals_overall=totals_overall,
        totals_filtered=totals_filtered,
        medals=medals,
        players=players,
        cups=cups,
        selected_filters={"owner": owner_f, "cup": cup_f, "state": state_f},
        season_label=season_label
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
        season_label=season_label
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

    # Re-use the export_events query, but select only what we need
    sql = """
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
    WHERE (t.season = ? OR e.is_sweep = 1)
    ORDER BY e.occurred_at DESC, e.id DESC
    """
    rows = db.execute(sql, (season_id,)).fetchall()

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
        season_label=season_label
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
    season_start = date.fromisoformat(season_row["start_date"])
    season_start_dt = datetime.combine(
        season_start,
        datetime.min.time(),
        tzinfo=ZoneInfo(LOCAL_TZ),
    )
    now_dt = datetime.now(ZoneInfo(LOCAL_TZ))

    # Helper factory for per-player stats dicts
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
            "total_seconds_held": 0.0,
        }

    # Players
    player_rows = db.execute(
        "SELECT id, name FROM players WHERE active = 1 ORDER BY name"
    ).fetchall()
    player_names = {r["id"]: r["name"] for r in player_rows}
    stats = {pid: make_empty_player_stats(name) for pid, name in player_names.items()}

    def ensure_player(pid):
        if pid is None:
            return None
        if pid not in stats:
            stats[pid] = make_empty_player_stats(player_names.get(pid, f"Player {pid}"))
        return stats[pid]

    # Track metadata for this season
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
    track_final_owner = {}
    for r in track_rows:
        track_info[r["id"]] = {
            "track_en": r["track_en"],
            "track_es": r["track_es"],
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
        }
        track_final_owner[r["id"]] = r["final_owner_id"]

        # Use final state/owner for current tracks_owned/locked_tracks
        owner_id = r["final_owner_id"]
        state = r["final_state"]
        if owner_id is not None:
            ps = ensure_player(owner_id)
            ps["tracks_owned"] += 1
            if state == 1:
                ps["locked_tracks"] += 1

    # Per-track stats holders
    track_race_count = defaultdict(int)
    track_ownership_changes = defaultdict(int)
    track_defenses = defaultdict(int)  # per-track total defenses

    # Player-track hold durations
    hold_durations = defaultdict(float)  # (player_id, track_id) -> seconds
    track_current_owner = {}
    track_current_since = {}
    tracks_with_events = set()

    # Sweeps per player
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

    # Normal race events, ordered per track for timeline purposes
    event_rows = db.execute(
        """
        SELECT e.*, t.id AS track_id
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        WHERE t.season = ? AND e.is_sweep = 0
        ORDER BY t.id ASC, e.occurred_at ASC, e.id ASC
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

        occurred_at = datetime.fromisoformat(e["occurred_at"])

        tracks_with_events.add(track_id)
        track_race_count[track_id] += 1

        # Init timeline for this track on first event
        if track_id not in track_current_owner:
            track_current_owner[track_id] = pre_owner_id
            track_current_since[track_id] = season_start_dt

        # Duration for current owner up to this event
        cur_owner = track_current_owner[track_id]
        cur_since = track_current_since[track_id]
        if cur_owner is not None:
            hold_durations[(cur_owner, track_id)] += (occurred_at - cur_since).total_seconds()

        # Update current owner to post-owner for next segment
        track_current_owner[track_id] = post_owner_id
        track_current_since[track_id] = occurred_at

        # Player-level stats
        winner_stats = ensure_player(winner_id)
        pre_owner_stats = ensure_player(pre_owner_id)
        post_owner_stats = ensure_player(post_owner_id)

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

        # General defense (owner keeps the track, regardless of risk)
        if pre_owner_id:
            if winner_id == pre_owner_id and post_owner_id == pre_owner_id:
                pre_owner_stats["defense_successes"] += 1
                track_defenses[track_id] += 1

        # Defense attempts on at-risk tracks
        if pre_owner_id and pre_state == -1:
            pre_owner_stats["defense_at_risk_attempts"] += 1
            if winner_id == pre_owner_id and post_owner_id == pre_owner_id:
                pre_owner_stats["defense_at_risk_successes"] += 1

        # Wins on at-risk tracks (any player)
        if winner_id and pre_state == -1:
            winner_stats["wins_on_risk"] += 1
            if pre_owner_id and winner_id != pre_owner_id:
                winner_stats["steals_from_risk"] += 1

        # Hunter marks: winner puts someone else's track at risk from default
        if (
            winner_id
            and pre_state == 0
            and pre_owner_id
            and winner_id != pre_owner_id
            and post_state == -1
        ):
            winner_stats["hunter_marks"] += 1

        # Wins when you are also the hunter mark
        if winner_id and pre_state == -1 and pre_threat == winner_id:
            winner_stats["wins_with_hunter_mark"] += 1

    # Close open ownership intervals for tracks with events
    for track_id, cur_owner in track_current_owner.items():
        cur_since = track_current_since[track_id]
        if cur_owner is not None:
            hold_durations[(cur_owner, track_id)] += (now_dt - cur_since).total_seconds()

    # Tracks with no events: final owner holds from season start to now
    for track_id, info in track_info.items():
        if track_id in tracks_with_events:
            continue
        owner_id = track_final_owner.get(track_id)
        if owner_id:
            hold_durations[(owner_id, track_id)] += (now_dt - season_start_dt).total_seconds()

    # Apply total held time to per-player stats
    for (pid, tid), seconds in hold_durations.items():
        ps = ensure_player(pid)
        if ps:
            ps["total_seconds_held"] += seconds

    # Derived fields per player
    for ps in stats.values():
        ps["net_tracks"] = ps["tracks_taken"] - ps["tracks_lost"]

        attempts = ps["races_as_owner"]
        if attempts > 0:
            ps["defense_success_rate"] = (ps["defense_successes"] / attempts) * 100.0
        else:
            ps["defense_success_rate"] = None

        risk_attempts = ps["defense_at_risk_attempts"]
        if risk_attempts > 0:
            ps["defense_at_risk_rate"] = (ps["defense_at_risk_successes"] / risk_attempts) * 100.0
        else:
            ps["defense_at_risk_rate"] = None

    # Player stats sorted by wins and tracks
    player_stats = sorted(
        stats.values(),
        key=lambda s: (-s["wins"], -s["tracks_owned"], s["name"].lower()),
    )

    # Track highlights

    # Most active tracks (by races)
    track_activity = []
    for tid, count in track_race_count.items():
        info = track_info.get(tid)
        if not info:
            continue
        track_activity.append({
            "track_en": info["track_en"],
            "track_es": info["track_es"],
            "cup_en": info["cup_en"],
            "cup_es": info["cup_es"],
            "races": count,
        })
    track_activity.sort(key=lambda x: (-x["races"], x["track_en"]))
    track_activity = track_activity[:10]

    # Most defended track (by total defenses for any player)
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

    # Track with most ownership changes
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

    # Track held the longest by any single player (total time)
    longest_held = None
    if hold_durations:
        # Find (player, track) pair with max hold seconds
        (pid, tid), seconds = max(hold_durations.items(), key=lambda kv: kv[1])
        info = track_info.get(tid)
        owner_name = player_names.get(pid, f"Player {pid}")
        longest_held = {
            "track_en": info["track_en"],
            "track_es": info["track_es"],
            "cup_en": info["cup_en"],
            "cup_es": info["cup_es"],
            "owner_name": owner_name,
            "seconds": seconds,
        }

    return render_template(
        "stats.html",
        season_label=season_label,
        player_stats=player_stats,
        track_activity=track_activity,
        most_defended=most_defended,
        most_contested=most_contested,
        longest_held=longest_held,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)