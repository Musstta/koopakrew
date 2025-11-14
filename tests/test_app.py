import json
import tempfile
import unittest
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

from flask import template_rendered

import app
import db_init

PLAYERS = ["Salim", "Sergio", "Fabian", "Sebas"]


def _insert_players(db):
    for name in PLAYERS:
        db.execute("INSERT INTO players (name, active) VALUES (?, 1)", (name,))
    db.commit()
    rows = db.execute("SELECT id, name FROM players").fetchall()
    return {r["name"]: r["id"] for r in rows}


def seed_template_season(db):
    players = _insert_players(db)
    cup_id = db.execute(
        'INSERT INTO cups (code, en, es, "order") VALUES (?, ?, ?, ?)',
        ("MUSH", "Mushroom Cup", "Copa Hongo", 1),
    ).lastrowid
    today = date.today()
    start = (today - timedelta(days=400)).isoformat()
    end = (today - timedelta(days=300)).isoformat()
    season_id = db.execute(
        "INSERT INTO season_meta (label, start_date, end_date) VALUES (?, ?, ?)",
        ("Season 1 — Archive", start, end),
    ).lastrowid
    track_codes = ["TRK1", "TRK2"]
    for idx, code in enumerate(track_codes, start=1):
        db.execute(
            """
            INSERT INTO tracks
              (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
            VALUES (?, ?, ?, ?, ?, NULL, 0, NULL, ?)
            """,
            (
                code,
                cup_id,
                f"Track {idx}",
                f"Pista {idx}",
                idx,
                season_id,
            ),
        )
    db.commit()
    return {"players": players, "season_id": season_id, "cup_id": cup_id, "track_count": len(track_codes)}


def seed_active_environment(db):
    players = _insert_players(db)
    cup_id = db.execute(
        'INSERT INTO cups (code, en, es, "order") VALUES (?, ?, ?, ?)',
        ("FLOW", "Flower Cup", "Copa Flor", 1),
    ).lastrowid
    today = date.today()
    start = (today - timedelta(days=5)).isoformat()
    end = (today + timedelta(days=90)).isoformat()
    season_id = db.execute(
        "INSERT INTO season_meta (label, start_date, end_date) VALUES (?, ?, ?)",
        ("Season X — Test", start, end),
    ).lastrowid
    track_blank = db.execute(
        """
        INSERT INTO tracks
            (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
        VALUES (?, ?, ?, ?, ?, NULL, 0, NULL, ?)
        """,
        ("ALPHA", cup_id, "Alpha Course", "Curso Alfa", 1, season_id),
    ).lastrowid
    track_owned = db.execute(
        """
        INSERT INTO tracks
            (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
        VALUES (?, ?, ?, ?, ?, ?, 0, NULL, ?)
        """,
        ("BRAVO", cup_id, "Bravo Course", "Curso Bravo", 2, players["Salim"], season_id),
    ).lastrowid
    db.commit()
    return {
        "players": players,
        "season_id": season_id,
        "cup_id": cup_id,
        "tracks": {"blank": track_blank, "owned": track_owned},
    }


@contextmanager
def captured_templates(flask_app):
    recorded = []

    def record(sender, template, context, **extra):
        recorded.append((template, context))

    template_rendered.connect(record, flask_app)
    try:
        yield recorded
    finally:
        template_rendered.disconnect(record, flask_app)


class AppTestCase(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def bootstrap(self, seed_fn):
        db_path = self.tmp_path / f"koopakrew_{uuid4().hex}.db"
        app.DB_PATH = str(db_path)
        app.app.config["TESTING"] = True
        with app.app.app_context():
            db = app.get_db()
            db_init.create_schema(db)
            ctx = seed_fn(db)
        client = app.app.test_client()
        return client, ctx, db_path


class AppModuleTests(AppTestCase):
    def test_season_autocreation_clones_tracks(self):
        _, ctx, _ = self.bootstrap(seed_template_season)
        with app.app.app_context():
            db = app.get_db()
            row = app.get_current_season_row(db)
            self.assertEqual(row["start_date"], date.today().isoformat())
            count = db.execute(
                "SELECT COUNT(*) AS n FROM tracks WHERE season = ?", (row["id"],)
            ).fetchone()["n"]
            self.assertEqual(count, ctx["track_count"])

    def test_standings_owner_filter(self):
        client, _, _ = self.bootstrap(seed_active_environment)
        resp = client.get("/?owner=Salim")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn("Bravo Course", body)
        self.assertNotIn("Alpha Course", body)

    def test_apply_result_and_undo(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_id, salim_id)
            row = db.execute("SELECT owner_id FROM tracks WHERE id = ?", (track_id,)).fetchone()
            self.assertEqual(row["owner_id"], salim_id)
            app.undo_last_event(db)
            row = db.execute("SELECT owner_id FROM tracks WHERE id = ?", (track_id,)).fetchone()
            self.assertIsNone(row["owner_id"])

    def test_stats_page_context(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        track_blank = ctx["tracks"]["blank"]
        track_owned = ctx["tracks"]["owned"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_blank, salim_id)
            app.apply_result(db, season_id, track_blank, salim_id)
            app.apply_result(db, season_id, track_owned, sergio_id)
            app.apply_result(db, season_id, track_owned, sergio_id)
        with captured_templates(app.app) as templates:
            resp = client.get("/stats")
            self.assertEqual(resp.status_code, 200)
            template, context = templates[0]
            self.assertEqual(template.name, "stats.html")
            salim_stats = next(p for p in context["player_stats"] if p["name"] == "Salim")
            sergio_stats = next(p for p in context["player_stats"] if p["name"] == "Sergio")
            self.assertEqual(salim_stats["wins_as_owner"], 1)
            self.assertGreaterEqual(sergio_stats["tracks_taken"], 1)

    def test_stats_page_includes_inactive_player_names(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        season_id = ctx["season_id"]
        track_blank = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_blank, salim_id)
            db.execute("UPDATE players SET active = 0 WHERE id = ?", (salim_id,))
            db.commit()
        with captured_templates(app.app) as templates:
            resp = client.get("/stats")
            self.assertEqual(resp.status_code, 200)
            template, context = templates[0]
            names = [p["name"] for p in context["player_stats"]]
            self.assertIn("Salim", names)

    def test_events_filter_by_type(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        season_id = ctx["season_id"]
        track_blank = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_blank, salim_id)
            db.execute(
                """
                INSERT INTO events
                  (track_id, winner_id, occurred_at,
                   pre_owner_id, pre_state, pre_threatened_by_id,
                   post_owner_id, post_state, post_threatened_by_id,
                   side_effects_json, is_sweep, sweep_cup_id, sweep_owner_id)
                VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, 1, ?, ?)
                """,
                (
                    track_blank,
                    salim_id,
                    "2025-01-01T00:00:00",
                    json.dumps([]),
                    ctx["cup_id"],
                    salim_id,
                ),
            )
            db.commit()
        resp = client.get("/events?event_type=sweep")
        body = resp.get_data(as_text=True)
        self.assertIn("SWEEP", body)
        self.assertNotIn("Winner:", body)
        resp = client.get("/events?event_type=race")
        self.assertNotIn("SWEEP", resp.get_data(as_text=True))

    def test_admin_players_add_toggle(self):
        client, _, _ = self.bootstrap(seed_active_environment)
        resp = client.post(
            "/admin/players",
            data={"action": "add", "name": "Koopa Kid"},
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Koopa Kid", resp.get_data(as_text=True))
        with app.app.app_context():
            db = app.get_db()
            new_id = db.execute(
                "SELECT id FROM players WHERE name = ?", ("Koopa Kid",)
            ).fetchone()["id"]
        resp = client.post(
            "/admin/players",
            data={"action": "toggle", "player_id": new_id},
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 200)
        with app.app.app_context():
            db = app.get_db()
            active = db.execute(
                "SELECT active FROM players WHERE id = ?", (new_id,)
            ).fetchone()["active"]
            self.assertEqual(active, 0)

    def test_set_default_player_and_quick_update(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        resp = client.post(
            "/admin/players/set-default?show=active",
            data={"player_id": salim_id, "show_mode": "active"},
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 200)
        with client.session_transaction() as sess:
            self.assertEqual(sess.get("default_player_id"), salim_id)
        track_id = ctx["tracks"]["blank"]
        resp = client.get(f"/update/{track_id}?quick=1")
        body = resp.get_data(as_text=True)
        self.assertIn("Quick confirmation", body)

    def test_events_involves_me_filter_uses_default(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        with client.session_transaction() as sess:
            sess["default_player_id"] = salim_id
        resp = client.get("/events?me=1")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn("Events CSV", body)
        self.assertIn(f'value="{salim_id}" selected', body)

    def test_archive_page_loads(self):
        client, _, _ = self.bootstrap(seed_active_environment)
        resp = client.get("/archive")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Season 1", resp.get_data(as_text=True))

    def test_undo_player_deactivation_restores_tracks(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        owned_track = ctx["tracks"]["owned"]
        client.post(
            "/admin/players",
            data={"action": "toggle", "player_id": salim_id},
            follow_redirects=True,
        )
        with app.app.app_context():
            db = app.get_db()
            active = db.execute(
                "SELECT active FROM players WHERE id = ?",
                (salim_id,),
            ).fetchone()["active"]
            self.assertEqual(active, 0)
            owner = db.execute(
                "SELECT owner_id FROM tracks WHERE id = ?",
                (owned_track,),
            ).fetchone()["owner_id"]
            self.assertIsNone(owner)
        client.post("/undo")
        with app.app.app_context():
            db = app.get_db()
            active = db.execute(
                "SELECT active FROM players WHERE id = ?",
                (salim_id,),
            ).fetchone()["active"]
            self.assertEqual(active, 1)
            owner = db.execute(
                "SELECT owner_id FROM tracks WHERE id = ?",
                (owned_track,),
            ).fetchone()["owner_id"]
            self.assertEqual(owner, salim_id)

    def test_presence_ping_tracks_online_players(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        app.ONLINE_PINGS.clear()
        with client.session_transaction() as sess:
            sess["default_player_id"] = salim_id
        resp = client.post("/presence/ping")
        self.assertEqual(resp.status_code, 200)
        with app.app.app_context():
            db = app.get_db()
            online = app.get_online_players(db)
        self.assertIn("Salim", online)

    def test_export_events_filters_other_season_sweeps(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        season_id = ctx["season_id"]
        track_blank = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_blank, salim_id)
            other_season = db.execute(
                "INSERT INTO season_meta (label, start_date, end_date) VALUES (?, ?, ?)",
                ("Season Y", "2024-01-01", "2024-04-01"),
            ).lastrowid
            other_cup_id = db.execute(
                'INSERT INTO cups (code, en, es, "order") VALUES (?, ?, ?, ?)',
                (f"T{other_season}", "Time Cup", "Copa Tiempo", other_season),
            ).lastrowid
            other_track = db.execute(
                """
                INSERT INTO tracks
                    (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
                VALUES (?, ?, ?, ?, ?, NULL, 0, NULL, ?)
                """,
                (f"TC{other_season}", other_cup_id, "Temporal Track", "Pista Temporal", 1, other_season),
            ).lastrowid
            db.execute(
                """
                INSERT INTO events
                  (track_id, winner_id, occurred_at,
                   pre_owner_id, pre_state, pre_threatened_by_id,
                   post_owner_id, post_state, post_threatened_by_id,
                   side_effects_json, is_sweep, sweep_cup_id, sweep_owner_id)
                VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, 1, ?, ?)
                """,
                (
                    other_track,
                    salim_id,
                    "2024-02-02T00:00:00",
                    json.dumps([]),
                    other_cup_id,
                    salim_id,
                ),
            )
            db.commit()
        resp = client.get("/export/events.csv")
        body = resp.get_data(as_text=True)
        self.assertNotIn("Time Cup", body)


if __name__ == "__main__":
    unittest.main()
