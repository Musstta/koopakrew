import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from threading import Thread
from unittest.mock import patch
from uuid import uuid4

from flask import template_rendered, g

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
    def _track_snapshot(self, db, track_id):
        return db.execute(
            "SELECT owner_id, state, threatened_by_id FROM tracks WHERE id = ?",
            (track_id,),
        ).fetchone()

    def _create_test_cup(self, db, season_id, owner_id, challenger_id):
        cup_code = f"TST{uuid4().hex[:5]}"
        cup_id = db.execute(
            'INSERT INTO cups (code, en, es, "order") VALUES (?, ?, ?, ?)',
            (cup_code, "Test Cup", "Copa Test", 999),
        ).lastrowid
        tracks = []
        for idx in range(4):
            code = f"{cup_code}_T{idx}"
            state = 0
            threatened = None
            owner = None
            if idx == 0:
                owner = owner_id
                state = 0
            elif idx == 1:
                owner = owner_id
                state = -1
                threatened = challenger_id
            elif idx == 2:
                owner = owner_id
                state = 1
            track_id = db.execute(
                """
                INSERT INTO tracks
                    (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    code,
                    cup_id,
                    f"Test Track {idx}",
                    f"Pista Test {idx}",
                    idx + 1,
                    owner,
                    state,
                    threatened,
                    season_id,
                ),
            ).lastrowid
            tracks.append(track_id)
        db.commit()
        return cup_id, tracks

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
            sample_track = db.execute(
                "SELECT season FROM tracks WHERE season = ? LIMIT 1", (row["id"],)
            ).fetchone()
            self.assertIsNotNone(sample_track)

    def test_standings_owner_filter(self):
        client, _, _ = self.bootstrap(seed_active_environment)
        resp = client.get("/?owner=Salim")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn("Bravo Course", body)
        self.assertNotIn("Alpha Course", body)
        self.assertIn('value="Salim" selected', body)
        self.assertIn('value="Salim"', body)

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
            event_count = db.execute("SELECT COUNT(*) AS n FROM events WHERE track_id = ?", (track_id,)).fetchone()["n"]
            self.assertEqual(event_count, 1)
            app.undo_last_event(db)
            row = db.execute("SELECT owner_id FROM tracks WHERE id = ?", (track_id,)).fetchone()
            self.assertIsNone(row["owner_id"])
            event_count_after = db.execute("SELECT COUNT(*) AS n FROM events WHERE track_id = ?", (track_id,)).fetchone()["n"]
            self.assertEqual(event_count_after, 0)

    def test_track_state_full_lifecycle(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_id, salim_id)
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["owner_id"], salim_id)
            self.assertEqual(snap["state"], 0)
            app.apply_result(db, season_id, track_id, salim_id)
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["state"], 1)
            app.apply_result(db, season_id, track_id, sergio_id)
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["owner_id"], salim_id)
            self.assertEqual(snap["state"], 0)
            events = db.execute(
                """
                SELECT pre_owner_id, pre_state, post_owner_id, post_state
                FROM events WHERE track_id = ? ORDER BY id ASC
                """,
                (track_id,),
            ).fetchall()
            self.assertEqual(len(events), 3)
            self.assertEqual(events[0]["pre_owner_id"], None)
            self.assertEqual(events[1]["pre_state"], 0)
            self.assertEqual(events[2]["pre_state"], 1)

    def test_state_machine_at_risk_defense(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["owned"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_id, sergio_id)
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["state"], -1)
            self.assertEqual(snap["owner_id"], salim_id)
            self.assertEqual(snap["threatened_by_id"], sergio_id)
            app.apply_result(db, season_id, track_id, salim_id)
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["state"], 0)
            self.assertEqual(snap["owner_id"], salim_id)
            self.assertIsNone(snap["threatened_by_id"])

    def test_state_machine_at_risk_theft(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        fabian_id = ctx["players"]["Fabian"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["owned"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_id, sergio_id)
            app.apply_result(db, season_id, track_id, fabian_id)
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["owner_id"], fabian_id)
            self.assertEqual(snap["state"], 0)
            self.assertIsNone(snap["threatened_by_id"])

    def test_state_machine_multi_race_sequence(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        fabian_id = ctx["players"]["Fabian"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_id, salim_id)  # claim
            app.apply_result(db, season_id, track_id, salim_id)  # lock
            app.apply_result(db, season_id, track_id, sergio_id)  # break lock
            app.apply_result(db, season_id, track_id, fabian_id)  # set at risk
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["state"], -1)
            self.assertEqual(snap["threatened_by_id"], fabian_id)
            app.apply_result(db, season_id, track_id, salim_id)  # defend
            app.apply_result(db, season_id, track_id, sergio_id)  # at risk again
            app.apply_result(db, season_id, track_id, fabian_id)  # steal
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["owner_id"], fabian_id)
            self.assertEqual(snap["state"], 0)
            self.assertIsNone(snap["threatened_by_id"])
            event_rows = db.execute(
                "SELECT COUNT(*) AS n FROM events WHERE track_id = ?",
                (track_id,),
            ).fetchone()
            self.assertEqual(event_rows["n"], 7)

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
            self.assertGreaterEqual(salim_stats["locks_applied"], 1)
            self.assertTrue(context["track_insights_enabled"])
            self.assertEqual(len(context["metric_rows"]), len(app.METRIC_DEFS))
            self.assertEqual(len(context["player_spotlights"]), len(context["player_stats"]))
            self.assertIn("hot_hand", context["streak_badges"])
            metric_ids = {row["id"] for row in context["metric_rows"]}
            self.assertIn("best_win_streak", metric_ids)
            self.assertIn("best_defense_streak", metric_ids)

    def test_stats_page_reports_cups_owned_and_spotlights(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        fabian_id = ctx["players"]["Fabian"]
        season_id = ctx["season_id"]
        track_blank = ctx["tracks"]["blank"]
        track_owned = ctx["tracks"]["owned"]
        with app.app.app_context():
            db = app.get_db()
            extra_sergio = db.execute(
                """
                INSERT INTO tracks
                    (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
                VALUES (?, ?, ?, ?, ?, ?, 0, NULL, ?)
                """,
                ("ECHO", ctx["cup_id"], "Echo Run", "Pista Eco", 3, sergio_id, season_id),
            ).lastrowid
            extra_blank = db.execute(
                """
                INSERT INTO tracks
                    (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
                VALUES (?, ?, ?, ?, ?, NULL, 0, NULL, ?)
                """,
                ("DELTA", ctx["cup_id"], "Delta Ridge", "Cresta Delta", 4, season_id),
            ).lastrowid
            # Salim claims the remaining tracks and steals Sergio's to trigger a lock sweep.
            app.apply_result(db, season_id, track_blank, salim_id)
            app.apply_result(db, season_id, extra_blank, salim_id)
            app.apply_result(db, season_id, extra_sergio, salim_id)
            app.apply_result(db, season_id, extra_sergio, salim_id)
            # Sergio pressures one of the locked tracks so Salim can defend it.
            app.apply_result(db, season_id, extra_blank, sergio_id)
            app.apply_result(db, season_id, extra_blank, sergio_id)
            app.apply_result(db, season_id, extra_blank, salim_id)
            # Fabian takes a swing to ensure multiple challengers appear in stats.
            app.apply_result(db, season_id, track_owned, fabian_id)
            app.apply_result(db, season_id, track_owned, salim_id)
        with captured_templates(app.app) as templates:
            resp = client.get("/stats")
            self.assertEqual(resp.status_code, 200)
            _, context = templates[0]
            salim_stats = next(p for p in context["player_stats"] if p["name"] == "Salim")
            sergio_stats = next(p for p in context["player_stats"] if p["name"] == "Sergio")
            self.assertGreaterEqual(salim_stats["locks_applied"], 1)
            self.assertGreaterEqual(salim_stats["cups_owned_count"], 1)
            self.assertGreaterEqual(sergio_stats["hunter_marks"], 1)
            cards = {p["name"]: p for p in context["player_spotlights"]}
            salim_card = cards["Salim"]
            self.assertTrue(salim_card["cups_owned"])
            self.assertIsNotNone(salim_card["best_track"])
            self.assertIsNotNone(salim_card["most_attacked_track"])
            self.assertIsNotNone(salim_card["most_defended_track"])
            self.assertIsNotNone(salim_card["favorite_cup"])
    def test_stats_page_filters_inactive_only_for_current(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        with app.app.app_context():
            db = app.get_db()
            db.execute("UPDATE players SET active = 0 WHERE id = ?", (salim_id,))
            old_start = (date.today() - timedelta(days=200)).isoformat()
            old_end = (date.today() - timedelta(days=100)).isoformat()
            old_season_id = db.execute(
                "INSERT INTO season_meta (label, start_date, end_date) VALUES (?, ?, ?)",
                ("Season Legacy", old_start, old_end),
            ).lastrowid
            db.execute(
                """
                INSERT INTO tracks
                    (code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
                VALUES (?, ?, ?, ?, ?, NULL, 0, NULL, ?)
                """,
                ("LEGACY", ctx["cup_id"], "Legacy Track", "Pista Legado", 1, old_season_id),
            )
            db.commit()
        with captured_templates(app.app) as templates:
            resp = client.get("/stats")
            self.assertEqual(resp.status_code, 200)
            _, context = templates[0]
            names = [p["name"] for p in context["player_stats"]]
            self.assertNotIn("Salim", names)
            self.assertTrue(context["track_insights_enabled"])
            self.assertEqual(context["selected_season"], str(ctx["season_id"]))
        with captured_templates(app.app) as templates:
            resp = client.get(f"/stats?season={old_season_id}")
            self.assertEqual(resp.status_code, 200)
            _, context = templates[0]
            names = [p["name"] for p in context["player_stats"]]
            self.assertIn("Salim", names)
            self.assertTrue(context["track_insights_enabled"])
            self.assertEqual(context["selected_season"], str(old_season_id))

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
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn("SWEEP", body)
        self.assertNotIn("Winner:", body)
        resp = client.get("/events?event_type=race")
        self.assertEqual(resp.status_code, 200)
        race_body = resp.get_data(as_text=True)
        self.assertNotIn("SWEEP", race_body)
        self.assertIn("Winner:", race_body)

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
        self.assertIn("deactivated", resp.get_data(as_text=True))
        self.assertIn("Inactive", resp.get_data(as_text=True))
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
        self.assertIn("Salim", body)

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
        self.assertIn("Salim", body)

    def test_archive_page_loads(self):
        client, _, _ = self.bootstrap(seed_active_environment)
        resp = client.get("/archive")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn("Season 1", body)
        self.assertIn("Archive", body)

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
        undo_resp = client.post("/undo", data={"next": "/admin/players"}, follow_redirects=True)
        self.assertEqual(undo_resp.status_code, 200)
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
        self.assertEqual(resp.get_json()["status"], "ok")
        with app.app.app_context():
            db = app.get_db()
            online = app.get_online_players(db)
        self.assertIn("Salim", online)

    def test_switching_default_player_disconnects_previous_presence(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        app.ONLINE_PINGS.clear()
        with client.session_transaction() as sess:
            sess["default_player_id"] = salim_id
        client.post("/presence/ping")
        self.assertTrue(app.ONLINE_PINGS)
        resp = client.post(
            "/admin/players/set-default",
            data={"player_id": sergio_id},
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(app.ONLINE_PINGS)
        with client.session_transaction() as sess:
            self.assertEqual(sess.get("default_player_id"), sergio_id)

    def test_clearing_default_player_disconnects_presence(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        app.ONLINE_PINGS.clear()
        with client.session_transaction() as sess:
            sess["default_player_id"] = salim_id
        client.post("/presence/ping")
        self.assertTrue(app.ONLINE_PINGS)
        resp = client.post("/admin/players/clear-default", follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(app.ONLINE_PINGS)
        with client.session_transaction() as sess:
            self.assertIsNone(sess.get("default_player_id"))

    def test_presence_ping_drops_token_when_default_inactive(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        app.ONLINE_PINGS.clear()
        with client.session_transaction() as sess:
            sess["default_player_id"] = salim_id
        first_resp = client.post("/presence/ping")
        self.assertEqual(first_resp.get_json()["status"], "ok")
        with app.app.app_context():
            db = app.get_db()
            db.execute("UPDATE players SET active = 0 WHERE id = ?", (salim_id,))
            db.commit()
        second_resp = client.post("/presence/ping")
        self.assertEqual(second_resp.get_json()["status"], "ignored")
        self.assertFalse(app.ONLINE_PINGS)

    def test_presence_ping_without_default_clears_token(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        app.ONLINE_PINGS.clear()
        ghost_token = "ghost"
        app.ONLINE_PINGS[ghost_token] = {"player_id": salim_id, "last_seen": 0}
        with client.session_transaction() as sess:
            sess["presence_token"] = ghost_token
        resp = client.post("/presence/ping")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["status"], "ignored")
        self.assertFalse(app.ONLINE_PINGS)
        with client.session_transaction() as sess:
            self.assertNotIn("presence_token", sess)

    def test_online_presence_status_windows(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        app.ONLINE_PINGS.clear()
        salim = ctx["players"]["Salim"]
        sergio = ctx["players"]["Sergio"]
        fabian = ctx["players"]["Fabian"]
        base_time = 1_000_000.0
        app.ONLINE_PINGS["fresh"] = {"player_id": salim, "last_seen": base_time - 30}
        app.ONLINE_PINGS["warming"] = {"player_id": sergio, "last_seen": base_time - 150}
        app.ONLINE_PINGS["cooling"] = {"player_id": fabian, "last_seen": base_time - 260}
        with app.app.app_context():
            db = app.get_db()
            with patch("app.time.time", return_value=base_time):
                presence = app.get_online_presence(db)
        statuses = [entry["status"] for entry in presence]
        self.assertEqual(statuses, ["fresh", "warming", "cooling"])

    def test_online_presence_deduplicates_players(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        app.ONLINE_PINGS.clear()
        salim = ctx["players"]["Salim"]
        sergio = ctx["players"]["Sergio"]
        base_time = 2_000_000.0
        app.ONLINE_PINGS["one"] = {"player_id": salim, "last_seen": base_time - 10}
        app.ONLINE_PINGS["two"] = {"player_id": salim, "last_seen": base_time - 40}
        app.ONLINE_PINGS["three"] = {"player_id": sergio, "last_seen": base_time - 20}
        with app.app.app_context():
            db = app.get_db()
            with patch("app.time.time", return_value=base_time):
                players = app.get_online_players(db)
        self.assertEqual(players.count("Salim"), 1)
        self.assertEqual(players[0], "Salim")

    def test_online_presence_purges_stale_tokens(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        app.ONLINE_PINGS.clear()
        salim = ctx["players"]["Salim"]
        stale_time = 100.0
        app.ONLINE_PINGS["old"] = {"player_id": salim, "last_seen": stale_time}
        with app.app.app_context():
            db = app.get_db()
            with patch("app.time.time", return_value=stale_time + app.ONLINE_TIMEOUT_SECONDS + 10):
                presence = app.get_online_presence(db)
        self.assertEqual(presence, [])

    def test_inactive_toggle_undo_restores_stats_visibility(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        season_id = ctx["season_id"]
        track_blank = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_blank, salim_id)

        def current_stat_names():
            with captured_templates(app.app) as templates:
                resp = client.get("/stats")
                self.assertEqual(resp.status_code, 200)
                return [p["name"] for p in templates[0][1]["player_stats"]]

        client.post(
            "/admin/players",
            data={"action": "toggle", "player_id": salim_id},
            follow_redirects=True,
        )
        after_toggle = current_stat_names()
        self.assertNotIn("Salim", after_toggle)
        client.post("/undo", data={"next": "/stats"}, follow_redirects=True)
        after_undo = current_stat_names()
        self.assertIn("Salim", after_undo)

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
        self.assertIn("Alpha Course", body)

    def test_sweep_locks_mixed_states(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        with app.app.app_context():
            db = app.get_db()
            cup_id, tracks = self._create_test_cup(db, season_id, salim_id, sergio_id)
            app.apply_result(db, season_id, tracks[3], salim_id)
            states = [
                self._track_snapshot(db, tid)
                for tid in tracks
            ]
            for snap in states:
                self.assertEqual(snap["state"], 1)
                self.assertIsNone(snap["threatened_by_id"])
            sweep_events = db.execute(
                "SELECT * FROM events WHERE track_id = ? ORDER BY id ASC",
                (tracks[3],),
            ).fetchall()
            self.assertEqual(len(sweep_events), 2)
            self.assertEqual(sweep_events[1]["is_sweep"], 1)
            side = json.loads(sweep_events[1]["side_effects_json"])
            track_ids = {item["track_id"] for item in side}
            self.assertIn(tracks[0], track_ids)
            self.assertIn(tracks[1], track_ids)
            self.assertIn(tracks[3], track_ids)
            self.assertNotIn(tracks[2], track_ids)

    def test_sweep_undo_restores_states(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        with app.app.app_context():
            db = app.get_db()
            _, tracks = self._create_test_cup(db, season_id, salim_id, sergio_id)
            app.apply_result(db, season_id, tracks[3], salim_id)
            app.undo_last_event(db)
            snap1 = self._track_snapshot(db, tracks[0])
            snap2 = self._track_snapshot(db, tracks[1])
            snap3 = self._track_snapshot(db, tracks[2])
            snap4 = self._track_snapshot(db, tracks[3])
            self.assertEqual((snap1["state"], snap1["owner_id"]), (0, salim_id))
            self.assertEqual((snap2["state"], snap2["threatened_by_id"]), (-1, sergio_id))
            self.assertEqual(snap3["state"], 1)
            self.assertEqual(snap4["state"], 0)
            app.undo_last_event(db)
            snap4 = self._track_snapshot(db, tracks[3])
            self.assertIsNone(snap4["owner_id"])
            self.assertEqual(snap4["state"], 0)

    def test_sweep_near_miss_no_event(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        with app.app.app_context():
            db = app.get_db()
            _, tracks = self._create_test_cup(db, season_id, salim_id, sergio_id)
            for tid in tracks[:3]:
                db.execute(
                    "UPDATE tracks SET owner_id = ?, state = 0, threatened_by_id = NULL WHERE id = ?",
                    (salim_id, tid),
                )
            db.commit()
            before_events = db.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
            app.apply_result(db, season_id, tracks[3], sergio_id)
            after_events = db.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
            self.assertEqual(after_events, before_events + 1)
            sweeps = db.execute("SELECT COUNT(*) AS n FROM events WHERE is_sweep = 1").fetchone()["n"]
            self.assertEqual(sweeps, 0)
            snap = self._track_snapshot(db, tracks[3])
            self.assertEqual(snap["owner_id"], sergio_id)
            owned = db.execute(
                "SELECT COUNT(*) AS n FROM tracks WHERE cup_id = (SELECT cup_id FROM tracks WHERE id = ?) AND owner_id = ?",
                (tracks[0], salim_id),
            ).fetchone()["n"]
            self.assertEqual(owned, 3)

    def test_undo_multiple_events(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        fabian_id = ctx["players"]["Fabian"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        winners = [salim_id, sergio_id, fabian_id, salim_id, sergio_id]
        with app.app.app_context():
            db = app.get_db()
            snapshots = []
            for winner in winners:
                snapshots.append(self._track_snapshot(db, track_id))
                app.apply_result(db, season_id, track_id, winner)
            total_events = db.execute("SELECT COUNT(*) AS n FROM events WHERE track_id = ?", (track_id,)).fetchone()["n"]
            self.assertEqual(total_events, len(winners))
            while snapshots:
                expected = snapshots.pop()
                app.undo_last_event(db)
                snap = self._track_snapshot(db, track_id)
                self.assertEqual(
                    (snap["owner_id"], snap["state"], snap["threatened_by_id"]),
                    (expected["owner_id"], expected["state"], expected["threatened_by_id"]),
                )
            remaining = db.execute("SELECT COUNT(*) AS n FROM events WHERE track_id = ?", (track_id,)).fetchone()["n"]
            self.assertEqual(remaining, 0)

    def test_concurrent_race_submissions_documented(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        winners = [ctx["players"]["Salim"], ctx["players"]["Sergio"]]
        errors = []

        def run_race(winner):
            with app.app.app_context():
                db = app.get_db()
                try:
                    app.apply_result(db, season_id, track_id, winner)
                except sqlite3.OperationalError:
                    errors.append("OperationalError")

        threads = [Thread(target=run_race, args=(winner,)) for winner in winners]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with app.app.app_context():
            db = app.get_db()
            count = db.execute(
                "SELECT COUNT(*) AS n FROM events WHERE track_id = ?",
                (track_id,),
            ).fetchone()["n"]
            snap = self._track_snapshot(db, track_id)
        self.assertTrue(count in (1, 2))
        if errors:
            self.assertEqual(errors, ["OperationalError"])
            self.assertEqual(count, 1)
        else:
            self.assertEqual(count, 2)
            self.assertEqual(snap["owner_id"], winners[-1])

    def test_undo_during_race_submission_documented(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_id, salim_id)
        errors = []

        def race():
            time.sleep(0.01)
            with app.app.app_context():
                db = app.get_db()
                try:
                    app.apply_result(db, season_id, track_id, sergio_id)
                except sqlite3.OperationalError:
                    errors.append("OperationalError")

        def undo():
            with app.app.app_context():
                db = app.get_db()
                try:
                    app.undo_last_event(db)
                except sqlite3.OperationalError:
                    errors.append("OperationalError")

        threads = [Thread(target=race), Thread(target=undo)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with app.app.app_context():
            db = app.get_db()
            count = db.execute(
                "SELECT COUNT(*) AS n FROM events WHERE track_id = ?",
                (track_id,),
            ).fetchone()["n"]
            snap = self._track_snapshot(db, track_id)
        self.assertTrue(count in (0, 1))
        self.assertLessEqual(len(errors), 1)
        if count == 0:
            self.assertIsNone(snap["owner_id"])
        else:
            self.assertIn(snap["owner_id"], {salim_id, sergio_id})

    def test_undo_handles_malformed_side_effects(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            db.execute(
                """
                INSERT INTO events
                    (track_id, winner_id, occurred_at,
                     pre_owner_id, pre_state, pre_threatened_by_id,
                     post_owner_id, post_state, post_threatened_by_id,
                     side_effects_json, is_sweep)
                VALUES (?, NULL, '2025-01-01T00:00:00', NULL, 0, NULL, NULL, 0, NULL, '{invalid', 0)
                """,
                (track_id,),
            )
            db.commit()
            result = app.undo_last_event(db)
            self.assertTrue(result)
            remaining = db.execute("SELECT COUNT(*) AS n FROM events WHERE track_id = ?", (track_id,)).fetchone()["n"]
            self.assertEqual(remaining, 0)

    def test_race_submission_with_inactive_player_rejected(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        track_id = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            db.execute("UPDATE players SET active = 0 WHERE id = ?", (salim_id,))
            db.commit()
        resp = client.post(
            f"/update/{track_id}",
            data={"winner": "Salim"},
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 200)
        with app.app.app_context():
            db = app.get_db()
            snapshot = self._track_snapshot(db, track_id)
            self.assertIsNone(snapshot["owner_id"])
            count = db.execute("SELECT COUNT(*) AS n FROM events WHERE track_id = ?", (track_id,)).fetchone()["n"]
            self.assertEqual(count, 0)

    def test_deactivate_player_bulk_tracks(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        with app.app.app_context():
            db = app.get_db()
            for _ in range(3):
                _, tracks = self._create_test_cup(db, season_id, salim_id, sergio_id)
                for tid in tracks:
                    db.execute(
                        "UPDATE tracks SET owner_id = ?, state = 0, threatened_by_id = NULL WHERE id = ?",
                        (salim_id, tid),
                    )
            db.commit()
            before_owned = db.execute(
                "SELECT COUNT(*) AS n FROM tracks WHERE owner_id = ?",
                (salim_id,),
            ).fetchone()["n"]
            db.commit()
            self.assertTrue(app.deactivate_player(db, salim_id))
            remaining = db.execute(
                "SELECT COUNT(*) AS n FROM tracks WHERE owner_id = ?",
                (salim_id,),
            ).fetchone()["n"]
            self.assertEqual(remaining, 0)
            payload = json.loads(
                db.execute("SELECT side_effects_json FROM events ORDER BY id DESC LIMIT 1").fetchone()["side_effects_json"]
            )
            self.assertEqual(len(payload["tracks"]), before_owned)
            app.undo_last_event(db)
            restored = db.execute(
                "SELECT COUNT(*) AS n FROM tracks WHERE owner_id = ?",
                (salim_id,),
            ).fetchone()["n"]
            self.assertEqual(restored, before_owned)

    def test_deactivate_player_restores_at_risk(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["owned"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_id, sergio_id)
            self.assertTrue(app.deactivate_player(db, salim_id))
            snap = self._track_snapshot(db, track_id)
            self.assertIsNone(snap["owner_id"])
            self.assertEqual(snap["state"], 0)
            self.assertIsNone(snap["threatened_by_id"])
            app.undo_last_event(db)
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["state"], -1)
            self.assertEqual(snap["threatened_by_id"], sergio_id)

    def test_race_after_deactivation_claims_track(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        sergio_id = ctx["players"]["Sergio"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["owned"]
        with app.app.app_context():
            db = app.get_db()
            self.assertTrue(app.deactivate_player(db, salim_id))
            app.apply_result(db, season_id, track_id, sergio_id)
            snap = self._track_snapshot(db, track_id)
            self.assertEqual(snap["owner_id"], sergio_id)
            self.assertEqual(snap["state"], 0)

    def test_auto_season_clone_integrity(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        with app.app.app_context():
            db = app.get_db()
            future = date.today() + timedelta(days=120)
            season_row = app.create_season_for_today(db, future)
            new_id = season_row["id"]
            tracks = db.execute(
                "SELECT * FROM tracks WHERE season = ? ORDER BY cup_id, order_in_cup",
                (new_id,),
            ).fetchall()
            self.assertGreater(len(tracks), 0)
            for row in tracks:
                self.assertIsNone(row["owner_id"])
                self.assertEqual(row["state"], 0)
                self.assertIsNone(row["threatened_by_id"])
            originals = {
                (r["cup_id"], r["order_in_cup"], r["code"])
                for r in db.execute("SELECT cup_id, order_in_cup, code FROM tracks WHERE season != ?", (new_id,)).fetchall()
            }
            for row in tracks:
                self.assertIn((row["cup_id"], row["order_in_cup"], row["code"]), originals)

    def test_stats_page_empty_new_season(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        with app.app.app_context():
            db = app.get_db()
            future = date.today() + timedelta(days=200)
            season_row = app.create_season_for_today(db, future)
            new_id = season_row["id"]
        with captured_templates(app.app) as templates:
            resp = client.get(f"/stats?season={new_id}")
            self.assertEqual(resp.status_code, 200)
            _, context = templates[0]
            for ps in context["player_stats"]:
                self.assertEqual(ps["wins"], 0)
                self.assertEqual(ps["tracks_owned"], 0)

    def test_cross_season_isolation(self):
        client, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            app.apply_result(db, season_id, track_id, salim_id)
            future = date.today() + timedelta(days=260)
            second = app.create_season_for_today(db, future)
        with captured_templates(app.app) as templates:
            resp = client.get(f"/stats?season={second['id']}")
            self.assertEqual(resp.status_code, 200)
            _, context = templates[0]
            for ps in context["player_stats"]:
                self.assertEqual(ps["wins"], 0)
        resp = client.get(f"/events?season={second['id']}")
        self.assertIn("No events yet", resp.get_data(as_text=True))

    def test_win_streak_recomputes_after_undo(self):
        _, ctx, _ = self.bootstrap(seed_active_environment)
        salim_id = ctx["players"]["Salim"]
        season_id = ctx["season_id"]
        track_id = ctx["tracks"]["blank"]
        with app.app.app_context():
            db = app.get_db()
            for _ in range(5):
                app.apply_result(db, season_id, track_id, salim_id)
            streaks = app.compute_player_streaks(db, season_id)
            self.assertEqual(streaks[salim_id]["current_win_streak"], 5)
            app.undo_last_event(db)
            streaks = app.compute_player_streaks(db, season_id)
            self.assertEqual(streaks[salim_id]["current_win_streak"], 4)
            for _ in range(4):
                app.undo_last_event(db)
            streaks = app.compute_player_streaks(db, season_id)
            self.assertEqual(streaks.get(salim_id, {}).get("current_win_streak", 0), 0)

    def test_update_invalid_track_returns_404(self):
        client, _, _ = self.bootstrap(seed_active_environment)
        resp = client.get("/update/999999")
        self.assertEqual(resp.status_code, 404)

    def test_language_switch_persists_between_pages(self):
        client, _, _ = self.bootstrap(seed_active_environment)
        resp = client.get("/?lang=es", follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        resp = client.get("/stats")
        self.assertIn('class="active">ES', resp.get_data(as_text=True))
        with client.session_transaction() as sess:
            sess.clear()
        resp = client.get("/stats")
        self.assertIn("Season stats", resp.get_data(as_text=True))

    def test_translate_text_preserves_placeholders(self):
        with app.app.app_context():
            g.current_lang = "es"
            text = "Player {name} won {count} races"
            translated = app.translate_text(text)
            self.assertIn("{name}", translated)
            self.assertIn("{count}", translated)

    def test_archive_missing_csv_graceful(self):
        client, _, _ = self.bootstrap(seed_active_environment)
        original_exists = app.os.path.exists

        def fake_exists(path):
            if path.endswith("MK8TracksSeason1.csv"):
                return False
            return original_exists(path)

        with patch("app.os.path.exists", side_effect=fake_exists):
            resp = client.get("/archive")
            self.assertEqual(resp.status_code, 200)
            self.assertIn("Season archive", resp.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
