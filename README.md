# Koopa Krew Tracker

![Koopa Krew logo](static/images/KoopaKrew.png)

Koopa Krew is the scorekeeper for our ongoing Mario Kart 8 championship. It preserves the tournament’s rules, state machine, and lore so every race result is transparent and undoable. This document focuses on how the league operates rather than how to install the code.

## Tournament Overview
- **Season cadence:** We run quarterly seasons (e.g., 2025 Q4). When a new season begins, the previous track list is cloned but every track starts unclaimed so the initial “repartición” can happen.
- **Players:** Only active racers appear in standings. Deactivating a player instantly releases their tracks back to neutral, unowned Default state but the system saves the snapshot so the action can be undone.
- **Tracks & cups:** Every cup contains exactly four tracks. Each track may be Default, At Risk, or Locked; transitions occur strictly according to race results, and wins only count when at least three racers are present (aforo rule).
- **Events:** Every interaction—race result, sweep, or player deactivation—is stored as an event containing the full pre/post snapshot so we can undo anything.
- **Archives:** Past seasons are read-only. Season 1 (2025 Q3) ships as a CSV snapshot, while Season 2+ are rendered live from SQLite.

## Rule Highlights
1. **State Machine**
   - Ownerless track → first winner claims it (Default).
   - Default owner wins → track locks.
   - Default owner loses → challenger puts it At Risk (hunter tag).
   - At Risk owner defends → back to Default.
   - At Risk owner loses → challenger becomes owner (Default).
   - Locked owner loses → lock breaks but owner remains.
2. **Sweeps**
   - Owning all four tracks in a cup triggers a sweep: each track locks and a sweep event is written. Undoing the sweep removes the locks and reverts the triggering race.
3. **Deactivation Events**
   - Admin “toggle” now logs a synthetic event that captures all tracks owned by the player. Undoing the event reactivates the racer and restores every track exactly as it was.
4. **Undo Stack**
   - Undo always targets the newest event. Race events, sweeps, and deactivations can all be reversed safely because their pre/post snapshots are persisted.
5. **Stats and Streaks**
   - Statistics (wins, defenses, risk plays) recompute from the event table. “Hot Hand” and “Shield Wall” badges activate at streak ≥ 2 and display the actual streak length.
6. **Control Center UX**
   - The floating drawer holds navigation, filters, CSV exports, online presence info, and now a Rules entry that mirrors this README.

## UI Extras
- **Quick search** on the standings page lets you filter tracks/cups live (English or Spanish names).
- **Season selector** on the Stats page supports both per-season analysis and all-time aggregation (track insights are disabled in all-season mode to prevent confusion).
- **Seasonal branding** swaps the hero logo for patriotic/spooky/holiday art automatically.

## Project Layout
- `koopakrew/` — Flask blueprints, config, and services (the actual app code).
- `app.py` / `run.py` — Dev and production entry points (use `run.py` for Gunicorn).
- `scripts/` — Helper utilities like the asset checker and DB seeder.
- `instance/` — Ignored runtime folder where SQLite files live (dev uses `instance/dev.sqlite`).
- `archive/` — Long-term storage for legacy files such as `archive/snapshots/season1_tracks.csv` and the retired `archive/legacy-data/koopakrew.db`.

## Tests & Reliability
We maintain a growing suite of unit tests in `tests/test_app.py` which cover:
- Season bootstrap and standings filters.
- Core state-machine transitions, drop/undo flows, and sweeps.
- Player deactivation synthetic events and undo recovery.
- Stats aggregation (per-season and all-time) and archive rendering.
- Admin workflows (add/toggle, default-player shortcuts, presence).

Run the suite with:
```bash
.venv/bin/python -m unittest tests.test_app
```

## Running Locally
Development settings live in `.env.development`. The factory loads that file automatically when `KOOPAKREW_CONFIG=development`, so you can keep secrets there without touching production values. The dev database is created at `instance/dev.sqlite`, separate from any production data and the legacy archives.

1. Create a virtualenv and install dependencies (for example `python -m venv .venv && .venv/bin/pip install -r requirements.txt`).
2. Copy or edit `.env.development` if you need custom overrides (tz, secret, etc.).
3. For a private dev server run `./run.sh` (binds to `127.0.0.1:5001`, seeds `instance/dev.sqlite`, and reloads on changes). For a production-style preview run `./serve-prod.sh` (binds to `0.0.0.0:5000`, uses `instance/koopakrew.sqlite`, and matches what `kart.musstta.cc` will expose through your Cloudflare tunnel).

### Code Quality & Hooks
- Format and lint with `black .` and `ruff check .` (configured via `pyproject.toml`).
- Install the pre-commit hooks once per clone: `pip install pre-commit && pre-commit install`. They’ll run Black and Ruff before each commit.

## Deploying
Production runs with a different SQLite file (`instance/koopakrew.sqlite`) and requires a real secret key. Use `.env.production.example` as a template:

1. Copy it to `.env.production`, set `KOOPAKREW_SECRET`, and adjust timezone/DB filename if needed.
2. Point your WSGI server at `run.py` (e.g., `gunicorn run:app -b 127.0.0.1:5000`). The helper builds an app using `ProductionConfig`, so debug-only extras stay disabled. When testing locally you can also run `./serve-prod.sh` which wraps the same entry point.
3. Keep the instance folder writable so the production database and config can live alongside the code.

### Local dev → prod workflow
- **Code safely** with `./run.sh`: port `5001`, dev config, `instance/dev.sqlite`, host `127.0.0.1` (not exposed—even if the tunnel is running, it forwards port `5000` so your work stays private).
- **Review as prod** with `./serve-prod.sh`: port `5000`, production config, `instance/koopakrew.sqlite`. Start your Cloudflare tunnel only when you want to share `kart.musstta.cc`.
- **Commit & merge**: work on a feature branch, `git add/commit`, push, and open a PR against `main`. After merge, pull on the machine that backs the tunnel and restart the prod process (Gunicorn/systemd) so port `5000` serves the latest code.
- **Clean shutdowns**: stop whichever script you’re not using so only one binds to a port at a time. Dev and prod databases stay isolated, so you can experiment freely without touching production data.

## Module Map & Diagram
- `app.py` wires HTTP routes, renders templates, and coordinates services.
- `koopakrew/services/core.py` encapsulates state-machine logic, stats, and undo handling.
- `koopakrew/infra/presence.py` tracks online players via session tokens.
- `koopakrew/blueprints/main.py` registers Flask routes while keeping legacy entry points alive.
- `koopakrew/config.py` holds environment-specific settings and defaults.

```
             +------------------+
             |   HTTP Layer     |
             | (Blueprints)     |
             +--------+---------+
                      |
                      v
+---------+   +---------------+   +--------------------+
| Templates|<-| app.py helpers |-> | Services (core.py) |
+---------+   +-------+-------+   +----+---------------+
                      |                |
                      v                v
               +------+-------+   +-----------+
               |Presence/Infra|   | SQLite DB |
               +--------------+   +-----------+
```

## Want to Explore?
- Visit `/rules` inside the app for an always-current rendering of these rules.
- Use the Control Center to jump between Standings, Events, Stats, Archive, Players, and Rules.
- Open `/archive` to browse past seasons, see final standings, and relive the highlights.

That’s it! Fire up the tracker, record your races, and keep bragging rights honest.
