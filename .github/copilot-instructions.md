# Koopa Krew Tracker – AI Agent Instructions

## Architecture Overview

**Single-file Flask app** (`app.py`, ~2300 lines) managing a Mario Kart 8 tournament with seasonal track ownership, state-machine transitions, and full event auditing. SQLite backend with manual schema in `db_init.py`.

### Core Components
- **State Machine**: Tracks transition between Default (0), At Risk (-1), and Locked (1) based on race outcomes
- **Event System**: All changes (races, sweeps, player deactivations) logged with pre/post snapshots for undo
- **Season Management**: Quarterly seasons auto-created; tracks cloned from previous season but start unowned
- **Bilingual I18n**: English default; Spanish via `i18n.py` dictionary + Google Translate fallback with placeholder protection

## Critical Workflows

### Database Initialization
```bash
# Bootstrap/reseed database
./run.sh  # Creates venv, installs deps, seeds from CSV if DB missing
# Force reseed: KOOPAKREW_FORCE_SEED=1 ./run.sh
# Seeder: db_init.py reads MK8TracksS2.csv (or KOOPAKREW_S2_CSV)
```

### Running Tests
```bash
.venv/bin/python -m unittest tests.test_app
# Tests cover: state transitions, undo, sweeps, player deactivation, stats aggregation
```

### Dev Server
```bash
source .venv/bin/activate
python app.py  # Flask dev server on port 5000
```

## Project-Specific Conventions

### State Machine Logic (`apply_result` in `app.py:1316`)
- **Ownerless track** → first winner claims (Default)
- **Owner wins Default** → locks (Locked)
- **Owner loses Default** → At Risk with hunter mark
- **Owner defends At Risk** → back to Default
- **Owner loses At Risk** → challenger becomes owner (Default)
- **Owner loses Locked** → breaks to Default but owner remains

### Event-Driven Undo (`undo_last_event` at `app.py:1456`)
- Every event stores `pre_*` and `post_*` snapshots
- Undo reverses newest event (races, sweeps, deactivations)
- Side effects (sweep locks, player deactivation) stored in `side_effects_json`

### Sweep Detection (`apply_result` sweep logic)
- After race, count tracks owned in cup
- If crossing from <4 to 4 tracks → lock all 4, create separate sweep event

### Player Deactivation (`deactivate_player` at `app.py:492`)
- Creates synthetic event with `action: "deactivate_player"` in JSON
- Releases all tracks to unowned state
- Undo restores player and all tracks exactly

### I18n Pattern
- `translate_text()` checks `SPANISH_TRANSLATIONS` dict first
- Falls back to Google Translate with placeholder regex protection (`{...}` preserved)
- Template helper: `_("text")` function injected via `inject_i18n()`

## Key Files & Patterns

### Database Schema (`db_init.py:22-104`)
- `players`, `season_meta`, `cups`, `tracks`, `events` tables
- Foreign keys enforced; tracks have CHECK constraints for state/threatened_by consistency
- `tracks.season` links to `season_meta.id`

### Routes Structure
- **`/`**: Standings with cup/state/owner filters (CSV export available)
- **`/update/<track_id>`**: Record race result; quick-win shortcut if default player set
- **`/undo`**: POST-only undo last event
- **`/events`**: Event log with filters (player, event type)
- **`/stats`**: Per-season or all-time stats; track insights only for single-season view
- **`/archive`**: Read-only past seasons (Season 1 from CSV, Season 2+ from DB)
- **`/admin/players`**: Add/toggle players, set default player
- **`/presence/ping`**: Real-time online presence tracking

### Templates (`templates/`)
- **`base.html`**: Control Center drawer (nav, filters, exports, presence)
- **`index.html`**: Standings with quick search (JS client-side filtering)
- **`stats.html`**: Player stats + track insights with season selector
- **`events.html`**: Event timeline with undo capability
- **`admin_players.html`**: Player management + default player setting

### Stats Computation (`compute_stats_data` at `app.py:515`)
- Aggregates wins, defenses, steals, streaks from events table
- Supports single-season or multi-season (all-time) queries
- Track insights disabled when viewing all seasons to avoid confusion

### Testing Patterns (`tests/test_app.py`)
- `bootstrap()` helper creates temp DB with test data
- `seed_template_season()` creates archived season
- `seed_active_environment()` creates current season scenario
- Use Flask test client: `self.client.post("/update/...", data={...})`

## Environment Variables
```bash
KOOPAKREW_DB=koopakrew.db           # Database path
KOOPAKREW_S2_CSV=MK8TracksS2.csv    # CSV seed file
KOOPAKREW_TZ=America/Costa_Rica      # Timezone for timestamps
KOOPAKREW_SECRET=change-me-in-prod   # Flask secret key
KOOPAKREW_SEASON_LABEL="Season 2 — 2025 Q4"
KOOPAKREW_SEASON_ID=2
KOOPAKREW_SEASON_START=2025-10-01
KOOPAKREW_SEASON_END=2026-01-01
KOOPAKREW_PLAYERS=Salim,Sergio,Fabian,Sebas
```

## Common Gotchas
- **State transitions**: Always use `apply_result()`; manual track updates break event log
- **Undo safety**: Only newest event can be undone; check `side_effects_json` structure
- **Season auto-creation**: Fails if no template tracks exist; seed database first
- **I18n placeholders**: Use `{key}` format; regex protection prevents Google Translate corruption
- **Sweep logic**: Cup sweep happens immediately after 4th track claimed; creates two events (race + sweep)
- **Track insights**: Only available in single-season stats view, not all-time
- **CSV columns**: Seeder expects `track_code,cup_code,cup_en,cup_es,track_en,track_es,owner,state,threat`
