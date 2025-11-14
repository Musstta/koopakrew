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

## Want to Explore?
- Visit `/rules` inside the app for an always-current rendering of these rules.
- Use the Control Center to jump between Standings, Events, Stats, Archive, Players, and Rules.
- Open `/archive` to browse past seasons, see final standings, and relive the highlights.

That’s it! Fire up the tracker, record your races, and keep bragging rights honest.
