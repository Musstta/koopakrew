"""
Cache-aware AI profile generation with Gemini → Mistral fallback.

Profiles are per-player (not per-season). Pentagon/stats vary by season but
the AI narrative is a single evolving identity per player.

Two generation modes:
  - First-time (is_initialized=0): full all-time stats → identity paragraph
  - Update (is_initialized=1):     prior paragraph + updated stats → refined paragraph

The profile paragraph and session notes are intentionally separate:
  - historical_paragraph: WHO the player is — style, tendencies, arc. No specific events.
  - session_notes:        WHAT happened last session — tracks, momentum, key moments.

Staleness: regenerate when (current_event_count - events_count_at_generation) >= regen_threshold.
regen_threshold = max(1, round(avg_events_per_session * 0.85)).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash-lite:generateContent"
)
_MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"
_MISTRAL_MODEL = "mistral-small-latest"


# ── API callers ────────────────────────────────────────────────────────────────

def _call_gemini(api_key: str, prompt: str) -> str | None:
    try:
        import urllib.request
        body = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode()
        req = urllib.request.Request(
            f"{_GEMINI_URL}?key={api_key}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read())
        candidates = data.get("candidates") or []
        if not candidates:
            return None
        parts = (candidates[0].get("content") or {}).get("parts") or []
        return "".join(p.get("text", "") for p in parts).strip() or None
    except Exception:
        logger.exception("Gemini API call failed")
        return None


def _call_mistral(api_key: str, prompt: str) -> str | None:
    try:
        import urllib.request
        body = json.dumps({
            "model": _MISTRAL_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.75,
            "max_tokens": 350,
        }).encode()
        req = urllib.request.Request(
            _MISTRAL_URL,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read())
        choices = data.get("choices") or []
        if not choices:
            return None
        return ((choices[0].get("message") or {}).get("content") or "").strip() or None
    except Exception:
        logger.exception("Mistral API call failed")
        return None


def _call_llm(prompt: str, gemini_key: str | None, mistral_key: str | None) -> str | None:
    """Try Gemini first; fall back to Mistral if Gemini fails or is unconfigured."""
    if gemini_key:
        result = _call_gemini(gemini_key, prompt)
        if result:
            return result
        logger.warning("Gemini failed, falling back to Mistral")
    if mistral_key:
        return _call_mistral(mistral_key, prompt)
    return None


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _avg_events_per_session(db, player_id: int) -> float:
    rows = db.execute(
        """
        SELECT DATE(e.occurred_at) AS day, COUNT(*) AS cnt
        FROM events e
        WHERE e.is_sweep = 0 AND (e.winner_id = ? OR e.pre_owner_id = ?)
        GROUP BY DATE(e.occurred_at)
        """,
        (player_id, player_id),
    ).fetchall()
    if not rows:
        return 1.0
    return sum(r["cnt"] for r in rows) / len(rows)


def _current_event_count(db, player_id: int) -> int:
    row = db.execute(
        "SELECT COUNT(*) AS n FROM events e "
        "WHERE e.is_sweep = 0 AND (e.winner_id = ? OR e.pre_owner_id = ?)",
        (player_id, player_id),
    ).fetchone()
    return row["n"] if row else 0


def _latest_session_events(db, player_id: int) -> tuple[list[dict], str]:
    """Return ordered events for the most recent session date this player participated in."""
    date_row = db.execute(
        """
        SELECT DATE(e.occurred_at) AS day
        FROM events e
        WHERE e.is_sweep = 0 AND (e.winner_id = ? OR e.pre_owner_id = ?)
        ORDER BY e.occurred_at DESC, e.id DESC LIMIT 1
        """,
        (player_id, player_id),
    ).fetchone()
    if not date_row:
        return [], "unknown"
    day = date_row["day"]
    rows = db.execute(
        """
        SELECT e.id, e.winner_id, e.pre_owner_id, e.post_owner_id,
               e.pre_state, e.post_state,
               t.en AS track_en, t.es AS track_es,
               p_winner.name AS winner_name, p_pre.name AS pre_owner_name
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        LEFT JOIN players p_winner ON p_winner.id = e.winner_id
        LEFT JOIN players p_pre    ON p_pre.id    = e.pre_owner_id
        WHERE e.is_sweep = 0 AND DATE(e.occurred_at) = ?
          AND (e.winner_id = ? OR e.pre_owner_id = ?)
        ORDER BY e.occurred_at ASC, e.id ASC
        """,
        (day, player_id, player_id),
    ).fetchall()
    return [dict(r) for r in rows], day


def _format_session_events(events: list[dict], player_id: int, player_name: str) -> str:
    lines = []
    for i, e in enumerate(events, 1):
        track = e.get("track_en") or "unknown track"
        winner_id = e.get("winner_id")
        pre_owner_id = e.get("pre_owner_id")
        winner_name = e.get("winner_name") or "?"
        pre_owner_name = e.get("pre_owner_name") or "?"

        if winner_id == player_id:
            if pre_owner_id == player_id:
                action = f"Defended {track}"
            elif pre_owner_id:
                action = f"Won {track} (took from {pre_owner_name})"
            else:
                action = f"Won {track}"
        else:
            action = f"Lost {track} (taken by {winner_name})"
        lines.append(f"{i}. {action}")

    # notable runs
    outcomes = ["W" if e.get("winner_id") == player_id else "L" for e in events]
    notes = []
    max_w = max_l = cur_w = cur_l = 0
    for o in outcomes:
        if o == "W":
            cur_w += 1; cur_l = 0; max_w = max(max_w, cur_w)
        else:
            cur_l += 1; cur_w = 0; max_l = max(max_l, cur_l)
    if max_w >= 3:
        notes.append(f"{max_w}-win streak")
    if max_l >= 3:
        notes.append(f"{max_l}-loss streak")
    for i in range(len(events) - 1):
        a, b = events[i], events[i + 1]
        if a.get("track_en") == b.get("track_en") and a.get("winner_id") != b.get("winner_id"):
            notes.append(f"traded {a.get('track_en')} back and forth")
            break

    result = "\n".join(lines)
    if notes:
        result += "\nNotable: " + "; ".join(notes) + "."
    return result


# ── Prompts ────────────────────────────────────────────────────────────────────

def _build_init_prompt(
    player_name: str,
    current_season_stats: dict,
    current_season_label: str,
    all_time_stats: dict,
    season_labels: list[str],
    attribute_scores: dict,
) -> str:
    cs = current_season_stats
    at = all_time_stats
    attr = attribute_scores
    return (
        f"You are the narrator for a Mario Kart 8 championship tracker called Koopa Krew.\n"
        f"Write 2-3 punchy sentences capturing the competitive identity of {player_name!r}.\n"
        f"Lead with their current season energy and tendencies. "
        f"Use stats as inspiration for the vibe, not as numbers to quote back. "
        f"Focus on WHO they are — their style, instincts, what makes them dangerous or unpredictable. "
        f"Make it striking and vivid, like a scout's one-paragraph scouting report. "
        f"Do NOT list specific stats or recount events. Present tense. Plain text only — no markdown, no bold, no asterisks.\n\n"
        f"Current season ({current_season_label}):\n"
        f"  Wins: {cs.get('wins', 0)}  |  Win rate: {cs.get('win_rate') or 0:.1f}%\n"
        f"  Tracks owned: {cs.get('tracks_owned', 0)}  |  Locked: {cs.get('locked_tracks', 0)}  |  Cups: {cs.get('cups_owned_count', 0)}\n"
        f"  Tracks taken: {cs.get('tracks_taken', 0)}  |  Tracks lost: {cs.get('tracks_lost', 0)}\n"
        f"  Best win streak: {cs.get('best_win_streak', 0)}  |  Best defense streak: {cs.get('best_defense_streak', 0)}\n"
        f"  Steals from risk: {cs.get('steals_from_risk', 0)}  |  Risk wins: {cs.get('wins_on_risk', 0)}\n\n"
        f"All-time across {', '.join(season_labels) or 'career'}:\n"
        f"  Wins: {at.get('wins', 0)}  |  Win rate: {at.get('win_rate') or 0:.1f}%  |  "
        f"Best win streak ever: {at.get('best_win_streak', 0)}  |  Best defense streak ever: {at.get('best_defense_streak', 0)}\n\n"
        f"Attribute scores (0–100, where 100 = elite):\n"
        f"  Aggression {attr.get('aggression', 0):.0f}  Defense {attr.get('defense', 0):.0f}  "
        f"Dominance {attr.get('dominance', 0):.0f}  Consistency {attr.get('consistency', 0):.0f}  Clutch {attr.get('clutch', 0):.0f}\n\n"
        f"Competitive identity paragraph:"
    )


def _build_update_prompt(
    player_name: str,
    prior_paragraph: str,
    current_season_stats: dict,
    current_season_label: str,
    all_time_stats: dict,
    attribute_scores: dict,
) -> str:
    cs = current_season_stats
    at = all_time_stats
    attr = attribute_scores
    return (
        f"You are the narrator for a Mario Kart 8 championship tracker called Koopa Krew.\n"
        f"Below is {player_name!r}'s current competitive identity paragraph. "
        f"Revise it to 2-3 punchy sentences reflecting their current form. "
        f"Lead with this season's energy; use all-time stats only to inform the arc. "
        f"Use stats as inspiration for the vibe, not numbers to quote back. "
        f"Focus on WHO they are — only shift the identity if the stats clearly support it. "
        f"Present tense. Plain text only — no markdown, no bold, no asterisks.\n\n"
        f"Current profile:\n{prior_paragraph}\n\n"
        f"Current season ({current_season_label}):\n"
        f"  Wins: {cs.get('wins', 0)}  |  Win rate: {cs.get('win_rate') or 0:.1f}%\n"
        f"  Tracks taken: {cs.get('tracks_taken', 0)}  |  Tracks lost: {cs.get('tracks_lost', 0)}\n"
        f"  Best win streak: {cs.get('best_win_streak', 0)}  |  Steals from risk: {cs.get('steals_from_risk', 0)}\n\n"
        f"All-time: Wins {at.get('wins', 0)}  Win rate {at.get('win_rate') or 0:.1f}%  "
        f"Best streak ever {at.get('best_win_streak', 0)}\n\n"
        f"Attributes: Aggression {attr.get('aggression', 0):.0f}  Defense {attr.get('defense', 0):.0f}  "
        f"Dominance {attr.get('dominance', 0):.0f}  Consistency {attr.get('consistency', 0):.0f}  Clutch {attr.get('clutch', 0):.0f}\n\n"
        f"Revised paragraph:"
    )


def _build_session_notes_prompt(
    player_name: str,
    session_text: str,
    session_date: str,
) -> str:
    return (
        f"You are the narrator for a Mario Kart 8 championship tracker called Koopa Krew.\n"
        f"Write 2-3 sentences capturing the story of {player_name!r}'s latest session.\n"
        f"Focus on the arc and feel of it — how they started, how momentum shifted, how it ended. "
        f"Highlight patterns and turning points rather than listing individual tracks. "
        f"Write like a sports analyst painting a picture, not a play-by-play commentator. "
        f"Plain text only — no markdown, no bold, no asterisks. Present tense.\n\n"
        f"Session data ({session_date}):\n{session_text}\n\n"
        f"Session story:"
    )


# ── Core public function ───────────────────────────────────────────────────────

def get_or_generate_profile(
    db,
    player_id: int,
    player_name: str,
    current_season_stats: dict,
    current_season_label: str,
    all_time_stats: dict,
    season_labels: list[str],
    attribute_scores: dict,
    gemini_key: str | None = None,
    mistral_key: str | None = None,
    inter_request_delay: float = 0.0,
) -> dict:
    """
    Fetch cached profile or generate (and cache) a new one.
    Tries Gemini first, falls back to Groq.

    Returns: {"historical_paragraph": str, "session_notes": str, "from_cache": bool, "generated": bool}
    """
    row = db.execute(
        "SELECT * FROM player_profiles WHERE player_id = ?", (player_id,)
    ).fetchone()

    current_count = _current_event_count(db, player_id)

    needs_regen = row is None
    if not needs_regen:
        stored_count = row["events_count_at_generation"]
        threshold = row["regen_threshold"]
        if (current_count - stored_count) >= threshold:
            needs_regen = True

    if not needs_regen:
        return {
            "historical_paragraph": (row["historical_paragraph"] or "") if row else "",
            "session_notes": (row["session_notes"] or "") if row else "",
            "from_cache": True,
            "generated": False,
        }

    if not gemini_key and not mistral_key:
        cached_para = (row["historical_paragraph"] or "") if row else ""
        placeholder = cached_para or (
            f"{player_name} is an active competitor in the Koopa Krew championship. "
            "Configure GEMINI_API_KEY or MISTRAL_API_KEY to generate a personalised profile."
        )
        return {
            "historical_paragraph": placeholder,
            "session_notes": (row["session_notes"] or "") if row else "",
            "from_cache": False,
            "generated": False,
        }

    is_initialized = row["is_initialized"] if row else 0
    prior_paragraph = (row["historical_paragraph"] or "") if row else ""

    if not is_initialized or not prior_paragraph:
        prompt = _build_init_prompt(
            player_name, current_season_stats, current_season_label,
            all_time_stats, season_labels, attribute_scores,
        )
    else:
        prompt = _build_update_prompt(
            player_name, prior_paragraph, current_season_stats,
            current_season_label, all_time_stats, attribute_scores,
        )

    historical_paragraph = _call_llm(prompt, gemini_key, mistral_key)

    if not historical_paragraph:
        return {
            "historical_paragraph": prior_paragraph or f"{player_name} — profile generation failed, will retry.",
            "session_notes": (row["session_notes"] or "") if row else "",
            "from_cache": False,
            "generated": False,
        }

    if inter_request_delay > 0:
        import time as _time
        _time.sleep(inter_request_delay)

    session_events, session_date = _latest_session_events(db, player_id)
    session_text = _format_session_events(session_events, player_id, player_name)
    notes_prompt = _build_session_notes_prompt(player_name, session_text, session_date)
    session_notes = _call_llm(notes_prompt, gemini_key, mistral_key) or ""

    prior_notes = (row["session_notes"] or "") if row else ""
    session_notes_to_store = session_notes if session_notes.strip() else prior_notes

    threshold = _compute_regen_threshold(db, player_id)
    _upsert_profile(
        db, player_id, current_count, threshold,
        is_initialized=1,
        historical_paragraph=historical_paragraph,
        session_notes=session_notes_to_store,
    )

    return {
        "historical_paragraph": historical_paragraph,
        "session_notes": session_notes_to_store,
        "from_cache": False,
        "generated": True,
    }


def _compute_regen_threshold(db, player_id: int) -> int:
    avg = _avg_events_per_session(db, player_id)
    return max(1, round(avg * 0.85))


def _upsert_profile(
    db,
    player_id: int,
    events_count: int,
    regen_threshold: int,
    is_initialized: int,
    historical_paragraph: str,
    session_notes: str,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        """
        INSERT INTO player_profiles
            (player_id, events_count_at_generation, regen_threshold,
             is_initialized, historical_paragraph, session_notes, generated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id) DO UPDATE SET
            events_count_at_generation = excluded.events_count_at_generation,
            regen_threshold            = excluded.regen_threshold,
            is_initialized             = excluded.is_initialized,
            historical_paragraph       = excluded.historical_paragraph,
            session_notes              = excluded.session_notes,
            generated_at               = excluded.generated_at
        """,
        (player_id, events_count, regen_threshold,
         is_initialized, historical_paragraph, session_notes, now),
    )
    db.commit()


__all__ = ["get_or_generate_profile"]
