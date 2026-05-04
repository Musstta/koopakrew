import secrets
import time
from typing import Any

from flask import session


class PresenceService:
    def __init__(self, timeout_seconds: int, fresh_seconds: int, warming_seconds: int):
        self.timeout_seconds = timeout_seconds
        self.fresh_seconds = fresh_seconds
        self.warming_seconds = warming_seconds
        self._pings: dict[str, dict[str, Any]] = {}

    def _purge(self, now: float | None = None):
        now = now or time.time()
        stale = [
            token
            for token, payload in self._pings.items()
            if now - payload.get("last_seen", 0) > self.timeout_seconds
        ]
        for token in stale:
            self._pings.pop(token, None)

    def disconnect(self):
        token = session.get("presence_token")
        if not token:
            return
        self._pings.pop(token, None)
        session.pop("presence_token", None)

    def ping(self, player_id: int, *, now: float | None = None) -> bool:
        if not player_id:
            self.disconnect()
            return False
        token = session.get("presence_token")
        if not token:
            token = secrets.token_hex(16)
            session["presence_token"] = token
        stamp = now or time.time()
        self._pings[token] = {"player_id": player_id, "last_seen": stamp}
        self._purge(stamp)
        return True

    def online_players(self, *, now: float | None = None):
        self._purge(now)
        last_seen_by_player: dict[int, float] = {}
        for payload in self._pings.values():
            pid = payload.get("player_id")
            if not pid:
                continue
            stamp = payload.get("last_seen", 0.0)
            prev = last_seen_by_player.get(pid)
            if prev is None or stamp > prev:
                last_seen_by_player[pid] = stamp
        ordered = sorted(last_seen_by_player.items(), key=lambda item: (-item[1], item[0]))
        return ordered

    def presence_states(self, *, now: float | None = None):
        now_val = now or time.time()
        states = []
        for pid, stamp in self.online_players(now=now_val):
            age = now_val - stamp
            if age >= self.timeout_seconds:
                continue
            if age < self.fresh_seconds:
                status = "fresh"
            elif age < self.warming_seconds:
                status = "warming"
            else:
                status = "cooling"
            states.append({"player_id": pid, "status": status, "last_seen": stamp})
        return states


__all__ = ["PresenceService"]
