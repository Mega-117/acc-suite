"""Summary generation from NDJSON events."""

from __future__ import annotations

import os
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from statistics import mean
from typing import Dict, Iterable, List, Optional

from .utils import fmt_hms, parse_iso, r1

FUEL_QUALY_THRESHOLD = 20.0  # >20 race, <=20 qualy


@dataclass
class LapData:
    lap: Optional[int]
    lap_ms: Optional[int]
    is_valid: Optional[bool]
    fuel_start: Optional[float]
    fuel_end: Optional[float]
    sectors_ms: List[int] = field(default_factory=list)


@dataclass
class StintData:
    start: Optional[str] = None
    end: Optional[str] = None
    fuel_start: Optional[float] = None
    fuel_end: Optional[float] = None
    classification: str = "unknown"
    laps: List[LapData] = field(default_factory=list)
    car_model: Optional[str] = None
    weather_air_start: Optional[float] = None
    weather_grip_start: Optional[str] = None

    def add_lap(self, lap: LapData) -> None:
        self.laps.append(lap)

    @property
    def valid_laps(self) -> int:
        return sum(1 for lap in self.laps if lap.is_valid)

    @property
    def invalid_laps(self) -> int:
        return sum(1 for lap in self.laps if lap.is_valid is False)

    @property
    def best_lap_ms(self) -> Optional[int]:
        valid = [lap.lap_ms for lap in self.laps if lap.is_valid and lap.lap_ms]
        if not valid:
            return None
        return min(valid)

    @property
    def average_lap_ms(self) -> Optional[int]:
        valid = [lap.lap_ms for lap in self.laps if lap.lap_ms]
        if not valid:
            return None
        return int(mean(valid))


@dataclass
class SessionData:
    session_type: Optional[str] = None
    track: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None
    car_model: Optional[str] = None
    stints: List[StintData] = field(default_factory=list)
    weather_air: List[float] = field(default_factory=list)
    weather_grip: List[str] = field(default_factory=list)
    bop_ballast: Optional[float] = None
    bop_restrictor: Optional[float] = None

    def active_stint(self) -> Optional[StintData]:
        return self.stints[-1] if self.stints else None


def load_events(path: str) -> Iterable[Dict[str, object]]:
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


class SummaryBuilder:
    """Builds session summaries from NDJSON events."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.sessions: List[SessionData] = []
        self._sector_cache: Dict[int, List[int]] = defaultdict(list)
        self._last_ts: Optional[str] = None
        self._last_weather: Dict[str, Optional[object]] = {"air": None, "grip": None}

    # ------------------------------------------------------------------
    def build(self) -> Dict[str, object]:
        self._parse()
        session_blocks = [self._session_block(session) for session in self.sessions]
        stints = self._flatten_stints()
        return {
            "file": os.path.basename(self.path),
            "generated_at": None,
            "session": session_blocks,
            "stints": stints,
        }

    # ------------------------------------------------------------------
    def _parse(self) -> None:
        current: Optional[SessionData] = None

        for event in load_events(self.path):
            ts = event.get("ts")
            etype = event.get("event")
            if ts:
                self._last_ts = ts

            if etype == "session_start":
                if current:
                    self._finalise_session(current, ts)
                current = SessionData(
                    session_type=str(event.get("session_type")) if event.get("session_type") else None,
                    track=event.get("track"),
                    start=ts,
                    car_model=event.get("car_model"),
                )
                self.sessions.append(current)
                self._sector_cache.clear()
                self._last_weather = {"air": None, "grip": None}
                continue

            if current is None:
                continue

            if etype == "session_end":
                current.end = ts
                self._close_open_stint(current, ts)
                current = None
                self._sector_cache.clear()
                continue

            if etype == "stint_start":
                stint = StintData(
                    start=ts,
                    fuel_start=r1(event.get("fuel_start")),
                )
                stint.classification = (
                    "race" if (stint.fuel_start is not None and float(stint.fuel_start) > FUEL_QUALY_THRESHOLD) else "qualy"
                )
                stint.car_model = event.get("car_model") or current.car_model
                stint.weather_air_start = (
                    float(self._last_weather["air"])
                    if self._last_weather.get("air") is not None
                    else None
                )
                stint.weather_grip_start = self._normalise_grip(self._last_weather.get("grip"))
                current.stints.append(stint)
                if event.get("car_model"):
                    current.car_model = event.get("car_model")
                continue

            if etype == "stint_end":
                stint = current.active_stint()
                if stint:
                    stint.end = ts
                    stint.fuel_end = r1(event.get("fuel_end"))
                continue

            if etype == "lap_complete":
                stint = current.active_stint()
                if stint is None:
                    stint = StintData(start=ts)
                    current.stints.append(stint)
                lap_number = event.get("lap")
                if lap_number is not None:
                    try:
                        lap_number_int = int(lap_number)
                    except (TypeError, ValueError):
                        lap_number_int = None
                else:
                    lap_number_int = None

                sectors = self._sector_cache.pop(lap_number_int, []) if lap_number_int is not None else []
                lap_ms_val = event.get("lap_ms")
                if lap_ms_val is not None:
                    try:
                        lap_ms_val = int(lap_ms_val)
                    except (TypeError, ValueError):
                        lap_ms_val = None
                lap = LapData(
                    lap=lap_number_int,
                    lap_ms=lap_ms_val,
                    is_valid=bool(event.get("is_valid")) if event.get("is_valid") is not None else None,
                    fuel_start=r1(event.get("fuel_start")),
                    fuel_end=r1(event.get("fuel_end")),
                    sectors_ms=sectors,
                )
                stint.add_lap(lap)
                continue

            if etype == "sector_split":
                lap = event.get("lap")
                split_ms = event.get("split_ms")
                if lap is not None and split_ms is not None:
                    try:
                        lap_idx = int(lap)
                        split_val = int(split_ms)
                    except (TypeError, ValueError):
                        continue
                    self._sector_cache[lap_idx].append(split_val)
                continue

            if etype == "wx":
                air = event.get("air_temp")
                grip = event.get("grip")
                if isinstance(air, (int, float)):
                    current.weather_air.append(float(air))
                    self._last_weather["air"] = float(air)
                if grip is not None:
                    grip_norm = self._normalise_grip(grip)
                    if grip_norm:
                        current.weather_grip.append(grip_norm)
                    self._last_weather["grip"] = grip_norm
                if event.get("ballast") is not None:
                    current.bop_ballast = event.get("ballast")
                if event.get("restrictor") is not None:
                    current.bop_restrictor = event.get("restrictor")
                if event.get("car_model"):
                    current.car_model = event.get("car_model")
                active = current.active_stint()
                if active:
                    if active.weather_air_start is None and isinstance(air, (int, float)):
                        active.weather_air_start = float(air)
                    if active.weather_grip_start is None and grip is not None:
                        active.weather_grip_start = self._normalise_grip(grip)
                continue

        if current:
                self._finalise_session(current, None)

    # ------------------------------------------------------------------
    def _close_open_stint(self, session: SessionData, ts: Optional[str]) -> None:
        fallback = ts or self._last_ts
        for stint in session.stints:
            if stint.end is None:
                stint.end = fallback

    # ------------------------------------------------------------------
    def _finalise_session(self, session: SessionData, ts: Optional[str]) -> None:
        if session.end is None:
            session.end = ts or self._last_ts
        self._close_open_stint(session, ts)

    # ------------------------------------------------------------------
    def _session_block(self, session: SessionData) -> Dict[str, object]:
        start_dt = parse_iso(session.start)
        end_dt = parse_iso(session.end)
        duration_min = None
        if start_dt and end_dt:
            duration_min = round((end_dt - start_dt).total_seconds() / 60.0, 3)

        drive_seconds = 0.0
        for stint in session.stints:
            s0 = parse_iso(stint.start)
            s1 = parse_iso(stint.end)
            if s0 and s1:
                drive_seconds += max(0.0, (s1 - s0).total_seconds())

        total_closed = sum(1 for stint in session.stints for lap in stint.laps if lap.lap_ms)
        total_valid = sum(1 for stint in session.stints for lap in stint.laps if lap.is_valid)
        total_invalid = sum(1 for stint in session.stints for lap in stint.laps if lap.is_valid is False)

        bests_internal: Dict[str, Optional[Dict[str, object]]] = {"race": None, "qualy": None}
        for stint in session.stints:
            best_ms = stint.best_lap_ms
            if best_ms is None:
                continue
            cls = stint.classification
            if cls not in bests_internal:
                continue
            current_best = bests_internal.get(cls)
            best_ms_threshold = (
                current_best["lap_time_ms"] if isinstance(current_best, dict) else None
            )
            if best_ms_threshold is None or best_ms < best_ms_threshold:
                bests_internal[cls] = {
                    "lap_time_ms": best_ms,
                    "lap_time_s": round(best_ms / 1000.0, 3),
                    "lap_time_hms": fmt_hms(best_ms),
                    "fuel_pitout_l": stint.fuel_start,
                }

        bests: Dict[str, Optional[Dict[str, object]]] = {}
        for key in ("race", "qualy"):
            entry = bests_internal.get(key)
            if entry is None:
                bests[key] = None
            else:
                bests[key] = {
                    "lap_time_s": entry["lap_time_s"],
                    "lap_time_hms": entry["lap_time_hms"],
                    "fuel_pitout_l": entry.get("fuel_pitout_l"),
                }

        air_avg = round(mean(session.weather_air), 1) if session.weather_air else None
        grip_mode = None
        if session.weather_grip:
            grip_mode = Counter(session.weather_grip).most_common(1)[0][0]

        sess_type = self._normalise_session_type(session.session_type)

        laps_payload = {"total_closed": total_closed}
        laps_payload.update({"valid": total_valid, "invalid": total_invalid})

        weather_block = {}
        if air_avg is not None:
            weather_block["air_temp_c"] = {"avg": air_avg}

        grip_block = {}
        if grip_mode:
            grip_block["most_frequent"] = grip_mode

        return {
            "type": sess_type,
            "track": session.track,
            "time": {
                "start_local": session.start,
                "end_local": session.end,
                "duration_min": duration_min,
                "driving_duration_min": round(drive_seconds / 60.0, 3),
            },
            "grip": grip_block if grip_block else None,
            "weather": weather_block if weather_block else None,
            "laps": laps_payload,
            "bests": bests,
        }

    # ------------------------------------------------------------------
    def _flatten_stints(self) -> List[Dict[str, object]]:
        flat: List[Dict[str, object]] = []
        index = 1
        for session in self.sessions:
            for stint in session.stints:
                flat.append(self._stint_summary(stint, session, index))
                index += 1
        return flat

    # ------------------------------------------------------------------
    def _stint_summary(self, stint: StintData, session: SessionData, index: int) -> Dict[str, object]:
        avg_ms = stint.average_lap_ms
        best_ms = stint.best_lap_ms

        start_dt = parse_iso(stint.start)
        end_dt = parse_iso(stint.end)
        duration_min = None
        if start_dt and end_dt:
            duration_min = round(max(0.0, (end_dt - start_dt).total_seconds()) / 60.0, 3)

        grip = self._normalise_grip(stint.weather_grip_start)

        return {
            "index": index,
            "stint_type": stint.classification.upper() if stint.classification else None,
            "car_model": stint.car_model or session.car_model,
            "time_start_local": stint.start,
            "time_end_local": stint.end,
            "driving_duration_min": duration_min,
            "weather_start": {
                "air": stint.weather_air_start,
                "grip": grip,
            },
            "fuel_pitout": stint.fuel_start,
            "laps_total": len(stint.laps),
            "laps_valid": stint.valid_laps,
            "laps_invalid": stint.invalid_laps,
            "avg": fmt_hms(avg_ms) if avg_ms is not None else None,
            "lap_time_s": round(best_ms / 1000.0, 3) if best_ms is not None else None,
            "best_lap_time": fmt_hms(best_ms) if best_ms is not None else None,
        }

    # ------------------------------------------------------------------
    def _normalise_grip(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        grip = str(value).strip()
        if not grip:
            return None
        grip = grip.upper().replace(" ", "_")
        if not grip.startswith("ACC_"):
            grip = f"ACC_{grip}"
        return grip

    # ------------------------------------------------------------------
    def _normalise_session_type(self, session_type: Optional[str]) -> Optional[str]:
        if not session_type:
            return None
        sess = str(session_type).strip().upper()
        if not sess:
            return None
        if not sess.startswith("ACC_"):
            sess = f"ACC_{sess}"
        return sess


__all__ = ["SummaryBuilder", "FUEL_QUALY_THRESHOLD"]
