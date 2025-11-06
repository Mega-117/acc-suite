"""ACC shared memory logger."""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional

from .files import LogFileManager, LogPaths
from .utils import as_int_ms, clean_str, iso_now, monotonic, r1, write_line


class ACCLogger:
    def __init__(
        self,
        file_manager: LogFileManager,
        hz: float,
        wx_every: float,
        hb_every: float,
        wx_delta: float,
    ) -> None:
        self.file_manager = file_manager
        self.hz = max(0.1, hz)
        self.wx_every = max(0.0, wx_every)
        self.hb_every = max(0.0, hb_every)
        self.wx_delta = wx_delta

        self.asm = None
        self.paths: Optional[LogPaths] = None
        self.fh = None

        self.prev_session_key = None
        self.have_session_open = False
        self.prev_in_pit_eff = None
        self.prev_sector_idx = None
        self.prev_completed = None
        self.lap_valid_acc = None
        self.lap_start_fuel = None

        self.car_model = None
        self.ballast = None
        self.restrictor = None

        self.last_wx_emit = 0.0
        self.last_hb_emit = 0.0
        self.last_wx_vals = {
            "air": None,
            "track": None,
            "grip": None,
            "rain": None,
            "clouds": None,
            "wind_spd": None,
            "wind_dir": None,
        }

        self.stop_flag = False
        self.event_count = 0
        self.lock = threading.Lock()
        self.on_rotate: Optional[Callable[[LogPaths], None]] = None

    # ------------------------------------------------------------------
    def _open_new_file(self) -> None:
        if self.fh:
            try:
                write_line(self.fh, {"ts": iso_now(), "event": "session_end"})
            except Exception:
                pass
            self.fh.close()
        self.paths = self.file_manager.create()
        if self.on_rotate:
            try:
                self.on_rotate(self.paths)
            except Exception:
                print("[LOGGER] Errore callback rotate")
        self.fh = open(self.paths.ndjson_path, "w", encoding="utf-8")
        self.event_count = 0

    # ------------------------------------------------------------------
    def start(self) -> None:
        try:
            from pyaccsharedmemory import accSharedMemory
        except Exception:
            print("[ERRORE] Manca 'pyaccsharedmemory'. Installa:  pip install pyaccsharedmemory")
            raise

        self.asm = accSharedMemory()
        self._open_new_file()

        interval = max(0.01, 1.0 / max(1.0, self.hz))
        print(
            f"[SHM] Lettura a {self.hz:.1f} Hz (wx_every={self.wx_every}s, hb_every={self.hb_every}s)"
        )

        while not self.stop_flag:
            sm = self.asm.read_shared_memory()
            if sm is None:
                time.sleep(interval)
                continue

            phys = getattr(sm, "Physics", None)
            graf = getattr(sm, "Graphics", None)
            stat = getattr(sm, "Static", None)
            if not graf or not phys or not stat:
                time.sleep(interval)
                continue

            session_type = clean_str(getattr(graf, "session_type", None))
            track = clean_str(getattr(stat, "track", None))
            sector_count = getattr(stat, "sector_count", None)

            last_ms = as_int_ms(getattr(graf, "last_time", None))
            split_ms = as_int_ms(getattr(graf, "last_sector_time", None))

            amb_g = getattr(graf, "ambient_temp", None)
            trk_g = getattr(graf, "road_temp", None)
            amb_p = getattr(phys, "air_temp", None)
            trk_p = getattr(phys, "road_temp", None)
            air = r1(amb_g if amb_g is not None else amb_p)
            track_t = r1(trk_g if trk_g is not None else trk_p)

            grip = clean_str(getattr(graf, "track_grip_status", None)) or clean_str(
                getattr(graf, "track_grip", None)
            )
            rain_int = clean_str(getattr(graf, "rain_intensity", None))
            clouds = r1(getattr(graf, "cloud_level", None))
            wind_spd = r1(getattr(graf, "wind_speed", None))
            wind_dir = r1(getattr(graf, "wind_direction", None))

            is_in_pit = bool(getattr(graf, "is_in_pit", False))
            is_in_pit_lane = bool(getattr(graf, "is_in_pit_lane", False))
            in_pit_eff = is_in_pit or is_in_pit_lane
            cur_sector_idx = getattr(graf, "current_sector_index", None)
            is_valid_flag = getattr(graf, "is_valid_lap", None)
            if is_valid_flag is not None:
                is_valid_flag = bool(is_valid_flag)

            fuel_now = r1(getattr(phys, "fuel", None))
            speed_kmh = r1(getattr(phys, "speed_kmh", None))
            completed_lap = getattr(graf, "completed_lap", None)

            self.car_model = clean_str(getattr(stat, "car_model", None)) or self.car_model
            self.ballast = (
                getattr(graf, "ballast", None)
                if getattr(graf, "ballast", None) is not None
                else self.ballast
            )
            self.restrictor = (
                getattr(graf, "restrictor", None)
                if getattr(graf, "restrictor", None) is not None
                else self.restrictor
            )

            now_iso = iso_now()
            now_mono = monotonic()

            session_key = (session_type, track)
            if self.prev_session_key is None:
                self._open_new_file()
                self._emit(
                    {
                        "ts": now_iso,
                        "event": "session_start",
                        "session_type": session_type,
                        "track": track,
                        "sector_count": sector_count,
                        "car_model": self.car_model,
                    }
                )
                self.have_session_open = True
            elif session_key != self.prev_session_key:
                if self.have_session_open:
                    self._emit({"ts": now_iso, "event": "session_end"})
                    self.have_session_open = False
                self._open_new_file()
                self._emit(
                    {
                        "ts": now_iso,
                        "event": "session_start",
                        "session_type": session_type,
                        "track": track,
                        "sector_count": sector_count,
                        "car_model": self.car_model,
                    }
                )
                self.have_session_open = True
                self.prev_in_pit_eff = None
                self.prev_sector_idx = None
                self.prev_completed = None
                self.lap_valid_acc = None
                self.lap_start_fuel = None

            self.prev_session_key = session_key

            if self.prev_in_pit_eff is None:
                self.prev_in_pit_eff = in_pit_eff
                if in_pit_eff is False:
                    self._emit(
                        {
                            "ts": now_iso,
                            "event": "stint_start",
                            "fuel_start": fuel_now,
                            "in_pit_lane": False,
                            "completed_lap": completed_lap,
                            "car_model": self.car_model,
                        }
                    )
                    self.prev_completed = completed_lap
                    self.lap_valid_acc = None
                    self.lap_start_fuel = fuel_now
                    self.prev_sector_idx = cur_sector_idx
            else:
                if self.prev_in_pit_eff and (in_pit_eff is False):
                    self._emit(
                        {
                            "ts": now_iso,
                            "event": "stint_start",
                            "fuel_start": fuel_now,
                            "in_pit_lane": False,
                            "completed_lap": completed_lap,
                            "car_model": self.car_model,
                        }
                    )
                    self.prev_completed = completed_lap
                    self.lap_valid_acc = None
                    self.lap_start_fuel = fuel_now
                    self.prev_sector_idx = cur_sector_idx
                if (self.prev_in_pit_eff is False) and in_pit_eff:
                    self._emit(
                        {
                            "ts": now_iso,
                            "event": "stint_end",
                            "fuel_end": fuel_now,
                            "in_pit_lane": True,
                        }
                    )
                self.prev_in_pit_eff = in_pit_eff

            if is_valid_flag is not None:
                if self.lap_valid_acc is None:
                    self.lap_valid_acc = bool(is_valid_flag)
                else:
                    self.lap_valid_acc = self.lap_valid_acc and bool(is_valid_flag)

            if (
                cur_sector_idx is not None
                and self.prev_sector_idx is not None
                and cur_sector_idx != self.prev_sector_idx
            ):
                if (
                    (self.prev_sector_idx == 0 and cur_sector_idx == 1)
                    or (self.prev_sector_idx == 1 and cur_sector_idx == 2)
                ):
                    if split_ms and split_ms > 0:
                        self._emit(
                            {
                                "ts": now_iso,
                                "event": "sector_split",
                                "lap": completed_lap,
                                "sector_index": int(self.prev_sector_idx),
                                "split_ms": int(split_ms),
                            }
                        )
                self.prev_sector_idx = cur_sector_idx
            elif self.prev_sector_idx is None and cur_sector_idx is not None:
                self.prev_sector_idx = cur_sector_idx

            if (
                completed_lap is not None
                and self.prev_completed is not None
                and completed_lap > self.prev_completed
            ):
                lap_ms = last_ms if last_ms is not None else None
                self._emit(
                    {
                        "ts": now_iso,
                        "event": "lap_complete",
                        "lap": int(completed_lap),
                        "lap_ms": int(lap_ms) if lap_ms is not None else None,
                        "is_valid": bool(self.lap_valid_acc)
                        if self.lap_valid_acc is not None
                        else None,
                        "fuel_start": self.lap_start_fuel,
                        "fuel_end": fuel_now,
                    }
                )
                self.prev_completed = completed_lap
                self.lap_valid_acc = None
                self.lap_start_fuel = fuel_now
            elif self.prev_completed is None:
                self.prev_completed = completed_lap

            if self.wx_every > 0:
                def changed(a, b, thr=0.5):
                    if a is None and b is None:
                        return False
                    if a is None or b is None:
                        return True
                    return abs(a - b) >= thr

                need = False
                if changed(self.last_wx_vals["air"], air, self.wx_delta) or changed(
                    self.last_wx_vals["track"], track_t, self.wx_delta
                ):
                    need = True
                if (
                    self.last_wx_vals["grip"] != grip
                    or self.last_wx_vals["rain"] != rain_int
                ):
                    need = True
                if (
                    self.last_wx_vals["wind_spd"] != wind_spd
                    or self.last_wx_vals["wind_dir"] != wind_dir
                ):
                    need = True
                if (monotonic() - self.last_wx_emit) >= self.wx_every:
                    need = True
                if need:
                    self._emit(
                        {
                            "ts": now_iso,
                            "event": "wx",
                            "air_temp": air,
                            "track_temp": track_t,
                            "grip": grip,
                            "rain_intensity": rain_int,
                            "cloud_level": clouds,
                            "wind_speed": wind_spd,
                            "wind_direction": wind_dir,
                            "ballast": self.ballast,
                            "restrictor": self.restrictor,
                            "car_model": self.car_model,
                        }
                    )
                    self.last_wx_emit = monotonic()
                    self.last_wx_vals = {
                        "air": air,
                        "track": track_t,
                        "grip": grip,
                        "rain": rain_int,
                        "clouds": clouds,
                        "wind_spd": wind_spd,
                        "wind_dir": wind_dir,
                    }

            if self.hb_every > 0 and (monotonic() - self.last_hb_emit) >= self.hb_every:
                self._emit(
                    {
                        "ts": now_iso,
                        "event": "hb",
                        "fuel": fuel_now,
                        "speed_kmh": speed_kmh,
                        "is_valid_lap": bool(is_valid_flag)
                        if is_valid_flag is not None
                        else None,
                    }
                )
                self.last_hb_emit = monotonic()

            time.sleep(interval)

        try:
            if self.have_session_open:
                self._emit({"ts": iso_now(), "event": "session_end"})
        except Exception:
            pass

        try:
            if self.asm:
                self.asm.close()
        except Exception:
            pass
        if self.fh:
            self.fh.close()

    # ------------------------------------------------------------------
    def set_rotate_callback(self, callback: Callable[[LogPaths], None]) -> None:
        self.on_rotate = callback

    # ------------------------------------------------------------------
    def _emit(self, payload) -> None:
        if not self.fh:
            return
        write_line(self.fh, payload)
        with self.lock:
            self.event_count += 1


__all__ = ["ACCLogger"]
