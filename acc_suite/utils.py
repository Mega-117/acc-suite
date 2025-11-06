"""Generic helpers for ACC Suite."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

SENTINEL_LAST_BEST_MS = 2_147_483_647


def iso_now() -> str:
    dt = datetime.now(timezone.utc)
    return (
        dt.replace(microsecond=int(dt.microsecond / 1000) * 1000)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def ensure_ascii_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def write_line(handle, payload: Any) -> None:
    handle.write(ensure_ascii_json(payload) + "\n")
    handle.flush()


def r1(value: Optional[Any]) -> Optional[float]:
    try:
        return None if value is None else round(float(value), 1)
    except Exception:
        return None


def as_int_ms(value: Optional[Any]) -> Optional[int]:
    try:
        if value is None:
            return None
        v = int(round(float(value)))
        if v <= 0 or v >= 100 * 60 * 1000 or v == SENTINEL_LAST_BEST_MS:
            return None
        return v
    except Exception:
        return None


def clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.split("\x00", 1)[0]
    return str(value)


def fmt_hms(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    total_sec = int(ms) / 1000.0
    minutes = int(total_sec // 60)
    seconds = total_sec - minutes * 60
    return f"{minutes}:{seconds:06.3f}"


def monotonic() -> float:
    return time.monotonic()


__all__ = [
    "SENTINEL_LAST_BEST_MS",
    "as_int_ms",
    "clean_str",
    "fmt_hms",
    "iso_now",
    "monotonic",
    "parse_iso",
    "r1",
    "write_line",
]
