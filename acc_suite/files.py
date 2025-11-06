"""File management helpers for ACC Suite."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from . import config
from .utils import iso_now


@dataclass
class LogPaths:
    token: str
    ndjson_path: str
    json_path: str


class LogFileManager:
    """Handles NDJSON/JSON file lifecycle."""

    def __init__(self, log_dir: str, summary_dir: str, prefix: str = "acc") -> None:
        self.log_dir = log_dir
        self.summary_dir = summary_dir
        self.prefix = prefix
        config.ensure_dir(self.log_dir)
        config.ensure_dir(self.summary_dir)

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def create(self) -> LogPaths:
        """Create paired NDJSON/JSON files and return their paths."""
        token = self._timestamp()
        ndjson_path = os.path.join(self.log_dir, f"{self.prefix}_{token}.ndjson")
        json_path = os.path.join(self.summary_dir, f"summary_{token}.json")

        ndjson_file = Path(ndjson_path)
        ndjson_file.parent.mkdir(parents=True, exist_ok=True)
        ndjson_file.touch(exist_ok=False)

        skeleton = {
            "file": os.path.basename(ndjson_path),
            "generated_at": iso_now(),
            "session": [],
            "stints": [],
        }
        with open(json_path, "w", encoding="utf-8") as handle:
            json.dump(skeleton, handle, ensure_ascii=False, indent=2)

        return LogPaths(token=token, ndjson_path=ndjson_path, json_path=json_path)

    def remove_pair(self, paths: LogPaths) -> None:
        for item in (paths.ndjson_path, paths.json_path):
            try:
                os.remove(item)
            except FileNotFoundError:
                continue


__all__ = ["LogFileManager", "LogPaths"]
