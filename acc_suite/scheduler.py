"""Summary scheduler."""

from __future__ import annotations

import json
import threading
import time
from typing import Optional

from .files import LogPaths
from .summary import SummaryBuilder
from .utils import iso_now


class SummaryScheduler:
    def __init__(self, file_manager, interval_s: int = 10) -> None:
        self.file_manager = file_manager
        self.interval_s = max(5, int(interval_s))
        self.stop_flag = False
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.current_paths: Optional[LogPaths] = None

    def attach(self, paths: LogPaths) -> None:
        self.current_paths = paths

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_flag = True
        self.thread.join(timeout=3)

    def _run(self) -> None:
        while not self.stop_flag:
            time.sleep(self.interval_s)
            if not self.current_paths:
                continue
            try:
                self._write_summary(self.current_paths)
            except Exception as exc:
                print(f"[SUMMARY] Errore aggiornamento: {exc}")

    def _write_summary(self, paths: LogPaths) -> Optional[dict]:
        builder = SummaryBuilder(paths.ndjson_path)
        summary = builder.build()
        summary["generated_at"] = iso_now()
        with open(paths.json_path, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
        print(f"[SUMMARY] Aggiornato: {paths.json_path}")
        return summary

    def flush_now(self) -> Optional[dict]:
        if not self.current_paths:
            return None
        return self._write_summary(self.current_paths)


__all__ = ["SummaryScheduler"]
