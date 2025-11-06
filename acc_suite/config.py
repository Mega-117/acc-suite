"""Configuration helpers for ACC Suite."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def base_dir() -> str:
    """Return the directory where the executable/script lives."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


DEFAULT_LOG_DIR = os.path.join(base_dir(), "logs")
DEFAULT_SUMMARY_DIR = os.path.join(base_dir(), "logs_json")
DEFAULT_CONFIG_FILE = os.path.join(base_dir(), "config.txt")
DEFAULT_VERSION_FILE = os.path.join(base_dir(), "version.json")


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def read_summary_dir(config_path: str = DEFAULT_CONFIG_FILE) -> str:
    """Return the directory for JSON summaries from config or default."""
    try:
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as handle:
                raw = handle.readline().strip().strip('"')
                if raw:
                    return raw
    except Exception:
        pass
    return DEFAULT_SUMMARY_DIR


@dataclass
class VersionInfo:
    version: str
    download_url: str
    sha256: Optional[str] = None
    notes: Optional[str] = None
    metadata_url: Optional[str] = None

    @classmethod
    def load(cls, path: str = DEFAULT_VERSION_FILE) -> "VersionInfo":
        """Load version metadata, tolerating missing files.

        When the project runs from sources the ``version.json`` file may live in
        the repository root instead of next to the package.  In frozen builds
        the file is bundled beside the executable.  We therefore probe a small
        set of locations and fall back to default metadata when nothing is
        available so start-up never crashes with ``FileNotFoundError``.
        """

        candidates = []
        if path:
            candidates.append(path)

        base = base_dir()
        if base:
            package_candidate = os.path.join(base, "version.json")
            if package_candidate not in candidates:
                candidates.append(package_candidate)
            parent = os.path.dirname(base)
            if parent:
                repo_candidate = os.path.join(parent, "version.json")
                if repo_candidate not in candidates:
                    candidates.append(repo_candidate)

        for candidate in candidates:
            if candidate and os.path.exists(candidate):
                with open(candidate, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                return cls(
                    version=payload.get("version", "0.0.0"),
                    download_url=payload.get("downloadUrl", ""),
                    sha256=payload.get("sha256"),
                    notes=payload.get("notes"),
                    metadata_url=payload.get("metadataUrl"),
                )

        # Default metadata when no file is found.  This keeps the updater
        # operational (it will fetch remote metadata) while avoiding crashes at
        # import time if the local file is missing.
        return cls(version="0.0.0", download_url="")


__all__ = [
    "DEFAULT_CONFIG_FILE",
    "DEFAULT_LOG_DIR",
    "DEFAULT_SUMMARY_DIR",
    "DEFAULT_VERSION_FILE",
    "VersionInfo",
    "base_dir",
    "ensure_dir",
    "read_summary_dir",
]
