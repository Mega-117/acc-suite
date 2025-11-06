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
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return cls(
            version=payload.get("version", "0.0.0"),
            download_url=payload.get("downloadUrl", ""),
            sha256=payload.get("sha256"),
            notes=payload.get("notes"),
            metadata_url=payload.get("metadataUrl"),
        )


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
