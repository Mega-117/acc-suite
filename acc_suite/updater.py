"""Self-update logic for ACC Suite."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.request import urlopen

from . import config

DEFAULT_METADATA_URL = "https://raw.githubusercontent.com/Mega-117/acc-suite/main/version.json"


def _parse_version(value: str) -> tuple:
    parts = []
    for chunk in value.split('.'):
        try:
            parts.append(int(chunk))
        except ValueError:
            parts.append(0)
    return tuple(parts)


@dataclass
class UpdateResult:
    updated: bool
    message: str


class UpdateError(Exception):
    pass


class UpdateManager:
    """Check GitHub for updates and apply them safely."""

    def __init__(self, base_path: str, metadata_url: Optional[str] = None) -> None:
        self.base_path = Path(base_path)
        self.metadata_url = metadata_url or DEFAULT_METADATA_URL
        self.version_info = config.VersionInfo.load()

    # ------------------------------------------------------------------
    def check_and_update(self, auto_run: bool = False) -> UpdateResult:
        try:
            remote = self._fetch_remote_info()
        except Exception as exc:  # pragma: no cover - network failure
            return UpdateResult(False, f"Impossibile contattare il server aggiornamenti: {exc}")

        if not remote:
            return UpdateResult(False, "Nessun metadato remoto disponibile")

        if _parse_version(remote.version) <= _parse_version(self.version_info.version):
            return UpdateResult(False, f"Versione aggiornata ({self.version_info.version})")

        try:
            archive_path = self._download(remote.download_url)
            if remote.sha256:
                self._verify_checksum(archive_path, remote.sha256)
            self._apply_update(archive_path)
            msg = f"Aggiornato alla versione {remote.version}"
            if auto_run:
                self._run_new_version()
            return UpdateResult(True, msg)
        except Exception as exc:
            self._restore_backup()
            raise UpdateError(str(exc))

    # ------------------------------------------------------------------
    def _fetch_remote_info(self) -> Optional[config.VersionInfo]:
        url = self.version_info.metadata_url or self.metadata_url
        with urlopen(url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        info = config.VersionInfo(
            version=payload.get("version", "0.0.0"),
            download_url=payload.get("downloadUrl", ""),
            sha256=payload.get("sha256"),
            notes=payload.get("notes"),
            metadata_url=payload.get("metadataUrl") or url,
        )
        return info

    # ------------------------------------------------------------------
    def _download(self, url: str) -> Path:
        if not url:
            raise UpdateError("URL download mancante")
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip")
        os.close(tmp_fd)
        print(f"[UPDATE] Scarico {url}")
        with urlopen(url, timeout=30) as response, open(tmp_path, "wb") as handle:
            shutil.copyfileobj(response, handle)
        print(f"[UPDATE] Scaricato in {tmp_path}")
        return Path(tmp_path)

    # ------------------------------------------------------------------
    def _verify_checksum(self, archive: Path, expected: str) -> None:
        print("[UPDATE] Verifica checksum...")
        digest = hashlib.sha256()
        with open(archive, "rb") as handle:
            for chunk in iter(lambda: handle.read(8192), b""):
                digest.update(chunk)
        if digest.hexdigest().upper() != expected.upper():
            raise UpdateError("Checksum SHA256 non corrispondente")

    # ------------------------------------------------------------------
    def _apply_update(self, archive: Path) -> None:
        backup_dir = self.base_path / "old" / self.version_info.version
        backup_dir.parent.mkdir(parents=True, exist_ok=True)
        working_backup = backup_dir / "backup"
        if working_backup.exists():
            shutil.rmtree(working_backup)
        working_backup.mkdir(parents=True)

        print(f"[UPDATE] Backup in {working_backup}")
        for item in self.base_path.iterdir():
            if item.name in {"old", "logs", "logs_json"}:
                continue
            target = working_backup / item.name
            if item.is_dir():
                shutil.copytree(item, target)
            else:
                shutil.copy2(item, target)

        extract_dir = Path(tempfile.mkdtemp())
        print(f"[UPDATE] Estraggo in {extract_dir}")
        with zipfile.ZipFile(archive, "r") as zip_handle:
            zip_handle.extractall(extract_dir)

        for entry in extract_dir.iterdir():
            dest = self.base_path / entry.name
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest)
                else:
                    dest.unlink()
            if entry.is_dir():
                shutil.copytree(entry, dest)
            else:
                shutil.copy2(entry, dest)

        print("[UPDATE] Aggiornamento completato")
        shutil.rmtree(extract_dir, ignore_errors=True)
        archive.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    def _restore_backup(self) -> None:
        backup_root = self.base_path / "old"
        if not backup_root.exists():
            return
        backups = sorted(backup_root.glob("*/backup"))
        if not backups:
            return
        latest = backups[-1]
        print(f"[UPDATE] Ripristino da {latest}")
        for item in latest.iterdir():
            dest = self.base_path / item.name
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest)
                else:
                    dest.unlink()
            if item.is_dir():
                shutil.copytree(item, dest)
            else:
                shutil.copy2(item, dest)

    # ------------------------------------------------------------------
    def _run_new_version(self) -> None:  # pragma: no cover - exec
        exe = Path(sys.executable)
        script = self.base_path / "acc_suite.py"
        if exe and exe.exists():
            print("[UPDATE] Avvio nuova versione...")
            os.execv(exe, [exe.as_posix(), script.as_posix()])


__all__ = ["UpdateManager", "UpdateResult", "UpdateError", "DEFAULT_METADATA_URL"]
