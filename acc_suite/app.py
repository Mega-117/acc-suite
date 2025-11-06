"""ACC Suite application entry point."""

from __future__ import annotations

import argparse
import signal
import sys
from typing import Optional

from . import config
from .files import LogFileManager
from .logger import ACCLogger
from .scheduler import SummaryScheduler
from .updater import UpdateError, UpdateManager


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ACC Suite: logger e report JSON")
    parser.add_argument("--hz", type=float, default=5.0, help="Frequenza lettura SHM (Hz)")
    parser.add_argument("--wx-every", type=float, default=2.0, help="Secondi tra eventi meteo (0=off)")
    parser.add_argument("--hb-every", type=float, default=0.5, help="Secondi tra heartbeat (0=off)")
    parser.add_argument("--wx-delta", type=float, default=0.5, help="Soglia variazione °C per forzare wx")
    parser.add_argument("--summary-interval", type=int, default=10, help="Aggiornamento summary (secondi)")
    parser.add_argument("--out-prefix", default="acc")
    parser.add_argument("--out-dir", default=config.DEFAULT_LOG_DIR)
    parser.add_argument("--skip-update", action="store_true", help="Disabilita controllo aggiornamenti all'avvio")
    return parser


def setup_update(skip: bool) -> None:
    if skip:
        print("[UPDATE] Controllo aggiornamenti disabilitato")
        return
    manager = UpdateManager(config.base_dir())
    try:
        result = manager.check_and_update(auto_run=False)
        print(f"[UPDATE] {result.message}")
    except UpdateError as exc:
        print(f"[UPDATE] Errore durante l'aggiornamento: {exc}")


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_update(args.skip_update)

    summary_dir = config.read_summary_dir()
    config.ensure_dir(args.out_dir)
    config.ensure_dir(summary_dir)
    print(f"[CONFIG] Summary JSON dir: {summary_dir}")

    file_manager = LogFileManager(args.out_dir, summary_dir, prefix=args.out_prefix)
    logger = ACCLogger(
        file_manager=file_manager,
        hz=args.hz,
        wx_every=args.wx_every,
        hb_every=args.hb_every,
        wx_delta=args.wx_delta,
    )
    scheduler = SummaryScheduler(file_manager, interval_s=args.summary_interval)
    logger.set_rotate_callback(scheduler.attach)

    def handle_sig(signum, frame):  # pragma: no cover - signal handler
        print("\n[STOP] Chiusura richiesta...")
        scheduler.stop_flag = True
        logger.stop_flag = True

    signal.signal(signal.SIGINT, handle_sig)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_sig)

    summary_snapshot = None
    try:
        scheduler.start()
        logger.start()
    finally:
        try:
            summary_snapshot = scheduler.flush_now()
        except Exception as exc:
            print(f"[SUMMARY] Errore flush finale: {exc}")
            summary_snapshot = None
        scheduler.stop()
        if (not summary_snapshot or not summary_snapshot.get("session")) and logger.paths:
            print("[SUMMARY] Nessun dato: rimuovo file vuoti")
            file_manager.remove_pair(logger.paths)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
