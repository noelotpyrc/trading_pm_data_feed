"""
Entry point for the BTCUSDT perp signal engine.

Usage (VPS):
    .venv/bin/python -m btcusdt_perp_signal.scripts.run_signal_engine

Optional env vars (in .env):
    OHLCV_DB_PATH        — path to OHLCV SQLite (default: data/btcusdt_perp_1m.sqlite)
    SIGNALS_DB_PATH      — path to signals SQLite (default: same as OHLCV_DB_PATH)
    TELEGRAM_BOT_TOKEN   — Telegram bot token for alerts
    TELEGRAM_CHAT_ID     — Telegram chat ID for alerts
"""
from __future__ import annotations

import logging
import os
import signal
import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from btcusdt_perp_signal.signal_engine import SignalEngine


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip())


def main() -> None:
    _load_env()

    # Logging
    log_path = ROOT / "data" / "signal_engine.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(str(log_path)),
            logging.StreamHandler(),
        ],
    )

    db_path = Path(os.environ.get("OHLCV_DB_PATH", ROOT / "data" / "btcusdt_perp_1m.sqlite"))
    signals_db_path_str = os.environ.get("SIGNALS_DB_PATH")
    signals_db_path = Path(signals_db_path_str) if signals_db_path_str else db_path

    engine = SignalEngine(db_path=db_path, signals_db_path=signals_db_path)

    # Graceful shutdown
    def _shutdown(signum, frame):
        logging.getLogger(__name__).info("Shutting down (signal %s)", signum)
        engine.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    engine.run()


if __name__ == "__main__":
    main()
