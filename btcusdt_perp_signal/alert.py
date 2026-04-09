"""
Telegram alert for signal notifications.

Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from environment or .env file.
Falls back to logging if Telegram is not configured.
"""
from __future__ import annotations

import logging
import os
import urllib.request
import urllib.parse
import json
from pathlib import Path

log = logging.getLogger(__name__)

_ENV_LOADED = False


def _load_env() -> None:
    """Load .env file once (stdlib only, no dotenv dependency)."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    _ENV_LOADED = True
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip())


def send_telegram(message: str) -> bool:
    """Send a message via Telegram bot. Returns True on success."""
    _load_env()
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        log.warning("Telegram not configured (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }).encode()

    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                log.info("Telegram alert sent")
                return True
            log.error("Telegram API error: %s", result)
            return False
    except Exception as e:
        log.error("Telegram send failed: %s", e)
        return False


def format_signal_message(timestamp: str, direction: str, features: dict) -> str:
    """Format a signal alert message."""
    emoji = "\U0001f7e2" if direction == "long" else "\U0001f534"
    lines = [
        f"{emoji} *BTCUSDT {direction.upper()} Signal*",
        f"Time: `{timestamp}`",
        "",
        "*Features:*",
    ]
    for key, val in features.items():
        lines.append(f"  {key}: `{val:.4f}`")
    return "\n".join(lines)
