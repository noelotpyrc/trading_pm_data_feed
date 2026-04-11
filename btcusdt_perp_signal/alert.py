"""
Discord webhook alert for signal notifications.

Reads DISCORD_WEBHOOK_URL from environment or .env file.
Falls back to logging if not configured.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
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


def send_discord(message: str) -> bool:
    """Send a message via Discord webhook. Returns True on success."""
    _load_env()
    url = os.environ.get("DISCORD_WEBHOOK_URL")

    if not url:
        log.warning("Discord not configured (missing DISCORD_WEBHOOK_URL)")
        return False

    payload = json.dumps({"content": message}).encode()

    try:
        req = urllib.request.Request(
            url, data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "SignalEngine/1.0",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            # Discord returns 204 No Content on success
            if resp.status in (200, 204):
                log.info("Discord alert sent")
                return True
            log.error("Discord API error: status=%d", resp.status)
            return False
    except Exception as e:
        log.error("Discord send failed: %s", e)
        return False


def format_signal_message(timestamp: str, direction: str, features: dict) -> str:
    """Format a signal alert message for Discord."""
    emoji = "\U0001f7e2" if direction == "long" else "\U0001f534"
    lines = [
        f"{emoji} **BTCUSDT {direction.upper()} Signal**",
        f"Time: `{timestamp}`",
        "",
        "**Features:**",
    ]
    for key, val in features.items():
        lines.append(f"  {key}: `{val:.4f}`")
    return "\n".join(lines)
