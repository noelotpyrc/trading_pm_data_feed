"""
Polymarket metadata fetcher.
Pulls all active events + nested markets from the Gamma API
and saves a timestamped JSON snapshot.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "pm_metadata"
PAGE_LIMIT = 100  # max per request


def fetch_all_active_events() -> list[dict]:
    """Paginate through Gamma /events/keyset to get all active, non-closed events."""
    all_events = []
    cursor: str | None = None

    with httpx.Client(timeout=30) as client:
        while True:
            params: dict = {
                "active": "true",
                "closed": "false",
                "limit": PAGE_LIMIT,
            }
            if cursor:
                params["after_cursor"] = cursor
            resp = client.get(f"{GAMMA_BASE}/events/keyset", params=params)
            resp.raise_for_status()
            payload = resp.json()

            batch = payload.get("events", [])
            all_events.extend(batch)
            print(f"  fetched {len(all_events)} events")

            cursor = payload.get("next_cursor")
            if not cursor:
                break

            time.sleep(0.2)  # be polite

    return all_events


def save_snapshot(events: list[dict]) -> Path:
    """Save events list as a timestamped JSON file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = DATA_DIR / f"events_{ts}.json"

    # build a lean summary alongside the raw dump
    summary = {
        "snapshot_utc": ts,
        "total_events": len(events),
        "total_markets": sum(len(e.get("markets", [])) for e in events),
        "events": events,
    }

    path.write_text(json.dumps(summary, indent=2))
    print(f"saved {path}  ({summary['total_events']} events, {summary['total_markets']} markets)")
    return path


def pull_snapshot() -> Path:
    """One-shot: fetch all active events and save snapshot."""
    print("pulling polymarket metadata snapshot …")
    events = fetch_all_active_events()
    return save_snapshot(events)


if __name__ == "__main__":
    pull_snapshot()
