"""
End-to-end smoke for the patched KlineFeed inside signal_stream_v3.

Imports the real production class (so the URL constant under test is the same
one the live signal streams use) and verifies that a closed 1m bar arrives
within the run window.

Run:
  /Users/noel/projects/venvs/production/bin/python tests/manual/test_klinefeed.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pm_btc15updown_data import signal_stream_v3 as ssv3
from pm_btc15updown_data.signal_stream_v3 import KlineFeed


def main(duration_s: int = 120) -> int:
    print(f"KLINE_WS in use: {ssv3.KLINE_WS}")
    feed = KlineFeed()
    feed.start()

    bars: list[dict] = []
    deadline = time.monotonic() + duration_s
    last_print = time.monotonic()
    try:
        while time.monotonic() < deadline:
            new_bars = feed.drain()
            if new_bars:
                for b in new_bars:
                    print(f"  closed bar t={b['t']} o={b['o']} h={b['h']} l={b['l']} c={b['c']}")
                bars.extend(new_bars)
            # heartbeat every 15s so we know the loop is alive
            now = time.monotonic()
            if now - last_print >= 15:
                remaining = int(deadline - now)
                print(f"[{remaining}s remaining] bars so far: {len(bars)}")
                last_print = now
            time.sleep(0.5)
    finally:
        # Ask the feed thread to exit cleanly
        ssv3._shutdown = True
        feed.stop()
        if feed._thread:
            feed._thread.join(timeout=5)
        # reset for any subsequent runs
        ssv3._shutdown = False

    print("-" * 60)
    print(f"Total closed bars received: {len(bars)}")
    if not bars:
        print("FAIL: no closed bars arrived — KlineFeed did not deliver data")
        return 1
    print("PASS: KlineFeed delivered at least one closed bar on the new URL")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
