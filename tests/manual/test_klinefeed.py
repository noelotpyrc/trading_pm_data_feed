"""
End-to-end smoke for the patched KlineFeed inside signal_stream_v3.

Imports the real production class so the WSS pattern under test is the same
one the live signal streams use. Two layers:

  Layer 1 (default): verify bars arrive on a normal 120s run.
  Layer 2 (--force-close): after 30s of normal bars, force-close the
      WebSocketApp from outside and verify the loop reconnects and bars
      resume within the remainder of the run window.

Run:
  /Users/noel/projects/venvs/production/bin/python tests/manual/test_klinefeed.py
  /Users/noel/projects/venvs/production/bin/python tests/manual/test_klinefeed.py --force-close
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pm_btc15updown_data import signal_stream_v3 as ssv3
from pm_btc15updown_data.signal_stream_v3 import KlineFeed


def main(duration_s: int, force_close_at: int | None) -> int:
    print(f"KLINE_WS in use: {ssv3.KLINE_WS}")
    if force_close_at is not None:
        print(f"Will force-close the WSS at +{force_close_at}s")
    feed = KlineFeed()
    feed.start()

    bars: list[dict] = []
    bars_before_close = 0
    forced = False
    start = time.monotonic()
    deadline = start + duration_s
    last_print = start
    try:
        while time.monotonic() < deadline:
            new_bars = feed.drain()
            if new_bars:
                for b in new_bars:
                    print(f"  closed bar t={b['t']} o={b['o']} h={b['h']} "
                          f"l={b['l']} c={b['c']}")
                bars.extend(new_bars)

            # Trigger the forced disconnect once
            if (force_close_at is not None and not forced
                    and time.monotonic() - start >= force_close_at):
                bars_before_close = len(bars)
                print(f"\n>>> forcing ws.close() at +{force_close_at}s, "
                      f"bars so far: {bars_before_close} <<<\n")
                try:
                    feed._ws.close()
                except Exception as e:
                    print(f"force-close raised: {e}")
                forced = True

            # heartbeat every 15s so we know the loop is alive
            now = time.monotonic()
            if now - last_print >= 15:
                remaining = int(deadline - now)
                print(f"[{remaining}s remaining] bars so far: {len(bars)}")
                last_print = now
            time.sleep(0.5)
    finally:
        ssv3._shutdown = True
        feed.stop()
        if feed._thread:
            feed._thread.join(timeout=5)
        ssv3._shutdown = False

    print("-" * 60)
    print(f"Total closed bars received: {len(bars)}")

    if force_close_at is None:
        if not bars:
            print("FAIL: no closed bars arrived — KlineFeed did not deliver data")
            return 1
        print("PASS: KlineFeed delivered at least one closed bar on the new URL")
        return 0

    # --force-close mode
    bars_after_close = len(bars) - bars_before_close
    print(f"Bars before close: {bars_before_close}")
    print(f"Bars after close:  {bars_after_close}")
    if bars_before_close == 0:
        print("FAIL: no bars before forced close — connection never delivered")
        return 1
    if bars_after_close == 0:
        print("FAIL: no bars after forced close — reconnect did not recover")
        return 1
    print("PASS: KlineFeed reconnected after forced close and resumed bars")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--force-close", dest="force_close", type=int, default=None,
                    metavar="SECONDS",
                    help="Force-close the WSS after this many seconds; "
                         "default 30 when --force-close given without value")
    ap.add_argument("--force-close-default", action="store_true",
                    help=argparse.SUPPRESS)
    args = ap.parse_args()
    fc = args.force_close
    raise SystemExit(main(args.seconds, fc))
