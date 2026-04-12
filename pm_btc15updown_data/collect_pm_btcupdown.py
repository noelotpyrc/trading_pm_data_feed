#!/usr/bin/env python3
"""
Polymarket BTC 15m Up/Down price collector.

Polls the current active market every 5 seconds, logs snapshots,
and sends a Discord alert when price delta exceeds threshold.

Usage:
  python -m pm_btc15updown_data.collect_pm_btcupdown \
    --log-dir data/pm_btcupdown [--poll-interval 5] [--threshold 0.10]

Output: daily JSONL files, e.g. data/pm_btcupdown/pm_btcupdown_2026-04-11.jsonl
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import websocket as ws_client

# Allow running as a script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from btcusdt_perp_signal.alert import send_discord

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
EPOCH_S = 900  # 15 minutes
ALERT_DELTA_FLOOR = 0.05  # minimum absolute delta to trigger alert

BINANCE_WS_URI = "wss://fstream.binance.com/ws/btcusdt@bookTicker"
BTC_HISTORY_SIZE = 30  # ~30s of ticks to keep

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    print(f"\n[{fmt_now()}] Caught {signal.Signals(sig).name}, shutting down...")
    _shutdown = True


class BtcPriceFeed:
    """Background thread that streams BTCUSDT best bid/ask from Binance."""

    def __init__(self, maxlen: int = BTC_HISTORY_SIZE):
        self.history: deque = deque(maxlen=maxlen)
        self._thread: threading.Thread | None = None
        self._ws = None
        self._last_store_s: float = 0

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def get_recent(self) -> list[dict]:
        """Return a copy of recent BTC price snapshots."""
        return list(self.history)

    def _run(self):
        delay = 5
        while not _shutdown:
            try:
                print(f"[{fmt_now()}] BTC feed: connecting to {BINANCE_WS_URI}")
                self._ws = ws_client.create_connection(BINANCE_WS_URI, timeout=10)
                print(f"[{fmt_now()}] BTC feed: connected")
                delay = 5
                while not _shutdown:
                    try:
                        raw = self._ws.recv()
                    except ws_client.WebSocketTimeoutException:
                        continue
                    data = json.loads(raw)
                    now_s = time.time()
                    # Only store one snapshot per second
                    if now_s - self._last_store_s >= 1.0:
                        self.history.append({
                            "ts_ms": data.get("E", int(now_s * 1000)),
                            "bid": data.get("b"),
                            "bid_size": data.get("B"),
                            "ask": data.get("a"),
                            "ask_size": data.get("A"),
                        })
                        self._last_store_s = now_s
            except Exception as e:
                if not _shutdown:
                    print(f"[{fmt_now()}] BTC feed: {e}. Reconnecting in {delay}s...")
            finally:
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
            if not _shutdown:
                time.sleep(delay)
                delay = min(delay * 2, 60)


def fmt_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def current_epoch_ts() -> int:
    """Return the start timestamp of the current 15m epoch."""
    return (int(time.time()) // EPOCH_S) * EPOCH_S


def resolve_market(epoch_ts: int) -> dict | None:
    """Resolve the current btc-updown-15m market from Gamma API.

    Returns dict with keys: slug, title, outcomes, token_ids
    or None if not found.
    """
    slug = f"btc-updown-15m-{epoch_ts}"
    url = f"{GAMMA_BASE}/events?slug={slug}"
    req = urllib.request.Request(url, headers={"User-Agent": "pm-collector/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"[{fmt_now()}] Gamma API error: {e}")
        return None

    if not data:
        return None

    event = data[0]
    markets = event.get("markets", [])
    if not markets:
        return None

    m = markets[0]
    raw_outcomes = m.get("outcomes", "[]")
    outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
    token_ids = json.loads(m.get("clobTokenIds", "[]"))

    return {
        "slug": slug,
        "title": event.get("title", ""),
        "outcomes": outcomes,
        "token_ids": token_ids,
    }


def fetch_prices(token_ids: list[str]) -> dict | None:
    """Fetch order book for each token. Returns snapshot dict with best bid/ask + sizes."""
    snapshot = {"ts_ms": int(time.time() * 1000), "tokens": []}
    for tid in token_ids:
        try:
            url = f"{CLOB_BASE}/book?token_id={tid}"
            req = urllib.request.Request(url, headers={"User-Agent": "pm-collector/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                book = json.loads(resp.read())

            bids = book.get("bids", [])
            asks = book.get("asks", [])
            # CLOB returns bids ascending, asks descending
            # Best bid = highest price (last), best ask = lowest price (last)
            best_bid = bids[-1] if bids else {"price": "0", "size": "0"}
            best_ask = asks[-1] if asks else {"price": "0", "size": "0"}
            bid_p = float(best_bid["price"])
            ask_p = float(best_ask["price"])
            mid = (bid_p + ask_p) / 2 if bid_p > 0 and ask_p > 0 else 0

            snapshot["tokens"].append({
                "token_id": tid,
                "mid": f"{mid:.4f}",
                "bid": best_bid["price"],
                "bid_size": best_bid["size"],
                "ask": best_ask["price"],
                "ask_size": best_ask["size"],
            })
        except Exception as e:
            print(f"[{fmt_now()}] CLOB book error for {tid[:20]}...: {e}")
            return None

    return snapshot


def get_log_file(base: Path) -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return base / f"pm_btcupdown_{date_str}.jsonl"


def append_event(base: Path, event: dict) -> Path:
    fp = get_log_file(base)
    with open(fp, "a") as f:
        f.write(json.dumps(event, separators=(",", ":")) + "\n")
    return fp


def format_alert(
    market_title: str, outcomes: list, deltas: list[dict],
    curr: dict, history: deque, btc_prices: list[dict],
) -> str:
    lines = [f"\U0001f4c8 **PM BTC 15m Up/Down Price Alert**", f"{market_title}", ""]
    for d in deltas:
        lines.append(
            f"**{d['outcome']}**: `{d['prev_mid']}` → `{d['curr_mid']}` "
            f"(delta: `{d['delta']:+.4f}`, `{d['pct']:+.1f}%`)"
        )
    lines.append("")
    # Current bid/ask with sizes
    for i, t in enumerate(curr["tokens"]):
        label = outcomes[i] if i < len(outcomes) else f"token_{i}"
        lines.append(f"{label}: bid=`{t['bid']}` ({t['bid_size']}) / ask=`{t['ask']}` ({t['ask_size']})")
    # 30s trailing Up prices
    lines.append("")
    lines.append("**Recent Up prices (30s):**")
    lines.append("```")
    for snap in history:
        ts_str = datetime.fromtimestamp(snap["ts_ms"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
        up_t = snap["tokens"][0]
        lines.append(f"  {ts_str}  mid={up_t['mid']}  bid={up_t['bid']}({up_t['bid_size']}) ask={up_t['ask']}({up_t['ask_size']})")
    lines.append("```")
    # BTC price action
    if btc_prices:
        lines.append("")
        lines.append("**BTCUSDT perp (30s):**")
        lines.append("```")
        # Sample ~6 evenly spaced entries to avoid flooding
        step = max(1, len(btc_prices) // 6)
        sampled = btc_prices[::step]
        if btc_prices[-1] not in sampled:
            sampled.append(btc_prices[-1])
        for tick in sampled:
            ts_str = datetime.fromtimestamp(tick["ts_ms"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
            lines.append(f"  {ts_str}  bid={tick['bid']} ask={tick['ask']}")
        lines.append("```")
    return "\n".join(lines)


def collect(
    log_base: Path,
    poll_interval: float,
    threshold: float,
    btc_feed: BtcPriceFeed | None = None,
) -> None:
    prev_snapshot: dict | None = None
    prev_epoch_ts: int | None = None
    market_info: dict | None = None
    total_snapshots = 0
    # Keep ~30s of history (30s / poll_interval)
    max_history = max(1, int(30 / poll_interval))
    history: deque = deque(maxlen=max_history)

    while not _shutdown:
        epoch_ts = current_epoch_ts()

        # Resolve market on epoch change or first run
        if market_info is None or epoch_ts != prev_epoch_ts:
            market_info = resolve_market(epoch_ts)
            if market_info:
                print(f"[{fmt_now()}] Market: {market_info['title']} ({market_info['slug']})")
                prev_snapshot = None  # reset delta tracking on new epoch
                history.clear()
            else:
                print(f"[{fmt_now()}] No market found for epoch {epoch_ts}, retrying...")
                time.sleep(poll_interval)
                continue
            prev_epoch_ts = epoch_ts

        # Fetch prices
        snapshot = fetch_prices(market_info["token_ids"])
        if snapshot is None:
            prev_snapshot = None  # reset so stale data doesn't cause false delta
            time.sleep(poll_interval)
            continue

        # Add market context to snapshot
        snapshot["slug"] = market_info["slug"]
        snapshot["outcomes"] = market_info["outcomes"]

        # Log
        fp = append_event(log_base, snapshot)
        total_snapshots += 1

        # Build display string
        parts = []
        for i, t in enumerate(snapshot["tokens"]):
            label = market_info["outcomes"][i] if i < len(market_info["outcomes"]) else f"t{i}"
            parts.append(f"{label}={t['mid']}")
        print(f"[{fmt_now()}] #{total_snapshots} {' '.join(parts)} → {fp.name}")

        history.append(snapshot)

        # Check delta on Up token (index 0) ask price
        if prev_snapshot is not None:
            up_idx = 0
            prev_ask = float(prev_snapshot["tokens"][up_idx]["ask"])
            curr_ask = float(snapshot["tokens"][up_idx]["ask"])

            # Skip delta calc if either price is missing/zero
            if prev_ask > 0 and curr_ask > 0:
                delta = curr_ask - prev_ask
                pct = delta / prev_ask * 100
                outcome = market_info["outcomes"][up_idx] if up_idx < len(market_info["outcomes"]) else "Up"

                if abs(pct) >= threshold * 100 and abs(delta) >= ALERT_DELTA_FLOOR:
                    deltas = [{
                        "outcome": outcome,
                        "prev_mid": f"{prev_ask:.4f}",
                        "curr_mid": f"{curr_ask:.4f}",
                        "delta": delta,
                        "pct": pct,
                    }]
                    btc_prices = btc_feed.get_recent() if btc_feed else []
                    msg = format_alert(
                        market_info["title"], market_info["outcomes"],
                        deltas, snapshot, history, btc_prices,
                    )
                    print(f"[{fmt_now()}] ALERT: Up ask {prev_ask} → {curr_ask} ({pct:+.1f}%)")
                    send_discord(msg, env_key="DISCORD_WEBHOOK_URL_PM")

        prev_snapshot = snapshot
        time.sleep(poll_interval)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="PM BTC 15m Up/Down price collector (Gamma/CLOB → JSONL + Discord)",
    )
    p.add_argument(
        "--log-dir", type=Path, default=Path("data/pm_btcupdown"),
        help="Directory for daily JSONL log files (default: data/pm_btcupdown)",
    )
    p.add_argument("--poll-interval", type=float, default=5.0, help="Seconds between polls (default: 5)")
    p.add_argument("--threshold", type=float, default=0.10, help="Delta pct threshold for alert, e.g. 0.10 = 10%% (default: 0.10)")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    log_base = args.log_dir
    log_base.mkdir(parents=True, exist_ok=True)
    print(f"[{fmt_now()}] PM BTC Up/Down collector starting")
    print(f"  Logs → {log_base.resolve()}")
    print(f"  Poll interval: {args.poll_interval}s")
    print(f"  Alert threshold: {args.threshold * 100:.0f}%")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    btc_feed = BtcPriceFeed()
    btc_feed.start()

    collect(log_base, args.poll_interval, args.threshold, btc_feed)

    btc_feed.stop()
    print(f"[{fmt_now()}] Collector stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
