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
PM_WSS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
EPOCH_S = 900  # 15 minutes
ALERT_DELTA_FLOOR = 0.05  # minimum absolute delta to trigger alert

BINANCE_WS_DEPTH = "wss://fstream.binance.com/ws/btcusdt@depth20@500ms"
BINANCE_FAPI = "https://fapi.binance.com"
DEPTH_HISTORY_SIZE = 30  # ~30s at 1/sec sampling
TRADE_HISTORY_SIZE = 2000  # plenty of headroom for many minutes

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    print(f"\n[{fmt_now()}] Caught {signal.Signals(sig).name}, shutting down...")
    _shutdown = True


class DepthFeed:
    """Background thread that streams BTCUSDT top-20 depth from Binance."""

    def __init__(self, maxlen: int = DEPTH_HISTORY_SIZE):
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

    def get_recent(self, window_s: float = 30.0) -> list[dict]:
        """Return depth snapshots from the last window_s seconds."""
        cutoff = int((time.time() - window_s) * 1000)
        return [s for s in self.history if s["ts_ms"] >= cutoff]

    def _run(self):
        delay = 5
        while not _shutdown:
            try:
                print(f"[{fmt_now()}] Depth feed: connecting to {BINANCE_WS_DEPTH}")
                self._ws = ws_client.create_connection(BINANCE_WS_DEPTH, timeout=10)
                print(f"[{fmt_now()}] Depth feed: connected")
                delay = 5
                while not _shutdown:
                    try:
                        raw = self._ws.recv()
                    except ws_client.WebSocketTimeoutException:
                        continue
                    now_s = time.time()
                    if now_s - self._last_store_s >= 1.0:
                        data = json.loads(raw)
                        # bids: [[price, qty], ...], asks: [[price, qty], ...]
                        bids = [[float(p), float(q)] for p, q in data.get("b", [])]
                        asks = [[float(p), float(q)] for p, q in data.get("a", [])]
                        self.history.append({
                            "ts_ms": data.get("E", int(now_s * 1000)),
                            "bids": bids,
                            "asks": asks,
                        })
                        self._last_store_s = now_s
            except Exception as e:
                if not _shutdown:
                    print(f"[{fmt_now()}] Depth feed: {e}. Reconnecting in {delay}s...")
            finally:
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
            if not _shutdown:
                time.sleep(delay)
                delay = min(delay * 2, 60)


class TradeFeed:
    """Background thread that streams real-time PM trades via WSS."""

    def __init__(self, maxlen: int = TRADE_HISTORY_SIZE):
        self.trades: deque = deque(maxlen=maxlen)
        self._thread: threading.Thread | None = None
        self._ws = None
        self._lock = threading.Lock()
        self._token_ids: list[str] = []
        self._yes_token_id: str | None = None
        self._restart = threading.Event()

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def set_market(self, token_ids: list[str], yes_token_id: str | None):
        """Update the active market. Clears buffer and reconnects on change."""
        with self._lock:
            if sorted(token_ids or []) != sorted(self._token_ids):
                self._token_ids = list(token_ids or [])
                self._yes_token_id = yes_token_id
                self.trades.clear()
                self._restart.set()
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                if token_ids:
                    print(f"[{fmt_now()}] Trade feed: subscribing to {len(token_ids)} tokens")

    def get_between(self, start_ms: int, end_ms: int) -> list[dict]:
        """Return trades with timestamps (ms) within [start_ms, end_ms]."""
        with self._lock:
            return [t for t in self.trades if start_ms <= t["ts_ms"] <= end_ms]

    def _run(self):
        delay = 5
        while not _shutdown:
            with self._lock:
                tids = list(self._token_ids)
                yes_tid = self._yes_token_id
            if not tids:
                time.sleep(2)
                continue
            try:
                print(f"[{fmt_now()}] Trade feed: connecting to {PM_WSS_MARKET}")
                self._ws = ws_client.create_connection(PM_WSS_MARKET, timeout=10)
                self._ws.send(json.dumps({"type": "market", "assets_ids": tids}))
                self._ws.settimeout(2)
                self._restart.clear()
                print(f"[{fmt_now()}] Trade feed: connected")
                delay = 5
                while not _shutdown and not self._restart.is_set():
                    try:
                        raw = self._ws.recv()
                    except ws_client.WebSocketTimeoutException:
                        continue
                    msgs = json.loads(raw) if raw.startswith("[") else [json.loads(raw)]
                    for msg in msgs:
                        if msg.get("event_type") != "last_trade_price":
                            continue
                        with self._lock:
                            token = "YES" if msg.get("asset_id") == self._yes_token_id else "NO"
                            self.trades.append({
                                "ts_ms": int(msg.get("timestamp", 0)),
                                "token": token,
                                "side": msg.get("side"),  # BUY or SELL (taker)
                                "size": float(msg.get("size", 0)),
                                "price": float(msg.get("price", 0)),
                            })
            except Exception as e:
                if not _shutdown:
                    print(f"[{fmt_now()}] Trade feed: {e}. Reconnecting in {delay}s...")
            finally:
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
            if not _shutdown and not self._restart.is_set():
                time.sleep(delay)
                delay = min(delay * 2, 60)


def compute_trade_stats(trades: list[dict]) -> dict:
    """Aggregate a slice of trades."""
    stats = {
        "count": len(trades),
        "notional": 0.0,
        "yes_buy": 0.0, "yes_sell": 0.0,
        "no_buy": 0.0, "no_sell": 0.0,
    }
    for t in trades:
        stats["notional"] += t["size"] * t["price"]
        key = f"{t['token'].lower()}_{t['side'].lower()}"
        if key in stats:
            stats[key] += t["size"]
    return stats


def compute_depth_stats(snap: dict, strike: float | None = None) -> dict:
    """Compute order book stats from a single depth snapshot."""
    bids = snap["bids"]  # [[price, qty], ...]
    asks = snap["asks"]

    bid_total = sum(q for _, q in bids)
    ask_total = sum(q for _, q in asks)
    imbalance = bid_total / ask_total if ask_total > 0 else 0

    best_bid_p, best_bid_q = bids[0] if bids else (0, 0)
    best_ask_p, best_ask_q = asks[0] if asks else (0, 0)
    spread = best_ask_p - best_bid_p if best_bid_p > 0 and best_ask_p > 0 else 0

    bid_vwap = sum(p * q for p, q in bids) / bid_total if bid_total > 0 else 0
    ask_vwap = sum(p * q for p, q in asks) / ask_total if ask_total > 0 else 0

    # Micro-price
    if best_bid_q + best_ask_q > 0:
        micro = (best_bid_p * best_ask_q + best_ask_p * best_bid_q) / (best_bid_q + best_ask_q)
    else:
        micro = 0

    # 20th level prices
    bid_20th = bids[-1][0] if len(bids) >= 20 else (bids[-1][0] if bids else 0)
    ask_20th = asks[-1][0] if len(asks) >= 20 else (asks[-1][0] if asks else 0)

    # Strike-to-price depth
    strike_depth = None
    strike_side = None
    if strike is not None and best_bid_p > 0 and best_ask_p > 0:
        mid = (best_bid_p + best_ask_p) / 2
        if mid > strike:
            # BTC above strike: sum bid qty between strike and best bid
            strike_depth = sum(q for p, q in bids if p >= strike)
            strike_side = "bid"
            # Check if strike is within range
            if bids and bids[-1][0] > strike:
                pass  # all bids are above strike, depth is valid
            elif not any(p <= strike for p, _ in bids):
                strike_depth = None  # strike outside range
        elif mid < strike:
            # BTC below strike: sum ask qty between best ask and strike
            strike_depth = sum(q for p, q in asks if p <= strike)
            strike_side = "ask"
            if not any(p >= strike for p, _ in asks):
                strike_depth = None  # strike outside range

    return {
        "ts_ms": snap["ts_ms"],
        "best_bid": best_bid_p,
        "best_ask": best_ask_p,
        "imb": round(imbalance, 3),
        "bid_total": round(bid_total, 2),
        "ask_total": round(ask_total, 2),
        "bid_vwap": round(bid_vwap, 2),
        "ask_vwap": round(ask_vwap, 2),
        "micro": round(micro, 2),
        "bid_20th": bid_20th,
        "ask_20th": ask_20th,
        "strike_depth": round(strike_depth, 2) if strike_depth is not None else None,
        "strike_side": strike_side,
    }


def fetch_strike(epoch_ts: int, retries: int = 2) -> str | None:
    """Fetch the BTC 1m candle open price at epoch_ts as strike price."""
    epoch_ms = epoch_ts * 1000
    url = (
        f"{BINANCE_FAPI}/fapi/v1/klines"
        f"?symbol=BTCUSDT&interval=1m&startTime={epoch_ms}&limit=1"
    )
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pm-collector/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            if data:
                return data[0][1]  # open price
        except Exception as e:
            print(f"[{fmt_now()}] Strike fetch error (attempt {attempt+1}): {e}")
        if attempt < retries - 1:
            time.sleep(3)
    return None


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
        "condition_id": m.get("conditionId"),
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
    curr: dict, history: deque,
    depth_stats: list[dict] | None = None,
    strike: str | None = None,
    trade_stats: dict | None = None,
) -> str:
    lines = [f"\U0001f4c8 **PM BTC 15m Up/Down Price Alert**"]
    lines.append(f"{market_title} | Strike: `{strike or '?'}`")
    lines.append("")
    for d in deltas:
        lines.append(
            f"**{d['outcome']}**: `{d['prev_mid']}` → `{d['curr_mid']}` "
            f"(delta: `{d['delta']:+.4f}`, `{d['pct']:+.1f}%`)"
        )
    # Trailing Up prices (last 4)
    lines.append("")
    lines.append("**Recent Up prices:**")
    lines.append("```")
    for snap in list(history)[-4:]:
        ts_str = datetime.fromtimestamp(snap["ts_ms"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
        up_t = snap["tokens"][0]
        lines.append(f"  {ts_str}  mid={up_t['mid']}  bid={up_t['bid']}({up_t['bid_size']}) ask={up_t['ask']}({up_t['ask_size']})")
    lines.append("```")
    # PM trades during the price move window
    if trade_stats is not None:
        lines.append("")
        lines.append(
            f"**PM trades during move ({trade_stats['window_s']}s):** "
            f"{trade_stats['count']} trades, ${trade_stats['notional']:.2f}"
        )
        lines.append(
            f"  YES: BUY `{trade_stats['yes_buy']:.2f}` SELL `{trade_stats['yes_sell']:.2f}` | "
            f"NO: BUY `{trade_stats['no_buy']:.2f}` SELL `{trade_stats['no_sell']:.2f}`"
        )
    # Unified book section (30s, 10 snapshots — price + depth + structure)
    if depth_stats:
        lines.append("")
        lines.append("**BTCUSDT book (30s):**")
        lines.append("```")
        for ds in depth_stats:
            ts_str = datetime.fromtimestamp(ds["ts_ms"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
            k_str = f"K={ds['strike_depth']}({ds['strike_side']})" if ds["strike_depth"] is not None else "K=OOR"
            lines.append(
                f"  {ts_str}  {ds['best_bid']:.1f}/{ds['best_ask']:.1f}  "
                f"micro={ds['micro']:.1f}  bV={ds['bid_vwap']:.1f}  aV={ds['ask_vwap']:.1f}  "
                f"b20={ds['bid_20th']:.1f}  a20={ds['ask_20th']:.1f}"
            )
            lines.append(
                f"            bidD={ds['bid_total']:.1f}  askD={ds['ask_total']:.1f}  imb={ds['imb']:.2f}  {k_str}"
            )
        lines.append("```")
    return "\n".join(lines)


def collect(
    log_base: Path,
    poll_interval: float,
    threshold: float,
    depth_feed: DepthFeed | None = None,
    trade_feed: TradeFeed | None = None,
) -> None:
    prev_snapshot: dict | None = None
    prev_epoch_ts: int | None = None
    market_info: dict | None = None
    strike: str | None = None
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
                strike = fetch_strike(epoch_ts)
                if strike:
                    print(f"[{fmt_now()}] Strike: {strike}")
                prev_snapshot = None  # reset delta tracking on new epoch
                history.clear()
                if trade_feed:
                    trade_feed.set_market(
                        market_info.get("token_ids", []),
                        market_info["token_ids"][0] if market_info.get("token_ids") else None,
                    )
            else:
                print(f"[{fmt_now()}] No market found for epoch {epoch_ts}, retrying...")
                time.sleep(poll_interval)
                continue
            prev_epoch_ts = epoch_ts

        # Retry strike if missing
        if market_info and strike is None:
            strike = fetch_strike(epoch_ts, retries=1)
            if strike:
                print(f"[{fmt_now()}] Strike (retry): {strike}")

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

        # Check positive delta on YES (idx 0) and NO (idx 1) ask prices
        if prev_snapshot is not None:
            deltas = []
            for idx in range(min(2, len(snapshot["tokens"]), len(prev_snapshot["tokens"]))):
                prev_ask = float(prev_snapshot["tokens"][idx]["ask"])
                curr_ask = float(snapshot["tokens"][idx]["ask"])
                if prev_ask <= 0 or curr_ask <= 0:
                    continue
                delta = curr_ask - prev_ask
                pct = delta / prev_ask * 100
                # Only trigger on POSITIVE movement
                # Fire if (delta >= floor AND pct >= threshold) OR pct >= 30%
                fires = (
                    (delta >= ALERT_DELTA_FLOOR and pct >= threshold * 100)
                    or pct >= 30.0
                )
                if not fires:
                    continue
                outcome = (
                    market_info["outcomes"][idx]
                    if idx < len(market_info["outcomes"]) else f"token_{idx}"
                )
                deltas.append({
                    "outcome": outcome,
                    "prev_mid": f"{prev_ask:.4f}",
                    "curr_mid": f"{curr_ask:.4f}",
                    "delta": delta,
                    "pct": pct,
                })

            if deltas:
                # Depth stats: 6 snapshots from 30s buffer
                depth_stats = None
                if depth_feed:
                    raw_depth = depth_feed.get_recent(30.0)
                    if raw_depth:
                        strike_f = float(strike) if strike else None
                        step = max(1, (len(raw_depth) - 1) // 5)
                        sampled = raw_depth[::step][:5]
                        if raw_depth[-1] not in sampled:
                            sampled.append(raw_depth[-1])
                        depth_stats = [compute_depth_stats(s, strike_f) for s in sampled]
                # Trade stats during the move window
                trade_stats = None
                if trade_feed:
                    slice_trades = trade_feed.get_between(
                        prev_snapshot["ts_ms"], snapshot["ts_ms"]
                    )
                    trade_stats = compute_trade_stats(slice_trades)
                    trade_stats["window_s"] = round(
                        (snapshot["ts_ms"] - prev_snapshot["ts_ms"]) / 1000, 1
                    )
                msg = format_alert(
                    market_info["title"], market_info["outcomes"],
                    deltas, snapshot, history,
                    depth_stats, strike, trade_stats,
                )
                summary = ", ".join(
                    f"{d['outcome']} {d['prev_mid']}→{d['curr_mid']} ({d['pct']:+.1f}%)"
                    for d in deltas
                )
                print(f"[{fmt_now()}] ALERT: {summary}")
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

    depth_feed = DepthFeed()
    trade_feed = TradeFeed()
    depth_feed.start()
    trade_feed.start()

    collect(log_base, args.poll_interval, args.threshold, depth_feed, trade_feed)

    depth_feed.stop()
    trade_feed.stop()
    print(f"[{fmt_now()}] Collector stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
