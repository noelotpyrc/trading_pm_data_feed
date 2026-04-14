#!/usr/bin/env python3
"""
Dual PM BTC Up/Down price display: 15m and 5m markets side by side.

Tracks the current 15m market continuously. In the last 5 minutes of the
15m window, also tracks the overlapping 5m market. Records both every 5s,
prints a summary every 15s. Shows strike price (BTC 1m candle open at epoch
start) for each market.

Usage:
  python -m pm_btc15updown_data.collect_pm_dual \
    --log-dir data/pm_dual [--poll-interval 5] [--display-interval 15]

Output: daily JSONL files with both market snapshots.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running as a script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from btcusdt_perp_signal.alert import send_discord

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
BINANCE_FAPI = "https://fapi.binance.com"
EPOCH_15M = 900
EPOCH_5M = 300
DISCORD_ENV_KEY = "DISCORD_WEBHOOK_URL_PM_DUAL"

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    print(f"\n[{fmt_now()}] Caught {signal.Signals(sig).name}, shutting down...")
    _shutdown = True


def fmt_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")


def fetch_strike(epoch_ts: int, retries: int = 2) -> str | None:
    """Fetch the BTC 1m candle open price at epoch_ts as strike price."""
    epoch_ms = epoch_ts * 1000
    url = (
        f"{BINANCE_FAPI}/fapi/v1/klines"
        f"?symbol=BTCUSDT&interval=1m&startTime={epoch_ms}&limit=1"
    )
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pm-dual/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            if data:
                return data[0][1]  # open price
        except Exception as e:
            print(f"[{fmt_now()}] Strike fetch error (attempt {attempt+1}): {e}")
        if attempt < retries - 1:
            time.sleep(3)
    return None


def resolve_market(slug: str) -> dict | None:
    """Resolve a btc-updown market by slug."""
    url = f"{GAMMA_BASE}/events?slug={slug}"
    req = urllib.request.Request(url, headers={"User-Agent": "pm-dual/1.0"})
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


def fetch_book_price(token_id: str) -> dict | None:
    """Fetch best bid/ask from CLOB book for a single token."""
    try:
        url = f"{CLOB_BASE}/book?token_id={token_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "pm-dual/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            book = json.loads(resp.read())

        bids = book.get("bids", [])
        asks = book.get("asks", [])
        best_bid = bids[-1] if bids else {"price": "0", "size": "0"}
        best_ask = asks[-1] if asks else {"price": "0", "size": "0"}
        bid_p = float(best_bid["price"])
        ask_p = float(best_ask["price"])
        mid = (bid_p + ask_p) / 2 if bid_p > 0 and ask_p > 0 else 0

        return {
            "mid": f"{mid:.4f}",
            "bid": best_bid["price"],
            "bid_size": best_bid["size"],
            "ask": best_ask["price"],
            "ask_size": best_ask["size"],
        }
    except Exception as e:
        print(f"[{fmt_now()}] CLOB error: {e}")
        return None


def _fmt_price_line(label: str, p: dict | None, strike: str | None) -> str:
    if not p:
        return f"{label}: --"
    return (
        f"{label} (K:`{strike or '?'}`): "
        f"bid=`{p['bid']}` ({p['bid_size']}) "
        f"ask=`{p['ask']}` ({p['ask_size']}) "
        f"mid=`{p['mid']}`"
    )


def check_arb(
    strike_15m: str | None,
    strike_5m: str | None,
    up_15m: dict | None,
    up_5m: dict | None,
) -> dict | None:
    """Check for arb: higher strike should have lower Up price.

    Returns arb info dict if mispricing detected, else None.
    """
    if not all([strike_15m, strike_5m, up_15m, up_5m]):
        return None

    k15 = float(strike_15m)
    k5 = float(strike_5m)
    ask_15m = float(up_15m["ask"])
    ask_5m = float(up_5m["ask"])
    mid_15m = float(up_15m["mid"])
    mid_5m = float(up_5m["mid"])

    # Higher strike should have lower Up price
    if k15 > k5 and (ask_15m >= ask_5m or mid_15m >= mid_5m):
        return {
            "higher": "15m", "lower": "5m",
            "k_diff": round(k15 - k5, 2),
            "ask_diff": round(ask_15m - ask_5m, 4),
            "mid_diff": round(mid_15m - mid_5m, 4),
        }
    if k5 > k15 and (ask_5m >= ask_15m or mid_5m >= mid_15m):
        return {
            "higher": "5m", "lower": "15m",
            "k_diff": round(k5 - k15, 2),
            "ask_diff": round(ask_5m - ask_15m, 4),
            "mid_diff": round(mid_5m - mid_15m, 4),
        }
    return None


def _fmt_market_block(
    label: str, strike: str | None,
    up: dict | None, no: dict | None,
) -> str:
    """Format a market's YES + NO prices for Discord."""
    lines = [f"**{label}** (K: `{strike or '?'}`)"]
    if up:
        lines.append(
            f"  YES: bid=`{up['bid']}` ({up['bid_size']}) "
            f"ask=`{up['ask']}` ({up['ask_size']}) mid=`{up['mid']}`"
        )
    else:
        lines.append("  YES: --")
    if no:
        lines.append(
            f"  NO:  bid=`{no['bid']}` ({no['bid_size']}) "
            f"ask=`{no['ask']}` ({no['ask_size']}) mid=`{no['mid']}`"
        )
    else:
        lines.append("  NO:  --")
    return "\n".join(lines)


def format_arb_message(
    remaining: int,
    strike_15m: str | None,
    strike_5m: str | None,
    arb: dict,
    snap: dict,
) -> str:
    """Format an arb alert for Discord."""
    lines = ["🚨 **PM Arb Signal**"]
    lines.append(f"Time: `{fmt_now()}` | Remaining: `{remaining}s`")
    lines.append(
        f"**{arb['higher']}** has higher strike (+`{arb['k_diff']}`) "
        f"but Up price ≥ **{arb['lower']}**"
    )
    lines.append(f"Ask diff: `{arb['ask_diff']}` | Mid diff: `{arb['mid_diff']}`")
    lines.append("")
    lines.append(_fmt_market_block("15m", strike_15m, snap.get("p15"), snap.get("no15")))
    lines.append(_fmt_market_block("5m", strike_5m, snap.get("p5"), snap.get("no5")))
    return "\n".join(lines)


def format_dual_message(
    remaining: int,
    strike_15m: str | None,
    strike_5m: str | None,
    snapshots: list[dict],
) -> str:
    """Format a dual market summary with buffered snapshots for Discord."""
    lines = ["📊 **PM Dual BTC Up/Down**"]
    lines.append(f"Time: `{fmt_now()}` | Remaining: `{remaining}s`")
    lines.append("")
    for snap in snapshots:
        ts_str = datetime.fromtimestamp(
            snap["ts_ms"] / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S")
        m15 = _fmt_price_line("15m", snap.get("p15"), strike_15m)
        m5 = _fmt_price_line("5m", snap.get("p5"), strike_5m)
        lines.append(f"`{ts_str}` {m15} | {m5}")
    return "\n".join(lines)


def get_log_file(base: Path) -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return base / f"pm_dual_{date_str}.jsonl"


def append_event(base: Path, event: dict) -> Path:
    fp = get_log_file(base)
    with open(fp, "a") as f:
        f.write(json.dumps(event, separators=(",", ":")) + "\n")
    return fp


def collect(
    log_base: Path,
    poll_interval: float,
    display_interval: float,
) -> None:
    prev_15m_epoch: int | None = None
    prev_5m_epoch: int | None = None
    market_15m: dict | None = None
    market_5m: dict | None = None
    strike_15m: str | None = None
    strike_5m: str | None = None
    total_snapshots = 0
    last_display_s: float = 0
    display_buffer: list[dict] = []  # buffered snapshots for Discord

    while not _shutdown:
        now = int(time.time())
        now_s = float(now)
        epoch_15m = (now // EPOCH_15M) * EPOCH_15M
        end_15m = epoch_15m + EPOCH_15M
        in_last_5m = now >= (end_15m - EPOCH_5M)

        # Resolve 15m market on epoch change
        if epoch_15m != prev_15m_epoch:
            slug_15m = f"btc-updown-15m-{epoch_15m}"
            market_15m = resolve_market(slug_15m)
            if market_15m:
                print(f"[{fmt_now()}] 15m: {market_15m['title']}")
                strike_15m = fetch_strike(epoch_15m)
                if strike_15m:
                    print(f"[{fmt_now()}] 15m strike: {strike_15m}")
            else:
                print(f"[{fmt_now()}] 15m: no market for {slug_15m}")
                strike_15m = None
            prev_15m_epoch = epoch_15m
            market_5m = None
            prev_5m_epoch = None
            strike_5m = None
            display_buffer = []

        # Retry 15m strike if missing
        if market_15m and strike_15m is None:
            strike_15m = fetch_strike(epoch_15m, retries=1)
            if strike_15m:
                print(f"[{fmt_now()}] 15m strike (retry): {strike_15m}")

        # Resolve 5m market when entering last 5 minutes
        if in_last_5m:
            epoch_5m = (now // EPOCH_5M) * EPOCH_5M
            if epoch_5m != prev_5m_epoch:
                slug_5m = f"btc-updown-5m-{epoch_5m}"
                market_5m = resolve_market(slug_5m)
                if market_5m:
                    print(f"[{fmt_now()}] 5m:  {market_5m['title']}")
                    strike_5m = fetch_strike(epoch_5m)
                    if strike_5m:
                        print(f"[{fmt_now()}] 5m  strike: {strike_5m}")
                else:
                    print(f"[{fmt_now()}] 5m:  no market for {slug_5m}")
                    strike_5m = None
                prev_5m_epoch = epoch_5m

            # Retry 5m strike if missing
            if market_5m and strike_5m is None:
                strike_5m = fetch_strike(epoch_5m, retries=1)
                if strike_5m:
                    print(f"[{fmt_now()}] 5m  strike (retry): {strike_5m}")

        # Fetch YES and NO token prices
        price_15m = None
        price_5m = None
        no_15m = None
        no_5m = None

        if market_15m:
            price_15m = fetch_book_price(market_15m["token_ids"][0])
            if len(market_15m["token_ids"]) > 1:
                no_15m = fetch_book_price(market_15m["token_ids"][1])

        if market_5m and in_last_5m:
            price_5m = fetch_book_price(market_5m["token_ids"][0])
            if len(market_5m["token_ids"]) > 1:
                no_5m = fetch_book_price(market_5m["token_ids"][1])

        # Build snapshot
        snapshot = {
            "ts_ms": int(time.time() * 1000),
            "m15": {
                "slug": market_15m["slug"] if market_15m else None,
                "strike": strike_15m,
                "up": price_15m,
                "no": no_15m,
            } if market_15m else None,
            "m5": {
                "slug": market_5m["slug"] if market_5m else None,
                "strike": strike_5m,
                "up": price_5m,
                "no": no_5m,
            } if market_5m and in_last_5m else None,
        }

        # Log every poll
        fp = append_event(log_base, snapshot)
        total_snapshots += 1

        # Buffer snapshot for Discord (only during overlap window)
        if in_last_5m:
            display_buffer.append({
                "ts_ms": snapshot["ts_ms"],
                "p15": price_15m,
                "p5": price_5m,
                "no15": no_15m,
                "no5": no_5m,
            })

            # Arb check on every poll
            arb = check_arb(strike_15m, strike_5m, price_15m, price_5m)
            if arb:
                remaining = end_15m - now
                msg = format_arb_message(
                    remaining, strike_15m, strike_5m, arb,
                    display_buffer[-1],
                )
                print(f"[{fmt_now()}] *** ARB SIGNAL: {arb['higher']} strike higher by {arb['k_diff']}, ask_diff={arb['ask_diff']}, mid_diff={arb['mid_diff']}")
                send_discord(msg, env_key=DISCORD_ENV_KEY)

        # Display every display_interval
        if now_s - last_display_s >= display_interval:
            last_display_s = now_s
            remaining = end_15m - now
            parts = [f"#{total_snapshots}"]

            if price_15m:
                parts.append(
                    f"15m Up: bid={price_15m['bid']} ask={price_15m['ask']} "
                    f"mid={price_15m['mid']} strike={strike_15m or '?'}"
                )

            if price_5m:
                parts.append(
                    f"5m Up: bid={price_5m['bid']} ask={price_5m['ask']} "
                    f"mid={price_5m['mid']} strike={strike_5m or '?'}"
                )

            parts.append(f"rem={remaining}s")
            print(f"[{fmt_now()}] {' | '.join(parts)}")

            # Send to Discord only during last 5 minutes
            if in_last_5m and display_buffer:
                msg = format_dual_message(
                    remaining, strike_15m, strike_5m, display_buffer,
                )
                send_discord(msg, env_key=DISCORD_ENV_KEY)
                display_buffer = []

        time.sleep(poll_interval)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Dual PM BTC Up/Down price display (15m + 5m overlap)",
    )
    p.add_argument(
        "--log-dir", type=Path, default=Path("data/pm_dual"),
        help="Directory for daily JSONL log files (default: data/pm_dual)",
    )
    p.add_argument("--poll-interval", type=float, default=5.0)
    p.add_argument("--display-interval", type=float, default=15.0)
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    log_base = args.log_dir
    log_base.mkdir(parents=True, exist_ok=True)
    print(f"[{fmt_now()}] PM Dual collector starting")
    print(f"  Logs → {log_base.resolve()}")
    print(f"  Poll: {args.poll_interval}s | Display: {args.display_interval}s")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    collect(log_base, args.poll_interval, args.display_interval)

    print(f"[{fmt_now()}] Collector stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
