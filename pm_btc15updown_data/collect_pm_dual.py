#!/usr/bin/env python3
"""
Dual PM BTC Up/Down price display: 15m and 5m markets side by side.

Tracks the current 15m market continuously. In the last 5 minutes of the
15m window, also tracks the overlapping 5m market. Polls every 3s and
checks for arb: higher-strike market with equal/higher Up price.
Sends Discord alert on arb signal with YES + NO prices for both markets.

Usage:
  python -m pm_btc15updown_data.collect_pm_dual \
    --log-dir data/pm_dual [--poll-interval 3]

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



def check_arb(
    strike_15m: str | None,
    strike_5m: str | None,
    up_15m: dict | None,
    up_5m: dict | None,
    no_15m: dict | None = None,
    no_5m: dict | None = None,
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

    # Skip if any YES or NO ask is 0 (no liquidity, market resolved)
    no_15m_ask = float(no_15m["ask"]) if no_15m else 0
    no_5m_ask = float(no_5m["ask"]) if no_5m else 0
    if ask_15m == 0 or ask_5m == 0 or no_15m_ask == 0 or no_5m_ask == 0:
        return None

    # Higher strike should have lower Up price (relaxed: ask or mid >= 0)
    if k15 > k5 and (ask_15m >= ask_5m or mid_15m >= mid_5m):
        return {
            "higher": "15m", "lower": "5m",
            "higher_ask": ask_15m, "lower_ask": ask_5m,
            "lower_no_ask": no_5m_ask,
            "k_diff": round(k15 - k5, 2),
            "ask_diff": round(ask_15m - ask_5m, 4),
            "mid_diff": round(mid_15m - mid_5m, 4),
        }
    if k5 > k15 and (ask_5m >= ask_15m or mid_5m >= mid_15m):
        return {
            "higher": "5m", "lower": "15m",
            "higher_ask": ask_5m, "lower_ask": ask_15m,
            "lower_no_ask": no_15m_ask,
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


def format_window_summary(
    epoch_15m: int,
    strike_15m: str | None,
    strike_5m: str | None,
    stats: dict,
    final_up_15m: dict | None, final_no_15m: dict | None,
    final_up_5m: dict | None, final_no_5m: dict | None,
) -> str:
    """Format an aggregated window summary (only when arb triggered)."""
    end_str = datetime.fromtimestamp(
        epoch_15m + EPOCH_15M, tz=timezone.utc
    ).strftime("%H:%M:%S UTC")
    first_str = datetime.fromtimestamp(
        stats["first_ts"], tz=timezone.utc
    ).strftime("%H:%M:%S")
    last_str = datetime.fromtimestamp(
        stats["last_ts"], tz=timezone.utc
    ).strftime("%H:%M:%S")

    yes_asks = stats["lower_yes_asks"]
    no_asks = stats["lower_no_asks"]
    yes_min, yes_avg = min(yes_asks), sum(yes_asks) / len(yes_asks)
    no_min, no_avg = min(no_asks), sum(no_asks) / len(no_asks)

    lines = ["\U0001f3c1 **PM Dual Window Summary**"]
    lines.append(f"Window close: `{end_str}` | Captured: `{fmt_now()}`")
    lines.append("")
    lines.append(f"**Arb triggers**: `{stats['count']}` | First: `{first_str}` | Last: `{last_str}`")
    lines.append(
        f"Max ask diff: `{stats['max_ask_diff']:.4f}` | "
        f"Max mid diff: `{stats['max_mid_diff']:.4f}`"
    )
    lines.append(f"Lower-strike ask stats (n=`{len(yes_asks)}`):")
    lines.append(f"  YES: min=`{yes_min:.4f}` avg=`{yes_avg:.4f}`")
    lines.append(f"  NO:  min=`{no_min:.4f}` avg=`{no_avg:.4f}`")
    lines.append("")
    lines.append("**Final prices:**")
    lines.append(_fmt_market_block("15m", strike_15m, final_up_15m, final_no_15m))
    lines.append(_fmt_market_block("5m", strike_5m, final_up_5m, final_no_5m))
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
) -> None:
    prev_15m_epoch: int | None = None
    prev_5m_epoch: int | None = None
    market_15m: dict | None = None
    market_5m: dict | None = None
    strike_15m: str | None = None
    strike_5m: str | None = None
    total_snapshots = 0
    summary_sent_epoch: int | None = None  # epoch for which window summary sent
    # Per-window trigger stats (reset on epoch change)
    trigger_stats: dict = {
        "count": 0,
        "first_ts": None,
        "last_ts": None,
        "max_ask_diff": 0.0,
        "max_mid_diff": 0.0,
        "lower_yes_asks": [],
        "lower_no_asks": [],
    }

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
            trigger_stats = {
                "count": 0,
                "first_ts": None,
                "last_ts": None,
                "max_ask_diff": 0.0,
                "max_mid_diff": 0.0,
                "lower_yes_asks": [],
                "lower_no_asks": [],
            }

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

        # Arb check (only during overlap window)
        if in_last_5m:
            arb = check_arb(strike_15m, strike_5m, price_15m, price_5m, no_15m, no_5m)
            if arb:
                trigger_stats["count"] += 1
                if trigger_stats["first_ts"] is None:
                    trigger_stats["first_ts"] = now
                trigger_stats["last_ts"] = now
                if arb["ask_diff"] > trigger_stats["max_ask_diff"]:
                    trigger_stats["max_ask_diff"] = arb["ask_diff"]
                if arb["mid_diff"] > trigger_stats["max_mid_diff"]:
                    trigger_stats["max_mid_diff"] = arb["mid_diff"]
                trigger_stats["lower_yes_asks"].append(arb["lower_ask"])
                trigger_stats["lower_no_asks"].append(arb["lower_no_ask"])
                print(
                    f"[{fmt_now()}] arb #{trigger_stats['count']}: "
                    f"{arb['higher']} K+{arb['k_diff']} ask_diff={arb['ask_diff']} "
                    f"mid_diff={arb['mid_diff']} "
                    f"lower_yes={arb['lower_ask']} lower_no={arb['lower_no_ask']}"
                )

        # Window summary at T-1s before 15m close (only if arb triggered at least once)
        remaining = end_15m - time.time()
        if (
            market_15m and summary_sent_epoch != epoch_15m
            and trigger_stats["count"] > 0
            and 0 < remaining <= poll_interval + 1
        ):
            sleep_until = max(0, remaining - 1)
            if sleep_until > 0:
                time.sleep(sleep_until)
            final_up_15m = fetch_book_price(market_15m["token_ids"][0])
            final_no_15m = (
                fetch_book_price(market_15m["token_ids"][1])
                if len(market_15m["token_ids"]) > 1 else None
            )
            final_up_5m = None
            final_no_5m = None
            if market_5m:
                final_up_5m = fetch_book_price(market_5m["token_ids"][0])
                if len(market_5m["token_ids"]) > 1:
                    final_no_5m = fetch_book_price(market_5m["token_ids"][1])
            msg = format_window_summary(
                epoch_15m, strike_15m, strike_5m, trigger_stats,
                final_up_15m, final_no_15m, final_up_5m, final_no_5m,
            )
            print(f"[{fmt_now()}] *** WINDOW SUMMARY: {trigger_stats['count']} triggers")
            send_discord(msg, env_key=DISCORD_ENV_KEY)
            summary_sent_epoch = epoch_15m
            continue

        time.sleep(poll_interval)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Dual PM BTC Up/Down price display (15m + 5m overlap)",
    )
    p.add_argument(
        "--log-dir", type=Path, default=Path("data/pm_dual"),
        help="Directory for daily JSONL log files (default: data/pm_dual)",
    )
    p.add_argument("--poll-interval", type=float, default=3.0)
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    log_base = args.log_dir
    log_base.mkdir(parents=True, exist_ok=True)
    print(f"[{fmt_now()}] PM Dual collector starting")
    print(f"  Logs → {log_base.resolve()}")
    print(f"  Poll: {args.poll_interval}s")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    collect(log_base, args.poll_interval)

    print(f"[{fmt_now()}] Collector stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
