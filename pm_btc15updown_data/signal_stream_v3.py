#!/usr/bin/env python3
"""
Live V3 signal stream: computes P(Yes) from V3 artifact + Binance 1m klines,
polls PM order books every 3s, sends Discord alerts on prob threshold triggers
and at each trading window close.

Usage:
  python -m pm_btc15updown_data.signal_stream_v3 \
    --db data/btcusdt_perp_1m.sqlite \
    --artifact-dir data/artifacts_v3 \
    [--prob-high 0.70] [--prob-low 0.30] [--debug]
"""
from __future__ import annotations

import argparse
import json
import math
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import websocket as ws_client

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from btcusdt_perp_signal.alert import send_discord
from pm_btc15updown_artifact.data_loader import load_ohlcv_window
from pm_btc15updown_artifact.vol_signal_artifacts_v3 import DailySignalArtifactV3
from pm_btc15updown_data.collect_pm_btcupdown import (
    resolve_market,
    fetch_prices,
    fetch_strike,
    current_epoch_ts,
    fmt_now,
)

KLINE_WS = "wss://fstream.binance.com/ws/btcusdt@kline_1m"
LOOP_TIMEOUT_S = 1.0
POLL_INTERVAL_MS = 3_000
ALERT_POLL_COUNT = 10
DELTA_ENTRY_THRESHOLD = 0.05
NEAR_CLOSE_OFFSET_MS = 2_000
NEAR_CLOSE_RETRY_MS = 1_000
GROUP_MINUTES = 15
WINDOW_MS = GROUP_MINUTES * 60 * 1000

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    print(f"\n[{fmt_now()}] Caught {signal.Signals(sig).name}, shutting down...")
    _shutdown = True


# ---------------------------------------------------------------------------
# OHLCV rolling buffer + feature computation
# ---------------------------------------------------------------------------

class OhlcvBuffer:
    """Rolling buffer of 1m OHLCV for V3 feature computation at runtime."""

    def __init__(self, maxlen: int = 1500):
        self.highs: deque[float] = deque(maxlen=maxlen)
        self.lows: deque[float] = deque(maxlen=maxlen)
        self.closes: deque[float] = deque(maxlen=maxlen)

    def load_from_db(self, db_path: Path, n_bars: int = 1500) -> int:
        end = pd.Timestamp.now("UTC")
        start = end - timedelta(minutes=n_bars)
        df = load_ohlcv_window(db_path, start=start, end=end)
        for h, l, c in zip(df["high"], df["low"], df["close"]):
            self.highs.append(float(h))
            self.lows.append(float(l))
            self.closes.append(float(c))
        return len(df)

    def append(self, h: float, l: float, c: float) -> None:
        self.highs.append(h)
        self.lows.append(l)
        self.closes.append(c)

    def compute_features(self) -> np.ndarray | None:
        """Compute 10 V3 features for the latest bar. None if insufficient data."""
        n = len(self.closes)
        if n < 1440:
            return None

        highs = np.array(self.highs)
        lows = np.array(self.lows)
        log_hl_sq = np.log(highs / lows) ** 2
        c = 1.0 / (4.0 * math.log(2.0))

        p5 = math.sqrt(c * float(np.mean(log_hl_sq[-5:])))
        p10 = math.sqrt(c * float(np.mean(log_hl_sq[-10:])))
        p15 = math.sqrt(c * float(np.mean(log_hl_sq[-15:])))
        p30 = math.sqrt(c * float(np.mean(log_hl_sq[-30:])))
        p1440 = math.sqrt(c * float(np.mean(log_hl_sq[-1440:])))

        if p1440 <= 0:
            return None

        # Absolute log returns (last 4)
        closes = np.array(self.closes)
        abs_rets = np.abs(np.diff(np.log(closes[-5:])))  # 4 values, newest last

        features = np.array([
            p5, p10, p15, p30,
            p15 / p1440, p30 / p1440,
            abs_rets[-1],
            float(np.mean(abs_rets[-2:])),
            float(np.mean(abs_rets[-3:])),
            float(np.mean(abs_rets[-4:])),
        ])
        return features if not np.isnan(features).any() else None


# ---------------------------------------------------------------------------
# Kline websocket feed
# ---------------------------------------------------------------------------

class KlineFeed:
    """Background WS thread for BTCUSDT 1m klines. Queues completed bars."""

    def __init__(self):
        self.pending: deque[dict] = deque()
        self.bar_ready = threading.Event()
        self._ws = None
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def drain(self) -> list[dict]:
        bars = []
        while self.pending:
            bars.append(self.pending.popleft())
        return bars

    def _run(self):
        delay = 5
        while not _shutdown:
            try:
                print(f"[{fmt_now()}] Kline feed: connecting")
                self._ws = ws_client.create_connection(KLINE_WS, timeout=10)
                print(f"[{fmt_now()}] Kline feed: connected")
                delay = 5
                while not _shutdown:
                    try:
                        raw = self._ws.recv()
                    except ws_client.WebSocketTimeoutException:
                        continue
                    k = json.loads(raw).get("k", {})
                    if k.get("x"):  # candle closed
                        self.pending.append({
                            "t": int(k["t"]),
                            "o": float(k["o"]),
                            "h": float(k["h"]),
                            "l": float(k["l"]),
                            "c": float(k["c"]),
                        })
                        self.bar_ready.set()
            except Exception as e:
                if not _shutdown:
                    print(f"[{fmt_now()}] Kline feed: {e}. Reconnecting in {delay}s...")
            finally:
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
            if not _shutdown:
                time.sleep(delay)
                delay = min(delay * 2, 60)


# ---------------------------------------------------------------------------
# Signal computation
# ---------------------------------------------------------------------------

def compute_ttl(bar_open_ms: int) -> int:
    minute_in_epoch = (bar_open_ms // 1000 // 60) % GROUP_MINUTES
    return ((GROUP_MINUTES - 2 - minute_in_epoch) % GROUP_MINUTES) + 1


def compute_prob_yes(
    features: np.ndarray,
    ttl: int,
    strike: float,
    close: float,
    artifact: DailySignalArtifactV3,
) -> float | None:
    if ttl not in artifact.models:
        return None
    model = artifact.models[ttl]
    coeffs = np.array(model.coefficients, dtype=float)
    pred_mar = max(float(np.dot(features, coeffs) + model.intercept), 0.0)
    if pred_mar <= 0:
        return None

    sigma_w = math.sqrt(math.pi / 2.0) * pred_mar * math.sqrt(ttl)
    if sigma_w <= 0:
        return None

    threshold = math.log(strike / close) / sigma_w
    idx = int(np.searchsorted(artifact.z_pool, threshold, side="right"))
    return (len(artifact.z_pool) - idx) / len(artifact.z_pool)


# ---------------------------------------------------------------------------
# Poll helpers + contrarian job
# ---------------------------------------------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def _safe_fetch(token_ids):
    try:
        return fetch_prices(token_ids)
    except Exception as e:
        print(f"[{fmt_now()}] PM fetch error: {e}")
        return None


def _extract_token(snap: dict, idx: int) -> dict:
    tok = snap["tokens"][idx] if idx < len(snap["tokens"]) else {}

    def _f(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    return {
        "ts_ms": snap["ts_ms"],
        "ask": _f(tok.get("ask")),
        "bid": _f(tok.get("bid")),
        "ask_size": tok.get("ask_size"),
        "bid_size": tok.get("bid_size"),
    }


@dataclass
class ContrarianJob:
    fired_at_ms: int
    window_start_ms: int
    window_end_ms: int
    ttl_at_trigger: int
    prob: float
    fair: float              # fair value of target token
    strike: float
    btc_close: float
    action: str              # "BUY NO" | "BUY YES"
    target_idx: int          # 0=YES | 1=NO
    outcomes: list[str]      # market outcome labels

    polls: list[dict] = field(default_factory=list)   # {ts_ms, ask, bid, delta}
    post_trigger_attempted: int = 0
    next_poll_at_ms: int = 0
    post_trigger_done: bool = False

    near_close_snapshot: dict | None = None  # {ts_ms, tokens: [tok0, tok1]}
    near_close_attempted: int = 0
    near_close_next_ms: int = 0

    summary_sent: bool = False

    @property
    def key(self) -> str:
        return f"{self.window_start_ms}:{self.ttl_at_trigger}"

    @property
    def real_entry_count(self) -> int:
        return sum(1 for p in self.polls if (p.get("delta") or 0) > DELTA_ENTRY_THRESHOLD)


# ---------------------------------------------------------------------------
# Discord message formatting
# ---------------------------------------------------------------------------

def _fmt_pm_token_line(snapshot: dict, token_idx: int, outcomes: list[str]) -> str:
    ts_str = datetime.fromtimestamp(
        snapshot["ts_ms"] / 1000, tz=timezone.utc
    ).strftime("%H:%M:%S")
    t = snapshot["tokens"][token_idx]
    label = outcomes[token_idx] if token_idx < len(outcomes) else f"t{token_idx}"
    return (
        f"  {ts_str}  {label:3s}: "
        f"ask={t['ask']}({t['ask_size']})  bid={t['bid']}({t['bid_size']})"
    )


def format_contrarian_summary(
    job: ContrarianJob, market: dict, settlement: float,
) -> str:
    outs = job.outcomes or ["YES", "NO"]
    target_label = outs[job.target_idx] if job.target_idx < len(outs) else "target"
    settle_label = outs[0] if settlement > job.strike else outs[1]
    hit = (
        (job.action == "BUY NO" and settlement <= job.strike) or
        (job.action == "BUY YES" and settlement > job.strike)
    )
    result = "\u2705" if hit else "\u274c"
    emoji = "\U0001f534" if job.action == "BUY NO" else "\U0001f7e2"
    delta_pct = (settlement - job.strike) / job.strike * 100
    window_close_utc = datetime.fromtimestamp(
        job.window_end_ms / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M UTC")
    trigger_utc = datetime.fromtimestamp(
        job.fired_at_ms / 1000, tz=timezone.utc
    ).strftime("%H:%M:%S")

    lines = [
        f"{emoji} **Contrarian {job.action}** | P(Yes)=`{job.prob:.4f}`  "
        f"fair {target_label}=`{job.fair:.4f}`  TTL={job.ttl_at_trigger}",
        f"{market.get('title', '')}",
        f"Window: {window_close_utc}  (trigger {trigger_utc})",
        f"Strike=`{job.strike:.1f}`  BTC@trigger=`{job.btc_close:.1f}`  "
        f"Settle=`{settlement:.1f}` ({delta_pct:+.2f}%) -> {settle_label} {result}",
        "",
    ]

    # Delta stats over 10 polls
    deltas = [p["delta"] for p in job.polls if p.get("delta") is not None]
    if deltas:
        lines.append(
            f"Real entries: **{job.real_entry_count}/{len(job.polls)}** "
            f"(delta > {DELTA_ENTRY_THRESHOLD})  "
            f"delta min=`{min(deltas):.4f}` "
            f"med=`{float(np.median(deltas)):.4f}` "
            f"max=`{max(deltas):.4f}`"
        )

    # Per-poll table
    if job.polls:
        lines.append("```")
        lines.append(f"  {'time':8s}  {'ask':6s}  {'delta':7s}")
        for p in job.polls:
            ts_str = datetime.fromtimestamp(
                p["ts_ms"] / 1000, tz=timezone.utc
            ).strftime("%H:%M:%S")
            ask_s = f"{p['ask']:.4f}" if p.get("ask") is not None else "  -   "
            delta_s = f"{p['delta']:+.4f}" if p.get("delta") is not None else "   -   "
            entry = " *" if (p.get("delta") or 0) > DELTA_ENTRY_THRESHOLD else ""
            lines.append(f"  {ts_str}  {ask_s}  {delta_s}{entry}")
        lines.append("```")

    # Near-close both tokens
    if job.near_close_snapshot:
        nc_ts = datetime.fromtimestamp(
            job.near_close_snapshot["ts_ms"] / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S")
        lines.append(f"Near-close @ {nc_ts}:")
        for i, tok in enumerate(job.near_close_snapshot["tokens"]):
            label = outs[i] if i < len(outs) else f"t{i}"
            ask = tok.get("ask")
            bid = tok.get("bid")
            ask_s = f"{ask:.4f}" if ask is not None else "-"
            bid_s = f"{bid:.4f}" if bid is not None else "-"
            marker = "  <- target" if i == job.target_idx else ""
            lines.append(f"  {label:4s}: ask=`{ask_s}`  bid=`{bid_s}`{marker}")
    else:
        lines.append("Near-close: (no snapshot)")

    return "\n".join(lines)


def format_epoch_close_alert(
    market: dict, pm_snapshot: dict, strike: float, settlement: float,
) -> str:
    outcome = "YES" if settlement > strike else "NO"
    delta_pct = (settlement - strike) / strike * 100
    outcomes = market.get("outcomes", ["YES", "NO"])
    lines = [
        f"\U0001f3c1 **Epoch Close** — {market.get('title', '')}",
        f"Strike=`{strike:.1f}` | Settlement=`{settlement:.1f}` ({delta_pct:+.2f}%) | **{outcome}**",
    ]
    if pm_snapshot:
        lines.append("```")
        for i in range(len(pm_snapshot.get("tokens", []))):
            lines.append(_fmt_pm_token_line(pm_snapshot, i, outcomes))
        lines.append("```")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Artifact loading with daily rollover
# ---------------------------------------------------------------------------

def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def load_best_artifact(artifact_dir: Path) -> DailySignalArtifactV3 | None:
    """Load today's artifact if available, else most recent prior date."""
    today = _today_utc()
    today_path = artifact_dir / today
    if today_path.exists():
        return DailySignalArtifactV3.load(today_path)
    # Fall back to most recent date-named subdir <= today
    candidates = sorted(
        (p for p in artifact_dir.iterdir() if p.is_dir() and p.name <= today),
        key=lambda p: p.name,
        reverse=True,
    )
    if not candidates:
        return None
    return DailySignalArtifactV3.load(candidates[0])


def maybe_reload_artifact(
    artifact: DailySignalArtifactV3,
    artifact_dir: Path,
) -> DailySignalArtifactV3:
    """If today's artifact differs from loaded one, swap it in."""
    today = _today_utc()
    if artifact.score_date == today:
        return artifact
    today_path = artifact_dir / today
    if not today_path.exists():
        return artifact
    try:
        new_artifact = DailySignalArtifactV3.load(today_path)
        print(f"[{fmt_now()}] Artifact rollover: {artifact.score_date} -> "
              f"{new_artifact.score_date}")
        return new_artifact
    except Exception as e:
        print(f"[{fmt_now()}] Artifact reload failed: {e}")
        return artifact


# ---------------------------------------------------------------------------
# Main stream loop
# ---------------------------------------------------------------------------

def run_stream(
    db_path: Path,
    artifact_dir: Path,
    prob_high: float,
    prob_low: float,
    webhook_key: str,
    debug: bool = False,
) -> int:
    # Load artifact (falls back to most recent if today's isn't built yet)
    artifact = load_best_artifact(artifact_dir)
    if artifact is None:
        print(f"[{fmt_now()}] ERROR: no artifact found in {artifact_dir}")
        return 1
    today = _today_utc()
    if artifact.score_date != today:
        print(f"[{fmt_now()}] WARN: using stale artifact {artifact.score_date} "
              f"(today={today}); will swap when today's is built")
    print(f"[{fmt_now()}] Artifact loaded: score_date={artifact.score_date}, "
          f"{len(artifact.models)} models, z_pool={artifact.z_pool.size}")

    # Load OHLCV buffer from SQLite
    buf = OhlcvBuffer()
    n_loaded = buf.load_from_db(db_path)
    print(f"[{fmt_now()}] OHLCV buffer: {n_loaded} bars")

    # Resolve current epoch + PM market
    epoch_ts = current_epoch_ts()
    market = resolve_market(epoch_ts)
    strike_str = fetch_strike(epoch_ts)
    strike: float | None = float(strike_str) if strike_str else None
    if market:
        print(f"[{fmt_now()}] Market: {market['title']}, strike={strike}")

    # Start kline feed
    kline = KlineFeed()
    kline.start()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    print(f"[{fmt_now()}] Streaming — prob_high={prob_high}, prob_low={prob_low}")

    jobs: list[ContrarianJob] = []

    while not _shutdown:
        # 1s timeout so scheduled polls stay crisp
        kline.bar_ready.wait(timeout=LOOP_TIMEOUT_S)
        kline.bar_ready.clear()

        # Daily artifact rollover (cheap filesystem stat)
        artifact = maybe_reload_artifact(artifact, artifact_dir)

        # Retry market resolution if needed
        if market is None:
            epoch_ts = current_epoch_ts()
            market = resolve_market(epoch_ts)
            if market:
                print(f"[{fmt_now()}] Market resolved: {market['title']}")
                if strike is None:
                    s = fetch_strike(epoch_ts)
                    strike = float(s) if s else None

        # --- Process completed kline bars ---
        for bar in kline.drain():
            ttl = compute_ttl(bar["t"])
            buf.append(bar["h"], bar["l"], bar["c"])
            bar_ts = datetime.fromtimestamp(
                bar["t"] / 1000, tz=timezone.utc
            ).strftime("%H:%M:%S")
            win_start = (bar["t"] // WINDOW_MS) * WINDOW_MS
            win_end = win_start + WINDOW_MS

            # --- Window close: summarize jobs in the closing window ---
            if ttl == GROUP_MINUTES and market and strike:
                settlement = bar["c"]
                for job in jobs:
                    if job.window_start_ms != win_start or job.summary_sent:
                        continue
                    # Last-chance near-close poll
                    if job.near_close_snapshot is None:
                        snap = _safe_fetch(market["token_ids"])
                        if snap:
                            tokens = [_extract_token(snap, i)
                                      for i in range(len(snap.get("tokens", [])))]
                            job.near_close_snapshot = {"ts_ms": snap["ts_ms"], "tokens": tokens}
                    # Only send if at least one real entry
                    if job.real_entry_count > 0:
                        msg = format_contrarian_summary(job, market, settlement)
                        send_discord(msg, env_key=webhook_key)
                        print(f"[{fmt_now()}] Summary sent for {job.key} "
                              f"(real_entries={job.real_entry_count})")
                    else:
                        print(f"[{fmt_now()}] No real entry for {job.key}, skipping Discord")
                    job.summary_sent = True
                jobs = [j for j in jobs if not j.summary_sent]

                # Transition to new epoch
                new_epoch = win_end // 1000
                market = resolve_market(new_epoch)
                strike = bar["c"]
                epoch_ts = new_epoch
                if market:
                    print(f"[{fmt_now()}] New epoch: {market['title']}, "
                          f"strike={strike:.1f}")

            # --- Compute signal and check trigger ---
            features = buf.compute_features()
            if features is not None and strike is not None and market:
                prob = compute_prob_yes(features, ttl, strike, bar["c"], artifact)
                if prob is not None:
                    if debug:
                        print(f"[{fmt_now()}] {bar_ts} TTL={ttl} "
                              f"prob={prob:.4f} close={bar['c']:.1f}")
                    if ttl <= 2 and (prob > prob_high or prob < prob_low):
                        buy_no = prob > prob_high
                        action = "BUY NO" if buy_no else "BUY YES"
                        fair = (1.0 - prob) if buy_no else prob
                        target_idx = 1 if buy_no else 0
                        outs = list(market.get("outcomes") or ["YES", "NO"])
                        job = ContrarianJob(
                            fired_at_ms=bar["t"] + 60_000,
                            window_start_ms=win_start,
                            window_end_ms=win_end,
                            ttl_at_trigger=ttl,
                            prob=prob,
                            fair=fair,
                            strike=strike,
                            btc_close=bar["c"],
                            action=action,
                            target_idx=target_idx,
                            outcomes=outs,
                            next_poll_at_ms=now_ms(),
                            near_close_next_ms=win_end - NEAR_CLOSE_OFFSET_MS,
                        )
                        jobs.append(job)
                        print(f"[{fmt_now()}] TRIGGER: {action} prob={prob:.4f} "
                              f"fair={fair:.4f} TTL={ttl}")
            elif debug:
                print(f"[{fmt_now()}] {bar_ts} TTL={ttl} skip (no features/strike/market)")

        # --- Drive active jobs ---
        if not market:
            continue
        cur = now_ms()
        for job in jobs:
            if job.summary_sent:
                continue

            # Post-trigger: 10 polls at 3s intervals
            if not job.post_trigger_done and cur >= job.next_poll_at_ms:
                snap = _safe_fetch(market["token_ids"])
                if snap:
                    tok = _extract_token(snap, job.target_idx)
                    tok["delta"] = (
                        (tok["ask"] - job.fair) if tok.get("ask") is not None else None
                    )
                    job.polls.append(tok)
                job.post_trigger_attempted += 1
                job.next_poll_at_ms = cur + POLL_INTERVAL_MS
                if job.post_trigger_attempted >= ALERT_POLL_COUNT:
                    job.post_trigger_done = True

            # Near-close: T-2s, retry once at T-1s, capture both tokens
            if (job.post_trigger_done
                    and job.near_close_snapshot is None
                    and job.near_close_attempted < 2
                    and cur >= job.near_close_next_ms):
                snap = _safe_fetch(market["token_ids"])
                job.near_close_attempted += 1
                if snap:
                    tokens = [_extract_token(snap, i)
                              for i in range(len(snap.get("tokens", [])))]
                    job.near_close_snapshot = {"ts_ms": snap["ts_ms"], "tokens": tokens}
                else:
                    retry_at = job.window_end_ms - NEAR_CLOSE_RETRY_MS
                    if job.near_close_attempted < 2 and cur < retry_at:
                        job.near_close_next_ms = retry_at

    kline.stop()
    print(f"[{fmt_now()}] Stream stopped.")
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Live V3 signal stream with PM order book alerts",
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to 1m OHLCV SQLite database")
    p.add_argument("--artifact-dir", type=Path, default=Path("data/artifacts_v3"),
                   help="Artifact directory (default: data/artifacts_v3)")
    p.add_argument("--prob-high", type=float, default=0.90,
                   help="Upper prob threshold for alert (default: 0.90)")
    p.add_argument("--prob-low", type=float, default=0.10,
                   help="Lower prob threshold for alert (default: 0.10)")
    p.add_argument("--webhook-key", type=str, default="DISCORD_WEBHOOK_URL_SIGNAL_V3",
                   help="Env var for Discord webhook (default: DISCORD_WEBHOOK_URL_SIGNAL_V3)")
    p.add_argument("--debug", action="store_true", help="Verbose output")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    return run_stream(
        db_path=args.db,
        artifact_dir=args.artifact_dir,
        prob_high=args.prob_high,
        prob_low=args.prob_low,
        webhook_key=args.webhook_key,
        debug=args.debug,
    )


if __name__ == "__main__":
    raise SystemExit(main())
