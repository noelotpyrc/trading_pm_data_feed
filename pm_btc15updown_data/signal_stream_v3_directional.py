#!/usr/bin/env python3
"""
Live V3 directional signal stream.

Fires on these TTL/prob combinations (directional — buy the side the model
leans toward):
  TTL=9  prob > 0.88  -> YES
  TTL=4  prob < 0.04  -> NO
  TTL=5  prob < 0.10  -> NO
  TTL=6  prob < 0.06  -> NO

On trigger:
  - Polls the target PM token (YES or NO) every 3s, 8 times (~21s window)
  - Near window close (T-2s; retry T-1s on failure), one more poll
  - At window close (TTL=15 bar close event), sends one Discord summary per
    trigger: ask min/max/avg over the 8 polls + near-close ask.
  - Raw trigger / poll / summary events appended as JSONL to --log-file.

Usage:
  python -m pm_btc15updown_data.signal_stream_v3_directional \
    --db data/btcusdt_perp_1m.sqlite \
    --artifact-dir data/artifacts_v3 \
    --log-file logs/signal_v3_directional.jsonl
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import websocket as ws_client

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from btcusdt_perp_signal.alert import send_discord
from pm_btc15updown_artifact.vol_signal_artifacts_v3 import DailySignalArtifactV3
from pm_btc15updown_data.collect_pm_btcupdown import (
    resolve_market,
    fetch_prices,
    fetch_strike,
    current_epoch_ts,
    fmt_now,
)
from pm_btc15updown_data.signal_stream_v3 import (
    OhlcvBuffer,
    compute_ttl,
    compute_prob_yes,
    load_best_artifact,
    maybe_reload_artifact,
    _today_utc,
)

KLINE_WS = "wss://fstream.binance.com/ws/btcusdt@kline_1m"
LOOP_TIMEOUT_S = 1.0
POLL_INTERVAL_MS = 3_000
POLL_COUNT = 8
NEAR_CLOSE_OFFSET_MS = 2_000   # first near-close attempt at T-2s
NEAR_CLOSE_RETRY_MS = 1_000    # retry at T-1s
GROUP_MINUTES = 15
WINDOW_MS = GROUP_MINUTES * 60 * 1000

TRIGGERS = [
    {"ttl": 9, "op": "gt", "threshold": 0.88, "target": "YES"},
    {"ttl": 4, "op": "lt", "threshold": 0.04, "target": "NO"},
    {"ttl": 5, "op": "lt", "threshold": 0.10, "target": "NO"},
    {"ttl": 6, "op": "lt", "threshold": 0.06, "target": "NO"},
]

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    print(f"\n[{fmt_now()}] Caught {signal.Signals(sig).name}, shutting down...")
    _shutdown = True


# ---------------------------------------------------------------------------
# Kline WS feed (duplicated from signal_stream_v3 so shutdown is local)
# ---------------------------------------------------------------------------

class KlineFeed:
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
        out = []
        while self.pending:
            out.append(self.pending.popleft())
        return out

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
                    if k.get("x"):
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
# JSONL logger
# ---------------------------------------------------------------------------

class JsonlLogger:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.file = open(path, "a", buffering=1)  # line-buffered

    def write(self, event: str, **data) -> None:
        rec = {
            "event": event,
            "ts_ms": int(time.time() * 1000),
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            **data,
        }
        self.file.write(json.dumps(rec, separators=(",", ":")) + "\n")

    def close(self) -> None:
        try:
            self.file.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Trigger job
# ---------------------------------------------------------------------------

@dataclass
class TriggerJob:
    fired_at_ms: int            # ~bar close time
    window_start_ms: int
    window_end_ms: int
    ttl_at_trigger: int
    prob: float
    strike: float
    btc_close: float
    target_outcome: str         # "YES" | "NO"
    target_idx: int             # 0 | 1
    market_slug: str
    market_title: str

    polls: list[dict] = field(default_factory=list)
    post_trigger_attempted: int = 0
    next_poll_at_ms: int = 0
    post_trigger_done: bool = False

    near_close_poll: dict | None = None
    near_close_attempted: int = 0
    near_close_next_ms: int = 0

    summary_sent: bool = False

    @property
    def key(self) -> str:
        return f"{self.window_start_ms}:{self.ttl_at_trigger}"


# ---------------------------------------------------------------------------
# Helpers
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


def _resolve_target_idx(market: dict, target: str) -> int:
    outcomes = market.get("outcomes") or ["YES", "NO"]
    upper = [o.upper() for o in outcomes]
    if target in upper:
        return upper.index(target)
    return 0 if target == "YES" else 1


def _format_summary(job: TriggerJob, settlement: float) -> str:
    asks = [p["ask"] for p in job.polls if p.get("ask") is not None]
    window_close_utc = datetime.fromtimestamp(
        job.window_end_ms / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M UTC")
    trigger_utc = datetime.fromtimestamp(
        job.fired_at_ms / 1000, tz=timezone.utc
    ).strftime("%H:%M:%S")
    delta_pct = (settlement - job.strike) / job.strike * 100
    settle_outcome = "YES" if settlement > job.strike else "NO"
    hit = "\u2705" if settle_outcome == job.target_outcome else "\u274c"
    emoji = "\U0001f7e2" if job.target_outcome == "YES" else "\U0001f534"

    lines = [
        f"{emoji} **Directional {job.target_outcome}** "
        f"| TTL={job.ttl_at_trigger} prob=`{job.prob:.4f}`",
        f"{job.market_title}",
        f"Window close: {window_close_utc}  (trigger {trigger_utc})",
        f"Strike=`{job.strike:.1f}`  BTC@trigger=`{job.btc_close:.1f}`  "
        f"Settle=`{settlement:.1f}` ({delta_pct:+.2f}%) -> {settle_outcome} {hit}",
        "",
    ]
    if asks:
        lines.append(
            f"**{job.target_outcome} ask** over {len(asks)} polls: "
            f"min=`{min(asks):.4f}`  max=`{max(asks):.4f}`  "
            f"avg=`{sum(asks)/len(asks):.4f}`"
        )
    else:
        lines.append("**No post-trigger polls captured a valid ask.**")
    if job.near_close_poll and job.near_close_poll.get("ask") is not None:
        nc_ts = datetime.fromtimestamp(
            job.near_close_poll["ts_ms"] / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S")
        lines.append(
            f"Near-close ask: `{job.near_close_poll['ask']:.4f}` @ {nc_ts}"
        )
    else:
        lines.append("Near-close ask: (no snapshot)")
    return "\n".join(lines)


def _summary_payload(job: TriggerJob) -> dict:
    asks = [p["ask"] for p in job.polls if p.get("ask") is not None]
    return {
        "window_start_ms": job.window_start_ms,
        "ttl": job.ttl_at_trigger,
        "prob": round(job.prob, 6),
        "target": job.target_outcome,
        "strike": job.strike,
        "btc_close_trigger": job.btc_close,
        "n_polls_total": len(job.polls),
        "n_polls_valid_ask": len(asks),
        "ask_min": round(min(asks), 6) if asks else None,
        "ask_max": round(max(asks), 6) if asks else None,
        "ask_avg": round(sum(asks) / len(asks), 6) if asks else None,
        "near_close_ask": job.near_close_poll["ask"] if job.near_close_poll else None,
        "near_close_ts_ms": job.near_close_poll["ts_ms"] if job.near_close_poll else None,
    }


# ---------------------------------------------------------------------------
# Main stream loop
# ---------------------------------------------------------------------------

def run_stream(
    db_path: Path,
    artifact_dir: Path,
    log_path: Path,
    webhook_key: str,
    debug: bool = False,
) -> int:
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

    buf = OhlcvBuffer()
    n_loaded = buf.load_from_db(db_path)
    print(f"[{fmt_now()}] OHLCV buffer: {n_loaded} bars")

    epoch_ts = current_epoch_ts()
    market = resolve_market(epoch_ts)
    strike_str = fetch_strike(epoch_ts)
    strike: float | None = float(strike_str) if strike_str else None
    if market:
        print(f"[{fmt_now()}] Market: {market['title']}, strike={strike}")

    logger = JsonlLogger(log_path)
    print(f"[{fmt_now()}] Logging to {log_path}")

    kline = KlineFeed()
    kline.start()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    rule_summary = ", ".join(
        f"TTL={r['ttl']}{'>' if r['op']=='gt' else '<'}{r['threshold']}->{r['target']}"
        for r in TRIGGERS
    )
    print(f"[{fmt_now()}] Directional rules: {rule_summary}")

    jobs: list[TriggerJob] = []

    while not _shutdown:
        kline.bar_ready.wait(timeout=LOOP_TIMEOUT_S)
        kline.bar_ready.clear()

        artifact = maybe_reload_artifact(artifact, artifact_dir)

        if market is None:
            epoch_ts = current_epoch_ts()
            market = resolve_market(epoch_ts)
            if market:
                print(f"[{fmt_now()}] Market resolved: {market['title']}")
                if strike is None:
                    s = fetch_strike(epoch_ts)
                    strike = float(s) if s else None

        # -- Drain completed bars --
        for bar in kline.drain():
            ttl = compute_ttl(bar["t"])
            buf.append(bar["h"], bar["l"], bar["c"])
            win_start = (bar["t"] // WINDOW_MS) * WINDOW_MS
            win_end = win_start + WINDOW_MS

            # --- Window close: summarize all jobs in the closing window ---
            if ttl == GROUP_MINUTES and market and strike:
                settlement = bar["c"]
                closing_win = win_start
                for job in jobs:
                    if job.window_start_ms != closing_win or job.summary_sent:
                        continue
                    # Last-chance near-close poll
                    if job.near_close_poll is None:
                        snap = _safe_fetch(market["token_ids"])
                        if snap:
                            tok = _extract_token(snap, job.target_idx)
                            if tok.get("ask") is not None:
                                job.near_close_poll = tok
                                logger.write(
                                    "poll", trigger_key=job.key,
                                    kind="near_close_fallback", **tok,
                                )
                    msg = _format_summary(job, settlement)
                    ok = send_discord(msg, env_key=webhook_key)
                    logger.write(
                        "summary", trigger_key=job.key, discord_ok=ok,
                        settlement=settlement, **_summary_payload(job),
                    )
                    print(f"[{fmt_now()}] Summary sent for {job.key} "
                          f"(discord_ok={ok})")
                    job.summary_sent = True

                jobs = [j for j in jobs if not j.summary_sent]

                # Transition to new epoch (matches v3 behavior)
                new_epoch = win_end // 1000
                market = resolve_market(new_epoch)
                strike = bar["c"]
                epoch_ts = new_epoch
                if market:
                    print(f"[{fmt_now()}] New epoch: {market['title']}, "
                          f"strike={strike:.1f}")

            # --- Trigger check ---
            features = buf.compute_features()
            if features is None or strike is None or market is None:
                if debug:
                    print(f"[{fmt_now()}] TTL={ttl} skip "
                          f"(features={features is not None}, strike={strike}, market={bool(market)})")
                continue
            prob = compute_prob_yes(features, ttl, strike, bar["c"], artifact)
            if prob is None:
                continue
            if debug:
                bar_ts = datetime.fromtimestamp(
                    bar["t"] / 1000, tz=timezone.utc
                ).strftime("%H:%M:%S")
                print(f"[{fmt_now()}] {bar_ts} TTL={ttl} prob={prob:.4f} "
                      f"close={bar['c']:.1f}")

            for rule in TRIGGERS:
                if rule["ttl"] != ttl:
                    continue
                fired = (
                    (rule["op"] == "gt" and prob > rule["threshold"]) or
                    (rule["op"] == "lt" and prob < rule["threshold"])
                )
                if not fired:
                    continue
                target = rule["target"]
                target_idx = _resolve_target_idx(market, target)
                job = TriggerJob(
                    fired_at_ms=bar["t"] + 60_000,
                    window_start_ms=win_start,
                    window_end_ms=win_end,
                    ttl_at_trigger=ttl,
                    prob=prob,
                    strike=strike,
                    btc_close=bar["c"],
                    target_outcome=target,
                    target_idx=target_idx,
                    market_slug=market.get("slug", ""),
                    market_title=market.get("title", ""),
                    next_poll_at_ms=now_ms(),
                    near_close_next_ms=win_end - NEAR_CLOSE_OFFSET_MS,
                )
                jobs.append(job)
                print(f"[{fmt_now()}] TRIGGER TTL={ttl} prob={prob:.4f} -> {target}")
                logger.write(
                    "trigger",
                    trigger_key=job.key,
                    window_start_ms=win_start,
                    window_end_ms=win_end,
                    ttl=ttl, prob=prob,
                    strike=strike, btc_close=bar["c"],
                    target=target, target_idx=target_idx,
                    rule_op=rule["op"], rule_threshold=rule["threshold"],
                    market_slug=job.market_slug,
                    market_title=job.market_title,
                )

        # -- Drive active jobs --
        if not market:
            continue
        cur = now_ms()
        for job in jobs:
            if job.summary_sent:
                continue

            # Post-trigger: 8 polls at 3s intervals
            if not job.post_trigger_done and cur >= job.next_poll_at_ms:
                snap = _safe_fetch(market["token_ids"])
                if snap:
                    tok = _extract_token(snap, job.target_idx)
                    job.polls.append(tok)
                    logger.write("poll", trigger_key=job.key,
                                 kind="post_trigger", **tok)
                job.post_trigger_attempted += 1
                job.next_poll_at_ms = cur + POLL_INTERVAL_MS
                if job.post_trigger_attempted >= POLL_COUNT:
                    job.post_trigger_done = True

            # Near-close: T-2s, retry once at T-1s
            if job.post_trigger_done \
               and job.near_close_poll is None \
               and job.near_close_attempted < 2 \
               and cur >= job.near_close_next_ms:
                snap = _safe_fetch(market["token_ids"])
                job.near_close_attempted += 1
                captured = False
                if snap:
                    tok = _extract_token(snap, job.target_idx)
                    if tok.get("ask") is not None:
                        job.near_close_poll = tok
                        logger.write("poll", trigger_key=job.key,
                                     kind="near_close", **tok)
                        captured = True
                if not captured:
                    retry_at = job.window_end_ms - NEAR_CLOSE_RETRY_MS
                    if job.near_close_attempted < 2 and cur < retry_at:
                        job.near_close_next_ms = retry_at
                    # else: window-close fallback will try once more

    kline.stop()
    logger.close()
    print(f"[{fmt_now()}] Stream stopped.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Live V3 directional signal stream",
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to 1m OHLCV SQLite database")
    p.add_argument("--artifact-dir", type=Path, default=Path("data/artifacts_v3"),
                   help="Artifact directory (default: data/artifacts_v3)")
    p.add_argument("--log-file", type=Path,
                   default=Path("logs/signal_v3_directional.jsonl"),
                   help="JSONL log path (default: logs/signal_v3_directional.jsonl)")
    p.add_argument("--webhook-key", type=str,
                   default="DISCORD_WEBHOOK_URL_SIGNAL_V3",
                   help="Env var for Discord webhook")
    p.add_argument("--debug", action="store_true", help="Verbose per-bar output")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    return run_stream(
        db_path=args.db,
        artifact_dir=args.artifact_dir,
        log_path=args.log_file,
        webhook_key=args.webhook_key,
        debug=args.debug,
    )


if __name__ == "__main__":
    raise SystemExit(main())
