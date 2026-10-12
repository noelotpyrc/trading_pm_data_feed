"""
pm_add_asym_probe: record the Polymarket BTC 15m book feed and paper-trade the add_asym_30 bottom signal. No orders.

  live    --data-dir D --cut C --lo L --hi H --cool S --hours N   record + signal, stops after N hours
  replay  --data-dir D                                            rebuild seconds/entries from D/raw into D/replay, diff vs live
  settle  --data-dir D                                            Gamma outcomes -> D/settled.csv + summary
"""
from __future__ import annotations

import argparse
import csv
import glob
import gzip
import json
import logging
import math
import socket
import statistics
import subprocess
import time
import urllib.request
from pathlib import Path

import websocket

from .engine import ENTRY_COLS, SEC_COLS, Engine

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com/markets?slug=btc-updown-15m-{}"     # closed markets need &closed=true
RECONNECT_AT = 300                    # second of each window when the subscription moves to (current, next)
log = logging.getLogger("pm_add_asym_probe")


def gamma(T: int, closed: bool = False) -> dict | None:
    req = urllib.request.Request(GAMMA.format(T) + ("&closed=true" if closed else ""), headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as f:
            ms = json.load(f)
    except Exception as e:
        log.warning("gamma %s: %s", T, e)
        return None
    return ms[0] if isinstance(ms, list) and ms else None


_tokens: dict[int, dict] = {}


def tokens(T: int) -> dict | None:
    """Up / Down token ids of window T; found ids are cached, so reconnects don't wait on Gamma."""
    if T not in _tokens:
        try:
            m = gamma(T)
            ids, outs = json.loads(m["clobTokenIds"]), [o.lower() for o in json.loads(m["outcomes"])]
            u = outs.index("up")
            if len(ids) == 2:
                _tokens[T] = {"T": T, "up": ids[u], "down": ids[1 - u]}
        except Exception as e:
            log.warning("tokens %s: %r", T, e)
    return _tokens.get(T)


class Out:
    """Raw recorder (hourly gzip JSON lines) + CSV appenders."""

    def __init__(self, d: Path, sub: str = "") -> None:
        self.d, self.raw, self.hour = d, None, None
        self.csv = {}
        for name, cols in (("seconds", SEC_COLS), ("entries", ENTRY_COLS)):
            p = d / sub / f"{name}.csv"
            p.parent.mkdir(parents=True, exist_ok=True)
            f = open(p, "a", newline="")
            w = csv.DictWriter(f, cols)
            if f.tell() == 0:
                w.writeheader()
            self.csv[name] = (f, w)

    def record(self, x: dict) -> None:
        h = time.strftime("%Y%m%dT%H", time.gmtime(x["r"]))
        if h != self.hour:
            if self.raw:
                self.raw.close()
            (self.d / "raw").mkdir(exist_ok=True)
            self.raw, self.hour = gzip.open(self.d / "raw" / f"{h}.jsonl.gz", "at"), h
        self.raw.write(json.dumps(x, separators=(",", ":")) + "\n")

    def drain(self, eng: Engine) -> None:
        for name in ("seconds", "entries"):
            rows = getattr(eng, name)
            if rows:
                self.csv[name][1].writerows(rows)
                rows.clear()

    def flush(self) -> None:
        if self.raw:
            self.raw.flush()
        for f, _ in self.csv.values():
            f.flush()

    def close(self) -> None:
        self.flush()
        if self.raw:
            self.raw.close()
        for f, _ in self.csv.values():
            f.close()


def live(a) -> None:
    d = Path(a.data_dir).expanduser()
    if (d / "run.json").exists():
        raise SystemExit(f"{d} already holds a run; use a new --data-dir")
    d.mkdir(parents=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                        handlers=[logging.FileHandler(d / "run.log"), logging.StreamHandler()])
    git = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=Path(__file__).parent).stdout.strip()
    (d / "run.json").write_text(json.dumps({"run": d.name, "start": time.time(), "host": socket.gethostname(), "git": git,
                                            "cut": a.cut, "lo": a.lo, "hi": a.hi, "cool": a.cool, "hours": a.hours}, indent=1))
    eng, out = Engine(a.cut, a.lo, a.hi, a.cool), Out(d)
    end = time.time() + a.hours * 3600

    def feed(x):
        out.record(x)
        eng.on_line(x)
        out.drain(eng)

    try:
        run(eng, out, feed, end)
    finally:
        out.close()
    log.info("done")


def run(eng, out, feed, end) -> None:
    conn, delay, reconnects, gap_at = 0, 1, 0, None
    last_status = last_flush = time.time()
    while time.time() < end:
        now = time.time()
        T = int(now // 900 * 900)
        cur = tokens(T)
        if not cur:
            time.sleep(10)
            continue
        sub = [cur] + [w for w in [tokens(T + 900)] if w]
        stop_at = min(end, T + RECONNECT_AT if now - T < RECONNECT_AT else T + 900 + RECONNECT_AT)
        conn += 1
        ws, why = None, "scheduled"
        try:
            ws = websocket.create_connection(WS_URL, timeout=10)
            ws.send(json.dumps({"type": "market", "assets_ids": [k for w in sub for k in (w["up"], w["down"])],
                                "custom_feature_enabled": True}))
            ws.settimeout(0.2)
            feed({"r": time.time(), "c": conn, "sub": sub})
            log.info("conn %d: windows %s", conn, [w["T"] for w in sub])
            last_ping = last_frame = time.time()
            while (now := time.time()) < stop_at:
                if now - last_ping >= 10:
                    ws.send("PING")
                    last_ping = now
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    raw = None
                now = time.time()
                if raw:
                    feed({"r": now, "c": conn, "m": raw})
                    last_frame, delay = now, 1
                    if gap_at is not None:
                        log.info("conn %d first frame %.1f s after the last close", conn, now - gap_at)
                        gap_at = None
                elif now - last_frame > 30:                # PONG comes every 10 s: 30 s of silence = dead socket
                    raise TimeoutError("no frame for 30 s")
                else:
                    eng.tick(now)
                    out.drain(eng)
                if now - last_flush >= 10:
                    out.flush()
                    last_flush = now
                if now - last_status >= 60:
                    lag = statistics.median(eng.lags) if eng.lags else math.nan
                    log.info("conn %d frames %d levels %d books %d bad %d late %d reconnects %d gaps %d lag_ms %.0f",
                             conn, eng.n["frames"], eng.n["levels"], eng.n["books"], eng.n["bad"], eng.n["late"], reconnects,
                             len(eng.gaps), lag)
                    eng.lags.clear()
                    last_status = now
        except Exception as e:
            why = f"error: {e!r}"
            log.warning("conn %d %s; reconnect in %d s", conn, why, delay)
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
            feed({"r": time.time(), "c": conn, "close": why})
            gap_at = gap_at or time.time()
        if why != "scheduled":
            reconnects += 1
            time.sleep(delay)
            delay = {1: 2, 2: 5}.get(delay, 5)


def read_csv(p: Path) -> list[dict]:
    with open(p, newline="") as f:
        return list(csv.DictReader(f))


def replay(a) -> None:
    d = Path(a.data_dir).expanduser()
    cfg = json.loads((d / "run.json").read_text())
    sub = "replay_no_timer" if a.no_timer else "replay"
    for name in ("seconds", "entries"):
        (d / sub / f"{name}.csv").unlink(missing_ok=True)
    eng, out = Engine(cfg["cut"], cfg["lo"], cfg["hi"], cfg["cool"]), Out(d, sub)
    if a.no_timer:                                        # research order: a second waits for the first later event
        eng.tick = lambda now: None
    for p in sorted(glob.glob(str(d / "raw" / "*.jsonl.gz"))):
        with gzip.open(p, "rt") as f:
            try:
                for line in f:
                    try:
                        x = json.loads(line)
                    except json.JSONDecodeError:          # torn last line after a crash
                        continue
                    eng.on_line(x)
                    out.drain(eng)
                    eng.lags.clear()
            except EOFError:                              # hour file not closed (crash, kill, or still being written)
                pass
    out.close()
    for name, skip in (("seconds", {"wall"}), ("entries", {"delay"})):
        A, B = read_csv(d / f"{name}.csv"), read_csv(d / sub / f"{name}.csv")
        n = min(len(A), len(B))
        bad = sum(any(x[k] != y[k] for k in x if k not in skip) for x, y in zip(A[:n], B[:n]))
        print(f"{name}: live {len(A):,} rows, replay {len(B):,} rows, differing rows {bad:,} (of the first {n:,})")


def settle(a) -> None:
    d = Path(a.data_dir).expanduser()
    E = read_csv(d / "entries.csv")
    win = {}
    for T in sorted({int(x["T"]) for x in E}):
        m = gamma(T, closed=True)
        px = [float(p) for p in json.loads(m["outcomePrices"])] if m and m.get("closed") else []
        if sorted(px) == [0.0, 1.0]:
            outs = [o.lower() for o in json.loads(m["outcomes"])]
            win[T] = {"up": px[outs.index("up")], "down": px[outs.index("down")]}
    rows = []
    for x in E:
        T, e = int(x["T"]), float(x["e"])
        if T in win:
            y = win[T][x["token"]]
            rows.append({**x, "win": y, "net": (y - e - 0.07 * e * (1 - e)) * 100})
    if not rows:
        print("no settled entries yet")
        return
    with open(d / "settled.csv", "w", newline="") as f:
        w = csv.DictWriter(f, list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    days = len({x["T"] for x in read_csv(d / "seconds.csv")}) / 96          # windows with signal seconds / 96
    print(f"windows settled {len(win)} of {len({x['T'] for x in E})}; span {days:.2f} days")
    for lab, sel in (("all", rows), ("gap 0", [x for x in rows if x["gap"] == "0"]), ("gap 1", [x for x in rows if x["gap"] == "1"])):
        if not sel:
            continue
        y = [x["net"] for x in sel]
        mu = sum(y) / len(y)
        cl = {}
        for x in sel:
            cl[x["T"]] = cl.get(x["T"], 0.0) + x["net"] - mu
        se = math.sqrt(sum(v * v for v in cl.values())) / len(y)
        print(f"{lab}: entries {len(y):,}, windows {len(cl):,}, per day {len(y) / days:.1f}, mean entry {sum(float(x['e']) for x in sel) / len(y):.3f}, "
              f"win rate {sum(x['win'] for x in sel) / len(y):.3f}, net {mu:+.2f} c/share (t {mu / se if se else math.nan:+.2f}), "
              f"total ${sum(y) * 10 / 100:+.2f}")


def main() -> None:
    p = argparse.ArgumentParser(prog="pm_add_asym_probe")
    sp = p.add_subparsers(dest="cmd", required=True)
    lv = sp.add_parser("live")
    for k in ("cut", "lo", "hi", "cool", "hours"):
        lv.add_argument(f"--{k}", type=float, required=True)
    rp = sp.add_parser("replay")
    rp.add_argument("--no-timer", action="store_true", help="emit a second only when a later event arrives (research order)")
    for c in (lv, rp, sp.add_parser("settle")):
        c.add_argument("--data-dir", required=True)
    a = p.parse_args()
    {"live": live, "replay": replay, "settle": settle}[a.cmd](a)


if __name__ == "__main__":
    main()
