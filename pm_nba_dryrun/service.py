"""One process, independent supervised workers, public data endpoints only."""
from __future__ import annotations

import json
import logging
import queue
import re
import shutil
import subprocess
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from http.client import HTTPException

from .book_capture import BookCapture, WireWriter, record_sports
from .espn_live_log import ESPNLog
from .sources import ESPN, GAMMA, HTTP, MARKET_WS, SPORTS_WS, discover, espn_date, map_event, resolved
from .storage import append, day, now_ms

LOG = logging.getLogger(__name__)
MINUTE = 60_000
MAX_GAME_MS = 8 * 60 * MINUTE


class Games:
    def __init__(self, store):
        self.store = store
        self.lock = threading.RLock()
        self.records = {}
        self.runtime = {}

    def all(self):
        with self.lock:
            return deepcopy(self.records)

    def update(self, slug, changes, recv_ms):
        with self.lock:
            old = self.records.get(slug, {})
            new = {**old, **changes, "recv_ms": recv_ms}
            if old.get("closed"):
                new["closed"] = True
                new["closedTime"] = old.get("closedTime") or new.get("closedTime")
            if not old:
                new.update(espn_event=None, team0_home=None, mapping_status="pending")
                self.runtime[slug] = {"eligible": not new["closed"], "stopped": False}
            if {k: v for k, v in old.items() if k != "recv_ms"} == {k: v for k, v in new.items() if k != "recv_ms"}:
                return
            if new.get("closed") and not old.get("closed"):
                self.runtime[slug]["close_seen_ms"] = recv_ms
            self.records[slug] = new
            append(self.store, f"games/{day(recv_ms)}.jsonl", "game", new)

    def espn(self, slug, state, latest_new, recv_ms):
        with self.lock:
            r = self.runtime[slug]
            if latest_new is not None:
                r["last_play_ms"] = latest_new
            r["espn_state"] = state
            if state == "post":
                r.setdefault("post_ms", recv_ms)
            else:
                r.pop("post_ms", None)

    def active(self, ms):
        with self.lock:
            active = {}
            for slug, game in self.records.items():
                r = self.runtime[slug]
                if not r["eligible"] or r["stopped"] or ms < game["kickoff_ms"] - 30 * MINUTE:
                    continue
                if ms >= game["kickoff_ms"] + MAX_GAME_MS:
                    append(self.store, f"health/{day(ms)}.jsonl", "game_timeout",
                           {"recv_ms": ms, "kind": "game_timeout", "slug": slug,
                            "reason": "kickoff_plus_8h", "cutoff_ms": game["kickoff_ms"] + MAX_GAME_MS,
                            "mapping_status": game["mapping_status"], "closed": game["closed"],
                            "espn_state": r.get("espn_state"), "last_play_ms": r.get("last_play_ms")})
                    r["stopped"] = True
                    continue
                # Never infer the last play from silence or a scheduled duration.
                # A final summary and a market close must both have been seen.
                tail = r.get("last_play_ms", r.get("post_ms"))
                if game["closed"] and r.get("espn_state") == "post" and tail is not None and ms >= max(
                        r.get("close_seen_ms", ms), tail + 30 * MINUTE):
                    r["stopped"] = True
                    continue
                active[slug] = deepcopy(game)
            return active

    def routes(self, ms):
        return {token: (slug, i, game["conditionId"]) for slug, game in self.active(ms).items()
                for i, token in enumerate(game["clobTokenIds"])}


def ntp_sample():
    """Read-only clock diagnostics. Never set the system clock or hide failure."""
    if shutil.which("chronyc"):
        command = ["chronyc", "-n", "tracking"]
        style = "chrony"
    elif shutil.which("ntpq"):
        command = ["ntpq", "-pn"]
        style = "ntpq"
    elif shutil.which("sntp"):
        command = ["sntp", "-t", "3", "time.apple.com"]
        style = "sntp"
    else:
        return {"offset_ms": None, "error": "No chronyc, ntpq, or sntp found"}
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=6, check=False)
        raw = result.stdout + result.stderr
        offset = None
        if result.returncode == 0:
            if style == "chrony":
                match = re.search(r"System time\s*:\s*([0-9.]+) seconds (fast|slow)", raw)
                if match and re.search(r"Leap status\s*:\s*Normal", raw):
                    offset = float(match[1]) * (1000 if match[2] == "fast" else -1000)
            elif style == "ntpq":
                selected = next((line.split() for line in raw.splitlines() if line.startswith("*")), None)
                if selected and len(selected) >= 10:
                    offset = -float(selected[8])  # ntpq reports peer minus system.
            else:
                match = re.search(r"(?:^|\s)([+-]\d+(?:\.\d+)?)\s+\+/-", raw)
                if match:
                    offset = -float(match[1]) * 1000
        return {"source": style, "offset_ms": offset, "raw": raw,
                "error": None if offset is not None else "NTP offset unavailable or clock unsynchronized"}
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        return {"source": style, "offset_ms": None, "error": str(exc)}


def read_frame(sock, websocket):
    """(text, is_control). recv() hides protocol pings, so liveness would only see data frames."""
    if not hasattr(sock, "recv_data_frame"):
        return sock.recv(), False
    opcode, frame = sock.recv_data_frame(True)
    if opcode in (websocket.ABNF.OPCODE_PING, websocket.ABNF.OPCODE_PONG):
        return None, True
    if opcode == websocket.ABNF.OPCODE_CLOSE:
        return "", False
    return frame.data, False


class Service:
    def __init__(self, store, *, http=None, clock=now_ms, socket_factory=None, backup_to=None):
        self.store = store
        self.http = http or HTTP()
        self.clock = clock
        self.socket_factory = socket_factory
        self.backup_to = backup_to
        self.games = Games(store)
        self.book = BookCapture(store)
        self.espn_log = ESPNLog(store)
        self.stop = threading.Event()
        self.failures = queue.Queue()
        self.threads = []
        self.metrics = Counter()
        self.metrics_lock = threading.Lock()
        self.subscribed = set()

    def event(self, kind, *, recv_ms=None, **details):
        ms = self.clock() if recv_ms is None else recv_ms
        append(self.store, f"health/{day(ms)}.jsonl", kind, {"recv_ms": ms, "kind": kind, **details})
        with self.metrics_lock:
            self.metrics[f"{details.get('source', 'service')}_{kind}"] += 1

    def poll(self, source, base, path, **params):
        start = self.clock()
        try:
            payload, recv = self.http.get(base, path, **params)
        except (OSError, ValueError, HTTPException) as exc:
            self.event("poll_error", source=source, path=path, params=params, started_ms=start, error=str(exc))
            return None
        self.event("poll", source=source, path=path, params=params, started_ms=start, response_recv_ms=recv)
        return payload, recv

    def discovery_worker(self):
        next_hour = 0
        refreshed = set()
        next_market = {}
        while not self.stop.is_set():
            ms = self.clock()
            games = self.games.all()
            due = {(s, g["kickoff_ms"]) for s, g in games.items() if (s, g["kickoff_ms"]) not in refreshed and ms >= g["kickoff_ms"] - 35 * MINUTE
                   and ms < g["kickoff_ms"]}
            if ms >= next_hour or due:
                try:
                    for game in discover(self.http, ms):
                        self.games.update(game["slug"], game, game["recv_ms"])
                    self.event("discovery", games=len(self.games.all()))
                    refreshed.update(due)
                    next_hour = ms + 60 * MINUTE
                except (OSError, ValueError, KeyError, TypeError, HTTPException, StopIteration) as exc:
                    self.event("poll_error", source="gamma_discovery", error=str(exc))
                    # Retain the due refresh and retry without a tight error loop.
                    self.stop.wait(60)
                    continue
            for slug, game in self.games.active(ms).items():
                if ms < next_market.get(slug, 0):
                    continue
                next_market[slug] = ms + MINUTE
                result = self.poll("gamma_close", GAMMA, f"/markets/{game['market_id']}")
                if result:
                    market, recv = result
                    if not isinstance(market, dict) or "closed" not in market:
                        self.event("poll_error", source="gamma_close", slug=slug, error="Invalid market response")
                        continue
                    self.games.update(slug, {"closed": bool(market.get("closed")), "closedTime": market.get("closedTime")}, recv)
            self.stop.wait(1)

    def scoreboard_worker(self):
        while not self.stop.is_set():
            started = time.monotonic()
            ms = self.clock()
            games = self.games.active(ms)
            dates = {espn_date(g) for g in games.values()}
            for date in sorted(dates):
                result = self.poll("espn_scoreboard", ESPN, "/scoreboard", dates=date, limit=1000)
                if not result:
                    continue
                board, recv = result
                if not isinstance(board, dict) or not isinstance(board.get("events"), list):
                    self.event("poll_error", source="espn_scoreboard", error="No events array", date=date)
                    continue
                append(self.store, f"espn/{date}/scoreboard.jsonl", "espn_scoreboard",
                       {"recv_ms": recv, "message": board})
                events = self.scoreboard_events(board["events"], recv)
                for slug, game in games.items():
                    try:
                        if espn_date(game) != date:
                            continue
                        mapping = {key: game[key] for key in ("espn_event", "team0_home", "mapping_status")}
                        if mapping["mapping_status"] != "matched":
                            mapping = map_event(game, events)
                            self.games.update(slug, {**mapping, "espn_date": date}, recv)
                        if mapping["espn_event"]:
                            event = next((e for e in events if str(e["id"]) == mapping["espn_event"]), None)
                            if event is None:
                                continue  # A partial scoreboard must not erase a verified join.
                            state = ((event.get("status") or {}).get("type") or {}).get("state")
                            if state in {"pre", "in", "post"}:
                                with self.games.lock:
                                    self.games.runtime[slug]["board_state"] = state
                            else:
                                raise ValueError("Missing game state")
                    except (KeyError, ValueError, TypeError, AttributeError, IndexError) as exc:
                        self.event("poll_error", source="espn_scoreboard", slug=slug, error=str(exc))
            self.stop.wait(max(0, 60 - (time.monotonic() - started)))

    def scoreboard_events(self, events, recv_ms):
        valid = []
        for index, event in enumerate(events):
            try:
                if not event["id"]:
                    raise ValueError("Missing event ID")
                competitors = event["competitions"][0]["competitors"]
                sides = {c["homeAway"] for c in competitors}
                if len(competitors) != 2 or sides != {"home", "away"} or not all(c["team"]["name"] for c in competitors):
                    raise ValueError("Incomplete game competitors")
            except (KeyError, ValueError, TypeError, IndexError) as exc:
                self.event("poll_error", recv_ms=recv_ms, source="espn_scoreboard", event_index=index, error=str(exc))
                continue
            valid.append(event)
        return valid

    def summary_worker(self):
        # Parallel HTTP prevents sixteen games from stretching a five-second poll
        # cadence into sixteen serialized network round trips.
        pending, next_poll = {}, {}
        with ThreadPoolExecutor(max_workers=16, thread_name_prefix="nba-summary") as pool:
            while not self.stop.is_set():
                ms = self.clock()
                for slug, future in list(pending.items()):
                    if not future.done():
                        continue
                    del pending[slug]
                    game, result = future.result()
                    if result:
                        payload, recv = result
                        try:
                            state, latest = self.espn_log.record(game, payload, recv)
                            self.games.espn(slug, state, latest, recv)
                        except (KeyError, ValueError, TypeError, IndexError) as exc:
                            self.event("poll_error", source="espn_summary", slug=slug, error=str(exc))
                for slug, game in self.games.active(ms).items():
                    if not game.get("espn_event") or ms < game["kickoff_ms"] - 5 * MINUTE or slug in pending or ms < next_poll.get(slug, 0):
                        continue
                    next_poll[slug] = ms + 5000
                    pending[slug] = pool.submit(self.summary_poll, game)
                self.stop.wait(.1)
            # Finish observations already received before closing the session.
            for future in pending.values():
                game, result = future.result()
                if result:
                    try:
                        self.espn_log.record(game, result[0], result[1])
                    except (KeyError, ValueError, TypeError, IndexError) as exc:
                        self.event("poll_error", source="espn_summary", slug=game["slug"], error=str(exc))

    def summary_poll(self, game):
        return game, self.poll("espn_summary", ESPN, "/summary", event=game["espn_event"])

    def settlement_pass(self, ms, next_poll, finished):
        for slug, game in self.games.all().items():
            six = (game["kickoff_ms"] // 86_400_000) * 86_400_000 + 6 * 60 * MINUTE
            if six <= game["kickoff_ms"]:
                six += 86_400_000
            if slug in finished or ms < next_poll.get(slug, six):
                continue
            result = self.poll("gamma_settlement", GAMMA, f"/markets/{game['market_id']}")
            # Every unresolved result and error retries hourly after the daily run.
            next_poll[slug] = ms + 60 * MINUTE
            if not result:
                continue
            market, recv = result
            if not isinstance(market, dict):
                self.event("poll_error", source="gamma_settlement", slug=slug, error="Invalid market response")
                continue
            done = resolved(market)
            append(self.store, f"settlement/{day(recv)}.jsonl", "settlement",
                   {"recv_ms": recv, "slug": slug, "market_id": game["market_id"], "resolved": done,
                    "outcomePrices": market.get("outcomePrices"), "closed": market.get("closed"),
                    "umaResolutionStatus": market.get("umaResolutionStatus"), "message": market})
            if done:
                finished.add(slug)

    def settlement_worker(self):
        next_poll, finished = {}, set()
        while not self.stop.is_set():
            self.settlement_pass(self.clock(), next_poll, finished)
            self.stop.wait(60)

    def websocket_worker(self, sports=False):
        with WireWriter(self.record_wire, self.stop) as writer:
            self.websocket_reader(writer, sports)

    def record_wire(self, item):
        task, *args = item
        if task == "event":
            kind, recv, details = args
            self.event(kind, recv_ms=recv, **details)
        elif task == "marker":
            routes, recv, event = args
            self.book.marker(routes, recv, event)
        elif task == "frame":
            raw, recv, routes, sports = args
            try:
                payload = json.loads(raw)
            except ValueError:
                self.event("websocket_invalid", recv_ms=recv, source="sports" if sports else "market", raw=raw)
                return
            if sports:
                record_sports(self.store, payload, recv)
            else:
                self.book.record(payload, recv, routes)

    def websocket_reader(self, writer, sports):
        import websocket

        create = self.socket_factory or websocket.create_connection
        url = SPORTS_WS if sports else MARKET_WS
        label = "sports" if sports else "market"
        backoff = 1
        while not self.stop.is_set():
            routes = {} if sports else self.games.routes(self.clock())
            next_routes = time.monotonic() + 1
            if not sports and not routes:
                self.stop.wait(1)
                continue
            sock = None
            try:
                sock = create(url, timeout=10, enable_multithread=True)
                sock.settimeout(1)
                ms = self.clock()
                writer.submit(("event", "websocket_connect", ms, {"source": label}))
                if not sports:
                    writer.submit(("marker", routes, ms, "connect_or_reconnect"))
                    sock.send(json.dumps({"type": "market", "assets_ids": sorted(routes)}))
                    with self.metrics_lock:
                        self.subscribed = {info[0] for info in routes.values()}
                last_ping = last_recv = time.monotonic()
                while not self.stop.is_set():
                    tick = time.monotonic()
                    if not sports and tick >= next_routes:
                        next_routes = tick + 1
                        desired = self.games.routes(self.clock())
                        removed = {t: routes[t] for t in routes if t not in desired or routes[t] != desired[t]}
                        added = {t: desired[t] for t in desired if t not in routes or desired[t] != routes[t]}
                        if removed:
                            sock.send(json.dumps({"operation": "unsubscribe", "assets_ids": sorted(removed)}))
                            writer.submit(("marker", removed, self.clock(), "unsubscribe"))
                        if added:
                            writer.submit(("marker", added, self.clock(), "subscribe"))
                            sock.send(json.dumps({"operation": "subscribe", "assets_ids": sorted(added)}))
                        routes = desired
                        with self.metrics_lock:
                            self.subscribed = {info[0] for info in routes.values()}
                        if not routes:
                            break
                    if not sports and tick - last_ping >= 10:
                        sock.send("PING")
                        last_ping = tick
                    if tick - last_recv > (20 if sports else 35):
                        raise TimeoutError("Websocket heartbeat timed out")
                    try:
                        raw, control = read_frame(sock, websocket)
                        recv = self.clock()  # Before parsing, routing, locks, or writing.
                    except websocket.WebSocketTimeoutException:
                        continue
                    if control:  # Protocol ping/pong (the sports feed pings every ~15 s); websocket-client answers pings.
                        last_recv = time.monotonic()
                        continue
                    if not raw:
                        raise ConnectionError("Websocket closed")
                    last_recv = time.monotonic()
                    backoff = 1
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    if raw.lower() == "ping":
                        sock.send("pong" if sports else "PONG")
                        continue
                    if raw.lower() == "pong":
                        continue
                    writer.submit(("frame", raw, recv, routes, sports))
            except (OSError, ValueError, websocket.WebSocketException) as exc:
                writer.submit(("event", "websocket_error", self.clock(), {"source": label, "error": str(exc)}))
            finally:
                try:
                    if not sports:
                        writer.submit(("marker", routes, self.clock(), "disconnect"))
                    writer.submit(("event", "websocket_disconnect", self.clock(), {"source": label}))
                finally:
                    if not sports:
                        with self.metrics_lock:
                            self.subscribed.clear()
                    if sock is not None:
                        try:
                            sock.close()
                        except (OSError, websocket.WebSocketException):
                            pass
            self.stop.wait(backoff)
            backoff = min(backoff * 2, 60)

    def health_worker(self):
        next_ntp = next_daily = 0
        ntp = None
        while not self.stop.is_set():
            ms = self.clock()
            if ms >= next_ntp:
                ntp = ntp_sample()
                self.event("ntp", **ntp)
                next_ntp = ms + 60 * MINUTE
            if ms >= next_daily:
                counts = self.store.summary()
                active = self.games.active(ms)
                with self.metrics_lock:
                    metrics, subscribed = dict(self.metrics), sorted(self.subscribed)
                summary = {"games_subscribed": len(subscribed), "subscribed_slugs": subscribed, "fills": counts.get("fill", 0),
                           "book_messages": counts.get("book_message", 0), "sports_messages": counts.get("sports_message", 0),
                           "polls": counts.get("poll", 0), "poll_errors": counts.get("poll_error", 0),
                           "websocket_connections": counts.get("websocket_connect", 0),
                           "websocket_reconnects": sum(max(0, metrics.get(f"{source}_websocket_connect", 0) - 1) for source in ("market", "sports")),
                           "espn_polls": sum(metrics.get(f"{source}_poll", 0) for source in ("espn_scoreboard", "espn_summary")),
                           "espn_errors": sum(metrics.get(f"{source}_poll_error", 0) for source in ("espn_scoreboard", "espn_summary")),
                           "unmapped_games": [s for s, g in active.items() if g.get("mapping_status") != "matched"],
                           "ntp": ntp, "counts": counts}
                self.event("daily_summary", **summary)
                LOG.info("NBA daily health %s", json.dumps(summary))
                next_daily = (ms // 86_400_000 + 1) * 86_400_000
            self.store.flush()
            self.stop.wait(1)

    def guarded(self, function):
        try:
            function()
        except BaseException as exc:
            self.failures.put(exc)
            self.stop.set()
            LOG.exception("NBA worker failed: %s", function.__name__)

    def backup_worker(self):
        from .backup import backup_run
        while not self.stop.is_set():
            try:
                manifest = backup_run(self.store, self.backup_to, self.stop)
                if manifest:
                    self.event("backup", destination=str(self.backup_to), snapshot_ms=manifest["recv_ms"],
                               files=len(manifest["files"]))
                delay = 7 * 24 * 3600
            except (OSError, ValueError) as exc:
                self.event("backup_error", destination=str(self.backup_to), error=str(exc))
                delay = 3600
            self.stop.wait(delay)

    def run(self):
        workers = [self.discovery_worker, self.scoreboard_worker, self.summary_worker,
                   self.settlement_worker, self.websocket_worker, lambda: self.websocket_worker(True), self.health_worker]
        if self.backup_to is not None:
            workers.append(self.backup_worker)
        try:
            self.event("session_start")
            for function in workers:
                thread = threading.Thread(target=self.guarded, args=(function,), name=f"nba-{function.__name__}")
                thread.start()
                self.threads.append(thread)
            while not self.stop.wait(.5):
                pass
        finally:
            self.stop.set()
            for thread in self.threads:
                thread.join()
            try:
                if self.failures.empty():
                    self.event("session_stop", counts=self.store.summary())
            finally:
                self.store.close()
        if not self.failures.empty():
            raise RuntimeError("NBA recording stopped after worker failure") from self.failures.get()
