"""Single-process live recording with network workers. No service installs itself."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import RULE_VERSION
from .book_capture import BookCapture, window_ms
from .engine import Engine
from .lifecycle import collection_end, in_discovery_window
from .sources import (BackoffError, GammaClient, Resolver, SofaClient, WS_URL,
                      team_whitelist_from, market_rows, match_from_event, score_row,
                      settlement_of, token_index)
from .storage import RecordingError, Store, day, now_ms, process_lock

LOG = logging.getLogger("pm_soccer_dryrun")


def log_event(store, producer, event, **fields):
    ms = now_ms()
    row = {"recv_ms": ms, "event": event, **fields}
    store.publish(producer, "ops", row, f"ops/{producer}/{day(ms)}.jsonl")
    LOG.info("%s %s", event, fields)


def update_match(store, slug, changes, producer):
    # Keep catalog updates and their live engine delivery serialized.
    with store.lock:
        matches = store.get("matches", {})
        previous = matches.get(slug, {})
        match = {**previous, **changes, "slug": slug, "recv_ms": now_ms()}
        if changes.get("sofa_event_id") is not None:
            match["score_at_mapping"] = store.get(f"score:{changes['sofa_event_id']}")
            seed = match["score_at_mapping"] or {}
            if seed.get("home_score") is not None and seed.get("away_score") is not None:
                match.setdefault("score_observed_ms", seed["poll_ms"])
        if {k: v for k, v in match.items() if k != "recv_ms"} == {k: v for k, v in previous.items() if k != "recv_ms"}:
            return previous
        matches[slug] = match
        store.put("matches", matches)
        store.append(producer, "match", match, f"matches/{producer}/{day(match['recv_ms'])}.jsonl")
    return match


def subscription_matches(matches, ms):
    out = {}
    for slug, match in matches.items():
        if ms < match["kickoff_ms"] - 30 * 60_000:
            continue
        end, _ = collection_end(match)
        if end is not None and ms >= end:
            continue
        out[slug] = match
    return out


def save_gamma_event(store, event, *, discovery=True):
    ms = now_ms()
    try:
        match = match_from_event(event, ms)
    except (ValueError, KeyError, TypeError) as exc:
        log_event(store, "metadata", "invalid_event", slug=event.get("slug"), error=str(exc))
        return
    if match:
        if discovery and not in_discovery_window(match["kickoff_ms"], ms):
            return
        old = store.get("matches", {}).get(match["slug"], {})
        if all(b["closed"] for b in match["books"].values()):
            match["all_closed_ms"] = old.get("all_closed_ms") or max(b["closed_ms"] or ms for b in match["books"].values())
            if old.get("full_time_ms") is None:
                # Keep a further 30 minutes when the score source never saw FT.
                match["full_time_ms"] = match["all_closed_ms"]
                match["full_time_source"] = "market_close_fallback"
        else:
            match["all_closed_ms"] = None
        update_match(store, match["slug"], match, "metadata")
    else:
        log_event(store, "metadata", "unusable_event", slug=event.get("slug"))


def maintenance(store, stop):
    client = GammaClient()
    refresh = {}
    last_discovery = 0
    try:
        while not stop.is_set():
            try:
                ms = now_ms()
                if ms - last_discovery >= 3600_000:
                    for event in client.discover():
                        if stop.is_set():
                            return
                        save_gamma_event(store, event)
                    last_discovery = now_ms()
                    log_event(store, "metadata", "discovery_complete", matches=len(store.get("matches", {})))
                matches = store.get("matches", {})
                ms = now_ms()  # Discovery may take minutes; expire against the current clock.
                for slug, match in matches.items():
                    end, reason = collection_end(match)
                    if end is not None and ms >= end and reason == "no_feed_or_close" and match.get("collection_expired_ms") is None:
                        update_match(store, slug, {"collection_expired_ms": end,
                                                  "collection_expiry_reason": reason}, "metadata")
                        log_event(store, "metadata", "collection_expired", slug=slug, expired_ms=end)
                active = subscription_matches(matches, ms)
                for slug, match in matches.items():
                    if stop.is_set():
                        return
                    preflight = match["kickoff_ms"] - 35 * 60_000 <= ms < match["kickoff_ms"] - 30 * 60_000
                    due = ms - refresh.get(slug, 0) >= 60_000
                    if due and (slug in active or preflight):
                        event = client.get(f"/events/{match['event_id']}")
                        save_gamma_event(store, event, discovery=False)
                        refresh[slug] = now_ms()
                # One settlement sweep per UTC day after 06:00, unfinished markets retried daily.
                today = day(ms)
                if datetime.fromtimestamp(ms / 1000, timezone.utc).hour >= 6 and store.get("settlement_day") != today:
                    settled = store.get("settled", {})
                    for slug, match in matches.items():
                        if day(match["kickoff_ms"]) >= today:
                            continue
                        for role, book in match["books"].items():
                            if stop.is_set():
                                return
                            if book["id"] in settled:
                                continue
                            market = client.get(f"/markets/{book['id']}")
                            result = settlement_of(market)
                            polled = now_ms()
                            with store.lock:
                                store.append("metadata", "settlement", {"recv_ms": polled, "slug": slug,
                                             "book_role": role, "market_id": book["id"],
                                             "result": result, "market": market}, f"settlement/{day(polled)}.jsonl")
                                if result is not None:
                                    settled[book["id"]] = result
                                    store.put("settled", settled)
                    with store.lock:
                        store.put("settlement_day", today)
                store.flush()
                stop.wait(1)
            except RecordingError:
                stop.set()
                raise
            except Exception:
                LOG.exception("Metadata maintenance failed; retrying in 30 seconds")
                stop.wait(30)
    finally:
        store.flush()


def collect(store, stop, capture=None):
    import websocket
    capture = capture if capture is not None else BookCapture(store)
    worker = threading.Thread(target=maintenance, args=(store, stop), name="soccer-metadata")
    worker.start()
    ws, subscribed, routing = None, set(), {}
    heartbeat = last_pong = last_received = last_health = 0
    last_routes = last_flush = 0
    frames = 0
    max_processing_ms = max_lock_wait_ms = 0.0
    reconnect_at = 0
    retry_s = 1
    try:
        while not stop.is_set():
            ms = now_ms()
            if ms - last_routes >= 1000:
                matches = subscription_matches(store.get("matches", {}), ms)
                routing = token_index(matches)
                with store.lock:
                    capture.set_routing(routing)
                wanted = set(routing)
                last_routes = ms
                if ws:
                    try:
                        for operation, tokens in (("subscribe", wanted - subscribed), ("unsubscribe", subscribed - wanted)):
                            if tokens:
                                ws.send(json.dumps({"assets_ids": sorted(tokens), "operation": operation}))
                        subscribed = wanted
                    except RecordingError:
                        stop.set()
                        raise
                    except Exception:
                        with store.lock:
                            capture.gap()
                        ws.close()
                        ws = None
                if not wanted and ws:
                    ws.close()
                    ws = None
            if ws is None:
                if routing and ms >= reconnect_at:
                    try:
                        ws = websocket.create_connection(WS_URL, timeout=10)
                        ws.settimeout(0.2)
                        ws.send(json.dumps({"type": "market", "assets_ids": sorted(routing)}))
                        subscribed = set(routing)
                        ms = now_ms()
                        with store.lock:
                            capture.gap()
                        store.publish("collector", "gap", {"recv_ms": ms, "reason": "connect_or_reconnect"}, f"ops/collector/{day(ms)}.jsonl")
                        last_pong = last_received = last_health = time.monotonic()
                        heartbeat = last_received - 10
                        frames = 0
                        max_processing_ms = max_lock_wait_ms = 0.0
                        retry_s = 1
                        log_event(store, "collector", "websocket_connected", tokens=len(subscribed))
                    except RecordingError:
                        stop.set()
                        raise
                    except Exception as exc:
                        if ws:
                            ws.close()
                        ws = None
                        log_event(store, "collector", "websocket_connect_error", error=str(exc))
                        reconnect_at = now_ms() + retry_s * 1000
                        retry_s = min(60, retry_s * 2)
                if ws is None:
                    store.flush()
                    stop.wait(0.2)
                    continue
            try:
                tick = time.monotonic()
                if tick - heartbeat >= 10:
                    ws.send("PING")
                    heartbeat = tick
                # Read queued data/PONGs before deciding the connection is dead.
                # Incoming data also proves liveness. Missing PONGs during a
                # busy stream are diagnosed, not used to discard the backlog.
                try:
                    frame = ws.recv()
                except websocket.WebSocketTimeoutException:
                    if time.monotonic() - last_received >= 30:
                        raise TimeoutError("No websocket frame or PONG for 30 seconds")
                    frame = None
                recv_ms = now_ms()
                if frame is not None:
                    last_received = time.monotonic()
                    frames += 1
                if frame == "" or frame == b"":
                    raise ConnectionError("Websocket closed")
                if frame in ("PONG", "pong", b"PONG", b"pong"):
                    last_pong = time.monotonic()
                elif frame is not None:
                    processing_started = time.monotonic()
                    try:
                        payload = json.loads(frame)
                    except (ValueError, TypeError):
                        log_event(store, "collector", "invalid_frame", frame=str(frame))
                        continue
                    lock_started = time.monotonic()
                    with store.lock:
                        max_lock_wait_ms = max(max_lock_wait_ms, (time.monotonic() - lock_started) * 1000)
                        for row in market_rows(payload, recv_ms, routing):
                            capture.record(row)
                    max_processing_ms = max(max_processing_ms, (time.monotonic() - processing_started) * 1000)
                tick = time.monotonic()
                if tick - last_health >= 60:
                    log_event(store, "collector", "websocket_health", interval_s=tick - last_health,
                              frames=frames, pong_age_s=tick - last_pong, pong_overdue=tick - last_pong >= 30,
                              incoming_age_s=tick - last_received, max_processing_ms=max_processing_ms,
                              max_lock_wait_ms=max_lock_wait_ms, book_capture=capture.summary())
                    last_health, frames = tick, 0
                    max_processing_ms = max_lock_wait_ms = 0.0
            except RecordingError:
                stop.set()
                raise
            except Exception as exc:
                ms = now_ms()
                with store.lock:
                    capture.gap()
                store.publish("collector", "gap", {"recv_ms": ms, "reason": "disconnect", "error": str(exc)}, f"ops/collector/{day(ms)}.jsonl")
                ws.close()
                ws = None
                reconnect_at = ms + retry_s * 1000
                retry_s = min(60, retry_s * 2)
                LOG.warning("Websocket disconnected: %s", exc)
            if now_ms() - last_flush >= 500:
                store.flush()
                last_flush = now_ms()
    finally:
        stop.set()
        if ws:
            ws.close()
        worker.join()
        store.flush()


def save_score(store, event, poll_ms, producer, source="live", *, matches=None, relay=None):
    # The live poll passes one scoped catalog snapshot for its whole batch.
    # Resolver/detail calls need only one copy, not one for each lifecycle test.
    if matches is None:
        matches = store.get("matches", {})
    with store.lock:
        previous = store.get(f"score_change:{event['id']}")
        row = score_row(event, poll_ms, previous, source)
        if relay is not None:
            row["relay"] = {**relay, "vps_applied_ms": poll_ms}
        store.append(producer, "score", row, f"score/{producer}/{day(poll_ms)}.jsonl")
        if not row["stale"]:
            store.put(f"score:{event['id']}", row)
            if row["changeTimestamp"] is not None:
                store.put(f"score_change:{event['id']}", row["changeTimestamp"])
    if not row["stale"] and row["home_score"] is not None and row["away_score"] is not None:
        for slug, match in matches.items():
            if match.get("sofa_event_id") == event["id"] and match.get("score_observed_ms") is None:
                update_match(store, slug, {"score_observed_ms": poll_ms}, producer)
    if not row["stale"] and row["status_type"] in ("finished", "canceled", "cancelled"):
        for slug, match in matches.items():
            final = [row["home_score"], row["away_score"]]
            if match.get("sofa_event_id") == event["id"]:
                fallback = match.get("full_time_source") == "market_close_fallback"
                if match.get("full_time_ms") is None or fallback or match.get("final_score") != final:
                    update_match(store, slug, {"full_time_ms": poll_ms if fallback else (match.get("full_time_ms") or poll_ms),
                                              "full_time_source": source, "final_score": final}, producer)
    return row


def save_live_scores(store, events, received, started, *, relay=None, event_count=None):
    """Keep full observations only for mapped matches in our collection window."""
    matches = subscription_matches(store.get("matches", {}), received)
    feed_ids = {m["sofa_event_id"] for m in matches.values() if m.get("sofa_event_id") is not None}
    tracked = [event for event in events if event.get("id") in feed_ids]
    store.publish("scores", "poll", {"poll_ms": received, "request_ms": started, "state": "ok",
                  "http_status": 200, "event_count": len(events) if event_count is None else event_count, "tracked_event_count": len(tracked),
                  "tracked_match_count": len(matches), "mapped_event_count": len(feed_ids),
                  **({"relay": relay} if relay is not None else {})},
                  f"polls/{day(received)}.jsonl")
    for event in tracked:
        save_score(store, event, received, "scores", matches=matches, relay=relay)


def resolve_matches(store, stop, client, alias_path):
    resolver = Resolver(client, team_whitelist_from(alias_path))
    last_detail, retry_after = {}, {}
    terminal = {"team_not_whitelisted", "team_identity_conflict"}
    try:
        while not stop.is_set():
            try:
                ms = now_ms()
                for slug, match in subscription_matches(store.get("matches", {}), ms).items():
                    if stop.is_set():
                        return
                    phase = "mapping"
                    try:
                        if (match.get("sofa_event_id") is None and match.get("mapping_status") not in terminal
                                and now_ms() >= retry_after.get(slug, 0)):
                            mapping = resolver.resolve(match)
                            if getattr(client, "last_timing", None):
                                mapping["mapping_relay"] = client.last_timing
                            match = update_match(store, slug, mapping, "resolver")
                            retry_after[slug] = now_ms() + 60_000
                            stop.wait(0.8)
                        feed_id = match.get("sofa_event_id")
                        latest = store.get(f"score:{feed_id}", {})
                        phase = "event_detail"
                        # Failed detail requests are paced too; they cannot starve other matches.
                        ms = now_ms()
                        if (not stop.is_set() and feed_id is not None and ms >= match["kickoff_ms"]
                                and match.get("full_time_ms") is None
                                and ms - latest.get("poll_ms", 0) > 60_000
                                and ms - last_detail.get(slug, 0) > 60_000):
                            last_detail[slug] = ms
                            response = client.get(f"/event/{feed_id}")
                            save_score(store, response["event"], now_ms(), "resolver", "event_detail",
                                       relay=getattr(response, "relay_timing", None))
                            stop.wait(0.8)
                    except BackoffError as exc:
                        if phase == "mapping":
                            retry_after[slug] = exc.until_ms
                            update_match(store, slug, {"mapping_status": "backoff",
                                "mapping_error": str(exc), "mapping_http_status": exc.status,
                                "mapping_retry_after_ms": exc.until_ms}, "resolver")
                        raise
                    except RecordingError:
                        raise
                    except Exception as exc:
                        failed_ms = now_ms()
                        status = getattr(exc, "status", None)
                        if phase == "mapping":
                            retry_after[slug] = failed_ms + 60_000
                            update_match(store, slug, {"sofa_event_id": None, "mapping_status": "error",
                                         "mapping_error": str(exc), "mapping_http_status": status}, "resolver")
                        log_event(store, "resolver", "match_error", slug=slug, phase=phase,
                                  http_status=status, error=str(exc), retry_after_ms=failed_ms + 60_000)
                        LOG.exception("Resolver %s failed for %s; continuing other matches", phase, slug)
                store.flush()
                stop.wait(1)
            except BackoffError:
                store.flush()
                stop.wait(5)
            except RecordingError:
                stop.set()
                raise
            except Exception:
                LOG.exception("Match resolver failed; retrying in 30 seconds")
                stop.wait(30)
    finally:
        store.flush()


def scores(store, stop, alias_path=None):
    client = SofaClient()
    team_whitelist_from(alias_path)  # Validate before starting the worker thread.
    worker = threading.Thread(target=resolve_matches, args=(store, stop, client, alias_path), name="soccer-resolver")
    worker.start()
    try:
        while not stop.is_set():
            started = now_ms()
            try:
                payload = client.get("/sport/football/events/live")
                if not isinstance(payload.get("events"), list):
                    raise ValueError("Sofascore live response has no events array")
                received = now_ms()
                save_live_scores(store, payload["events"], received, started)
            except BackoffError as exc:
                received = now_ms()
                store.publish("scores", "poll", {"poll_ms": received, "state": "backoff", "http_status": exc.status,
                              "backoff_until_ms": exc.until_ms}, f"polls/{day(received)}.jsonl")
            except RecordingError:
                stop.set()
                raise
            except Exception as exc:
                received = now_ms()
                store.publish("scores", "poll", {"poll_ms": received, "state": "error", "http_status": None,
                              "error": str(exc)}, f"polls/{day(received)}.jsonl")
                LOG.exception("Live score poll failed")
            store.flush()
            stop.wait(max(0, 5 - (now_ms() - started) / 1000))
    finally:
        stop.set()
        worker.join()
        store.flush()


def engine_batch(store, engine, events, observed_ms):
    """Evaluate live input and write outputs directly; nothing is checkpointed."""
    records = []
    for event in events:
        try:
            records.extend(engine.handle(event, observed_ms))
        except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
            engine.state["gap_ms"] = event["payload"].get("recv_ms", observed_ms)
            records.append({"kind": "input_error", "candidate_id": engine.event_id(event["seq"]),
                            "run_id": engine.run_id, "rule_version": RULE_VERSION, "emitted_ms": observed_ms,
                            "source_event_seq": event["seq"], "error": str(exc)})
    if not events:
        records.extend(engine.advance(observed_ms, observed_ms))
    for row in records:
        store.append("engine", row["kind"], row, f"fires/{engine.run_id}/{day(row['emitted_ms'])}.jsonl")
        if row["kind"] == "fire":
            LOG.info("fire %s %s d=%s candidate=%s fire_ms=%s", row["slug"], row["book_role"],
                     row["d"], row["candidate_id"], row["fire_recv_ms"])
    return records


def daily_summary(store):
    today = day(now_ms())
    if store.get("summary_day") == today:
        return
    log_event(store, "engine", "daily_summary", interval="since_session_start", started_ms=store.started_ms,
              matches_live=len(subscription_matches(store.get("matches", {}), now_ms())), **store.summary())
    store.put("summary_day", today)


def run_live(store, stop, run_id, alias_path=None, book_window_minutes=5, relay_config=None,
             fotmob_shadow_dir=None):
    """Network workers share one recorder and one fresh, in-memory engine."""
    engine = Engine(run_id, session_id=store.session_id)
    capture = BookCapture(store, book_window_minutes)
    fotmob = None
    if fotmob_shadow_dir is not None:
        from .fotmob_capture import FotmobCapture
        fotmob = FotmobCapture(fotmob_shadow_dir, store, engine)
    # append() holds the shared lock: input and output sequences are ordered,
    # and the timer cannot close a second midway through a websocket frame.
    def deliver(event):
        for row in engine_batch(store, engine, [event], now_ms()):
            capture.fire(row)
            if fotmob is not None:
                capture.fallback_candidate(row, fotmob.decision(row))
    store.on_event = deliver
    errors = []

    def worker(target, *args):
        try:
            target(*args)
        except Exception as exc:
            errors.append(exc)
            LOG.exception("Live worker failed")
        finally:
            stop.set()

    score_target, score_args = scores, (store, stop, alias_path)
    if relay_config:
        from .relay import relay_scores
        score_target, score_args = relay_scores, (store, stop, alias_path, relay_config)
    workers = [threading.Thread(target=worker, args=(collect, store, stop, capture), name="soccer-collector"),
               threading.Thread(target=worker, args=(score_target, *score_args), name="soccer-scores")]
    if fotmob is not None:
        workers.append(threading.Thread(target=worker, args=(fotmob.run, stop),
                                        name="fotmob-capture-input"))
    log_event(store, "engine", "session_start", session_id=store.session_id, recovery=False,
              book_window_minutes=book_window_minutes)
    last_second = None
    try:
        for thread in workers:
            thread.start()
        while not stop.is_set():
            with store.lock:
                ms = now_ms()
                if ms // 1000 != last_second:
                    engine_batch(store, engine, [], ms)
                    last_second = ms // 1000
                store.flush()
                daily_summary(store)
            stop.wait(0.02)
    finally:
        stop.set()
        for thread in workers:
            if thread.ident is not None:
                thread.join()
        with store.lock:
            # Let the last pending second really finish before finalizing it.
            # No synthetic observation time and no state carried across restarts.
            pending_end = max(((int(key.rsplit(":", 1)[1]) + 1) * 1000
                               for key in engine.state["pending"]), default=0)
            time.sleep(max(0, (pending_end - now_ms()) / 1000))
            engine_batch(store, engine, [], now_ms())
            log_event(store, "engine", "session_end", session_id=store.session_id,
                      book_capture=capture.summary(), **store.summary())
            store.flush()
            store.on_event = None
    if errors:
        raise RuntimeError("Live recording worker failed") from errors[0]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Soccer live recording and causal signals; no trading or execution simulation")
    parser.add_argument("role", choices=("live",))
    parser.add_argument("--data-dir", type=Path, default=Path("data/pm_soccer_dryrun"))
    parser.add_argument("--run-id", default="soccer-v2-20260916")
    parser.add_argument("--aliases", type=Path,
                        help="Verified team whitelist CSV: pm_name,query,sofascore_team_id (no name-search fallback)")
    parser.add_argument("--book-window-minutes", type=float, default=5,
                        help="Depth recording duration after each filter-passing fire (default: 5)")
    parser.add_argument("--fotmob-shadow-dir", type=Path,
                        help="Opt-in: read FotMob shadow observations for outage-only candidate book windows")
    parser.add_argument("--score-relay-bind", help="Listen on a Tailscale IPv4 address or 127.0.0.1 for an SSH tunnel")
    parser.add_argument("--score-relay-port", type=int, default=18765)
    parser.add_argument("--score-relay-peer", help="Only accept this worker Tailscale IPv4 address, or 127.0.0.1 for SSH")
    args = parser.parse_args(argv)
    if args.fotmob_shadow_dir:
        primary, shadow = args.data_dir.resolve(), args.fotmob_shadow_dir.resolve()
        if primary == shadow or primary in shadow.parents or shadow in primary.parents:
            parser.error("FotMob shadow and primary recording directories must be separate")
    relay_config = None
    if args.score_relay_bind:
        from .relay import validate_addresses
        try:
            validate_addresses(args.score_relay_bind, args.score_relay_peer, args.score_relay_port)
        except ValueError as exc:
            parser.error(str(exc))
        relay_config = {"bind": args.score_relay_bind, "port": args.score_relay_port, "peer": args.score_relay_peer}
    elif args.score_relay_peer:
        parser.error("--score-relay-peer requires --score-relay-bind")
    try:
        duration_ms = window_ms(args.book_window_minutes)
    except ValueError as exc:
        parser.error(str(exc))
    if not args.run_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in args.run_id):
        parser.error("run-id must contain only letters, digits, underscores and hyphens")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    teams = team_whitelist_from(args.aliases)
    team_digest = hashlib.sha256(json.dumps(teams, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    with process_lock(args.data_dir, "live"):
        digest = hashlib.sha256()
        for name in ("engine.py", "sources.py", "service.py", "storage.py", "lifecycle.py", "book_capture.py", "relay.py"):
            digest.update(Path(__file__).with_name(name).read_bytes())
        if args.fotmob_shadow_dir:
            for name in ("fotmob_capture.py", "fotmob.py", "fotmob_team_aliases.csv"):
                digest.update(Path(__file__).with_name(name).read_bytes())
        manifest = {"run_id": args.run_id, "rule_version": RULE_VERSION, "code_sha256": digest.hexdigest(),
                    "storage_version": "post-fire-books-2", "book_window_ms": duration_ms,
                    "team_whitelist_sha256": team_digest}
        if relay_config:
            manifest["score_relay"] = relay_config
        if args.fotmob_shadow_dir:
            from .fotmob_capture import POLICY, MAX_AGE_MS
            manifest["storage_version"] = "post-fire-and-fallback-candidate-books-3"
            manifest["fotmob_capture"] = {"policy": POLICY, "max_age_ms": MAX_AGE_MS,
                                         "shadow_dir": str(args.fotmob_shadow_dir.resolve())}
        manifest_path = args.data_dir / "run.json"
        if manifest_path.exists() and json.loads(manifest_path.read_text()) != manifest:
            raise ValueError("Run ID or implementation changed; use a new data directory")
        if not manifest_path.exists():
            with manifest_path.open("x") as out:
                out.write(json.dumps(manifest, indent=2) + "\n")
        session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]
        root = args.data_dir / "sessions" / session_id
        store = Store(root, session_id=session_id)
        (root / "session.json").write_text(json.dumps({**manifest, "session_id": session_id,
                                                      "started_ms": store.started_ms, "recovery": False}, indent=2) + "\n")
        LOG.info("Recording fresh session to %s", root)
        try:
            # Bind the actual resolver input to the validated run configuration.
            whitelist_path = root / "team_whitelist.csv"
            with whitelist_path.open("x", newline="") as out:
                writer = csv.writer(out)
                writer.writerow(["pm_name", "query", "sofascore_team_id"])
                writer.writerows((name, team["name"], team["id"]) for name, team in sorted(teams.items()))
            relay_args = {"relay_config": relay_config} if relay_config else {}
            if args.fotmob_shadow_dir:
                relay_args["fotmob_shadow_dir"] = args.fotmob_shadow_dir
            run_live(store, stop, args.run_id, whitelist_path, args.book_window_minutes, **relay_args)
        finally:
            store.close()


if __name__ == "__main__":
    main()
