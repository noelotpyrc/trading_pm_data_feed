"""Signal state machine; no positions, entry prices, fees, or P&L.

State lives only in memory. Decimal arithmetic keeps cent boundaries exact.
"""
from __future__ import annotations

from decimal import Decimal
from statistics import median

from . import RULE_VERSION
from .lifecycle import collection_end


def all_known(values):
    if False in values:
        return False
    return None if None in values else True


def reference(prices):
    if len(prices) < 5:
        return None
    p = [Decimal(str(v)) for v in prices[-(10 if len(prices) >= 10 else 5):]]
    ref = median(p)
    noise = max(Decimal("0.01"), 3 * median([abs(v - ref) for v in p]))
    return ref, noise


def minute_of(score, at_ms):
    period, start = score.get("period"), score.get("currentPeriodStartTimestamp")
    if period not in (1, 2) or start is None or score.get("status_type") != "inprogress":
        return None
    elapsed = (at_ms / 1000 - start) / 60
    return (0 if period == 1 else 45) + elapsed if elapsed >= 0 else None


def implied_direction(role, other, direction, lead):
    """Reference script 17's cross-book direction; None means unknown score."""
    if role != "draw":
        scorer = role if direction > 0 else ("away" if role == "home" else "home")
        if other != "draw":
            return 1 if other == scorer else -1
        if lead is None:
            return None
        side_lead = lead if scorer == "home" else -lead
        return -1 if side_lead >= 0 else 1
    if lead is None:
        return None
    if lead == 0:
        return 0  # Either win-book direction confirms a downward draw candidate.
    scorer = ("away" if lead > 0 else "home") if direction > 0 else ("home" if lead > 0 else "away")
    return 1 if other == scorer else -1


class Engine:
    def __init__(self, run_id, state=None, session_id=None):
        self.run_id = run_id
        self.session_id = session_id
        self.state = state or {
            "matches": {}, "books": {}, "scores": {}, "polls": [],
            "seconds": {}, "pending": {}, "closed_before": 0, "gap_ms": None,
        }

    def event_id(self, seq):
        prefix = f"{self.run_id}:{self.session_id}" if self.session_id else self.run_id
        return f"{prefix}:{seq}"

    def score_snapshot(self, match, at_ms):
        feed_id = match.get("sofa_event_id")
        history = self.state["scores"].get(str(feed_id), [])
        eligible = [s for s in history if s["poll_ms"] <= at_ms]
        latest = eligible[-1] if eligible else {}
        accepted = next((s for s in reversed(eligible) if not s.get("stale")), {})
        polls = [p for p in self.state["polls"] if p["poll_ms"] <= at_ms]
        health = polls[-1] if polls else {}
        h, a = accepted.get("home_score"), accepted.get("away_score")
        if match.get("sofa_swapped"):
            h, a = a, h
        change = accepted.get("changeTimestamp")
        return {
            "sofa_event_id": feed_id, "home_score": h, "away_score": a,
            "no_score": h is None or a is None,
            "score_poll_ms": accepted.get("poll_ms"), "changeTimestamp": change,
            "score_age_s": (at_ms - accepted["poll_ms"]) / 1000 if accepted else None,
            "score_change_age_s": at_ms / 1000 - change if change is not None else None,
            "score_stale": bool(latest.get("stale")),
            "poller_state": "unmapped" if feed_id is None else health.get("state", "unknown"),
            "poll_http_status": health.get("http_status"),
            **({"score_relay": accepted["relay"]} if "relay" in accepted else {}),
            "feed_period": accepted.get("period"), "feed_status": accepted.get("status"),
            "period_start": accepted.get("currentPeriodStartTimestamp"),
            "feed_minute": minute_of(accepted, at_ms),
        }

    @staticmethod
    def checks(role, d, ref, snapshot):
        h, a, minute = snapshot["home_score"], snapshot["away_score"], snapshot["feed_minute"]
        lead = h - a if h is not None and a is not None else None
        side = None if role == "draw" else (role if d > 0 else ("away" if role == "home" else "home"))
        side_lead = None if lead is None or side is None else (lead if side == "home" else -lead)
        reasons = []
        if not Decimal("0.05") <= ref <= Decimal("0.95"):
            reasons.append("dead_price")
        if side_lead is not None and side_lead >= 2:
            reasons.append("favoured_leads_by_2")
        if role == "draw" and d > 0 and lead == 0:
            reasons.append("draw_up_level")
        needs_score = role != "draw" or d > 0
        gate = False if reasons else (None if needs_score and lead is None else True)
        if role == "draw":
            hypo = (None if lead is None else lead != 0) if d > 0 else True
        elif d > 0 or ref < Decimal("0.85") or (minute is not None and minute < 75):
            hypo = True
        elif lead is None:
            hypo = None
        else:
            leader = lead > 0 if role == "home" else lead < 0
            hypo = (None if minute is None else minute < 75) if leader else True
        own_up = role != "draw" and d > 0
        selected_side = (None if lead is None else side_lead == 1) if own_up else False
        return dict(gate=gate, gate_reasons=reasons, hypothesis=hypo, favoured_side=side,
                    lead_home=lead, f_minute30=None if minute is None else minute >= 30,
                    f_leader1_up=selected_side)

    def record(self, kind, candidate, observed_ms, **extra):
        return {**candidate, "kind": kind, "run_id": self.run_id, "rule_version": RULE_VERSION,
                "emitted_ms": observed_ms, **extra}

    def confirm(self, candidate, movement, observed_ms):
        expected = implied_direction(candidate["book_role"], movement["book_role"], candidate["d"], candidate["lead_home"])
        if expected is None or (expected and expected != movement["d"]):
            return []
        candidate["cross_book"] = True
        candidate["confirming_book"] = movement["book_role"]
        candidate["confirming_event_seq"] = movement["event_seq"]
        candidate["confirmation_recv_ms"] = max(candidate["candidate_recv_ms"], movement["recv_ms"])
        fire = all_known([candidate["gate"], True, candidate["hypothesis"]])
        kind = "fire" if fire is True else "candidate_result"
        return [self.record(kind, candidate, observed_ms, fire=fire,
                            fire_recv_ms=max(observed_ms, candidate["confirmation_recv_ms"]) if fire is True else None)]

    def advance(self, cutoff_ms, observed_ms):
        """Finalize seconds strictly before cutoff; call only after draining input."""
        cutoff = cutoff_ms // 1000
        out = []
        for key, candidates in list(self.state["pending"].items()):
            if int(key.rsplit(":", 1)[1]) < cutoff:
                for candidate in candidates:
                    out.append(self.record("candidate_result", candidate, observed_ms, cross_book=False, fire=False,
                                           reason="second_closed", fire_recv_ms=None))
                del self.state["pending"][key]
        self.state["seconds"] = {k: v for k, v in self.state["seconds"].items() if int(k.rsplit(":", 1)[1]) >= cutoff}
        self.state["closed_before"] = max(self.state["closed_before"], cutoff)
        expired = {slug for slug, m in self.state["matches"].items()
                   if (end := collection_end(m)[0]) is not None and cutoff_ms > end + 60_000}
        for slug in expired:
            del self.state["matches"][slug]
            for role in ("home", "draw", "away"):
                self.state["books"].pop(slug + ":" + role, None)
        feed_ids = {str(m.get("sofa_event_id")) for m in self.state["matches"].values()}
        self.state["scores"] = {k: v for k, v in self.state["scores"].items() if k in feed_ids}
        return out

    def handle(self, event, observed_ms):
        kind, row, seq = event["kind"], event["payload"], event["seq"]
        if kind == "match":
            self.state["matches"][row["slug"]] = row
            seed = row.get("score_at_mapping")
            if seed and str(seed["id"]) not in self.state["scores"]:
                self.state["scores"][str(seed["id"])] = [seed]
            return []
        if kind in ("score", "poll"):
            if kind == "score" and row["id"] not in {m.get("sofa_event_id") for m in self.state["matches"].values()}:
                return []
            if kind == "score" and not row.get("stale") and row.get("home_score") is not None and row.get("away_score") is not None:
                for match in self.state["matches"].values():
                    if match.get("sofa_event_id") == row["id"]:
                        match.setdefault("score_observed_ms", row["poll_ms"])
            history = self.state["polls"] if kind == "poll" else self.state["scores"].setdefault(str(row["id"]), [])
            history.append(row)
            # Keep an accepted predecessor and two minutes of observations.
            cutoff = row["poll_ms"] - 120_000
            old = [r for r in history if r["poll_ms"] < cutoff and not r.get("stale")]
            history[:] = old[-1:] + [r for r in history if r["poll_ms"] >= cutoff]
            return []
        if kind == "gap":
            self.state["gap_ms"] = row["recv_ms"]
            return []
        if kind != "market":
            return []
        if row["message"].get("event_type") != "last_trade_price":
            # Continuous depth traffic must not prevent second-close records.
            # Repeating catalog/window cleanup for every depth update within
            # an already advanced second does no signal work.
            if row["recv_ms"] // 1000 <= self.state["closed_before"]:
                return []
            return self.advance(row["recv_ms"], observed_ms)
        recv = row["recv_ms"]
        msg = row["message"]
        match = self.state["matches"].get(row["slug"])
        if not match or recv < match["kickoff_ms"]:
            return self.advance(recv, observed_ms)
        role, token = row["book_role"], str(msg["asset_id"])
        value = Decimal(str(msg["price"]))
        if not value.is_finite() or not 0 <= value <= 1:
            raise ValueError("Invalid fill price")
        price = value if row["outcome"] == "Yes" else 1 - value
        # Validate before closing seconds; malformed input must not consume
        # pending candidates without returning their terminal records.
        out = self.advance(recv, observed_ms)
        prices = self.state["books"].setdefault(match["slug"] + ":" + role, [])
        pair = reference(prices)
        prices.append(str(price))
        del prices[:-10]
        if pair is None:
            return out
        ref, noise = pair
        gap = price - ref
        direction = 1 if gap > 0 else -1
        second_key = match["slug"] + ":" + str(recv // 1000)
        closed = recv // 1000 < self.state["closed_before"]
        late = closed or observed_ms // 1000 > recv // 1000
        movement = {"event_seq": seq, "book_role": role, "d": direction, "recv_ms": recv}
        if abs(gap) > noise and not closed:
            remaining = []
            for candidate in self.state["pending"].get(second_key, []):
                result = self.confirm(candidate, movement, observed_ms) if candidate["book_role"] != role else []
                out.extend(result)
                if not result:
                    remaining.append(candidate)
            self.state["pending"][second_key] = remaining
        if abs(gap) >= max(3 * noise, Decimal("0.05")):
            snapshot = self.score_snapshot(match, recv)
            gap_ms = self.state["gap_ms"]
            candidate = {
                "candidate_id": self.event_id(seq), "source_event_seq": seq,
                "slug": match["slug"], "book_role": role, "token": token,
                "candidate_recv_ms": recv, "recv_ms": recv, "exchange_timestamp": msg.get("timestamp"),
                "p": float(price), "ref": float(ref), "noise": float(noise), "gap": float(gap), "d": direction,
                **snapshot, **self.checks(role, direction, ref, snapshot),
                "cross_book": None, "confirming_book": None,
                "after_gap": gap_ms is not None and 0 <= recv - gap_ms < 60_000,
                "late": late, "processing_delay_ms": max(0, observed_ms - recv),
            }
            out.append(self.record("candidate", candidate, observed_ms, fire=None, fire_recv_ms=None))
            result = []
            if not closed:
                for other in self.state["seconds"].get(second_key, []):
                    if other["book_role"] != role:
                        result = self.confirm(candidate, other, observed_ms)
                        if result:
                            break
            if result:
                out.extend(result)
            elif closed:
                out.append(self.record("candidate_result", candidate, observed_ms, fire=None,
                                       reason="late_processing", fire_recv_ms=None))
            else:
                self.state["pending"].setdefault(second_key, []).append(candidate)
        if abs(gap) > noise and not closed:
            self.state["seconds"].setdefault(second_key, []).append(movement)
        return out
