"""Study ESPN change logger, with durable first sightings and tombstones."""
from __future__ import annotations

from .storage import append

KEYS = ("seq", "wallclock", "period", "clock", "type", "scoring", "away", "home", "text", "modified")


def plays(payload):
    raw = payload.get("plays")
    if not isinstance(raw, list):
        raise ValueError("ESPN summary has no plays list")
    return {str(p["id"]): {"seq": p.get("sequenceNumber"), "wallclock": p.get("wallclock"),
                          "period": (p.get("period") or {}).get("number"),
                          "clock": (p.get("clock") or {}).get("displayValue"),
                          "type": (p.get("type") or {}).get("text"), "scoring": p.get("scoringPlay"),
                          "away": p.get("awayScore"), "home": p.get("homeScore"), "text": p.get("text"),
                          "modified": p.get("modified"), "raw_play": p} for p in raw}


class ESPNLog:
    def __init__(self, store):
        self.store = store
        self.seen = {}
        self.first = {}
        self.headers = {}
        self.started = set()

    def record(self, game, payload, recv_ms):
        event = game["espn_event"]
        date = game["espn_date"]
        base = {"recv_ms": recv_ms, "event": event, "slug": game["slug"]}
        c = payload["header"]["competitions"][0]
        scores = {x["homeAway"]: x.get("score") for x in c["competitors"]}
        status = c["status"]
        header = {"away": scores.get("away"), "home": scores.get("home"), "clock": status.get("displayClock"),
                  "period": status.get("period"), "state": status["type"]["state"]}
        if self.headers.get(event) != header:
            append(self.store, f"espn/{date}/header.jsonl", "espn_header",
                   {**base, **header, "raw_header": payload["header"]})
            self.headers[event] = header
        old = self.seen.get(event, {})
        opening = header["state"] == "pre" or header["state"] == "in" and header["period"] in (None, 0, 1)
        if payload.get("plays") in (None, []) and event not in self.started and opening:
            # ESPN has no plays before the opening play. Once any play is seen,
            # a missing collection is a feed error, never evidence of deletion.
            return header["state"], None
        new = plays(payload)
        if new:
            self.started.add(event)
        latest_new = None
        for pid, p in new.items():
            key = (event, pid)
            first = key not in self.first
            if first:
                self.first[key] = recv_ms
                latest_new = recv_ms
            if pid not in old or any(old[pid][k] != p[k] for k in KEYS):
                append(self.store, f"espn/{date}/plays.jsonl", "espn_play",
                       {**base, "id": pid, **p, "first_recv_ms": self.first[key],
                        "kind": "new" if first else "changed"})
        for pid in old.keys() - new.keys():
            append(self.store, f"espn/{date}/plays.jsonl", "espn_play",
                   {**base, "id": pid, **old[pid], "first_recv_ms": self.first[(event, pid)], "kind": "gone"})
        self.seen[event] = new
        return header["state"], latest_new

