"""Read-only Gamma and ESPN adapters; receipt timestamps follow body receipt."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .storage import now_ms

GAMMA = "https://gamma-api.polymarket.com"
ESPN = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SPORTS_WS = "wss://sports-api.polymarket.com/ws"
SLUG = re.compile(r"^nba-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}-\d{2}$")
ALIAS = {"PHO": "Suns"}  # Gamma preseason outcome name


def array(value):
    return json.loads(value) if isinstance(value, str) else value or []


def timestamp_ms(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def espn_date(game):
    # Matches the study's US-date convention, including games after UTC midnight.
    return (datetime.fromtimestamp(game["kickoff_ms"] / 1000, timezone.utc) - timedelta(hours=6)).strftime("%Y%m%d")


class HTTP:
    def get(self, base, path, **params):
        url = base + path + ("?" + urlencode(params) if params else "")
        with urlopen(Request(url, headers={"User-Agent": "trading-pm-data-feed/nba-recorder"}), timeout=10) as response:
            raw = response.read()
            recv_ms = now_ms()
        return json.loads(raw), recv_ms


def game_from_event(event, recv_ms):
    slug = event.get("slug", "")
    if not SLUG.fullmatch(slug):
        return None
    candidates = [m for m in event.get("markets") or [] if m.get("slug") == slug]
    if len(candidates) != 1:
        return None
    m = candidates[0]
    tokens, outcomes = array(m.get("clobTokenIds")), array(m.get("outcomes"))
    if len(tokens) != 2 or len(outcomes) != 2 or len(set(map(str, tokens))) != 2 or len(set(outcomes)) != 2:
        return None
    if not m.get("gameStartTime") or not m.get("conditionId") or not m.get("id"):
        return None
    return {"slug": slug, "event_id": str(event["id"]), "market_id": str(m["id"]),
            "conditionId": m["conditionId"], "clobTokenIds": list(map(str, tokens)), "outcomes": outcomes,
            "gameStartTime": m["gameStartTime"], "kickoff_ms": timestamp_ms(m["gameStartTime"]),
            "closed": bool(m.get("closed")), "closedTime": m.get("closedTime"),
            "fee_fields": {k: v for k, v in m.items() if "fee" in k.lower()},
            "recv_ms": recv_ms}


def discover(http, ms):
    sports, _ = http.get(GAMMA, "/sports")
    tag = next(s["primaryTagId"] for s in sports if s.get("sport", "").lower() == "nba")
    start = datetime.fromtimestamp(ms / 1000, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
    seen = set()
    for week in range(3):
        lower = start + timedelta(days=7 * week)
        offset = 0
        fingerprints = set()
        while True:
            events, recv = http.get(GAMMA, "/events", tag_id=tag, end_date_min=lower.isoformat(),
                                    end_date_max=(lower + timedelta(days=7)).isoformat(), limit=100, offset=offset)
            if not isinstance(events, list):
                raise ValueError("Gamma events response must be a list")
            fingerprint = tuple(str(e["id"]) for e in events)
            if events and fingerprint in fingerprints:
                raise ValueError("Gamma repeated a pagination page")
            fingerprints.add(fingerprint)
            for event in events:
                game = game_from_event(event, recv)
                if game and game["slug"] not in seen:
                    seen.add(game["slug"])
                    yield game
            if len(events) < 100:
                break
            offset += 100


def map_event(game, events):
    names = [ALIAS.get(t, t) for t in game["outcomes"]]
    matches = []
    for event in events:
        comps = event.get("competitions") or []
        if not comps:
            continue
        sides = {c["homeAway"]: c.get("team", {}).get("name") for c in comps[0].get("competitors", [])}
        if set(sides.values()) == set(names) and set(sides) == {"home", "away"}:
            matches.append((event, sides))
    if len(matches) != 1:
        return {"espn_event": None, "team0_home": None, "mapping_status": "ambiguous" if matches else "missing"}
    event, sides = matches[0]
    return {"espn_event": str(event["id"]), "team0_home": sides["home"] == names[0], "mapping_status": "matched"}


def resolved(market):
    # Closure alone is not a settlement, and a tie can pay 0.5 on each token.
    prices = array(market.get("outcomePrices"))
    return bool(market.get("closed") and market.get("umaResolutionStatus") == "resolved" and len(prices) == 2)
