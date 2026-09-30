"""Read-only source adapters. Team matching is adapted from research scripts 11/12."""
from __future__ import annotations

import csv
import json
import re
import threading
import time
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .storage import now_ms

TAGS = ("epl", "la-liga", "sea", "bundesliga", "ligue-1", "ucl", "uel",
        "mex", "brazil-serie-a", "mls", "fifa-friendly")
GAMMA = "https://gamma-api.polymarket.com"
SOFA = "https://www.sofascore.com/api/v1"
SOFA_FALLBACK = "https://api.sofascore.com/api/v1"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_AGENT = "trading-pm-data-feed/soccer-dryrun"
SOFA_HEADERS = {"Referer": "https://www.sofascore.com/", "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
GENERIC = re.compile(r"^(?:fc|cf|sc|ac|as|afc|cd|ca|sv|fk|bk|if|sk|us|ss|ssc|rc|rcd|sd|ud|cs|club|calcio|balompie|futbol|fussball|de|di|da|del|la|le|el|los|and|\d{2,4})$")


def timestamp_ms(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value * 1000) if value < 10**11 else int(value)
    value = str(value)
    if value.isdigit():
        return timestamp_ms(int(value))
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def array(value):
    return json.loads(value) if isinstance(value, str) else (value or [])


def words(name):
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return {w for w in re.sub(r"[^a-z0-9 ]+", " ", s).split() if not GENERIC.match(w)}


def similarity(a, b):
    wa, wb = words(a), words(b)
    if not wa or not wb:
        return 0
    overlap = len(wa & wb)
    return 100 * overlap / min(len(wa), len(wb)) + 10 * overlap + SequenceMatcher(None, " ".join(sorted(wa)), " ".join(sorted(wb))).ratio()


def aliases_from(path=None):
    path = Path(path) if path else Path(__file__).with_name("soccer_team_aliases.csv")
    with path.open() as f:
        return {r["pm_name"]: r["query"] for r in csv.DictReader(f)}


def team_whitelist_from(path=None):
    """Load reviewed identities; a query-only alias file cannot authorize a team."""
    path = Path(path) if path else Path(__file__).with_name("soccer_team_aliases.csv")
    teams = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        required = {"pm_name", "query", "sofascore_team_id"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"{path}: team whitelist requires pm_name, query, sofascore_team_id columns")
        for row in reader:
            name, query = (row.get("pm_name") or "").strip(), (row.get("query") or "").strip()
            raw_id = (row.get("sofascore_team_id") or "").strip()
            if not name or not query or not raw_id.isascii() or not raw_id.isdigit() or int(raw_id) <= 0:
                raise ValueError(f"{path}:{reader.line_num}: expected names and a positive Sofascore team ID")
            if name in teams:
                raise ValueError(f"{path}:{reader.line_num}: duplicate PM team name {name!r}")
            teams[name] = {"id": int(raw_id), "name": query}
    if not teams:
        raise ValueError(f"{path}: team whitelist is empty")
    return teams


def match_from_event(event, received_ms):
    """Accept only an unambiguous, complete trio of dated moneyline questions."""
    title = event.get("title", "")
    teams = re.split(r"\s+vs\.?\s+", title, maxsplit=1, flags=re.I)
    if len(teams) != 2 or not re.fullmatch(r"[A-Za-z0-9_-]+", event.get("slug", "")):
        return None
    home, away = teams
    books, starts = {}, []
    for market in event.get("markets", []):
        if market.get("sportsMarketType") not in ("moneyline", "child_moneyline"):
            continue
        question = market.get("question", "")
        if "end in a draw" in question.lower():
            role = "draw"
        else:
            m = re.match(r"^Will\s+(.*?)\s+win on\b", question, re.I)
            if not m:
                continue
            sh, sa = similarity(m[1], home), similarity(m[1], away)
            if sh == sa or max(sh, sa) < 100:
                continue
            role = "home" if sh > sa else "away"
        tokens, outcomes = array(market.get("clobTokenIds")), array(market.get("outcomes"))
        if len(tokens) != 2 or set(outcomes) != {"Yes", "No"}:
            continue
        start = timestamp_ms(market.get("gameStartTime") or event.get("gameStartTime"))
        if start is None or role in books:
            return None
        starts.append(start)
        books[role] = {
            "id": str(market["id"]), "condition_id": market["conditionId"], "question": question,
            "tokens": dict(zip(outcomes, map(str, tokens))), "sportsMarketType": market["sportsMarketType"],
            "fee_fields": {k: market[k] for k in ("feeSchedule", "fee_schedule", "feesEnabled", "takerBaseFee", "makerBaseFee") if k in market},
            "closed": bool(market.get("closed")), "closed_ms": timestamp_ms(market.get("closedTime")),
            "outcomes": outcomes, "outcomePrices": array(market.get("outcomePrices")),
            "umaResolutionStatus": market.get("umaResolutionStatus"),
        }
    if set(books) != {"home", "draw", "away"} or len(set(starts)) != 1:
        return None
    return {"slug": event["slug"], "event_id": str(event["id"]), "title": title,
            "home": home, "away": away, "kickoff_ms": starts[0], "books": books, "recv_ms": received_ms}


class GammaClient:
    def get(self, path, **params):
        url = GAMMA + path + ("?" + urlencode(params) if params else "")
        with urlopen(Request(url, headers={"User-Agent": USER_AGENT}), timeout=20) as response:
            return json.load(response)

    def discover(self):
        seen = set()
        for tag in TAGS:
            cursor = None
            cursors = set()
            while True:
                params = dict(tag_slug=tag, closed="false", limit=100)
                if cursor:
                    params["after_cursor"] = cursor
                payload = self.get("/events/keyset", **params)
                batch = payload.get("events")
                if not isinstance(batch, list):
                    raise ValueError("Gamma events response is not a list")
                for event in batch:
                    if str(event["id"]) not in seen:
                        seen.add(str(event["id"]))
                        yield event
                cursor = payload.get("next_cursor")
                if not cursor:
                    break
                if cursor in cursors:
                    raise ValueError("Gamma repeated its pagination cursor")
                cursors.add(cursor)
                time.sleep(0.2)


class BackoffError(RuntimeError):
    def __init__(self, status, until_ms):
        self.status, self.until_ms = status, until_ms
        super().__init__(f"Sofascore HTTP {status}; backoff until {until_ms}")


class SofaHTTPError(RuntimeError):
    def __init__(self, status, path):
        self.status, self.path = status, path
        super().__init__(f"Sofascore HTTP {status}: {path}")


class SofaClient:
    """Bounded IPv4 requests with endpoint cooldowns and provider-wide rate limits.

    A single request lock serializes HTTP admission across live/auxiliary threads;
    waiting and both endpoint attempts share a ten-second budget. It does not
    hold the recorder or relay locks. Endpoint failure never poisons its peer.
    """
    def __init__(self, *, session_factory=None, clock=None, monotonic=None):
        from curl_cffi import requests, CurlOpt
        factory = session_factory or requests.Session
        self.endpoints = [SOFA, SOFA_FALLBACK]
        self.sessions = {url: factory(curl_options={CurlOpt.IPRESOLVE: 1}) for url in self.endpoints}
        self.health = {url: {"until_ms": 0, "status": None} for url in self.endpoints}
        self.until_ms, self.status = 0, None  # Provider-wide 429 cooldown only.
        self.lock = threading.Lock()
        self.clock = clock or now_ms
        self.monotonic = monotonic or time.monotonic
        self.local = threading.local()

    @property
    def last_http(self):
        return getattr(self.local, "http", None)

    @staticmethod
    def retry_after_ms(value, now):
        try:
            return max(180_000, int(float(value) * 1000))
        except (TypeError, ValueError, OverflowError):
            try:
                return max(180_000, int(parsedate_to_datetime(value).timestamp() * 1000) - now)
            except (TypeError, ValueError, OverflowError):
                return 180_000

    def get(self, path, **params):
        deadline = self.monotonic() + 10
        trace = {"provider": "sofascore", "address_family": "IPv4", "attempts": []}
        self.local.http = trace
        if not self.lock.acquire(timeout=10):
            raise TimeoutError("Sofascore request admission timed out")
        try:
            if self.clock() < self.until_ms:
                raise BackoffError(self.status, self.until_ms)
            for url in self.endpoints:
                state = self.health[url]
                if self.clock() < state["until_ms"]:
                    continue
                remaining = deadline - self.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Sofascore request budget exhausted")
                attempt = {"hostname": url.split('/')[2], "request_ms": self.clock()}
                trace["attempts"].append(attempt)
                try:
                    response = self.sessions[url].get(url + path, params=params,
                        headers=SOFA_HEADERS, impersonate="chrome", timeout=min(5, remaining))
                except Exception as exc:
                    attempt.update(error=type(exc).__name__, received_ms=self.clock())
                    state.update(until_ms=self.clock() + 10_000, status=503)
                    continue
                status = response.status_code
                attempt.update(status=status, received_ms=self.clock())
                if status in (403, 429):
                    try:
                        error = response.json().get("error", {})
                        reason = error.get("reason") if isinstance(error, dict) else None
                        if reason: attempt["reason"] = str(reason)[:120]
                    except (ValueError, AttributeError):
                        pass
                    delay = self.retry_after_ms(response.headers.get("Retry-After"), self.clock())
                    state.update(until_ms=self.clock() + delay, status=status)
                    if status == 429:
                        self.until_ms, self.status = state["until_ms"], status
                        raise BackoffError(status, self.until_ms)
                    continue
                if status >= 500:
                    state.update(until_ms=self.clock() + 10_000, status=status)
                    continue
                if status != 200:
                    raise SofaHTTPError(status, path)  # Especially 404: preserve resolver semantics.
                try:
                    payload = response.json()
                    key = "event" if re.fullmatch(r"/event/\d+", path) else "events"
                    expected = dict if key == "event" else list
                    if not isinstance(payload, dict) or not isinstance(payload.get(key), expected):
                        raise ValueError("Unexpected Sofascore JSON schema")
                    if key == "event" and str(payload[key].get("id")) != path.rsplit('/', 1)[1]:
                        raise ValueError("Sofascore event ID mismatch")
                    if key == "events" and any(not isinstance(e, dict) or not isinstance(e.get("id"), int)
                                               for e in payload[key]):
                        raise ValueError("Invalid Sofascore event list")
                except (ValueError, TypeError) as exc:
                    attempt["error"] = str(exc)[:120]
                    state.update(until_ms=self.clock() + 10_000, status=502)
                    continue
                state.update(until_ms=0, status=None)
                trace.update(hostname=attempt["hostname"],
                             cache={k: response.headers[k] for k in ("Date", "Age", "Cache-Control")
                                    if k in response.headers})
                return payload
            blocked = min(self.health.values(), key=lambda h: h["until_ms"])
            raise BackoffError(blocked["status"], blocked["until_ms"])
        finally:
            self.lock.release()


def score_row(event, poll_ms, previous_change=None, source="live"):
    status, clock = event.get("status") or {}, event.get("time") or {}
    description = status.get("description", "").lower()
    period = {"1st half": 1, "first half": 1, "2nd half": 2, "second half": 2}.get(description)
    if period is None and status.get("type") == "inprogress":
        period = {6: 1, 7: 2}.get(status.get("code"))
    change = (event.get("changes") or {}).get("changeTimestamp", event.get("changeTimestamp"))
    stale = previous_change is not None and change is not None and change < previous_change
    return {
        "id": event["id"], "home_team": (event.get("homeTeam") or {}).get("name"),
        "away_team": (event.get("awayTeam") or {}).get("name"),
        "home_score": (event.get("homeScore") or {}).get("current"),
        "away_score": (event.get("awayScore") or {}).get("current"),
        "status": status, "status_type": status.get("type"), "period": period,
        "currentPeriodStartTimestamp": clock.get("currentPeriodStartTimestamp"),
        "lastPeriodEndTimestamp": clock.get("lastPeriodEndTimestamp"),
        "changeTimestamp": change, "poll_ms": poll_ms, "stale": int(stale), "source": source,
        "raw_event": event,
    }


class Resolver:
    def __init__(self, client, teams):
        self.client, self.teams = client, teams

    def get(self, path, **params):
        # Pace auxiliary requests without delaying the separate live poll loop.
        time.sleep(0.8)
        return self.client.get(path, **params)

    def team(self, name):
        return self.teams.get(name.strip())

    def resolve(self, match):
        teams = {side: self.team(match[side]) for side in ("home", "away")}
        missing = [match[side] for side, team in teams.items() if team is None]
        result = {"sofa_event_id": None, "mapping_status": "missing", "mapping_error": None,
                  "mapping_http_status": None, "mapping_lookup_404s": [], "unverified_teams": missing}
        if missing:
            return {**result, "mapping_status": "team_not_whitelisted"}
        home_id, away_id = teams["home"]["id"], teams["away"]["id"]
        result["mapping_team_ids"] = {"home": home_id, "away": away_id}
        if home_id == away_id:
            return {**result, "mapping_status": "team_identity_conflict"}
        # Historical script 11 used /last; pre-kickoff resolution needs /next too.
        events = {}
        for direction in ("next", "last"):
            path = f"/team/{home_id}/events/{direction}/0"
            try:
                payload = self.get(path)
            except SofaHTTPError as exc:
                if exc.status != 404:
                    raise
                result["mapping_lookup_404s"].append(path)
                continue
            if not isinstance(payload.get("events"), list):
                raise ValueError(f"Sofascore {path}: response has no events array")
            events.update({e["id"]: e for e in payload.get("events", [])})
        near = []
        for event in events.values():
            delta = abs(event["startTimestamp"] * 1000 - match["kickoff_ms"])
            if delta > 3 * 3600_000:
                continue
            home, away = event["homeTeam"], event["awayTeam"]
            pair = (home["id"], away["id"])
            swapped = pair == (away_id, home_id)
            if not swapped and pair != (home_id, away_id):
                continue
            near.append((delta, event, swapped))
        near.sort(key=lambda v: v[0])
        if not near:
            return result
        if len(near) > 1 and near[1][0] - near[0][0] <= 30 * 60_000:
            return {**result, "mapping_status": "ambiguous"}
        delta, event, swapped = near[0]
        return {**result, "sofa_event_id": event["id"], "mapping_status": "matched", "sofa_swapped": swapped,
                "sofa_home": event["homeTeam"]["name"], "sofa_away": event["awayTeam"]["name"],
                "sofa_start_ms": event["startTimestamp"] * 1000, "mapping_delta_ms": delta}


def token_index(matches):
    return {token: (match["slug"], role, outcome)
            for match in matches.values() for role, book in match["books"].items()
            for outcome, token in book["tokens"].items()}


def market_rows(payload, recv_ms, index):
    """Split transport arrays; never dedup trades, including identical messages."""
    for message in payload if isinstance(payload, list) else [payload]:
        if not isinstance(message, dict):
            continue
        if message.get("event_type") == "price_change":
            groups = {}
            for change in message.get("price_changes", []):
                info = index.get(str(change.get("asset_id")))
                if info:
                    groups.setdefault(info[0], []).append(change)
            for slug, changes in groups.items():
                yield {"slug": slug, "recv_ms": recv_ms, "exchange_timestamp": message.get("timestamp"),
                       "message": {**message, "price_changes": changes}}
        else:
            info = index.get(str(message.get("asset_id")))
            if info:
                slug, role, outcome = info
                yield {"slug": slug, "book_role": role, "outcome": outcome, "recv_ms": recv_ms,
                       "exchange_timestamp": message.get("timestamp"), "message": message}


def settlement_of(market):
    outcomes, prices = array(market.get("outcomes")), array(market.get("outcomePrices"))
    if not market.get("closed") or market.get("umaResolutionStatus") != "resolved":
        return None
    if len(outcomes) != 2 or len(prices) != 2 or set(outcomes) != {"Yes", "No"}:
        return None
    values = dict(zip(outcomes, map(float, prices)))
    if values not in ({"Yes": 1.0, "No": 0.0}, {"Yes": 0.0, "No": 1.0}):
        return {"y": None, "status": "non_binary_resolution", "outcomePrices": prices}
    return {"y": int(values["Yes"]), "status": "resolved", "outcomePrices": prices}
