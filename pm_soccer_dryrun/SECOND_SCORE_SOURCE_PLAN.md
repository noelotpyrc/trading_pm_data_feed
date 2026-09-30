# Score-source resilience plan — 2026-09-30

Status: SofaScore routing implemented with 139 passing regression tests; live
preflight and deployment are tracked separately. The isolated FotMob shadow
collector is now built and tested; see [FOTMOB_SHADOW.md](FOTMOB_SHADOW.md).
Continuous deployment and active-provider selection remain future steps.
An opt-in candidate-based book-capture gate is now implemented separately from
active-provider selection; see FOTMOB_SHADOW.md. It preserves original fire and
filter results and conservatively captures extra depth during SofaScore outages.
Keep the live recorder causal and order-free, with JSONL storage,
no recovery journal, and the existing qualifying-fire book windows.

## 1. Recommended sequence

1. Restore SofaScore access with the website API over IPv4 as the preferred
   route. Keep the original API hostname as an independently monitored fallback.
2. Add FotMob as a separately recorded second source in shadow mode. Initially
   it cannot change fires, score filters, match expiry, or book-window decisions.
3. Compare coverage, timing, clock semantics, score corrections and disagreement
   on live matches. Only then introduce an explicit provider-selection policy
   and version the signal input contract. Do not silently substitute FotMob
   inside the existing SofaScore fields.

Two SofaScore hostnames are transport alternatives for one provider; FotMob is
a second provider. These are different kinds of fallback.

## 2. SofaScore endpoint routing

Preferred: `https://www.sofascore.com/api/v1`, IPv4.
Fallback: `https://api.sofascore.com/api/v1`, IPv4.
Use the existing Chrome impersonation and request headers. Apply this routing
to live scores, fixture lookups and event-detail requests, not just live polls.

The original hostname currently returns 403 on both machines. It is a configured
candidate fallback, not demonstrated redundancy. Both hosts can fail together.

- Keep separate HTTP sessions and health/cooldown state per endpoint, shared by
  the live poller and auxiliary resolver. Serialize admission of recovery probes
  so simultaneous threads cannot generate duplicate retries.
- On 403, record the response's short error reason and cool that endpoint down
  for at least three minutes. Try the other eligible endpoint once within the
  request budget. A blocked endpoint must not block a healthy endpoint.
- On 429, respect `Retry-After` and apply provider-wide backoff rather than using
  the other hostname to continue requests during a rate limit.
- On transport failures, 5xx, or a 200 response with invalid JSON/schema, allow
  one alternate-endpoint attempt with a short failed-endpoint cooldown. Use a
  five-second per-attempt timeout and ten-second overall request budget, below
  the relay's source-staleness timeout. No nested retry loops.
- A valid 404 remains a missing resource. Preserve the resolver's next/last
  fixture handling; do not turn every missing fixture into endpoint switching.
- Stay on the successful route. While using fallback, probe the preferred route
  once when its cooldown expires, using an ordinary required request. Failed
  recovery probes must not suppress useful fallback results.
- If both routes fail, publish explicit unavailable/backoff health, preserving
  the existing historical score-age behavior. Do not fabricate fresh scores or
  change frozen fire/filter semantics as part of a transport fix.
- Record provider, hostname, address family, request/response/application times,
  route changes, status, short error reason, timeout, cooldown and relevant
  cache headers. Propagate route provenance into score observations and fires.
  Do not log cookies or full HTTP headers.
- Record a diagnostic on mapping backoff with affected slug and retry deadline;
  currently a 403 can leave mapping status blank, making the outage less clear.

No periodic IPv6/address rotation or CAPTCHA handling is part of this design.

## 3. FotMob collection and mapping

Verified website routes:

- `https://www.fotmob.com/api/data/matches?date=YYYYMMDD&timezone=America/New_York&ccode3=USA`
- `https://www.fotmob.com/api/data/matchDetails?matchId=<id>`

The older `/api/matches` route returned 404. The current routes returned valid
match lists and event details from Genie and the local machine, including
scores, halftime/period information, goal incidents and `hasPendingVAR`.
This is a connectivity result, not proof of sustained availability or a
supported data-feed agreement.

### Mapping

- Add separately verified FotMob team IDs to a provider-specific alias file,
  covering the existing league/team whitelist. Never reuse SofaScore numeric IDs.
- Join fixtures using both verified team IDs, home/away orientation, competition
  and kickoff. Retain the existing three-hour tolerance and ambiguity checks;
  log kickoff disagreement explicitly. No unreviewed fuzzy-name fallback.
- Use the configured date's fixtures and adjacent dates when the collection
  window crosses midnight; retain UTC kickoff and the requested date/timezone.
- Store independent `fotmob_match_id`, mapping status, diagnostics and observed
  kickoff. A SofaScore mapping failure must not prevent FotMob mapping.
- Restrict live retention to PM-tracked matches. Global match lists can support
  discovery in memory but are not archived.

### Polling and payloads

- Initially request required match lists every ten seconds and tracked in-play
  details every ten seconds. The observed
  cache headers specify ten-second caching; polling every five seconds cannot
  be assumed to give five-second freshness. Pace fixture/detail work globally,
  bound concurrency, and measure request load with multiple simultaneous games.
- Start/stop with the existing collection window; do not add an all-day
  high-frequency FotMob loop when no matches need collection. Check final status
  after disappearance and retain postponement/cancellation distinctly from FT.
- Preserve the complete tracked match-list object and complete available incident
  timeline, including goals, cards, substitutions and VAR corrections, whenever
  the endpoint provides them. Do not retain unrelated matches or assume every
  fixture has full event coverage.
- Record every successful observation's receive/application time. Store unchanged
  raw payloads once by content hash in the session, with explicit references from
  later observations; never omit observation timestamps merely because the score
  is unchanged. Hash references must resolve within the backed-up session.
- Keep response cache age/date, coverage level, provider match ID and raw provider
  clocks. Match-list/detail observations remain distinguishable. Offline re-pulls
  can add final stats but cannot replace live incident/VAR evidence.
- Validate payload and relay size limits. A too-large payload or failed write
  must produce explicit missing-coverage/error status, never silent truncation.

## 4. Relay, causality and source comparison

- Keep both providers on Genie in separate processes. The implemented sidecar
  uses a separate authenticated SSH connection and loopback port 18766, leaving
  SofaScore port 18765 untouched. Cooldowns, health and receiver state are separate.
- A distinct FotMob handshake checks protocol version and alias digest. The VPS
  supplies read-only catalog scope; there is no general-purpose HTTP RPC. Only
  approved match-list/detail routes are used, and the receiver verifies mappings.
- Namespace score state by `(provider, event_id)`. Maintain existing SofaScore
  behavior while writing FotMob observations to separate provider directories.
- Preserve source request/receipt/send, VPS receipt and locked application times.
  Only application time makes an observation eligible for a live decision.
  Reconnect with a fresh poll; no queued replay or backdating.
- Normalize home/away, score and status without mixing fields from two providers.
  A FotMob minute string or timezone-ambiguous period timestamp must not be fed
  into SofaScore's minute calculation. Unknown clocks stay unknown; define and
  test a FotMob clock adapter before it can supply filters.
- Compare first observed score transitions, VAR reversals, period/FT changes,
  mapping coverage, cache age, missing intervals and source disagreements.
  HTTP latency is not goal-reporting latency. Neither provider is ground truth
  merely because it is first or reports a higher score.
- Shadow disagreements are recorded diagnostics, not retrospective corrections
  to fires. A future active-provider policy must define freshness, ambiguity,
  switching/recovery and disagreement behavior, and record the complete selected
  snapshot and reason on each fire. Release it as a versioned behavior change.

## 5. Validation and rollout

SofaScore routing tests: primary success; 403 then alternate success; both
blocked; one recovery probe across concurrent workers; provider-wide 429 and
Retry-After; malformed success; timeout budget; 404 preservation; sticky route;
hostname/address-family provenance through the relay. Existing causal engine,
resolver and book-window tests must still pass.

FotMob adapter tests: exact/swapped/ambiguous teams, date boundaries, missing
scores/clocks, HT/FT/postponed status, score decrement/VAR, duplicate payload
references, provider ID collisions, independent outages, frame-size failures
and causal application-time ordering. Shadow input must not change existing
SofaScore fires or qualify a book window.

Run a controlled relay integration test before production replacement. Preserve
old recordings; honor the run manifest's code/schema identity by starting a new
run directory for changed contracts. Deployment requires coordinated Genie/VPS
protocol compatibility and rollback to the preserved package/configuration.

Acceptance before provider promotion: multiple live-match batches including
simultaneous games, measured update-delay distributions and disagreement cases,
no unexplained mapping gaps, bounded resource/storage cost, and independently
tested failure/recovery. A few HTTP 200 responses are insufficient.

## 6. Diagnostic evidence

The 2026-09-30 four-way probe found website IPv4 succeeds on both machines;
website IPv6 succeeds locally but is challenged on Genie; the original API
hostname fails on both address families. The same curl_cffi version and headers
were used. The machines share public IPv4 but have different IPv6 addresses.

Bounded preferred-route probe output:
`data/sofa-route-probe-20260930/genie.jsonl` (repository root).
This standalone diagnostic does not change the live worker or demonstrate a
healthy original-host fallback.

Completed 09:59:33–10:02:33 EDT on 2026-09-30:

- Website IPv4: 40/40 HTTP 200 responses (38 live-list requests including the
  initial check, one team-fixture lookup and one event-detail lookup).
- Scheduled sequence: 37 live polls at five-second intervals across 180 seconds.
  Live HTTP latency median 23.34 ms, p95 124.08 ms, maximum 124.40 ms.
- Eighteen distinct live payloads were observed. This verifies changing response
  content, not the latency of an individual goal or absence of cached data.
- Original-host baseline: HTTP 403 `Forbidden`. Website requests remained
  successful while that hostname was in its diagnostic cooldown.
- No website failure occurred, so a website-to-original successful failover was
  not observed. Deterministic routing tests and relay integration subsequently
  passed, and the SofaScore change was deployed in commit `0b367f6`. Sustained
  second-provider comparison remains future work.
- Reproduction script and machine-readable summary are alongside the JSONL as
  `probe.py` and `summary.json`.
