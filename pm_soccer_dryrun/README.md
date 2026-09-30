# Soccer impulse signal — live recording, offline evaluation

The implementation records market data, score observations, and causal signals.
It has no orders, positions, hypothetical fills, or P&L. `SPEC.md` defines the
contract. Offline evaluation belongs in `data_analysis/inplay_momentum_feasibility`;
its reference is `results/13_reference_cell.md` / `scripts/18_reference_cell.py`.

SofaScore now prefers the website API over IPv4, with the original API hostname
as fallback. [The second-source plan](SECOND_SCORE_SOURCE_PLAN.md) describes the
remaining provider-selection work. The [FotMob shadow collector](FOTMOB_SHADOW.md)
is built and tested as a separate sidecar; it is not enabled in production.
The optional `--fotmob-shadow-dir` flag lets fresh FotMob observations open
candidate-based book windows during SofaScore outages. It preserves original
fire/filter results; see the shadow collector guide for policy and activation.

## Run

Requires Python 3.10+, `websocket-client`, and `curl_cffi` (see this directory's
`requirements.txt`). The repository's local interpreter is
`/Users/noel/projects/venvs/production/bin/python`. Install missing dependencies
in the intended environment before launching.

Run one process from the repository root:

```sh
python -m pm_soccer_dryrun live --data-dir data/pm_soccer_dryrun --run-id soccer-v2-20260919 --book-window-minutes 5
```

The collector, score poller, metadata resolver and signal engine share one
process. Network requests run in worker threads. A shared lock orders input
recording and synchronous signal evaluation; there is no database, durable
input queue, engine checkpoint or restart replay. The old `collect`, `scores`
and `engine` commands are replaced by `live`.

Each invocation creates `sessions/<UTC timestamp>-<unique id>/` inside the data
directory. The parent `run.json` binds the run ID, rule version, code hash,
team-whitelist hash and depth-window duration; changing these requires a new data directory. Restarting the same implementation
starts a fresh session, rediscovers matches and rebuilds fill/score windows from
new observations. Previously recorded inputs are not replayed. Session IDs keep
candidate IDs and event joins distinct across restarts. Only one live process
may own a data directory.

- Websocket recording subscribes to the six tokens of each eligible match.
  Hourly Gamma discovery uses keyset pagination across `epl`, `la-liga`, `sea`,
  `bundesliga`, `ligue-1`, `ucl`, `uel`, `mex`, `brazil-serie-a`, `mls`, and
  `fifa-friendly`; the generic soccer tag is excluded. New matches must start
  between four hours ago and seven days ahead. Known matches refresh at T−35
  minutes and every minute while subscribed. Full discovery responses are
  discarded after parsing. Only changed compact catalog records are written:
  IDs, tokens, team names, kickoff, fee fields, mapping and lifecycle fields.
- One Sofascore live poll runs every five seconds. The auxiliary resolver
  uses verified team IDs to fetch upcoming and recent fixtures, requires both
  IDs and kickoff within three hours, and records home/away swaps. Two candidate
  fixtures within 30 minutes of each other's kickoff-distance remain ambiguous.
  `--aliases PATH.csv` overrides the bundled `pm_name,query,sofascore_team_id`
  whitelist; legacy query-only files fail startup. `query` is the canonical name
  retained for readability; the resolver never calls global team search.
  The bundled file covers 865 verified PM names from the configured
  leagues; [TEAM_ALIASES.md](TEAM_ALIASES.md) and
  [the audit CSV](soccer_team_aliases_audit.csv) document identities, historical
  membership and 25 names requiring review. Either team missing from the whitelist
  leaves the match unmapped with `mapping_status=team_not_whitelisted` and
  `unverified_teams`; market recording continues. No fallback guesses a team.
  Missing/ambiguous fixtures and request errors retry after 60 seconds. A fixture
  list returning 404 does not prevent checking the other direction, and an
  individual match error does not abort the batch. Failed detail requests are
  also paced to at most once per minute. Mapping rows retain verified IDs,
  missing fixture endpoints and error diagnostics; other request failures are
  logged with match slug and phase in `ops/resolver`. Successful mappings persist.
  Auxiliary requests are paced. SofaScore uses `www.sofascore.com/api/v1` over
  IPv4 first, then `api.sofascore.com/api/v1`. A 403 cools only its endpoint for
  at least three minutes; a 429 pauses both endpoints and respects Retry-After.
  Transient failures and invalid payloads can try the alternate route once.
  Requests share a ten-second budget including admission, with five seconds per
  HTTP attempt. Route/status/cache metadata travels with relay observations into
  fire snapshots. Mapping backoff is explicit in the match catalog.
  Only mapped matches inside the market collection window are saved from the
  global live response. Every observation retains the complete `raw_event`,
  including unchanged and stale copies; no field subset or downsampling is
  applied. Poll health retains total/retained event counts, active match count
  and mapped event count, including polls with no tracked observations. A new
  mapping waits for its first tracked observation rather than seeding from an
  unrecorded global match. Detail fallback observations also retain all fields.
  The separate incident timeline (goals, cards, substitutions, VAR, etc.) is an
  offline pull of all incident types; it is not part of the live event payload.
  Offline incidents cannot establish when an incident first became visible live.
- Settlement responses are recorded daily after 06:00 UTC for earlier matches
  known in the current session, retrying unresolved markets on later days.
  Catalogs from earlier sessions remain available for offline settlement pulls;
  startup does not restore them. Binary settlement requires `closed`,
  `umaResolutionStatus=resolved` and 1/0 prices; other results have `y=null`.

A game can disappear from the live endpoint before full time is observed. The
mapping worker fetches `/event/{id}` at most once per minute for tracked games
absent for over a minute after kickoff. Those rows say `source=event_detail`.
If all markets close without a final score observation, market close supplies
an explicit lifecycle fallback, with another 30 minutes of collection. It never
supplies an invented score or clock.

A match with no accepted score, no final status, and no market close expires
at scheduled kickoff + four hours. A successful mapping alone is not score
coverage. Expiry removes it from both subscriptions and resolver work, is
recorded in the catalog/ops log, and releases engine windows after a one-minute
grace period. Tracked games with lifecycle evidence still follow the ordinary
later-of-close/full-time-plus-30-minutes rule, and targeted lifecycle refreshes
can attach market closure after the discovery admission window has passed.

## Signal records

A `candidate` is appended when the price threshold is met. It carries a stable
`candidate_id`, `candidate_recv_ms`, and the score/clock known at that receipt.
`fire=null` and `cross_book=null` mean confirmation is pending.

A linked `fire` is appended as soon as same-second confirmation is observed and
gate/hypothesis pass. An earlier confirming fill in that second allows an
immediate fire. Failed gates/hypotheses still get a linked `candidate_result`.
Unconfirmed candidates get `candidate_result` with `cross_book=false` at
second-close. There is one terminal record per candidate.

`confirmation_recv_ms` is when the collector had both required fills.
`fire_recv_ms` is when the engine actually knows the fire, never earlier than
confirmation; `emitted_ms` records evaluation time. Per the revised offline
spec, primary hypothetical entry starts at **`candidate_recv_ms + 1,000`**;
entry from **`fire_recv_ms + 1,000`** is the conservative variant. Both remain
offline computations. Score/clock
columns stay frozen at the candidate, including when a later poll arrives before
confirmation. Each confirming move uses that book's own prior-fill reference.

Processing beyond the collector's second is flagged `late`; known confirmation
can still produce a fire at the actual later time. If the second was already
finalized, cross-book stays unknown with `reason=late_processing`. Completed
seconds are not reopened and fires are never backdated.

Second-close progresses on depth messages as well as fills, or on the idle
clock between input deliveries. A quiet trade tape with continuous depth
updates therefore does not hold terminal candidate records open.

Every `last_trade_price` message counts once, including identical messages or
matching transaction hashes. No-token prices become `1-price`. The window starts
at scheduled kickoff, excludes the current fill, and uses the last five prior
fills until ten are available. The sixth fill is the first eligible candidate.
Pre-kickoff data is archived. All periods are collected; minute is null outside
active first/second halves.

Checks and filters use null for unknown facts and false for known failures.
Missing/stale feeds do not suppress candidate recording. `score_age_s` is time
since the accepted observation; `score_change_age_s` is time since the source's
change timestamp. An unchanged score can still have a fresh observation.
`score_stale` means the latest returned copy for that match was older; the engine
uses the last accepted copy. `poller_state` is `ok`, `backoff`, `error`, `unknown`
before any observation, or `unmapped`. Polls received after the candidate cannot
supply its score, even if delivered first to the engine.

`after_gap` covers connection/reconnection/disconnection and malformed input for
60 seconds. The public trade protocol has no documented sequence number, so
otherwise invisible lost trades cannot be detected. Malformed input stays in
raw data and gets an `input_error` diagnostic instead of a substitute value.

The market connection sends text `PING` every ten seconds, following the
[PM market heartbeat protocol](https://docs.polymarket.com/market-data/realtime-data).
Its monotonic watchdog reads queued frames before checking for thirty seconds
without any incoming data or PONG. Traffic proves the connection is alive, but
does not prove that the market data is fresh. Missing PONGs while data continues
are reported in `websocket_health` rather than forcing a reconnect that discards
queued messages. Once a minute, that diagnostic records frame count, PONG age,
incoming age, maximum frame-processing time and maximum recorder-lock wait.
Exchange and local receive timestamps remain on every market row for age
analysis; no freshness filter is added to the frozen signal rule.

## Output and restart behavior

The following paths are relative to `--data-dir/sessions/<session-id>/`; dates
are UTC. Records are written once, directly into their JSONL destination.

| Path | Contents |
|---|---|
| `session.json` | Run/code identity, session ID, start time; no recovery state |
| `team_whitelist.csv` | Immutable startup copy of the verified IDs used by this session (about 29 KB) |
| `matches/{metadata,resolver,scores}/YYYY-MM-DD.jsonl` | Changed catalog/mapping/lifecycle records |
| `pm_soccer/<slug>/YYYY-MM-DD.jsonl` | Market messages with collector receive time |
| `book_windows/<slug>/YYYY-MM-DD.jsonl` | Window open/extension markers and derived opening book snapshots |
| `score/{scores,resolver}/YYYY-MM-DD.jsonl` | Score observations, stale copies, raw event |
| `polls/YYYY-MM-DD.jsonl` | Poll health, receipt time, errors, backoff |
| `settlement/YYYY-MM-DD.jsonl` | Gamma market responses and resolution status |
| `fires/<run-id>/YYYY-MM-DD.jsonl` | Candidates, linked fires/results, input errors |
| `ops/<producer>/YYYY-MM-DD.jsonl` | Transport/discovery diagnostics, session boundaries, daily summary |

There is no `journal.sqlite` or `discovery/` archive. Discovery diagnostics retain
counts and errors, not full API payloads. The compact catalog is required for
offline joins and observed fee/mapping provenance. Unchanged refreshes do not
append another catalog row.

Every fill and low-volume market metadata message is retained throughout the
subscription. Depth (`book` and `price_change`) is written only after a `fire`
with both `f_minute30=true` and `f_leader1_up=true`, for five minutes by default
(`--book-window-minutes N`). The window covers all three books and both outcomes
of that match. Later qualifying fires extend the deadline to their fire time
plus N minutes, without duplicate updates. Other matches and unqualified fires
do not open or extend it. The deadline is exclusive and uses local receipt time.
Capture starts synchronously after the fire is written; later messages in its
websocket frame are included even if they share an earlier frame receipt time.
Collection/session termination can truncate a window; its deadline is not a
guarantee of coverage through that time.

Outside windows, only the current ladders are kept in memory, with no persisted
depth history. At opening, `book_windows` records the trigger, deadline, expected
tokens, missing tokens and a derived snapshot per initialized token. Each snapshot
has `source=local_book_state`, `recorded_ms`, `seed_recv_ms`, and the last depth
observation's `book_asof_recv_ms` and `exchange_timestamp`. This is the state known
at opening, not evidence of upstream freshness. Reconstruct from these snapshots
plus subsequent raw snapshots and absolute-size changes in event-sequence order.
`seed_metadata` preserves the initializing snapshot's non-level fields (its hash
and timestamps describe that original snapshot); `tick_size` reflects subsequent
observed tick-size changes, which are also retained in the market stream.
Both Yes and No updates are now retained inside windows, split only by match.
Disconnects/reconnects invalidate cached books; deltas alone cannot initialize a
complete ladder. Missing tokens remain unknown until a fresh wire snapshot arrives.
Invalid depth also invalidates affected cached tokens and records a diagnostic.

At most one small `capture_clock` market row per local second replaces discarded
depth traffic, preserving the engine's second-close ordering for replay. It is
explicitly tagged `source=local_depth_clock` and is not a wire market observation.
Minute-level websocket health and the final session summary include cumulative
depth received/recorded/discarded counts, window counts and cached token count.
Existing recordings are not modified or removed.

Each JSONL row has `session_id` and `event_seq`. Together they identify a source
observation and preserve delivery order across files; `event_seq` replaces
`journal_seq` in the new format. Candidate IDs include run ID, session ID and
source sequence. Signals retain `source_event_seq` and `confirming_event_seq`.

Engine rows are flushed to the OS immediately. Raw recording buffers are
normally flushed by the live loop every 20 ms and by producer workers. Open
file handles are bounded. SIGTERM/SIGINT stops workers, finalizes the pending
second and flushes/fsyncs output files before closing. Unexpected crashes can
lose buffered rows, leave a partial final JSONL line or leave a candidate
without a terminal result; power loss can also lose OS-buffered output. There
is intentionally no recovery/replay or exactly-once guarantee across a crash.
Restart uses a separate session directory rather than repairing old files.

Disk/write failures stop the session instead of silently continuing without
recording. Daily summaries use small in-memory counters since session start.
Copy completed session directories to the SSD using the existing backup
workflow. No database backup or WAL handling is needed. Data is not automatically
deleted. JSONL rotates daily; stdout goes to the supervisor journal.

## Deployment and tests

The VPS units use the isolated `/root/trading_pm_data_feed/.venv-soccer`
environment, with dependencies pinned in `requirements-vps.txt`. The recorder
instance is `pm-soccer@live`; do not start the old three instances. It restarts
on process failure and starts a fresh recording session. Its configured data
directory is `/root/trading_pm_data_feed/data/pm_soccer_dryrun/vps-genie-20260930`.
The earlier `vps-genie-20260921` recordings are preserved separately.
Review interpreter, data directory, and `SOCCER_RUN_ID` before installation.
Per `../CLAUDE.md`, VPS deployment requires explicit user authorization;
authorization was given on 2026-09-20.

`pm-soccer-resources.timer` invokes a separate, small sampler every minute.
It writes `resources/YYYY-MM-DD.jsonl` with cumulative CPU time, process RSS,
peak RSS, swap, systemd memory usage and restarts, host available memory and
swap, free disk space, and allocated recording size. CPU utilization can be
calculated from consecutive CPU-time samples. It has no disk cutoff, service
stop action or deletion. Retention belongs to the scheduled backup workflow;
these units do not configure backups. Include the run manifest, session files
and resource logs in backups, and do not remove files still being written.

Starting the service is conditional on successful Gamma, Sofascore and market
websocket connectivity checks. On 2026-09-20, the VPS returned HTTP 403 for
the Sofascore live endpoint on both `api.sofascore.com` and `www.sofascore.com`;
the latter explicitly returned `reason=challenge`. The same client succeeded
locally. Deployment remains blocked pending a working score
connection. The Genie relay below supplies that connection; direct VPS
Sofascore HTTP access remains blocked.
The normal shared three-minute 403/429 backoff remains unchanged.

### Genie score worker

`relay.py` separates Sofascore HTTP access from the recorder. The VPS still
collects Polymarket, resolves the verified team identities, records data, and
evaluates the frozen rule. Genie makes all Sofascore requests. It polls the live
endpoint every five seconds and immediately pushes full event objects only for
the VPS's tracked event IDs. Fixture/detail lookups use a separate thread over
the same persistent connection; they do not wait in the live polling thread.
Fixture responses carry identity/kickoff fields for mapping. Full tracked live
and detail event objects remain in the score recordings. No source recordings,
recovery queue, or discovery archive are written on Genie.

The transport uses framed JSON, TCP_NODELAY, heartbeat/disconnect detection,
bounded frames and requests, per-connection sequence numbers, and matching
team-whitelist digests. It binds to an explicit Tailscale address or to
`127.0.0.1` on both ends for an SSH tunnel, and accepts one configured peer IP.
Tailscale or SSH supplies encryption and peer identity; do not expose this
protocol on a public interface. The worker reconnects after
five seconds and resumes fresh polling, without replaying an outage backlog.
403/429 responses share the existing three-minute backoff across both threads.

VPS score `poll_ms` is the time the VPS applies the observation under the store
lock. It is never backdated to Genie's observation time. `relay` metadata retains
source request/response times, HTTP duration, worker send time, VPS receipt and
application times, and connection/sequence IDs; signals include `score_relay`.
Cross-host age fields are explicitly **unadjusted for clock offset**. Acknowledgment
RTT is measured on the worker's clock. Minute-level relay operations logs include
poll/observation counts, status, cumulative worker CPU time and peak RSS.
Source timeout/disconnection is recorded in poll health; this does not change
the frozen strategy's filter predicates or introduce trading.

Genie uses `/Users/noel/services/pm-soccer-score-worker/.venv` with the pinned
dependencies. The supplied `com.noel.pm-soccer-score-worker.plist` is a LaunchAgent
template: it starts at user login and restarts on exit, not before login after
a reboot. Create its `logs` directory before installation. Genie's system
sleep setting was already zero.

System-service migration was activated by the user and verified after noel
logged out on 2026-09-21 UTC. Both jobs run in the system domain. The daemon
templates under `launchdaemons/` set `UserName=noel` and `GroupName=staff` while loading in the system
domain, so they can run without a desktop session. The one-time migration
command used on Genie was:

```sh
sudo /usr/bin/python3 /Users/noel/services/pm-soccer-score-worker/pm_soccer_dryrun/install_genie_daemons.py
```

The installer validates both templates, backs up and retires the two
LaunchAgents, and installs root-owned system plists. It restores the old agent
files if installation fails. After migration, use `sudo launchctl print
system/com.noel.pm-soccer-score-worker` (and `...-tunnel`) to inspect services;
restart with `sudo launchctl kickstart -k system/<label>`. Fresh VPS scores
were verified after switching and after logout.
Genie has FileVault enabled: this removes the desktop-login dependency once
the OS boots, but does not automatically unlock its encrypted disk after a
cold boot. FileVault and power settings are unchanged.

The deployed connection uses a dedicated SSH tunnel from Genie to the VPS's
public address, because Genie cannot see the VPS through Tailscale. The VPS
recorder uses:

```sh
--score-relay-bind 127.0.0.1 --score-relay-peer 127.0.0.1 --score-relay-port 18765
```

Use a fresh run/data directory and point the resource sampler to the same root.
Verify actual relayed HTTP 200 polls, scoped score rows, market traffic and
resource samples before treating continuous deployment as running. Stop each
Genie daemon with `sudo launchctl bootout system/<label>` and the recorder
with `systemctl stop pm-soccer@live`; preserve completed recordings for the normal backup workflow.
The user authorized SSH key creation and installation on 2026-09-21 UTC.
The private key stays at `~/.ssh/pm_soccer_relay_ed25519` on Genie. The dedicated
VPS user `soccer-relay` has no shell/SFTP sessions, remote forwarding, or access
to other destinations; only local forwarding to `127.0.0.1:18765` is allowed.
The VPS public host key was obtained through the existing authenticated
administrative connection and pinned in the worker's `known_hosts` file.
Genie's original SSH config and existing VPS administrative keys are unchanged.

`com.noel.pm-soccer-score-tunnel.plist` supervises `/usr/bin/ssh -N -T -F
/Users/noel/services/pm-soccer-score-worker/ssh_config pm-soccer-relay`. SSH
heartbeats detect a dead connection in roughly 30 seconds; launchd restarts
the tunnel. The worker independently reconnects every five seconds. Restart
either system service on Genie with:

```sh
sudo launchctl kickstart -k system/com.noel.pm-soccer-score-tunnel
sudo launchctl kickstart -k system/com.noel.pm-soccer-score-worker
```

Worker/tunnel logs are under
`/Users/noel/services/pm-soccer-score-worker/logs`. Logs are not automatically
deleted. VPS recordings and resource samples use the data directory above.
The deployment backup at `/root/pm-soccer-relay-deploy-20260921` contains the
previous package, systemd units, SSH configuration, and bounded preflight data.
The SSH account restrictions follow the [OpenSSH server configuration](https://man.openbsd.org/sshd_config)
(`AllowTcpForwarding local`, `PermitOpen`, `PermitListen none`, `MaxSessions 0`).

```sh
/Users/noel/projects/venvs/production/bin/python -m pytest tests/test_soccer_dryrun.py tests/test_soccer_book_capture.py tests/test_soccer_resolver.py tests/test_soccer_resources.py tests/test_soccer_relay.py -q
```

Tests use deterministic source fixtures and temporary recording directories,
with no external requests. Offline entry/dedup/labels and baseline reconciliation remain in the
analysis project. Evaluation applies the post-match `keep` universe (at least
1,000 in-play fills and $100k taker notional per book), reports the unfiltered
set alongside it, and distinguishes the inherited market-hour baseline from
feed-clock match-hours. No pre-kickoff liquidity floor is applied live.
