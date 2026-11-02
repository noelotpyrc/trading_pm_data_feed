# NBA live recording

This package records the public feeds described in the private `SPEC.md`.
It evaluates no signals, submits no orders, and simulates no fills. Offline
evaluation and the statistical report belong in `data_analysis/pm_lob_nba`.

From the repository root, use the production Python environment:

```sh
PY=/Users/noel/projects/venvs/production/bin/python
$PY -m pm_nba_dryrun --help
$PY -m pm_nba_dryrun --rules pm_nba_dryrun/rules.json --check
```

The private `rules.example.json` supplied alongside the spec contains the frozen
definitions and the four report 07 cells. Copy it to `rules.json`; it needs no
further choices. Entry frequency and the decision rule are analysis choices kept in
`pm_lob_nba/dryrun_rules.json` (SPEC section 2); the recorder refuses rules files
that contain them. `--check` validates locally
without network access or creating directories. Rules files remain ignored by
Git, consistent with the repository's private strategy configuration policy.
The clock bin uses the highest `seq` among ESPN plays first seen by the fill (SPEC section 2).

After choosing the host, the recording command is:

```sh
$PY -m pm_nba_dryrun --rules pm_nba_dryrun/rules.json --run-id nba-2026-v1
```

Never deploy or run this on the VPS without explicit user confirmation, as
required by the repository's `CLAUDE.md`. This implementation does not install
services or choose a host. Runtime dependencies are the standard library and
`websocket-client`, already included in the root `requirements.txt`.

Each run freezes the exact rules file bytes and their SHA-256 in `run.json`.
Restarting the same run with identical rules creates a fresh session, with fresh
in-memory state. Changing any rules requires a new run ID. A data-directory
process lock prevents two concurrent collectors from writing the same dataset.
No prior stream is replayed at startup.

```text
data/pm_nba_dryrun/<run_id>/
  rules.json
  run.json
  sessions/<UTC timestamp>-<unique suffix>/
    run.json
    games/YYYY-MM-DD.jsonl
    book/<slug>.jsonl
    espn/YYYYMMDD/scoreboard.jsonl
    espn/YYYYMMDD/plays.jsonl
    espn/YYYYMMDD/header.jsonl
    pm_sports/YYYY-MM-DD.jsonl
    settlement/YYYY-MM-DD.jsonl
    health/YYYY-MM-DD.jsonl
```

Every JSONL row has `recv_ms`, a session-local `event_seq`, and `session_id`.
Receipt times are taken immediately after reading the HTTP body or websocket
frame, before parsing and disk writes. Each socket reader passes frames and gap
markers through a bounded FIFO to a separate writer thread, so disk writes and
JSON processing do not delay reading the next frame. Queue saturation or writer
failure stops recording; accepted frames drain on graceful shutdown. Market
routes are refreshed at most once per second, rather than on every frame.
All events within one websocket frame
share a receipt time; `frame_index` and `event_seq` preserve their delivery order.
Local UTC time can jump if the host clock steps; the sequence remains useful
for delivery order. Keep the chosen host synchronized with NTP.

Gamma discovery scans three offset-paged weekly windows (previous week through
the next two weeks), hourly and again at kickoff minus 35 minutes. Games already
discovered stay in memory until the process ends, including unresolved games
outside the moving discovery window. After a restart, discovery rebuilds this
catalog from Gamma; historical stream state is not recovered. A resolution
delayed beyond the discovery lookback needs a separate historical Gamma pull
in the offline analysis.

The market connection subscribes to both tokens at kickoff minus 30 minutes.
It adds/removes subscriptions without interrupting other games and resubscribes
all active tokens after reconnect. A game remains subscribed until both a Gamma
close and a final ESPN summary have been observed and the last new play's
receipt time plus 30 minutes has passed. Games already closed at first discovery
are collected for settlement but do not start historical book subscriptions.
Once `closed` is observed it remains true, even if a later Gamma endpoint disagrees.
Every game has a hard cutoff at kickoff plus eight hours, including unmapped
games or games lacking final status. A `game_timeout` health row records that
cutoff, mapping/close state and last observed ESPN state; later scoreboard and
summary polls exclude that game. Settlement retries continue independently.

Book rows retain the entire parsed wire message, including all depth levels,
trade sides, identical trades, and any cross-token price-change batch. A batch
covering multiple games is copied intact to each affected game's file; consumers
must filter by the game's token IDs. Tokens and team ordering come from the game
rows as known at that receive time. There is no live ladder cache or depth window.

Gap rows in each book file mark connect/reconnect, subscribe, unsubscribe, and
disconnect boundaries, with affected token IDs. Invalidate only those tokens;
deltas cannot restore a token's book until its next wire `book` snapshot. Do not
carry ladders across sessions. `replay.Ladders` is an offline inspection helper
that applies these rules, absolute size updates, deletes, and tick changes and
returns best-ask capacity. The live service never imports it.

ESPN scoreboards are retained every minute on the study's US kickoff date
(`kickoff UTC - 6 hours`). Exact team matching uses the study's alias table;
ambiguous joins remain unmapped. A verified join survives a partial scoreboard.
Mapped games receive independent summary requests every five seconds from
kickoff minus five minutes, regardless of the scoreboard's state. Concurrent
requests prevent slow games from serializing
the poll cadence. Each play row includes its original `first_recv_ms`, flattened
study fields and `raw_play`. Reappearing plays are `changed`, retaining the
original sighting. Before any play has been seen, a missing/empty `plays` list during
pregame or the opening period is normal; headers are still recorded. After a
play has been seen, or later in the game, a missing/malformed `plays` list produces a poll
error rather than deleting the entire known play history. Header rows also preserve `raw_header`.
HTTP protocol errors (including truncated reads) and malformed scoreboard
entries are logged and retried without stopping other feeds. Valid entries in
the same scoreboard remain usable. Recording failures remain fatal.

Settlement polling starts at the first 06:00 UTC after kickoff and retries
unresolved games or errors hourly. Closure alone does not prove resolution;
`umaResolutionStatus=resolved` and two outcome prices are required. Raw responses
are retained, including tie payouts. The public sports websocket records NBA
updates for all games, even before discovery, and answers each `ping` with `pong`.

Health files contain HTTP request timings and errors, socket lifecycle events,
hourly NTP samples, and daily cumulative session counters. Clock diagnostics use
`chronyc`, `ntpq`, or macOS `sntp` in read-only mode; offset means system time minus
NTP time. Missing or unsynchronized diagnostics are explicitly recorded as
unavailable. Counters include ESPN polls/errors, fills, depth/metadata messages,
sports messages, socket reconnects and current subscribed/unmapped games.
The nested counts contain only recorded NBA event kinds, without soccer engine
counters or an injected zero connection count.
Files flush every second. SIGINT/SIGTERM stop workers and fsync output; any
recording failure stops the process so it cannot continue collecting silently.

For an existing mounted SSD directory, add:

```sh
--backup-to /Volumes/<SSD>/pm_nba_recordings
```

This takes a flushed snapshot on startup and every seven days, retrying failed
copies hourly. Only append-only file lengths are captured under the writer lock;
large copies run outside the lock. Each output file is atomically replaced and
`backup.json` records the copied byte boundaries. An interrupted backup retains
the previous manifest; use its file list and byte limits until a backup completes.
Without a supplied destination, no SSD sync is scheduled.

Local verification:

```sh
$PY -m pytest tests/test_nba_dryrun.py -q
```

The tests use fake HTTP/websocket feeds and temporary local output. NBA websocket
fill parity versus `/trades`, live ESPN corrections versus after-game data,
latency variants, and the report 07 statistical evaluation still require actual
recorded games and the separate analysis project. No parity claim is assumed.
