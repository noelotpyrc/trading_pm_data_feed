# FotMob shadow collector

Deployed continuously on Genie and VPS on 2026-09-30, with recording-only
fallback capture enabled. SofaScore remains the sole source for original fire
and filter results. FotMob can open additional candidate-based book windows.

Current primary run:
`/root/trading_pm_data_feed/data/pm_soccer_dryrun/vps-genie-20260930-fotmob`.
Shadow recordings:
`/root/trading_pm_data_feed/data/pm_soccer_fotmob_shadow/vps-genie-20260930`.
The existing resource timer now records memory/CPU/disk samples for both VPS
services into their respective data directories. Previous recordings remain
intact. Deployment backup and rollback script:
`/root/pm-soccer-fotmob-deploy-20260930/`.

Runtime code is commit `de46990` (including the SSH host-alias correction).
Genie uses system LaunchDaemons and does not require a desktop login. The
FotMob tunnel reuses the verified `pm-soccer-vps` host-key pin with strict checking.

## Isolation and connection

```text
VPS primary recorder ── JSONL catalog/scores (read-only) ──► shadow receiver
                                                               │ scope
                                                               ▼
                        separate SSH forward, localhost:18766 ↔ Genie FotMob worker
                                                               │ observations
                                                               ▼
                                   separate FotMob JSONL + compressed payloads
```

The primary uses its existing SofaScore port 18765 and signal rules. Deployment
restarted it into the new run directory and enabled the optional recording-only
integration below. The option defaults off for other runs. The shadow receiver never calls the engine or publishes
scores into its Store. It owns its own Store with no engine callback and no
fire/book-window outputs. There is no recovery journal.

The receiver incrementally tails only the current primary session, derives the
same collection window, and sends compact match identities to the worker. An
unmapped SofaScore match is still eligible for FotMob. A primary-session change
clears cached scope. Incomplete JSONL tails are retried, not discarded. Scope
pauses if primary catalog/poll files have not changed for 30 seconds. Thus a dead
primary cannot leave obsolete subscriptions running indefinitely.

Provider errors, reconnects and payload-size failures are recorded in shadow
health files. They do not change primary health, match expiry, fires or filters.
With the optional integration enabled, fresh observations may open additional
depth windows. The receiver is loopback-only; its CLI rejects the primary port
and any overlap between its output directory and primary recordings. The
systemd template additionally makes primary data read-only, limits memory to
128 MiB and CPU to 10%, and runs at reduced priority.

## Data and identity contract

- Independent FotMob IDs, exact team-pair matching, competition check and a
  three-hour kickoff tolerance. Close competing candidates stay ambiguous.
- `fotmob_team_aliases.csv` contains 333 PM aliases matched by exact normalized
  PM/SofaScore canonical names against current men's FotMob competition
  participants. No fuzzy matches or club-word removal. Evidence URLs are in
  each row; `fotmob_team_aliases_audit.csv` accounts for all 865 existing aliases.
  The other 532 remain unverified, including historical teams absent from the
  queried competitions. They are explicitly skipped, not silently guessed.
  Extending coverage means adding evidence-backed IDs on both shadow hosts;
  the receiver/worker handshake checks the canonical mapping digest.
- Match lists use `/api/data/matches` and UTC dates intersecting the permitted
  kickoff window. Grouped friendlies are checked against parent competition 114.
  Only mapped PM-tracked match objects are retained; global lists are discarded.
- Details use `/api/data/matchDetails`. Retained data includes full `general`,
  `header`, `ongoing`, `hasPendingVAR`, `content.matchFacts` and
  `content.liveticker`. All incident types and unknown fields inside these
  sections are preserved. Other sections such as stats, shot maps and lineups
  can be fetched offline; their omitted keys are named in each snapshot.
- Every observation retains its own times and cache headers. Repeated payloads
  reference one SHA-256-addressed gzip file per session. References are written
  only after the payload is durably written. Back up the entire session,
  including `payloads/`; observation JSONL alone is not a complete backup.
- Source request/receipt, worker send and receiver receipt/application times
  are separate. Fields named `vps_*` describe the receiver role; a local test
  records its actual hostname in the session manifest. Clock offsets are not
  corrected. Retain VAR reversals and score decreases; do not choose a provider
  merely because it reports the higher score.
- Provider clocks/status remain verbatim. Ambiguous display-clock strings are
  not interpreted as UTC or converted to SofaScore period starts. FotMob rows
  explicitly say `clock_eligible_for_engine=false`.
- Comparison rows pair a FotMob observation with the primary score visible to
  the receiver then, including the primary snapshot age and stale flag. These
  diagnose disagreements; they neither establish ground truth nor amend fires.

The worker requests a cycle every ten seconds, with a global minimum 0.5-second
spacing between HTTP requests and independent 403/429 backoff. It emits
heartbeats while HTTP work runs. There is no HTTP traffic when scope is empty.
Cycles can take longer than ten seconds with many matches or slow requests;
observed timestamps expose the actual cadence. Responses advertised ten-second
caching, so this is not a promise of ten-second goal-reporting latency.

## Plugging it in without restarting production

The separate services can be installed without restarting the primary. These
steps were completed on 2026-09-30; activating the recording gate additionally
requires the controlled primary restart described below.

1. Copy the package to **separate application directories**:
   `/root/pm_soccer_fotmob_shadow/` on VPS and
   `/Users/noel/services/pm-soccer-fotmob-worker/` on Genie. Reuse the existing
   Python environments; no new dependency installation is required. Both copies
   must include the same FotMob alias file. Do not overwrite the live package.
2. Create the separate VPS output parent
   `/root/trading_pm_data_feed/data/pm_soccer_fotmob_shadow/` and Genie `logs/`.
   The included `fotmob-shadow.service` points at the current primary run
   `vps-genie-20260930-fotmob`. If production later moves run directories, update this
   sidecar argument; it does not silently search for a different primary run.
3. Extend the existing dedicated SSH relay user's forwarding permissions to
   allow **127.0.0.1:18766 as well as 18765**, in both sshd and the authorized-key
   restriction if present. Preserve the existing key and all other restrictions.
   Validate sshd configuration before reload. Use a separate SSH connection and
   `fotmob-ssh-config.example`; never restart or replace the SofaScore tunnel.
4. First run receiver and worker with `--duration 120` and inspect observations,
   mappings, comparisons and health. Start the receiver from its separate app
   directory using the existing VPS Python environment:

   ```sh
   /root/trading_pm_data_feed/.venv-soccer/bin/python -m pm_soccer_dryrun.fotmob_shadow receiver \
     --primary-dir /root/trading_pm_data_feed/data/pm_soccer_dryrun/vps-genie-20260930-fotmob \
     --data-dir /root/trading_pm_data_feed/data/pm_soccer_fotmob_shadow/vps-genie-20260930 \
     --duration 120
   ```

   On Genie, start the new port-18766 SSH forward, then from the separate app
   directory:

   ```sh
   /Users/noel/services/pm-soccer-score-worker/.venv/bin/python -m pm_soccer_dryrun.fotmob_shadow worker --duration 120
   ```

5. After verification, install only `fotmob-shadow.service` and the two new
   `com.noel.pm-fotmob-shadow-*` system LaunchDaemons. Their templates live in
   `launchdaemons/`. These run as Noel without a desktop login, like the existing
   worker. Starting/stopping these three components never restarts the primary
   recorder. All install paths in the templates must exist beforehand.

Rollback consists of stopping only the new FotMob worker, tunnel and receiver.
Keep its recordings for analysis. The existing SofaScore processes continue.

## Validation evidence

`tests/test_fotmob_shadow.py` covers strict/swapped/ambiguous mappings, competition
and UTC date boundaries, missing scores/clocks, HT/FT/postponed/cancelled states,
full incident/VAR retention, score reversals, content deduplication, scope expiry,
partial JSONL tails, oversize errors, output-path/port separation and a separate
relay round trip that leaves primary input bytes unchanged.

The bounded Genie→SSH→local receiver test on 2026-09-30 recorded 12 observations
across three cycles for Seychelles–Sri Lanka and Botswana–Mozambique. Both list
and detail payloads arrived, with six unique compressed payloads and roughly
53 KB total test output. The latter fixture was available from FotMob despite
the independent SofaScore mapping gap. Test input bytes and production process
IDs were unchanged. This short test does not establish sustained availability
or score-update latency. Artifacts are in `data/fotmob-build-20260930/`.

## Optional recording-only fallback gate

Enabled in the current VPS run. To enable on another primary run:

```sh
python -m pm_soccer_dryrun.service live \
  --data-dir data/pm_soccer_dryrun/NEW_RUN \
  --run-id NEW_RUN \
  --fotmob-shadow-dir data/pm_soccer_fotmob_shadow/SHADOW_RUN
```

Keep the deployment's existing SofaScore relay and other command options.
Point the shadow receiver's `--primary-dir` at that same NEW_RUN directory.
Changing the primary code/recording contract requires a fresh run directory and
one primary restart. The shadow receiver and Genie worker must also be running;
adding the flag alone does not fetch FotMob. Once enabled, later SofaScore
outages require no operator switch. Rollback is disabling the option in another
fresh run and repointing the shadow receiver, leaving old recordings intact.

The primary tails complete shadow observation rows every 250 ms in a background
thread. The market-data path reads only its bounded in-memory cache. No scores
are injected into the engine and no original candidate/fire/filter is rewritten.
The original qualifying-fire book capture remains active as before.

For an additional window, all of these must hold:

- The engine emits a price-movement **candidate** on a home/away win book moving
  up. Waiting for a fire would miss candidates whose confirmation needs a score.
- SofaScore poll health is unavailable, its score is missing/stale, or its score
  observation is older than 30 seconds. A healthy fresh score disables fallback.
- FotMob has a verified mapping to the current primary session/team IDs/kickoff,
  reports in-progress status, and shows that candidate's team leading by one.
- The latest eligible observation was received **and imported** before candidate
  receipt. VPS receipt/application/import ages must be at most 30 seconds;
  source-request age plus reported cache Age must also be at most 30 seconds.
  Cross-machine times are uncorrected; future times fail closed. This bounds
  observed transport/cache age, not the provider's actual goal-reporting delay.
- A plain reported minute below 30 excludes capture. An unknown minute (including
  HT text) allows conservative capture for offline evaluation. No clock is
  extrapolated; `45+2` remains first-half added time, represented by base minute 45.

The existing N-minute window opens at candidate emission, with the cached book
snapshot then available; another qualifying candidate extends it. Open/extend
markers include `capture_reason=fotmob_fallback_candidate`, `passing_fire=false`,
candidate ID/times, failure reason, minute hint, complete normalized FotMob
observation and raw-payload reference. They have **no** `fire_recv_ms`; these are
recording decisions, not extra passing fires. Original fire counts stay original.
References identify the separate shadow session: back it up with the primary.

A later score correction/FT observation supersedes prior values; it cannot revive
an older favorable score. Missing, expired or malformed shadow input disables
additional capture and logs health without stopping primary collection. No
queued candidate replay or retrospective opening occurs. Extra windows can
increase outage-time storage; the policy/window settings are in the run manifest.

`tests/test_fotmob_capture.py` covers outage/recovery gating, stale/cache/future
observations, session/identity checks, score corrections, clocks, partial rows,
opening/expiry and a primary service + background reader integration with no
score injection or fabricated fires.

Deployment validation: 190 regression tests passed. Genie delivered list and
detail observations for all three currently tracked matches. Both VPS services
were active with no automatic restarts after activation; primary SofaScore polls
returned HTTP 200. An isolated temporary recording test used a fresh live
Botswana–Mozambique observation with a synthetic candidate: it opened a five-minute
window and rejected capture when SofaScore was marked healthy. Synthetic data
never entered production recordings. Evidence is under the repository's ignored
`data/fotmob-deploy-20260930/` directory.

Next validation after deployment: several simultaneous matches, goal/VAR update
delay comparisons, source outages/reconnects, alias gaps, and actual CPU/memory/
disk growth. Actual FotMob fire-filter promotion still requires a separately
versioned selection/clock policy; the optional gate changes recording only.
