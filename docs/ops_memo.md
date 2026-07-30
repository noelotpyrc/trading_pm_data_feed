# Ops Memo — Running Pipelines

Snapshot of what runs on `vps-madrid` and where the code lives. Keep in sync when adding/removing a stream.

Last verified: 2026-07-30 (VPS now tracks `origin/main`; `signal` stopped — see changelog)

**Changelog**
- **2026-07-30** — **Deploy model changed.** The VPS no longer vendors subtrees and commits locally; it now **reconciles onto `origin/main`** (`git fetch origin && git reset --hard origin/main`). The 6 old VPS-only deploy commits were dropped after verifying `pm_signal_sim`/`pm_shock_signal` were already byte-identical to `origin/main` — nothing lost. Pre-reset HEAD `4d11d7a` is tagged **`deploy-history-20260729`** on the VPS if it's ever needed. VPS is now 0/0 with `origin/main`. See [Deploys](#deploys). Also **stopped the `signal` session** (SIGINT 02:30:06 UTC) and dropped it from the watchdog's `EXPECTED_SESSIONS`; launch block kept below for restart.
- **2026-07-29 (later)** — Redeployed `pm_signal_sim` (live-test v2, origin `d524d60`, VPS deploy `4d11d7a`): pre-registered S1 `fade` / S2 `z30_gate` evaluated at fire+3s + `ask_d5` fill log; book rows now carry top-of-book **sizes**, 500ms sampling in [fire, fire+5s], 120s pre-fire book dump; new tables `epoch_strike` / `signal_evals` / `fill_log`; Discord sweep now only posts windows with a passing S1/S2 fire (see `pm_signal_sim/LIVE_TEST_SPEC.md`). tmux session relaunched; `pm_signal_sim` re-added to the watchdog `EXPECTED_SESSIONS`.
- **2026-07-29** — **41h outage** (2026-07-28 01:24 → 2026-07-29 18:36 UTC). Unattended-upgrades rebooted the box (kernel `6.8.0-101` → `6.8.0-136`); the reboot killed the tmux server *and* left Tailscale MagicDNS owning `/etc/resolv.conf` in direct-takeover mode with no working upstream, so every public hostname failed to resolve. All collection stopped; cron accumulators looped on `Temporary failure in name resolution`. Fix: `tailscale set --accept-dns=false` (durable) + restored `/etc/resolv.conf` from `/etc/resolv.pre-tailscale-backup.conf` (Vultr `108.61.10.10` + Quad9 `9.9.9.9`). Relaunched `signal`, `btc_depth`, `liq_collector`, `pm_collector`; `pm_signal_sim` left down (being reworked). Both 1m OHLCV DBs **self-backfilled** the full gap on the first post-fix cron run; the ~41h of streaming data (depth / liquidations / PM book) is **permanently lost**. Added an off-box watchdog — see [Monitoring](#monitoring).
- **2026-06-19** — Stopped `pm_shock` (tmux killed; DB `data/pm_shock_signal.sqlite` + launch block kept for restart) and deployed `pm_signal_sim` (`pm_signal_sim.scripts.run_signal_sim`) in its place, live. Multi-def 15updown collector + honest raw capture (reports 22–24). Reuses the **`#pm-trading-signals`** webhook: new `DISCORD_WEBHOOK_URL_PM_SIGNAL_SIM` = the `…_PM_SHOCK` value (which `pm_shock` vacated). `.env` backed up to `.env.bak.sigsim.*`. Code vendored from origin `4ac974b` (subtree checkout).
- **2026-06-14** — Collection cadence **5s → 1s** for finer archives: `btc_depth` (`--sample-interval 1`; WS already at 500ms) and `pm_collector` (`--poll-interval 1`). Both ~5× JSONL volume — watch disk/backups. `pm_collector` 1s confirmed safe: `/book` REST limit is 1,500 req/10s (150/s); 1s poll = 2 req/s ≈ 1.3% of limit, and over-limit is throttled not 429 ([docs](https://docs.polymarket.com/api-reference/rate-limits)). Live 30s burst test: 60/60 OK, 0 throttle, ~0.1s latency.
- **2026-06-13** — Deployed `pm_shock` (PM 15updown shock-continuation **sim**; `pm_shock_signal.scripts.run_shock_signal`) as a new tmux session, live. Sim-only (no real orders) — forward, out-of-sample, spread-aware validation of the `btc_depth_15updown` backtest edge. Reuses the `#pm-trading-signals` channel via a new `DISCORD_WEBHOOK_URL_PM_SHOCK` key (= `DISCORD_WEBHOOK_URL_SIGNAL_V3` value). `.env` backed up to `/root/trading_pm_data_feed/.env.bak.pmshock.*`. Code vendored into the prod working tree from origin `a1359a9` (subtree checkout, prod deploy commit `4035154`).
- **2026-05-29** — Stopped 4 streams: `pm_dual` tmux, `signal-v3-contrarian` tmux, `signal-v3-dir` tmux, and the v1 `build_daily_artifact` cron (commented out, crontab backed up to `/root/crontab.bak.20260529-153845`). The v3 artifact cron (`build_daily_artifact_v3`) stays running — it feeds a downstream **live trading system**, not the now-stopped V3 signal streams.
- **2026-04-29** — V2 keyset migration restart.

## Data streams (code)

### Collection
| Module | Purpose | Output |
|---|---|---|
| `cex_data_feed.scripts.accumulate_1m` | 1m BTCUSDT perp OHLCV (Binance) | `data/btcusdt_perp_1m.sqlite` |
| `cex_data_feed.scripts.repair_gaps_1m` | Daily gap scan/repair | same DB |
| `cex_data_feed.scripts.coinbase_accumulate_1m` | 1m BTC-USD spot OHLCV (Coinbase) | `data/btcusd_coinbase_1m.sqlite` |
| `cex_data_feed.scripts.collect_btc_depth` | Orderbook depth logger (depth20@500ms WS, **1s** sampling since 2026-06-14) | `data/btc_depth/` |
| `cex_data_feed.scripts.collect_liquidations` | Liquidation stream | `data/liquidations/` |
| `pm_btc15updown_data.collect_pm_btcupdown` | Polymarket single-market (CLOB `/book`, **1s** poll since 2026-06-14) | `data/pm_btcupdown/` |
| `pm_btc15updown_data.collect_pm_dual` | Polymarket dual-market | `data/pm_dual/` ⏹ **stopped 2026-05-29** |

### Artifacts / signals
| Module | Purpose | Output |
|---|---|---|
| `pm_btc15updown_artifact.scripts.build_daily_artifact` | v1 vol signal artifact | `data/artifacts/` ⏹ **stopped 2026-05-29** (cron commented out) |
| `pm_btc15updown_artifact.scripts.build_daily_artifact_v3` | v3 per-TTL MAR artifact | `data/artifacts_v3/` — **consumed by downstream live trading system; keep running** |
| `pm_btc15updown_data.signal_stream_v3` | Live V3 contrarian stream — triggers TTL≤2 prob>0.90/<0.10, 10 polls at 3s, sends only on real entry (delta>0.05), near-close both tokens | in-process ⏹ **stopped 2026-05-29** |
| `pm_btc15updown_data.signal_stream_v3_directional` | Live V3 directional stream — 4 specific TTL/prob rules, 8 polls at 3s, near-close both tokens, JSONL log | in-process + `logs/signal_v3_directional.jsonl` ⏹ **stopped 2026-05-29** |
| `btcusdt_perp_signal.scripts.run_signal_engine` | Signal engine + alerts | `data/signal_engine.log` |
| `pm_shock_signal.scripts.run_shock_signal` | PM shock-continuation sim (d5/d10 back-ratio) | `data/pm_shock_signal.sqlite` ⏹ **stopped 2026-06-19** (replaced by `pm_signal_sim`; launch block kept) |
| `pm_shock_signal.scripts.resolve_outcomes` | Backfill `resolved_outcome` on shock sim trades via Gamma `outcomePrices` | `data/pm_shock_signal.sqlite` — manual pass (for the retained shock DB) |
| `pm_signal_sim.scripts.run_signal_sim` | Live multi-def 15updown collector + sim — 4 k=1.5 configs (trailmean w60, consistent {2,5,10,20}, asym (2,5,40)&(5,5,10)) on p≥0.5; per fire persists a bounded multi-source **raw slice** (PM trades/book, BTC depth20/tick) + window resolution; batched merged-per-window Discord post-resolution | `data/pm_signal_sim.sqlite`, `data/pm_signal_sim.log` — **active (deployed 2026-06-19)** |

## Running on VPS

### Cron (`crontab -l`)
```
*/5    * * * *   accumulate_1m            → data/accumulate_1m.log
2-57/5 * * * *   coinbase_accumulate_1m   → data/coinbase_accumulate_1m.log
0      0 * * *   repair_gaps_1m           → data/repair_gaps_1m.log
# 1    0 * * *   build_daily_artifact     → data/build_artifact.log   ⏹ STOPPED 2026-05-29 (line commented out)
5      0 * * *   build_daily_artifact_v3  → data/build_artifact_v3.log  (feeds downstream live trading system — keep)
```
Coinbase runs offset (`2-57/5`) so it doesn't collide with the Binance accumulator on the same minute. The v1 `build_daily_artifact` line is commented out in the live crontab as of 2026-05-29; crontab backed up to `/root/crontab.bak.20260529-153845`. To re-enable, uncomment that line via `crontab -e`.

### Long-running (tmux — one session per process)
List: `ssh vps-madrid tmux ls`  ·  Attach: `ssh vps-madrid -t tmux attach -t <session>`

**Expected sessions as of 2026-07-30:** `btc_depth`, `liq_collector`, `pm_collector`, `pm_signal_sim` (4 active). Stopped: `signal`, `pm_shock`, `pm_dual`, `signal-v3-contrarian`, `signal-v3-dir` (launch commands kept below for restart).

This list must match `EXPECTED_SESSIONS` in `utils/vps_watchdog.sh` — update both together.

Each block below is the full launch command; copy-paste it directly into the VPS shell to (re)create the session detached. All commands assume the project venv at `/root/trading_pm_data_feed/.venv`.

Webhook URLs live in VPS `.env` (loaded by `alert.send_discord`). Missing or empty env var → send silently skipped. All channels are in Discord server `1465846881836863796`:

| Env var | Channel | Channel ID | Used by | Discord status |
|---|---|---|---|---|
| `DISCORD_WEBHOOK_URL` | `#trading-signals` | `1492314707372150814` | `signal`, `liq_collector` | ✅ active |
| `DISCORD_WEBHOOK_URL_PM` | `#pm-price-alert` | `1492688541405413396` | `pm_collector` | 🔇 silenced 2026-05-07 |
| `DISCORD_WEBHOOK_URL_PM_DUAL` | `#pm-dual-price` | `1493441980968075488` | `pm_dual` | ⏹ session stopped 2026-05-29 (was 🔇 silenced 2026-05-07) |
| `DISCORD_WEBHOOK_URL_SIGNAL_V3` | `#pm-trading-signals` | `1494415390875320330` | `signal-v3-contrarian`, `signal-v3-dir` | ⏹ both sessions stopped 2026-05-29 |
| `DISCORD_WEBHOOK_URL_PM_SHOCK` | `#pm-trading-signals` | `1494415390875320330` | `pm_shock` | ⏹ session stopped 2026-06-19 (key kept; value still set) |
| `DISCORD_WEBHOOK_URL_PM_SIGNAL_SIM` | `#pm-trading-signals` | `1494415390875320330` | `pm_signal_sim` | ✅ active 2026-06-19 (= the `…_PM_SHOCK` value; same channel, freed by stopping pm_shock) |

**Webhook silencing decision (2026-05-07):** `DISCORD_WEBHOOK_URL_PM` and `DISCORD_WEBHOOK_URL_PM_DUAL` were emptied in `.env` to silence the per-poll `pm_collector` price chatter and the per-arb `pm_dual` triggers — too noisy for the value they were providing. The underlying sessions keep running and persisting JSONL to disk (`data/pm_btcupdown/*.jsonl`, `data/pm_dual/*.jsonl`) as before; only the `send_discord` calls no-op. To re-enable later, restore the URL line in `.env` (a timestamped backup is on the VPS) and restart the affected session(s). `DISCORD_WEBHOOK_URL` (signal engine + liquidation alerts) and `DISCORD_WEBHOOK_URL_SIGNAL_V3` (V3 streams) remain active.

#### `signal` — signal engine + alerts (since Apr 13) — ⏹ STOPPED 2026-07-30
Webhook: `DISCORD_WEBHOOK_URL`
```bash
tmux new -d -s signal "cd /root/trading_pm_data_feed && .venv/bin/python -m btcusdt_perp_signal.scripts.run_signal_engine --db data/btcusdt_perp_1m.sqlite"
```

#### `btc_depth` — Binance orderbook depth logger (since Apr 13)
Webhook: —  ·  Cadence: **1s** since 2026-06-14 (`--sample-interval 1`; was 5s). Underlying WS is `depth20@500ms`.
```bash
tmux new -d -s btc_depth "cd /root/trading_pm_data_feed && .venv/bin/python -m cex_data_feed.scripts.collect_btc_depth --log-dir data/btc_depth --sample-interval 1"
```

#### `liq_collector` — Binance liquidation stream (since Apr 17)
Webhook: `DISCORD_WEBHOOK_URL` (default)
```bash
tmux new -d -s liq_collector "cd /root/trading_pm_data_feed && .venv/bin/python -m cex_data_feed.scripts.collect_liquidations --log-dir data/liquidations"
```

#### `pm_collector` — Polymarket single-market BTC up/down (since Apr 29, V2 keyset)
Webhook: `DISCORD_WEBHOOK_URL_PM` (silenced)  ·  Cadence: **1s** since 2026-06-14 (`--poll-interval 1`; was 5s). REST `/book` 2 req/poll ≈ 2 req/s, ~1.3% of the 1,500 req/10s limit.
```bash
tmux new -d -s pm_collector "cd /root/trading_pm_data_feed && .venv/bin/python -m pm_btc15updown_data.collect_pm_btcupdown --log-dir data/pm_btcupdown --poll-interval 1"
```

#### `pm_signal_sim` — PM 15updown multi-def collector + sim (since Jun 19) — ✅ ACTIVE
Webhook: `DISCORD_WEBHOOK_URL_PM_SIGNAL_SIM` (→ `#pm-trading-signals`; fallback `…_PM_SHOCK`). Sim-only. Logs → `data/pm_signal_sim.log`; DB → `data/pm_signal_sim.sqlite` (captures/fires/raw_pm_*/raw_btc_*/resolution). Discord is **batched, merged-per-window, post-resolution** (not per-fire); a sweep runs each window roll + at startup.
```bash
tmux new -d -s pm_signal_sim "cd /root/trading_pm_data_feed && .venv/bin/python -m pm_signal_sim.scripts.run_signal_sim"
```
Flags: `--dry-run` (log alerts, don't post; non-destructive — windows stay unalerted), `--relax` (dev-only k≈1.01/no p-floor to force fires for the §8 capture test — **not** real data), `--db PATH`. Configs live in `pm_signal_sim/config.py`. Raw slices only persist for windows that fire (k=1.5 on p≥0.5 → rare; watch disk once it has run a while).

#### `pm_shock` — PM 15updown shock-continuation sim (since Jun 13) — ⏹ STOPPED 2026-06-19
Replaced by `pm_signal_sim` on the same channel. DB `data/pm_shock_signal.sqlite` retained. Launch command kept for restart:
```bash
tmux new -d -s pm_shock "cd /root/trading_pm_data_feed && .venv/bin/python -m pm_shock_signal.scripts.run_shock_signal"
```
Flags: `--dry-run`, `--relax`, `--db PATH`. Operating points in `pm_shock_signal/config.py`. Backfill: `.venv/bin/python -m pm_shock_signal.scripts.resolve_outcomes`.

#### `pm_dual` — Polymarket dual-market arb tracker (since Apr 29, V2 keyset) — ⏹ STOPPED 2026-05-29
Webhook: `DISCORD_WEBHOOK_URL_PM_DUAL`. Launch command kept for restart:
```bash
tmux new -d -s pm_dual "cd /root/trading_pm_data_feed && .venv/bin/python -m pm_btc15updown_data.collect_pm_dual --log-dir data/pm_dual"
```

#### `signal-v3-contrarian` — V3 contrarian live stream (since Apr 29, V2 keyset) — ⏹ STOPPED 2026-05-29
Webhook: `DISCORD_WEBHOOK_URL_SIGNAL_V3`. Launch command kept for restart:
```bash
tmux new -d -s signal-v3-contrarian "cd /root/trading_pm_data_feed && .venv/bin/python -u -m pm_btc15updown_data.signal_stream_v3 --db data/btcusdt_perp_1m.sqlite --artifact-dir data/artifacts_v3"
```

#### `signal-v3-dir` — V3 directional live stream (since Apr 29, V2 keyset) — ⏹ STOPPED 2026-05-29
Webhook: `DISCORD_WEBHOOK_URL_SIGNAL_V3`. Launch command kept for restart:
```bash
tmux new -d -s signal-v3-dir "cd /root/trading_pm_data_feed && .venv/bin/python -u -m pm_btc15updown_data.signal_stream_v3_directional --db data/btcusdt_perp_1m.sqlite --artifact-dir data/artifacts_v3"
```

To restart any session: `tmux kill-session -t <name>` then re-run its block above. To stop cleanly while attached: `Ctrl-C` in the window, then `exit`.

## External data sources

Canonical list of every external endpoint we depend on, grouped by vendor. When something starts silently failing — process alive, no data — check here first to see if a vendor changed something.

### Binance USDⓈ-M Futures

On **2026-04-23** the legacy unrouted `wss://fstream.binance.com/ws/<stream>` URL stopped pushing `/market` and `/private` streams (only `/public` still flows on legacy paths). This silently broke `signal`, `signal-v3-contrarian`, `signal-v3-dir`, and `liq_collector` until 2026-05-02. See [Important WebSocket Change Notice](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice).

| Endpoint | Class | Used by | Last verified |
|---|---|---|---|
| `wss://fstream.binance.com/market/ws/btcusdt@kline_1m` | /market | `signal`, `signal-v3-contrarian`, `signal-v3-dir` | 2026-05-02 |
| `wss://fstream.binance.com/market/ws/btcusdt@forceOrder` | /market | `liq_collector` | 2026-05-02 |
| `wss://fstream.binance.com/ws/btcusdt@depth20@500ms` | /public (legacy URL still serves) | `btc_depth`, `pm_collector` DepthFeed, `pm_signal_sim` (`BtcDepth20Feed`) | 2026-06-19 — migrate to `/public/ws/...` as cleanup |
| `wss://fstream.binance.com/ws/btcusdt@bookTicker` | /public (legacy URL still serves) | `pm_signal_sim` (`BtcMidFeed` → mid); `pm_shock` (stopped) | 2026-06-19 — same legacy `/ws/` family as depth20; verified pushing; migrate to `/public/ws/...` as cleanup |
| `https://fapi.binance.com/fapi/v1/klines` | REST | `accumulate_1m` cron, `repair_gaps_1m` cron, `pm_collector` (`fetch_strike`), `pm_signal_sim` (`fetch_strike`), `pm_dual`, `signal-v3-*` | 2026-05-02 |

Watch list: [Binance Derivatives Change Log](https://developers.binance.com/docs/derivatives/change-log)

### Polymarket

V2 cutover **2026-04-28**; legacy offset-paginated `/events` deprecated **2026-05-01** in favor of cursor-based `/events/keyset`. We are on V2 + keyset (commits `d8c45fe`, `64794d9`).

| Endpoint | Used by | Last verified |
|---|---|---|
| `https://gamma-api.polymarket.com/events/keyset` | `pm_collector`, `pm_dual`, `signal-v3-*` (`resolve_market`), `pm_signal_sim` (`resolve_market`), `pm_metadata` fetcher | 2026-06-19 |
| `https://clob.polymarket.com/book` | `pm_collector`, `pm_dual`, `signal-v3-*` (`fetch_prices`) | 2026-04-28 (post V2) |
| `wss://ws-subscriptions-clob.polymarket.com/ws/market` | `pm_collector` (`TradeFeed`), `pm_signal_sim` (`PmTokenFeed` subclass: top-of-book + size-bearing trade buffer) | 2026-06-19 |

Watch list: [Polymarket Changelog](https://docs.polymarket.com/changelog)

### Coinbase Exchange

| Endpoint | Used by | Last verified |
|---|---|---|
| `https://api.exchange.coinbase.com/products/BTC-USD/candles` | `coinbase_accumulate_1m` cron | 2026-04-29 |

Watch list: [Coinbase Exchange API changelog](https://docs.cdp.coinbase.com/exchange/docs/changelog)

### Per-session source map

| Session / cron | Sources |
|---|---|
| `signal` tmux | Binance `kline_1m` WSS · SQLite warmup |
| `btc_depth` tmux | Binance `depth20@500ms` WSS |
| `liq_collector` tmux | Binance `forceOrder` WSS |
| `pm_collector` tmux | Polymarket Gamma + CLOB + WSS market · Binance `depth20@500ms` WSS · Binance FAPI `/klines` |
| `pm_signal_sim` tmux | Binance `bookTicker` + `depth20@500ms` WSS · Polymarket Gamma `/events/keyset` + WSS `/market` · Binance FAPI `/klines` (strike) |
| `pm_shock` tmux ⏹ stopped 2026-06-19 | Binance `bookTicker` WSS · Polymarket Gamma `/events/keyset` + WSS `/market` · Binance FAPI `/klines` (strike) |
| `pm_dual` tmux ⏹ stopped 2026-05-29 | Polymarket Gamma + CLOB · Binance FAPI `/klines` |
| `signal-v3-contrarian`, `signal-v3-dir` tmux ⏹ stopped 2026-05-29 | Binance `kline_1m` WSS · Polymarket Gamma + CLOB · Binance FAPI `/klines` · SQLite warmup · daily v3 artifact |
| `accumulate_1m` cron | Binance FAPI `/klines` |
| `coinbase_accumulate_1m` cron | Coinbase Exchange `/candles` |
| `repair_gaps_1m` cron | Binance FAPI `/klines` |
| `build_daily_artifact_v3` cron | SQLite only (no external) — output feeds downstream live trading system |
| `build_daily_artifact` cron ⏹ stopped 2026-05-29 | SQLite only (no external) |
| `pm_metadata` fetcher (manual) | Polymarket Gamma `/events/keyset` |

### Review reminder

Run a vendor-changelog sweep monthly as part of [monthly maintenance](monthly_maintenance.md). For each watch list above, scan since the last "Last verified" date for:

- Deprecation notices with cutover dates
- Endpoint URL or path changes (routing prefixes, version bumps)
- New required headers, auth, or subscription mechanisms
- Payload / wrapping format changes

When you confirm an endpoint is still serving correctly, bump the "Last verified" column.

**Soft-failure fingerprint** (what bit us on 2026-04-23): WSS handshake succeeds, process stays alive, but no frames are ever pushed. Heuristic: if a streaming session's expected output file goes more than 24h without growing, treat it as a probable vendor change until proven otherwise. The legacy `/ws/` Binance URL is the exemplar — connect succeeds, recv silently times out forever.

## Deploys

**`origin/main` is the only source of truth. Never `git commit` on the VPS.**

Deploy = push to `origin/main` from local, then on the VPS:

```bash
ssh vps-madrid "cd /root/trading_pm_data_feed && git fetch origin && git reset --hard origin/main"
```

`git status` on the box is then a real health signal: clean + `0/0` vs `origin/main` means prod matches the repo. Only `.env`, `.env.bak*`, `data/`, and `logs/` should ever show as untracked — all gitignored or deliberately local.

Before 2026-07-30 the box vendored subtrees and made its own deploy commits, so it drifted 21 behind / 6 ahead of origin and `git status` told you nothing. That's gone; pre-reset HEAD is tagged `deploy-history-20260729` on the VPS.

**A reset does not restart anything.** Running processes keep the code they loaded at start — restart a tmux session only if the deploy changed code it uses. Cron jobs pick up changes on their next tick automatically.

## Monitoring

`utils/vps_watchdog.sh` — polls the VPS every 10 min and alerts to Discord **`#pm-dual-price`** on failure.

It runs on **leon-air4 (the Mac), not the VPS** — deliberately. A monitor on the box can't alert when the box is what broke: during the 2026-07-28 outage a VPS-local check would have had no DNS to reach Discord with, and would have sat silent. It reaches the VPS over Tailscale, which stayed up throughout.

- Schedule: LaunchAgent `com.noel.vps-watchdog` (`~/Library/LaunchAgents/`), `StartInterval` 600s, survives reboot.
- Checks: SSH reachable · public DNS resolves · every expected tmux session alive · perp/coinbase DB `max(timestamp)` < 15 min old · `btc_depth` + `pm_btcupdown` newest JSONL < 5 min old. `liq_collector` is liveness-only (event-driven — no liquidation, no write).
- Alerts on **state change only** (one on break, one on recovery), and only after a problem repeats on **2 consecutive runs** — a laptop poller sees transient blips, so detection is 10–20 min rather than 10.
- Webhook URL lives in `~/.config/vps-watchdog/webhook` (chmod 600, **not** in git and **not** the VPS `.env`). Missing file → checks still log, no alert sent.
- Logs: `~/.config/vps-watchdog/watchdog.log`. Tunables (expected sessions, thresholds, interval) at the top of the script.

**When adding or removing a stream, update `EXPECTED_SESSIONS` in the script.** (`pm_signal_sim` re-added 2026-07-29 on the live-test v2 redeploy.)

## Health checks

```bash
# From local
python -m utils.local db-stats
python -m utils.local tail-log accumulate_1m

# On VPS
ssh vps-madrid "crontab -l; ps -ef | grep python | grep -v grep"
```
