# Ops Memo — Running Pipelines

Snapshot of what runs on `vps-madrid` and where the code lives. Keep in sync when adding/removing a stream.

Last verified: 2026-04-29 (V2 keyset migration restart)

## Data streams (code)

### Collection
| Module | Purpose | Output |
|---|---|---|
| `cex_data_feed.scripts.accumulate_1m` | 1m BTCUSDT perp OHLCV (Binance) | `data/btcusdt_perp_1m.sqlite` |
| `cex_data_feed.scripts.repair_gaps_1m` | Daily gap scan/repair | same DB |
| `cex_data_feed.scripts.coinbase_accumulate_1m` | 1m BTC-USD spot OHLCV (Coinbase) | `data/btcusd_coinbase_1m.sqlite` |
| `cex_data_feed.scripts.collect_btc_depth` | Orderbook depth logger | `data/btc_depth/` |
| `cex_data_feed.scripts.collect_liquidations` | Liquidation stream | `data/liquidations/` |
| `pm_btc15updown_data.collect_pm_btcupdown` | Polymarket single-market | `data/pm_btcupdown/` |
| `pm_btc15updown_data.collect_pm_dual` | Polymarket dual-market | `data/pm_dual/` |

### Artifacts / signals
| Module | Purpose | Output |
|---|---|---|
| `pm_btc15updown_artifact.scripts.build_daily_artifact` | v1 vol signal artifact | `data/artifacts/` |
| `pm_btc15updown_artifact.scripts.build_daily_artifact_v3` | v3 per-TTL MAR artifact | `data/artifacts_v3/` |
| `pm_btc15updown_data.signal_stream_v3` | Live V3 contrarian stream — triggers TTL≤2 prob>0.90/<0.10, 10 polls at 3s, sends only on real entry (delta>0.05), near-close both tokens | in-process |
| `pm_btc15updown_data.signal_stream_v3_directional` | Live V3 directional stream — 4 specific TTL/prob rules, 8 polls at 3s, near-close both tokens, JSONL log | in-process + `logs/signal_v3_directional.jsonl` |
| `btcusdt_perp_signal.scripts.run_signal_engine` | Signal engine + alerts | `data/signal_engine.log` |

## Running on VPS

### Cron (`crontab -l`)
```
*/5    * * * *   accumulate_1m            → data/accumulate_1m.log
2-57/5 * * * *   coinbase_accumulate_1m   → data/coinbase_accumulate_1m.log
0      0 * * *   repair_gaps_1m           → data/repair_gaps_1m.log
1      0 * * *   build_daily_artifact     → data/build_artifact.log
5      0 * * *   build_daily_artifact_v3  → data/build_artifact_v3.log
```
Coinbase runs offset (`2-57/5`) so it doesn't collide with the Binance accumulator on the same minute.

### Long-running (tmux — one session per process)
List: `ssh vps-madrid tmux ls`  ·  Attach: `ssh vps-madrid -t tmux attach -t <session>`

Each block below is the full launch command; copy-paste it directly into the VPS shell to (re)create the session detached. All commands assume the project venv at `/root/trading_pm_data_feed/.venv`.

Webhook URLs live in VPS `.env` (loaded by `alert.send_discord`). Missing env var → send silently skipped. All channels are in Discord server `1465846881836863796`:

| Env var | Channel | Channel ID | Used by |
|---|---|---|---|
| `DISCORD_WEBHOOK_URL` | `#trading-signals` | `1492314707372150814` | `signal`, `liq_collector` |
| `DISCORD_WEBHOOK_URL_PM` | `#pm-price-alert` | `1492688541405413396` | `pm_collector` |
| `DISCORD_WEBHOOK_URL_PM_DUAL` | `#pm-dual-price` | `1493441980968075488` | `pm_dual` |
| `DISCORD_WEBHOOK_URL_SIGNAL_V3` | `#pm-trading-signals` | `1494415390875320330` | `signal-v3-contrarian`, `signal-v3-dir` |

#### `signal` — signal engine + alerts (since Apr 13)
Webhook: `DISCORD_WEBHOOK_URL`
```bash
tmux new -d -s signal "cd /root/trading_pm_data_feed && .venv/bin/python -m btcusdt_perp_signal.scripts.run_signal_engine --db data/btcusdt_perp_1m.sqlite"
```

#### `btc_depth` — Binance orderbook depth logger (since Apr 13)
Webhook: —
```bash
tmux new -d -s btc_depth "cd /root/trading_pm_data_feed && .venv/bin/python -m cex_data_feed.scripts.collect_btc_depth --log-dir data/btc_depth"
```

#### `liq_collector` — Binance liquidation stream (since Apr 17)
Webhook: `DISCORD_WEBHOOK_URL` (default)
```bash
tmux new -d -s liq_collector "cd /root/trading_pm_data_feed && .venv/bin/python -m cex_data_feed.scripts.collect_liquidations --log-dir data/liquidations"
```

#### `pm_collector` — Polymarket single-market BTC up/down (since Apr 29, V2 keyset)
Webhook: `DISCORD_WEBHOOK_URL_PM`
```bash
tmux new -d -s pm_collector "cd /root/trading_pm_data_feed && .venv/bin/python -m pm_btc15updown_data.collect_pm_btcupdown --log-dir data/pm_btcupdown"
```

#### `pm_dual` — Polymarket dual-market arb tracker (since Apr 29, V2 keyset)
Webhook: `DISCORD_WEBHOOK_URL_PM_DUAL`
```bash
tmux new -d -s pm_dual "cd /root/trading_pm_data_feed && .venv/bin/python -m pm_btc15updown_data.collect_pm_dual --log-dir data/pm_dual"
```

#### `signal-v3-contrarian` — V3 contrarian live stream (since Apr 29, V2 keyset)
Webhook: `DISCORD_WEBHOOK_URL_SIGNAL_V3`
```bash
tmux new -d -s signal-v3-contrarian "cd /root/trading_pm_data_feed && .venv/bin/python -u -m pm_btc15updown_data.signal_stream_v3 --db data/btcusdt_perp_1m.sqlite --artifact-dir data/artifacts_v3"
```

#### `signal-v3-dir` — V3 directional live stream (since Apr 29, V2 keyset)
Webhook: `DISCORD_WEBHOOK_URL_SIGNAL_V3`
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
| `wss://fstream.binance.com/ws/btcusdt@depth20@500ms` | /public (legacy URL still serves) | `btc_depth`, `pm_collector` DepthFeed | 2026-05-02 — migrate to `/public/ws/...` as cleanup |
| `https://fapi.binance.com/fapi/v1/klines` | REST | `accumulate_1m` cron, `repair_gaps_1m` cron, `pm_collector` (`fetch_strike`), `pm_dual`, `signal-v3-*` | 2026-05-02 |

Watch list: [Binance Derivatives Change Log](https://developers.binance.com/docs/derivatives/change-log)

### Polymarket

V2 cutover **2026-04-28**; legacy offset-paginated `/events` deprecated **2026-05-01** in favor of cursor-based `/events/keyset`. We are on V2 + keyset (commits `d8c45fe`, `64794d9`).

| Endpoint | Used by | Last verified |
|---|---|---|
| `https://gamma-api.polymarket.com/events/keyset` | `pm_collector`, `pm_dual`, `signal-v3-*` (`resolve_market`), `pm_metadata` fetcher | 2026-04-29 |
| `https://clob.polymarket.com/book` | `pm_collector`, `pm_dual`, `signal-v3-*` (`fetch_prices`) | 2026-04-28 (post V2) |
| `wss://ws-subscriptions-clob.polymarket.com/ws/market` | `pm_collector` (`TradeFeed`) | 2026-04-28 (post V2) |

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
| `pm_dual` tmux | Polymarket Gamma + CLOB · Binance FAPI `/klines` |
| `signal-v3-contrarian`, `signal-v3-dir` tmux | Binance `kline_1m` WSS · Polymarket Gamma + CLOB · Binance FAPI `/klines` · SQLite warmup · daily v3 artifact |
| `accumulate_1m` cron | Binance FAPI `/klines` |
| `coinbase_accumulate_1m` cron | Coinbase Exchange `/candles` |
| `repair_gaps_1m` cron | Binance FAPI `/klines` |
| `build_daily_artifact`, `build_daily_artifact_v3` cron | SQLite only (no external) |
| `pm_metadata` fetcher (manual) | Polymarket Gamma `/events/keyset` |

### Review reminder

Run a vendor-changelog sweep monthly as part of [monthly maintenance](monthly_maintenance.md). For each watch list above, scan since the last "Last verified" date for:

- Deprecation notices with cutover dates
- Endpoint URL or path changes (routing prefixes, version bumps)
- New required headers, auth, or subscription mechanisms
- Payload / wrapping format changes

When you confirm an endpoint is still serving correctly, bump the "Last verified" column.

**Soft-failure fingerprint** (what bit us on 2026-04-23): WSS handshake succeeds, process stays alive, but no frames are ever pushed. Heuristic: if a streaming session's expected output file goes more than 24h without growing, treat it as a probable vendor change until proven otherwise. The legacy `/ws/` Binance URL is the exemplar — connect succeeds, recv silently times out forever.

## Health checks

```bash
# From local
python -m utils.local db-stats
python -m utils.local tail-log accumulate_1m

# On VPS
ssh vps-madrid "crontab -l; ps -ef | grep python | grep -v grep"
```
