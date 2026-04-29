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
| `btcusdt_perp_signal.scripts.run_signal_engine` | Signal engine + alerts | `data/` signal DB |

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

Webhook URLs live in VPS `.env` (loaded by `alert.send_discord`). Missing env var → send silently skipped.

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

## Health checks

```bash
# From local
python -m utils.local db-stats
python -m utils.local tail-log accumulate_1m

# On VPS
ssh vps-madrid "crontab -l; ps -ef | grep python | grep -v grep"
```
