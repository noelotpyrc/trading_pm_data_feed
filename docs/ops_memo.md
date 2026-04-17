# Ops Memo — Running Pipelines

Snapshot of what runs on `vps-madrid` and where the code lives. Keep in sync when adding/removing a stream.

Last verified: 2026-04-17

## Data streams (code)

### Collection
| Module | Purpose | Output |
|---|---|---|
| `cex_data_feed.scripts.accumulate_1m` | 1m BTCUSDT perp OHLCV (Binance) | `data/btcusdt_perp_1m.sqlite` |
| `cex_data_feed.scripts.repair_gaps_1m` | Daily gap scan/repair | same DB |
| `cex_data_feed.scripts.collect_btc_depth` | Orderbook depth logger | `data/btc_depth/` |
| `cex_data_feed.scripts.collect_liquidations` | Liquidation stream | `data/liquidations/` |
| `pm_btc15updown_data.collect_pm_btcupdown` | Polymarket single-market | `data/pm_btcupdown/` |
| `pm_btc15updown_data.collect_pm_dual` | Polymarket dual-market | `data/pm_dual/` |

### Artifacts / signals
| Module | Purpose | Output |
|---|---|---|
| `pm_btc15updown_artifact.scripts.build_daily_artifact` | v1 vol signal artifact | `data/artifacts/` |
| `pm_btc15updown_artifact.scripts.build_daily_artifact_v3` | v3 per-TTL MAR artifact | `data/artifacts_v3/` |
| `pm_btc15updown_data.signal_stream_v3` | Live V3 signal stream (auto-reloads daily artifact) | in-process |
| `btcusdt_perp_signal.scripts.run_signal_engine` | Signal engine + alerts | `data/` signal DB |

## Running on VPS

### Cron (`crontab -l`)
```
*/5 * * * *   accumulate_1m         → data/accumulate_1m.log
0   0 * * *   repair_gaps_1m        → data/repair_gaps_1m.log
1   0 * * *   build_daily_artifact  → data/build_artifact.log
5   0 * * *   build_daily_artifact_v3 → data/build_artifact_v3.log
```

### Long-running (tmux — one session per process)
List: `ssh vps-madrid tmux ls`  ·  Attach: `ssh vps-madrid -t tmux attach -t <session>`

All commands run from `/root/trading_pm_data_feed` inside the `signal` tmux session pattern:
`tmux new -s <session> -d "cd /root/trading_pm_data_feed && <cmd>"`

| tmux session | Discord webhook env var | Since | Command |
|---|---|---|---|
| `signal` | `DISCORD_WEBHOOK_URL` | Apr 13 | `.venv/bin/python -m btcusdt_perp_signal.scripts.run_signal_engine --db data/btcusdt_perp_1m.sqlite` |
| `btc_depth` | — | Apr 13 | `.venv/bin/python -m cex_data_feed.scripts.collect_btc_depth --log-dir data/btc_depth` |
| `pm_collector` | `DISCORD_WEBHOOK_URL_PM` | Apr 14 | `.venv/bin/python -m pm_btc15updown_data.collect_pm_btcupdown --log-dir data/pm_btcupdown` |
| `pm_dual` | `DISCORD_WEBHOOK_URL_PM_DUAL` | Apr 15 | `.venv/bin/python -m pm_btc15updown_data.collect_pm_dual --log-dir data/pm_dual` |
| `liq_collector` | `DISCORD_WEBHOOK_URL` (default) | Apr 15 | `.venv/bin/python -m cex_data_feed.scripts.collect_liquidations --log-dir data/liquidations` |
| `pm_signal_contrarian` | `DISCORD_WEBHOOK_URL_SIGNAL_V3` (via `--webhook-key`) | Apr 16 | `.venv/bin/python -m pm_btc15updown_data.signal_stream_v3 --db data/btcusdt_perp_1m.sqlite --artifact-dir data/artifacts_v3` |

Webhook URLs live in VPS `.env` (loaded by `alert.send_discord`). Missing env var → send silently skipped.

## Health checks

```bash
# From local
python -m utils.local db-stats
python -m utils.local tail-log accumulate_1m

# On VPS
ssh vps-madrid "crontab -l; ps -ef | grep python | grep -v grep"
```
