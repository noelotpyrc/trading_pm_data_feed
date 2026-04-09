# BTCUSDT Perp Signal Engine

Real-time signal alerting for BTCUSDT perpetual futures based on 1m candle features.

## Architecture

```
VPS
├── cex_data_feed (cron, every 5m)     ← accumulates OHLCV to SQLite
├── btcusdt_perp_signal (long-running) ← WebSocket listener + feature engine
│   ├── Binance kline_1m WS stream
│   ├── In-memory rolling buffer (1800 bars, seeded from DB on start)
│   ├── Feature computation on each candle close
│   ├── Threshold check → signal detection
│   ├── signals table (SQLite)
│   └── Telegram alert on signal fire
```

The signal engine runs alongside the existing cron accumulator. It reads from the same OHLCV DB on startup to seed its buffer, then maintains the buffer in memory — appending one candle per minute from the WebSocket stream. No repeated DB reads during normal operation.

## Components

| File | Purpose |
|------|---------|
| `features.py` | Feature calculations (parkinson vol, volume ratios, VWAP, etc.) and threshold checks |
| `signal_engine.py` | WebSocket listener, rolling buffer, feature pipeline, alert dispatch |
| `signal_db.py` | `signals_btcusdt_perp` table — persists fired signals with feature values |
| `alert.py` | Telegram bot push notification (stdlib only, no extra deps) |
| `scripts/run_signal_engine.py` | Entry point with logging, .env loading, graceful shutdown |
| `strategy_guide.md` | Full feature definitions and strategy logic (thresholds, entry/exit rules) |

## Features computed

All from 1m OHLCV + num_trades. Largest rolling window is 1440 bars (1 day).

- `parkinson_ratio` — short-term (30) vs long-term (1440) Parkinson volatility
- `volume_ratio_30`, `volume_ratio_60` — current volume vs rolling SMA
- `avg_trade_size_zscore_60` — z-score of log avg trade size
- `vwap_60_cross_rate_90` — frequency of VWAP crossovers
- `cum_return_5bar_norm_p30` — 5-bar cumulative return normalized by vol
- `efficiency_ratio_360` — price efficiency over 360 bars

## Signal logic

Common filters must pass first, then long or short specific filters. See `strategy_guide.md` for exact thresholds.

## Steps to production

### Done

- [x] Feature calculations ported from strategy guide
- [x] Signal engine with WebSocket, rolling buffer, auto-reconnect
- [x] Signal persistence (SQLite signals table)
- [x] Telegram alert module (ready, needs bot token config)
- [x] Entry point with logging and graceful shutdown
- [x] Phase 1 smoke test (live WS, pulls warmup from VPS)

### TODO

- [ ] Set up Telegram bot — create bot via BotFather, get token and chat ID, add to `.env`
- [ ] Deploy to VPS — `git pull`, `pip install websocket-client`
- [ ] Run smoke test on VPS to validate
- [ ] Choose process manager — tmux for initial validation, systemd for long-term
- [ ] Create systemd service file if going that route
- [ ] Add `signal_engine.log` to `utils/local.py tail-log` and monthly backup
- [ ] Phase 2 parity test — validate signals match offline batch computation (deferred, needs truth data design)

### Deployment commands (VPS)

```bash
# Install dependency
.venv/bin/pip install websocket-client

# Add Telegram config to .env
echo "TELEGRAM_BOT_TOKEN=your_token" >> .env
echo "TELEGRAM_CHAT_ID=your_chat_id" >> .env

# Quick start with tmux
tmux new -s signal
.venv/bin/python -m btcusdt_perp_signal.scripts.run_signal_engine
# Ctrl+B, D to detach

# Or systemd (create /etc/systemd/system/btcusdt-signal.service first)
systemctl enable --now btcusdt-signal
systemctl status btcusdt-signal
journalctl -u btcusdt-signal -f
```

## Logs

- VPS: `data/signal_engine.log` (file + stdout)
- Local tail: `python -m utils.local tail-log signal_engine`

## Current status

**Phase 1 smoke test in progress (2026-04-09)** — running locally against live Binance WS with warmup data pulled from VPS. Validating no crashes, no NaN features, correct buffer management.
