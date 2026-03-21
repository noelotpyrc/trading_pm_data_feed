# trading_pm_data_feed

Data feed and artifact pipelines for the trading PM system. Runs on a VPS to accumulate market data and build daily signal artifacts for local trading nodes.

## Pipelines

### 1. `cex_data_feed/` — 1-Minute BTCUSDT Perp OHLCV

Accumulates 1-minute candle data from Binance into a SQLite database. Runs continuously on VPS.

- **Backfill**: Downloads historical kline data from Binance Vision
- **Accumulate**: Cron script fetches new candles from `db_max + 1 min` (O(1) lookup)
- **Repair**: Manual/daily script scans for internal gaps (O(n) scan)

See [docs/pipeline_1m.md](docs/pipeline_1m.md) for details.

```bash
# Backfill from June 2025
.venv/bin/python -m cex_data_feed.scripts.backfill_1m \
  --db data/btcusdt_perp_1m.sqlite --start 2025-06

# Periodic accumulation (cron, every 2 min)
.venv/bin/python -m cex_data_feed.scripts.accumulate_1m \
  --db data/btcusdt_perp_1m.sqlite

# Repair gaps (manual/daily)
.venv/bin/python -m cex_data_feed.scripts.repair_gaps_1m \
  --db data/btcusdt_perp_1m.sqlite
```

### 2. `pm_btc15updown_artifact/` — Daily Volatility Signal Artifacts

Builds daily signal artifacts from the 1m OHLCV SQLite DB. Runs once per day on VPS before trading starts. Produces model coefficients + empirical z-pool for computing P(close > K) on 15-minute epochs.

See [docs/artifact_bundle_plan.md](docs/artifact_bundle_plan.md) for details.

```bash
# Build today's artifact
.venv/bin/python -m pm_btc15updown_artifact.scripts.build_daily_artifact \
  --db data/btcusdt_perp_1m.sqlite --out-dir data/artifacts
```

## Project Structure

```
cex_data_feed/              # Pipeline 1: OHLCV data collection
  binance/                  #   Binance API client
  pipeline_1m/              #   SQLite persistence + fetch logic
  scripts/                  #   CLI entry points (backfill, accumulate, repair)
pm_btc15updown_artifact/    # Pipeline 2: signal artifact builder
  vol_signal_artifacts.py   #   Core artifact build logic
  vol_signal_spec.md        #   Signal generation specification
  scripts/                  #   CLI entry points (build_daily_artifact)
tests/
docs/
data/                       # SQLite databases + artifacts (gitignored)
```

## Setup

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Tests

```bash
.venv/bin/python -m pytest tests/ -v
```
