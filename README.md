# trading_pm_data_feed

Data feed pipelines for the trading PM system. Accumulates historical and live market data into local databases for consumption by downstream strategy, analysis, and model training applications.

## Current Pipelines

### pipeline_1m — 1-Minute BTCUSDT Perp OHLCV

Accumulates 1-minute candle data for BTCUSDT perpetual futures from Binance into a SQLite database.

- **Backfill**: Downloads historical kline data from Binance Vision and imports into SQLite
- **Accumulate**: Periodic cron script fetches new candles from `db_max + 1 min` (O(1) lookup)
- **Repair**: Manual/daily script scans for gaps and fills them (O(n) scan)

See [docs/pipeline_1m.md](docs/pipeline_1m.md) for details.

**Quick start:**

```bash
# Backfill from June 2025 to now
.venv/bin/python -m cex_data_feed.scripts.backfill_1m \
  --db data/btcusdt_perp_1m.sqlite --start 2025-06

# Periodic accumulation (cron, every 2 min)
.venv/bin/python -m cex_data_feed.scripts.accumulate_1m \
  --db data/btcusdt_perp_1m.sqlite

# Repair gaps (manual/daily)
.venv/bin/python -m cex_data_feed.scripts.repair_gaps_1m \
  --db data/btcusdt_perp_1m.sqlite
```

## Project Structure

```
cex_data_feed/
  binance/        # Binance API client
  pipeline_1m/    # SQLite persistence + fetch logic for 1m candles
  scripts/        # CLI entry points (backfill, accumulate, repair, download, merge)
tests/
docs/
data/             # SQLite databases (gitignored)
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
