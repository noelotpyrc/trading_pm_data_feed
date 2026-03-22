# pipeline_1m: 1-Minute BTCUSDT Perp Data Accumulator

## What

A two-phase pipeline that builds and maintains a SQLite database of 1-minute BTCUSDT perpetual futures OHLCV candles from Binance.

The database serves as **historical warmup data** for downstream consumer applications (model training, analysis, strategy initialization). Consumers read from this SQLite DB to construct their initial historical state, then maintain their own live streams for real-time execution.

## Architecture

```
Phase 1 — Backfill (one-time):
  Binance Vision ZIPs → merge → CSV → SQLite

Phase 2 — Accumulate (cron, every 5 min):
  DB max + 1 min → Binance FAPI → SQLite insert

Repair (manual/daily):
  O(n) gap scan → Binance FAPI → SQLite insert
```

## Database Schema

Table: `ohlcv_btcusdt_1m`

| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | Primary key, autoincrement |
| timestamp | TEXT | `YYYY-MM-DD HH:MM:SS` (UTC, no tz suffix) |
| open | REAL | |
| high | REAL | |
| low | REAL | |
| close | REAL | |
| volume | REAL | |
| quote_asset_volume | REAL | |
| num_trades | INTEGER | |
| taker_buy_base_volume | REAL | |
| taker_buy_quote_volume | REAL | |
| ingested_at | TEXT | UTC timestamp of when the row was inserted |

**Key design decisions:**
- `timestamp` is **not** a unique key. Multiple rows per timestamp are allowed to track data corrections over time.
- Consumers should dedup by taking the row with the latest `ingested_at` per timestamp.
- `ingested_at` records when each row was written, enabling audit trails and correction detection.
- SQLite WAL mode is enabled for safe concurrent reads while the accumulator writes.

## Phase 1: Backfill

Downloads historical data from Binance Vision (monthly + daily ZIPs), merges into CSV, and imports into SQLite.

```bash
.venv/bin/python -m cex_data_feed.scripts.backfill_1m \
  --db data/btcusdt_perp_1m.sqlite \
  --start 2025-06 \
  --debug
```

Internally runs four steps:
1. Download monthly kline ZIPs from Binance Vision (futures USDT-M)
2. Download daily kline ZIPs for the current partial month (up to yesterday)
3. Merge all ZIPs into a single temporary CSV
4. Import CSV into SQLite in chunks (default 50k rows/chunk)

Options:
- `--end YYYY-MM` — last month to backfill (default: last completed month)
- `--keep-zips DIR` — cache downloaded ZIPs for faster re-runs
- `--chunk-size N` — rows per import chunk
- `--dry-run` — preview without downloading or writing

## Phase 2: Accumulate

Lightweight run-and-exit script. Reads `max(timestamp)` from the DB, fetches all closed 1m candles from `max + 1 min` to now, and inserts them.

```bash
.venv/bin/python -m cex_data_feed.scripts.accumulate_1m \
  --db data/btcusdt_perp_1m.sqlite
```

- O(1) DB lookup — just reads `max(timestamp)`
- Pages through Binance API in batches of 1500 if the gap is large (e.g. after downtime)
- Designed to run frequently on cron (every 5 minutes)

Options:
- `--symbol` — Binance symbol (default: BTCUSDT)
- `--dry-run` — fetch but don't write
- `--debug` — verbose output

## Repair Gaps

Detects and fills gaps in the DB. Checks two things in order:

1. **Trailing gap** (O(1)) — is `db_max` behind current time? If so, fetches from `db_max + 1 min` to now.
2. **Internal gaps** (O(n)) — scans all distinct timestamps for missing minutes between min and max. If found, fetches from the first gap forward.

Both checks run in a single invocation. Duplicates for existing data are expected and harmless.

```bash
.venv/bin/python -m cex_data_feed.scripts.repair_gaps_1m \
  --db data/btcusdt_perp_1m.sqlite
```

Run manually or on a less frequent schedule (e.g. daily).

Options:
- `--symbol` — Binance symbol (default: BTCUSDT)
- `--dry-run` — detect gaps but don't write
- `--debug` — verbose output

## Cron Setup

First, find the project path on your VPS:

```bash
cd /path/to/trading_pm_data_feed
pwd  # copy this output, e.g. /root/trading_pm_data_feed
```

Then set up cron:

```bash
crontab -e
```

```
# Accumulate new candles every 5 minutes
*/5 * * * * cd /root/trading_pm_data_feed && .venv/bin/python -m cex_data_feed.scripts.accumulate_1m --db data/btcusdt_perp_1m.sqlite >> data/accumulate_1m.log 2>&1

# Repair gaps once a day at midnight UTC
0 0 * * * cd /root/trading_pm_data_feed && .venv/bin/python -m cex_data_feed.scripts.repair_gaps_1m --db data/btcusdt_perp_1m.sqlite >> data/repair_gaps_1m.log 2>&1
```

Replace `/root/trading_pm_data_feed` with the actual output of `pwd`.

## Module Layout

```
cex_data_feed/
  pipeline_1m/
    sqlite_db.py    # SQLite persistence (ensure_table, insert_candles, read_last_n, coverage_stats, find_first_gap)
    fetch.py        # API wrapper — fetch_closed_1m_since(start_ts) with paging
  binance/
    api.py          # Binance FAPI client (Kline, fetch_klines, klines_to_dataframe)
  scripts/
    backfill_1m.py          # Unified backfill: download + merge + import
    backfill_1m_from_csv.py # CSV-only import (used by backfill_1m internally)
    accumulate_1m.py        # Periodic accumulator (cron target, O(1) DB lookup)
    repair_gaps_1m.py       # Gap repair (manual/daily, O(n) gap scan)
    download_binance_monthly_klines.py
    download_binance_daily_klines.py
    merge_binance_klines.py
```

## Consumer Usage

Read the SQLite DB directly. To get the latest deduplicated candles:

```python
from cex_data_feed.pipeline_1m.sqlite_db import read_last_n, coverage_stats

# Get the 100 most recent candles (deduped by latest ingested_at)
df = read_last_n("data/btcusdt_perp_1m.sqlite", 100)

# Check coverage
stats = coverage_stats("data/btcusdt_perp_1m.sqlite")
if stats:
    min_ts, max_ts, count = stats
    print(f"{count:,} candles from {min_ts} to {max_ts}")
```

For remote consumers: `rsync` the SQLite file from the VPS.
