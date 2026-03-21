# Artifact Bundle Pipeline — Plan

## Goal

Build a pipeline on the VPS that transforms the live `ohlcv_btcusdt_1m.sqlite` into a daily artifact bundle that local trading nodes can pull and consume without any network dependency during trading.

## Data Flow

```
VPS (continuous):
  accumulate_1m (cron) → ohlcv_btcusdt_1m.sqlite (live, WAL mode)

VPS (pre-trading-day):
  1. SQLite .backup() → snapshot.sqlite (point-in-time, no WAL)
  2. snapshot.sqlite → artifact bundle (transformed data)

Local machine (pre-trading-day):
  3. Pull artifact bundle (required)
  4. Pull snapshot.sqlite (optional, for debugging/parity checks)

Local trading node (runtime):
  5. Read only local files — no VPS connection needed
```

## Why Snapshot First

The live DB uses WAL mode and is being written to by the accumulator cron. SQLite's `.backup()` API creates a consistent point-in-time copy that:
- Is safe to read while the live DB is being written to
- Produces a single-file DB (no `-wal` or `-shm` files)
- Can be transferred without worrying about partial writes

## Components to Build

### 1. Snapshot Script

`cex_data_feed/scripts/snapshot_db.py`

- Uses `sqlite3.Connection.backup()` to copy live DB → snapshot
- Output: `data/snapshots/btcusdt_perp_1m_YYYYMMDD.sqlite`
- Options: `--db` (source), `--out-dir`, `--date` (default: today)

### 2. Artifact Bundle Pipeline

TBD — depends on what the local trading node needs. Possible outputs:
- Resampled bars (5m, 15m, 1h, 4h, 1d)
- Pre-computed features/indicators
- Parquet files for fast columnar reads
- A single bundle directory or archive per day

### 3. Pull Script (local side)

TBD — rsync or scp wrapper to pull:
- Required: artifact bundle for the day
- Optional: snapshot SQLite for debugging

### 4. Cron Integration (VPS)

Add to the existing crontab:
```
# Build daily snapshot + artifact bundle at 00:05 UTC
5 0 * * * cd /root/trading_pm_data_feed && .venv/bin/python -m cex_data_feed.scripts.snapshot_db --db data/btcusdt_perp_1m.sqlite >> data/snapshot.log 2>&1
```

## Open Questions

- What format does the local trading node consume? (Parquet, SQLite, CSV, in-memory?)
- What transformations are needed? (Resampling, feature engineering, normalization?)
- Retention policy for snapshots? (Keep last N days, or archive to object storage?)
- Should the artifact bundle be a single file or a directory?
- Pull mechanism: rsync, scp, or something else?
