# Artifact Bundle Pipeline

## Overview

This project contains two independent pipelines:

1. **`cex_data_feed/`** — Collects 1m BTCUSDT perp OHLCV into SQLite (VPS, continuous cron)
2. **`pm_btc15updown_artifact/`** — Builds daily volatility signal artifacts from that SQLite (VPS, once/day pre-trading)

The artifact bundle is consumed by a local trading node to compute P(close > K) for 15-minute up/down probability signals.

## Data Flow

```
VPS (continuous):
  accumulate_1m (cron) → ohlcv_btcusdt_1m.sqlite (live, WAL mode)

VPS (once/day, pre-trading):
  1. build_daily_artifact reads from live SQLite
  2. Outputs: data/artifacts/YYYY-MM-DD/
       ├── model.json        (linear regression coefficients per MAR horizon)
       ├── z_pool.npy         (empirical z-score distribution)
       └── metadata.json      (build info, coverage stats)

Local machine (pre-trading):
  3. Consumer pulls artifact dir via rsync/scp (consumer's responsibility)
  4. Optional: pull SQLite snapshot for debugging/parity checks

Local trading node (runtime):
  5. Loads artifact files locally — no VPS connection needed
  6. Uses model + z_pool to compute live P(up) signals on each 1m bar
```

## Artifact Contents

Each daily artifact (`data/artifacts/YYYY-MM-DD/`) contains:

| File | Contents |
|------|----------|
| `model.json` | Walk-forward linear regression models for MAR horizons 1, 3, 5. Includes intercept, coefficients, training window info. |
| `z_pool.npy` | Sorted array of standardized historical returns for empirical probability calculation. |
| `metadata.json` | Build metadata: score date, data coverage, row counts, config. |

## Build Script

`pm_btc15updown_artifact/scripts/build_daily_artifact.py`

Runs on VPS once per day before the trading node starts.

```bash
.venv/bin/python -m pm_btc15updown_artifact.scripts.build_daily_artifact \
  --db data/btcusdt_perp_1m.sqlite \
  --out-dir data/artifacts \
  [--date 2026-03-21] [--retry 3] [--debug]
```

- Reads 1m OHLCV from SQLite (maps `timestamp` → `datetime_utc` internally)
- Requires ~15 days of history (1d parkinson_1440 warmup + 7d training + 7d z-pool)
- Trains 3 linear regression models (MAR horizons 1, 3, 5)
- Builds z-pool from 7 days of scored history
- Retries on failure (for cron reliability)
- Exits non-zero if artifact build fails after all retries

## Column Naming Convention

- **SQLite DB** uses `timestamp` as the column name
- **`pm_btc15updown_artifact`** uses `datetime_utc` internally (historical convention from the signal spec)
- Mapping happens at the read boundary — the build script aliases `timestamp` → `datetime_utc` when loading from SQLite

## Data Quality in Metadata

The build script records source data quality stats in `metadata.json`, so consumers can decide whether to use the artifact or fall back to an older one:

```json
"training_data_quality": {
    "expected_candles": 10080,
    "actual_candles": 10080,
    "missing_candles": 0,
    "missing_pct": 0.0,
    "largest_gap_minutes": 0,
    "largest_gap_at": null
},
"zpool_data_quality": {
    "expected_candles": 10065,
    "actual_candles": 10065,
    "missing_candles": 0,
    "missing_pct": 0.0,
    "largest_gap_minutes": 0,
    "largest_gap_at": null
}
```

The build is **permissive** — it does not reject artifacts due to gaps. It records the issue and defers to the consumer to decide whether the model is usable.

## Parity Checker

`pm_btc15updown_artifact/check_vol_signal_artifact_parity.py`

Used for **local development testing only**. Compares the artifact builder's computed features and predictions against a known-good truth CSV to verify the migrated code produces identical results.

```bash
# Using local SQLite DB (default: data/btcusdt_perp_1m.sqlite)
/Users/noel/projects/venvs/production/bin/python -m pm_btc15updown_artifact.check_vol_signal_artifact_parity \
  --sqlite-db data/btcusdt_perp_1m.sqlite \
  --truth-csv "/Volumes/Extreme SSD/trading_data/cex/ohlvc/binance_btcusdt_perp_1m/BTCUSDT-1m-features-vol.csv"
```

Defaults: `--feature-day 2025-07-01`, `--prediction-day 2025-08-01`, `--tolerance 1e-10`.

Checks two things:
1. **Feature day** — OHLCV, parkinson volatilities, ratios, forward returns, MAR targets (30 columns)
2. **Prediction day** — pred_mar_1/3/5, mar_blend, strike_K, ttl, sigma_W_ttl (7 columns)

All columns should match within floating-point tolerance (~1e-15). Requires local SQLite DB with data from 2025-06 onwards and the truth CSV on the external SSD.

## Consumer Pull (not in this project)

Pulling artifacts from the VPS is the consumer's responsibility. Recommended approach:

```bash
# Pull today's artifact bundle
rsync -az user@vps:/root/trading_pm_data_feed/data/artifacts/$(date -u +%Y-%m-%d)/ \
  ~/data/artifacts/$(date -u +%Y-%m-%d)/

# Validate files exist before starting trading node
for f in model.json z_pool.npy metadata.json; do
  [ -f ~/data/artifacts/$(date -u +%Y-%m-%d)/$f ] || { echo "Missing $f"; exit 1; }
done
```

Optional: also pull the SQLite snapshot for debugging/parity checks.

## Cron Setup (VPS)

```bash
crontab -e
```

```
# Build daily artifact at 00:05 UTC (before trading day)
5 0 * * * cd /root/trading_pm_data_feed && .venv/bin/python -m pm_btc15updown_artifact.scripts.build_daily_artifact --db data/btcusdt_perp_1m.sqlite --out-dir data/artifacts --retry 3 >> data/build_artifact.log 2>&1
```

Replace `/root/trading_pm_data_feed` with the actual project path on VPS (`pwd`).

## Verified

- Parity check passed (2026-03-21): all 37 columns match truth within ~1e-15 tolerance
- Build tested against local dev DB with data from 2025-06 to 2026-03-21
