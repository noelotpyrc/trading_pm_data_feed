# pm_btc15updown_artifact: Daily Volatility Signal Artifact Builder

## What

Builds daily signal artifacts from the 1m OHLCV SQLite database. Each artifact contains pre-trained linear regression models and an empirical z-score distribution used by a local trading node to compute P(close > K) for 15-minute up/down probability signals.

This is a standalone pipeline — it takes 1m OHLCV data as input and produces self-contained artifact files as output.

## Architecture

```
VPS (once/day, 00:01 UTC):
  build_daily_artifact reads SQLite → outputs data/artifacts/YYYY-MM-DD/
    ├── model.json      (OLS coefficients per MAR horizon)
    ├── z_pool.npy      (empirical z-score distribution)
    └── metadata.json   (build info, data quality stats)

Local (consumer's responsibility):
  rsync artifact dir from VPS → load locally → no VPS connection at runtime
```

## Artifact Contents

Each daily artifact (`data/artifacts/YYYY-MM-DD/`) contains:

| File | Contents |
|------|----------|
| `model.json` | Walk-forward OLS models for MAR horizons 1, 3, 5. Includes intercept, coefficients, training window. |
| `z_pool.npy` | Sorted array of standardized historical returns for empirical probability calculation. |
| `metadata.json` | Build metadata: score date, data coverage, row counts, config, data quality stats. |

## Build Script

```bash
# Build today's artifact
.venv/bin/python -m pm_btc15updown_artifact.scripts.build_daily_artifact \
  --db data/btcusdt_perp_1m.sqlite \
  --out-dir data/artifacts

# Build for a specific date
.venv/bin/python -m pm_btc15updown_artifact.scripts.build_daily_artifact \
  --db data/btcusdt_perp_1m.sqlite \
  --out-dir data/artifacts \
  --date 2026-03-21 \
  --debug
```

Options:
- `--date YYYY-MM-DD` — score date (default: today UTC)
- `--retry N` — retry attempts on failure (default: 3)
- `--debug` — verbose output

What the build does:
1. Loads ~15 days of OHLCV from SQLite (1d parkinson_1440 warmup + 7d training + 7d z-pool)
2. Computes parkinson volatility features at multiple windows (10, 15, 30, 45, 60, 1440 min)
3. Trains 3 OLS models (MAR horizons 1, 3, 5) on a 7-day walk-forward training window
4. Builds z-pool from 7 days of scored history
5. Saves artifacts + data quality stats to output dir

## Data Quality in Metadata

The build records source data quality in `metadata.json` so consumers can decide whether to use the artifact or fall back to an older one:

```json
"training_data_quality": {
    "expected_candles": 10080,
    "actual_candles": 10080,
    "missing_candles": 0,
    "missing_pct": 0.0,
    "largest_gap_minutes": 0,
    "largest_gap_at": null
},
"zpool_data_quality": { ... }
```

The build is **permissive** — it does not reject artifacts due to gaps. It records the issue and defers to the consumer.

## Column Naming Convention

- **SQLite DB** uses `timestamp`
- **This package** uses `datetime_utc` internally (historical convention from the signal spec)
- Mapping happens at the read boundary in `data_loader.py`

## Cron Setup (VPS)

```bash
crontab -e
```

```
# Build daily artifact at 00:01 UTC (before trading day)
1 0 * * * cd /root/trading_pm_data_feed && .venv/bin/python -m pm_btc15updown_artifact.scripts.build_daily_artifact --db data/btcusdt_perp_1m.sqlite --out-dir data/artifacts --retry 3 >> data/build_artifact.log 2>&1
```

Replace `/root/trading_pm_data_feed` with the actual project path on VPS (`pwd`).

## Consumer Pull

Pulling artifacts is the consumer's responsibility:

```bash
# Pull today's artifact bundle
rsync -az vps-madrid:/root/trading_pm_data_feed/data/artifacts/$(date -u +%Y-%m-%d)/ \
  ~/data/artifacts/$(date -u +%Y-%m-%d)/

# Validate files exist before starting trading node
for f in model.json z_pool.npy metadata.json; do
  [ -f ~/data/artifacts/$(date -u +%Y-%m-%d)/$f ] || { echo "Missing $f"; exit 1; }
done
```

Or use the local management utility:

```bash
python -m utils.local pull-artifact
python -m utils.local pull-artifact --date 2026-03-21
```

Optional: pull the SQLite DB for debugging/parity checks:

```bash
python -m utils.local pull-db
```

## Parity Checker

Verifies the artifact builder produces identical results to the original implementation.

```bash
/Users/noel/projects/venvs/production/bin/python \
  -m pm_btc15updown_artifact.check_vol_signal_artifact_parity \
  --sqlite-db data/btcusdt_perp_1m.sqlite \
  --truth-csv "/Volumes/Extreme SSD/trading_data/cex/ohlvc/binance_btcusdt_perp_1m/BTCUSDT-1m-features-vol.csv"
```

Defaults: `--feature-day 2025-07-01`, `--prediction-day 2025-08-01`, `--tolerance 1e-10`.

Checks:
1. **Feature day** — OHLCV, parkinson volatilities, ratios, forward returns, MAR targets (30 columns)
2. **Prediction day** — pred_mar_1/3/5, mar_blend, strike_K, ttl, sigma_W_ttl (7 columns)

Requires local SQLite DB with data from 2025-06 onwards and the truth CSV on external SSD.

## Module Layout

```
pm_btc15updown_artifact/
  vol_signal_artifacts.py            # Core build logic (features, models, z-pool)
  vol_signal_spec.md                 # Signal generation specification
  data_loader.py                     # OHLCV loading from SQLite/CSV with dedup + quality check
  check_vol_signal_artifact_parity.py  # Parity checker (local dev only)
  scripts/
    build_daily_artifact.py          # CLI entry point (cron target)
```

## Status

- **Deployed**: VPS cron running daily at 00:01 UTC
- **Parity verified**: 2026-03-21, all 37 columns match truth within ~1e-15 tolerance
- **Data quality**: metadata.json records training + z-pool window quality stats
- **Dependencies**: pandas, numpy (see requirements.txt)
