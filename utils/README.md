# Utils

VPS and local management utilities for the pipeline.

## Setup

1. Add SSH config to `~/.ssh/config`:

```
Host vps-madrid
    HostName <your-vps-ip>
    User root
    IdentityFile ~/.ssh/mykey
    IdentitiesOnly yes
```

2. Create `.env` in project root:

```
VPS_SSH_HOST=vps-madrid
VPS_PROJECT_DIR=/root/trading_pm_data_feed
```

## local.py — Run from your machine

Manages the VPS remotely via SSH.

```bash
# Check VPS DB status
python -m utils.local db-stats

# List available artifact dates on VPS
python -m utils.local list-artifacts

# Pull today's artifact bundle
python -m utils.local pull-artifact
python -m utils.local pull-artifact --date 2026-03-21 --out-dir data/artifacts

# Pull the SQLite DB (for debugging/parity checks)
python -m utils.local pull-db
python -m utils.local pull-db --out data/btcusdt_perp_1m.sqlite

# Run SQL on VPS DB
python -m utils.local query-db "SELECT * FROM ohlcv_btcusdt_1m ORDER BY timestamp DESC LIMIT 5"
python -m utils.local query-db "SELECT COUNT(*) FROM ohlcv_btcusdt_1m WHERE timestamp BETWEEN '2026-03-21 00:00:00' AND '2026-03-21 23:59:00'"

# Tail a log file on VPS
python -m utils.local tail-log accumulate_1m
python -m utils.local tail-log repair_gaps_1m --lines 20
python -m utils.local tail-log build_artifact --lines 10
```

## remote.py — Run on the VPS

Data/log/artifact management on the server.

```bash
# DB coverage stats
.venv/bin/python -m utils.remote db-stats

# Delete artifact dirs older than 30 days
.venv/bin/python -m utils.remote cleanup-artifacts
.venv/bin/python -m utils.remote cleanup-artifacts --keep-days 7 --dry-run

# Rotate log files larger than 50 MB
.venv/bin/python -m utils.remote rotate-logs
.venv/bin/python -m utils.remote rotate-logs --max-mb 20
```

## Notes

- `local.py` requires the SSH config alias to be set up (see Setup above)
- `.env` is gitignored — each machine needs its own
- `remote.py` uses relative paths from the project root, so always `cd` into the project dir first (the cron jobs already do this)
