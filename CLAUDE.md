# Project: trading_pm_data_feed

VPS data pipeline for 1m BTCUSDT OHLCV accumulation and daily artifact generation.

## Virtual Environment

Uses `.venv` on VPS, unified production venv locally at `/Users/noel/projects/venvs/production`.

- Local Python: `/Users/noel/projects/venvs/production/bin/python`
- VPS Python: `.venv/bin/python`

## VPS Access

SSH alias `vps-madrid` configured in `~/.ssh/config`. See `utils/README.md` for setup.

## Monthly Maintenance Reminder

At the start of each month (1st–3rd), remind the user to run the monthly VPS cleanup and backup checklist documented in `docs/monthly_maintenance.md`. Steps include: pull DB/artifact backups to local, cleanup old artifacts, rotate logs, verify pipeline health.
