# Monthly Maintenance

Manual checklist for VPS cleanup and local backup, run at the start of each month.

## Backup destination

All backups go to external SSD, organized per pipeline:

```
/Volumes/Extreme SSD/vps_madrid_backup/
├── cex_data_feed/
│   ├── YYYY-MM-DD_btcusdt_perp_1m.sqlite
│   └── logs/
├── pm_btc15updown_artifact/
│   ├── YYYY-MM-DD/   (artifact dirs)
│   └── logs/
└── <future_pipeline>/
    └── logs/
```

## 1. Backup DB to SSD

```bash
scp vps-madrid:/root/trading_pm_data_feed/data/btcusdt_perp_1m.sqlite \
  "/Volumes/Extreme SSD/vps_madrid_backup/cex_data_feed/$(date -u +%Y-%m-%d)_btcusdt_perp_1m.sqlite"
```

## 2. Backup artifacts to SSD

```bash
rsync -az vps-madrid:/root/trading_pm_data_feed/data/artifacts/ \
  "/Volumes/Extreme SSD/vps_madrid_backup/pm_btc15updown_artifact/"
```

## 3. Backup logs to SSD

```bash
scp vps-madrid:/root/trading_pm_data_feed/data/accumulate_1m.log \
  "/Volumes/Extreme SSD/vps_madrid_backup/cex_data_feed/logs/"
scp vps-madrid:/root/trading_pm_data_feed/data/repair_gaps_1m.log \
  "/Volumes/Extreme SSD/vps_madrid_backup/cex_data_feed/logs/"
scp vps-madrid:/root/trading_pm_data_feed/data/build_daily_artifact.log \
  "/Volumes/Extreme SSD/vps_madrid_backup/pm_btc15updown_artifact/logs/"
```

## 4. Check VPS disk and DB status

```bash
python -m utils.local db-stats
ssh vps-madrid "df -h /root/trading_pm_data_feed"
```

## 5. Cleanup old artifacts on VPS

Keep last 30 days, preview first:

```bash
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote cleanup-artifacts --keep-days 30 --dry-run"
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote cleanup-artifacts --keep-days 30"
```

## 6. Rotate logs on VPS

```bash
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote rotate-logs"
```

## 7. Delete temp files on VPS

```bash
ssh vps-madrid "rm -f /root/trading_pm_data_feed/data/*.csv"
```

## 8. Verify pipelines are healthy

```bash
# Check accumulator is running
python -m utils.local tail-log accumulate_1m --lines 5

# Check artifact build ran today
python -m utils.local list-artifacts

# Check for gaps
python -m utils.local tail-log repair_gaps_1m --lines 5
```
