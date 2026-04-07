# Monthly Maintenance

Manual checklist for VPS cleanup and local backup, run at the start of each month.

## 1. Backup DB to local

```bash
python -m utils.local pull-db --out data/backups/btcusdt_perp_1m_$(date -u +%Y-%m).sqlite
```

## 2. Backup artifacts to local

```bash
rsync -az vps-madrid:/root/trading_pm_data_feed/data/artifacts/ data/backups/artifacts/
```

## 3. Check VPS disk and DB status

```bash
python -m utils.local db-stats
ssh vps-madrid "df -h /root/trading_pm_data_feed"
```

## 4. Cleanup old artifacts on VPS

Keep last 30 days, preview first:

```bash
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote cleanup-artifacts --keep-days 30 --dry-run"
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote cleanup-artifacts --keep-days 30"
```

## 5. Rotate logs on VPS

```bash
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote rotate-logs"
```

## 6. Delete temp files on VPS

```bash
ssh vps-madrid "rm -f /root/trading_pm_data_feed/data/*.csv"
```

## 7. Verify pipelines are healthy

```bash
# Check accumulator is running
python -m utils.local tail-log accumulate_1m --lines 5

# Check artifact build ran today
python -m utils.local list-artifacts

# Check for gaps
python -m utils.local tail-log repair_gaps_1m --lines 5
```
