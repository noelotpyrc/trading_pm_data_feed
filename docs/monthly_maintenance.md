# Monthly Maintenance

Manual checklist for VPS cleanup and local backup, run at the start of each month.

> **Stopped streams (2026-05-29):** `pm_dual`, `signal-v3-contrarian`, `signal-v3-dir` (tmux) and the v1 `build_daily_artifact` cron were stopped — see [`ops_memo.md` changelog](ops_memo.md). Their SSD folders (`pm_dual/`, `pm_btc15updown_artifact/`) are now **frozen**: no new daily files after that date, so it's expected they stop growing while `btc_depth/`, `pm_btcupdown/`, `liquidations/`, and `pm_btc15updown_artifact_v3/` keep advancing. The v3 artifact cron stays running (feeds the downstream live trading system).

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
├── btc_depth/        (daily JSONL snapshots from collect_btc_depth)
│   └── depth_YYYY-MM-DD.jsonl
├── pm_btcupdown/     (daily JSONL snapshots from collect_pm_btcupdown)
│   └── pm_btcupdown_YYYY-MM-DD.jsonl
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

## 3. Backup daily JSONL streams to SSD

Only completed days (never today's live file). Pattern: rsync with an exclude for today's date.

```bash
TODAY=$(date -u +%Y-%m-%d)

# BTC order book depth (15 MB/day)
rsync -av --exclude="*_${TODAY}.jsonl" \
  vps-madrid:/root/trading_pm_data_feed/data/btc_depth/ \
  "/Volumes/Extreme SSD/vps_madrid_backup/btc_depth/"

# Polymarket btcupdown single-market (7 MB/day)
rsync -av --exclude="*_${TODAY}.jsonl" \
  vps-madrid:/root/trading_pm_data_feed/data/pm_btcupdown/ \
  "/Volumes/Extreme SSD/vps_madrid_backup/pm_btcupdown/"
```

Verify before deleting on VPS — spot-check the newest archived file:

```bash
DAY=$(date -u -v-1d +%Y-%m-%d)  # yesterday
shasum -a 256 "/Volumes/Extreme SSD/vps_madrid_backup/btc_depth/depth_${DAY}.jsonl"
ssh vps-madrid "sha256sum /root/trading_pm_data_feed/data/btc_depth/depth_${DAY}.jsonl"
```

Then delete archived days on VPS (keep today's live file):

```bash
ssh vps-madrid "find /root/trading_pm_data_feed/data/btc_depth     -name 'depth_*.jsonl'        ! -name '*_${TODAY}.jsonl' -delete"
ssh vps-madrid "find /root/trading_pm_data_feed/data/pm_btcupdown  -name 'pm_btcupdown_*.jsonl' ! -name '*_${TODAY}.jsonl' -delete"
```

## 4. Backup logs to SSD

```bash
scp vps-madrid:/root/trading_pm_data_feed/data/accumulate_1m.log \
  "/Volumes/Extreme SSD/vps_madrid_backup/cex_data_feed/logs/"
scp vps-madrid:/root/trading_pm_data_feed/data/repair_gaps_1m.log \
  "/Volumes/Extreme SSD/vps_madrid_backup/cex_data_feed/logs/"
scp vps-madrid:/root/trading_pm_data_feed/data/build_daily_artifact.log \
  "/Volumes/Extreme SSD/vps_madrid_backup/pm_btc15updown_artifact/logs/"
```

## 5. Check VPS disk and DB status

```bash
python -m utils.local db-stats
ssh vps-madrid "df -h /root/trading_pm_data_feed"
```

## 6. Cleanup old artifacts on VPS

Keep last 30 days, preview first:

```bash
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote cleanup-artifacts --keep-days 30 --dry-run"
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote cleanup-artifacts --keep-days 30"
```

## 7. Rotate logs on VPS

```bash
ssh vps-madrid "cd /root/trading_pm_data_feed && .venv/bin/python -m utils.remote rotate-logs"
```

## 8. Delete temp files on VPS

```bash
ssh vps-madrid "rm -f /root/trading_pm_data_feed/data/*.csv"
```

## 9. Verify pipelines are healthy

```bash
# Check accumulator is running
python -m utils.local tail-log accumulate_1m --lines 5

# Check artifact build ran today
python -m utils.local list-artifacts

# Check for gaps
python -m utils.local tail-log repair_gaps_1m --lines 5
```

## 10. Review external data source changelogs

Sweep each vendor's changelog since the last "Last verified" date in [`ops_memo.md` → External data sources](ops_memo.md#external-data-sources). Watch lists:

- [Binance Derivatives Change Log](https://developers.binance.com/docs/derivatives/change-log)
- [Polymarket Changelog](https://docs.polymarket.com/changelog)
- [Coinbase Exchange API changelog](https://docs.cdp.coinbase.com/exchange/docs/changelog)

Look for: deprecation cutovers, URL/path changes, auth or subscription changes, payload format changes. Bump the "Last verified" column in `ops_memo.md` when you confirm each endpoint is still healthy. If a streaming session's output file hasn't grown in >24h despite the process being alive, suspect a vendor change first.

## 11. Check for patched kernel (CVE-2026-31431) — temporary, until done

Vultr advisory disclosed 2026-04-29 ("Copy Fail"). Mitigation already in place: `/etc/modprobe.d/cve-2026-31431.conf` on the VPS blacklists `algif_aead` so the vulnerable code path can't auto-load. A reboot into a properly-patched kernel is still pending — no kernel newer than `6.8.0-111` (released 2026-04-11, pre-disclosure) had reached our apt mirror at last check.

Each monthly run, recheck:

```bash
ssh vps-madrid "apt update && apt list --upgradable 2>/dev/null | grep -E 'linux-(image|generic|headers|cloud)' || echo '(no kernel upgrades pending)'"
```

If a kernel newer than `6.8.0-111` appears, schedule a reboot:

1. Run Step 1 above first (fresh DB snapshot to SSD) so we have a known-good restore point.
2. `ssh vps-madrid "apt install -y linux-image-generic && reboot"`.
3. After ~1 min, reconnect. Cron auto-resumes; **manually relaunch the 4 active tmux sessions** (`signal`, `btc_depth`, `liq_collector`, `pm_collector`) per [`ops_memo.md`](ops_memo.md). Each session has its full launch command in that file. (The stopped V3/`pm_dual` sessions are intentionally not relaunched — see the stopped-streams note at the top.)
4. Verify pipelines are writing again — file mtimes in `data/btc_depth/`, `data/pm_btcupdown/`, etc. should be within a few minutes of "now".

Once the upgrade + reboot is done and pipelines are healthy, **delete this section**.
