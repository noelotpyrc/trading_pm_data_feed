#!/bin/bash
# vps-madrid watchdog — runs on this Mac, polls the VPS over Tailscale.
#
# Deliberately off-box: a monitor on the VPS can't alert when the VPS is the
# thing that broke (see the 2026-07-28 DNS/reboot incident).
#
# Checks: SSH reachable, public DNS resolves, expected tmux sessions alive,
# OHLCV DBs and JSONL streams still advancing.
# Alerts to Discord #pm-dual-price on state CHANGE only (no repeat spam).
#
# Setup: put the webhook URL in ~/.config/vps-watchdog/webhook (chmod 600).

set -uo pipefail

HOST="vps-madrid"
CONF_DIR="$HOME/.config/vps-watchdog"
WEBHOOK_FILE="$CONF_DIR/webhook"
STATE_FILE="$CONF_DIR/state"    # last state we alerted on
OBS_FILE="$CONF_DIR/observed"   # last observed state + consecutive count
LOG_FILE="$CONF_DIR/watchdog.log"

# Require the same result twice in a row before alerting. A laptop poller sees
# transient blips (sleep/wake, wifi switch, a hiccuped ssh); without this they
# become false alarms. Costs one extra interval of detection latency.
CONFIRM_RUNS=2

# Sessions that must be running. pm_signal_sim is intentionally NOT listed —
# it's being reworked; add it back here when redeployed.
EXPECTED_SESSIONS=(signal btc_depth liq_collector pm_collector)

# Staleness limits (seconds)
DB_MAX_AGE=900       # cron accumulators run every 5 min
STREAM_MAX_AGE=300   # btc_depth / pm_collector write at 1s cadence

mkdir -p "$CONF_DIR"
log() { printf '[%s] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" "$*" >>"$LOG_FILE"; }

# --- probe the VPS in a single round trip -----------------------------------
REMOTE_OUT=$(ssh -o ConnectTimeout=15 -o BatchMode=yes "$HOST" bash -s <<'REMOTE' 2>/dev/null
cd /root/trading_pm_data_feed/data || { echo "err=nodatadir"; exit 0; }
now=$(date -u +%s)

getent hosts fapi.binance.com >/dev/null 2>&1 && echo "dns=ok" || echo "dns=fail"

echo "tmux=$(tmux ls 2>/dev/null | cut -d: -f1 | paste -sd, -)"

# Newest file per stream dir (not today's by name) so UTC midnight rollover
# doesn't read as a failure.
for pair in "depth:btc_depth" "pmbook:pm_btcupdown"; do
  label=${pair%%:*}; dir=${pair##*:}
  newest=$(ls -t "$dir"/*.jsonl 2>/dev/null | head -1)
  if [ -n "$newest" ]; then
    echo "${label}_age=$(( now - $(stat -c%Y "$newest") ))"
  else
    echo "${label}_age=-1"
  fi
done

/root/trading_pm_data_feed/.venv/bin/python - <<'PY'
import sqlite3, datetime
D = "/root/trading_pm_data_feed/data/"
targets = [("perp", D + "btcusdt_perp_1m.sqlite", "ohlcv_btcusdt_1m"),
           ("coinbase", D + "btcusd_coinbase_1m.sqlite", "ohlcv_btcusd_coinbase_1m")]
for label, db, tbl in targets:
    try:
        c = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        m = c.execute(f"select max(timestamp) from {tbl}").fetchone()[0]
        age = (datetime.datetime.utcnow()
               - datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S")).total_seconds()
        print(f"{label}_age={int(age)}")
    except Exception:
        print(f"{label}_age=-1")
PY
REMOTE
)
SSH_RC=$?

# --- evaluate ---------------------------------------------------------------
problems=()

if [ $SSH_RC -ne 0 ] || [ -z "$REMOTE_OUT" ]; then
  problems+=("SSH unreachable (rc=$SSH_RC) — VPS down, network, or tailnet issue")
else
  get() { echo "$REMOTE_OUT" | grep "^$1=" | head -1 | cut -d= -f2-; }

  [ "$(get dns)" = "ok" ] || problems+=("DNS broken — fapi.binance.com does not resolve")

  running=",$(get tmux),"
  for s in "${EXPECTED_SESSIONS[@]}"; do
    [[ "$running" == *",$s,"* ]] || problems+=("tmux session down: $s")
  done

  check_age() {  # label human max
    local age; age=$(get "${1}_age")
    [ -z "$age" ] && { problems+=("$2: no reading"); return; }
    if [ "$age" -lt 0 ]; then
      problems+=("$2: no data found")
    elif [ "$age" -gt "$3" ]; then
      problems+=("$2: stale — last write $((age / 60))m ago")
    fi
  }
  check_age perp     "perp OHLCV DB"      "$DB_MAX_AGE"
  check_age coinbase "coinbase OHLCV DB"  "$DB_MAX_AGE"
  check_age depth    "btc_depth stream"   "$STREAM_MAX_AGE"
  check_age pmbook   "pm_btcupdown stream" "$STREAM_MAX_AGE"
fi

# --- alert on state change only ---------------------------------------------
if [ ${#problems[@]} -eq 0 ]; then
  current="OK"
  message="✅ vps-madrid recovered — all checks passing."
else
  current=$(printf '%s\n' "${problems[@]}")
  message="🚨 **vps-madrid pipeline alert**"$'\n'"$(printf '• %s\n' "${problems[@]}")"
fi

# Debounce: count consecutive identical observations.
prev_obs=$(sed '$d' "$OBS_FILE" 2>/dev/null)
prev_count=$(tail -1 "$OBS_FILE" 2>/dev/null)
[[ "$prev_count" =~ ^[0-9]+$ ]] || prev_count=0
if [ "$current" = "$prev_obs" ]; then count=$((prev_count + 1)); else count=1; fi
printf '%s\n%s' "$current" "$count" >"$OBS_FILE"

if [ "$count" -lt "$CONFIRM_RUNS" ]; then
  log "unconfirmed (${#problems[@]} problem(s), seen ${count}/${CONFIRM_RUNS}) — waiting"
  exit 0
fi

previous=$(cat "$STATE_FILE" 2>/dev/null)
if [ "$current" = "$previous" ]; then
  log "no change (${#problems[@]} problem(s))"
  exit 0
fi
printf '%s' "$current" >"$STATE_FILE"

# Don't announce "recovered" on the very first run.
if [ ${#problems[@]} -eq 0 ] && [ -z "$previous" ]; then
  log "first run: healthy"
  exit 0
fi

log "state change -> ${current//$'\n'/; }"

if [ ! -r "$WEBHOOK_FILE" ]; then
  log "ERROR: no webhook at $WEBHOOK_FILE — alert not sent"
  exit 1
fi
payload=$(MSG="$message" python3 -c 'import json,os; print(json.dumps({"content": os.environ["MSG"]}))')
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 \
  -H 'Content-Type: application/json' -d "$payload" "$(cat "$WEBHOOK_FILE")")
log "discord POST http $code"
