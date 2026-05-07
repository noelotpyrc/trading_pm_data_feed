# Discord Ops Memo — Channel Parsing Reference

Companion to `ops_memo.md`. Documents the message patterns that arrive in each Discord channel and how to parse them when reading via the Discord REST API. For producing-stream definitions (which tmux session/cron writes what), see `ops_memo.md`.

## Channels

All in Discord server `1465846881836863796`.

| Env var | Channel | Channel ID | Used by |
|---|---|---|---|
| `DISCORD_WEBHOOK_URL` | `#trading-signals` | `1492314707372150814` | `signal`, `liq_collector` |
| `DISCORD_WEBHOOK_URL_PM` | `#pm-price-alert` | `1492688541405413396` | `pm_collector` |
| `DISCORD_WEBHOOK_URL_PM_DUAL` | `#pm-dual-price` | `1493441980968075488` | `pm_dual` |
| `DISCORD_WEBHOOK_URL_SIGNAL_V3` | `#pm-trading-signals` | `1494415390875320330` | `signal-v3-contrarian`, `signal-v3-dir` |

A channel can receive messages from multiple streams (e.g. `#trading-signals` carries both the signal engine and the liq collector). Disambiguate by **content pattern**, not by webhook author — the same webhook formatter produces all message types within one channel.

---

## `#trading-signals`

Two streams; three distinct message types. The leading emoji uniquely identifies each type, so categorization is a simple substring check.

### Type A — Trading signal (LONG / SHORT)

- **Source:** `btcusdt_perp_signal.scripts.run_signal_engine` (tmux `signal`)
- **Cadence:** ad-hoc — fires whenever feature thresholds match on a freshly-closed 1m bar
- **Marker:** content starts with `🟢 **BTCUSDT LONG Signal**` or `🔴 **BTCUSDT SHORT Signal**`

Example:
```
🟢 **BTCUSDT LONG Signal**
Time: `2026-05-02 21:39:00`

**Trigger bar:**
  O: `78963.8` H: `78979.1` L: `78859.5` C: `78863.1`
  Vol: `370.45` Trades: `5353`

**Features:**
  parkinson_ratio: `4.1339`
  volume_ratio_30: `1.7387`
  volume_ratio_60: `3.2422`
  avg_trade_size_zscore_60: `2.1268`
  vwap_60_cross_rate_90: `20.0000`
  cum_return_5bar_norm_p30: `5.0897`
  efficiency_ratio_360: `0.1319`
  parkinson_30: `0.0009`
```

Fields:
- **Direction** — emoji and bold text. 🟢 = LONG, 🔴 = SHORT.
- **Time** — UTC timestamp marking the **start** of the 1m trigger bar (the bar that just closed and fired the signal). The Discord `timestamp` field on the message is ~1 minute later (bar must close + processing).
- **Trigger bar** — raw 1m OHLCV: O, H, L, C, Vol (BTC), Trades (count).
- **Features** — 8 computed indicators, names stable, values floats wrapped in backticks.

Parse rule: `"LONG Signal" in content` → LONG; `"SHORT Signal" in content` → SHORT.

### Type B — Individual liquidation event

- **Source:** `cex_data_feed.scripts.collect_liquidations` (tmux `liq_collector`)
- **Cadence:** real-time, one message per Binance forceOrder event
- **Marker:** content starts with `💥 **BTCUSDT BUY Side Liquidation**` or `💥 **BTCUSDT SELL Side Liquidation**`

Example:
```
💥 **BTCUSDT BUY Side Liquidation**
Qty: `1.000` | Filled: `1.000`
Price: `79013.80` | Avg Price: `78709.10`
Status: `FILLED` | TIF: `IOC`
Time: `2026-05-03 13:02:33 UTC`
```

Fields:
- **Side** — `BUY` = short liquidation (forced cover); `SELL` = long liquidation (forced sell).
- **Qty / Filled** — both in BTC. Qty = order size, Filled = executed amount (usually equal for forceOrder).
- **Price** — limit price of the liquidation order. **Avg Price** — VWAP of the actual fill (often diverges from limit on fast moves).
- **Status / TIF** — `FILLED` + `IOC` (immediate-or-cancel) is the typical/only combination from Binance forceOrder.
- **Time** — UTC timestamp of the event; aligns closely with Discord `timestamp`.

Parse rule: `"Side Liquidation" in content` → individual event; side from `"BUY Side"` vs `"SELL Side"` substring.

### Type C — 15-min liquidation heartbeat

- **Source:** `cex_data_feed.scripts.collect_liquidations` (tmux `liq_collector`), heartbeat formatter
- **Cadence:** every 15 minutes, regardless of activity (zero-event heartbeats are still emitted — useful as a liveness check)
- **Marker:** content starts with `💓 **BTCUSDT Liq Heartbeat (15m)**`

Example:
```
💓 **BTCUSDT Liq Heartbeat (15m)**
BUY (short liq):  `2` events, `0.024` BTC
SELL (long liq):  `0` events, `0.000` BTC
Total: `2` events, `0.024` BTC
Time: `2026-05-03 19:53:15 UTC`
```

Fields:
- **BUY / SELL** — per-side aggregate over the past 15 min: event count and total BTC liquidated.
- **Total** — sum across both sides.
- **Time** — UTC timestamp of the heartbeat tick.

Parse rule: `"Liq Heartbeat" in content`.

### Categorization order

When classifying a batch of messages, check in this order to avoid false positives:
1. `"Liq Heartbeat" in content` → Type C
2. `"Side Liquidation" in content` → Type B
3. `"LONG Signal"` / `"SHORT Signal" in content` → Type A

A naive `"Liquidation" in content` check would match both Type B and Type C.

---

## `#pm-price-alert`

TBD.

## `#pm-dual-price`

TBD.

## `#pm-trading-signals`

TBD.
