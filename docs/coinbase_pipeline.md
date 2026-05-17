# Coinbase 1m BTC-USD pipeline

Sibling to the Binance 1m pipeline. Pulls BTC-USD 1m candles from Coinbase
Exchange's public REST endpoint (`api.exchange.coinbase.com`, no auth).

- **REST client:** `cex_data_feed/coinbase/api.py`
- **Pipeline:** `cex_data_feed/pipeline_1m_coinbase/`
- **Scripts:** `cex_data_feed/scripts/coinbase_{accumulate,repair_gaps,backfill}_1m.py`
- **DB:** `data/btcusd_coinbase_1m.sqlite`, table `ohlcv_btcusd_coinbase_1m`
- **Schema:** `timestamp, open, high, low, close, volume, ingested_at`
  (slim — Coinbase's candle endpoint exposes nothing else)

## Coinbase API quirks vs. Binance

| | Binance FAPI | Coinbase Exchange |
|---|---|---|
| Auth required | No | No |
| Rate limit | ~weight-based | 3 req/s by IP |
| Max candles/call | 1500 | **300** |
| Pagination model | `startTime` + `limit` | `[start, end]` window |
| Volume fields | quote vol, trade count, taker breakdown | volume only |
| No-tick minutes | Forward-filled | **Not published** |

The 300-row cap is why `fetch_closed_1m_since` walks forward in 5h windows.

## Known gaps (BTC-USD, 2025-04-28 → 2026-05-15)

Two major Coinbase Exchange outages (2025-10-25 and 2026-05-08) plus ~70
scattered single-tick-less minutes. **All confirmed real Coinbase-side gaps**
— probing the live API returns no candles. Not fixable via REST.

| Start (UTC) | End (UTC) | Minutes |
|---|---|---|
| 2025-10-20 08:11 | 2025-10-20 08:11 | 1 |
| 2025-10-20 09:16 | 2025-10-20 09:17 | 2 |
| 2025-10-23 04:38 | 2025-10-23 04:38 | 1 |
| 2025-10-25 14:48 | 2025-10-25 14:49 | 2 |
| 2025-10-25 14:51 | 2025-10-25 14:52 | 2 |
| 2025-10-25 14:55 | 2025-10-25 15:00 | 6 |
| 2025-10-25 15:06 | 2025-10-25 15:06 | 1 |
| **2025-10-25 15:14** | **2025-10-25 21:02** | **349** |
| 2025-10-25 21:11 | 2025-10-25 21:11 | 1 |
| 2025-12-12 15:32 | 2025-12-12 15:33 | 2 |
| 2026-02-03 17:49 | 2026-02-03 17:49 | 1 |
| 2026-02-17 19:05 | 2026-02-17 19:05 | 1 |
| 2026-02-17 20:06 | 2026-02-17 20:09 | 4 |
| 2026-02-18 01:52 | 2026-02-18 01:53 | 2 |
| 2026-02-19 22:20 | 2026-02-19 22:24 | 5 |
| 2026-02-20 23:43 | 2026-02-20 23:43 | 1 |
| 2026-02-22 22:22 | 2026-02-22 22:23 | 2 |
| 2026-02-24 01:07 | 2026-02-24 01:08 | 2 |
| 2026-02-24 03:53 | 2026-02-24 03:56 | 4 |
| 2026-02-24 04:13 | 2026-02-24 04:13 | 1 |
| 2026-02-25 23:23 | 2026-02-25 23:23 | 1 |
| 2026-02-26 17:46 | 2026-02-26 17:48 | 3 |
| 2026-03-05 17:36 | 2026-03-05 17:36 | 1 |
| 2026-03-11 22:13 | 2026-03-11 22:13 | 1 |
| 2026-03-11 23:46 | 2026-03-11 23:47 | 2 |
| 2026-03-12 00:20 | 2026-03-12 00:20 | 1 |
| 2026-03-12 17:48 | 2026-03-12 17:48 | 1 |
| 2026-03-17 00:36 | 2026-03-17 00:36 | 1 |
| 2026-04-25 05:28 | 2026-04-25 05:30 | 3 |
| 2026-04-25 12:33 | 2026-04-25 12:33 | 1 |
| **2026-05-08 01:17** | **2026-05-08 07:47** | **391** |
| _(plus ~11 min of micro-gaps in May, not individually enumerated)_ | | |

**Two Coinbase Exchange outages dominate:** the 349-min one on 2025-10-25
and the 391-min one on 2026-05-08. Together they account for ~94% of all
missing minutes. The rest are single-tick-less minutes that Coinbase's
candle endpoint omits (unlike Binance, which forward-fills empty bars).

Coverage as of 2026-05-15: **~99.91%** (≈550,000 distinct timestamps).
To regenerate an up-to-date gap table, run a scan against the latest DB
snapshot (`SELECT DISTINCT timestamp ... ORDER BY` and diff against
`pd.date_range(min, max, freq="1min")`).

## Downstream handling

If your model needs a continuous 1-min grid:

```python
df = df.set_index("timestamp").reindex(
    pd.date_range(df.timestamp.min(), df.timestamp.max(), freq="1min")
)
df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].ffill()
df["volume"] = df["volume"].fillna(0.0)
```

This is a feature-engineering concern, not a pipeline concern — the DB
faithfully stores what Coinbase returns.

## ⚠️ `repair_gaps` caveat

`find_first_gap()` returns the first missing minute, then `fetch_closed_1m_since`
paginates from there to **now**. Because the first gap is on 2025-10-20, every
run of `coinbase_repair_gaps_1m` would re-fetch ~6+ months of candles trying to
fill an unfillable gap.

**Recommended:** don't run `coinbase_repair_gaps_1m` on a cron. Use only:
- `coinbase_accumulate_1m` (every 5 min) — handles trailing gaps cheaply
- `coinbase_repair_gaps_1m` **manually**, after a known accumulator outage

A future improvement (if needed): add a known-gaps allow-list so the repair
script skips unfillable timestamps.
