"""
1m BTCUSDT Perp data accumulator pipeline.

Two-phase workflow:
  1. Initial backfill: download Binance historical kline ZIPs → merge CSV →
     import into SQLite via backfill_1m_from_csv.py
  2. Periodic accumulation: run accumulate_1m.py on a cron/timer to keep
     the SQLite DB current with recently closed 1m candles from the API.
"""
