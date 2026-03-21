"""Tests for cex_data_feed.pipeline_1m.sqlite_db."""
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from cex_data_feed.pipeline_1m.sqlite_db import (
    ensure_table,
    upsert_candles,
    read_last_n,
    coverage_stats,
)


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.sqlite"


def _make_df(timestamps, base_price=100.0):
    """Helper: create a minimal OHLCV DataFrame."""
    rows = []
    for i, ts in enumerate(timestamps):
        rows.append({
            "timestamp": pd.Timestamp(ts),
            "open": base_price + i,
            "high": base_price + i + 1,
            "low": base_price + i - 1,
            "close": base_price + i + 0.5,
            "volume": 1000.0 + i,
            "quote_asset_volume": 50000.0 + i,
            "num_trades": 100 + i,
            "taker_buy_base_volume": 500.0 + i,
            "taker_buy_quote_volume": 25000.0 + i,
        })
    return pd.DataFrame(rows)


class TestEnsureTable:
    def test_creates_table(self, db_path):
        ensure_table(db_path)
        assert db_path.exists()

    def test_idempotent(self, db_path):
        ensure_table(db_path)
        ensure_table(db_path)  # should not raise


class TestUpsertCandles:
    def test_insert_rows(self, db_path):
        ensure_table(db_path)
        df = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
        inserted = upsert_candles(db_path, df)
        assert inserted == 2

    def test_duplicate_ignored(self, db_path):
        ensure_table(db_path)
        df = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
        upsert_candles(db_path, df)
        inserted = upsert_candles(db_path, df)
        assert inserted == 0

    def test_empty_df(self, db_path):
        ensure_table(db_path)
        df = pd.DataFrame()
        assert upsert_candles(db_path, df) == 0

    def test_partial_overlap(self, db_path):
        ensure_table(db_path)
        df1 = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
        df2 = _make_df(["2024-01-01 00:01:00", "2024-01-01 00:02:00"])
        upsert_candles(db_path, df1)
        inserted = upsert_candles(db_path, df2)
        assert inserted == 1  # only 00:02 is new


class TestReadLastN:
    def test_returns_correct_count(self, db_path):
        ensure_table(db_path)
        df = _make_df([
            "2024-01-01 00:00:00",
            "2024-01-01 00:01:00",
            "2024-01-01 00:02:00",
        ])
        upsert_candles(db_path, df)
        result = read_last_n(db_path, 2)
        assert len(result) == 2

    def test_ascending_order(self, db_path):
        ensure_table(db_path)
        df = _make_df([
            "2024-01-01 00:00:00",
            "2024-01-01 00:01:00",
            "2024-01-01 00:02:00",
        ])
        upsert_candles(db_path, df)
        result = read_last_n(db_path, 2)
        assert result["timestamp"].iloc[0] < result["timestamp"].iloc[1]

    def test_empty_db(self, db_path):
        ensure_table(db_path)
        result = read_last_n(db_path, 5)
        assert len(result) == 0


class TestCoverageStats:
    def test_empty_db(self, db_path):
        ensure_table(db_path)
        assert coverage_stats(db_path) is None

    def test_with_data(self, db_path):
        ensure_table(db_path)
        df = _make_df([
            "2024-01-01 00:00:00",
            "2024-01-01 00:01:00",
            "2024-01-01 00:02:00",
        ])
        upsert_candles(db_path, df)
        min_ts, max_ts, count = coverage_stats(db_path)
        assert count == 3
        assert min_ts == pd.Timestamp("2024-01-01 00:00:00")
        assert max_ts == pd.Timestamp("2024-01-01 00:02:00")
