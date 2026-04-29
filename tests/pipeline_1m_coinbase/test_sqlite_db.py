"""Tests for cex_data_feed.pipeline_1m_coinbase.sqlite_db."""
import pandas as pd
import pytest

from cex_data_feed.pipeline_1m_coinbase.sqlite_db import (
    ensure_table,
    insert_candles,
    read_last_n,
    coverage_stats,
    find_first_gap,
)


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.sqlite"


def _make_df(timestamps, base_price=100.0):
    rows = []
    for i, ts in enumerate(timestamps):
        rows.append({
            "timestamp": pd.Timestamp(ts),
            "open": base_price + i,
            "high": base_price + i + 1,
            "low": base_price + i - 1,
            "close": base_price + i + 0.5,
            "volume": 1000.0 + i,
        })
    return pd.DataFrame(rows)


class TestEnsureTable:
    def test_creates_table(self, db_path):
        ensure_table(db_path)
        assert db_path.exists()

    def test_idempotent(self, db_path):
        ensure_table(db_path)
        ensure_table(db_path)


class TestInsertCandles:
    def test_insert_rows(self, db_path):
        ensure_table(db_path)
        df = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
        assert insert_candles(db_path, df) == 2

    def test_duplicate_timestamps_allowed(self, db_path):
        ensure_table(db_path)
        df = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
        insert_candles(db_path, df)
        assert insert_candles(db_path, df) == 2

    def test_empty_df(self, db_path):
        ensure_table(db_path)
        assert insert_candles(db_path, pd.DataFrame()) == 0


class TestReadLastN:
    def test_returns_correct_count(self, db_path):
        ensure_table(db_path)
        df = _make_df([
            "2024-01-01 00:00:00",
            "2024-01-01 00:01:00",
            "2024-01-01 00:02:00",
        ])
        insert_candles(db_path, df)
        assert len(read_last_n(db_path, 2)) == 2

    def test_ascending_order(self, db_path):
        ensure_table(db_path)
        df = _make_df([
            "2024-01-01 00:00:00",
            "2024-01-01 00:01:00",
            "2024-01-01 00:02:00",
        ])
        insert_candles(db_path, df)
        result = read_last_n(db_path, 2)
        assert result["timestamp"].iloc[0] < result["timestamp"].iloc[1]

    def test_dedup_takes_latest_ingested(self, db_path):
        ensure_table(db_path)
        insert_candles(db_path, _make_df(["2024-01-01 00:00:00"], base_price=100.0))
        insert_candles(db_path, _make_df(["2024-01-01 00:00:00"], base_price=200.0))
        result = read_last_n(db_path, 1)
        assert len(result) == 1
        assert result["open"].iloc[0] == 200.0

    def test_empty_db(self, db_path):
        ensure_table(db_path)
        assert len(read_last_n(db_path, 5)) == 0


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
        insert_candles(db_path, df)
        min_ts, max_ts, count = coverage_stats(db_path)
        assert count == 3
        assert min_ts == pd.Timestamp("2024-01-01 00:00:00")
        assert max_ts == pd.Timestamp("2024-01-01 00:02:00")


class TestFindFirstGap:
    def test_no_gap(self, db_path):
        ensure_table(db_path)
        df = _make_df([
            "2024-01-01 00:00:00",
            "2024-01-01 00:01:00",
            "2024-01-01 00:02:00",
        ])
        insert_candles(db_path, df)
        assert find_first_gap(db_path) is None

    def test_internal_gap(self, db_path):
        ensure_table(db_path)
        df = _make_df([
            "2024-01-01 00:00:00",
            "2024-01-01 00:01:00",
            "2024-01-01 00:03:00",  # 00:02 missing
        ])
        insert_candles(db_path, df)
        gap = find_first_gap(db_path)
        assert gap == pd.Timestamp("2024-01-01 00:02:00")
