from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyarrow")

from crypto_ingest.config import Settings
from crypto_ingest.storage import ParquetLake


def settings(tmp_path) -> Settings:
    return Settings(
        data_root=tmp_path,
        symbols=("BTCUSDT",),
        datasets=("klines_1m",),
        spot_base_url="https://api.binance.com",
        futures_base_url="https://fapi.binance.com",
        timeout_seconds=5,
        max_retries=1,
        max_pages=10,
        weight_per_minute=100,
        lookback_hours=1,
    )


def test_partition_layout_is_hive_compatible(tmp_path):
    lake = ParquetLake(settings(tmp_path))
    path = lake.partition_path(
        "klines_1m", "BTCUSDT", datetime(2026, 10, 21, 7, tzinfo=timezone.utc)
    )
    assert path.as_posix().endswith(
        "lake/exchange=binance/dataset=klines_1m/symbol=BTCUSDT/"
        "date=2026-10-21/hour=07/part-00000.parquet"
    )


def test_write_then_read_roundtrip(tmp_path):
    lake = ParquetLake(settings(tmp_path))
    hour = datetime(2026, 10, 21, 7, tzinfo=timezone.utc)
    rows = [
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "open_time": hour,
            "close_time": hour,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "quote_volume": 15.0,
            "trades": 3,
            "taker_buy_base": 4.0,
            "taker_buy_quote": 6.0,
            "ingested_at": hour,
        }
    ]
    path = lake.write("klines_1m", "BTCUSDT", hour, rows)
    assert path is not None and path.exists()
    table = lake.read("klines_1m", "BTCUSDT", hour)
    assert table is not None and table.num_rows == 1
    assert table.to_pylist()[0]["close"] == 1.5


def test_empty_batch_does_not_create_a_file(tmp_path):
    lake = ParquetLake(settings(tmp_path))
    hour = datetime(2026, 10, 21, 7, tzinfo=timezone.utc)
    assert lake.write("funding_rate", "BTCUSDT", hour, []) is None
    assert not lake.partition_path("funding_rate", "BTCUSDT", hour).exists()
