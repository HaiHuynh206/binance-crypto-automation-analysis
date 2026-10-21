from __future__ import annotations

from datetime import datetime, timezone

from crypto_ingest.schemas import dedupe_and_sort


def ts(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 10, 21, hour, minute, second, tzinfo=timezone.utc)


def test_dedupe_keeps_first_occurrence_and_sorts():
    rows = [
        {"open_time": ts(1), "close": 2.0},
        {"open_time": ts(0), "close": 1.0},
        {"open_time": ts(1), "close": 99.0},
    ]
    result = dedupe_and_sort("klines_1m", rows)
    assert [row["open_time"] for row in result] == [ts(0), ts(1)]
    assert result[1]["close"] == 2.0


def test_agg_trades_dedupe_uses_trade_id():
    rows = [
        {"agg_trade_id": 10, "price": 1.0},
        {"agg_trade_id": 9, "price": 2.0},
        {"agg_trade_id": 10, "price": 3.0},
    ]
    result = dedupe_and_sort("agg_trades", rows)
    assert [row["agg_trade_id"] for row in result] == [9, 10]
    assert result[1]["price"] == 1.0
