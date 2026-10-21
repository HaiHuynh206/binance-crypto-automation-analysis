from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyarrow")

from crypto_ingest import pipeline
from crypto_ingest.config import Settings
from crypto_ingest.pipeline import IngestError, ingest_window
from crypto_ingest.state import CompletionStore


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


def dt(hour: int) -> datetime:
    return datetime(2026, 10, 21, hour, tzinfo=timezone.utc)


def make_row(hour: datetime) -> dict:
    return {
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


def test_window_is_idempotent_and_refreshes_last_hour(tmp_path, monkeypatch):
    calls: list[datetime] = []

    def fake_fetch(client, symbol, start, end):
        calls.append(start)
        return [make_row(start)]

    monkeypatch.setitem(pipeline.FETCHERS, "klines_1m", fake_fetch)
    cfg = settings(tmp_path)

    first = ingest_window(cfg, "klines_1m", dt(0), dt(4), now=dt(5))
    assert first.partitions_written == 4
    assert first.rows == 4
    assert len(calls) == 4

    calls.clear()
    second = ingest_window(cfg, "klines_1m", dt(0), dt(4), now=dt(5))
    assert second.partitions_skipped == 3
    assert second.partitions_written == 1
    assert calls == [dt(3)]

    calls.clear()
    third = ingest_window(cfg, "klines_1m", dt(0), dt(4), now=dt(5), force=True)
    assert third.partitions_written == 4
    assert len(calls) == 4


def test_failure_keeps_successful_partitions(tmp_path, monkeypatch):
    def flaky_fetch(client, symbol, start, end):
        if start == dt(2):
            raise RuntimeError("boom")
        return [make_row(start)]

    monkeypatch.setitem(pipeline.FETCHERS, "klines_1m", flaky_fetch)
    cfg = settings(tmp_path)

    with pytest.raises(IngestError) as excinfo:
        ingest_window(cfg, "klines_1m", dt(0), dt(4), now=dt(5))

    result = excinfo.value.result
    assert result.partitions_written == 3
    assert len(result.errors) == 1
    state = CompletionStore(cfg)
    assert state.is_done("klines_1m", "BTCUSDT", dt(0))
    assert not state.is_done("klines_1m", "BTCUSDT", dt(2))
