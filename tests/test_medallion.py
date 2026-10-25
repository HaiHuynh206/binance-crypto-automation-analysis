from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyarrow")
duckdb = pytest.importorskip("duckdb")

from crypto_ingest.config import Settings
from crypto_ingest.medallion import (
    DQ_RULES,
    SILVER_SPECS,
    DataQualityError,
    Layout,
    build_catalog_layers,
    build_gold,
    build_silver,
    iter_layer_files,
    layer_summary,
    run_dq,
    silver_datasets_for,
)
from crypto_ingest.schemas import MIN_ROWS_PER_HOUR
from crypto_ingest.storage import ParquetLake

DAY = datetime(2026, 10, 20, tzinfo=timezone.utc)
END = DAY + timedelta(days=1)
NOW = DAY + timedelta(days=3)  # every hour of DAY is complete


def settings(tmp_path) -> Settings:
    return Settings(
        data_root=tmp_path,
        symbols=("BTCUSDT",),
        datasets=("klines_1m", "agg_trades", "funding_rate"),
        spot_base_url="https://api.binance.com",
        futures_base_url="https://fapi.binance.com",
        timeout_seconds=5,
        max_retries=1,
        max_pages=10,
        weight_per_minute=100,
        lookback_hours=1,
    )


def bar(ts: datetime, **over) -> dict:
    row = {
        "symbol": "BTCUSDT", "interval": "1m", "open_time": ts,
        "close_time": ts + timedelta(seconds=59),
        "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
        "volume": 10.0, "quote_volume": 1000.0, "trades": 5,
        "taker_buy_base": 4.0, "taker_buy_quote": 400.0, "ingested_at": ts,
    }
    row.update(over)
    return row


def trade(i: int, ts: datetime, **over) -> dict:
    row = {
        "symbol": "BTCUSDT", "agg_trade_id": i, "price": 100.0, "qty": 0.5,
        "first_trade_id": i, "last_trade_id": i, "trade_time": ts,
        "is_buyer_maker": False, "ingested_at": ts,
    }
    row.update(over)
    return row


def write_bars(lake: ParquetLake, hours: range, *, extra: dict[int, list[dict]] | None = None):
    for hour in hours:
        start = DAY + timedelta(hours=hour)
        rows = [bar(start + timedelta(minutes=m)) for m in range(60)]
        rows += (extra or {}).get(hour, [])
        lake.write("klines_1m", "BTCUSDT", start, rows)


def read(con, path):
    return con.execute(f"SELECT * FROM read_parquet('{path}')").fetchall()


def test_silver_dedupes_adds_market_and_quarantines(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    # A newer corrupt copy of minute 5 must not hide the older valid copy.
    bad = bar(DAY + timedelta(minutes=5), high=50.0, ingested_at=DAY + timedelta(hours=3))
    dup = bar(DAY + timedelta(minutes=7), ingested_at=DAY + timedelta(hours=2))
    write_bars(lake, range(24), extra={0: [bad, dup]})

    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("bars_1m",))
    build_silver(layout, DAY, END, datasets=("bars_1m",), force=True)  # idempotent

    path = layout.silver_path("bars_1m", "BTCUSDT", DAY.date())
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT market, count(*), count(DISTINCT open_time) FROM read_parquet('{path}') GROUP BY 1"
    ).fetchall()
    assert rows == [("spot", 1440, 1440)]  # minute 5 keeps the valid copy, minute 7 collapses

    quarantine = list((tmp_path / "silver").glob("dataset=bars_1m_quarantine/**/*.parquet"))
    assert quarantine, "rejected rows must land in quarantine"
    rejected = con.execute(
        f"SELECT reject_reason, count(*) FROM read_parquet('{quarantine[0]}') GROUP BY 1"
    ).fetchall()
    assert rejected == [("ohlc_inconsistent", 1)]


def test_dq_flags_missing_hour_and_gates_failure(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    write_bars(lake, [h for h in range(24) if h != 10])
    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("bars_1m",))

    with pytest.raises(DataQualityError) as err:
        run_dq(layout, DAY, END, fail_on=("missing",), now=NOW)
    assert err.value.report.by_status.get("missing", 0) >= 1

    report = run_dq(layout, DAY, END, fail_on=(), now=NOW, raise_on_fail=False)
    assert report.hours_checked > 0


def test_dq_detects_trade_id_gaps(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    start = DAY
    ids = [1, 2, 3, 10, 11]  # gap between 3 and 10
    lake.write(
        "agg_trades", "BTCUSDT", start,
        [trade(i, start + timedelta(minutes=n)) for n, i in enumerate(ids)],
    )
    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("trades",))
    with pytest.raises(DataQualityError):
        run_dq(layout, DAY, END, fail_on=("id_gaps",), now=NOW)


def test_funding_zero_mark_price_becomes_null(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    lake.write(
        "funding_rate", "BTCUSDT", DAY,
        [{"symbol": "BTCUSDT", "funding_time": DAY, "funding_rate": 0.0001,
          "mark_price": 0.0, "ingested_at": DAY}],
    )
    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("funding",))
    con = duckdb.connect()
    path = layout.silver_path("funding", "BTCUSDT", DAY.date())
    assert con.execute(f"SELECT mark_price FROM read_parquet('{path}')").fetchone() == (None,)


def test_silver_skips_unchanged_day_and_force_rebuilds(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    write_bars(lake, range(1))
    layout = Layout.from_settings(cfg)

    first = build_silver(layout, DAY, END, datasets=("bars_1m",))
    assert first[0].partitions_written == 1
    second = build_silver(layout, DAY, END, datasets=("bars_1m",))
    assert second[0].partitions_written == 0
    assert second[0].partitions_skipped == 1
    forced = build_silver(layout, DAY, END, datasets=("bars_1m",), force=True)
    assert forced[0].partitions_written == 1


def test_rebuild_keeps_previous_silver_when_all_rows_rejected(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    write_bars(lake, range(24))
    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("bars_1m",))

    path = layout.silver_path("bars_1m", "BTCUSDT", DAY.date())
    con = duckdb.connect()
    assert con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0] == 1440

    # Re-ingest every hour with corrupt bars: no valid row remains.
    for hour in range(24):
        start = DAY + timedelta(hours=hour)
        rows = [bar(start + timedelta(minutes=m), high=50.0, ingested_at=start) for m in range(60)]
        lake.write("klines_1m", "BTCUSDT", start, rows)

    result = build_silver(layout, DAY, END, datasets=("bars_1m",), force=True)
    assert result[0].rows == 0
    assert result[0].rejected == 1440
    # The good day must survive; rejected rows only land in quarantine.
    assert con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0] == 1440
    quarantine = list((tmp_path / "silver").glob("dataset=bars_1m_quarantine/**/*.parquet"))
    assert quarantine
    rejected = con.execute(
        f"SELECT count(*) FROM read_parquet('{quarantine[0]}')"
    ).fetchone()[0]
    assert rejected == 1440


def test_dq_flags_stale_funding(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    lake.write(
        "funding_rate", "BTCUSDT", DAY,
        [{"symbol": "BTCUSDT", "funding_time": DAY, "funding_rate": 0.0001,
          "mark_price": 100.0, "ingested_at": DAY}],
    )
    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("funding",))
    run_dq(layout, DAY, END, fail_on=("missing",), now=NOW, raise_on_fail=False)

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    dq_path = layout.silver_path("dq_hourly", "BTCUSDT", DAY.date())
    statuses = con.execute(
        f"SELECT hour(CAST(hour_start AS TIMESTAMP)), status FROM read_parquet('{dq_path}') "
        "WHERE dataset_name = 'funding' ORDER BY 1"
    ).fetchall()
    assert {hour for hour, status in statuses if status == "missing"} == set(range(8, 24))

    # A funding point every 8 hours covers every hour again.
    lake.write(
        "funding_rate", "BTCUSDT", DAY,
        [
            {"symbol": "BTCUSDT", "funding_time": DAY + timedelta(hours=h),
             "funding_rate": 0.0001, "mark_price": 100.0, "ingested_at": DAY}
            for h in (0, 8, 16)
        ],
    )
    build_silver(layout, DAY, END, datasets=("funding",), force=True)
    run_dq(layout, DAY, END, fail_on=("missing",), now=NOW, raise_on_fail=False)
    statuses = con.execute(
        f"SELECT DISTINCT status FROM read_parquet('{dq_path}') WHERE dataset_name = 'funding'"
    ).fetchall()
    assert statuses == [("ok",)]


def test_silver_datasets_follow_configured_bronze():
    assert silver_datasets_for(("klines_1m", "agg_trades")) == ("bars_1m", "trades")
    assert silver_datasets_for(("open_interest",)) == ("open_interest",)


def test_gold_ohlcv_and_catalog_and_layer_files(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    write_bars(lake, range(24))
    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("bars_1m",))
    run_dq(layout, DAY, END, fail_on=(), now=NOW, raise_on_fail=False)
    build_gold(layout, DAY, END, marts=("ohlcv", "data_quality_daily"))

    gold = list((tmp_path / "gold").glob("mart=ohlcv/**/*.parquet"))
    assert gold
    con = duckdb.connect()
    n = con.execute(
        f"SELECT count(*) FROM read_parquet('{gold[0]}')"
    ).fetchone()[0]
    assert n > 0

    dq_gold = list((tmp_path / "gold").glob("mart=data_quality_daily/**/*.parquet"))
    assert dq_gold
    source = con.execute(
        f"SELECT DISTINCT dataset FROM read_parquet('{dq_gold[0]}')"
    ).fetchall()
    assert source == [("bars_1m",)]

    catalog = tmp_path / "catalog.duckdb"
    views = build_catalog_layers(layout, catalog)
    assert any(name.startswith("silver_") for name in views)
    assert any(name.startswith("gold_") for name in views)

    files = [path for _, path in iter_layer_files(layout, ("BTCUSDT",))]
    assert files and all("_quarantine" not in str(p) for p in files)


def test_dq_rules_reuse_ingest_thresholds():
    assert set(DQ_RULES) == set(SILVER_SPECS)
    for name, spec in SILVER_SPECS.items():
        time_col, expected = DQ_RULES[name]
        assert time_col == spec.time_col
        if name == "funding":  # ingest allows 0/hour; DQ still needs a positive marker
            assert expected == 1
        else:
            assert expected == MIN_ROWS_PER_HOUR[spec.bronze]


def test_dq_can_be_limited_to_selected_datasets(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    write_bars(lake, range(24))
    lake.write("agg_trades", "BTCUSDT", DAY, [trade(1, DAY)])
    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("bars_1m", "trades"))

    run_dq(
        layout,
        DAY,
        END,
        fail_on=(),
        now=NOW,
        datasets=("bars_1m",),
        raise_on_fail=False,
    )
    con = duckdb.connect()
    path = layout.silver_path("dq_hourly", "BTCUSDT", DAY.date())
    datasets = con.execute(
        f"SELECT DISTINCT dataset_name FROM read_parquet('{path}')"
    ).fetchall()
    assert datasets == [("bars_1m",)]

    # A second filtered run must keep the first dataset instead of overwriting it.
    run_dq(
        layout,
        DAY,
        END,
        fail_on=(),
        now=NOW,
        datasets=("trades",),
        raise_on_fail=False,
    )
    datasets = con.execute(
        f"SELECT DISTINCT dataset_name FROM read_parquet('{path}') ORDER BY 1"
    ).fetchall()
    assert datasets == [("bars_1m",), ("trades",)]

    with pytest.raises(ValueError, match="Unknown DQ dataset"):
        run_dq(layout, DAY, END, datasets=("nope",), now=NOW)


def test_catalog_and_sync_helpers_skip_quarantine_and_filter_datasets(tmp_path):
    cfg = settings(tmp_path)
    lake = ParquetLake(cfg)
    write_bars(lake, range(24))
    # One corrupt bar so a quarantine partition exists next to the good one.
    lake.write("klines_1m", "BTCUSDT", DAY, [bar(DAY, high=50.0)])
    layout = Layout.from_settings(cfg)
    build_silver(layout, DAY, END, datasets=("bars_1m",), force=True)
    assert list((tmp_path / "silver").glob("dataset=bars_1m_quarantine/**/*.parquet"))

    views = build_catalog_layers(layout, tmp_path / "catalog.duckdb")
    assert all("quarantine" not in name for name in views)

    summary = layer_summary(layout, silver_datasets=("bars_1m",))
    assert set(summary["silver"]) == {"bars_1m"}
    filtered = [
        path
        for layer, path in iter_layer_files(
            layout, ("BTCUSDT",), silver_datasets=("bars_1m",)
        )
        if layer == "silver"
    ]
    assert filtered
    assert all("dataset=bars_1m/" in path.as_posix() for path in filtered)
