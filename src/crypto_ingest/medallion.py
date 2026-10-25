"""Silver and Gold layers built with DuckDB on top of the Bronze Parquet lake.

Bronze is the lake written by :mod:`crypto_ingest.pipeline`. This module adds the
layers above it with the same conventions: Hive layout, one Parquet file per
partition, atomic ``os.replace`` writes, and every step safe to re-run::

    silver  data/silver/dataset=<name>/symbol=<SYMBOL>/date=<YYYY-MM-DD>/part-00000.parquet
    gold    data/gold/mart=<name>/symbol=<SYMBOL>/date=<YYYY-MM-DD>/part-00000.parquet

* Silver is cleaned and conformed. Duplicates are removed across partitions, rows
  that cannot be right go to ``<name>_quarantine`` instead of disappearing, and
  derived columns (aggressor side, notional, ...) are added.
* ``dq_hourly`` (stored next to Silver) records, for every complete hour, whether
  the data arrived. It can stop the build before Gold is produced.
* Gold holds analysis-ready marts and is only ever built from Silver.

Silver and Gold are rebuilt one UTC day at a time, so a run over a window rewrites
the days that window touches and nothing else.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from .schemas import MIN_ROWS_PER_HOUR
from .timeutil import ensure_utc, utc_now

if TYPE_CHECKING:  # pragma: no cover
    from .config import Settings

log = logging.getLogger(__name__)

FILE_NAME = "part-00000.parquet"
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)

DQ_DATASET = "dq_hourly"

# Market labels written to Silver. Kept here so the literal appears once.
SPOT_MARKET = "spot"
FUTURES_MARKET = "usdm_futures"

FUNDING_MAX_GAP_HOURS = 8
DQ_STATUSES = ("missing", "partial", "id_gaps")

# Bronze dataset -> Silver dataset. Used to default the layers to CRYPTO_DATASETS.
SILVER_FROM_BRONZE: dict[str, str] = {
    "klines_1s": "bars_1s",
    "klines_1m": "bars_1m",
    "agg_trades": "trades",
    "funding_rate": "funding",
    "open_interest": "open_interest",
}


def silver_datasets_for(bronze_datasets: tuple[str, ...]) -> tuple[str, ...]:
    """Silver datasets that can be built from the configured Bronze datasets."""
    return tuple(
        silver for bronze, silver in SILVER_FROM_BRONZE.items() if bronze in bronze_datasets
    )


class MedallionError(RuntimeError):
    """One or more Silver/Gold partitions failed; ``results`` has the partial work."""

    def __init__(self, message: str, results: list[LayerResult]) -> None:
        super().__init__(message)
        self.results = results


class DataQualityError(RuntimeError):
    """Raised when a complete hour has a status listed in ``fail_on``."""

    def __init__(self, message: str, report: DQReport) -> None:
        super().__init__(message)
        self.report = report


@dataclass
class LayerResult:
    layer: str
    dataset: str
    partitions_written: int = 0
    partitions_skipped: int = 0
    rows: int = 0
    rejected: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "layer": self.layer,
            "dataset": self.dataset,
            "partitions_written": self.partitions_written,
            "partitions_skipped": self.partitions_skipped,
            "rows": self.rows,
            "rejected": self.rejected,
            "errors": self.errors,
        }


@dataclass
class DQReport:
    hours_checked: int = 0
    partitions_written: int = 0
    by_status: dict[str, int] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "hours_checked": self.hours_checked,
            "partitions_written": self.partitions_written,
            "by_status": self.by_status,
            "failures": self.failures[:20],
            "failure_count": len(self.failures),
        }


@dataclass(frozen=True)
class Layout:
    bronze_root: Path
    silver_root: Path
    gold_root: Path
    exchange: str = "binance"
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")

    @classmethod
    def from_settings(cls, settings: Settings) -> Layout:
        return cls(
            bronze_root=settings.lake_root,
            silver_root=settings.data_root / "silver",
            gold_root=settings.data_root / "gold",
            exchange=settings.exchange,
            symbols=settings.symbols,
        )

    def bronze_files(self, dataset: str, symbol: str, day: date) -> list[Path]:
        base = (
            self.bronze_root
            / f"exchange={self.exchange}"
            / f"dataset={dataset}"
            / f"symbol={symbol}"
            / f"date={day:%Y-%m-%d}"
        )
        return sorted(base.glob("hour=*/*.parquet"))

    def silver_path(self, name: str, symbol: str, day: date) -> Path:
        return (
            self.silver_root
            / f"dataset={name}"
            / f"symbol={symbol}"
            / f"date={day:%Y-%m-%d}"
            / FILE_NAME
        )

    def gold_path(self, mart: str, symbol: str, day: date) -> Path:
        return (
            self.gold_root
            / f"mart={mart}"
            / f"symbol={symbol}"
            / f"date={day:%Y-%m-%d}"
            / FILE_NAME
        )

    def silver_file(self, name: str, symbol: str, day: date) -> list[Path]:
        path = self.silver_path(name, symbol, day)
        return [path] if path.exists() else []

    def silver_files_until(self, name: str, symbol: str, day: date) -> list[Path]:
        base = self.silver_root / f"dataset={name}" / f"symbol={symbol}"
        limit = f"date={day:%Y-%m-%d}"
        return sorted(p for p in base.glob(f"date=*/{FILE_NAME}") if p.parent.name <= limit)

    def first_silver_file(self, name: str, symbol: str) -> Path | None:
        base = self.silver_root / f"dataset={name}" / f"symbol={symbol}"
        files = sorted(base.glob(f"date=*/{FILE_NAME}"))
        return files[0] if files else None


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def iter_days(start: datetime, end: datetime) -> Iterator[date]:
    """Yield every UTC date that intersects the half-open interval [start, end)."""
    start, end = ensure_utc(start), ensure_utc(end)
    if end <= start:
        return
    cursor = start.date()
    last = (end - timedelta(microseconds=1)).date()
    while cursor <= last:
        yield cursor
        cursor += DAY


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + DAY


def _q(value: str) -> str:
    return value.replace("'", "''")


def _files(paths: list[Path]) -> str:
    return "[" + ", ".join(f"'{_q(p.as_posix())}'" for p in paths) + "]"


def _ts(value: datetime) -> str:
    return f"CAST('{ensure_utc(value).isoformat()}' AS TIMESTAMPTZ)"


def _hour(column: str) -> str:
    return f"CAST(date_trunc('hour', {column}) AS TIMESTAMPTZ)"


def _connect():
    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def _publish(con, select_sql: str, target: Path, rows: int, *, prune: bool = False) -> bool:
    """Write ``select_sql`` to ``target`` atomically.

    An empty result never deletes an existing partition unless ``prune`` is set:
    a bad re-ingest must not wipe a previously good Silver/Gold day.
    """
    if rows == 0:
        if prune:
            target.unlink(missing_ok=True)
        elif target.exists():
            log.warning("Keeping existing %s: rebuild produced no rows", target)
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp")
    con.execute(
        f"COPY ({select_sql}) TO '{_q(tmp.as_posix())}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    os.replace(tmp, target)
    return True


def _raise_on_errors(layer: str, results: list[LayerResult]) -> None:
    errors = [error for result in results for error in result.errors]
    if errors:
        raise MedallionError(
            f"{layer}: {len(errors)} partition(s) failed, first error: {errors[0]}", results
        )


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SilverSpec:
    name: str
    bronze: str
    time_col: str
    key: str
    market: str
    select_sql: str  # reads ``src``; must return ``reject_reason`` (NULL = good row)


_BARS_SELECT = """
    SELECT '{market}' AS market, symbol, open_time, close_time,
           open, high, low, close, volume, quote_volume, trades,
           taker_buy_base, volume - taker_buy_base AS taker_sell_base,
           taker_buy_quote, quote_volume - taker_buy_quote AS taker_sell_quote,
           ingested_at,
           CASE
             WHEN open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
               THEN 'null_price'
             WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
               THEN 'non_positive_price'
             WHEN high < greatest(open, close) OR low > least(open, close) OR high < low
               THEN 'ohlc_inconsistent'
             WHEN volume IS NULL OR quote_volume IS NULL OR volume < 0 OR quote_volume < 0
               THEN 'invalid_volume'
             WHEN taker_buy_base > volume + 1e-9
               THEN 'taker_exceeds_volume'
           END AS reject_reason
    FROM src
"""

_TRADES_SELECT = """
    SELECT '{market}' AS market, symbol, agg_trade_id, trade_time,
           price, qty, price * qty AS notional,
           CASE WHEN is_buyer_maker THEN 'SELL' ELSE 'BUY' END AS aggressor_side,
           first_trade_id, last_trade_id, ingested_at,
           CASE
             WHEN price IS NULL OR qty IS NULL THEN 'null_value'
             WHEN is_buyer_maker IS NULL THEN 'null_side'
             WHEN price <= 0 OR qty <= 0 THEN 'non_positive_value'
           END AS reject_reason
    FROM src
"""

_FUNDING_SELECT = """
    SELECT '{market}' AS market, symbol, funding_time, funding_rate,
           NULLIF(mark_price, 0) AS mark_price, ingested_at,
           CASE WHEN funding_rate IS NULL THEN 'null_rate' END AS reject_reason
    FROM src
"""

_OPEN_INTEREST_SELECT = """
    SELECT '{market}' AS market, symbol, ts, open_interest, open_interest_value,
           ingested_at,
           CASE
             WHEN open_interest IS NULL OR open_interest < 0 THEN 'invalid_open_interest'
             WHEN open_interest_value IS NULL OR open_interest_value < 0 THEN 'invalid_value'
           END AS reject_reason
    FROM src
"""

SILVER_SPECS: dict[str, SilverSpec] = {
    "bars_1s": SilverSpec(
        "bars_1s", "klines_1s", "open_time", "symbol, open_time", SPOT_MARKET, _BARS_SELECT
    ),
    "bars_1m": SilverSpec(
        "bars_1m", "klines_1m", "open_time", "symbol, open_time", SPOT_MARKET, _BARS_SELECT
    ),
    "trades": SilverSpec(
        "trades", "agg_trades", "trade_time", "symbol, agg_trade_id", SPOT_MARKET, _TRADES_SELECT
    ),
    "funding": SilverSpec(
        "funding",
        "funding_rate",
        "funding_time",
        "symbol, funding_time",
        FUTURES_MARKET,
        _FUNDING_SELECT,
    ),
    "open_interest": SilverSpec(
        "open_interest",
        "open_interest",
        "ts",
        "symbol, ts",
        FUTURES_MARKET,
        _OPEN_INTEREST_SELECT,
    ),
}

# Silver dataset -> minimum rows expected in a complete hour. Reuses the Bronze
# ingestion thresholds so the two never drift apart. Funding is published every
# 8 hours, so its ingest minimum is 0; DQ still needs a positive marker to
# report a missing funding point.
DQ_MIN_ROWS: dict[str, int] = {
    name: MIN_ROWS_PER_HOUR[spec.bronze] for name, spec in SILVER_SPECS.items()
}
DQ_MIN_ROWS["funding"] = 1
DQ_RULES: dict[str, tuple[str, int]] = {
    name: (spec.time_col, DQ_MIN_ROWS[name]) for name, spec in SILVER_SPECS.items()
}


def _stage_sql(spec: SilverSpec, files: list[Path], symbol: str, start: datetime, end: datetime):
    return f"""
    CREATE OR REPLACE TEMP TABLE stage AS
    WITH src AS (
        SELECT * FROM read_parquet({_files(files)}, union_by_name = true)
        WHERE symbol = '{_q(symbol)}'
          AND {spec.time_col} >= {_ts(start)}
          AND {spec.time_col} < {_ts(end)}
    )
    {spec.select_sql.format(market=_q(spec.market))}
    """


def _silver_partition(
    con, layout: Layout, spec: SilverSpec, symbol: str, day: date, result, *, force: bool = False
):
    files = layout.bronze_files(spec.bronze, symbol, day)
    if not files:
        result.partitions_skipped += 1
        return
    target = layout.silver_path(spec.name, symbol, day)
    if not force and target.exists():
        newest = max(path.stat().st_mtime_ns for path in files)
        if target.stat().st_mtime_ns > newest:
            result.partitions_skipped += 1
            return
    start, end = _day_bounds(day)
    con.execute(_stage_sql(spec, files, symbol, start, end))
    # Validate every row first, then dedupe the valid ones. A newer corrupt copy
    # must not hide an older valid copy of the same key.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE silver_good AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY {spec.key} ORDER BY ingested_at DESC
            ) AS rn
            FROM stage
            WHERE reject_reason IS NULL
        ) WHERE rn = 1
        """
    )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE silver_bad AS "
        "SELECT * FROM stage WHERE reject_reason IS NOT NULL"
    )
    good = con.execute("SELECT count(*) FROM silver_good").fetchone()[0]
    bad = con.execute("SELECT count(*) FROM silver_bad").fetchone()[0]
    order = f"ORDER BY {spec.time_col}"
    wrote = _publish(
        con,
        f"SELECT * EXCLUDE (reject_reason) FROM silver_good {order}",
        layout.silver_path(spec.name, symbol, day),
        good,
    )
    _publish(
        con,
        f"SELECT * FROM silver_bad {order}",
        layout.silver_path(f"{spec.name}_quarantine", symbol, day),
        bad,
        prune=True,
    )
    if wrote:
        result.partitions_written += 1
    else:
        result.partitions_skipped += 1
    result.rows += good
    result.rejected += bad
    if bad:
        log.warning("silver/%s/%s/%s: %s row(s) moved to quarantine", spec.name, symbol, day, bad)


def build_silver(
    layout: Layout,
    start: datetime,
    end: datetime,
    *,
    datasets: tuple[str, ...] | None = None,
    symbols: tuple[str, ...] | None = None,
    force: bool = False,
) -> list[LayerResult]:
    """Rebuild Silver for every UTC day that intersects ``[start, end)``.

    Days whose Silver file is already newer than every Bronze file are skipped
    unless ``force`` is set.
    """
    names = tuple(datasets) if datasets else tuple(SILVER_SPECS)
    unknown = sorted(set(names) - set(SILVER_SPECS))
    if unknown:
        raise ValueError(f"Unknown silver dataset(s): {unknown}. Known: {sorted(SILVER_SPECS)}")
    symbols = tuple(symbols) if symbols else layout.symbols

    results: list[LayerResult] = []
    con = _connect()
    try:
        for name in names:
            spec = SILVER_SPECS[name]
            result = LayerResult("silver", name)
            for symbol in symbols:
                for day in iter_days(start, end):
                    try:
                        _silver_partition(
                            con, layout, spec, symbol, day, result, force=force
                        )
                    except Exception as exc:  # keep going, report at the end
                        label = f"silver/{name}/{symbol}/{day}"
                        result.errors.append(f"{label}: {exc}")
                        log.exception("Silver build failed for %s", label)
            results.append(result)
    finally:
        con.close()
    _raise_on_errors("silver", results)
    return results


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------


def _dq_partition(
    con, layout, symbol, day, now, fail_on, report: DQReport, datasets: tuple[str, ...] | None
) -> None:
    day_start, day_end = _day_bounds(day)
    hours = [day_start + i * HOUR for i in range(24) if day_start + (i + 1) * HOUR <= now]
    if not hours:  # nothing in this day is complete yet
        return

    con.execute("CREATE OR REPLACE TEMP TABLE dq_spine (hour_start TIMESTAMPTZ)")
    con.executemany(
        "INSERT INTO dq_spine VALUES (CAST(? AS TIMESTAMPTZ))", [[h.isoformat()] for h in hours]
    )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE dq_meta "
        "(dataset VARCHAR, expected_min BIGINT, first_hour TIMESTAMPTZ)"
    )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE dq_counts "
        "(dataset VARCHAR, hour_start TIMESTAMPTZ, n_rows BIGINT, id_gap_missing BIGINT)"
    )

    active = 0
    for name, (time_col, expected) in DQ_RULES.items():
        if datasets is not None and name not in datasets:
            continue
        first_file = layout.first_silver_file(name, symbol)
        if first_file is None:  # dataset not ingested at all: nothing to judge
            continue
        active += 1
        # Hours before the first hour ever seen are not "missing", the lake just started.
        con.execute(
            f"INSERT INTO dq_meta SELECT '{name}', {expected}, "
            f"min({_hour(time_col)}) FROM read_parquet('{_q(first_file.as_posix())}')"
        )
        files = layout.silver_file(name, symbol, day)
        if name == "funding":
            # Every completed hour must be covered by a funding point from the last
            # FUNDING_MAX_GAP_HOURS hours (measured at the end of the hour, because a
            # funding timestamp at HH:00:00.006 still belongs to that hour). One missed
            # 8h funding shows up as 8 missing hours.
            candidates = layout.silver_file(name, symbol, day - DAY) + files
            if not candidates:
                continue
            con.execute(
                f"""
                INSERT INTO dq_counts
                SELECT 'funding', s.hour_start, count(f.funding_time), CAST(NULL AS BIGINT)
                FROM dq_spine s
                LEFT JOIN read_parquet({_files(candidates)}) f
                  ON f.funding_time >= s.hour_start + INTERVAL 1 HOUR
                                      - INTERVAL {FUNDING_MAX_GAP_HOURS} HOUR
                 AND f.funding_time <= s.hour_start + INTERVAL 1 HOUR
                GROUP BY s.hour_start
                """
            )
        elif name == "trades":
            if not files:
                continue
            # agg_trade_id normally increases by exactly 1, so a jump means missing trades.
            # The previous day is read only to judge the first trade of this day.
            both = layout.silver_file(name, symbol, day - DAY) + files
            con.execute(
                f"""
                INSERT INTO dq_counts
                SELECT 'trades', hour_start, count(*),
                       CAST(sum(CASE WHEN gap > 0 THEN gap ELSE 0 END) AS BIGINT)
                FROM (
                    SELECT {_hour('trade_time')} AS hour_start,
                           agg_trade_id - lag(agg_trade_id) OVER (ORDER BY agg_trade_id) - 1
                               AS gap
                    FROM read_parquet({_files(both)})
                ) t
                WHERE hour_start >= {_ts(day_start)} AND hour_start < {_ts(day_end)}
                GROUP BY hour_start
                """
            )
        else:
            if not files:
                continue
            con.execute(
                f"INSERT INTO dq_counts SELECT '{name}', {_hour(time_col)}, count(*), "
                f"CAST(NULL AS BIGINT) FROM read_parquet({_files(files)}) GROUP BY 2"
            )
    if not active:
        return

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE dq_out AS
        WITH grid AS (
            SELECT m.dataset, s.hour_start, m.expected_min,
                   coalesce(c.n_rows, 0) AS n_rows, c.id_gap_missing
            FROM dq_spine s
            CROSS JOIN dq_meta m
            LEFT JOIN dq_counts c ON c.dataset = m.dataset AND c.hour_start = s.hour_start
            WHERE s.hour_start >= m.first_hour
        )
        SELECT dataset AS dataset_name, '{_q(symbol)}' AS symbol, hour_start, n_rows, expected_min,
               id_gap_missing,
               CASE
                 WHEN n_rows = 0 AND expected_min > 0 THEN 'missing'
                 WHEN n_rows < expected_min THEN 'partial'
                 WHEN coalesce(id_gap_missing, 0) > 0 THEN 'id_gaps'
                 ELSE 'ok'
               END AS status,
               {_ts(now)} AS checked_at
        FROM grid
        """
    )
    total = con.execute("SELECT count(*) FROM dq_out").fetchone()[0]
    publish_table = "dq_out"
    if datasets is not None:
        # A filtered run must not wipe the other datasets already recorded today.
        existing = layout.silver_file(DQ_DATASET, symbol, day)
        if existing:
            selected_sql = ", ".join(f"'{_q(name)}'" for name in datasets)
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE dq_publish AS
                SELECT * FROM read_parquet({_files(existing)}, hive_partitioning = false)
                WHERE dataset_name NOT IN ({selected_sql})
                UNION ALL
                SELECT * FROM dq_out
                """
            )
            publish_table = "dq_publish"
    publish_rows = con.execute(f"SELECT count(*) FROM {publish_table}").fetchone()[0]
    wrote = _publish(
        con,
        f"SELECT * FROM {publish_table} ORDER BY dataset_name, hour_start",
        layout.silver_path(DQ_DATASET, symbol, day),
        publish_rows,
        prune=True,
    )
    if wrote:
        report.partitions_written += 1
    report.hours_checked += total
    for status, count in con.execute(
        "SELECT status, count(*) FROM dq_out GROUP BY 1"
    ).fetchall():
        report.by_status[status] = report.by_status.get(status, 0) + count
    if fail_on:
        wanted = ", ".join(f"'{status}'" for status in fail_on)
        bad = con.execute(
            f"SELECT dataset_name, CAST(hour_start AS VARCHAR), status, n_rows, expected_min "
            f"FROM dq_out WHERE status IN ({wanted}) ORDER BY hour_start, dataset_name"
        ).fetchall()
        for dataset, hour, status, n_rows, expected in bad:
            report.failures.append(
                f"{symbol} {dataset} {hour}: {status} (rows={n_rows}, expected>={expected})"
            )


def run_dq(
    layout: Layout,
    start: datetime,
    end: datetime,
    *,
    fail_on: tuple[str, ...] = ("missing",),
    now: datetime | None = None,
    symbols: tuple[str, ...] | None = None,
    datasets: tuple[str, ...] | None = None,
    raise_on_fail: bool = True,
) -> DQReport:
    """Write ``dq_hourly`` for the window and stop on the statuses in ``fail_on``.

    Statuses: ``missing`` (no rows in an hour that should have some), ``partial``
    (fewer rows than the minimum), ``id_gaps`` (agg_trade_id jumps), ``ok``.
    Funding is ``missing`` when no funding point landed in the last
    ``FUNDING_MAX_GAP_HOURS`` hours. Hours that are not complete yet, or that come
    before the first data ever ingested, are not judged.
    """
    fail_on = tuple(fail_on)
    unknown = sorted(set(fail_on) - set(DQ_STATUSES))
    if unknown:
        raise ValueError(f"Unknown DQ status(es): {unknown}. Known: {list(DQ_STATUSES)}")
    if datasets is not None:
        datasets = tuple(datasets)
        unknown = sorted(set(datasets) - set(DQ_RULES))
        if unknown:
            raise ValueError(
                f"Unknown DQ dataset(s): {unknown}. Known: {sorted(DQ_RULES)}"
            )
    now = ensure_utc(now or utc_now())
    symbols = tuple(symbols) if symbols else layout.symbols

    report = DQReport()
    con = _connect()
    try:
        for symbol in symbols:
            for day in iter_days(start, end):
                _dq_partition(con, layout, symbol, day, now, fail_on, report, datasets)
    finally:
        con.close()
    if report.failures and raise_on_fail:
        raise DataQualityError(
            f"{len(report.failures)} hour(s) failed data quality "
            f"({', '.join(fail_on)}), first: {report.failures[0]}",
            report,
        )
    return report


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GoldMart:
    name: str
    # Returns SQL that creates TEMP TABLE ``gold_out``, or None if inputs are missing.
    build: Callable[[Layout, str, date], str | None]
    order_by: str


_OHLCV_INTERVALS = (
    ("5m", "CAST(time_bucket(INTERVAL '5 minutes', open_time) AS TIMESTAMPTZ)", 5),
    ("1h", "CAST(date_trunc('hour', open_time) AS TIMESTAMPTZ)", 60),
    ("1d", "CAST(date_trunc('day', open_time) AS TIMESTAMPTZ)", 1440),
)


def _ohlcv_part(label: str, bucket_expr: str, minutes: int) -> str:
    return f"""
        SELECT symbol, '{label}' AS "interval", {bucket_expr} AS bucket,
               first(open ORDER BY open_time) AS open, max(high) AS high, min(low) AS low,
               last(close ORDER BY open_time) AS close,
               sum(volume) AS volume, sum(quote_volume) AS quote_volume,
               CAST(sum(trades) AS BIGINT) AS trades,
               sum(taker_buy_quote) AS taker_buy_quote,
               sum(quote_volume) / nullif(sum(volume), 0) AS vwap,
               CAST(count(*) AS INTEGER) AS n_bars,
               CAST({minutes} AS INTEGER) AS expected_bars,
               count(*) = {minutes} AS is_complete
        FROM bars GROUP BY symbol, bucket"""


def _gold_ohlcv(layout: Layout, symbol: str, day: date) -> str | None:
    files = layout.silver_file("bars_1m", symbol, day)
    if not files:
        return None
    parts = " UNION ALL ".join(_ohlcv_part(*interval) for interval in _OHLCV_INTERVALS)
    return f"""
    CREATE OR REPLACE TEMP TABLE gold_out AS
    WITH bars AS (SELECT * FROM read_parquet({_files(files)}))
    {parts}
    """


_NO_TRADES = """
    SELECT CAST(NULL AS TIMESTAMPTZ) AS hour_start, CAST(NULL AS BIGINT) AS n_agg_trades,
           CAST(NULL AS DOUBLE) AS trade_volume, CAST(NULL AS DOUBLE) AS trade_notional,
           CAST(NULL AS DOUBLE) AS trade_vwap, CAST(NULL AS DOUBLE) AS avg_trade_notional,
           CAST(NULL AS DOUBLE) AS buy_volume, CAST(NULL AS DOUBLE) AS sell_volume
    WHERE false
"""


def _gold_market_hourly(layout: Layout, symbol: str, day: date) -> str | None:
    current = layout.silver_file("bars_1m", symbol, day)
    if not current:
        return None
    bars = layout.silver_file("bars_1m", symbol, day - DAY) + current
    trade_files = layout.silver_file("trades", symbol, day)
    trades = (
        f"""
        SELECT {_hour('trade_time')} AS hour_start,
               CAST(count(*) AS BIGINT) AS n_agg_trades,
               sum(qty) AS trade_volume, sum(notional) AS trade_notional,
               sum(notional) / nullif(sum(qty), 0) AS trade_vwap,
               avg(notional) AS avg_trade_notional,
               sum(CASE WHEN aggressor_side = 'BUY' THEN qty ELSE 0 END) AS buy_volume,
               sum(CASE WHEN aggressor_side = 'SELL' THEN qty ELSE 0 END) AS sell_volume
        FROM read_parquet({_files(trade_files)}) GROUP BY 1
        """
        if trade_files
        else _NO_TRADES
    )
    day_start, day_end = _day_bounds(day)
    return f"""
    CREATE OR REPLACE TEMP TABLE gold_out AS
    WITH bars AS (
        SELECT symbol, open_time, open, high, low, close, volume, quote_volume, trades,
               taker_buy_quote, {_hour('open_time')} AS hour_start,
               ln(close / lag(close) OVER (ORDER BY open_time)) AS r
        FROM read_parquet({_files(bars)})
    ), hb AS (
        SELECT symbol, hour_start,
               first(open ORDER BY open_time) AS open, max(high) AS high, min(low) AS low,
               last(close ORDER BY open_time) AS close,
               sum(volume) AS volume, sum(quote_volume) AS quote_volume,
               CAST(sum(trades) AS BIGINT) AS exchange_trades,
               sum(quote_volume) / nullif(sum(volume), 0) AS vwap,
               sum(taker_buy_quote) / nullif(sum(quote_volume), 0) AS taker_buy_share,
               sqrt(sum(r * r)) AS realized_vol,
               CAST(count(*) AS INTEGER) AS n_minutes
        FROM bars
        WHERE hour_start >= {_ts(day_start)} AND hour_start < {_ts(day_end)}
        GROUP BY symbol, hour_start
    ), th AS ({trades})
    SELECT hb.*, th.n_agg_trades, th.trade_volume, th.trade_notional, th.trade_vwap,
           th.avg_trade_notional, th.buy_volume, th.sell_volume,
           (th.buy_volume - th.sell_volume)
               / nullif(th.buy_volume + th.sell_volume, 0) AS aggressor_imbalance
    FROM hb LEFT JOIN th ON hb.hour_start = th.hour_start
    """


_NO_FUNDING = """
    SELECT CAST(NULL AS VARCHAR) AS symbol, CAST(NULL AS TIMESTAMPTZ) AS funding_time,
           CAST(NULL AS DOUBLE) AS funding_rate
    WHERE false
"""


def _gold_derivatives_hourly(layout: Layout, symbol: str, day: date) -> str | None:
    oi_files = layout.silver_file("open_interest", symbol, day)
    if not oi_files:
        return None
    funding_files = layout.silver_files_until("funding", symbol, day)
    funding = (
        f"SELECT symbol, funding_time, funding_rate FROM read_parquet({_files(funding_files)})"
        if funding_files
        else _NO_FUNDING
    )
    return f"""
    CREATE OR REPLACE TEMP TABLE gold_out AS
    WITH oi AS (
        SELECT symbol, {_hour('ts')} AS hour_start,
               first(open_interest ORDER BY ts) AS oi_open,
               last(open_interest ORDER BY ts) AS oi_close,
               last(open_interest_value ORDER BY ts) AS oi_value_close,
               CAST(count(*) AS INTEGER) AS n_oi_points
        FROM read_parquet({_files(oi_files)})
        GROUP BY symbol, hour_start
    ), f AS ({funding})
    SELECT 'usdm_futures' AS market, oi.symbol, oi.hour_start,
           oi.oi_open, oi.oi_close, oi.oi_close - oi.oi_open AS oi_change,
           oi.oi_value_close, oi.n_oi_points,
           f.funding_time AS last_funding_time, f.funding_rate AS last_funding_rate
    FROM oi ASOF LEFT JOIN f ON oi.symbol = f.symbol AND oi.hour_start >= f.funding_time
    """


def _gold_data_quality_daily(layout: Layout, symbol: str, day: date) -> str | None:
    files = layout.silver_file(DQ_DATASET, symbol, day)
    if not files:
        return None
    return f"""
    CREATE OR REPLACE TEMP TABLE gold_out AS
    SELECT dataset_name AS dataset, symbol,
           CAST(count(*) AS INTEGER) AS hours_checked,
           CAST(count(*) FILTER (WHERE status = 'ok') AS INTEGER) AS hours_ok,
           CAST(count(*) FILTER (WHERE status = 'missing') AS INTEGER) AS hours_missing,
           CAST(count(*) FILTER (WHERE status = 'partial') AS INTEGER) AS hours_partial,
           CAST(count(*) FILTER (WHERE status = 'id_gaps') AS INTEGER) AS hours_with_id_gaps,
           CAST(coalesce(sum(id_gap_missing), 0) AS BIGINT) AS id_gap_missing,
           count(*) FILTER (WHERE status = 'ok') * 1.0 / count(*) AS pct_ok
    FROM read_parquet({_files(files)})
    GROUP BY dataset_name, symbol
    """


GOLD_MARTS: dict[str, GoldMart] = {
    "ohlcv": GoldMart("ohlcv", _gold_ohlcv, '"interval", bucket'),
    "market_hourly": GoldMart("market_hourly", _gold_market_hourly, "hour_start"),
    "derivatives_hourly": GoldMart("derivatives_hourly", _gold_derivatives_hourly, "hour_start"),
    "data_quality_daily": GoldMart("data_quality_daily", _gold_data_quality_daily, "dataset"),
}


def build_gold(
    layout: Layout,
    start: datetime,
    end: datetime,
    *,
    marts: tuple[str, ...] | None = None,
    symbols: tuple[str, ...] | None = None,
) -> list[LayerResult]:
    """Rebuild Gold marts for every UTC day that intersects ``[start, end)``."""
    names = tuple(marts) if marts else tuple(GOLD_MARTS)
    unknown = sorted(set(names) - set(GOLD_MARTS))
    if unknown:
        raise ValueError(f"Unknown gold mart(s): {unknown}. Known: {sorted(GOLD_MARTS)}")
    symbols = tuple(symbols) if symbols else layout.symbols

    results: list[LayerResult] = []
    con = _connect()
    try:
        for name in names:
            mart = GOLD_MARTS[name]
            result = LayerResult("gold", name)
            for symbol in symbols:
                for day in iter_days(start, end):
                    try:
                        sql = mart.build(layout, symbol, day)
                        if sql is None:
                            result.partitions_skipped += 1
                            continue
                        con.execute(sql)
                        rows = con.execute("SELECT count(*) FROM gold_out").fetchone()[0]
                        wrote = _publish(
                            con,
                            f"SELECT * FROM gold_out ORDER BY {mart.order_by}",
                            layout.gold_path(name, symbol, day),
                            rows,
                        )
                        if wrote:
                            result.partitions_written += 1
                        else:
                            result.partitions_skipped += 1
                        result.rows += rows
                    except Exception as exc:
                        label = f"gold/{name}/{symbol}/{day}"
                        result.errors.append(f"{label}: {exc}")
                        log.exception("Gold build failed for %s", label)
            results.append(result)
    finally:
        con.close()
    _raise_on_errors("gold", results)
    return results


# ---------------------------------------------------------------------------
# Catalog and sharing helpers
# ---------------------------------------------------------------------------

_BRONZE_DATASETS = ("klines_1s", "klines_1m", "agg_trades", "funding_rate", "open_interest")


def build_catalog_layers(layout: Layout, catalog_path: Path) -> dict[str, str]:
    """Add ``bronze_*``, ``silver_*`` and ``gold_*`` views to the DuckDB catalog."""
    import duckdb

    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    views: dict[str, str] = {}
    con = duckdb.connect(str(catalog_path))
    try:
        existing = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        for dataset in _BRONZE_DATASETS:
            if dataset in existing:
                con.execute(f"CREATE OR REPLACE VIEW bronze_{dataset} AS SELECT * FROM {dataset}")
                views[f"bronze_{dataset}"] = f"-> {dataset}"

        for prefix, root, marker in (
            ("silver", layout.silver_root, "dataset"),
            ("gold", layout.gold_root, "mart"),
        ):
            for folder in sorted(root.glob(f"{marker}=*")):
                if not any(folder.glob("symbol=*/date=*/*.parquet")):
                    continue
                name = folder.name.split("=", 1)[1]
                if name.endswith("_quarantine"):
                    continue
                # Same trick as catalog.py: a path relative to the working directory
                # works from the host and from the Airflow container alike.
                glob = os.path.relpath(folder / "**" / "*.parquet", Path.cwd())
                view = f"{prefix}_{name}"
                con.execute(
                    f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet("
                    f"'{_q(glob)}', hive_partitioning = true, union_by_name = true)"
                )
                views[view] = glob
        con.execute("CHECKPOINT")
    finally:
        con.close()
    return views


def iter_layer_files(
    layout: Layout,
    symbols: tuple[str, ...],
    silver_datasets: tuple[str, ...] | None = None,
) -> Iterator[tuple[str, Path]]:
    """Silver and Gold files to publish. Quarantine stays private.

    ``silver_datasets`` limits the Silver side to the given names (``dq_hourly`` is
    always kept when a filter is in use); Gold is never filtered because every
    mart is built from whichever Silver datasets are configured.
    """
    keep = None
    if silver_datasets is not None:
        keep = {*silver_datasets, DQ_DATASET}
    for layer, root in (("silver", layout.silver_root), ("gold", layout.gold_root)):
        for symbol in symbols:
            for path in sorted(root.glob(f"*/symbol={symbol}/date=*/*.parquet")):
                if any(part.endswith("_quarantine") for part in path.parts):
                    continue
                if layer == "silver" and keep is not None:
                    name = next(
                        (part.split("=", 1)[1] for part in path.parts if part.startswith("dataset=")),
                        "",
                    )
                    if name not in keep:
                        continue
                yield layer, path


def layer_summary(
    layout: Layout, silver_datasets: tuple[str, ...] | None = None
) -> dict[str, dict[str, dict[str, int]]]:
    """Files and bytes per Silver dataset / Gold mart, for the S3 manifest."""
    keep = None
    if silver_datasets is not None:
        keep = {*silver_datasets, DQ_DATASET}
    summary: dict[str, dict[str, dict[str, int]]] = {"silver": {}, "gold": {}}
    for layer, root, marker in (
        ("silver", layout.silver_root, "dataset"),
        ("gold", layout.gold_root, "mart"),
    ):
        for folder in sorted(root.glob(f"{marker}=*")):
            name = folder.name.split("=", 1)[1]
            if name.endswith("_quarantine"):
                continue
            if layer == "silver" and keep is not None and name not in keep:
                continue
            files = list(folder.rglob("*.parquet"))
            if files:
                summary[layer][name] = {
                    "files": len(files),
                    "bytes": sum(f.stat().st_size for f in files),
                }
    return summary
