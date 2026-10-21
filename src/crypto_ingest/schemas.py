"""Arrow schemas and deterministic ordering/deduplication rules per dataset."""

from __future__ import annotations

import pyarrow as pa

TS = pa.timestamp("ms", tz="UTC")

SCHEMAS: dict[str, pa.Schema] = {
    "klines_1s": pa.schema(
        [
            ("symbol", pa.string()),
            ("interval", pa.string()),
            ("open_time", TS),
            ("close_time", TS),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("volume", pa.float64()),
            ("quote_volume", pa.float64()),
            ("trades", pa.int64()),
            ("taker_buy_base", pa.float64()),
            ("taker_buy_quote", pa.float64()),
            ("ingested_at", TS),
        ]
    ),
    "klines_1m": pa.schema(
        [
            ("symbol", pa.string()),
            ("interval", pa.string()),
            ("open_time", TS),
            ("close_time", TS),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("volume", pa.float64()),
            ("quote_volume", pa.float64()),
            ("trades", pa.int64()),
            ("taker_buy_base", pa.float64()),
            ("taker_buy_quote", pa.float64()),
            ("ingested_at", TS),
        ]
    ),
    "agg_trades": pa.schema(
        [
            ("symbol", pa.string()),
            ("agg_trade_id", pa.int64()),
            ("price", pa.float64()),
            ("qty", pa.float64()),
            ("first_trade_id", pa.int64()),
            ("last_trade_id", pa.int64()),
            ("trade_time", TS),
            ("is_buyer_maker", pa.bool_()),
            ("ingested_at", TS),
        ]
    ),
    "funding_rate": pa.schema(
        [
            ("symbol", pa.string()),
            ("funding_time", TS),
            ("funding_rate", pa.float64()),
            ("mark_price", pa.float64()),
            ("ingested_at", TS),
        ]
    ),
    "open_interest": pa.schema(
        [
            ("symbol", pa.string()),
            ("ts", TS),
            ("open_interest", pa.float64()),
            ("open_interest_value", pa.float64()),
            ("ingested_at", TS),
        ]
    ),
}

PRIMARY_KEY: dict[str, str] = {
    "klines_1s": "open_time",
    "klines_1m": "open_time",
    "agg_trades": "agg_trade_id",
    "funding_rate": "funding_time",
    "open_interest": "ts",
}

# Minimum rows expected for a complete hour (used for soft quality checks).
MIN_ROWS_PER_HOUR: dict[str, int] = {
    "klines_1s": 3_000,
    "klines_1m": 55,
    "agg_trades": 1,
    "funding_rate": 0,
    "open_interest": 1,
}


def dedupe_and_sort(dataset: str, rows: list[dict]) -> list[dict]:
    """Drop duplicate primary keys and sort deterministically for reproducible files."""
    key = PRIMARY_KEY[dataset]
    seen: set = set()
    unique: list[dict] = []
    for row in rows:
        value = row[key]
        if value in seen:
            continue
        seen.add(value)
        unique.append(row)
    unique.sort(key=lambda row: row[key])
    return unique
