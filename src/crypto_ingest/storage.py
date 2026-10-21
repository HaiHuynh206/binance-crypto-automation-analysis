"""Partitioned Parquet lake, one file per (dataset, symbol, hour).

Layout (Hive-style, directly queryable by DuckDB / Spark / Athena)::

    data/lake/exchange=binance/dataset=klines_1m/symbol=BTCUSDT/
        date=2026-10-21/hour=14/part-00000.parquet

Writing is atomic (temp file + ``os.replace``), so a crashed or retried task can
never leave a half-written partition behind.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings
from .schemas import SCHEMAS

log = logging.getLogger(__name__)

FILE_NAME = "part-00000.parquet"


class ParquetLake:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def partition_dir(self, dataset: str, symbol: str, hour: datetime) -> Path:
        return (
            self.settings.lake_root
            / f"exchange={self.settings.exchange}"
            / f"dataset={dataset}"
            / f"symbol={symbol}"
            / f"date={hour:%Y-%m-%d}"
            / f"hour={hour:%H}"
        )

    def partition_path(self, dataset: str, symbol: str, hour: datetime) -> Path:
        return self.partition_dir(dataset, symbol, hour) / FILE_NAME

    def write(
        self,
        dataset: str,
        symbol: str,
        hour: datetime,
        rows: list[dict],
    ) -> Path | None:
        if not rows:
            return None
        schema = SCHEMAS[dataset]
        table = pa.Table.from_pylist(rows, schema=schema)
        target = self.partition_path(dataset, symbol, hour)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{FILE_NAME}.tmp")

        pq.write_table(
            table,
            str(tmp),
            compression="zstd",
            compression_level=3,
            use_dictionary=True,
            write_statistics=True,
        )
        os.replace(tmp, target)
        log.info(
            "Wrote %s rows to %s (%.1f KiB)",
            table.num_rows,
            target,
            target.stat().st_size / 1024,
        )
        return target

    def read(self, dataset: str, symbol: str, hour: datetime) -> pa.Table | None:
        path = self.partition_path(dataset, symbol, hour)
        if not path.exists():
            return None
        return pq.read_table(path)

    def disk_usage(self) -> dict[str, int]:
        totals: dict[str, int] = {}
        if not self.settings.lake_root.exists():
            return totals
        for path in self.settings.lake_root.rglob("*.parquet"):
            dataset = _dataset_from_path(path)
            totals[dataset] = totals.get(dataset, 0) + path.stat().st_size
        return totals


def _dataset_from_path(path: Path) -> str:
    for part in path.parts:
        if part.startswith("dataset="):
            return part.split("=", 1)[1]
    return "unknown"
