"""Build a DuckDB catalog (views) over the Parquet lake.

DuckDB reads Hive partitions natively, so no data is copied and the catalog can
be rebuilt at any time in milliseconds. Datasets that have no files yet get a
typed empty view so queries and dashboards do not break on day one.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pyarrow as pa

from .config import Settings
from .schemas import SCHEMAS

log = logging.getLogger(__name__)


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _duckdb_type(dtype: pa.DataType) -> str:
    if pa.types.is_string(dtype):
        return "VARCHAR"
    if pa.types.is_float64(dtype):
        return "DOUBLE"
    if pa.types.is_int64(dtype):
        return "BIGINT"
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP WITH TIME ZONE" if dtype.tz else "TIMESTAMP"
    raise ValueError(f"Unsupported Arrow type for catalog: {dtype}")


def _empty_view_sql(dataset: str) -> str:
    columns = ", ".join(
        f'"{field.name}" {_duckdb_type(field.type)}' for field in SCHEMAS[dataset]
    )
    return f"""
        CREATE OR REPLACE TABLE _empty_{dataset} ({columns});
        CREATE OR REPLACE VIEW {dataset} AS SELECT * FROM _empty_{dataset};
    """


def build_catalog(settings: Settings) -> dict[str, str]:
    import duckdb

    settings.catalog_path.parent.mkdir(parents=True, exist_ok=True)
    views: dict[str, str] = {}
    connection = duckdb.connect(str(settings.catalog_path))
    try:
        for dataset in settings.datasets:
            glob = (
                settings.lake_root
                / f"exchange={settings.exchange}"
                / f"dataset={dataset}"
                / "**"
                / "*.parquet"
            )
            # Store a path relative to the working directory. The same catalog
            # is used from the host (CWD = project root) and from the Airflow
            # container (CWD = /opt/airflow, where ./data is mounted), so an
            # absolute host path would break one of them.
            stored_glob = os.path.relpath(glob, Path.cwd())
            if not any(settings.lake_root.glob(f"exchange=*/dataset={dataset}/**/*.parquet")):
                log.info("No files yet for %s, creating an empty typed view", dataset)
                connection.execute(_empty_view_sql(dataset))
                views[dataset] = "(empty)"
                continue
            connection.execute(
                f"""
                CREATE OR REPLACE VIEW {dataset} AS
                SELECT * FROM read_parquet(
                    '{_sql_string(stored_glob)}',
                    hive_partitioning = true,
                    union_by_name = true
                )
                """
            )
            views[dataset] = stored_glob
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    log.info("Catalog rebuilt at %s (%s views)", settings.catalog_path, len(views))
    return views
