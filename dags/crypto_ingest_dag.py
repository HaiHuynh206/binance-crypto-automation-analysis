"""Airflow DAG: ingest Binance market data every 2 hours.

The DAG itself is intentionally thin. All fetching, retries, rate limiting,
partitioning and state handling live in the ``crypto_ingest`` package so the
same code can be run locally with the CLI or tested without Airflow.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from crypto_ingest.catalog import build_catalog as build_duckdb_catalog
from crypto_ingest.config import Settings
from crypto_ingest.pipeline import IngestError, ingest_window
from crypto_ingest.s3 import sync_to_s3

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),
}


@dag(
    dag_id="crypto_market_ingest",
    description="Ingest high-frequency crypto market data into a partitioned Parquet lake",
    schedule="0 */2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args=DEFAULT_ARGS,
    tags=["crypto", "market-data", "ingestion"],
    doc_md=__doc__,
)
def crypto_market_ingest():
    @task
    def ingest(dataset: str, **context) -> dict:
        settings = Settings.from_env()
        start = context["data_interval_start"] - timedelta(hours=settings.lookback_hours)
        end = context["data_interval_end"]
        log.info("Ingesting %s from %s to %s", dataset, start, end)
        try:
            result = ingest_window(
                settings,
                dataset,
                start,
                end,
                refresh_hours=1,
            )
        except IngestError as exc:
            # Keep the partial result visible in the task log before failing.
            log.error("Partial result: %s", exc.result.as_dict())
            raise
        log.info("Result: %s", result.as_dict())
        return result.as_dict()

    @task(trigger_rule="all_done")
    def refresh_catalog(results: list | None = None) -> None:
        # ``results`` only exists to declare the upstream dependency; the catalog
        # is always rebuilt from whatever partitions exist on disk.
        build_duckdb_catalog(Settings.from_env())

    @task(trigger_rule="all_done")
    def publish_to_s3(results: list | None = None) -> dict | None:
        # Publish whatever was ingested, even if one dataset failed.
        settings = Settings.from_env()
        if not settings.s3_configured:
            log.info("CRYPTO_S3_BUCKET is not set; skipping S3 publish")
            return None
        sync = sync_to_s3(settings)
        log.info("S3 publish: %s", sync.as_dict())
        return sync.as_dict()

    datasets = Settings.from_env().datasets
    ingest_tasks = [ingest.override(task_id=f"ingest_{dataset}")(dataset) for dataset in datasets]
    refresh_catalog(ingest_tasks)
    publish_to_s3(ingest_tasks)


crypto_market_ingest()
