"""Airflow DAG: ingest Binance market data every 2 hours, then Bronze -> Silver -> Gold.

The DAG itself is intentionally thin. All fetching, retries, rate limiting,
partitioning and state handling live in the ``crypto_ingest`` package so the
same code can be run locally with the CLI or tested without Airflow.

Tasks are organised in three task groups::

    bronze        ingest_<dataset> for every configured dataset
    silver_gold   build_silver_layer -> dq_check -> build_gold_layer
    serve         refresh_catalog, publish_to_s3

    bronze.ingest_* -> silver_gold.build_silver_layer -> silver_gold.dq_check
                    -> silver_gold.build_gold_layer -> serve.refresh_catalog
                                                    -> serve.publish_to_s3

``dq_check`` is the gate: when it fails, Gold is not rebuilt, the catalog still
refreshes, and S3 receives Bronze only. Set ``CRYPTO_MEDALLION_ENABLED=0`` to run
the legacy Bronze-only flow (``bronze`` -> ``serve``) while rolling the medallion
layers out; the ``silver_gold`` group is then not created.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from crypto_ingest.catalog import build_catalog as build_duckdb_catalog
from crypto_ingest.config import Settings
from crypto_ingest.medallion import (
    DataQualityError,
    Layout,
    build_gold,
    build_silver,
    run_dq,
    silver_datasets_for,
)
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

    def _window(settings: Settings, context: dict):
        start = context["data_interval_start"] - timedelta(hours=settings.lookback_hours)
        return start, context["data_interval_end"]

    @task(trigger_rule="all_done")
    def build_silver_layer(results: list | None = None, **context) -> list:
        # all_done: clean whatever Bronze exists; gaps are reported by dq_check.
        settings = Settings.from_env()
        start, end = _window(settings, context)
        out = build_silver(
            Layout.from_settings(settings),
            start,
            end,
            datasets=silver_datasets_for(settings.datasets),
        )
        return [item.as_dict() for item in out]

    @task(trigger_rule="all_done")
    def dq_check(
        silver: list | None = None, ingest: list | None = None, **context
    ) -> dict:
        # all_done: ingest/silver failures must still be reported by the gate.
        settings = Settings.from_env()
        start, end = _window(settings, context)
        fail_on = tuple(
            part.strip()
            for part in os.getenv("CRYPTO_DQ_FAIL_ON", "missing").split(",")
            if part.strip()
        )
        report = run_dq(
            Layout.from_settings(settings),
            start,
            end,
            fail_on=fail_on,
            datasets=silver_datasets_for(settings.datasets),
            raise_on_fail=False,
        )
        problems: list[str] = []
        if silver is None:
            problems.append("silver build did not complete")
        if ingest is None:
            problems.append("ingest results unavailable")
        else:
            for result in ingest:
                if result is None:
                    problems.append("an ingest task did not produce a result")
                elif result.get("errors"):
                    problems.extend(result["errors"])
        if problems:
            report.failures.extend(problems)
            raise DataQualityError(
                f"{len(problems)} ingestion/silver problem(s), first: {problems[0]}",
                report,
            )
        if report.failures:
            raise DataQualityError(
                f"{len(report.failures)} hour(s) failed data quality "
                f"({', '.join(fail_on)}), first: {report.failures[0]}",
                report,
            )
        return report.as_dict()

    @task
    def build_gold_layer(report: dict | None = None, **context) -> list:
        settings = Settings.from_env()
        start, end = _window(settings, context)
        out = build_gold(Layout.from_settings(settings), start, end)
        return [item.as_dict() for item in out]

    @task(trigger_rule="all_done")
    def refresh_catalog(results: list | None = None) -> None:
        # ``results`` only exists to declare the upstream dependency; the catalog
        # is always rebuilt from whatever partitions exist on disk.
        build_duckdb_catalog(Settings.from_env())

    @task(trigger_rule="all_done")
    def publish_to_s3(results: list | None = None, dq: dict | None = None) -> dict | None:
        # Bronze is published even after a failure; Silver/Gold only when the gate
        # and the Gold build both completed.
        settings = Settings.from_env()
        if not settings.s3_configured:
            log.info("CRYPTO_S3_BUCKET is not set; skipping S3 publish")
            return None
        include_layers = results is not None and dq is not None
        if not include_layers:
            log.warning("DQ or Gold did not complete; publishing Bronze only")
        sync = sync_to_s3(settings, include_layers=include_layers)
        log.info("S3 publish: %s", sync.as_dict())
        return sync.as_dict()

    settings = Settings.from_env()
    datasets = settings.datasets

    with TaskGroup("bronze"):
        ingest_tasks = [
            ingest.override(task_id=f"ingest_{dataset}")(dataset) for dataset in datasets
        ]

    if settings.medallion_enabled:
        with TaskGroup("silver_gold"):
            silver = build_silver_layer(ingest_tasks)
            dq = dq_check(silver, ingest_tasks)
            gold = build_gold_layer(dq)

        with TaskGroup("serve"):
            refresh_catalog(gold)
            publish_to_s3(gold, dq)
    else:
        # Rollout switch: keep ingesting Bronze and publishing it, skip the layers.
        with TaskGroup("serve"):
            refresh_catalog(ingest_tasks)
            publish_to_s3(ingest_tasks)


market_ingest_dag = crypto_market_ingest()
