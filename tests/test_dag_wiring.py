"""Structural checks for the Airflow DAG.

The full wiring test needs Airflow installed (it is provided by the Docker image,
not by the local CLI environment), so it is skipped when Airflow is unavailable.
The AST check runs everywhere and guards the task-group refactor from regressing.
"""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "crypto_ingest_dag.py"


def test_dag_source_parses_and_declares_groups() -> None:
    tree = ast.parse(DAG_PATH.read_text(encoding="utf-8"))

    groups = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "TaskGroup"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }
    assert groups == {"bronze", "silver_gold", "serve"}

    # The legacy Bronze-only path must stay gated by the rollout switch.
    assert "medallion_enabled" in DAG_PATH.read_text(encoding="utf-8")


def _load_dag(monkeypatch: pytest.MonkeyPatch, medallion: str):
    pytest.importorskip("airflow")
    monkeypatch.setenv("CRYPTO_MEDALLION_ENABLED", medallion)
    monkeypatch.setenv("CRYPTO_DATASETS", "klines_1m,funding_rate")
    monkeypatch.setenv("CRYPTO_SYMBOLS", "BTCUSDT,ETHUSDT")

    spec = importlib.util.spec_from_file_location(f"dag_under_test_{medallion}", DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.market_ingest_dag


def _ids(dag) -> set[str]:
    return {task.task_id for task in dag.tasks}


def test_medallion_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag(monkeypatch, "1")

    assert dag.dag_id == "crypto_market_ingest"
    ids = _ids(dag)
    ingest_ids = {"bronze.ingest_klines_1m", "bronze.ingest_funding_rate"}
    assert ingest_ids <= ids
    assert {
        "silver_gold.build_silver_layer",
        "silver_gold.dq_check",
        "silver_gold.build_gold_layer",
        "serve.refresh_catalog",
        "serve.publish_to_s3",
    } <= ids

    silver = dag.get_task("silver_gold.build_silver_layer")
    dq = dag.get_task("silver_gold.dq_check")
    gold = dag.get_task("silver_gold.build_gold_layer")
    catalog = dag.get_task("serve.refresh_catalog")
    publish = dag.get_task("serve.publish_to_s3")

    assert silver.upstream_task_ids == ingest_ids
    # The gate must still see both the Silver build and every ingest result.
    assert dq.upstream_task_ids == {"silver_gold.build_silver_layer"} | ingest_ids
    assert gold.upstream_task_ids == {"silver_gold.dq_check"}
    assert catalog.upstream_task_ids == {"silver_gold.build_gold_layer"}
    assert publish.upstream_task_ids == {
        "silver_gold.build_gold_layer",
        "silver_gold.dq_check",
    }

    # all_done keeps the gate and the serving tasks running after a failure.
    assert silver.trigger_rule == "all_done"
    assert dq.trigger_rule == "all_done"
    assert catalog.trigger_rule == "all_done"
    assert publish.trigger_rule == "all_done"
    assert gold.trigger_rule == "all_success"


def test_legacy_bronze_only_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag(monkeypatch, "0")

    ids = _ids(dag)
    ingest_ids = {"bronze.ingest_klines_1m", "bronze.ingest_funding_rate"}
    assert ingest_ids <= ids
    assert not {task_id for task_id in ids if task_id.startswith("silver_gold")}
    assert {"serve.refresh_catalog", "serve.publish_to_s3"} <= ids

    assert dag.get_task("serve.refresh_catalog").upstream_task_ids == ingest_ids
    assert dag.get_task("serve.publish_to_s3").upstream_task_ids == ingest_ids
