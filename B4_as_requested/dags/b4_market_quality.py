"""
Airflow DAG: Binance HFT market-quality pipeline (group_4).

Deploy note: copy/sync this file to the Airflow server's DAG folder, or point
AIRFLOW__CORE__DAGS_FOLDER at group_4/dags. Paths assume GROUP4_ROOT env var
or default /opt/airflow/group_4 on the worker — set GROUP4_ROOT to the absolute
path of this project on the machine that runs tasks.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

GROUP4_ROOT = os.environ.get("GROUP4_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
if GROUP4_ROOT not in sys.path:
    sys.path.insert(0, GROUP4_ROOT)

DEFAULT_SYMBOLS = os.environ.get("B4_SYMBOLS", "BTCUSDT,ADAUSDT")
DEFAULT_MONTH = os.environ.get("B4_MONTH", "2024-01")
DEFAULT_AGG_DAYS = os.environ.get("B4_AGGTRADES_DAYS", "3")

default_args = {
    "owner": "group_4",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _qa_checks(**_context):
    """Fail hard if expected clean/metrics partitions are missing — no fabrication."""
    from pathlib import Path

    root = Path(GROUP4_ROOT)
    symbols = [s.strip() for s in DEFAULT_SYMBOLS.split(",") if s.strip()]
    month = DEFAULT_MONTH
    missing = []
    for sym in symbols:
        kl = root / "data" / "clean" / "klines" / f"symbol={sym}" / f"month={month}" / "part.parquet"
        if not kl.exists():
            missing.append(str(kl))
        met = root / "data" / "metrics" / f"symbol={sym}" / f"month={month}"
        for name in ("roll_amihud_hourly.parquet", "ofi_impact_hourly.parquet", "variance_ratio_hourly.parquet"):
            p = met / name
            if not p.exists():
                missing.append(str(p))
    if missing:
        raise FileNotFoundError(
            "QA failed — required real artifacts missing (will not invent):\n" + "\n".join(missing)
        )
    print("QA OK:", len(symbols), "symbols for", month)


with DAG(
    dag_id="b4_market_quality",
    default_args=default_args,
    description="Ingest Binance HFT → clean → metrics → QA",
    schedule=None,  # manual / backfill only for research
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["group4", "binance", "market-quality"],
) as dag:
    symbols_cli = " ".join(f"--symbol {s.strip()}" for s in DEFAULT_SYMBOLS.split(",") if s.strip())

    ingest = BashOperator(
        task_id="ingest_binance",
        bash_command=(
            f"cd '{GROUP4_ROOT}' && "
            f"python -m src.ingest_binance {symbols_cli} "
            f"--month {DEFAULT_MONTH} --klines --aggtrades-days {DEFAULT_AGG_DAYS}"
        ),
    )

    # derive first N days of month for metrics dates
    compute = BashOperator(
        task_id="compute_metrics",
        bash_command=(
            f"cd '{GROUP4_ROOT}' && "
            f"python - <<'PY'\n"
            f"from datetime import date, timedelta\n"
            f"y,m=map(int,'{DEFAULT_MONTH}'.split('-'))\n"
            f"start=date(y,m,1)\n"
            f"n=int('{DEFAULT_AGG_DAYS}')\n"
            f"days=' '.join((start+timedelta(days=i)).isoformat() for i in range(n))\n"
            f"import os\n"
            f"os.system(f\"python -m src.compute_all_metrics {symbols_cli} --month {DEFAULT_MONTH} --aggtrades-dates {{days}}\")\n"
            f"PY"
        ),
    )

    qa = PythonOperator(task_id="qa_checks", python_callable=_qa_checks)

    publish = BashOperator(
        task_id="publish_note",
        bash_command=(
            f"cd '{GROUP4_ROOT}' && "
            f"echo 'Metrics ready under data/metrics for notebook B4_phan_tich_chat_luong_tt.ipynb'"
        ),
    )

    ingest >> compute >> qa >> publish
