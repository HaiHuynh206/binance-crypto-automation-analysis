"""Evidence for issue 01 (phần "verified-ok"): DuckDB đọc lake hive đúng.

Dựng lake giả theo đúng layout của src/crypto_ingest/storage.py, trong đó file parquet
ĐÃ chứa cột `symbol` và `symbol=` cũng là partition key, rồi kiểm tra 4 thao tác mà
compute_from_r2/catalog.py dùng.

Run:  python issue/repro/duckdb_hive_check.py
Deps: pip install duckdb numpy
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import duckdb

SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
PARTITIONS = (("2026-10-19", "22"), ("2026-10-20", "03"))


def build_lake(root: Path) -> str:
    for sym in SYMBOLS:
        for date, hour in PARTITIONS:
            d = (
                root / "lake" / "exchange=binance" / "dataset=klines_1s"
                / f"symbol={sym}" / f"date={date}" / f"hour={hour}"
            )
            d.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect()
            con.execute(
                f"""COPY (
                    SELECT '{sym}' AS symbol,
                           TIMESTAMP '2026-10-20 00:00:00' AS open_time,
                           CAST(1.5 AS DOUBLE) AS close
                ) TO '{(d / 'part-00000.parquet').as_posix()}' (FORMAT PARQUET)"""
            )
            con.close()
    return (root / "lake" / "**" / "*.parquet").as_posix()


def show(label: str, con, sql: str) -> None:
    print(f"--- {label}")
    try:
        rows = con.sql(sql).fetchall()
        print(f"rows={len(rows)} symbols={sorted({r[0] for r in rows})}")
        print(f"columns={[d[0] for d in con.sql(sql).description]}")
    except Exception as exc:  # noqa: BLE001
        print("ERROR:", type(exc).__name__, exc)


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="b4_hive_"))
    try:
        glob = build_lake(tmp)
        con = duckdb.connect()
        base = f"read_parquet('{glob}', hive_partitioning=true)"
        where = (
            "WHERE symbol IN ('BTCUSDT', 'ETHUSDT') "
            "AND date >= DATE '2026-10-19' AND date <= DATE '2026-10-21'"
        )

        show("read hive, không filter", con, f"SELECT * FROM {base}")
        show("filter symbol + date (compute_from_r2)", con, f"SELECT * FROM {base} {where}")
        show(
            "union_by_name (catalog.py)",
            con,
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true)",
        )

        out = tmp / "out.parquet"
        con.execute(f"COPY (SELECT * FROM {base} {where}) TO '{out.as_posix()}' (FORMAT PARQUET)")
        show("COPY TO local parquet (export_dataset)", con, f"SELECT * FROM '{out.as_posix()}'")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
