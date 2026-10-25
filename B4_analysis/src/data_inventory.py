"""Measured data inventory for the B4 report (rubric: dataset, source, licence, period, rows, cols, size, missing).

Reads Silver + dq_hourly locally (read-only) and the exported panels; writes docs/results/data_inventory.csv.
Run:  python -m src.data_inventory
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT.parent / "data" / "silver"
EXPORT = ROOT / "data_export"
SYMBOLS = ("BTCUSDT", "ETHUSDT")
SRC = "Binance public data (data.binance.vision / REST) -> crypto_ingest lake (Silver)"
LIC = "CC BY-NC-SA 4.0"


def size_mb(paths) -> float:
    return round(sum(p.stat().st_size for p in paths) / 2**20, 1)


def main():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    sym = ",".join(f"'{s}'" for s in SYMBOLS)
    dq = f"read_parquet('{SILVER}/dataset=dq_hourly/**/*.parquet', hive_partitioning=true)"
    rows = []
    for ds in ("trades", "bars_1s", "bars_1m"):
        files = [p for s in SYMBOLS for p in (SILVER / f"dataset={ds}" / f"symbol={s}").glob("date=*/*.parquet")]
        rel = f"read_parquet('{SILVER}/dataset={ds}/symbol=*/date=*/*.parquet', hive_partitioning=true)"
        n, d0, d1, present = con.execute(f"""SELECT count(*), min(date), max(date), string_agg(DISTINCT symbol, '+' ORDER BY symbol)
                                             FROM {rel} WHERE symbol IN ({sym})""").fetchone()
        ncol = len(con.execute(f"DESCRIBE SELECT * FROM {rel} LIMIT 0").fetchall()) - 2  # minus hive cols dataset/date
        ok, miss = con.execute(f"""SELECT count(*) FILTER (WHERE status='ok'), count(*) FILTER (WHERE status<>'ok')
                                   FROM {dq} WHERE dataset_name='{ds}' AND symbol IN ({sym})""").fetchone()
        rows.append(dict(dataset=f"silver/{ds}", source=SRC, licence=LIC, period=f"{d0} -> {d1}", symbols=present,
                         rows=n, cols=ncol, size_mb=size_mb(files), real_or_simulated="real",
                         pct_hours_not_ok=round(100 * miss / (ok + miss), 3), used_in_notebook="via export"))
    for f in sorted(EXPORT.glob("*.parquet")):
        d = pd.read_parquet(f)
        rows.append(dict(dataset=f"data_export/{f.name}", source="src/build_minute_panel.py from Silver", licence=LIC,
                         period="2026-07-01 -> 2026-10-24", symbols="+".join(sorted(d["symbol"].unique())),
                         rows=len(d), cols=d.shape[1], size_mb=size_mb([f]), real_or_simulated="real (derived)",
                         pct_cells_null=round(100 * d.isna().mean().mean(), 3), used_in_notebook="yes"))
    # Backup dataset named in DE_BAI.md. Download attempt 2026-10-25: the sample URL
    # https://lobsterdata.com/info/sample/LOBSTER_SampleFile_AAPL_2012-06-21_10.zip returns the site's
    # HTML app shell (457 B), not a zip.
    rows.append(dict(dataset="LOBSTER sample (bộ dự phòng theo đề)", source="lobsterdata.com",
                     period="thử tải 2026-10-25: KHÔNG truy cập được",
                     real_or_simulated="không có dữ liệu",
                     used_in_notebook=("KHÔNG dùng làm bằng chứng. Link tệp mẫu hỏng (trả về trang HTML 457 B thay vì zip), "
                                       "nay phải xin bằng email học thuật. Kết quả AAPL 21/06/2012 (118.497 snapshot) chạy "
                                       "trước đó lấy từ tệp không còn, không kiểm chứng được nên không dùng. "
                                       "Nhóm KHÔNG có bộ dự phòng.")))
    out = pd.DataFrame(rows)
    out.to_csv(ROOT / "docs" / "results" / "data_inventory.csv", index=False)
    print(out.to_string())


if __name__ == "__main__":
    main()
