"""
Pull Parquet from Airflow/R2 lake → local cache → hourly market-quality metrics.

Uses real lake only (BTCUSDT, ETHUSDT on this deployment). No fabricated rows.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from src.metrics_ofi_impact import hourly_ofi_and_impact
from src.metrics_roll_amihud import hourly_roll_amihud_from_klines
from src.metrics_variance_ratio import hourly_variance_ratios_from_klines
from src.paths import CLEAN, METRICS, ROOT, ensure_data_dirs
from src.r2_lake import duckdb_connect, lake_glob, load_env, require_r2


def export_dataset(
    con,
    cfg: dict[str, str],
    dataset: str,
    symbols: list[str],
    date_from: str,
    date_to: str,
    out_path: Path,
) -> int:
    glob = lake_glob(cfg["CRYPTO_S3_BUCKET"], cfg["CRYPTO_S3_PREFIX"], dataset)
    sym_list = ", ".join(f"'{s}'" for s in symbols)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # COPY via DuckDB to local parquet (stream from R2)
    sql = f"""
    COPY (
      SELECT * FROM read_parquet('{glob}', hive_partitioning=true)
      WHERE symbol IN ({sym_list})
        AND date >= DATE '{date_from}'
        AND date <= DATE '{date_to}'
    ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
    """
    print(f"Export {dataset} {symbols} {date_from}..{date_to} -> {out_path}")
    con.execute(sql)
    n = len(pd.read_parquet(out_path))
    print(f"  rows={n:,}")
    return n


def klines_for_metrics(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["open_time"] = pd.to_datetime(d["open_time"], utc=True)
    if "n_trades" not in d.columns and "trades" in d.columns:
        d["n_trades"] = d["trades"]
    return d


def agg_for_metrics(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    rename = {}
    if "qty" in d.columns and "quantity" not in d.columns:
        rename["qty"] = "quantity"
    if "trade_time" in d.columns and "transact_time" not in d.columns:
        rename["trade_time"] = "transact_time"
    d = d.rename(columns=rename)
    d["transact_time"] = pd.to_datetime(d["transact_time"], utc=True)
    return d


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    p.add_argument("--date-from", default="2026-10-19")
    p.add_argument("--date-to", default="2026-10-21")
    p.add_argument("--skip-export", action="store_true", help="Reuse local R2 cache")
    args = p.parse_args(argv)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    ensure_data_dirs()

    try:
        cfg = require_r2(load_env())
    except (FileNotFoundError, RuntimeError) as e:
        print(e, file=sys.stderr)
        return 1

    cache = CLEAN / "r2"
    kl_path = cache / f"klines_1s_{args.date_from}_{args.date_to}.parquet"
    ag_path = cache / f"agg_trades_{args.date_from}_{args.date_to}.parquet"

    if not args.skip_export or not kl_path.exists() or not ag_path.exists():
        con = duckdb_connect(cfg)
        n1 = export_dataset(con, cfg, "klines_1s", symbols, args.date_from, args.date_to, kl_path)
        n2 = export_dataset(con, cfg, "agg_trades", symbols, args.date_from, args.date_to, ag_path)
        if n1 == 0 or n2 == 0:
            print("STOPPED — empty export from R2 (no invent).", file=sys.stderr)
            return 1
    else:
        print("Using cached:", kl_path, ag_path)

    kl = klines_for_metrics(pd.read_parquet(kl_path))
    ag = agg_for_metrics(pd.read_parquet(ag_path))

    for symbol in symbols:
        k = kl[kl["symbol"] == symbol]
        a = ag[ag["symbol"] == symbol]
        if k.empty or a.empty:
            print(f"STOPPED — missing symbol data on R2: {symbol}", file=sys.stderr)
            return 1
        ra = hourly_roll_amihud_from_klines(k)
        vr = hourly_variance_ratios_from_klines(k)
        ofi = hourly_ofi_and_impact(a)
        out = METRICS / "source=r2" / f"symbol={symbol}" / f"{args.date_from}_{args.date_to}"
        out.mkdir(parents=True, exist_ok=True)
        ra.to_parquet(out / "roll_amihud_hourly.parquet", index=False)
        vr.to_parquet(out / "variance_ratio_hourly.parquet", index=False)
        ofi.to_parquet(out / "ofi_impact_hourly.parquet", index=False)
        print(f"{symbol}: hours roll={len(ra)} vr={len(vr)} ofi={len(ofi)} -> {out}")

    # summary CSV for report
    rows = []
    for symbol in symbols:
        out = METRICS / "source=r2" / f"symbol={symbol}" / f"{args.date_from}_{args.date_to}"
        ra = pd.read_parquet(out / "roll_amihud_hourly.parquet")
        ofi = pd.read_parquet(out / "ofi_impact_hourly.parquet")
        vr = pd.read_parquet(out / "variance_ratio_hourly.parquet")
        rows.append(
            {
                "symbol": symbol,
                "source": "r2_airflow_lake",
                "date_from": args.date_from,
                "date_to": args.date_to,
                "n_hours": len(ra),
                "amihud_mean": ra["amihud"].mean(),
                "ofi_mean": ofi["ofi"].mean(),
                "vr2_mean": vr["vr_2"].mean(),
                "vr5_mean": vr["vr_5"].mean(),
                "vr10_mean": vr["vr_10"].mean(),
            }
        )
    summary = pd.DataFrame(rows)
    sum_path = METRICS / "source=r2" / f"summary_{args.date_from}_{args.date_to}.csv"
    summary.to_csv(sum_path, index=False)
    print(summary.to_string(index=False))
    print("Summary:", sum_path)
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    raise SystemExit(main())
