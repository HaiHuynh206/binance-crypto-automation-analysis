"""Compute hourly metrics from clean Parquet produced by ingest (real data only)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from src.metrics_ofi_impact import hourly_ofi_and_impact
from src.metrics_roll_amihud import hourly_roll_amihud_from_klines
from src.metrics_variance_ratio import hourly_variance_ratios_from_klines
from src.paths import CLEAN, METRICS, ensure_data_dirs


def load_klines(symbol: str, month: str) -> pd.DataFrame:
    path = CLEAN / "klines" / f"symbol={symbol}" / f"month={month}" / "part.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing clean klines (run ingest first): {path}")
    return pd.read_parquet(path)


def load_aggtrades(symbol: str, dates: list[str]) -> pd.DataFrame:
    frames = []
    missing = []
    for ds in dates:
        path = CLEAN / "aggTrades" / f"symbol={symbol}" / f"date={ds}" / "part.parquet"
        if not path.exists():
            missing.append(str(path))
            continue
        frames.append(pd.read_parquet(path))
    if missing and not frames:
        raise FileNotFoundError("No aggTrades parquet found:\n" + "\n".join(missing))
    if missing:
        print("WARNING missing aggTrades days (skipped, not fabricated):", *missing, sep="\n  ")
    return pd.concat(frames, ignore_index=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", action="append", required=True)
    p.add_argument("--month", required=True)
    p.add_argument("--aggtrades-dates", nargs="+", required=True, help="YYYY-MM-DD list")
    args = p.parse_args(argv)
    ensure_data_dirs()

    try:
        for symbol in args.symbol:
            kl = load_klines(symbol, args.month)
            # restrict klines to aggTrades date window for comparable hours
            kl["open_time"] = pd.to_datetime(kl["open_time"], utc=True)
            days = pd.to_datetime(args.aggtrades_dates, utc=True)
            day_set = {d.date() for d in days}
            kl = kl[kl["open_time"].dt.date.isin(day_set)]
            if kl.empty:
                raise FileNotFoundError(f"No kline rows for {symbol} in dates {args.aggtrades_dates}")

            ra = hourly_roll_amihud_from_klines(kl)
            vr = hourly_variance_ratios_from_klines(kl)
            ag = load_aggtrades(symbol, args.aggtrades_dates)
            ofi = hourly_ofi_and_impact(ag)

            out_dir = METRICS / f"symbol={symbol}" / f"month={args.month}"
            out_dir.mkdir(parents=True, exist_ok=True)
            ra.to_parquet(out_dir / "roll_amihud_hourly.parquet", index=False)
            vr.to_parquet(out_dir / "variance_ratio_hourly.parquet", index=False)
            ofi.to_parquet(out_dir / "ofi_impact_hourly.parquet", index=False)
            print(f"{symbol}: wrote {out_dir} rows roll={len(ra)} vr={len(vr)} ofi={len(ofi)}")
    except FileNotFoundError as e:
        print(f"\nSTOPPED — real input missing:\n{e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    raise SystemExit(main())
