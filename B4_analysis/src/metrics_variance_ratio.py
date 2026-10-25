"""Lo–MacKinlay style variance ratio on hourly / multi-hour returns."""
from __future__ import annotations

import numpy as np
import pandas as pd


def variance_ratio(log_returns: np.ndarray, q: int) -> float:
    """
    VR(q) = Var(r_t(q)) / (q * Var(r_t(1)))
    where r_t(q) is q-period summed log return.
    Under random walk VR≈1; microstructure noise often pulls VR(q)<1 for small horizons.
    """
    r = np.asarray(log_returns, dtype=float)
    r = r[np.isfinite(r)]
    if len(r) < q + 2:
        return float("nan")
    var1 = np.var(r, ddof=1)
    if var1 <= 0:
        return float("nan")
    rq = np.array([r[i : i + q].sum() for i in range(0, len(r) - q + 1, q)])
    if len(rq) < 2:
        return float("nan")
    varq = np.var(rq, ddof=1)
    return float(varq / (q * var1))


def hourly_variance_ratios_from_klines(df: pd.DataFrame, qs: tuple[int, ...] = (2, 5, 10)) -> pd.DataFrame:
    """
    Build 1-minute log returns within each hour from 1s closes, then VR(q) on those minutes.
    Expect: open_time, close, symbol.
    """
    d = df.copy()
    d["open_time"] = pd.to_datetime(d["open_time"], utc=True)
    d = d.set_index("open_time").sort_index()
    # resample to 1min close
    rows = []
    for symbol, g in d.groupby("symbol"):
        g1 = g["close"].resample("1min").last().dropna()
        log_r = np.log(g1).diff().dropna()
        by_hour = log_r.groupby(log_r.index.floor("h"))
        for hour, hr in by_hour:
            arr = hr.values
            row = {"symbol": symbol, "hour": hour, "n_min": len(arr)}
            for q in qs:
                row[f"vr_{q}"] = variance_ratio(arr, q)
            rows.append(row)
    return pd.DataFrame(rows)
