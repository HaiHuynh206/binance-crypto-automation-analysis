"""Roll effective spread proxy and Amihud illiquidity from real trade/kline data."""
from __future__ import annotations

import numpy as np
import pandas as pd


def roll_effective_spread(prices: pd.Series) -> float:
    """
    Roll (1984) spread estimate: 2 * sqrt(-Cov(DeltaP_t, DeltaP_{t-1}))
    when serial covariance of price changes is negative; else NaN.
    """
    p = prices.astype(float).dropna()
    if len(p) < 3:
        return float("nan")
    dp = p.diff().dropna()
    if len(dp) < 2:
        return float("nan")
    cov = np.cov(dp.iloc[1:].values, dp.iloc[:-1].values)[0, 1]
    if cov >= 0:
        return float("nan")
    return float(2.0 * np.sqrt(-cov))


def amihud_illiquidity(returns: pd.Series, dollar_volume: pd.Series) -> float:
    """
    Amihud (2002): mean(|r| / dollar_volume) over observations with positive volume.
    """
    r = returns.astype(float)
    v = dollar_volume.astype(float)
    mask = (v > 0) & r.notna() & v.notna()
    if mask.sum() == 0:
        return float("nan")
    return float((r.abs()[mask] / v[mask]).mean())


def hourly_roll_amihud_from_klines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expect columns: open_time (UTC), close, volume, quote_volume, symbol.
    Aggregates to hour then computes Roll on close path and Amihud on hourly bars.
    Also returns within-hour Roll using 1s closes.
    """
    d = df.copy()
    d["open_time"] = pd.to_datetime(d["open_time"], utc=True)
    d["hour"] = d["open_time"].dt.floor("h")
    d["ret"] = d["close"].pct_change()

    rows = []
    for (symbol, hour), g in d.groupby(["symbol", "hour"], sort=True):
        g = g.sort_values("open_time")
        dollar_vol = g["quote_volume"].sum() if "quote_volume" in g else (g["close"] * g["volume"]).sum()
        # Amihud on 1s bars within the hour
        amihud = amihud_illiquidity(g["ret"], g["quote_volume"] if "quote_volume" in g else g["close"] * g["volume"])
        roll = roll_effective_spread(g["close"])
        rows.append(
            {
                "symbol": symbol,
                "hour": hour,
                "n_bars": len(g),
                "close_last": float(g["close"].iloc[-1]),
                "dollar_volume": float(dollar_vol),
                "roll_spread": roll,
                "amihud": amihud,
            }
        )
    return pd.DataFrame(rows)
