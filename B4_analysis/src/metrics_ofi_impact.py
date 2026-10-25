"""Order-flow imbalance and simple price impact from aggTrades."""
from __future__ import annotations

import numpy as np
import pandas as pd


def signed_volume(df: pd.DataFrame) -> pd.Series:
    """
    Binance is_buyer_maker=True => buyer is maker => seller aggressor => negative flow.
    Signed qty = +qty (buy aggressor) or -qty (sell aggressor).
    """
    buy_aggressor = ~df["is_buyer_maker"].astype(bool)
    sign = np.where(buy_aggressor, 1.0, -1.0)
    return sign * df["quantity"].astype(float)


def hourly_ofi_and_impact(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expect: transact_time, price, quantity, is_buyer_maker, symbol.
    OFI ~ sum(signed volume) / sum(volume) in hour.
    Price impact: regression delta_mid ~ signed_volume (simple OLS slope per hour
    using cumulative signed volume vs price change within hour).
    """
    d = df.copy()
    d["transact_time"] = pd.to_datetime(d["transact_time"], utc=True)
    d["hour"] = d["transact_time"].dt.floor("h")
    d["signed_qty"] = signed_volume(d)
    d["notional"] = d["price"].astype(float) * d["quantity"].astype(float)

    rows = []
    for (symbol, hour), g in d.groupby(["symbol", "hour"], sort=True):
        g = g.sort_values("transact_time")
        vol = g["quantity"].astype(float).sum()
        ofi = float(g["signed_qty"].sum() / vol) if vol > 0 else float("nan")
        px0 = float(g["price"].iloc[0])
        px1 = float(g["price"].iloc[-1])
        ret = (px1 / px0) - 1.0 if px0 else float("nan")
        # impact: return per unit signed notional (scaled)
        signed_notional = float((g["signed_qty"] * g["price"].astype(float)).sum())
        impact = float(ret / signed_notional) if signed_notional != 0 else float("nan")
        rows.append(
            {
                "symbol": symbol,
                "hour": hour,
                "n_trades": len(g),
                "volume": float(vol),
                "dollar_volume": float(g["notional"].sum()),
                "ofi": ofi,
                "hour_return": ret,
                "price_impact": impact,
            }
        )
    return pd.DataFrame(rows)
