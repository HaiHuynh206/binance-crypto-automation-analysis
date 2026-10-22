"""Export analysis charts from real metrics (run from group_4/: python docs/figures/export_figures.py)."""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
METRICS = ROOT / "data" / "metrics"
OUT = Path(__file__).resolve().parent
MONTH = "2024-01"
SYMBOLS = ["BTCUSDT", "ADAUSDT"]


def load(sym: str, name: str) -> pd.DataFrame:
    df = pd.read_parquet(METRICS / f"symbol={sym}" / f"month={MONTH}" / name)
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    df["hour_of_day"] = df["hour"].dt.hour
    df["symbol"] = sym
    return df


def main() -> None:
    ra = pd.concat([load(s, "roll_amihud_hourly.parquet") for s in SYMBOLS], ignore_index=True)
    ofi = pd.concat([load(s, "ofi_impact_hourly.parquet") for s in SYMBOLS], ignore_index=True)
    profile = (
        ra.groupby(["symbol", "hour_of_day"], as_index=False)
        .agg(amihud=("amihud", "mean"), dollar_volume=("dollar_volume", "mean"))
    )

    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharex=True)
    for ax, sym in zip(axes, SYMBOLS):
        g = profile[profile["symbol"] == sym]
        ax.plot(g["hour_of_day"], g["amihud"], marker="o")
        ax.set_title(f"Amihud by hour UTC — {sym}")
        ax.set_xlabel("Hour (UTC)")
        ax.set_ylabel("Amihud")
        ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "amihud_by_hour.png", dpi=120)
    plt.close(fig)

    # side-by-side mean comparison
    cmp = ra.groupby("symbol")["amihud"].mean()
    fig, ax = plt.subplots(figsize=(5, 4))
    cmp.plot(kind="bar", ax=ax, color=["C0", "C1"])
    ax.set_yscale("log")
    ax.set_ylabel("Amihud (log scale)")
    ax.set_title("Mean Amihud — BTC vs ADA (72 hours)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "amihud_btc_vs_ada.png", dpi=120)
    plt.close(fig)

    ofi_p = ofi.groupby(["symbol", "hour_of_day"], as_index=False)["ofi"].mean()
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=False)
    for ax, sym in zip(axes, SYMBOLS):
        g = ofi_p[ofi_p["symbol"] == sym]
        ax.bar(g["hour_of_day"], g["ofi"], width=0.8)
        ax.axhline(0, color="k", lw=0.8)
        ax.set_title(f"OFI by hour UTC — {sym}")
        ax.set_xlabel("Hour (UTC)")
    fig.tight_layout()
    fig.savefig(OUT / "ofi_by_hour.png", dpi=120)
    plt.close(fig)
    print("Wrote figures to", OUT)


if __name__ == "__main__":
    main()
