"""Charts from R2/Airflow-lake metrics."""
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "data" / "metrics" / "source=r2"
RANGE = "2026-10-19_2026-10-21"
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
OUT = Path(__file__).resolve().parent


def load(sym: str, name: str) -> pd.DataFrame:
    df = pd.read_parquet(BASE / f"symbol={sym}" / RANGE / name)
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    df["hour_of_day"] = df["hour"].dt.hour
    df["symbol"] = sym
    return df


def main() -> None:
    ra = pd.concat([load(s, "roll_amihud_hourly.parquet") for s in SYMBOLS], ignore_index=True)
    ofi = pd.concat([load(s, "ofi_impact_hourly.parquet") for s in SYMBOLS], ignore_index=True)
    profile = ra.groupby(["symbol", "hour_of_day"], as_index=False).agg(amihud=("amihud", "mean"))

    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharex=True)
    for ax, sym in zip(axes, SYMBOLS):
        g = profile[profile["symbol"] == sym]
        ax.plot(g["hour_of_day"], g["amihud"], marker="o")
        ax.set_title(f"Amihud by hour UTC — {sym} (R2 lake)")
        ax.set_xlabel("Hour (UTC)")
        ax.set_ylabel("Amihud")
        ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "r2_amihud_by_hour.png", dpi=120)
    plt.close(fig)

    cmp = ra.groupby("symbol")["amihud"].mean()
    fig, ax = plt.subplots(figsize=(5, 4))
    cmp.plot(kind="bar", ax=ax, color=["C0", "C1"])
    ax.set_yscale("log")
    ax.set_ylabel("Amihud (log)")
    ax.set_title("Mean Amihud — BTC vs ETH (Airflow R2)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "r2_amihud_btc_vs_eth.png", dpi=120)
    plt.close(fig)

    ofi_p = ofi.groupby(["symbol", "hour_of_day"], as_index=False)["ofi"].mean()
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for ax, sym in zip(axes, SYMBOLS):
        g = ofi_p[ofi_p["symbol"] == sym]
        ax.bar(g["hour_of_day"], g["ofi"], width=0.8)
        ax.axhline(0, color="k", lw=0.8)
        ax.set_title(f"OFI by hour — {sym} (R2)")
        ax.set_xlabel("Hour (UTC)")
    fig.tight_layout()
    fig.savefig(OUT / "r2_ofi_by_hour.png", dpi=120)
    plt.close(fig)
    print("Wrote r2_*.png to", OUT)


if __name__ == "__main__":
    main()
