"""
Load LOBSTER sample message + orderbook CSVs (real files only).

Expected under data/raw/lobster/:
  *_message_*.csv
  *_orderbook_*.csv

Prices in LOBSTER are fixed-point × 1e-4 (dollars).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from src.paths import CLEAN, RAW, ensure_data_dirs

MSG_COLS = ["time", "event_type", "order_id", "size", "price", "direction"]


def find_pair(raw_dir: Path) -> tuple[Path, Path]:
    messages = sorted(raw_dir.glob("*_message_*.csv"))
    books = sorted(raw_dir.glob("*_orderbook_*.csv"))
    if not messages or not books:
        raise FileNotFoundError(
            f"STOPPED — no LOBSTER CSVs in {raw_dir}. "
            "Need *_message_*.csv and *_orderbook_*.csv (sample 2012-06-21)."
        )
    # prefer matching stem prefix
    for m in messages:
        prefix = m.name.split("_message_")[0]
        for b in books:
            if b.name.startswith(prefix):
                return m, b
    return messages[0], books[0]


def load_lobster(message_path: Path, book_path: Path, levels: int | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    msg = pd.read_csv(message_path, header=None, names=MSG_COLS)
    # infer levels from orderbook width: 4 cols per level
    raw = pd.read_csv(book_path, header=None)
    nlev = levels or (raw.shape[1] // 4)
    cols = []
    for i in range(1, nlev + 1):
        cols += [f"ask_price_{i}", f"ask_size_{i}", f"bid_price_{i}", f"bid_size_{i}"]
    if raw.shape[1] < len(cols):
        raise ValueError(f"Orderbook has {raw.shape[1]} cols, expected >= {len(cols)} for {nlev} levels")
    book = raw.iloc[:, : len(cols)].copy()
    book.columns = cols
    # align rows
    n = min(len(msg), len(book))
    msg = msg.iloc[:n].reset_index(drop=True)
    book = book.iloc[:n].reset_index(drop=True)
    # scale prices
    for c in book.columns:
        if "price" in c:
            book[c] = book[c].astype(float) / 1e4
    msg["price"] = msg["price"].astype(float) / 1e4
    return msg, book


def quoted_spread_stats(book: pd.DataFrame) -> pd.DataFrame:
    """Best-bid/ask spread and mid from level-1 book."""
    ask = book["ask_price_1"]
    bid = book["bid_price_1"]
    mid = (ask + bid) / 2.0
    spread = ask - bid
    rel = spread / mid
    return pd.DataFrame(
        {
            "n_snapshots": [len(book)],
            "spread_mean": [float(spread.mean())],
            "spread_median": [float(spread.median())],
            "rel_spread_bps_mean": [float((rel * 1e4).mean())],
            "bid_size1_mean": [float(book["bid_size_1"].mean())],
            "ask_size1_mean": [float(book["ask_size_1"].mean())],
        }
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--raw-dir", default=str(RAW / "lobster"))
    args = p.parse_args(argv)
    ensure_data_dirs()
    raw_dir = Path(args.raw_dir)
    try:
        mpath, bpath = find_pair(raw_dir)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1

    print(f"Using message={mpath.name} orderbook={bpath.name}")
    msg, book = load_lobster(mpath, bpath)
    out = CLEAN / "lobster"
    out.mkdir(parents=True, exist_ok=True)
    msg.to_parquet(out / "message.parquet", index=False)
    book.to_parquet(out / "orderbook.parquet", index=False)
    stats = quoted_spread_stats(book)
    stats["source_message"] = mpath.name
    stats["source_orderbook"] = bpath.name
    stats.to_csv(out / "quoted_spread_summary.csv", index=False)
    print(stats.to_string(index=False))
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    raise SystemExit(main())
