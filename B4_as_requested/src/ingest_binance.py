"""
Download real Binance Vision public data (no synthetic rows).

Sources:
  https://data.binance.vision/
  S3 listing: https://s3-ap-northeast-1.amazonaws.com/data.binance.vision

License note (as of course brief): research/education only (CC BY-NC-SA 4.0).
"""
from __future__ import annotations

import argparse
import hashlib
import io
import sys
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from src.paths import CLEAN, RAW, ensure_data_dirs

BASE = "https://data.binance.vision"
S3_LIST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

KLINE_COLS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "n_trades",
    "taker_buy_base",
    "taker_buy_quote",
    "ignore",
]

# Binance Vision aggTrades: 8 columns (is_best_match added; 7-name load shifts index)
AGG_COLS = [
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
    "is_best_match",
]


class IngestError(RuntimeError):
    """Hard stop: missing remote file or download failure — do not invent data."""


def _http_get(url: str, timeout: int = 120) -> bytes:
    req = Request(url, headers={"User-Agent": "group4-mff4030-research/1.0"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except HTTPError as e:
        raise IngestError(f"HTTP {e.code} for {url}") from e
    except URLError as e:
        raise IngestError(f"Network error for {url}: {e}") from e


def _remote_exists(key: str) -> bool:
    """HEAD-like check via S3 list prefix exact key."""
    url = f"{S3_LIST}?prefix={key}&max-keys=1"
    xml = _http_get(url, timeout=30).decode("utf-8", errors="replace")
    return f"<Key>{key}</Key>" in xml


def _download_zip(key: str, dest_zip: Path) -> Path:
    if not _remote_exists(key):
        raise IngestError(
            f"Remote file missing (will not invent data): {key}\n"
            f"Check listing at https://data.binance.vision/?prefix={key.rsplit('/', 1)[0]}/"
        )
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    url = f"{BASE}/{key}"
    print(f"Downloading {url} -> {dest_zip}")
    data = _http_get(url, timeout=600)
    dest_zip.write_bytes(data)
    print(f"  wrote {dest_zip.stat().st_size:,} bytes")
    return dest_zip


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            raise IngestError(f"No CSV inside {zip_path}")
        with zf.open(names[0]) as fh:
            raw = fh.read()
    # Binance CSVs sometimes have no header
    sample = raw[:200].decode("utf-8", errors="replace")
    has_header = sample.split("\n", 1)[0].lower().startswith(("open_time", "agg_trade_id"))
    buf = io.BytesIO(raw)
    if has_header:
        return pd.read_csv(buf)
    # decide schema by filename convention
    if "aggTrades" in zip_path.name or "aggTrade" in zip_path.name:
        return pd.read_csv(buf, header=None, names=AGG_COLS)
    return pd.read_csv(buf, header=None, names=KLINE_COLS)


def download_monthly_klines(symbol: str, year_month: str, interval: str = "1s") -> Path:
    """year_month: YYYY-MM"""
    ensure_data_dirs()
    key = f"data/spot/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year_month}.zip"
    dest = RAW / "klines" / symbol / interval / f"{symbol}-{interval}-{year_month}.zip"
    return _download_zip(key, dest)


def download_daily_aggtrades(symbol: str, day: date) -> Path:
    ensure_data_dirs()
    ds = day.strftime("%Y-%m-%d")
    key = f"data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{ds}.zip"
    dest = RAW / "aggTrades" / symbol / f"{symbol}-aggTrades-{ds}.zip"
    return _download_zip(key, dest)


def klines_zip_to_parquet(zip_path: Path, symbol: str, year_month: str) -> Path:
    df = _read_csv_from_zip(zip_path)
    # normalize column names if header present with different casing
    df.columns = [str(c).strip() for c in df.columns]
    rename = {c: c for c in KLINE_COLS if c in df.columns}
    if "open_time" not in df.columns:
        raise IngestError(f"Unexpected kline schema in {zip_path}: {list(df.columns)}")
    df["symbol"] = symbol
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for c in ("open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base", "taker_buy_quote"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    out = CLEAN / "klines" / f"symbol={symbol}" / f"month={year_month}" / "part.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"  parquet {out} rows={len(df):,}")
    return out


def aggtrades_zip_to_parquet(zip_path: Path, symbol: str, day: date) -> Path:
    df = _read_csv_from_zip(zip_path)
    df.columns = [str(c).strip() for c in df.columns]
    if "transact_time" not in df.columns and "time" in df.columns:
        df = df.rename(columns={"time": "transact_time"})
    if "transact_time" not in df.columns:
        raise IngestError(f"Unexpected aggTrades schema in {zip_path}: {list(df.columns)}")
    df["symbol"] = symbol
    df["transact_time"] = pd.to_datetime(df["transact_time"], unit="ms", utc=True)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    # is_buyer_maker: True => seller aggressor (market sell)
    if df["is_buyer_maker"].dtype == object:
        df["is_buyer_maker"] = df["is_buyer_maker"].astype(str).str.lower().isin(("true", "1"))
    else:
        df["is_buyer_maker"] = df["is_buyer_maker"].astype(bool)
    ds = day.strftime("%Y-%m-%d")
    out = CLEAN / "aggTrades" / f"symbol={symbol}" / f"date={ds}" / "part.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"  parquet {out} rows={len(df):,}")
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest Binance Vision data into group_4/data")
    p.add_argument("--symbol", action="append", required=True, help="e.g. BTCUSDT (repeatable)")
    p.add_argument("--month", required=True, help="YYYY-MM for monthly 1s klines")
    p.add_argument("--klines", action="store_true", help="Download monthly 1s klines")
    p.add_argument(
        "--aggtrades-days",
        type=int,
        default=0,
        help="Number of consecutive days of daily aggTrades from month start",
    )
    p.add_argument("--aggtrades-start", default=None, help="Override start date YYYY-MM-DD")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.klines and args.aggtrades_days <= 0:
        print("Nothing to do: pass --klines and/or --aggtrades-days N", file=sys.stderr)
        return 2

    year, month = map(int, args.month.split("-"))
    if args.aggtrades_start:
        start = datetime.strptime(args.aggtrades_start, "%Y-%m-%d").date()
    else:
        start = date(year, month, 1)

    manifest = []
    try:
        for symbol in args.symbol:
            if args.klines:
                z = download_monthly_klines(symbol, args.month)
                pq = klines_zip_to_parquet(z, symbol, args.month)
                manifest.append({"kind": "klines", "symbol": symbol, "path": str(pq), "sha256_zip": _sha256(z)})
            for i in range(args.aggtrades_days):
                day = start + timedelta(days=i)
                z = download_daily_aggtrades(symbol, day)
                pq = aggtrades_zip_to_parquet(z, symbol, day)
                manifest.append(
                    {"kind": "aggTrades", "symbol": symbol, "date": day.isoformat(), "path": str(pq), "sha256_zip": _sha256(z)}
                )
    except IngestError as e:
        print(f"\nSTOPPED — missing/failed real data:\n{e}", file=sys.stderr)
        return 1

    man_path = CLEAN / f"manifest_{args.month}.csv"
    pd.DataFrame(manifest).to_csv(man_path, index=False)
    print(f"Manifest: {man_path}")
    return 0


if __name__ == "__main__":
    # allow `python -m src.ingest_binance` from group_4/
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    raise SystemExit(main())
