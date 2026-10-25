"""Silver (local, read-only) -> small Parquet panels for the B4 model notebook.

Outputs (default B4_analysis/data_export/):
  minute_panel.parquet  : symbol, ts (1m, UTC), last trade price, usd volume, buy/sell usd, rv_1s, high/low/close of 1s bars
  flow_10s.parquet      : symbol, ts (10s), last price, usd volume, signed usd flow
  hourly_roll.parquet   : symbol, hour, Roll inputs from trade-level price changes
Run:  python -m src.build_minute_panel --symbols BTCUSDT ETHUSDT [--silver ../data/silver]
      python -m src.build_minute_panel --source r2     # R2 lake, key từ .env hoặc biến môi trường
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def r2_connect():
    """DuckDB on the R2 lake. Credentials: B4_analysis/.env if present, else environment variables."""
    from src.r2_lake import ENV_FILE, duckdb_connect, load_env, require_r2
    env = {**os.environ, **(load_env() if ENV_FILE.exists() else {})}
    cfg = require_r2(env)
    con = duckdb_connect(cfg)
    return con, f"s3://{cfg['CRYPTO_S3_BUCKET']}/{cfg['CRYPTO_S3_PREFIX'].strip('/')}/silver"


def build(symbols, silver, out: Path, date_from=None, date_to=None, con=None):
    out.mkdir(parents=True, exist_ok=True)
    if con is None:
        con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; SET memory_limit='6GB'; SET preserve_insertion_order=false")
    sym = ",".join(f"'{s}'" for s in symbols)
    rng = ""
    if date_from:
        rng += f" AND date >= DATE '{date_from}'"
    if date_to:
        rng += f" AND date <= DATE '{date_to}'"
    tr = f"read_parquet('{silver}/dataset=trades/symbol=*/date=*/*.parquet', hive_partitioning=true)"
    b1 = f"read_parquet('{silver}/dataset=bars_1s/symbol=*/date=*/*.parquet', hive_partitioning=true)"

    con.execute(f"""
    COPY (
      WITH t AS (
        SELECT symbol, date_trunc('minute', trade_time) AS ts, trade_time, agg_trade_id, price, notional,
               aggressor_side
        FROM {tr} WHERE symbol IN ({sym}) {rng}
      ), m AS (
        SELECT symbol, ts,
               arg_max(price, (trade_time, agg_trade_id)) AS close,
               sum(notional) AS usd_vol,
               sum(notional) FILTER (WHERE aggressor_side='BUY')  AS buy_usd,
               sum(notional) FILTER (WHERE aggressor_side='SELL') AS sell_usd,
               count(*) AS n_trades
        FROM t GROUP BY 1,2
      ), s AS (
        SELECT symbol, date_trunc('minute', open_time) AS ts,
               sum(pow(ln(close/nullif(lag_c,0)),2)) AS rv_1s,
               max(high) AS high_1s, min(low) AS low_1s, arg_max(close, open_time) AS close_1s
        FROM (SELECT symbol, open_time, close, high, low,
                     lag(close) OVER (PARTITION BY symbol ORDER BY open_time) AS lag_c
              FROM {b1} WHERE symbol IN ({sym}) {rng})
        GROUP BY 1,2
      )
      SELECT m.*, s.rv_1s, s.high_1s, s.low_1s, s.close_1s FROM m LEFT JOIN s USING (symbol, ts) ORDER BY symbol, ts
    ) TO '{out}/minute_panel.parquet' (FORMAT parquet, COMPRESSION zstd)
    """)

    con.execute(f"""
    COPY (
      SELECT symbol, time_bucket(INTERVAL 10 SECOND, trade_time) AS ts,
             arg_max(price, agg_trade_id) AS close,
             sum(notional) AS usd_vol,
             sum(CASE WHEN aggressor_side = 'BUY' THEN notional ELSE -notional END) AS signed_usd
      FROM {tr} WHERE symbol IN ({sym}) {rng}
      GROUP BY 1, 2 ORDER BY 1, 2
    ) TO '{out}/flow_10s.parquet' (FORMAT parquet, COMPRESSION zstd)
    """)

    # Roll: serial covariance of trade-to-trade log-price changes, per hour.
    # One symbol-day file at a time: a window over the whole lake does not fit in memory.
    parts = []
    files = [r[0] for r in con.execute(f"SELECT file FROM glob('{silver}/dataset=trades/symbol=*/date=*/*.parquet')").fetchall()]
    for f in sorted(files):
        s_, d_ = (part.split("=", 1)[1] for part in f.split("/")[-3:-1])
        if s_ not in symbols or (date_from and d_ < date_from) or (date_to and d_ > date_to):
            continue
        parts.append(con.execute(f"""
          WITH t AS (
            SELECT date_trunc('hour', trade_time) AS hour, agg_trade_id,
                   ln(price) - lag(ln(price)) OVER (ORDER BY agg_trade_id) AS dp
            FROM read_parquet('{f}')
          ), u AS (SELECT hour, dp, lag(dp) OVER (ORDER BY agg_trade_id) AS dp1 FROM t)
          SELECT '{s_}' AS symbol, hour, count(*) AS n, covar_samp(dp, dp1) AS cov1
          FROM u WHERE dp IS NOT NULL AND dp1 IS NOT NULL GROUP BY 1, 2
        """).df())
    pd.concat(parts).sort_values(["symbol", "hour"]).to_parquet(out / "hourly_roll.parquet", index=False)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    ap.add_argument("--source", choices=["local", "r2"], default="local")
    ap.add_argument("--silver", type=Path, default=ROOT.parent / "data" / "silver")
    ap.add_argument("--out", type=Path, default=ROOT / "data_export")
    ap.add_argument("--date-from"); ap.add_argument("--date-to")
    a = ap.parse_args()
    con, silver = r2_connect() if a.source == "r2" else (None, a.silver)
    build(a.symbols, silver, a.out, a.date_from, a.date_to, con)
