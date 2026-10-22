"""
Read Binance Parquet lake on Cloudflare R2 (consumer).

Requires in group_4/.env:
  CRYPTO_S3_ENDPOINT_URL, CRYPTO_S3_BUCKET, CRYPTO_S3_PREFIX, CRYPTO_S3_REGION
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

Does not invent credentials — exits if keys missing.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import urlparse

from src.paths import ENV_FILE, ROOT


def load_env(path: Path = ENV_FILE) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path} — copy .env.example and fill keys.")
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def require_r2(env: dict[str, str]) -> dict[str, str]:
    needed = [
        "CRYPTO_S3_ENDPOINT_URL",
        "CRYPTO_S3_BUCKET",
        "CRYPTO_S3_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ]
    missing = [k for k in needed if not env.get(k)]
    if missing:
        raise RuntimeError(
            "STOPPED — R2 credentials incomplete in .env (will not invent):\n  "
            + "\n  ".join(missing)
            + "\nAdd AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY then re-run."
        )
    return {
        **{k: env[k] for k in needed},
        "CRYPTO_S3_PREFIX": env.get("CRYPTO_S3_PREFIX") or "crypto",
    }


def duckdb_connect(cfg: dict[str, str]):
    try:
        import duckdb
    except ImportError as e:
        raise RuntimeError("Install duckdb: pip install duckdb") from e

    host = urlparse(cfg["CRYPTO_S3_ENDPOINT_URL"]).netloc
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{host}';")
    con.execute(f"SET s3_region='{cfg['CRYPTO_S3_REGION']}';")
    con.execute(f"SET s3_access_key_id='{cfg['AWS_ACCESS_KEY_ID']}';")
    con.execute(f"SET s3_secret_access_key='{cfg['AWS_SECRET_ACCESS_KEY']}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET TimeZone='UTC';")
    return con


def lake_glob(bucket: str, prefix: str, dataset: str) -> str:
    prefix = prefix.strip("/")
    return f"s3://{bucket}/{prefix}/lake/exchange=binance/dataset={dataset}/**/*.parquet"


def sample_query(dataset: str = "klines_1m", limit: int = 10):
    """Return DuckDB relation for smoke test / notebooks."""
    cfg = require_r2(load_env())
    con = duckdb_connect(cfg)
    glob = lake_glob(cfg["CRYPTO_S3_BUCKET"], cfg["CRYPTO_S3_PREFIX"], dataset)
    return con.sql(
        f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true) LIMIT {int(limit)}"
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Smoke-query R2 crypto lake via DuckDB")
    p.add_argument(
        "--dataset",
        default="klines_1m",
        help="klines_1s | klines_1m | agg_trades | funding_rate | open_interest",
    )
    p.add_argument("--limit", type=int, default=10)
    args = p.parse_args(argv)

    try:
        cfg = require_r2(load_env())
        con = duckdb_connect(cfg)
        glob = lake_glob(cfg["CRYPTO_S3_BUCKET"], cfg["CRYPTO_S3_PREFIX"], args.dataset)
        print("Query:", glob)
        df = con.sql(
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true) "
            f"LIMIT {int(args.limit)}"
        ).df()
        # Avoid Windows console Unicode box-drawing from DuckDB pretty-print
        print(df.to_string())
        print(f"rows={len(df)} cols={list(df.columns)}")
    except (FileNotFoundError, RuntimeError) as e:
        print(e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    raise SystemExit(main())
