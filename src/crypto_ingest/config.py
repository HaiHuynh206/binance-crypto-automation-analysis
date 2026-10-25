"""Runtime configuration, read from environment variables.

All settings use the ``CRYPTO_`` prefix so they can be injected by Airflow,
Docker Compose or a local ``.env`` file without touching code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SUPPORTED_DATASETS: tuple[str, ...] = (
    "klines_1s",
    "klines_1m",
    "agg_trades",
    "funding_rate",
    "open_interest",
)

DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT"


def _csv_env(name: str, default: str, *, upper: bool = True) -> tuple[str, ...]:
    raw = os.getenv(name, default)
    items = [part.strip() for part in raw.split(",") if part.strip()]
    return tuple(item.upper() for item in items) if upper else tuple(items)


def _bool_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


@dataclass(frozen=True)
class Settings:
    data_root: Path
    symbols: tuple[str, ...]
    datasets: tuple[str, ...]
    spot_base_url: str
    futures_base_url: str
    timeout_seconds: float
    max_retries: int
    max_pages: int
    weight_per_minute: int
    lookback_hours: int
    medallion_enabled: bool = True
    s3_bucket: str | None = None
    s3_prefix: str = "crypto"
    s3_endpoint_url: str | None = None
    s3_region: str = "auto"
    s3_acl: str | None = None

    @classmethod
    def from_env(cls) -> Settings:
        datasets = _csv_env("CRYPTO_DATASETS", ",".join(SUPPORTED_DATASETS), upper=False)
        unknown = sorted(set(datasets) - set(SUPPORTED_DATASETS))
        if unknown:
            raise ValueError(
                f"Unsupported dataset(s): {unknown}. Supported: {SUPPORTED_DATASETS}"
            )
        if not datasets:
            raise ValueError("CRYPTO_DATASETS must contain at least one dataset")
        symbols = _csv_env("CRYPTO_SYMBOLS", DEFAULT_SYMBOLS)
        if not symbols:
            raise ValueError("CRYPTO_SYMBOLS must contain at least one symbol")

        return cls(
            data_root=Path(os.getenv("CRYPTO_DATA_ROOT", "data")).expanduser(),
            symbols=symbols,
            datasets=datasets,
            spot_base_url=os.getenv("CRYPTO_SPOT_BASE_URL", "https://api.binance.com").rstrip("/"),
            futures_base_url=os.getenv(
                "CRYPTO_FUTURES_BASE_URL", "https://fapi.binance.com"
            ).rstrip("/"),
            timeout_seconds=float(os.getenv("CRYPTO_HTTP_TIMEOUT", "20")),
            max_retries=int(os.getenv("CRYPTO_MAX_RETRIES", "5")),
            max_pages=int(os.getenv("CRYPTO_MAX_PAGES", "5000")),
            # Conservative per-process budget. Binance spot allows 6000 weight/min
            # per IP; keep headroom for other tasks sharing the IP.
            weight_per_minute=int(os.getenv("CRYPTO_WEIGHT_PER_MINUTE", "2400")),
            lookback_hours=int(os.getenv("CRYPTO_LOOKBACK_HOURS", "24")),
            medallion_enabled=_bool_env("CRYPTO_MEDALLION_ENABLED", True),
            s3_bucket=os.getenv("CRYPTO_S3_BUCKET") or None,
            s3_prefix=os.getenv("CRYPTO_S3_PREFIX", "crypto").strip("/") or "crypto",
            s3_endpoint_url=os.getenv("CRYPTO_S3_ENDPOINT_URL") or None,
            s3_region=os.getenv("CRYPTO_S3_REGION", "auto"),
            s3_acl=os.getenv("CRYPTO_S3_ACL") or None,
        )

    @property
    def lake_root(self) -> Path:
        return self.data_root / "lake"

    @property
    def state_root(self) -> Path:
        return self.data_root / "_state"

    @property
    def catalog_path(self) -> Path:
        return self.data_root / "catalog.duckdb"

    @property
    def exchange(self) -> str:
        return "binance"

    @property
    def s3_configured(self) -> bool:
        return bool(self.s3_bucket)
