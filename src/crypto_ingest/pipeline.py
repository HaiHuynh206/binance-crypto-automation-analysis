"""Orchestration: turn a time window into idempotent hourly lake partitions."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .binance import FETCHERS, BinanceClient
from .config import Settings
from .http import BinanceHTTP
from .schemas import MIN_ROWS_PER_HOUR, dedupe_and_sort
from .state import CompletionStore
from .storage import ParquetLake
from .timeutil import HOUR, ensure_utc, iter_hours, utc_now

log = logging.getLogger(__name__)


class IngestError(RuntimeError):
    def __init__(self, message: str, result: IngestResult) -> None:
        super().__init__(message)
        self.result = result


@dataclass
class IngestResult:
    dataset: str
    hours: int = 0
    partitions_written: int = 0
    partitions_skipped: int = 0
    rows: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "dataset": self.dataset,
            "hours": self.hours,
            "partitions_written": self.partitions_written,
            "partitions_skipped": self.partitions_skipped,
            "rows": self.rows,
            "warnings": self.warnings,
            "errors": self.errors,
        }


def ingest_window(
    settings: Settings,
    dataset: str,
    start: datetime,
    end: datetime,
    *,
    symbols: tuple[str, ...] | None = None,
    force: bool = False,
    refresh_hours: int = 1,
    now: datetime | None = None,
) -> IngestResult:
    """Fetch every complete hour in ``[start, end)`` for one dataset.

    Re-running the same window is safe: partitions are overwritten atomically and
    completion markers prevent redundant API calls for old hours.
    """
    if dataset not in FETCHERS:
        raise ValueError(f"Unsupported dataset: {dataset}")

    now = ensure_utc(now or utc_now())
    start, end = ensure_utc(start), ensure_utc(end)
    end = min(end, now)
    if end <= start:
        raise ValueError(f"Empty window: start={start} end={end}")

    symbols = symbols or settings.symbols
    result = IngestResult(dataset=dataset)
    lake = ParquetLake(settings)
    state = CompletionStore(settings)

    http = BinanceHTTP(
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
        weight_per_minute=settings.weight_per_minute,
    )
    client = BinanceClient(
        http,
        spot_base=settings.spot_base_url,
        futures_base=settings.futures_base_url,
        max_pages=settings.max_pages,
    )

    hours = list(iter_hours(start, end))
    result.hours = len(hours)
    fetch = FETCHERS[dataset]

    for hour in hours:
        if hour + HOUR > now:
            result.warnings.append(f"{hour.isoformat()}: incomplete hour, skipped")
            continue
        refresh = refresh_hours > 0 and hour >= end - timedelta(hours=refresh_hours)

        for symbol in symbols:
            label = f"{dataset}/{symbol}/{hour:%Y-%m-%dT%H}"
            if not force and not refresh and state.is_done(dataset, symbol, hour):
                result.partitions_skipped += 1
                continue
            try:
                rows = dedupe_and_sort(dataset, fetch(client, symbol, hour, hour + HOUR))
                path = lake.write(dataset, symbol, hour, rows)
                state.mark_done(dataset, symbol, hour, rows=len(rows), file_path=path)
                result.partitions_written += 1
                result.rows += len(rows)
                minimum = MIN_ROWS_PER_HOUR.get(dataset, 0)
                if rows and len(rows) < minimum:
                    message = f"{label}: only {len(rows)} rows (expected >= {minimum})"
                    result.warnings.append(message)
                    log.warning(message)
            except Exception as exc:  # keep going, report at the end
                message = f"{label}: {exc}"
                result.errors.append(message)
                log.exception("Ingestion failed for %s", label)

    if result.errors:
        raise IngestError(
            f"{dataset}: {len(result.errors)} partition(s) failed, first error: "
            f"{result.errors[0]}",
            result,
        )
    return result
