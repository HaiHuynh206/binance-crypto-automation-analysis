"""Completion markers.

One small JSON file per (dataset, symbol, hour). Per-file markers avoid the
read-modify-write race of a single shared state file when Airflow tasks run in
parallel, and make backfills trivially resumable.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from .config import Settings
from .timeutil import ensure_utc, utc_now

log = logging.getLogger(__name__)


class CompletionStore:
    def __init__(self, settings: Settings) -> None:
        self.root = settings.state_root / "completed"

    def marker_path(self, dataset: str, symbol: str, hour: datetime) -> Path:
        hour = ensure_utc(hour)
        return (
            self.root
            / f"dataset={dataset}"
            / f"symbol={symbol}"
            / f"date={hour:%Y-%m-%d}"
            / f"hour={hour:%H}.json"
        )

    def is_done(self, dataset: str, symbol: str, hour: datetime) -> bool:
        return self.marker_path(dataset, symbol, hour).exists()

    def mark_done(
        self,
        dataset: str,
        symbol: str,
        hour: datetime,
        *,
        rows: int,
        file_path: Path | None,
    ) -> None:
        marker = self.marker_path(dataset, symbol, hour)
        marker.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "dataset": dataset,
            "symbol": symbol,
            "hour": ensure_utc(hour).isoformat(),
            "rows": rows,
            "file": str(file_path) if file_path else None,
            "finished_at": utc_now().isoformat(),
        }
        tmp = marker.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(tmp, marker)

    def clear(self, dataset: str, symbol: str, hour: datetime) -> None:
        marker = self.marker_path(dataset, symbol, hour)
        marker.unlink(missing_ok=True)

    def summary(self) -> list[dict]:
        results: list[dict] = []
        if not self.root.exists():
            return results
        for marker in sorted(self.root.rglob("*.json")):
            try:
                results.append(json.loads(marker.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                log.warning("Ignoring unreadable marker: %s", marker)
        return results
