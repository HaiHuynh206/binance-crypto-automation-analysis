"""Publish the Parquet lake to S3-compatible object storage.

Works with AWS S3, Cloudflare R2, MinIO, Backblaze B2... Credentials are read
from the standard AWS environment variables so every tool in the ecosystem can
use the same bucket:

    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY

Objects keep the exact Hive layout used locally, so consumers can query them
with DuckDB, Athena, Spark or Trino without any transformation.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .medallion import Layout, iter_layer_files, layer_summary, silver_datasets_for
from .schemas import SCHEMAS
from .state import CompletionStore
from .timeutil import utc_now

log = logging.getLogger(__name__)

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
MANIFEST_NAME = "_manifest.json"


@dataclass
class SyncResult:
    uploaded: int = 0
    skipped: int = 0
    bytes_uploaded: int = 0
    manifest_uploaded: bool = False
    dry_run: bool = False
    files: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "uploaded": self.uploaded,
            "skipped": self.skipped,
            "bytes_uploaded": self.bytes_uploaded,
            "manifest_uploaded": self.manifest_uploaded,
            "dry_run": self.dry_run,
            "files": self.files[:20],
        }


def object_key(settings: Settings, path: Path) -> str:
    """Map a local lake file to its object key, preserving ``lake/...`` layout."""
    relative = path.relative_to(settings.data_root).as_posix()
    return f"{settings.s3_prefix}/{relative}"


def manifest_key(settings: Settings) -> str:
    return f"{settings.s3_prefix}/{MANIFEST_NAME}"


def _client(settings: Settings) -> Any:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - dependency installed in prod
        raise RuntimeError("boto3 is required for S3 sync (pip install boto3)") from exc

    kwargs: dict[str, Any] = {"region_name": settings.s3_region}
    if settings.s3_endpoint_url:
        kwargs["endpoint_url"] = settings.s3_endpoint_url
    return boto3.client("s3", **kwargs)


def _md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _already_synced(client: Any, bucket: str, key: str, path: Path) -> bool:
    from botocore.exceptions import ClientError

    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise

    if int(head.get("ContentLength", -1)) != path.stat().st_size:
        return False

    etag = str(head.get("ETag", "")).strip('"')
    if "-" not in etag and len(etag) == 32:
        return _md5(path) == etag

    # Multipart ETags are not MD5; fall back to modification time.
    remote_mtime = head["LastModified"].timestamp()
    return path.stat().st_mtime <= remote_mtime + 2


def iter_lake_files(
    settings: Settings,
    datasets: tuple[str, ...],
    symbols: tuple[str, ...],
) -> Iterator[tuple[str, Path]]:
    for dataset in datasets:
        for symbol in symbols:
            base = (
                settings.lake_root
                / f"exchange={settings.exchange}"
                / f"dataset={dataset}"
                / f"symbol={symbol}"
            )
            for path in sorted(base.rglob("*.parquet")):
                yield dataset, path


def build_manifest(
    settings: Settings,
    *,
    include_layers: bool = True,
    datasets: tuple[str, ...] | None = None,
) -> dict:
    markers = CompletionStore(settings).summary()
    rows: dict[str, int] = defaultdict(int)
    for marker in markers:
        rows[marker["dataset"]] += int(marker.get("rows") or 0)

    selected = datasets or settings.datasets
    datasets_summary: dict[str, dict] = {}
    for dataset in selected:
        files = 0
        size = 0
        seen_hours: set[str] = set()
        first_hour: str | None = None
        last_hour: str | None = None
        base = settings.lake_root / f"exchange={settings.exchange}" / f"dataset={dataset}"
        for path in base.rglob("*.parquet"):
            files += 1
            size += path.stat().st_size
            date = next((part[5:] for part in path.parts if part.startswith("date=")), None)
            hour = next((part[5:] for part in path.parts if part.startswith("hour=")), None)
            if date and hour:
                bucket = f"{date}T{hour}:00:00+00:00"
                seen_hours.add(bucket)
                first_hour = min(first_hour or bucket, bucket)
                last_hour = max(last_hour or bucket, bucket)
        datasets_summary[dataset] = {
            "partitions": len(seen_hours),
            "rows": rows.get(dataset, 0),
            "files": files,
            "bytes": size,
            "first_hour": first_hour,
            "last_hour": last_hour,
            "columns": [
                {"name": field.name, "type": str(field.type)}
                for field in SCHEMAS[dataset]
            ],
        }

    manifest = {
        "generated_at": utc_now().astimezone(timezone.utc).isoformat(),
        "exchange": settings.exchange,
        "symbols": list(settings.symbols),
        "layout": (
            "lake/exchange=<exchange>/dataset=<dataset>/symbol=<symbol>/"
            "date=<YYYY-MM-DD>/hour=<HH>/part-00000.parquet"
        ),
        "datasets": datasets_summary,
    }
    if include_layers:
        manifest["layers_layout"] = {
            "silver": "silver/dataset=<name>/symbol=<symbol>/date=<YYYY-MM-DD>/part-00000.parquet",
            "gold": "gold/mart=<name>/symbol=<symbol>/date=<YYYY-MM-DD>/part-00000.parquet",
        }
        manifest["layers"] = layer_summary(
            Layout.from_settings(settings), silver_datasets_for(selected)
        )
    return manifest


def sync_to_s3(
    settings: Settings,
    datasets: tuple[str, ...] | None = None,
    symbols: tuple[str, ...] | None = None,
    *,
    dry_run: bool = False,
    force: bool = False,
    upload_manifest: bool = True,
    include_layers: bool = True,
) -> SyncResult:
    if not settings.s3_configured:
        raise ValueError("CRYPTO_S3_BUCKET is not set")

    datasets = datasets or settings.datasets
    symbols = symbols or settings.symbols
    client = _client(settings)
    result = SyncResult(dry_run=dry_run)

    # A filtered run only sees part of the lake, so its manifest would replace the
    # full one with a subset. Only an unfiltered run may publish the manifest.
    filtered = set(datasets) != set(settings.datasets) or set(symbols) != set(
        settings.symbols
    )
    if filtered and upload_manifest and not dry_run:
        log.info("Dataset/symbol filter given; leaving _manifest.json untouched")
        upload_manifest = False

    publish_layers = include_layers and settings.medallion_enabled
    if include_layers and not settings.medallion_enabled:
        log.warning("CRYPTO_MEDALLION_ENABLED is off; publishing Bronze only")

    paths = [path for _, path in iter_lake_files(settings, datasets, symbols)]
    if publish_layers:  # Silver and Gold share the bucket, same prefix, same Hive layout
        layout = Layout.from_settings(settings)
        layer_datasets = silver_datasets_for(datasets)
        paths.extend(
            path for _, path in iter_layer_files(layout, symbols, layer_datasets)
        )

    for path in paths:
        key = object_key(settings, path)
        if not force and _already_synced(client, settings.s3_bucket, key, path):
            result.skipped += 1
            continue

        size = path.stat().st_size
        if dry_run:
            log.info("Would upload s3://%s/%s (%.1f KiB)", settings.s3_bucket, key, size / 1024)
        else:
            extra: dict[str, Any] = {"ContentType": PARQUET_CONTENT_TYPE}
            if settings.s3_acl:
                extra["ACL"] = settings.s3_acl
            client.upload_file(str(path), settings.s3_bucket, key, ExtraArgs=extra)
            log.info("Uploaded s3://%s/%s (%.1f KiB)", settings.s3_bucket, key, size / 1024)
        result.uploaded += 1
        result.bytes_uploaded += size
        result.files.append(f"s3://{settings.s3_bucket}/{key}")

    if upload_manifest and not dry_run:
        body = json.dumps(
            build_manifest(
                settings, include_layers=publish_layers, datasets=datasets
            ),
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        extra = {"ContentType": "application/json"}
        if settings.s3_acl:
            extra["ACL"] = settings.s3_acl
        client.put_object(
            Bucket=settings.s3_bucket,
            Key=manifest_key(settings),
            Body=body,
            **extra,
        )
        result.manifest_uploaded = True
        log.info(
            "Uploaded manifest s3://%s/%s",
            settings.s3_bucket,
            manifest_key(settings),
        )

    return result
