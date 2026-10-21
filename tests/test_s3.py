from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("boto3")

from botocore.exceptions import ClientError

from crypto_ingest import s3
from crypto_ingest.config import Settings
from crypto_ingest.state import CompletionStore
from crypto_ingest.storage import ParquetLake


def make_settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "data_root": tmp_path,
        "symbols": ("BTCUSDT",),
        "datasets": ("klines_1m",),
        "spot_base_url": "https://api.binance.com",
        "futures_base_url": "https://fapi.binance.com",
        "timeout_seconds": 5,
        "max_retries": 1,
        "max_pages": 10,
        "weight_per_minute": 100,
        "lookback_hours": 1,
        "s3_bucket": "test-bucket",
        "s3_prefix": "crypto",
        "s3_endpoint_url": "http://localhost:9000",
        "s3_region": "us-east-1",
    }
    values.update(overrides)
    return Settings(**values)


def make_partition(settings: Settings, payload: bytes = b"parquet-bytes") -> Path:
    lake = ParquetLake(settings)
    hour = datetime(2026, 10, 21, 7, tzinfo=timezone.utc)
    path = lake.partition_path("klines_1m", "BTCUSDT", hour)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    CompletionStore(settings).mark_done(
        "klines_1m", "BTCUSDT", hour, rows=42, file_path=path
    )
    return path


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploads: list[str] = []

    def head_object(self, *, Bucket: str, Key: str):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        body = self.objects[Key]
        return {
            "ContentLength": len(body),
            "ETag": f'"{hashlib.md5(body).hexdigest()}"',
            "LastModified": datetime.now(tz=timezone.utc),
        }

    def upload_file(self, filename: str, bucket: str, key: str, ExtraArgs=None):
        self.objects[key] = Path(filename).read_bytes()
        self.uploads.append(key)

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs):
        self.objects[Key] = Body


def test_object_key_preserves_hive_layout(tmp_path):
    settings = make_settings(tmp_path)
    path = make_partition(settings)
    key = s3.object_key(settings, path)
    assert key == (
        "crypto/lake/exchange=binance/dataset=klines_1m/symbol=BTCUSDT/"
        "date=2026-10-21/hour=07/part-00000.parquet"
    )
    assert s3.manifest_key(settings) == "crypto/_manifest.json"


def test_manifest_summarises_rows_and_files(tmp_path):
    settings = make_settings(tmp_path)
    make_partition(settings, b"x" * 10)

    manifest = s3.build_manifest(settings)

    assert manifest["exchange"] == "binance"
    assert manifest["symbols"] == ["BTCUSDT"]
    dataset = manifest["datasets"]["klines_1m"]
    assert dataset["partitions"] == 1
    assert dataset["rows"] == 42
    assert dataset["files"] == 1
    assert dataset["bytes"] == 10
    assert dataset["first_hour"] == "2026-10-21T07:00:00+00:00"
    assert any(column["name"] == "close" for column in dataset["columns"])


def test_sync_uploads_then_skips_unchanged(tmp_path, monkeypatch):
    settings = make_settings(tmp_path)
    make_partition(settings)
    client = FakeS3()
    monkeypatch.setattr(s3, "_client", lambda _settings: client)

    first = s3.sync_to_s3(settings)
    assert first.uploaded == 1
    assert first.skipped == 0
    assert first.manifest_uploaded is True
    assert "crypto/_manifest.json" in client.objects

    second = s3.sync_to_s3(settings)
    assert second.uploaded == 0
    assert second.skipped == 1
    assert len(client.uploads) == 1

    forced = s3.sync_to_s3(settings, force=True)
    assert forced.uploaded == 1
    assert len(client.uploads) == 2


def test_dry_run_does_not_upload(tmp_path, monkeypatch):
    settings = make_settings(tmp_path)
    make_partition(settings)
    client = FakeS3()
    monkeypatch.setattr(s3, "_client", lambda _settings: client)

    result = s3.sync_to_s3(settings, dry_run=True)

    assert result.uploaded == 1
    assert result.dry_run is True
    assert client.objects == {}


def test_sync_requires_a_bucket(tmp_path):
    settings = make_settings(tmp_path, s3_bucket=None)
    with pytest.raises(ValueError, match="CRYPTO_S3_BUCKET"):
        s3.sync_to_s3(settings)
