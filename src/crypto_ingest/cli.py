"""Command line interface: local runs, backfills and lake inspection."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import timedelta

from .catalog import build_catalog
from .config import SUPPORTED_DATASETS, Settings
from .medallion import (
    DataQualityError,
    Layout,
    MedallionError,
    build_gold,
    build_silver,
    run_dq,
    silver_datasets_for,
)
from .pipeline import IngestError, ingest_window
from .s3 import sync_to_s3
from .state import CompletionStore
from .storage import ParquetLake
from .timeutil import parse_datetime, utc_now

log = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def _datasets(values: list[str] | None, settings: Settings) -> tuple[str, ...]:
    if not values:
        return settings.datasets
    selected: list[str] = []
    for value in values:
        selected.extend(part.strip() for part in value.split(",") if part.strip())
    unknown = sorted(set(selected) - set(SUPPORTED_DATASETS))
    if unknown:
        raise SystemExit(f"Unknown dataset(s): {unknown}")
    return tuple(dict.fromkeys(selected))


def _symbols(value: str | None, settings: Settings) -> tuple[str, ...]:
    if not value:
        return settings.symbols
    return tuple(s.strip().upper() for s in value.split(",") if s.strip())


def _run(settings: Settings, datasets: tuple[str, ...], args) -> int:
    end = parse_datetime(args.end) if args.end else utc_now()
    if args.start:
        start = parse_datetime(args.start)
    else:
        start = end - timedelta(hours=getattr(args, "hours", 2))

    if end <= start:
        raise SystemExit("--end must be after --start")

    failed = False
    for dataset in datasets:
        try:
            result = ingest_window(
                settings,
                dataset,
                start,
                end,
                symbols=_symbols(getattr(args, "symbols", None), settings),
                force=getattr(args, "force", False),
                refresh_hours=0 if getattr(args, "force", False) else 1,
            )
        except IngestError as exc:
            failed = True
            print(json.dumps(exc.result.as_dict(), indent=2))
        else:
            print(json.dumps(result.as_dict(), indent=2))
    return 1 if failed else 0


def _cmd_ingest(args) -> int:
    settings = Settings.from_env()
    return _run(settings, _datasets(args.dataset, settings), args)


def _cmd_backfill(args) -> int:
    settings = Settings.from_env()
    if args.end:
        end = parse_datetime(args.end)
    else:
        end = utc_now().replace(minute=0, second=0, microsecond=0)
    start = parse_datetime(args.start) if args.start else end - timedelta(days=args.days)
    args.start = start.isoformat()
    args.end = end.isoformat()
    args.hours = int((end - start).total_seconds() // 3600)
    return _run(settings, _datasets(args.dataset, settings), args)


def _cmd_status(args) -> int:
    settings = Settings.from_env()
    markers = CompletionStore(settings).summary()
    sizes = ParquetLake(settings).disk_usage()

    grouped: dict[str, dict] = defaultdict(
        lambda: {"partitions": 0, "rows": 0, "first_hour": None, "last_hour": None}
    )
    for marker in markers:
        group = grouped[marker["dataset"]]
        group["partitions"] += 1
        group["rows"] += int(marker.get("rows") or 0)
        hour = marker.get("hour")
        if hour:
            group["first_hour"] = min(group["first_hour"] or hour, hour)
            group["last_hour"] = max(group["last_hour"] or hour, hour)

    payload = {
        dataset: {
            **grouped.get(dataset, {"partitions": 0, "rows": 0, "first_hour": None, "last_hour": None}),
            "bytes": sizes.get(dataset, 0),
        }
        for dataset in settings.datasets
    }
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0

    print(f"data root: {settings.data_root}")
    print(f"{'dataset':<16}{'partitions':>12}{'rows':>14}{'size':>12}  window")
    for dataset, info in payload.items():
        window = (
            f"{info['first_hour']} .. {info['last_hour']}"
            if info["first_hour"]
            else "-"
        )
        print(
            f"{dataset:<16}{info['partitions']:>12,}{info['rows']:>14,}"
            f"{_human(info['bytes']):>12}  {window}"
        )
    return 0


def _human(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TiB"


def _cmd_catalog(args) -> int:
    views = build_catalog(Settings.from_env())
    for dataset, glob in views.items():
        print(f"{dataset:<16} -> {glob}")
    return 0


def _cmd_sync_s3(args) -> int:
    settings = Settings.from_env()
    if not settings.s3_configured:
        print(
            "CRYPTO_S3_BUCKET is not set. Add S3 settings to .env "
            "(see docs/share-s3.md).",
            file=sys.stderr,
        )
        return 2
    result = sync_to_s3(
        settings,
        _datasets(args.dataset, settings),
        _symbols(args.symbols, settings),
        dry_run=args.dry_run,
        force=args.force,
        upload_manifest=not args.no_manifest,
        include_layers=not args.bronze_only,
    )
    print(json.dumps(result.as_dict(), indent=2))
    return 0


def _layer_window(args):
    end = parse_datetime(args.end) if args.end else utc_now()
    start = parse_datetime(args.start) if args.start else end - timedelta(hours=args.hours)
    if end <= start:
        raise SystemExit("--end must be after --start")
    return start, end


def _csv(values: list[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    items: list[str] = []
    for value in values:
        items.extend(part.strip() for part in value.split(",") if part.strip())
    return tuple(dict.fromkeys(items)) or None


def _fail_on(args) -> tuple[str, ...]:
    raw = args.fail_on if args.fail_on is not None else os.getenv("CRYPTO_DQ_FAIL_ON", "missing")
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _print_results(results) -> None:
    print(json.dumps([result.as_dict() for result in results], indent=2))


def _cmd_silver(args) -> int:
    settings = Settings.from_env()
    start, end = _layer_window(args)
    try:
        results = build_silver(
            Layout.from_settings(settings),
            start,
            end,
            datasets=_csv(args.dataset) or silver_datasets_for(settings.datasets),
            symbols=_symbols(args.symbols, settings),
            force=args.force,
        )
    except MedallionError as exc:
        _print_results(exc.results)
        print(exc, file=sys.stderr)
        return 1
    _print_results(results)
    return 0


def _cmd_dq(args) -> int:
    settings = Settings.from_env()
    start, end = _layer_window(args)
    try:
        report = run_dq(
            Layout.from_settings(settings),
            start,
            end,
            fail_on=_fail_on(args),
            symbols=_symbols(args.symbols, settings),
            datasets=_csv(args.dataset),
        )
    except DataQualityError as exc:
        print(json.dumps(exc.report.as_dict(), indent=2))
        print(exc, file=sys.stderr)
        return 1
    print(json.dumps(report.as_dict(), indent=2))
    return 0


def _cmd_gold(args) -> int:
    settings = Settings.from_env()
    start, end = _layer_window(args)
    try:
        results = build_gold(
            Layout.from_settings(settings),
            start,
            end,
            marts=_csv(args.mart),
            symbols=_symbols(args.symbols, settings),
        )
    except MedallionError as exc:
        _print_results(exc.results)
        print(exc, file=sys.stderr)
        return 1
    _print_results(results)
    return 0


def _cmd_layers(args) -> int:
    """Silver, then the data quality gate, then Gold. Gold is skipped if the gate fails."""
    settings = Settings.from_env()
    layout = Layout.from_settings(settings)
    start, end = _layer_window(args)
    symbols = _symbols(args.symbols, settings)
    try:
        _print_results(
            build_silver(
                layout,
                start,
                end,
                datasets=silver_datasets_for(settings.datasets),
                symbols=symbols,
                force=args.force,
            )
        )
        report = run_dq(layout, start, end, fail_on=_fail_on(args), symbols=symbols)
        print(json.dumps(report.as_dict(), indent=2))
        _print_results(build_gold(layout, start, end, symbols=symbols))
    except MedallionError as exc:
        _print_results(exc.results)
        print(exc, file=sys.stderr)
        return 1
    except DataQualityError as exc:
        print(json.dumps(exc.report.as_dict(), indent=2))
        print(f"{exc}\nGold was not built.", file=sys.stderr)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crypto-ingest", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ing = subparsers.add_parser("ingest", help="Ingest the last N hours (default 2)")
    ing.add_argument("-d", "--dataset", action="append", help="Repeatable or comma separated")
    ing.add_argument("-s", "--symbols")
    ing.add_argument("--start", help="ISO-8601 UTC timestamp")
    ing.add_argument("--end", help="ISO-8601 UTC timestamp (default: now)")
    ing.add_argument("--hours", type=int, default=2)
    ing.add_argument("--force", action="store_true", help="Re-fetch even if marked done")
    ing.set_defaults(func=_cmd_ingest)

    back = subparsers.add_parser("backfill", help="Backfill a historical range")
    back.add_argument("-d", "--dataset", action="append")
    back.add_argument("-s", "--symbols")
    back.add_argument("--days", type=int, default=7)
    back.add_argument("--start")
    back.add_argument("--end")
    back.add_argument("--force", action="store_true")
    back.set_defaults(func=_cmd_backfill)

    status = subparsers.add_parser("status", help="Show ingested partitions and size")
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=_cmd_status)

    catalog = subparsers.add_parser("catalog", help="Rebuild DuckDB views")
    catalog.set_defaults(func=_cmd_catalog)

    s3 = subparsers.add_parser("sync-s3", help="Upload the lake to S3/R2/MinIO")
    s3.add_argument("-d", "--dataset", action="append", help="Repeatable or comma separated")
    s3.add_argument("-s", "--symbols")
    s3.add_argument("--dry-run", action="store_true", help="Only show what would be uploaded")
    s3.add_argument("--force", action="store_true", help="Upload even if unchanged")
    s3.add_argument("--no-manifest", action="store_true", help="Skip _manifest.json")
    s3.add_argument(
        "--bronze-only", action="store_true", help="Do not upload the Silver and Gold layers"
    )
    s3.set_defaults(func=_cmd_sync_s3)

    def add_window(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("-s", "--symbols")
        sub.add_argument("--start", help="ISO-8601 UTC timestamp")
        sub.add_argument("--end", help="ISO-8601 UTC timestamp (default: now)")
        sub.add_argument(
            "--hours",
            type=int,
            default=36,
            help="Window size when --start is not given (default 36). Whole UTC days are rebuilt.",
        )

    fail_help = (
        "Statuses that stop the run: missing, partial, id_gaps "
        "(comma separated, default: $CRYPTO_DQ_FAIL_ON or 'missing')"
    )

    silver = subparsers.add_parser("silver", help="Build the Silver layer from Bronze")
    silver.add_argument("-d", "--dataset", action="append", help="bars_1s, bars_1m, trades, ...")
    silver.add_argument(
        "--force", action="store_true", help="Rebuild days even if Silver is already up to date"
    )
    add_window(silver)
    silver.set_defaults(func=_cmd_silver)

    dq = subparsers.add_parser("dq", help="Write hourly data quality and stop on failures")
    dq.add_argument(
        "-d", "--dataset", action="append", help="Limit to bars_1s, bars_1m, trades, ..."
    )
    dq.add_argument("--fail-on", help=fail_help)
    add_window(dq)
    dq.set_defaults(func=_cmd_dq)

    gold = subparsers.add_parser("gold", help="Build the Gold marts from Silver")
    gold.add_argument("-m", "--mart", action="append", help="ohlcv, market_hourly, ...")
    add_window(gold)
    gold.set_defaults(func=_cmd_gold)

    layers = subparsers.add_parser("layers", help="Silver, then DQ gate, then Gold")
    layers.add_argument("--fail-on", help=fail_help)
    layers.add_argument(
        "--force", action="store_true", help="Rebuild Silver even if it is already up to date"
    )
    add_window(layers)
    layers.set_defaults(func=_cmd_layers)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _configure_logging(args.verbose)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        return 130
    except ValueError as exc:
        # Bad dataset/mart/status names and missing config: report without a traceback.
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
