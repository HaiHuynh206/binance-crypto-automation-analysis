"""Time helpers. Everything is UTC and exact to the millisecond."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def ensure_utc(value: datetime) -> datetime:
    """Return a plain stdlib datetime in UTC.

    Airflow hands us ``pendulum.DateTime`` objects. Converting them with
    ``astimezone`` keeps the pendulum type, and pendulum 3 loses ``tzinfo`` when
    subtracting a timedelta from a DateTime carrying a stdlib tzinfo, which then
    blows up comparisons. Rebuilding a plain datetime keeps arithmetic safe.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    converted = value.astimezone(timezone.utc)
    return datetime(
        converted.year,
        converted.month,
        converted.day,
        converted.hour,
        converted.minute,
        converted.second,
        converted.microsecond,
        tzinfo=timezone.utc,
    )


def floor_hour(value: datetime) -> datetime:
    value = ensure_utc(value)
    return value.replace(minute=0, second=0, microsecond=0)


def to_ms(value: datetime) -> int:
    return int(ensure_utc(value).timestamp() * 1000)


def from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def iter_hours(start: datetime, end: datetime):
    """Yield every hour bucket that intersects the half-open interval [start, end)."""
    start = ensure_utc(start)
    end = ensure_utc(end)
    if end <= start:
        return
    cursor = floor_hour(start)
    while cursor < end:
        yield cursor
        cursor += HOUR


def parse_datetime(value: str) -> datetime:
    """Parse ISO-8601 (``2026-10-20``, ``2026-10-20T14:00:00Z``) or epoch milliseconds."""
    value = value.strip()
    if value.isdigit():
        return from_ms(int(value))
    normalised = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalised)
    return ensure_utc(parsed)
