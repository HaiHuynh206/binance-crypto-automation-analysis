from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_ingest.timeutil import ensure_utc, from_ms, iter_hours, parse_datetime, to_ms


def dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 21, hour, minute, tzinfo=timezone.utc)


def test_iter_hours_covers_partial_buckets():
    hours = list(iter_hours(dt(0, 30), dt(3)))
    assert hours == [dt(0), dt(1), dt(2)]


def test_iter_hours_empty_when_end_not_after_start():
    assert list(iter_hours(dt(3), dt(3))) == []


def test_parse_datetime_accepts_iso_and_epoch_ms():
    assert parse_datetime("2026-10-21T00:00:00Z") == dt(0)
    assert parse_datetime("2026-10-21") == dt(0)
    assert from_ms(to_ms(dt(5))) == dt(5)


def test_parse_datetime_rejects_garbage():
    with pytest.raises(ValueError):
        parse_datetime("not-a-date")


def test_ensure_utc_returns_plain_datetime_for_arithmetic():
    class AwkwardDateTime(datetime):
        """Mimics pendulum: astimezone() keeps the subclass."""

        def astimezone(self, tz=None):
            return AwkwardDateTime(
                self.year, self.month, self.day, self.hour, self.minute,
                self.second, self.microsecond, tzinfo=tz,
            )

    value = AwkwardDateTime(2026, 10, 21, 12, tzinfo=timezone.utc)
    result = ensure_utc(value)
    assert type(result) is datetime
    assert (result - timedelta(hours=1)).tzinfo is timezone.utc
