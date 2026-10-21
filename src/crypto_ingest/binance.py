"""Binance REST fetchers.

Each fetcher returns a list of dicts that already match the Arrow schema for its
dataset (see :mod:`crypto_ingest.schemas`). Pagination is cursor based so a full
hour is always either fetched completely or the call fails loudly.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

from .http import BinanceHTTP
from .timeutil import from_ms, to_ms

INTERVAL_MS = {"1s": 1_000, "1m": 60_000}
OI_PERIOD_MS = 5 * 60 * 1_000

# Binance request weights (conservative upper bounds).
WEIGHT = {"klines": 2, "agg_trades": 4, "funding_rate": 1, "open_interest": 1}


class IncompleteFetch(RuntimeError):
    """Raised when a page cap is hit before the requested window is covered."""


def _f(value) -> float:
    return float(value)


class BinanceClient:
    def __init__(self, http: BinanceHTTP, spot_base: str, futures_base: str, max_pages: int):
        self.http = http
        self.spot_base = spot_base
        self.futures_base = futures_base
        self.max_pages = max_pages
        self._now = lambda: datetime.now(tz=timezone.utc)

    def _pages(self):
        for page in range(1, self.max_pages + 1):
            yield page, page == self.max_pages
        raise IncompleteFetch(f"Exceeded CRYPTO_MAX_PAGES={self.max_pages}")


def fetch_klines(
    client: BinanceClient, symbol: str, interval: str, start: datetime, end: datetime
) -> list[dict]:
    if interval not in INTERVAL_MS:
        raise ValueError(f"Unsupported interval: {interval}")

    url = f"{client.spot_base}/api/v3/klines"
    step_ms = INTERVAL_MS[interval]
    start_ms, end_ms = to_ms(start), to_ms(end)
    cursor, rows = start_ms, []
    ingested_at = client._now()

    for _, is_last in client._pages():
        payload = client.http.get(
            url,
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms - 1,
                "limit": 1000,
            },
            weight=WEIGHT["klines"],
        )
        if not payload:
            break
        for k in payload:
            open_ms = int(k[0])
            if start_ms <= open_ms < end_ms:
                rows.append(
                    {
                        "symbol": symbol,
                        "interval": interval,
                        "open_time": from_ms(open_ms),
                        "close_time": from_ms(int(k[6])),
                        "open": _f(k[1]),
                        "high": _f(k[2]),
                        "low": _f(k[3]),
                        "close": _f(k[4]),
                        "volume": _f(k[5]),
                        "quote_volume": _f(k[7]),
                        "trades": int(k[8]),
                        "taker_buy_base": _f(k[9]),
                        "taker_buy_quote": _f(k[10]),
                        "ingested_at": ingested_at,
                    }
                )
        next_cursor = int(payload[-1][0]) + step_ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if cursor >= end_ms:
            break
    else:  # pragma: no cover - defensive, _pages() raises first
        raise IncompleteFetch(f"Klines pagination did not finish for {symbol} {interval}")

    return rows


def fetch_agg_trades(
    client: BinanceClient, symbol: str, start: datetime, end: datetime
) -> list[dict]:
    url = f"{client.spot_base}/api/v3/aggTrades"
    start_ms, end_ms = to_ms(start), to_ms(end)
    ingested_at = client._now()
    rows: list[dict] = []
    from_id: int | None = None

    for _, is_last in client._pages():
        params: dict[str, int | str] = {"symbol": symbol, "limit": 1000}
        if from_id is None:
            params["startTime"] = start_ms
            params["endTime"] = end_ms - 1
        else:
            params["fromId"] = from_id
        payload = client.http.get(url, params, weight=WEIGHT["agg_trades"])
        if not payload:
            break
        for trade in payload:
            trade_ms = int(trade["T"])
            if start_ms <= trade_ms < end_ms:
                rows.append(
                    {
                        "symbol": symbol,
                        "agg_trade_id": int(trade["a"]),
                        "price": _f(trade["p"]),
                        "qty": _f(trade["q"]),
                        "first_trade_id": int(trade["f"]),
                        "last_trade_id": int(trade["l"]),
                        "trade_time": from_ms(trade_ms),
                        "is_buyer_maker": bool(trade["m"]),
                        "ingested_at": ingested_at,
                    }
                )
        if int(payload[-1]["T"]) >= end_ms or len(payload) < 1000:
            break
        from_id = int(payload[-1]["a"]) + 1
        if is_last:
            raise IncompleteFetch(f"aggTrades truncated for {symbol} at {from_ms(from_id)}")

    return rows


def fetch_funding_rate(
    client: BinanceClient, symbol: str, start: datetime, end: datetime
) -> list[dict]:
    url = f"{client.futures_base}/fapi/v1/fundingRate"
    start_ms, end_ms = to_ms(start), to_ms(end)
    cursor, ingested_at, rows = start_ms, client._now(), []

    for _, is_last in client._pages():
        payload = client.http.get(
            url,
            {"symbol": symbol, "startTime": cursor, "endTime": end_ms - 1, "limit": 1000},
            weight=WEIGHT["funding_rate"],
        )
        if not payload:
            break
        for item in payload:
            funding_ms = int(item["fundingTime"])
            if start_ms <= funding_ms < end_ms:
                rows.append(
                    {
                        "symbol": symbol,
                        "funding_time": from_ms(funding_ms),
                        "funding_rate": _f(item["fundingRate"]),
                        "mark_price": _f(item.get("markPrice") or 0.0),
                        "ingested_at": ingested_at,
                    }
                )
        next_cursor = int(payload[-1]["fundingTime"]) + 1
        if next_cursor <= cursor:
            break
        if next_cursor >= end_ms or len(payload) < 1000:
            break
        if is_last:
            raise IncompleteFetch(f"fundingRate truncated for {symbol} at {from_ms(next_cursor)}")
        cursor = next_cursor

    return rows


def fetch_open_interest(
    client: BinanceClient, symbol: str, start: datetime, end: datetime
) -> list[dict]:
    url = f"{client.futures_base}/futures/data/openInterestHist"
    start_ms, end_ms = to_ms(start), to_ms(end)
    cursor, ingested_at, rows = start_ms, client._now(), []

    for _, is_last in client._pages():
        payload = client.http.get(
            url,
            {
                "symbol": symbol,
                "period": "5m",
                "startTime": cursor,
                "endTime": end_ms - 1,
                "limit": 500,
            },
            weight=WEIGHT["open_interest"],
        )
        if not payload:
            break
        for item in payload:
            ts_ms = int(item["timestamp"])
            if start_ms <= ts_ms < end_ms:
                rows.append(
                    {
                        "symbol": symbol,
                        "ts": from_ms(ts_ms),
                        "open_interest": _f(item["sumOpenInterest"]),
                        "open_interest_value": _f(item["sumOpenInterestValue"]),
                        "ingested_at": ingested_at,
                    }
                )
        next_cursor = int(payload[-1]["timestamp"]) + OI_PERIOD_MS
        if next_cursor <= cursor:
            break
        if next_cursor >= end_ms or len(payload) < 500:
            break
        if is_last:
            raise IncompleteFetch(f"openInterestHist truncated for {symbol} at {from_ms(next_cursor)}")
        cursor = next_cursor

    return rows


FETCHERS: dict[str, Callable[[BinanceClient, str, datetime, datetime], list[dict]]] = {
    "klines_1s": lambda c, s, a, b: fetch_klines(c, s, "1s", a, b),
    "klines_1m": lambda c, s, a, b: fetch_klines(c, s, "1m", a, b),
    "agg_trades": fetch_agg_trades,
    "funding_rate": fetch_funding_rate,
    "open_interest": fetch_open_interest,
}
