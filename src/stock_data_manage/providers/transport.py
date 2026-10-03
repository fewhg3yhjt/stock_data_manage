from __future__ import annotations

import re
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence
from threading import Lock, BoundedSemaphore
from time import monotonic, sleep
from contextlib import contextmanager
from math import isfinite
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .contracts import EndpointContract, FailureClass, HttpResponse, ProviderContractError


class HttpTransport(Protocol):
    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse: ...


class RequestPacer:
    """Pace physical calls without changing the delegated transport/session/retries."""

    def __init__(self, *, clock=monotonic, wait=sleep):
        self.clock = clock
        self.wait = wait
        self._groups = {}

    def configure(self, group: str, interval_seconds: float, concurrency: int = 1) -> None:
        if not group or not isfinite(interval_seconds) or interval_seconds < 0 or concurrency < 1:
            raise ValueError("invalid request pacing policy")
        if group in self._groups:
            current = self._groups[group]
            if current["started"]:
                if interval_seconds > current["interval"] or concurrency < current["concurrency"]:
                    raise ValueError("configure shared request policies before issuing requests")
                return
            interval_seconds = max(interval_seconds, current["interval"])
            concurrency = min(concurrency, current["concurrency"])
        self._groups[group] = {
            "interval": interval_seconds, "concurrency": concurrency,
            "lock": Lock(), "semaphore": BoundedSemaphore(concurrency), "last_start": None, "started": False,
        }

    @contextmanager
    def request(self, group: str):
        policy = self._groups[group]
        with policy["semaphore"]:
            with policy["lock"]:
                if policy["last_start"] is not None:
                    remaining = policy["interval"] - (self.clock() - policy["last_start"])
                    if remaining > 0:
                        self.wait(remaining)
                policy["last_start"] = self.clock()
                policy["started"] = True
            yield


@dataclass(slots=True)
class PacedTransport:
    transport: HttpTransport
    pacer: RequestPacer
    group: str

    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse:
        with self.pacer.request(self.group):
            return self.transport.get(url, params=params, timeout_seconds=timeout_seconds)


@dataclass(slots=True)
class UrlLibTransport:
    user_agent: str = "stock-data-manage/0.1"

    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse:
        request = Request(
            f"{url}?{urlencode(params)}",
            headers={"User-Agent": self.user_agent, "Accept": "application/json,text/plain,*/*"},
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return HttpResponse(int(response.status), dict(response.headers.items()), response.read())
        except HTTPError as exc:
            return HttpResponse(exc.code, dict(exc.headers.items()) if exc.headers else {}, exc.read())
        except (TimeoutError, socket.timeout) as exc:
            raise ProviderContractError("provider request timed out", FailureClass.TIMEOUT, retryable=True) from exc
        except URLError as exc:
            raise ProviderContractError(
                f"provider connection failed: {exc.reason}", FailureClass.CONNECTION, retryable=True
            ) from exc


@dataclass(frozen=True, slots=True)
class RealtimeFetchResponse:
    rows: tuple[Mapping[str, Any], ...]
    requested_symbols: tuple[str, ...]
    response_statuses: tuple[int, ...] = ()
    field_semantics: tuple[str, ...] = ()
    units: tuple[str, ...] = ()
    returned_row_count: int | None = None
    returned_first_key: str | None = None
    returned_last_key: str | None = None

    @property
    def returned_symbols(self) -> frozenset[str]:
        return frozenset(str(row["symbol"]) for row in self.rows)


@dataclass(frozen=True, slots=True)
class SnapshotFetchResponse:
    rows: tuple[Mapping[str, Any], ...]
    requested_symbols: tuple[str, ...]

    @property
    def returned_symbols(self) -> frozenset[str]:
        return frozenset(str(row["symbol"]) for row in self.rows)


def parse_provider_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    if len(text) == 12 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d%H%M")
    if len(text) == 14 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d%H%M%S")
    return datetime.fromisoformat(text.replace("/", "-"))


def parse_tencent_snapshot_line(line: str) -> dict[str, Any] | None:
    match = re.match(r"\s*v_([^=]+)=\"(.*?)\";?\s*$", line)
    if match is None:
        return None
    symbol, body = match.groups()
    parts = body.split("~")

    def item(index: int) -> str | None:
        return None if index >= len(parts) or parts[index] in {"", "-"} else parts[index]

    quote_time = item(30)
    if quote_time is None or len(quote_time) < 8:
        return None
    timestamp = parse_provider_datetime(quote_time)
    return {
        "symbol": symbol, "trade_date": timestamp.date().isoformat(), "bar_time": timestamp.isoformat(),
        "name": item(1), "open": item(5), "high": item(33), "low": item(34), "close": item(3),
        "pre_close": item(4), "volume": item(6), "amount": item(37), "volume_unit": "lot",
        "amount_unit": "wan_cny", "quote_time": quote_time,
    }


def parse_sina_snapshot_line(line: str) -> dict[str, Any] | None:
    match = re.match(r'\s*var\s+hq_str_([^=]+)="(.*?)";?\s*$', line)
    if match is None:
        return None
    symbol, body = match.groups()
    parts = body.split(",")
    if len(parts) < 32 or not parts[30] or not parts[31]:
        return None
    try:
        timestamp = datetime.strptime(f"{parts[30]} {parts[31]}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return {
        "symbol": symbol, "trade_date": timestamp.date().isoformat(), "bar_time": timestamp.isoformat(),
        "name": parts[0] or None, "open": parts[1] or None, "high": parts[4] or None,
        "low": parts[5] or None, "close": parts[3] or None, "pre_close": parts[2] or None,
        "volume": parts[8] or None, "amount": parts[9] or None, "volume_unit": "share",
        "amount_unit": "cny", "quote_time": f"{parts[30]} {parts[31]}",
    }


def tdx_code(symbol: str) -> str:
    text = str(symbol).lower()
    return text[2:] if len(text) >= 8 and text[:2] in {"sh", "sz", "bj"} else text


def call_tdx_bars(client: object, code: str, category: int, count: int) -> object:
    bars = getattr(client, "bars", None)
    if not callable(bars):
        raise ProviderContractError("TDX client has no bars method", FailureClass.SCHEMA_CHANGED, retryable=False)
    try:
        return bars(symbol=code, frequency=category, offset=count)
    except TypeError:
        return bars(symbol=code, category=category, offset=count)


def tdx_records(value: object) -> list[object]:
    if value is None:
        return []
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return list(to_dict("records") or [])
        except TypeError:
            return list(to_dict(orient="records") or [])
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def tdx_value(item: object, *names: str) -> object:
    if isinstance(item, Mapping):
        for name in names:
            if name in item:
                return item[name]
        return None
    for name in names:
        if hasattr(item, name):
            return getattr(item, name)
    return None
