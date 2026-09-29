from __future__ import annotations

import socket
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .contracts import (
    EndpointContract,
    FailureClass,
    HttpResponse,
    ProviderContractError,
    WindowStatus,
)
from .base import FetchResult
from ..routing.capabilities import ProviderCapability


class HttpTransport(Protocol):
    def get(
        self, url: str, *, params: Mapping[str, str], timeout_seconds: float
    ) -> HttpResponse: ...


@dataclass(slots=True)
class UrlLibTransport:
    user_agent: str = "stock-data-manage/0.1"

    def get(
        self, url: str, *, params: Mapping[str, str], timeout_seconds: float
    ) -> HttpResponse:
        request = Request(
            f"{url}?{urlencode(params)}",
            headers={"User-Agent": self.user_agent, "Accept": "application/json,text/plain,*/*"},
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return HttpResponse(
                    status_code=int(response.status),
                    headers=dict(response.headers.items()),
                    body=response.read(),
                )
        except HTTPError as exc:
            return HttpResponse(
                status_code=exc.code,
                headers=dict(exc.headers.items()) if exc.headers else {},
                body=exc.read(),
            )
        except (TimeoutError, socket.timeout) as exc:
            raise ProviderContractError(
                "provider request timed out", FailureClass.TIMEOUT, retryable=True
            ) from exc
        except URLError as exc:
            raise ProviderContractError(
                f"provider connection failed: {exc.reason}",
                FailureClass.CONNECTION,
                retryable=True,
            ) from exc


@dataclass(slots=True)
class TencentDailyProvider:
    transport: HttpTransport
    name: str = "tencent"
    endpoint: str = "recent_history"
    capability_priority: int = 90
    capability_version: str = "tencent-kline-v1"
    timeout_seconds: float = 15.0
    max_rows: int = 1024
    url: str = "https://web.ifzq.gtimg.cn/appstock/app/kline/kline"
    contract: EndpointContract = field(
        default_factory=lambda: EndpointContract(
            frozenset({"symbol", "trade_date", "open", "high", "low", "close", "volume"}),
            max_rows_per_request=1024,
            supports_pagination=False,
        )
    )

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        returned_rows: list[dict[str, Any]] = []
        statuses: list[WindowStatus] = []
        response_statuses: list[int] = []
        for symbol in requested:
            response = self.transport.get(
                self.url,
                params={"param": f"{symbol},day,,,{self.max_rows}"},
                timeout_seconds=self.timeout_seconds,
            )
            response_statuses.append(response.status_code)
            payload = self.contract.parse_json(response)
            if not isinstance(payload, dict) or payload.get("code") not in {0, "0", None}:
                raise ProviderContractError(
                    "Tencent response envelope changed",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            stock = ((payload.get("data") or {}).get(symbol) or {})
            bars = stock.get("day") or []
            parsed = [
                {
                    "symbol": symbol,
                    "trade_date": item[0],
                    "open": item[1],
                    "close": item[2],
                    "high": item[3],
                    "low": item[4],
                    "volume": item[5],
                    "amount": None,
                }
                for item in bars
                if isinstance(item, list) and len(item) >= 6
            ]
            statuses.append(self.contract.validate_rows(parsed))
            returned_rows.extend(parsed)
            rows.extend(item for item in parsed if str(item["trade_date"]) == trade_date.isoformat())
        return FetchResult(
            tuple(rows),
            requested,
            _combined_status(statuses, bool(rows)),
            tuple(response_statuses),
            ("trade_date", "open", "high", "low", "close", "volume", "amount"),
            ("volume:unverified", "amount:unverified"),
            len(returned_rows),
            str(returned_rows[0]["trade_date"]) if returned_rows else None,
            str(returned_rows[-1]["trade_date"]) if returned_rows else None,
        )


@dataclass(slots=True)
class SinaDailyProvider:
    transport: HttpTransport
    name: str = "sina"
    endpoint: str = "full_history"
    capability_priority: int = 100
    capability_version: str = "sina-cn-marketdata-v1"
    timeout_seconds: float = 15.0
    max_rows: int = 1023
    url: str = (
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
        "CN_MarketData.getKLineData"
    )
    contract: EndpointContract = field(
        default_factory=lambda: EndpointContract(
            frozenset({"symbol", "trade_date", "open", "high", "low", "close", "volume"}),
            max_rows_per_request=1023,
            supports_pagination=False,
        )
    )

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        returned_rows: list[dict[str, Any]] = []
        statuses: list[WindowStatus] = []
        response_statuses: list[int] = []
        for symbol in requested:
            response = self.transport.get(
                self.url,
                params={
                    "symbol": symbol,
                    "scale": "240",
                    "ma": "no",
                    "datalen": str(self.max_rows),
                },
                timeout_seconds=self.timeout_seconds,
            )
            response_statuses.append(response.status_code)
            payload = self.contract.parse_json(response)
            if payload is None:
                parsed: list[dict[str, Any]] = []
            elif not isinstance(payload, list):
                raise ProviderContractError(
                    "Sina response envelope changed",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            else:
                parsed = [
                    {
                        "symbol": symbol,
                        "trade_date": item.get("day"),
                        "open": item.get("open"),
                        "high": item.get("high"),
                        "low": item.get("low"),
                        "close": item.get("close"),
                        "volume": item.get("volume"),
                        "amount": item.get("amount"),
                    }
                    for item in payload
                    if isinstance(item, dict)
            ]
            statuses.append(self.contract.validate_rows(parsed))
            returned_rows.extend(parsed)
            rows.extend(item for item in parsed if str(item["trade_date"]) == trade_date.isoformat())
        return FetchResult(
            tuple(rows),
            requested,
            _combined_status(statuses, bool(rows)),
            tuple(response_statuses),
            ("trade_date", "open", "high", "low", "close", "volume", "amount"),
            ("volume:unverified", "amount:unverified"),
            len(returned_rows),
            str(returned_rows[0]["trade_date"]) if returned_rows else None,
            str(returned_rows[-1]["trade_date"]) if returned_rows else None,
        )


def _combined_status(statuses: list[WindowStatus], target_rows_found: bool) -> WindowStatus:
    if WindowStatus.TRUNCATED in statuses:
        return WindowStatus.TRUNCATED
    if not target_rows_found:
        return WindowStatus.TEMPORARY_EMPTY
    if WindowStatus.PARTIAL in statuses:
        return WindowStatus.PARTIAL
    return WindowStatus.COMPLETE


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


@dataclass(slots=True)
class TencentSnapshotProvider:
    transport: HttpTransport
    capability: ProviderCapability
    name: str = "tencent"
    endpoint: str = "bulk_snapshot"
    timeout_seconds: float = 10.0
    max_symbols_per_request: int = 100
    url: str = "https://qt.gtimg.cn/q="

    @property
    def capability_version(self) -> str:
        return self.capability.version

    @property
    def capability_priority(self) -> int:
        return self.capability.priority

    @property
    def quality_status(self):
        from ..domain import QualityStatus

        return QualityStatus.PROVISIONAL

    source_method: str = "snapshot"

    def fetch_snapshot(self, symbols: Sequence[str], as_of: datetime) -> SnapshotFetchResponse:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        batch_size = max(1, self.max_symbols_per_request)
        for offset in range(0, len(requested), batch_size):
            batch = requested[offset : offset + batch_size]
            response = self.transport.get(
                self.url + ",".join(batch), params={}, timeout_seconds=self.timeout_seconds
            )
            if response.status_code >= 400:
                EndpointContract(frozenset()).parse_json(response)
            for line in response.text.splitlines():
                parsed = _parse_tencent_snapshot_line(line)
                if parsed is not None and parsed["symbol"] in batch:
                    rows.append(parsed)
        return SnapshotFetchResponse(tuple(rows), requested)

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        response = self.fetch_snapshot(
            symbols, datetime(trade_date.year, trade_date.month, trade_date.day, 15, 16, tzinfo=timezone.utc)
        )
        rows = tuple(row for row in response.rows if row.get("trade_date") == trade_date.isoformat())
        return FetchResult(rows, response.requested_symbols)


@dataclass(slots=True)
class SinaSnapshotProvider:
    transport: HttpTransport
    capability: ProviderCapability
    name: str = "sina"
    endpoint: str = "snapshot"
    timeout_seconds: float = 10.0
    max_symbols_per_request: int = 100
    url: str = "https://hq.sinajs.cn/list="

    @property
    def capability_version(self) -> str:
        return self.capability.version

    @property
    def capability_priority(self) -> int:
        return self.capability.priority

    @property
    def quality_status(self):
        from ..domain import QualityStatus

        return QualityStatus.PROVISIONAL

    source_method: str = "snapshot"

    def fetch_snapshot(self, symbols: Sequence[str], as_of: datetime) -> SnapshotFetchResponse:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        batch_size = max(1, self.max_symbols_per_request)
        for offset in range(0, len(requested), batch_size):
            batch = requested[offset : offset + batch_size]
            response = self.transport.get(
                self.url + ",".join(batch), params={}, timeout_seconds=self.timeout_seconds
            )
            if response.status_code >= 400:
                EndpointContract(frozenset()).parse_json(response)
            for line in response.text.splitlines():
                parsed = _parse_sina_snapshot_line(line)
                if parsed is not None and parsed["symbol"] in batch:
                    rows.append(parsed)
        return SnapshotFetchResponse(tuple(rows), requested)

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        response = self.fetch_snapshot(
            symbols, datetime(trade_date.year, trade_date.month, trade_date.day, 15, 16, tzinfo=timezone.utc)
        )
        rows = tuple(row for row in response.rows if row.get("trade_date") == trade_date.isoformat())
        return FetchResult(rows, response.requested_symbols)


@dataclass(slots=True)
class TencentMinuteProvider:
    transport: HttpTransport
    capability: ProviderCapability
    name: str = "tencent"
    endpoint: str = "native_1m"
    timeout_seconds: float = 10.0
    count: int = 120
    url: str = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"

    @property
    def capability_version(self) -> str:
        return self.capability.version

    def fetch_realtime_minute(
        self, symbols: Sequence[str], as_of: datetime
    ) -> RealtimeFetchResponse:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        response_statuses: list[int] = []
        for symbol in requested:
            response = self.transport.get(
                self.url,
                params={"param": f"{symbol},m1,,{self.count}"},
                timeout_seconds=self.timeout_seconds,
            )
            response_statuses.append(response.status_code)
            payload = EndpointContract(frozenset()).parse_json(response)
            if not isinstance(payload, dict) or payload.get("code") not in {0, "0", None}:
                raise ProviderContractError(
                    "Tencent minute response envelope changed",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            bars = (((payload.get("data") or {}).get(symbol) or {}).get("m1") or [])
            for item in bars:
                if not isinstance(item, list) or len(item) < 6:
                    continue
                parsed_time = _parse_provider_datetime(item[0])
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": parsed_time.date().isoformat(),
                        "bar_time": parsed_time.isoformat(),
                        "open": item[1],
                        "close": item[2],
                        "high": item[3],
                        "low": item[4],
                        "volume": item[5],
                        "amount": None,
                    }
                )
        as_of_naive = as_of.replace(tzinfo=None)
        rows = [row for row in rows if _parse_provider_datetime(row["bar_time"]) <= as_of_naive]
        return RealtimeFetchResponse(
            tuple(rows),
            requested,
            tuple(response_statuses),
            ("trade_date", "bar_time", "open", "high", "low", "close", "volume"),
            tuple(f"volume:{'share' if symbol[2:].startswith(('688', '689')) else 'lot'}" for symbol in requested),
            len(rows),
            str(rows[0]["bar_time"]) if rows else None,
            str(rows[-1]["bar_time"]) if rows else None,
        )


@dataclass(slots=True)
class SinaMinuteProvider:
    transport: HttpTransport
    capability: ProviderCapability
    name: str = "sina"
    endpoint: str = "native_5m"
    timeout_seconds: float = 15.0
    scale: int = 5
    count: int = 120
    url: str = (
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
        "CN_MarketData.getKLineData"
    )

    @property
    def capability_version(self) -> str:
        return self.capability.version

    def fetch_realtime_minute(
        self, symbols: Sequence[str], as_of: datetime
    ) -> RealtimeFetchResponse:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        response_statuses: list[int] = []
        for symbol in requested:
            response = self.transport.get(
                self.url,
                params={
                    "symbol": symbol,
                    "scale": str(self.scale),
                    "ma": "no",
                    "datalen": str(self.count),
                },
                timeout_seconds=self.timeout_seconds,
            )
            response_statuses.append(response.status_code)
            payload = EndpointContract(frozenset()).parse_json(response)
            if payload is None:
                continue
            if not isinstance(payload, list):
                raise ProviderContractError(
                    "Sina minute response envelope changed",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            for item in payload:
                if not isinstance(item, dict) or not item.get("day"):
                    continue
                parsed_time = _parse_provider_datetime(item["day"])
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": parsed_time.date().isoformat(),
                        "bar_time": parsed_time.isoformat(),
                        "open": item.get("open"),
                        "close": item.get("close"),
                        "high": item.get("high"),
                        "low": item.get("low"),
                        "volume": item.get("volume"),
                        "amount": item.get("amount"),
                    }
                )
        as_of_naive = as_of.replace(tzinfo=None)
        rows = [row for row in rows if _parse_provider_datetime(row["bar_time"]) <= as_of_naive]
        return RealtimeFetchResponse(
            tuple(rows),
            requested,
            tuple(response_statuses),
            ("trade_date", "bar_time", "open", "high", "low", "close", "volume", "amount"),
            tuple("volume:share" for _ in requested),
            len(rows),
            str(rows[0]["bar_time"]) if rows else None,
            str(rows[-1]["bar_time"]) if rows else None,
        )


@dataclass(slots=True)
class TdxMinuteProvider:
    """Adapter around a TDX-compatible client (for example mootdx).

    The client is injected so the core package does not depend on a native
    TDX library or its socket lifecycle.  A client only needs a ``bars``
    method accepting ``symbol`` and either ``frequency`` or ``category`` and
    returning mappings, row objects, or a pandas-like object with
    ``to_dict('records')``.
    """

    client_factory: Callable[[], object]
    capability: ProviderCapability
    name: str = "tdx"
    endpoint: str = "delayed_1m"
    timeout_seconds: float = 10.0
    count: int = 240
    frequency: int = 1
    frequency_codes: Mapping[int, int] = field(default_factory=lambda: {1: 8, 5: 0})

    @property
    def capability_version(self) -> str:
        return self.capability.version

    def fetch_realtime_minute(
        self, symbols: Sequence[str], as_of: datetime
    ) -> RealtimeFetchResponse:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        client = self.client_factory()
        try:
            category = self.frequency_codes.get(self.frequency, self.frequency)
            for source_symbol in requested:
                code = _tdx_code(source_symbol)
                try:
                    bars = _call_tdx_bars(client, code, category, self.count)
                except ProviderContractError:
                    raise
                except (TimeoutError, OSError, ConnectionError) as exc:
                    raise ProviderContractError(
                        f"TDX minute request failed for {source_symbol}",
                        FailureClass.CONNECTION,
                        retryable=True,
                    ) from exc
                for item in _tdx_records(bars):
                    timestamp = _tdx_value(item, "datetime", "bar_time", "time", "date")
                    if timestamp in (None, ""):
                        continue
                    parsed_time = _parse_provider_datetime(timestamp)
                    open_value = _tdx_value(item, "open", "opening")
                    close_value = _tdx_value(item, "close", "price")
                    high_value = _tdx_value(item, "high")
                    low_value = _tdx_value(item, "low")
                    volume_value = _tdx_value(item, "volume", "vol")
                    if any(value is None for value in (open_value, close_value, high_value, low_value, volume_value)):
                        raise ProviderContractError(
                            f"TDX minute row schema changed for {source_symbol}",
                            FailureClass.SCHEMA_CHANGED,
                            retryable=False,
                        )
                    rows.append(
                        {
                            "symbol": source_symbol,
                            "trade_date": parsed_time.date().isoformat(),
                            "bar_time": parsed_time.isoformat(),
                            "open": open_value,
                            "close": close_value,
                            "high": high_value,
                            "low": low_value,
                            "volume": volume_value,
                            "amount": _tdx_value(item, "amount", "turnover"),
                        }
                    )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        return RealtimeFetchResponse(
            tuple(rows),
            requested,
            (),
            ("trade_date", "bar_time", "open", "high", "low", "close", "volume", "amount"),
            ("volume:client_defined", "amount:client_defined"),
            len(rows),
            str(rows[0]["bar_time"]) if rows else None,
            str(rows[-1]["bar_time"]) if rows else None,
        )


def _tdx_code(symbol: str) -> str:
    text = str(symbol).lower()
    if len(text) >= 8 and text[:2] in {"sh", "sz", "bj"}:
        return text[2:]
    return text


def _call_tdx_bars(client: object, code: str, category: int, count: int) -> object:
    bars = getattr(client, "bars", None)
    if not callable(bars):
        raise ProviderContractError(
            "TDX client has no bars method", FailureClass.SCHEMA_CHANGED, retryable=False
        )
    try:
        return bars(symbol=code, frequency=category, offset=count)
    except TypeError:
        return bars(symbol=code, category=category, offset=count)


def _tdx_records(value: object) -> list[object]:
    if value is None:
        return []
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            records = to_dict("records")
        except TypeError:
            records = to_dict(orient="records")
        return list(records or [])
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _tdx_value(item: object, *names: str) -> object:
    if isinstance(item, Mapping):
        for name in names:
            if name in item:
                return item[name]
        return None
    for name in names:
        if hasattr(item, name):
            return getattr(item, name)
    return None


def _parse_tencent_snapshot_line(line: str) -> dict[str, Any] | None:
    match = re.match(r"\s*v_([^=]+)=\"(.*?)\";?\s*$", line)
    if match is None:
        return None
    symbol, body = match.groups()
    parts = body.split("~")

    def item(index: int) -> str | None:
        if index >= len(parts) or parts[index] in {"", "-"}:
            return None
        return parts[index]

    quote_time = item(30)
    if quote_time is None or len(quote_time) < 8:
        return None
    timestamp = _parse_provider_datetime(quote_time)
    return {
        "symbol": symbol,
        "trade_date": timestamp.date().isoformat(),
        "bar_time": timestamp.isoformat(),
        "name": item(1),
        "open": item(5),
        "high": item(33),
        "low": item(34),
        "close": item(3),
        "pre_close": item(4),
        "volume": item(6),
        "amount": item(37),
        "volume_unit": "lot",
        "amount_unit": "wan_cny",
        "quote_time": quote_time,
    }


def _parse_sina_snapshot_line(line: str) -> dict[str, Any] | None:
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
        "symbol": symbol,
        "trade_date": timestamp.date().isoformat(),
        "bar_time": timestamp.isoformat(),
        "name": parts[0] or None,
        "open": parts[1] or None,
        "high": parts[4] or None,
        "low": parts[5] or None,
        "close": parts[3] or None,
        "pre_close": parts[2] or None,
        "volume": parts[8] or None,
        "amount": parts[9] or None,
        "volume_unit": "share",
        "amount_unit": "cny",
        "quote_time": f"{parts[30]} {parts[31]}",
    }


def _parse_provider_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    if len(text) == 12 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d%H%M")
    if len(text) == 14 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d%H%M%S")
    return datetime.fromisoformat(text.replace("/", "-"))
