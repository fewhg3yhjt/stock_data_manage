from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

from ..contracts import EndpointContract, FailureClass, ProviderContractError
from ..transport import HttpTransport


@dataclass(frozen=True, slots=True)
class RealtimeQuoteRecord:
    symbol: str
    exchange: str
    name: str | None
    price: float | None
    change_pct: float | None
    change_amount: float | None
    open: float | None
    high: float | None
    low: float | None
    pre_close: float | None
    volume: float | None
    amount: float | None
    total_market_cap: float | None
    float_market_cap: float | None
    pe_dynamic: float | None
    pb: float | None
    turnover_rate: float | None
    source_time: str | None


@dataclass(frozen=True, slots=True)
class IntradayTrendRecord:
    symbol: str
    bar_time: str
    price: float | None
    average_price: float | None
    high: float | None
    low: float | None
    volume: float | None
    amount: float | None


@dataclass(frozen=True, slots=True)
class RealtimeQuoteFetchResult:
    records: tuple[RealtimeQuoteRecord, ...]
    requested_symbols: tuple[str, ...]
    response_status: int


@dataclass(frozen=True, slots=True)
class IntradayTrendFetchResult:
    records: tuple[IntradayTrendRecord, ...]
    requested_symbols: tuple[str, ...]
    response_status: int


@dataclass(slots=True)
class EastMoneyRealtimeQuoteProvider:
    transport: HttpTransport
    endpoint: str = "single_quote"
    name: str = "eastmoney"
    capability_version: str = "eastmoney-realtime-quote-v1"
    timeout_seconds: float = 12.0
    single_url: str = "https://push2.eastmoney.com/api/qt/stock/get"
    batch_url: str = "https://push2.eastmoney.com/api/qt/ulist.np/get"
    trends_url: str = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    ut: str = "fa5fd1943c7b386f172d6893dbbd1d0c"

    def fetch_quotes(self, symbols: Sequence[str]) -> RealtimeQuoteFetchResult:
        requested = tuple(symbols)
        if self.endpoint == "single_quote":
            records: list[RealtimeQuoteRecord] = []
            statuses: list[int] = []
            for symbol in requested:
                response = self._get(self.single_url, {"secid": _secid(symbol), "fields": _quote_fields(), "fltt": "2", "invt": "2", "ut": self.ut})
                statuses.append(response.status_code)
                data = _json_data(response)
                if not isinstance(data, dict) or not data:
                    continue
                record = _quote_record(data, exchange=_exchange_from_symbol(symbol))
                if record is not None:
                    records.append(record)
            return RealtimeQuoteFetchResult(tuple(records), requested, statuses[0] if statuses else 200)
        if self.endpoint == "batch_quote":
            response = self._get(self.batch_url, {
                "secids": ",".join(_secid(symbol) for symbol in requested),
                "fields": "f2,f3,f4,f12,f13,f14",
                "fltt": "2", "invt": "2", "ut": self.ut,
            })
            data = _json_data(response)
            rows = data.get("diff") if isinstance(data, dict) else None
            if rows is None:
                rows = data.get("data", {}).get("diff") if isinstance(data, dict) and isinstance(data.get("data"), dict) else None
            rows = list(rows.values()) if isinstance(rows, dict) else rows if isinstance(rows, list) else []
            records = tuple(_quote_record(row) for row in rows if isinstance(row, dict))
            return RealtimeQuoteFetchResult(tuple(record for record in records if record), requested, response.status_code)
        raise ValueError(f"endpoint {self.endpoint} does not fetch quote records")

    def fetch_trends(self, symbols: Sequence[str], *, days: int = 5) -> IntradayTrendFetchResult:
        requested = tuple(symbols)
        records: list[IntradayTrendRecord] = []
        status = 200
        for symbol in requested:
            response = self._get(self.trends_url, {
                "secid": _secid(symbol), "ndays": str(days), "iscr": "0",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", "ut": self.ut,
            })
            status = response.status_code
            data = _json_data(response)
            rows = data.get("trends") if isinstance(data, dict) else None
            if rows is None and isinstance(data, dict) and isinstance(data.get("data"), dict):
                rows = data["data"].get("trends")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, str):
                    continue
                values = row.split(",")
                if len(values) < 8:
                    continue
                records.append(IntradayTrendRecord(
                    symbol=symbol, bar_time=values[0], price=_number(values[1]),
                    average_price=_number(values[2]), high=_number(values[3]),
                    low=_number(values[4]), volume=_number(values[5]), amount=_number(values[6]),
                ))
        return IntradayTrendFetchResult(tuple(records), requested, status)

    def _get(self, url: str, params: Mapping[str, str]):
        response = self.transport.get(url, params=params, timeout_seconds=self.timeout_seconds)
        EndpointContract(frozenset()).parse_json(response)
        return response


def _secid(symbol: str) -> str:
    text = str(symbol).lower()
    if len(text) < 8 or text[:2] not in {"sh", "sz", "bj"}:
        raise ValueError(f"EastMoney requires exchange-prefixed symbol: {symbol}")
    return f"{'1' if text[:2] == 'sh' else '0'}.{text[2:]}"


def _quote_fields() -> str:
    return "f57,f58,f43,f44,f45,f46,f47,f48,f60,f84,f85,f116,f117,f162,f167,f168,f169,f170"


def _json_data(response: Any) -> Any:
    import json

    try:
        payload = json.loads(response.text)
    except json.JSONDecodeError as exc:
        raise ProviderContractError("EastMoney realtime response is not JSON", FailureClass.INVALID_JSON, retryable=False) from exc
    if not isinstance(payload, dict):
        raise ProviderContractError("EastMoney realtime envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
    return payload.get("data")


def _quote_record(row: Mapping[str, Any], *, exchange: str | None = None) -> RealtimeQuoteRecord | None:
    code = row.get("f57") or row.get("f12")
    market = row.get("f13")
    if market is None and exchange is not None:
        market = "1" if exchange == "XSHG" else "0"
    if code in (None, "") or market not in (0, 1, "0", "1"):
        return None
    exchange = "XSHG" if str(market) == "1" else "XSHE"
    return RealtimeQuoteRecord(
        symbol=("sh" if exchange == "XSHG" else "sz") + str(code).zfill(6),
        exchange=exchange,
        name=_text(row.get("f58") or row.get("f14")),
        price=_number(row.get("f43") or row.get("f2")),
        change_pct=_number(row.get("f170") or row.get("f3")),
        change_amount=_number(row.get("f169") or row.get("f4")),
        open=_number(row.get("f46")), high=_number(row.get("f44")), low=_number(row.get("f45")),
        pre_close=_number(row.get("f60")), volume=_number(row.get("f47")), amount=_number(row.get("f48")),
        total_market_cap=_number(row.get("f116")), float_market_cap=_number(row.get("f117")),
        pe_dynamic=_number(row.get("f162")), pb=_number(row.get("f167")),
        turnover_rate=_number(row.get("f168")), source_time=_text(row.get("f124")),
    )


def _number(value: Any) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    return None if value in (None, "", "-") else str(value)


def _exchange_from_symbol(symbol: str) -> str:
    text = str(symbol).lower()
    if text.startswith("sh"):
        return "XSHG"
    if text.startswith("sz"):
        return "XSHE"
    if text.startswith("bj"):
        return "BSE"
    raise ValueError(f"EastMoney requires exchange-prefixed symbol: {symbol}")
