from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence

import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..contracts import EndpointContract, FailureClass, ProviderContractError
from ..contracts import HttpResponse
from ..contracts import InputFetchResult


def history_stock_identity(code):
    """Explicit stock identity for the three verified single-security SDK inputs."""
    import re
    text = str(code).lower()
    explicit = text[:2] if re.fullmatch(r"(?:sh|sz|bj)\d{6}", text) else None
    bare = text[2:] if explicit else text
    if not re.fullmatch(r"(?:60\d{4}|68\d{4}|00\d{4}|30\d{4}|[48]\d{5}|92\d{4})", bare):
        raise ValueError("history input requires a stock code")
    market = "bj" if bare.startswith(("4", "8", "92")) else "sh" if bare.startswith(("60", "68")) else "sz"
    if explicit is not None and explicit != market:
        raise ValueError("stock code and exchange prefix disagree")
    exchange = {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[market]
    return bare, market, f"{exchange}:{bare}"


@contextmanager
def history_replay_clock(function, manifest, code):
    """Reproduce only the SDK's volatile request timestamp for strict offline replay."""
    from pathlib import Path
    from types import SimpleNamespace
    from unittest.mock import patch
    from urllib.parse import parse_qs, urlsplit
    bare, market, _ = history_stock_identity(code)
    secid = f"{1 if market == 'sh' else 0}.{bare}"
    matches = []
    for line in Path(manifest).read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        url = urlsplit(event.get("url", ""))
        query = parse_qs(url.query)
        if url.path == "/api/qt/stock/fflow/daykline/get" and query.get("secid") == [secid] and query.get("_"):
            matches.append(query["_"][0])
    if not matches:
        raise ValueError("fund-flow replay timestamp is missing; never falls back to live")
    # An archive may contain repeated probes; choose its latest matching timestamp.
    stamp = max(int(value) for value in matches)
    namespace = getattr(function, "__globals__", {})
    if "time" not in namespace:
        raise ValueError("fund-flow SDK clock contract changed")
    with patch.dict(namespace, {"time": SimpleNamespace(time=lambda: stamp / 1000)}):
        yield


def history_input_result(frame, *, code, required_fields, source_payloads, source_url, fund_flow=False, implemented_dividends=False):
    """Check SDK rows against retained response identities; do not replace the SDK transport/parser."""
    import pandas as pd
    bare, market, instrument_id = history_stock_identity(code)
    if not set(required_fields) <= set(frame.columns):
        raise ProviderContractError("history SDK source columns changed", FailureClass.SCHEMA_CHANGED, retryable=False)
    payloads = tuple(source_payloads())
    if not payloads:
        raise ValueError("retained source responses are required for history identity verification")
    counts = set()
    for payload in payloads:
        if fund_flow:
            data = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(payload, dict) and payload.get("rc", 0) != 0:
                raise ProviderContractError("fund-flow business response failed", FailureClass.SCHEMA_CHANGED, retryable=False)
            if not isinstance(data, dict) or str(data.get("code")) != bare or data.get("market") != (1 if market == "sh" else 0):
                raise ProviderContractError("fund-flow source security disagrees with request", FailureClass.SCHEMA_CHANGED, retryable=False)
            if not isinstance(data.get("klines"), list):
                raise ProviderContractError("fund-flow source rows changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            counts.add(len(data["klines"]))
        else:
            data = payload.get("result") if isinstance(payload, dict) else None
            if isinstance(payload, dict) and payload.get("success") is False:
                raise ProviderContractError("history business response failed", FailureClass.SCHEMA_CHANGED, retryable=False)
            if not isinstance(data, dict) or not isinstance(data.get("data"), list):
                raise ProviderContractError("history source envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            if any(not isinstance(row, dict) or str(row.get("SECURITY_CODE")) != bare for row in data["data"]):
                raise ProviderContractError("history source security disagrees with request", FailureClass.SCHEMA_CHANGED, retryable=False)
            if type(data.get("count")) is not int or data["count"] < 0:
                raise ProviderContractError("history source total count is missing", FailureClass.SCHEMA_CHANGED, retryable=False)
            counts.add(data["count"])
    if counts != {len(frame)}:
        raise ProviderContractError("SDK row count disagrees with source total", FailureClass.SCHEMA_CHANGED, retryable=False)
    rows = tuple({name: None if pd.isna(value) else value for name, value in row.items()}
                 for row in frame.to_dict(orient="records"))
    if "代码" in frame.columns and any(str(row["代码"]) != bare for row in rows):
        raise ProviderContractError("SDK security identity changed", FailureClass.SCHEMA_CHANGED, retryable=False)
    selected, excluded = [], []
    for index, row in enumerate(rows):
        if implemented_dividends and row["方案进度"] != "实施分配":
            excluded.append({"source_row_index": index, "reason": "not_implemented_dividend_plan", "row": row})
        else:
            selected.append(row)
    return InputFetchResult(tuple(selected), source_url, source_rows=rows,
                            mapping_context={"instrument_id": instrument_id, "source_security_code": bare},
                            excluded_rows=tuple(excluded))


class EastMoneyTransport(Protocol):
    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse: ...


class EastMoneyRequestsTransport:
    """Transport matching the verified EastMoney probe session behavior."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/153.0.0.0 Safari/537.36"
            ),
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "application/json,text/plain,*/*",
        })
        retry = Retry(
            total=2, connect=2, read=2, status=2, backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]), raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse:
        response = self.session.get(url, params=params, timeout=(5, timeout_seconds))
        return HttpResponse(response.status_code, dict(response.headers), response.content)


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
    transport: EastMoneyTransport | None = None
    endpoint: str = "single_quote"
    name: str = "eastmoney"
    capability_version: str = "eastmoney-realtime-quote-v1"
    timeout_seconds: float = 12.0
    single_url: str = "https://push2.eastmoney.com/api/qt/stock/get"
    batch_url: str = "https://push2.eastmoney.com/api/qt/ulist.np/get"
    trends_url: str = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    ut: str = "fa5fd1943c7b386f172d6893dbbd1d0c"
    push2: str = "https://push2.eastmoney.com"
    push2_delay: str = "https://push2delay.eastmoney.com"
    push2_his: str = "https://push2his.eastmoney.com"

    def __post_init__(self) -> None:
        if self.transport is None:
            self.transport = EastMoneyRequestsTransport()

    def fetch_quotes(self, symbols: Sequence[str]) -> RealtimeQuoteFetchResult:
        requested = tuple(symbols)
        if self.endpoint == "single_quote":
            records: list[RealtimeQuoteRecord] = []
            statuses: list[int] = []
            for symbol in requested:
                response = self._first_valid(
                    [(f"{host}/api/qt/stock/get", {"secid": _secid(symbol), "fields": _quote_fields(), "fltt": "2", "invt": "2", "ut": self.ut}) for host in (self.push2, self.push2_delay)]
                )
                statuses.append(response.status_code)
                data = _json_data(response)
                if not isinstance(data, dict) or not data:
                    continue
                record = _quote_record(data, exchange=_exchange_from_symbol(symbol))
                if record is not None:
                    records.append(record)
            return RealtimeQuoteFetchResult(tuple(records), requested, statuses[0] if statuses else 200)
        if self.endpoint == "batch_quote":
            params = {"secids": ",".join(_secid(symbol) for symbol in requested), "fields": "f2,f3,f4,f12,f13,f14", "fltt": "2", "invt": "2", "ut": self.ut}
            response = self._first_valid([(f"{host}/api/qt/ulist.np/get", params) for host in (self.push2, self.push2_delay)])
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
            params = {
                "secid": _secid(symbol), "ndays": str(days), "iscr": "0",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", "ut": self.ut,
            }
            response = self._first_valid([(f"{host}/api/qt/stock/trends2/get", params) for host in (self.push2, self.push2_delay, self.push2_his)])
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
        assert self.transport is not None
        response = self.transport.get(url, params=params, timeout_seconds=self.timeout_seconds)
        EndpointContract(frozenset()).parse_json(response)
        return response

    def _first_valid(self, candidates):
        errors = []
        for url, params in candidates:
            try:
                response = self._get(url, params)
                payload = json.loads(response.text)
                data = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(data, dict) and data:
                    return response
                errors.append(f"{url}: business data empty")
            except Exception as exc:
                errors.append(f"{url}: {type(exc).__name__}: {exc}")
        raise ProviderContractError(
            "EastMoney realtime candidates failed: " + " | ".join(errors),
            FailureClass.CONNECTION,
            retryable=True,
        )


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
