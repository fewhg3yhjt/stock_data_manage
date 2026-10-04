from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Sequence

from ..base import FetchResult
from ..contracts import EndpointContract, FailureClass, ProviderContractError, WindowStatus
from ..transport import HttpTransport


@dataclass(slots=True)
class SinaDailyProvider:
    transport: HttpTransport
    name: str = "sina"
    endpoint: str = "full_history"
    capability_priority: int = 100
    capability_version: str = "sina-cn-marketdata-v1"
    timeout_seconds: float = 15.0
    max_rows: int = 1023
    url: str = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    contract: EndpointContract = field(
        default_factory=lambda: EndpointContract(
            frozenset({"symbol", "trade_date", "open", "high", "low", "close", "volume"}),
            max_rows_per_request=1023, supports_pagination=False,
        )
    )

    def fetch_futures_kline(self, *, symbol, start=None, end=None):
        from .snapshot import _sina_futures_code, _sina_input_http, _sina_number, _sina_price, _sina_date
        from ..contracts import InputFetchResult
        from datetime import datetime
        import json
        import re
        code = _sina_futures_code(symbol)
        lo = _sina_date(start) if start else None
        hi = _sina_date(end) if end else None
        if lo and hi and lo > hi:
            raise ValueError("end precedes start")
        url = f"https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_{code}=/InnerFuturesNewService.getDailyKLine"
        response = _sina_input_http(url, params={"symbol":code})
        match = re.search(rf"var _{re.escape(code)}=\((.*)\);?\s*$",response.content.decode("gbk","replace"),re.S)
        if not match:
            raise RuntimeError("Sina futures JSONP variable or wrapper changed")
        body = match.group(1).strip()
        if body == "null":
            raise ValueError("futures history is unavailable for this contract")
        try:
            items = json.loads(body)
        except ValueError as exc:
            raise RuntimeError("Sina futures response is not JSON") from exc
        if not isinstance(items,list) or not all(isinstance(item,dict) for item in items):
            raise RuntimeError("Sina futures rows must be an object list")
        rows, excluded, seen = [], [], set()
        try:
            for item in items:
                day = _sina_date(item["d"])
                if day in seen:
                    raise RuntimeError("Sina futures history contains duplicate dates")
                seen.add(day)
                if lo and day < lo or hi and day > hi:
                    excluded.append(item)
                    continue
                rows.append({"date":day,"symbol":code,
                    **{name:_sina_price(item[key]) for name,key in {"open":"o","high":"h","low":"l","close":"c","settle":"s"}.items()},
                    "volume":_sina_number(item["v"]),"open_interest":_sina_number(item["p"])})
        except (ValueError,KeyError) as exc:
            raise RuntimeError("Sina futures row date or required field changed") from exc
        if not rows:
            raise ValueError("requested date window contains no futures bars")
        rows.sort(key=lambda row:row["date"])
        return InputFetchResult(tuple(rows),source_url=response.url,source_rows=tuple(items),excluded_rows=tuple(excluded),
            mapping_context={"source_contract":code,"series_identity":"main_continuous" if re.fullmatch(r"[A-Z]{1,2}0",code) else "contract"})

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        returned_rows: list[dict[str, Any]] = []
        statuses: list[WindowStatus] = []
        response_statuses: list[int] = []
        for symbol in requested:
            response = self.transport.get(
                self.url,
                params={"symbol": symbol, "scale": "240", "ma": "no", "datalen": str(self.max_rows)},
                timeout_seconds=self.timeout_seconds,
            )
            response_statuses.append(response.status_code)
            payload = self.contract.parse_json(response)
            if payload is None:
                parsed: list[dict[str, Any]] = []
            elif not isinstance(payload, list):
                raise ProviderContractError("Sina response envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            else:
                parsed = [{
                    "symbol": symbol, "trade_date": item.get("day"), "open": item.get("open"),
                    "high": item.get("high"), "low": item.get("low"), "close": item.get("close"),
                    "volume": item.get("volume"), "amount": item.get("amount"),
                } for item in parsed_items(payload)]
            statuses.append(self.contract.validate_rows(parsed))
            returned_rows.extend(parsed)
            rows.extend(item for item in parsed if str(item["trade_date"]) == trade_date.isoformat())
        return FetchResult(
            tuple(rows), requested, _combined_status(statuses, bool(rows)), tuple(response_statuses),
            ("trade_date", "open", "high", "low", "close", "volume", "amount"),
            ("volume:unverified", "amount:unverified"), len(returned_rows),
            str(returned_rows[0]["trade_date"]) if returned_rows else None,
            str(returned_rows[-1]["trade_date"]) if returned_rows else None,
        )


def parsed_items(payload: list[object]) -> list[dict[str, Any]]:
    return [item for item in payload if isinstance(item, dict)]


def _combined_status(statuses: list[WindowStatus], target_rows_found: bool) -> WindowStatus:
    if WindowStatus.TRUNCATED in statuses:
        return WindowStatus.TRUNCATED
    if not target_rows_found:
        return WindowStatus.TEMPORARY_EMPTY
    if WindowStatus.PARTIAL in statuses:
        return WindowStatus.PARTIAL
    return WindowStatus.COMPLETE
