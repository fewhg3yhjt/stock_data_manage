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
    client: Any = field(default=None, kw_only=True)
    contract: EndpointContract = field(
        default_factory=lambda: EndpointContract(
            frozenset({"symbol", "trade_date", "open", "high", "low", "close", "volume"}),
            max_rows_per_request=1023, supports_pagination=False,
        )
    )

    def fetch_adjustment_factors(self, *, code, kind="qfq", source_payloads):
        """Keep both original SDK requests; choose one factor history without pricing bars."""
        from ..akshare.session import load_client
        from ..eastmoney.realtime import history_stock_identity
        from ..contracts import InputFetchResult
        if kind not in {"qfq", "hfq"}:
            raise ValueError("factor kind must be qfq or hfq")
        bare, market, instrument = history_stock_identity(code)
        symbol = market + bare
        self.client = self.client or load_client()
        frames = {name: self.client.stock_zh_a_daily(symbol=symbol, adjust=name + "-factor")
                  for name in ("qfq", "hfq")}
        payloads = list(source_payloads())
        if len(payloads) != 2 or [p[0] for p in payloads] != ["qfq", "hfq"]:
            raise RuntimeError("both ordered source factor responses are required")
        source, parsed = [], []
        for name, source_symbol, items in payloads:
            frame = frames[name]
            if source_symbol != symbol or list(frame.columns) != ["date", name + "_factor"] or len(frame) != len(items):
                raise RuntimeError("factor SDK identity, fields or count changed")
            for old, item in zip(frame.to_dict(orient="records"), items):
                day = old["date"].date().isoformat()
                if day != item["d"] or old[name + "_factor"] != item["f"]:
                    raise RuntimeError("factor SDK date or coefficient differs from retained response")
                source.append({"factor_kind": name, **item})
                parsed.append({"factor_kind": name, "factor_date": day, "raw_factor": item["f"],
                               "source_date": old["date"], "source_factor": old[name + "_factor"]})
        selected = tuple(row for row in parsed if row["factor_kind"] == kind)
        return InputFetchResult(selected, source_url=f"https://finance.sina.com.cn/realstock/company/{symbol}/{kind}.js",
            source_rows=tuple(source), excluded_rows=tuple(row for row in parsed if row["factor_kind"] != kind),
            mapping_context={"instrument_id": instrument, "source_symbol": symbol, "factor_kind": kind})

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


def parse_adjustment_payload(body, url, status_code=200):
    """Literal-only gate after raw retention and before the SDK's legacy eval parser."""
    import json
    import re
    from decimal import Decimal, InvalidOperation
    from urllib.parse import urlsplit
    parts = urlsplit(url)
    match = re.fullmatch(r"/realstock/company/((?:sh|sz|bj)[0-9]{6})/(qfq|hfq)\.js", parts.path)
    if status_code != 200 or parts.hostname != "finance.sina.com.cn" or not match or parts.query:
        raise RuntimeError("factor response status or endpoint changed")
    symbol, kind = match.groups()
    try:
        text = body.decode("utf-8")
        wrapper = re.fullmatch(r"var " + re.escape(symbol + kind) + r"=([^\r\n]+)(?:\r?\n\s*/\*(?:(?!\*/)[\s\S])*\*/)?\s*", text)
        if not wrapper:
            raise ValueError("factor assignment changed")
        def unique_object(pairs):
            result = dict(pairs)
            if len(result) != len(pairs):
                raise ValueError("duplicate JSON keys")
            return result
        payload = json.loads(wrapper[1], object_pairs_hook=unique_object)
        items = payload["data"]
        if type(payload["total"]) is not int or payload["total"] != len(items) or not isinstance(items, list) or not items:
            raise ValueError("factor count or rows changed")
        seen = set()
        for item in items:
            if list(item) != ["d", "f"] or not isinstance(item["f"], str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", item["d"]):
                raise ValueError("factor ordered source fields changed")
            date.fromisoformat(item["d"])
            if item["d"] in seen or not Decimal(item["f"]).is_finite():
                raise ValueError("factor duplicate date or nonfinite value")
            seen.add(item["d"])
    except (ValueError, TypeError, KeyError, UnicodeError, InvalidOperation) as exc:
        raise RuntimeError("factor literal response schema changed") from exc
    return kind, symbol, items


def validate_adjustment_response(response):
    parse_adjustment_payload(response.content, response.url, response.status_code)


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
