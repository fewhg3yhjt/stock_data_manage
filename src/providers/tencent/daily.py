from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Sequence
from datetime import datetime, timedelta
from time import time

from ...domain import Adjustment, AssetType
from ...routing.capabilities import ProviderCapability
from ..base import FetchResult
from ..contracts import EndpointContract, FailureClass, ProviderContractError, WindowStatus, InputFetchResult
from ..transport import HttpTransport


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
    adjustment: Adjustment = Adjustment.NONE
    supported_asset_types: frozenset[AssetType] = field(default_factory=lambda: frozenset(AssetType))
    input_hosts: tuple[str, ...] = ("https://web.ifzq.gtimg.cn", "https://proxy.finance.qq.com/ifzqgtimg", "https://ifzq.gtimg.cn")
    host_down_until: dict[str, float] = field(default_factory=dict)
    # Only this host was used in the independently retained alternative probes.
    alternative_input_hosts: tuple[str, ...] = ("https://proxy.finance.qq.com/ifzqgtimg",)

    def __post_init__(self) -> None:
        if self.adjustment is not Adjustment.NONE:
            self.endpoint = "forward_history" if self.adjustment is Adjustment.FORWARD else "backward_history"
            self.capability_version = f"tencent-{_adjustment_token(self.adjustment)}-newfqkline-v2"
            self.url = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
            self.max_rows = 640
            self.contract = EndpointContract(
                frozenset({"symbol", "trade_date", "open", "high", "low", "close", "volume"}),
                max_rows_per_request=640,
                supports_pagination=False,
            )
            self.supported_asset_types = frozenset({AssetType.STOCK, AssetType.ETF})

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        returned_rows: list[dict[str, Any]] = []
        source_rows: list[dict[str, Any]] = []
        statuses: list[WindowStatus] = []
        response_statuses: list[int] = []
        for symbol in requested:
            adjust = _adjustment_token(self.adjustment)
            param = (
                f"{symbol},day,{trade_date.isoformat()},{trade_date.isoformat()},{self.max_rows},{adjust}"
                if adjust != "none"
                else f"{symbol},day,,,{self.max_rows}"
            )
            response = self.transport.get(
                self.url,
                params={"param": param},
                timeout_seconds=self.timeout_seconds,
            )
            response_statuses.append(response.status_code)
            payload = self.contract.parse_json(response)
            if not isinstance(payload, dict) or payload.get("code") not in {0, "0", None}:
                raise ProviderContractError(
                    "Tencent response envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False
                )
            stock = ((payload.get("data") or {}).get(symbol) or {})
            bars = stock.get(_adjustment_bar_key(self.adjustment)) or []
            parsed = []
            for item in bars:
                if not isinstance(item, list) or len(item) < 6:
                    raise ProviderContractError(
                        "Tencent daily row changed", FailureClass.SCHEMA_CHANGED, retryable=False
                    )
                if adjust != "none" and len(item) not in {10, 11}:
                    raise ProviderContractError(
                        "alternative Tencent daily row changed", FailureClass.SCHEMA_CHANGED, retryable=False
                    )
                source = {"symbol": symbol, **{str(index): value for index, value in enumerate(item)}}
                source.update(source_api="kline" if adjust == "none" else "newfqkline",
                              source_bar_key=_adjustment_bar_key(self.adjustment))
                source_rows.append(source)
                date_text = str(item[0]).strip()
                try:
                    row_date = datetime.strptime(
                        date_text, "%Y%m%d" if len(date_text) == 8 and date_text.isdigit() else "%Y-%m-%d"
                    ).date()
                except ValueError as exc:
                    raise ProviderContractError(
                        "Tencent daily date changed", FailureClass.SCHEMA_CHANGED, retryable=False
                    ) from exc
                parsed.append({
                    "symbol": symbol,
                    "trade_date": row_date.isoformat(),
                    "open": item[1],
                    "close": item[2],
                    "high": item[3],
                    "low": item[4],
                    "volume": item[5],
                    "amount": None,
                })
            statuses.append(self.contract.validate_rows(parsed))
            returned_rows.extend(parsed)
            rows.extend(item for item in parsed if item["trade_date"] == trade_date.isoformat())
        return FetchResult(
            tuple(rows), requested, _combined_status(statuses, bool(rows)), tuple(response_statuses),
            ("trade_date", "open", "high", "low", "close", "volume", "amount"),
            ("volume:unverified", "amount:unverified"), len(returned_rows),
            str(returned_rows[0]["trade_date"]) if returned_rows else None,
            str(returned_rows[-1]["trade_date"]) if returned_rows else None, self.adjustment,
            tuple(source_rows),
        )

    def capture_window(self, code: str, start: date, end: date, adjust: str = "qfq", api: str = "newfqkline",
                       asset_type: str | None = None):
        """Acquire response packages only; business contents never trigger host fallback."""
        if api not in {"kline", "fqkline", "newfqkline"}:
            raise ValueError("unsupported Tencent daily API")
        if adjust not in {"none", "qfq", "hfq"}:
            raise ValueError("unsupported Tencent daily adjustment")
        if api == "kline" and adjust != "none":
            raise ValueError("api=kline requires adjust=none")
        if api != "kline" and adjust == "none":
            raise ValueError("adjust=none requires api=kline")
        adjustment = _adjustment_from_token(adjust)
        if asset_type == "index" and adjustment is not Adjustment.NONE:
            raise ValueError("Tencent index daily data is available only without adjustment")
        symbol = tencent_symbol(code, allow_bse=api in {"kline", "newfqkline"}, asset_type=asset_type)
        if end < start:
            raise ValueError("daily window end must not precede start")
        cursor, segments = start, []
        path = "/appstock/app/kline/kline" if api == "kline" else f"/appstock/app/{api}/get"
        input_hosts = self.alternative_input_hosts if api == "newfqkline" else self.input_hosts
        while cursor <= end:
            stop = end if api in {"kline", "newfqkline"} else min(cursor + timedelta(days=699), end)
            param = f"{symbol},day,,,1024" if api == "kline" else f"{symbol},day,{cursor.isoformat()},{stop.isoformat()},640,{adjust}"
            response, host = tencent_kline_call(self.transport, path, param, input_hosts, self.host_down_until)
            segments.append((cursor, stop, response, host))
            cursor = stop + timedelta(days=1)
        return {"symbol": symbol, "start": start, "end": end, "adjust": adjust,
                "api": api, "path": path, "segments": tuple(segments)}

    def parse_window(self, captured):
        """Pure interpretation of previously retained responses; never calls a transport."""
        symbol, start, end = captured["symbol"], captured["start"], captured["end"]
        api, adjust, path = captured["api"], captured["adjust"], captured["path"]
        adjustment = _adjustment_from_token(adjust)
        bar_key = _adjustment_bar_key(adjustment)
        by_date, hosts, source_rows = {}, [], []
        for cursor, stop, response, host in captured["segments"]:
            payload = EndpointContract(frozenset()).parse_json(response)
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
                raise ProviderContractError("Tencent daily envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            if payload.get("code", 0) != 0:
                raise ProviderContractError("Tencent rejected the daily request parameters: " + str(payload.get("msg", "unknown")), FailureClass.SCHEMA_CHANGED, retryable=False)
            data = payload["data"]
            row_limit = 1024 if api == "kline" else 640
            node = data.get(symbol)
            if not isinstance(node, dict) or not isinstance(node.get(bar_key), list):
                raise ProviderContractError("Tencent daily list changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            items = node[bar_key]
            if adjustment is not Adjustment.NONE and not items and node.get("day"):
                raise ProviderContractError(
                    "empty adjusted list cannot use unadjusted prices", FailureClass.SCHEMA_CHANGED, retryable=False
                )
            if len(items) > row_limit:
                raise ProviderContractError("Tencent daily row limit changed", FailureClass.TRUNCATED, retryable=False)
            segment_dates = set()
            for item in items:
                if not isinstance(item, list) or len(item) < 6:
                    raise ProviderContractError("Tencent daily row changed", FailureClass.SCHEMA_CHANGED, retryable=False)
                if api == "newfqkline" and len(item) not in {10, 11}:
                    raise ProviderContractError("alternative Tencent daily row changed", FailureClass.SCHEMA_CHANGED, retryable=False)
                try:
                    text = str(item[0]).strip()
                    stamp = datetime.strptime(text, "%Y%m%d" if len(text) == 8 and text.isdigit() else "%Y-%m-%d").date()
                except ValueError as exc:
                    raise ProviderContractError("Tencent daily date changed", FailureClass.SCHEMA_CHANGED, retryable=False) from exc
                if stamp in segment_dates:
                    raise ProviderContractError("duplicate Tencent daily date", FailureClass.SCHEMA_CHANGED, retryable=False)
                segment_dates.add(stamp)
                source = {"symbol": symbol, **{str(i): value for i, value in enumerate(item)}}
                source.update(source_api=api, source_bar_key=bar_key)
                source_rows.append(source)
                if not cursor <= stamp <= stop and api == "fqkline":
                    raise ProviderContractError("Tencent daily returned a date outside its requested segment", FailureClass.SCHEMA_CHANGED, retryable=False)
                if not cursor <= stamp <= stop:
                    continue
                if stamp in by_date:
                    raise ProviderContractError("duplicate Tencent daily date", FailureClass.SCHEMA_CHANGED, retryable=False)
                by_date[stamp] = source
            if api == "newfqkline" and len(items) == 640 and segment_dates and start < min(segment_dates):
                raise ProviderContractError("requested Tencent history exceeds retained 640-bar window", FailureClass.TRUNCATED, retryable=False)
            if api == "kline" and len(items) == 1024 and segment_dates and start < min(segment_dates):
                raise ProviderContractError("requested Tencent history exceeds retained 1024-bar window", FailureClass.TRUNCATED, retryable=False)
            hosts.append(host)
        if not by_date:
            raise ProviderContractError("Tencent window has zero bars", FailureClass.TEMPORARY_EMPTY, retryable=False)
        return InputFetchResult(tuple(by_date[stamp] for stamp in sorted(by_date)),
            " | ".join(host + path for host in dict.fromkeys(hosts)), source_rows=tuple(source_rows),
            mapping_context={"source_api": api, "adjustment": adjustment.value, "adjustment_parameter": adjust,
                "source_bar_key": bar_key, "source_total_count": len(source_rows),
                "selected_row_count": len(by_date), "history_completeness_verified": False})


def _adjustment_from_token(value: str) -> Adjustment:
    return {"none": Adjustment.NONE, "qfq": Adjustment.FORWARD, "hfq": Adjustment.BACKWARD}[value]


def _adjustment_token(adjustment: Adjustment) -> str:
    return {Adjustment.NONE: "none", Adjustment.FORWARD: "qfq", Adjustment.BACKWARD: "hfq"}[adjustment]


def _adjustment_bar_key(adjustment: Adjustment) -> str:
    return {Adjustment.NONE: "day", Adjustment.FORWARD: "qfqday", Adjustment.BACKWARD: "hfqday"}[adjustment]


def tencent_symbol(code: str, *, allow_bse: bool = False, asset_type: str | None = None) -> str:
    import re
    match = re.fullmatch(r"(?:(sh|sz|bj))?(\d{6})", str(code).lower())
    if match is None:
        raise ValueError("invalid Tencent security code")
    prefix, digits = match.groups()
    inferred = "bj" if digits.startswith(("4", "8", "92")) else "sh" if digits.startswith(("5", "6", "9")) else "sz"
    if asset_type == "index" and prefix is None:
        raise ValueError("Tencent index code requires an explicit exchange prefix")
    if asset_type == "index" and prefix:
        if prefix == "bj" and not allow_bse:
            raise ValueError("this verified Tencent input excludes BSE")
        return prefix + digits
    if prefix and prefix != inferred:
        raise ValueError("security code and exchange prefix disagree")
    if inferred == "bj" and not allow_bse:
        raise ValueError("this verified Tencent input excludes BSE")
    return inferred + digits


def tencent_kline_call(transport, path, param, hosts, down_until):
    """Preserve host order/timeouts/cooldown; only explicit acquisition failures switch hosts."""
    from ...routing.retry import acquisition_failure
    last_error = None
    for host in hosts:
        if down_until.get(host, 0) > time():
            continue
        try:
            response = transport.get(host + path, params={"param": param}, timeout_seconds=(8, 20))
            if response.status_code >= 400:
                failure = FailureClass.HTTP_5XX if response.status_code >= 500 else FailureClass.HTTP_ERROR
                raise ProviderContractError(f"Tencent HTTP {response.status_code}", failure,
                                           retryable=response.status_code >= 500, http_status=response.status_code)
            return response, host
        except Exception as exc:
            failure = acquisition_failure(exc)
            if failure not in {FailureClass.CONNECTION, FailureClass.TIMEOUT, FailureClass.HTTP_5XX}:
                raise
            down_until[host] = time() + 120
            last_error = exc
    if last_error is not None:
        raise last_error
    raise ProviderContractError("Tencent hosts are cooling down", FailureClass.CONNECTION, retryable=True)


def _combined_status(statuses: list[WindowStatus], target_rows_found: bool) -> WindowStatus:
    if WindowStatus.TRUNCATED in statuses:
        return WindowStatus.TRUNCATED
    if not target_rows_found:
        return WindowStatus.TEMPORARY_EMPTY
    if WindowStatus.PARTIAL in statuses:
        return WindowStatus.PARTIAL
    return WindowStatus.COMPLETE
