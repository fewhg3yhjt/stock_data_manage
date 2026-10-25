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
            self.capability_version = f"tencent-{_adjustment_token(self.adjustment)}-kline-v1"
            self.url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
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
        statuses: list[WindowStatus] = []
        response_statuses: list[int] = []
        for symbol in requested:
            adjust = _adjustment_token(self.adjustment)
            response = self.transport.get(
                self.url,
                params={
                    "param": (
                        f"{symbol},day,,,{self.max_rows},{adjust}"
                        if adjust != "none"
                        else f"{symbol},day,,,{self.max_rows}"
                    )
                },
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
            tuple(rows), requested, _combined_status(statuses, bool(rows)), tuple(response_statuses),
            ("trade_date", "open", "high", "low", "close", "volume", "amount"),
            ("volume:unverified", "amount:unverified"), len(returned_rows),
            str(returned_rows[0]["trade_date"]) if returned_rows else None,
            str(returned_rows[-1]["trade_date"]) if returned_rows else None, self.adjustment,
        )

    def fetch_window(self, code: str, start: date, end: date, adjust: str = "qfq", api: str = "fqkline",
                     asset_type: str | None = None) -> InputFetchResult:
        """Verified historical request shape; returns source positional fields for YAML mapping."""
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
        bar_key = _adjustment_bar_key(adjustment)
        cursor, by_date, hosts, source_rows = start, {}, [], []
        path = "/appstock/app/kline/kline" if api == "kline" else f"/appstock/app/{api}/get"
        input_hosts = self.alternative_input_hosts if api == "newfqkline" else self.input_hosts
        while cursor <= end:
            # kline and newfqkline may return a rolling recent window regardless
            # of requested dates; repeating segments cannot extend that history.
            stop = end if api in {"kline", "newfqkline"} else min(cursor + timedelta(days=699), end)
            if api == "kline":
                param = f"{symbol},day,,,1024"
                row_limit = 1024
            else:
                adjustment_suffix = f",{adjust}"
                param = f"{symbol},day,{cursor.isoformat()},{stop.isoformat()},640{adjustment_suffix}"
                row_limit = 640
            data, host = tencent_kline_call(self.transport, path,
                param, input_hosts, self.host_down_until)
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
            cursor = stop + timedelta(days=1)
        if not by_date:
            raise ProviderContractError("Tencent window has zero bars", FailureClass.TEMPORARY_EMPTY, retryable=True)
        return InputFetchResult(tuple(by_date[stamp] for stamp in sorted(by_date)),
                                " | ".join(host + path for host in dict.fromkeys(hosts)),
                                source_rows=tuple(source_rows),
                                mapping_context={"source_api": api,
                                    "adjustment": adjustment.value,
                                    "adjustment_parameter": adjust,
                                    "source_bar_key": bar_key,
                                    "source_total_count": len(source_rows),
                                    "selected_row_count": len(by_date),
                                    "history_completeness_verified": False})


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
    """Keep the source helper's host order, 120s cooldown, param-error and empty checks."""
    errors = []
    for host in hosts:
        if down_until.get(host, 0) > time():
            continue
        try:
            response = transport.get(host + path, params={"param": param}, timeout_seconds=(8, 20))
            payload = EndpointContract(frozenset()).parse_json(response) if response.body.strip() else {}
        except Exception as exc:
            # Missing archive entries / persistence failures are execution failures, never a source fallback.
            from requests import RequestException
            if isinstance(exc, ValueError) or (isinstance(exc, OSError) and not isinstance(exc, RequestException)):
                raise
            errors.append(type(exc).__name__)
            down_until[host] = time() + 120
            continue
        if not isinstance(payload, dict):
            down_until[host] = time() + 120
            continue
        message = str(payload.get("msg") or "").strip().lower()
        if message in {"param error", "bad params"}:
            raise ValueError(f"Tencent rejected the daily request parameters: {message}")
        if isinstance(payload.get("data"), dict) and payload["data"]:
            return payload["data"], host
        down_until[host] = time() + 120
    raise ProviderContractError(f"Tencent hosts failed: {','.join(errors)}", FailureClass.CONNECTION, retryable=True)


def _combined_status(statuses: list[WindowStatus], target_rows_found: bool) -> WindowStatus:
    if WindowStatus.TRUNCATED in statuses:
        return WindowStatus.TRUNCATED
    if not target_rows_found:
        return WindowStatus.TEMPORARY_EMPTY
    if WindowStatus.PARTIAL in statuses:
        return WindowStatus.PARTIAL
    return WindowStatus.COMPLETE
