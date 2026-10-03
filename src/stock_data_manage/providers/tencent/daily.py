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

    def __post_init__(self) -> None:
        if self.adjustment is Adjustment.BACKWARD:
            raise ValueError("Tencent daily adapter does not implement backward adjustment")
        if self.adjustment is Adjustment.FORWARD:
            self.endpoint = "forward_history"
            self.capability_version = "tencent-qfq-kline-v1"
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
            response = self.transport.get(
                self.url,
                params={
                    "param": (
                        f"{symbol},day,,,{self.max_rows},qfq"
                        if self.adjustment is Adjustment.FORWARD
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
            bars = stock.get("qfqday" if self.adjustment is Adjustment.FORWARD else "day") or []
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

    def fetch_window(self, code: str, start: date, end: date, adjust: str = "qfq") -> InputFetchResult:
        """Verified historical request shape; returns source positional fields for YAML mapping."""
        symbol = tencent_symbol(code)
        if adjust != "qfq" or end < start:
            raise ValueError("only an ordered qfq window is supported by this input")
        cursor, by_date, hosts = start, {}, []
        while cursor <= end:
            stop = min(cursor + timedelta(days=699), end)
            data, host = tencent_kline_call(self.transport, "/appstock/app/fqkline/get",
                f"{symbol},day,{cursor.isoformat()},{stop.isoformat()},640,qfq", self.input_hosts, self.host_down_until)
            node = data.get(symbol)
            if not isinstance(node, dict) or not isinstance(node.get("qfqday", node.get("day")), list):
                raise ProviderContractError("Tencent daily list changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            items = node.get("qfqday", node.get("day"))
            if "qfqday" in node and not items and node.get("day"):
                raise ProviderContractError("empty adjusted list cannot use unadjusted prices", FailureClass.SCHEMA_CHANGED, retryable=False)
            for item in items:
                if not isinstance(item, list) or len(item) < 6:
                    raise ProviderContractError("Tencent daily row changed", FailureClass.SCHEMA_CHANGED, retryable=False)
                try:
                    text = str(item[0]).strip()
                    stamp = datetime.strptime(text, "%Y%m%d" if len(text) == 8 and text.isdigit() else "%Y-%m-%d").date()
                except ValueError as exc:
                    raise ProviderContractError("Tencent daily date changed", FailureClass.SCHEMA_CHANGED, retryable=False) from exc
                if not cursor <= stamp <= stop:
                    raise ProviderContractError("Tencent daily returned a date outside its requested segment", FailureClass.SCHEMA_CHANGED, retryable=False)
                if stamp in by_date:
                    raise ProviderContractError("duplicate Tencent daily date", FailureClass.SCHEMA_CHANGED, retryable=False)
                by_date[stamp] = {"symbol": symbol, **{str(i): value for i, value in enumerate(item)}}
            hosts.append(host)
            cursor = stop + timedelta(days=1)
        if not by_date:
            raise ProviderContractError("Tencent window has zero bars", FailureClass.TEMPORARY_EMPTY, retryable=True)
        return InputFetchResult(tuple(by_date[stamp] for stamp in sorted(by_date)),
                                " | ".join(host + "/appstock/app/fqkline/get" for host in dict.fromkeys(hosts)))


def tencent_symbol(code: str) -> str:
    import re
    match = re.fullmatch(r"(?:(sh|sz|bj))?(\d{6})", str(code).lower())
    if match is None:
        raise ValueError("invalid Tencent security code")
    prefix, digits = match.groups()
    inferred = "bj" if digits.startswith(("4", "8", "92")) else "sh" if digits.startswith(("5", "6", "9")) else "sz"
    if prefix and prefix != inferred:
        raise ValueError("security code and exchange prefix disagree")
    if inferred == "bj":
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
        if payload.get("msg") == "param error":
            raise ValueError("Tencent rejected the explicit kline parameters")
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
