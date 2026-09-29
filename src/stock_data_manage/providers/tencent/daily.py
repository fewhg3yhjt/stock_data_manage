from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Sequence

from ...domain import Adjustment, AssetType
from ...routing.capabilities import ProviderCapability
from ..base import FetchResult
from ..contracts import EndpointContract, FailureClass, ProviderContractError, WindowStatus
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


def _combined_status(statuses: list[WindowStatus], target_rows_found: bool) -> WindowStatus:
    if WindowStatus.TRUNCATED in statuses:
        return WindowStatus.TRUNCATED
    if not target_rows_found:
        return WindowStatus.TEMPORARY_EMPTY
    if WindowStatus.PARTIAL in statuses:
        return WindowStatus.PARTIAL
    return WindowStatus.COMPLETE
