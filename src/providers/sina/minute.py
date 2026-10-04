from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence

from ...routing.capabilities import ProviderCapability
from ..contracts import EndpointContract, FailureClass, ProviderContractError
from ..transport import HttpTransport, RealtimeFetchResponse, parse_provider_datetime


@dataclass(slots=True)
class SinaMinuteProvider:
    transport: HttpTransport
    capability: ProviderCapability
    name: str = "sina"
    endpoint: str = "native_5m"
    timeout_seconds: float = 15.0
    scale: int = 5
    count: int = 120
    url: str = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"

    @property
    def capability_version(self) -> str:
        return self.capability.version

    def fetch_realtime_minute(self, symbols: Sequence[str], as_of: datetime) -> RealtimeFetchResponse:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        response_statuses: list[int] = []
        for symbol in requested:
            response = self.transport.get(
                self.url,
                params={"symbol": symbol, "scale": str(self.scale), "ma": "no", "datalen": str(self.count)},
                timeout_seconds=self.timeout_seconds,
            )
            response_statuses.append(response.status_code)
            payload = EndpointContract(frozenset()).parse_json(response)
            if payload is None:
                continue
            if not isinstance(payload, list):
                raise ProviderContractError("Sina minute response envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            for item in payload:
                if not isinstance(item, dict) or not item.get("day"):
                    continue
                parsed_time = parse_provider_datetime(item["day"])
                rows.append({
                    "symbol": symbol, "trade_date": parsed_time.date().isoformat(),
                    "bar_time": parsed_time.isoformat(), "open": item.get("open"),
                    "close": item.get("close"), "high": item.get("high"), "low": item.get("low"),
                    "volume": item.get("volume"), "amount": item.get("amount"),
                })
        as_of_naive = as_of.replace(tzinfo=None)
        rows = [row for row in rows if parse_provider_datetime(row["bar_time"]) <= as_of_naive]
        return RealtimeFetchResponse(
            tuple(rows), requested, tuple(response_statuses),
            ("trade_date", "bar_time", "open", "high", "low", "close", "volume", "amount"),
            tuple("volume:share" for _ in requested), len(rows),
            str(rows[0]["bar_time"]) if rows else None, str(rows[-1]["bar_time"]) if rows else None,
        )
