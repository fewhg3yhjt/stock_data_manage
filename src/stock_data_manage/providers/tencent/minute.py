from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Sequence

from ...routing.capabilities import ProviderCapability
from ..contracts import EndpointContract, FailureClass, ProviderContractError, InputFetchResult
from ..transport import HttpTransport, RealtimeFetchResponse, parse_provider_datetime
from .daily import tencent_symbol, tencent_kline_call


@dataclass(slots=True)
class TencentMinuteProvider:
    transport: HttpTransport
    capability: ProviderCapability
    name: str = "tencent"
    endpoint: str = "native_1m"
    timeout_seconds: float = 10.0
    count: int = 120
    url: str = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
    input_hosts: tuple[str, ...] = ("https://web.ifzq.gtimg.cn", "https://proxy.finance.qq.com/ifzqgtimg", "https://ifzq.gtimg.cn")
    host_down_until: dict[str, float] = field(default_factory=dict)

    @property
    def capability_version(self) -> str:
        return self.capability.version

    def fetch_realtime_minute(self, symbols: Sequence[str], as_of: datetime) -> RealtimeFetchResponse:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        response_statuses: list[int] = []
        for symbol in requested:
            response = self.transport.get(
                self.url, params={"param": f"{symbol},m1,,{self.count}"}, timeout_seconds=self.timeout_seconds
            )
            response_statuses.append(response.status_code)
            payload = EndpointContract(frozenset()).parse_json(response)
            if not isinstance(payload, dict) or payload.get("code") not in {0, "0", None}:
                raise ProviderContractError(
                    "Tencent minute response envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False
                )
            bars = (((payload.get("data") or {}).get(symbol) or {}).get("m1") or [])
            for item in bars:
                if not isinstance(item, list) or len(item) < 6:
                    continue
                parsed_time = parse_provider_datetime(item[0])
                rows.append({
                    "symbol": symbol, "trade_date": parsed_time.date().isoformat(),
                    "bar_time": parsed_time.isoformat(), "open": item[1], "close": item[2],
                    "high": item[3], "low": item[4], "volume": item[5], "amount": None,
                })
        as_of_naive = as_of.replace(tzinfo=None)
        rows = [row for row in rows if parse_provider_datetime(row["bar_time"]) <= as_of_naive]
        return RealtimeFetchResponse(
            tuple(rows), requested, tuple(response_statuses),
            ("trade_date", "bar_time", "open", "high", "low", "close", "volume"),
            tuple(f"volume:{'share' if symbol[2:].startswith(('688', '689')) else 'lot'}" for symbol in requested),
            len(rows), str(rows[0]["bar_time"]) if rows else None, str(rows[-1]["bar_time"]) if rows else None,
        )

    def fetch_recent(self, code: str, count: int = 96) -> InputFetchResult:
        symbol = tencent_symbol(code)
        if type(count) is not int or not 1 <= count <= 320:
            raise ValueError("recent 5m count must be an integer in 1..320")
        data, host = tencent_kline_call(self.transport, "/appstock/app/kline/mkline",
            f"{symbol},m5,,{count}", self.input_hosts, self.host_down_until)
        node = data.get(symbol)
        if not isinstance(node, dict) or not isinstance(node.get("m5"), list):
            raise ProviderContractError("Tencent m5 list changed", FailureClass.SCHEMA_CHANGED, retryable=False)
        rows, seen = [], set()
        for item in node["m5"]:
            if not isinstance(item, list) or len(item) < 6:
                raise ProviderContractError("Tencent m5 row changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            try:
                stamp = datetime.strptime(str(item[0]), "%Y%m%d%H%M")
            except ValueError as exc:
                raise ProviderContractError("Tencent m5 timestamp changed", FailureClass.SCHEMA_CHANGED, retryable=False) from exc
            if stamp in seen:
                raise ProviderContractError("duplicate Tencent m5 timestamp", FailureClass.SCHEMA_CHANGED, retryable=False)
            seen.add(stamp)
            rows.append({"symbol": symbol, **{str(i): value for i, value in enumerate(item)}})
        if not rows:
            raise ProviderContractError("Tencent recent m5 has zero bars", FailureClass.TEMPORARY_EMPTY, retryable=True)
        return InputFetchResult(tuple(rows), host + "/appstock/app/kline/mkline")
