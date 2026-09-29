from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Mapping, Sequence

from ...routing.capabilities import ProviderCapability
from ..contracts import FailureClass, ProviderContractError
from ..transport import RealtimeFetchResponse, call_tdx_bars, parse_provider_datetime, tdx_code, tdx_records, tdx_value


@dataclass(slots=True)
class TdxMinuteProvider:
    """Adapter around an injected TDX-compatible client."""

    client_factory: Callable[[], object]
    capability: ProviderCapability
    name: str = "tdx"
    endpoint: str = "delayed_1m"
    timeout_seconds: float = 10.0
    count: int = 240
    frequency: int = 1
    frequency_codes: Mapping[int, int] = field(default_factory=lambda: {1: 8, 5: 0})

    @property
    def capability_version(self) -> str:
        return self.capability.version

    def fetch_realtime_minute(self, symbols: Sequence[str], as_of: datetime) -> RealtimeFetchResponse:
        requested = tuple(symbols)
        rows = []
        client = self.client_factory()
        try:
            category = self.frequency_codes.get(self.frequency, self.frequency)
            for source_symbol in requested:
                code = tdx_code(source_symbol)
                try:
                    bars = call_tdx_bars(client, code, category, self.count)
                except ProviderContractError:
                    raise
                except (TimeoutError, OSError, ConnectionError) as exc:
                    raise ProviderContractError(
                        f"TDX minute request failed for {source_symbol}", FailureClass.CONNECTION, retryable=True
                    ) from exc
                for item in tdx_records(bars):
                    timestamp = tdx_value(item, "datetime", "bar_time", "time", "date")
                    if timestamp in (None, ""):
                        continue
                    parsed_time = parse_provider_datetime(timestamp)
                    values = {
                        "open": tdx_value(item, "open", "opening"),
                        "close": tdx_value(item, "close", "price"),
                        "high": tdx_value(item, "high"),
                        "low": tdx_value(item, "low"),
                        "volume": tdx_value(item, "volume", "vol"),
                    }
                    if any(value is None for value in values.values()):
                        raise ProviderContractError(
                            f"TDX minute row schema changed for {source_symbol}", FailureClass.SCHEMA_CHANGED, retryable=False
                        )
                    rows.append({
                        "symbol": source_symbol, "trade_date": parsed_time.date().isoformat(),
                        "bar_time": parsed_time.isoformat(), **values,
                        "amount": tdx_value(item, "amount", "turnover"),
                    })
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        return RealtimeFetchResponse(
            tuple(rows), requested, (),
            ("trade_date", "bar_time", "open", "high", "low", "close", "volume", "amount"),
            ("volume:client_defined", "amount:client_defined"), len(rows),
            str(rows[0]["bar_time"]) if rows else None, str(rows[-1]["bar_time"]) if rows else None,
        )
