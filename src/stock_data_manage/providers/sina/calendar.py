from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from ..akshare.session import load_client
from ..contracts import InputFetchResult, ProviderContractError, FailureClass
from ...service.calendar import CalendarDay


@dataclass(slots=True)
class SinaTradingCalendarProvider:
    client: Any | None = None
    name: str = "sina"
    endpoint: str = "trading_calendar"
    capability_version: str = "sina-trade-calendar-input-v1"
    priority: int = 10

    def fetch(self) -> InputFetchResult:
        frame = (self.client or load_client()).tool_trade_date_hist_sina()
        if tuple(frame.columns) != ("trade_date",):
            raise ProviderContractError("Sina calendar columns changed", FailureClass.SCHEMA_CHANGED, retryable=False)
        return InputFetchResult(tuple(frame.to_dict(orient="records")),
                                "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt")

    def fetch_calendar(self, start: date, end: date) -> list[CalendarDay]:
        if end < start:
            raise ValueError("calendar end precedes start")
        # Only positive dates are returned; absence is never inferred as a closed session.
        return [CalendarDay(row["trade_date"], True, self.name, self.priority)
                for row in self.fetch().rows if start <= row["trade_date"] <= end]
