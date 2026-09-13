from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping, Protocol

from .trading_calendar import CalendarDay, CalendarMergeResult, TradingCalendarStore, merge_calendar_sources


class CalendarProvider(Protocol):
    name: str
    priority: int

    def fetch_calendar(self, start: date, end: date) -> list[CalendarDay]: ...


@dataclass(frozen=True, slots=True)
class CalendarUpdateResult:
    merge: CalendarMergeResult
    source_errors: dict[str, str]


class TradingCalendarUpdater:
    def __init__(self, store: TradingCalendarStore, providers: list[CalendarProvider]) -> None:
        self.store = store
        self.providers = tuple(providers)

    def update(self, *, start: date, end: date) -> CalendarUpdateResult:
        sources: dict[str, list[CalendarDay]] = {}
        errors: dict[str, str] = {}
        for provider in self.providers:
            try:
                sources[provider.name] = [
                    CalendarDay(day.trade_date, day.is_trading_day, provider.name, provider.priority, day.evidence)
                    for day in provider.fetch_calendar(start, end)
                ]
            except Exception as exc:
                sources[provider.name] = []
                errors[provider.name] = str(exc)
        previous = tuple(day for day in self.store.all() if start <= day.trade_date <= end)
        merged = merge_calendar_sources(sources, previous=previous)
        self.store.upsert(merged.days)
        return CalendarUpdateResult(merged, errors)

