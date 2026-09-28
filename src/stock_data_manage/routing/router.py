from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, Mapping

from .capabilities import ProviderCapability
from ..domain import ItemStatus


COMPLETED_ITEM_STATUSES = {
    ItemStatus.SUCCESS,
    ItemStatus.NO_TRADE,
    ItemStatus.CONFIRMED_NO_DATA,
}

ALLOWED_MINUTE_UNIVERSES = {"watchlist", "strategy_candidate", "static"}


def calculate_missing_set(
    expected_instrument_ids: Iterable[str],
    item_statuses: Mapping[str, ItemStatus],
) -> frozenset[str]:
    expected = frozenset(expected_instrument_ids)
    completed = {
        instrument_id
        for instrument_id, status in item_statuses.items()
        if status in COMPLETED_ITEM_STATUSES
    }
    return frozenset(expected - completed)


def split_trading_dates(
    start_date: date,
    end_date: date,
    trading_dates: Iterable[date],
) -> tuple[date, ...]:
    if end_date < start_date:
        raise ValueError("end_date cannot be before start_date")
    available = set(trading_dates)
    days = []
    current = start_date
    while current <= end_date:
        if current in available:
            days.append(current)
        current += timedelta(days=1)
    return tuple(days)


@dataclass(frozen=True, slots=True)
class RealtimePlan:
    symbols: tuple[str, ...]
    capability: ProviderCapability
    estimated_cycle_seconds: float


def plan_realtime_collection(
    *,
    universe_type: str,
    symbols: Iterable[str],
    capabilities: Iterable[ProviderCapability],
    max_watchlist_symbols: int,
    cycle_deadline_seconds: float,
) -> RealtimePlan:
    if universe_type not in ALLOWED_MINUTE_UNIVERSES:
        raise ValueError(f"minute collection does not allow universe: {universe_type}")
    unique_symbols = tuple(dict.fromkeys(symbols))
    if len(unique_symbols) > max_watchlist_symbols:
        raise ValueError(
            f"watchlist has {len(unique_symbols)} symbols; maximum is {max_watchlist_symbols}"
        )
    viable = [
        (capability.estimated_cycle_seconds(len(unique_symbols)), capability)
        for capability in capabilities
        if capability.estimated_cycle_seconds(len(unique_symbols)) <= cycle_deadline_seconds
    ]
    if not viable:
        raise ValueError("no capability can meet the realtime cycle deadline")
    estimated, selected = min(viable, key=lambda pair: (-pair[1].priority, pair[0]))
    return RealtimePlan(unique_symbols, selected, estimated)
