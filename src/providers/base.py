from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping, Protocol, Sequence

from ..domain import Adjustment, AssetType
from .contracts import WindowStatus


@dataclass(frozen=True, slots=True)
class FetchResult:
    rows: tuple[Mapping[str, Any], ...]
    requested_symbols: tuple[str, ...]
    window_status: WindowStatus | None = None
    response_statuses: tuple[int, ...] = ()
    field_semantics: tuple[str, ...] = ()
    units: tuple[str, ...] = ()
    returned_row_count: int | None = None
    returned_first_key: str | None = None
    returned_last_key: str | None = None
    adjustment: Adjustment = Adjustment.NONE

    @property
    def returned_symbols(self) -> frozenset[str]:
        return frozenset(str(row["symbol"]) for row in self.rows)

    @property
    def missing_symbols(self) -> frozenset[str]:
        return frozenset(self.requested_symbols) - self.returned_symbols


class DailyProvider(Protocol):
    name: str
    endpoint: str
    capability_priority: int
    capability_version: str
    adjustment: Adjustment
    supported_asset_types: frozenset[AssetType]

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult: ...


@dataclass(slots=True)
class FixtureDailyProvider:
    name: str
    endpoint: str
    rows_by_symbol: Mapping[str, Mapping[str, Any]]
    capability_priority: int = 100
    capability_version: str = "fixture-v1"
    failures: list[Exception] = field(default_factory=list)
    requests: list[tuple[str, ...]] = field(default_factory=list)
    adjustment: Adjustment = Adjustment.NONE
    supported_asset_types: frozenset[AssetType] = field(
        default_factory=lambda: frozenset(AssetType)
    )

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        requested = tuple(symbols)
        self.requests.append(requested)
        if self.failures:
            raise self.failures.pop(0)
        rows = tuple(dict(self.rows_by_symbol[symbol]) for symbol in requested if symbol in self.rows_by_symbol)
        return FetchResult(rows=rows, requested_symbols=requested, adjustment=self.adjustment)
