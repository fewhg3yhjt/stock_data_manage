from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import ceil
from typing import Callable

from ..domain import Adjustment, AssetType, Dataset, Exchange


@dataclass(frozen=True, slots=True)
class ProviderCapability:
    provider: str
    endpoint: str
    version: str
    datasets: frozenset[Dataset]
    exchanges: frozenset[Exchange]
    asset_types: frozenset[AssetType]
    frequencies: frozenset[int]
    adjustments: frozenset[Adjustment]
    priority: int
    validated_at: datetime
    validation_expires_at: datetime
    enabled: bool = True
    role: str = "primary"
    code_prefixes: tuple[str, ...] = ()
    excluded_code_prefixes: tuple[str, ...] = ()
    max_symbols_per_request: int = 1
    max_rows_per_request: int | None = None
    max_days_per_request: int | None = None
    supports_pagination: bool = False
    request_interval_seconds: float = 1.0
    effective_concurrency: int = 1
    freshness_delay_seconds: int = 0
    request_shape: str = "symbol_batch"
    base_requests_per_fetch: int = 1
    max_pages_per_request: int | None = None
    validated_symbols: frozenset[str] = frozenset()
    evidence_hash: str | None = None

    @property
    def capability_id(self) -> str:
        return f"{self.provider}.{self.endpoint}@{self.version}"

    def is_valid_at(self, now: datetime) -> bool:
        return self.enabled and self.validated_at <= now < self.validation_expires_at

    def matches(
        self,
        *,
        dataset: Dataset,
        exchange: Exchange,
        asset_type: AssetType,
        symbol: str,
        frequency: int | None,
        adjustment: Adjustment,
    ) -> bool:
        if dataset not in self.datasets:
            return False
        if exchange not in self.exchanges or asset_type not in self.asset_types:
            return False
        if frequency is not None and frequency not in self.frequencies:
            return False
        if adjustment not in self.adjustments:
            return False
        if self.validated_symbols and symbol not in self.validated_symbols:
            return False
        if self.code_prefixes and not symbol.startswith(self.code_prefixes):
            return False
        if self.excluded_code_prefixes and symbol.startswith(self.excluded_code_prefixes):
            return False
        return True

    def estimated_cycle_seconds(self, symbol_count: int) -> float:
        if symbol_count < 0:
            raise ValueError("symbol_count cannot be negative")
        if symbol_count == 0:
            return 0.0
        if self.supports_pagination and self.max_pages_per_request is None:
            return float("inf")
        if self.request_shape in {"date_snapshot", "full_snapshot", "file_package", "paged_list"}:
            batches = 1
        else:
            batch_size = 1 if self.request_shape == "single_symbol" else max(1, self.max_symbols_per_request)
            batches = ceil(symbol_count / batch_size)
        requests = batches * self.base_requests_per_fetch * (self.max_pages_per_request or 1)
        # A group's minimum start interval still applies when requests overlap.
        return requests * max(0.0, self.request_interval_seconds)


class CapabilityRegistry:
    def __init__(self, capabilities: list[ProviderCapability] | None = None, *, availability=None) -> None:
        self._capabilities = list(capabilities or [])
        self._availability = availability

    def register(self, capability: ProviderCapability) -> None:
        self._capabilities.append(capability)

    def select(
        self,
        *,
        now: datetime,
        dataset: Dataset,
        exchange: Exchange,
        asset_type: AssetType,
        symbol: str,
        adjustment: Adjustment,
        frequency: int | None = None,
        symbol_count: int = 1,
        cycle_deadline_seconds: float | None = None,
        include_validation_only: bool = False,
        availability: Callable[[ProviderCapability], bool] | None = None,
    ) -> list[ProviderCapability]:
        selected = []
        for capability in self._capabilities:
            if not capability.is_valid_at(now):
                continue
            if capability.role == "disabled":
                continue
            if capability.role == "validation_only" and not include_validation_only:
                continue
            if self._availability is not None and not self._availability(capability, now):
                continue
            if availability is not None and not availability(capability):
                continue
            if not capability.matches(
                dataset=dataset,
                exchange=exchange,
                asset_type=asset_type,
                symbol=symbol,
                frequency=frequency,
                adjustment=adjustment,
            ):
                continue
            if (
                cycle_deadline_seconds is not None
                and capability.estimated_cycle_seconds(symbol_count) > cycle_deadline_seconds
            ):
                continue
            selected.append(capability)
        return sorted(
            selected,
            key=lambda item: (
                -item.priority,
                item.estimated_cycle_seconds(symbol_count),
                item.provider,
                item.endpoint,
            ),
        )
