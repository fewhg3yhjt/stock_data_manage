from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Callable, Mapping, Protocol, Sequence

from .capability import ProviderCapability
from .domain import Adjustment, AssetType, BarRecord, Dataset, Exchange, ItemStatus, QualityStatus
from .hot_store import HotMinuteStore
from .normalizer import Normalizer
from .planner import plan_realtime_collection
from .sessions import MarketSchedule
from .validator import validate_bar


@dataclass(frozen=True, slots=True)
class MinuteFetchResult:
    rows: tuple[Mapping[str, Any], ...]
    requested_symbols: tuple[str, ...]

    @property
    def returned_symbols(self) -> frozenset[str]:
        return frozenset(str(row["symbol"]) for row in self.rows)


class RealtimeMinuteProvider(Protocol):
    name: str
    endpoint: str
    capability: ProviderCapability

    def fetch_realtime_minute(self, symbols: Sequence[str], as_of: datetime) -> MinuteFetchResult: ...


@dataclass(slots=True)
class FixtureRealtimeMinuteProvider:
    name: str
    endpoint: str
    capability: ProviderCapability
    rows: Mapping[str, Mapping[str, Any]]
    requests: list[tuple[str, ...]]

    def fetch_realtime_minute(self, symbols: Sequence[str], as_of: datetime) -> MinuteFetchResult:
        requested = tuple(symbols)
        self.requests.append(requested)
        return MinuteFetchResult(
            tuple(dict(self.rows[symbol]) for symbol in requested if symbol in self.rows), requested
        )


@dataclass(frozen=True, slots=True)
class RealtimeCycleResult:
    accepted: tuple[BarRecord, ...]
    missing_symbols: tuple[str, ...]
    rejected_rows: int
    estimated_cycle_seconds: float
    skipped: bool


class RealtimeMinuteCollector:
    def __init__(
        self,
        *,
        provider: RealtimeMinuteProvider,
        capability: ProviderCapability,
        normalizer: Normalizer,
        hot_store: HotMinuteStore,
        schedule: MarketSchedule | None = None,
        max_watchlist_symbols: int = 200,
        cycle_deadline_seconds: float = 50,
        finalize_delay_seconds: int = 2,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.provider = provider
        self.capability = capability
        self.normalizer = normalizer
        self.hot_store = hot_store
        self.schedule = schedule or MarketSchedule()
        self.max_watchlist_symbols = max_watchlist_symbols
        self.cycle_deadline_seconds = cycle_deadline_seconds
        self.finalize_delay_seconds = finalize_delay_seconds
        self.clock = clock or datetime.now

    def collect_cycle(
        self,
        *,
        universe_type: str,
        symbols: Sequence[str],
        instrument_id_by_symbol: Mapping[str, str],
        exchange: Exchange,
        asset_type: AssetType,
        now: datetime | None = None,
        adjustment: Adjustment = Adjustment.NONE,
    ) -> RealtimeCycleResult:
        now = now or self.clock()
        if not self.schedule.in_realtime_window(now) or self.schedule.session_for(now) is None:
            return RealtimeCycleResult((), (), 0, 0, True)
        plan = plan_realtime_collection(
            universe_type=universe_type,
            symbols=symbols,
            capabilities=[self.capability],
            max_watchlist_symbols=self.max_watchlist_symbols,
            cycle_deadline_seconds=self.cycle_deadline_seconds,
        )
        response = self.provider.fetch_realtime_minute(plan.symbols, now)
        accepted: list[BarRecord] = []
        rejected = 0
        cutoff = now - timedelta(seconds=self.finalize_delay_seconds)
        for raw in response.rows:
            symbol = str(raw.get("symbol", ""))
            instrument_id = instrument_id_by_symbol.get(symbol)
            if instrument_id is None:
                rejected += 1
                continue
            try:
                record = self.normalizer.normalize_bar(
                    raw,
                    dataset=Dataset.MINUTE_BAR_1M,
                    instrument_id=instrument_id,
                    exchange=exchange,
                    asset_type=asset_type,
                    provider=self.provider.name,
                    endpoint=self.provider.endpoint,
                    adjustment=adjustment,
                    capability_priority=self.capability.priority,
                    capability_version=self.capability.version,
                    raw_object_path=str(raw.get("raw_object_path", "realtime")),
                    fetch_time=now,
                    quality_status=QualityStatus.PROVISIONAL,
                    source_method="realtime_minute",
                    frequency=1,
                )
                if record.bar_time is None or record.bar_time > cutoff:
                    rejected += 1
                    continue
                if not self.schedule.is_expected_bar_time(record.bar_time) or not validate_bar(record).valid:
                    rejected += 1
                    continue
            except Exception:
                rejected += 1
                continue
            self.hot_store.upsert(record)
            accepted.append(record)
        missing = tuple(sorted(set(plan.symbols) - response.returned_symbols))
        return RealtimeCycleResult(tuple(accepted), missing, rejected, plan.estimated_cycle_seconds, False)
