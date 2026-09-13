from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Iterable

from .canonical_storage import CanonicalPartitionStore, PublishResult
from .domain import Adjustment, BarRecord, Dataset, ItemStatus
from .hot_store import HotMinuteStore
from .metadata import MetadataStore
from .resolution import Conflict, resolve_records
from .sessions import MarketSchedule


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    publish: PublishResult
    selected_count: int
    expected_count: int
    hard_conflict_keys: tuple[tuple[object, ...], ...]
    completeness_status: str


class MinuteReconciliation:
    def __init__(
        self,
        *,
        canonical: CanonicalPartitionStore,
        hot: HotMinuteStore,
        metadata: MetadataStore,
        schedule: MarketSchedule | None = None,
    ) -> None:
        self.canonical = canonical
        self.hot = hot
        self.metadata = metadata
        self.schedule = schedule or MarketSchedule()

    def reconcile(
        self,
        *,
        instrument_id: str,
        asset_type: str,
        trade_date: date,
        history_records: Iterable[BarRecord],
        interval_minutes: int = 1,
        adjustment: Adjustment = Adjustment.NONE,
        expected_bar_times: Iterable[datetime] | None = None,
        run_id: str = "minute-reconciliation",
    ) -> ReconciliationResult:
        if interval_minutes not in {1, 5}:
            raise ValueError("only 1m and 5m reconciliation is supported")
        expected = tuple(
            expected_bar_times
            if expected_bar_times is not None
            else self.schedule.expected_bar_times(trade_date)
        )
        start = datetime.combine(trade_date, time(0), tzinfo=self.schedule.timezone)
        end = datetime.combine(trade_date, time(23, 59, 59), tzinfo=self.schedule.timezone)
        hot_records = self.hot.query(
            instrument_id,
            start,
            end,
            interval_minutes=interval_minutes,
            adjustment=adjustment,
        )
        history = [
            record
            for record in history_records
            if record.instrument_id == instrument_id
            and record.trade_date == trade_date
            and record.interval_minutes == interval_minutes
            and record.adjustment == adjustment
        ]
        grouped: dict[tuple[object, ...], list[BarRecord]] = {}
        for record in [*hot_records, *history]:
            grouped.setdefault(record.key, []).append(record)
        selected: list[BarRecord] = []
        conflicts: list[Conflict] = []
        hard_keys: list[tuple[object, ...]] = []
        for key, candidates in grouped.items():
            result = resolve_records(candidates)
            conflicts.extend(result.conflicts)
            if result.quarantined:
                hard_keys.append(key)
            elif result.selected is not None:
                selected.append(result.selected)
        status = "complete" if len(selected) >= len(expected) and not hard_keys else "partial"
        if hard_keys:
            status = "complete_with_conflicts" if len(selected) >= len(expected) else "partial"
        publish = self.canonical.publish(
            dataset=Dataset.MINUTE_BAR_1M if interval_minutes == 1 else Dataset.MINUTE_BAR_5M,
            asset_type=asset_type,
            partition_key=trade_date.isoformat(),
            new_records=selected,
            expected_count=len(expected),
            run_id=run_id,
            conflict_count=len(conflicts),
            quarantined_count=len(hard_keys),
        )
        first_timestamp = min((record.bar_time for record in selected if record.bar_time), default=None)
        last_timestamp = max((record.bar_time for record in selected if record.bar_time), default=None)
        self.metadata.record_minute_completeness(
            instrument_id=instrument_id,
            trade_date=trade_date,
            interval_minutes=interval_minutes,
            adjustment=adjustment.value,
            expected_rows=len(expected),
            actual_rows=len(selected),
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            status=status,
        )
        self.metadata.record_conflicts(
            dataset=(Dataset.MINUTE_BAR_1M if interval_minutes == 1 else Dataset.MINUTE_BAR_5M).value,
            partition_key=trade_date.isoformat(),
            conflicts=tuple(conflicts),
        )
        return ReconciliationResult(publish, len(selected), len(expected), tuple(hard_keys), status)

