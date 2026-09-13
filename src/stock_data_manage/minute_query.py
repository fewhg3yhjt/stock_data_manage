from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from .canonical_storage import CanonicalPartitionStore
from .domain import Adjustment, BarRecord, Dataset
from .hot_store import HotMinuteStore
from .resolution import Conflict, resolve_records


@dataclass(frozen=True, slots=True)
class MinuteQueryResult:
    records: tuple[BarRecord, ...]
    conflicts: tuple[Conflict, ...]


class MinuteQuery:
    def __init__(self, *, canonical: CanonicalPartitionStore, hot: HotMinuteStore) -> None:
        self.canonical = canonical
        self.hot = hot

    def query(
        self,
        *,
        instrument_id: str,
        asset_type: str,
        start: datetime,
        end: datetime,
        interval_minutes: int = 1,
        adjustment: Adjustment = Adjustment.NONE,
    ) -> MinuteQueryResult:
        records: list[BarRecord] = []
        current = start.date()
        while current <= end.date():
            records.extend(
                record
                for record in self.canonical.read(
                    Dataset.MINUTE_BAR_1M if interval_minutes == 1 else Dataset.MINUTE_BAR_5M,
                    asset_type,
                    current.isoformat(),
                )
                if record.instrument_id == instrument_id
                and record.bar_time is not None
                and start <= record.bar_time <= end
                and record.interval_minutes == interval_minutes
                and record.adjustment == adjustment
            )
            current += timedelta(days=1)
        records.extend(
            self.hot.query(
                instrument_id,
                start,
                end,
                interval_minutes=interval_minutes,
                adjustment=adjustment,
            )
        )
        grouped: dict[tuple[object, ...], list[BarRecord]] = {}
        for record in records:
            grouped.setdefault(record.key, []).append(record)
        selected: list[BarRecord] = []
        conflicts: list[Conflict] = []
        for candidates in grouped.values():
            result = resolve_records(candidates)
            conflicts.extend(result.conflicts)
            if result.selected is not None:
                selected.append(result.selected)
        return MinuteQueryResult(tuple(sorted(selected, key=lambda record: record.key)), tuple(conflicts))

