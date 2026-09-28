from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable

from ..quality.resolution import Conflict, resolve_records
from ..domain import Adjustment, BarRecord, Dataset, ItemStatus, QualityStatus
from ..storage.metadata import MetadataStore
from ..storage.parquet import CanonicalPartitionStore, PublishResult


@dataclass(frozen=True, slots=True)
class DailyReconciliationResult:
    publish: PublishResult
    selected_count: int
    expected_count: int
    hard_conflict_keys: tuple[tuple[object, ...], ...]
    completeness_status: str


class DailyReconciliation:
    """Promote provisional daily bars when a final history source arrives."""

    def __init__(self, *, canonical: CanonicalPartitionStore, metadata: MetadataStore) -> None:
        self.canonical = canonical
        self.metadata = metadata

    def reconcile(
        self,
        *,
        asset_type: str,
        trade_date: date,
        expected_instrument_ids: Iterable[str],
        history_records: Iterable[BarRecord],
        adjustment: Adjustment = Adjustment.NONE,
        run_id: str = "daily-reconciliation",
    ) -> DailyReconciliationResult:
        expected_ids = tuple(dict.fromkeys(str(item) for item in expected_instrument_ids))
        existing = self.canonical.read(Dataset.DAILY_BAR, asset_type, trade_date.isoformat())
        candidates: dict[tuple[object, ...], list[BarRecord]] = {}
        for record in [*existing, *history_records]:
            if (
                record.dataset is Dataset.DAILY_BAR
                and record.trade_date == trade_date
                and record.adjustment == adjustment
                and record.instrument_id in expected_ids
            ):
                candidates.setdefault(record.key, []).append(record)

        selected: list[BarRecord] = []
        conflicts: list[Conflict] = []
        hard_keys: list[tuple[object, ...]] = []
        statuses: dict[str, ItemStatus] = {}
        source_providers: dict[str, str | None] = {}
        for instrument_id in expected_ids:
            key_candidates = [
                records for key, records in candidates.items() if key[0] == instrument_id
            ]
            if not key_candidates:
                statuses[instrument_id] = ItemStatus.MISSING
                continue
            # A daily partition has one adjustment key per instrument. Multiple
            # keys indicate inconsistent input and are treated as conflicts.
            flattened = [record for records in key_candidates for record in records]
            result = resolve_records(flattened) if len({record.key for record in flattened}) == 1 else None
            if result is None:
                hard_keys.append((instrument_id, trade_date, adjustment))
                statuses[instrument_id] = ItemStatus.QUARANTINED
                continue
            conflicts.extend(result.conflicts)
            if result.quarantined or result.selected is None:
                hard_keys.append(result.selected.key if result.selected is not None else flattened[0].key)
                statuses[instrument_id] = ItemStatus.QUARANTINED
                continue
            selected.append(result.selected)
            statuses[instrument_id] = (
                ItemStatus.SUCCESS
                if result.selected.quality_status is QualityStatus.FINAL
                else ItemStatus.TEMPORARY_EMPTY
            )
            source_providers[instrument_id] = result.selected.source_provider

        if all(status is ItemStatus.SUCCESS for status in statuses.values()) and len(statuses) == len(expected_ids):
            completeness = "complete"
        elif hard_keys and len(selected) == len(expected_ids):
            completeness = "complete_with_conflicts"
        else:
            completeness = "partial"
        publish = self.canonical.publish(
            dataset=Dataset.DAILY_BAR,
            asset_type=asset_type,
            partition_key=trade_date.isoformat(),
            new_records=selected,
            expected_count=len(expected_ids),
            run_id=run_id,
            conflict_count=len(conflicts),
            quarantined_count=len(hard_keys),
            item_statuses=statuses,
            source_providers=source_providers,
        )
        now = datetime.now(timezone.utc)
        self.metadata.record_partition_publish(
            manifest=publish.manifest,
            manifest_path=publish.manifest_path,
            item_statuses=statuses,
            source_providers=source_providers,
            updated_at=now,
        )
        self.metadata.record_conflicts(
            dataset=Dataset.DAILY_BAR.value,
            partition_key=trade_date.isoformat(),
            conflicts=tuple(conflicts),
            created_at=now,
        )
        return DailyReconciliationResult(
            publish, len(selected), len(expected_ids), tuple(hard_keys), completeness
        )
