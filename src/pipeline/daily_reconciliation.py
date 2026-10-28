"""Explicit reconciliation of complete daily results; snapshots remain candidates."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

from ..domain import DailyRecord, ItemStatus
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
    """Replace explicitly supplied complete historical results using the common store."""

    def __init__(self, *, canonical: CanonicalPartitionStore, metadata: MetadataStore) -> None:
        self.canonical = canonical
        self.metadata = metadata

    def reconcile(self, *, asset_type: str, trade_date: date,
                  expected_instrument_ids: Iterable[str], history_records: Iterable[DailyRecord],
                  run_id: str = "daily-reconciliation") -> DailyReconciliationResult:
        expected = set(expected_instrument_ids)
        records = list(history_records)
        if not expected or any(not isinstance(r, DailyRecord) or not r.complete for r in records):
            raise ValueError("reconciliation requires complete aggregate daily results")
        if len({r.key for r in records}) != len(records):
            raise ValueError("duplicate historical daily result")
        if {r.instrument_id for r in records} != expected:
            raise ValueError("historical daily results must cover the explicit target scope")
        statuses = {identity: ItemStatus.SUCCESS for identity in expected}
        publish = self.canonical.publish_daily(asset_type=asset_type, partition_key=trade_date.isoformat(),
            new_records=records, expected_count=len(expected), run_id=run_id,
            item_statuses=statuses, replace_instrument_ids=expected)
        self.metadata.record_partition_publish(manifest=publish.manifest, manifest_path=publish.manifest_path,
            item_statuses=statuses, source_providers=publish.manifest.source_providers)
        return DailyReconciliationResult(publish, len(records), len(expected), (), "complete")
