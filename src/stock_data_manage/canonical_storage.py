from __future__ import annotations

import os
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .domain import Adjustment, BarRecord, Dataset, ItemStatus, QualityStatus
from .integrity import Manifest, row_hash
from .resolution import Conflict, resolve_records
from .validator import validate_bar


BAR_SCHEMA = pa.schema(
    [
        ("dataset", pa.string()),
        ("instrument_id", pa.string()),
        ("source_symbol", pa.string()),
        ("trade_date", pa.date32()),
        ("bar_time", pa.timestamp("us", tz="Asia/Shanghai")),
        ("interval_minutes", pa.int16()),
        ("adjustment", pa.string()),
        ("open", pa.decimal128(38, 8)),
        ("high", pa.decimal128(38, 8)),
        ("low", pa.decimal128(38, 8)),
        ("close", pa.decimal128(38, 8)),
        ("volume", pa.decimal128(38, 8)),
        ("amount", pa.decimal128(38, 8)),
        ("source_provider", pa.string()),
        ("endpoint", pa.string()),
        ("source_method", pa.string()),
        ("quality_status", pa.string()),
        ("field_level", pa.string()),
        ("capability_priority", pa.int32()),
        ("capability_version", pa.string()),
        ("raw_object_path", pa.string()),
        ("fetch_time", pa.timestamp("us", tz="Asia/Shanghai")),
        ("volume_semantics", pa.string()),
        ("freshness_class", pa.string()),
        ("source_delay_seconds", pa.int32()),
        ("as_of", pa.timestamp("us", tz="Asia/Shanghai")),
        ("normalizer_version", pa.string()),
        ("resolution_policy_version", pa.string()),
        ("flags", pa.list_(pa.string())),
    ]
)


@dataclass(frozen=True, slots=True)
class PublishResult:
    manifest: Manifest
    manifest_path: Path
    records: tuple[BarRecord, ...]
    conflicts: tuple[Conflict, ...]
    quarantined_keys: tuple[tuple[object, ...], ...]
    no_op: bool


class PartitionLockedError(RuntimeError):
    pass


class InvalidPartitionError(RuntimeError):
    pass


class PartitionLock(AbstractContextManager["PartitionLock"]):
    def __init__(self, path: Path) -> None:
        self.path = path
        self._acquired = False

    def __enter__(self) -> "PartitionLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise PartitionLockedError(f"partition is already locked: {self.path}") from exc
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(f"pid={os.getpid()}\n")
            stream.flush()
            os.fsync(stream.fileno())
        self._acquired = True
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._acquired:
            self.path.unlink(missing_ok=True)
            self._acquired = False


class CanonicalPartitionStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def partition_directory(self, dataset: Dataset, asset_type: str, partition_key: str) -> Path:
        return self.root / dataset.value / f"asset_type={asset_type}" / f"trade_date={partition_key}"

    def read(self, dataset: Dataset, asset_type: str, partition_key: str) -> list[BarRecord]:
        partition = self.partition_directory(dataset, asset_type, partition_key)
        path = partition / "data.parquet"
        manifest_path = partition / "manifest.json"
        if not path.exists() and not manifest_path.exists():
            return []
        if not path.exists() or not manifest_path.exists():
            raise InvalidPartitionError(f"canonical partition is incomplete: {partition}")
        try:
            manifest = Manifest.load(manifest_path)
        except Exception as exc:
            raise InvalidPartitionError(f"canonical manifest is invalid: {manifest_path}") from exc
        if not manifest.verify(path):
            raise InvalidPartitionError(f"canonical content does not match manifest: {partition}")
        return [_mapping_to_bar(item) for item in pq.read_table(path).to_pylist()]

    def publish(
        self,
        *,
        dataset: Dataset,
        asset_type: str,
        partition_key: str,
        new_records: Iterable[BarRecord],
        expected_count: int,
        run_id: str,
        conflict_count: int = 0,
        quarantined_count: int = 0,
        item_statuses: Mapping[str, ItemStatus] | None = None,
        source_providers: Mapping[str, str | None] | None = None,
        failure_hook: Callable[[str], None] | None = None,
    ) -> PublishResult:
        partition = self.partition_directory(dataset, asset_type, partition_key)
        partition.mkdir(parents=True, exist_ok=True)
        with PartitionLock(partition / ".publish.lock"):
            existing = self.read(dataset, asset_type, partition_key)
            merged, conflicts, quarantined = _merge(existing, list(new_records))
            invalid = [record for record in merged if not validate_bar(record).valid]
            if invalid:
                raise ValueError(f"refusing to publish {len(invalid)} invalid canonical records")
            if len({record.key for record in merged}) != len(merged):
                raise ValueError("canonical records contain duplicate keys")
            merged.sort(key=lambda record: tuple(str(part) for part in record.key))

            final_data = partition / "data.parquet"
            final_manifest = partition / "manifest.json"
            current_manifest = Manifest.load(final_manifest) if final_manifest.exists() else None
            candidate_hash = row_hash(merged)
            current_hash = row_hash(existing) if existing else None
            total_conflicts = conflict_count + len(conflicts)
            total_quarantined = quarantined_count + len(quarantined)
            resolved_item_statuses = dict(item_statuses or {})
            resolved_source_providers = dict(source_providers or {})
            for record in merged:
                resolved_item_statuses.setdefault(record.instrument_id, ItemStatus.SUCCESS)
                resolved_source_providers.setdefault(record.instrument_id, record.source_provider)
            if (
                current_manifest is not None
                and current_manifest.verify(final_data)
                and candidate_hash == current_hash
                and current_manifest.expected_count == expected_count
                and current_manifest.conflict_count == total_conflicts
                and current_manifest.quarantined_count == total_quarantined
                and current_manifest.item_statuses
                == {key: value.value for key, value in resolved_item_statuses.items()}
            ):
                return PublishResult(
                    current_manifest,
                    final_manifest,
                    tuple(existing),
                    tuple(conflicts),
                    tuple(quarantined),
                    True,
                )

            temp_data = partition / f"data.{run_id}.tmp.parquet"
            temp_manifest = partition / f"manifest.{run_id}.tmp.json"
            pq.write_table(_records_to_table(merged), temp_data, compression="zstd")
            _fsync_file(temp_data)
            _call_hook(failure_hook, "after_temp_data")

            raw_refs = tuple(
                dict.fromkeys(
                    path
                    for record in merged
                    for path in record.raw_object_path.split("|")
                    if path
                )
            )
            manifest = Manifest.from_file(
                temp_data,
                dataset=dataset.value,
                asset_type=asset_type,
                partition_key=partition_key,
                row_count=len(merged),
                expected_count=expected_count,
                first_key=str(merged[0].key) if merged else None,
                last_key=str(merged[-1].key) if merged else None,
                raw_refs=raw_refs,
                conflict_count=total_conflicts,
                quarantined_count=total_quarantined,
                item_statuses={key: value.value for key, value in resolved_item_statuses.items()},
                source_providers=resolved_source_providers,
            )
            manifest.write_atomic(temp_manifest)
            if not manifest.verify(temp_data):
                raise OSError("temporary canonical file failed manifest verification")
            _call_hook(failure_hook, "after_temp_manifest")
            os.replace(temp_data, final_data)
            _call_hook(failure_hook, "after_data_replace")
            os.replace(temp_manifest, final_manifest)
            _call_hook(failure_hook, "after_manifest_replace")
            return PublishResult(
                manifest,
                final_manifest,
                tuple(merged),
                tuple(conflicts),
                tuple(quarantined),
                False,
            )


def _merge(
    existing: list[BarRecord], new: list[BarRecord]
) -> tuple[list[BarRecord], list[Conflict], list[tuple[object, ...]]]:
    existing_by_key = {record.key: record for record in existing}
    candidates: dict[tuple[object, ...], list[BarRecord]] = {}
    for record in new:
        candidates.setdefault(record.key, []).append(record)
    merged = dict(existing_by_key)
    conflicts: list[Conflict] = []
    quarantined: list[tuple[object, ...]] = []
    for key, records in candidates.items():
        prior = existing_by_key.get(key)
        resolution = resolve_records(([prior] if prior is not None else []) + records)
        conflicts.extend(resolution.conflicts)
        if resolution.quarantined:
            quarantined.append(key)
            continue
        assert resolution.selected is not None
        merged[key] = resolution.selected
    return list(merged.values()), conflicts, quarantined


def _records_to_table(records: list[BarRecord]) -> pa.Table:
    mappings = [_bar_to_mapping(record) for record in records]
    return pa.Table.from_pylist(mappings, schema=BAR_SCHEMA)


def _bar_to_mapping(record: BarRecord) -> dict[str, object]:
    return {
        "dataset": record.dataset.value,
        "instrument_id": record.instrument_id,
        "source_symbol": record.source_symbol,
        "trade_date": record.trade_date,
        "bar_time": record.bar_time,
        "interval_minutes": record.interval_minutes,
        "adjustment": record.adjustment.value,
        "open": record.open,
        "high": record.high,
        "low": record.low,
        "close": record.close,
        "volume": record.volume,
        "amount": record.amount,
        "source_provider": record.source_provider,
        "endpoint": record.endpoint,
        "source_method": record.source_method,
        "quality_status": record.quality_status.value,
        "field_level": record.field_level,
        "capability_priority": record.capability_priority,
        "capability_version": record.capability_version,
        "raw_object_path": record.raw_object_path,
        "fetch_time": record.fetch_time,
        "volume_semantics": record.volume_semantics,
        "freshness_class": record.freshness_class,
        "source_delay_seconds": record.source_delay_seconds,
        "as_of": record.as_of,
        "normalizer_version": record.normalizer_version,
        "resolution_policy_version": record.resolution_policy_version,
        "flags": list(record.flags),
    }


def _mapping_to_bar(item: dict[str, object]) -> BarRecord:
    return BarRecord(
        dataset=Dataset(str(item["dataset"])),
        instrument_id=str(item["instrument_id"]),
        source_symbol=None if item["source_symbol"] is None else str(item["source_symbol"]),
        trade_date=item["trade_date"] if isinstance(item["trade_date"], date) else date.fromisoformat(str(item["trade_date"])),
        bar_time=item["bar_time"] if isinstance(item["bar_time"], datetime) else None,
        interval_minutes=None if item["interval_minutes"] is None else int(item["interval_minutes"]),
        adjustment=Adjustment(str(item["adjustment"])),
        open=Decimal(item["open"]),
        high=Decimal(item["high"]),
        low=Decimal(item["low"]),
        close=Decimal(item["close"]),
        volume=None if item["volume"] is None else Decimal(item["volume"]),
        amount=None if item["amount"] is None else Decimal(item["amount"]),
        source_provider=str(item["source_provider"]),
        endpoint=str(item["endpoint"]),
        source_method=str(item["source_method"]),
        quality_status=QualityStatus(str(item["quality_status"])),
        field_level=str(item["field_level"]),
        capability_priority=int(item["capability_priority"]),
        capability_version=str(item["capability_version"]),
        raw_object_path=str(item["raw_object_path"]),
        fetch_time=item["fetch_time"] if isinstance(item["fetch_time"], datetime) else datetime.fromisoformat(str(item["fetch_time"])),
        volume_semantics=None if item["volume_semantics"] is None else str(item["volume_semantics"]),
        freshness_class=None if item["freshness_class"] is None else str(item["freshness_class"]),
        source_delay_seconds=None if item["source_delay_seconds"] is None else int(item["source_delay_seconds"]),
        as_of=item["as_of"] if isinstance(item["as_of"], datetime) else None,
        normalizer_version=str(item["normalizer_version"]),
        resolution_policy_version=str(item["resolution_policy_version"]),
        flags=tuple(item["flags"] or ()),
    )


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as stream:
        stream.flush()
        os.fsync(stream.fileno())


def _call_hook(hook: Callable[[str], None] | None, stage: str) -> None:
    if hook is not None:
        hook(stage)
