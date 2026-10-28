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

from ..domain import Adjustment, BarRecord, DailyRecord, QuoteGroup, QuoteStatus, AssetType, Dataset, ItemStatus, QualityStatus
from ..quality.resolution import Conflict, resolve_records
from ..quality.validation import validate_bar
from .integrity import Manifest, row_hash


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


DAILY_SCHEMA_VERSION = "daily-aggregate-v1"
QUOTE_SCHEMA = pa.struct([
    ("status", pa.string()), *[(name, pa.decimal128(38, 8)) for name in ("open", "high", "low", "close", "volume", "amount")],
    *[(name, pa.string()) for name in ("source_provider", "endpoint", "raw_object_path")],
    ("fetch_time", pa.timestamp("us", tz="Asia/Shanghai")),
    *[(name, pa.string()) for name in ("normalizer_version", "capability_version", "reason", "evidence_ref")],
    ("flags", pa.list_(pa.string())),
])
DAILY_SCHEMA = pa.schema([
    ("instrument_id", pa.string()), ("trade_date", pa.date32()), ("asset_type", pa.string()),
    ("value_kind", pa.string()), ("quote_unit", pa.string()),
    ("quotes", pa.struct([(a.value, QUOTE_SCHEMA) for a in Adjustment])),
])


def daily_records_to_table(records):
    from dataclasses import asdict
    rows = []
    for record in records:
        if not isinstance(record, DailyRecord) or not record.complete:
            raise ValueError("only complete aggregate daily records can be stored")
        rows.append({"instrument_id": record.instrument_id, "trade_date": record.trade_date,
            "asset_type": record.asset_type.value, "value_kind": record.value_kind, "quote_unit": record.quote_unit,
            "quotes": {name: {**asdict(q), "status": q.status.value} for name, q in record.quotes.items()}})
    return pa.Table.from_pylist(rows, schema=DAILY_SCHEMA)


def mapping_to_daily(row):
    quotes = {name: QuoteGroup(**{**value, "status": QuoteStatus(value["status"]),
                                 "flags": tuple(value.get("flags", ()))}) for name, value in row["quotes"].items()}
    record = DailyRecord(row["instrument_id"], row["trade_date"], AssetType(row["asset_type"]), **quotes)
    if row["value_kind"] != record.value_kind or row["quote_unit"] != record.quote_unit or not record.complete:
        raise InvalidPartitionError("daily aggregate type, unit or completeness differs")
    return record


def _write_partition(table, partition, run_id, manifest_fields, failure_hook=None):
    """One atomic file-pair writer shared by bars and aggregate daily results."""
    temp_data = partition / f"data.{run_id}.tmp.parquet"
    temp_manifest = partition / f"manifest.{run_id}.tmp.json"
    pq.write_table(table, temp_data, compression="zstd")
    _fsync_file(temp_data)
    _call_hook(failure_hook, "after_temp_data")
    manifest = Manifest.from_file(temp_data, **manifest_fields)
    manifest.write_atomic(temp_manifest)
    if not manifest.verify(temp_data):
        raise OSError("temporary canonical file failed manifest verification")
    _call_hook(failure_hook, "after_temp_manifest")
    os.replace(temp_data, partition / "data.parquet")
    _call_hook(failure_hook, "after_data_replace")
    os.replace(temp_manifest, partition / "manifest.json")
    _call_hook(failure_hook, "after_manifest_replace")
    return manifest


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


def write_normalized_rows(path: str | Path, rows: Iterable[Mapping], fields: Mapping) -> dict:
    """Persist source candidates using the declared field contract, without publishing."""
    import hashlib
    from uuid import uuid4

    path = Path(path)
    records = list(rows)
    if any(set(row) - set(fields) for row in records):
        raise ValueError("normalized rows contain undeclared fields")
    arrow_fields = []
    for name, definition in fields.items():
        values = [row.get(name) for row in records if row.get(name) is not None]
        kind = definition["type"]
        if definition.get("required") and len(values) != len(records):
            raise ValueError(f"missing required normalized field: {name}")
        if kind == "decimal":
            if any(not isinstance(value, Decimal) or not value.is_finite() for value in values):
                raise ValueError(f"decimal field requires finite Decimal values: {name}")
            scale = max([0] + [-value.as_tuple().exponent for value in values])
            integer_digits = max([0] + [max(0, value.adjusted() + 1) for value in values])
            precision = max(1, integer_digits + scale)
            if precision > 76:
                raise ValueError(f"decimal precision exceeds Parquet capacity: {name}")
            arrow_type = pa.decimal128(precision, scale) if precision <= 38 else pa.decimal256(precision, scale)
        elif kind in {"string", "enum"}:
            if any(not isinstance(value, str) for value in values):
                raise ValueError(f"string field requires string values: {name}")
            if kind == "enum" and any(value not in definition["values"] for value in values):
                raise ValueError(f"undeclared enum value: {name}")
            arrow_type = pa.string()
        elif kind == "date":
            if any(not isinstance(value, date) or isinstance(value, datetime) for value in values):
                raise ValueError(f"date field requires date values: {name}")
            arrow_type = pa.date32()
        elif kind == "datetime":
            if any(not isinstance(value, datetime) for value in values):
                raise ValueError(f"datetime field requires datetime values: {name}")
            aware = [value.utcoffset() is not None for value in values]
            if aware and any(aware) != all(aware):
                raise ValueError(f"mixed naive and aware timestamps: {name}")
            arrow_type = pa.timestamp("us", tz="UTC" if any(aware) else None)
        elif kind == "integer":
            if any(type(value) is not int for value in values):
                raise ValueError(f"integer field requires integer values: {name}")
            arrow_type = pa.int64()
        else:
            raise ValueError(f"unsupported normalized field type: {kind}")
        arrow_fields.append(pa.field(name, arrow_type, nullable=not definition.get("required", False)))
    table = pa.Table.from_pylist(records, schema=pa.schema(arrow_fields))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".tmp-{uuid4().hex[:12]}.parquet")
    try:
        pq.write_table(table, temporary)
        with temporary.open("r+b") as stream:
            os.fsync(stream.fileno())
        # Each candidate path belongs to one run. Publish the completed file
        # with a same-directory atomic rename; no hard-link capability needed.
        if path.exists():
            raise FileExistsError(path)
        os.rename(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "row_count": len(records),
            "byte_length": path.stat().st_size, "arrow_schema": str(table.schema)}


class PartitionLock(AbstractContextManager["PartitionLock"]):
    def __init__(self, path: Path, *, recover_stale: bool = False) -> None:
        self.path = path
        self._acquired = False
        self.recover_stale = recover_stale

    def __enter__(self) -> "PartitionLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.recover_stale and self.path.exists():
            self._clear_dead_owner()
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

    def _clear_dead_owner(self) -> None:
        """Reclaim only a well-formed lock whose owning process is proven dead."""
        import re
        original = self.path.read_bytes()
        matched = re.fullmatch(rb"pid=([1-9][0-9]*)\r?\n", original)
        if not matched:
            return
        pid = int(matched[1])
        if os.name == "nt":
            import ctypes
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.OpenProcess.restype = ctypes.c_void_p
            kernel.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
            kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
            handle = kernel.OpenProcess(0x00101000, False, pid)
            if handle:
                exited = kernel.WaitForSingleObject(handle, 0) == 0
                kernel.CloseHandle(handle)
                if not exited:
                    return
            elif ctypes.get_last_error() != 87:  # ERROR_INVALID_PARAMETER: no such process
                return
        else:
            try:
                os.kill(pid, 0)
                return
            except ProcessLookupError:
                pass
            except PermissionError:
                return
        if self.path.read_bytes() == original:
            self.path.unlink()

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._acquired:
            self.path.unlink(missing_ok=True)
            self._acquired = False


class CanonicalPartitionStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def partition_directory(self, dataset: Dataset, asset_type: str, partition_key: str,
                            adjustment: Adjustment = Adjustment.NONE) -> Path:
        from datetime import date
        import re
        if date.fromisoformat(partition_key).isoformat() != partition_key or not re.fullmatch(r"[A-Za-z0-9_-]+", asset_type):
            raise ValueError("invalid canonical date or asset type")
        partition = self.root / partition_key / dataset.value / asset_type
        if dataset is Dataset.DAILY_BAR:
            adjustment = Adjustment(adjustment)
            partition = partition / f"adjustment={adjustment.value}"
        return partition

    def daily_directory(self, asset_type, partition_key):
        # Reuse date/asset validation; aggregate identity has no adjustment dimension.
        return self.partition_directory(Dataset.DAILY_BAR, asset_type, partition_key).parent

    def read_daily(self, asset_type, partition_key, *, allow_pending=False):
        partition = self.daily_directory(asset_type, partition_key)
        if not allow_pending and (partition / "task-commit.json").exists():
            raise InvalidPartitionError("daily publication is pending task finalization")
        data, descriptor = partition / "data.parquet", partition / "manifest.json"
        if not data.exists() and not descriptor.exists():
            return []
        if not data.exists() or not descriptor.exists():
            raise InvalidPartitionError("daily aggregate partition is incomplete")
        manifest = Manifest.load(descriptor)
        if (manifest.schema_version != DAILY_SCHEMA_VERSION or manifest.adjustment != "aggregate"
                or manifest.dataset != "daily_bar" or manifest.asset_type != asset_type or manifest.partition_key != partition_key
                or not manifest.verify(data)):
            raise InvalidPartitionError("daily aggregate manifest is incompatible or corrupt")
        table = pq.read_table(data)
        if not table.schema.equals(DAILY_SCHEMA) or len(table) != manifest.row_count:
            raise InvalidPartitionError("daily aggregate schema or row count differs")
        records = [mapping_to_daily(row) for row in table.to_pylist()]
        if len({r.key for r in records}) != len(records) or any(r.trade_date.isoformat() != partition_key or r.asset_type.value != asset_type for r in records):
            raise InvalidPartitionError("daily aggregate keys escaped partition")
        return records

    def publish_daily(self, *, asset_type, partition_key, new_records, expected_count, run_id,
                      item_statuses=None, replace_instrument_ids=None, failure_hook=None):
        incoming = list(new_records)
        daily_records_to_table(incoming)
        if len({r.key for r in incoming}) != len(incoming):
            raise ValueError("duplicate aggregate daily keys")
        if any(r.trade_date.isoformat() != partition_key or r.asset_type.value != asset_type for r in incoming):
            raise ValueError("daily aggregate outside target partition")
        partition = self.daily_directory(asset_type, partition_key)
        partition.mkdir(parents=True, exist_ok=True)
        marker = partition / "task-commit.json"
        if marker.exists():
            import json
            if json.loads(marker.read_text(encoding="utf-8"))["task_id"] != run_id:
                raise InvalidPartitionError("another daily task publication is pending")
        with PartitionLock(partition / ".publish.lock", recover_stale=marker.exists()):
            existing = self.read_daily(asset_type, partition_key, allow_pending=True)
            merged = {r.key: r for r in existing if replace_instrument_ids is None or r.instrument_id not in replace_instrument_ids}
            merged.update({r.key: r for r in incoming})
            records = sorted(merged.values(), key=lambda r: r.key)
            statuses = {k: v.value if isinstance(v, ItemStatus) else v for k, v in (item_statuses or {}).items()}
            providers = {r.instrument_id: "|".join(dict.fromkeys(q.source_provider for q in r.quotes.values() if q.status is QuoteStatus.AVAILABLE)) for r in records}
            descriptor = partition / "manifest.json"
            old = Manifest.load(descriptor) if descriptor.exists() else None
            if old and row_hash(records) == row_hash(existing) and old.expected_count == expected_count and old.item_statuses == statuses:
                return PublishResult(old, descriptor, tuple(existing), (), (), True)
            raw_refs = tuple(dict.fromkeys(q.raw_object_path for r in records for q in r.quotes.values() if q.raw_object_path))
            manifest = _write_partition(daily_records_to_table(records), partition, run_id, dict(
                dataset="daily_bar", asset_type=asset_type, partition_key=partition_key, schema_version=DAILY_SCHEMA_VERSION,
                adjustment="aggregate", row_count=len(records), expected_count=expected_count,
                first_key=str(records[0].key) if records else None, last_key=str(records[-1].key) if records else None,
                raw_refs=raw_refs, item_statuses=statuses, source_providers=providers), failure_hook)
            return PublishResult(manifest, descriptor, tuple(records), (), (), False)

    def read(self, dataset: Dataset, asset_type: str, partition_key: str, *,
             adjustment: Adjustment | None = None, allow_pending: bool = False) -> list[BarRecord]:
        if dataset is Dataset.DAILY_BAR:
            if adjustment is not None:
                raise ValueError("daily aggregates have no adjustment partition; project a quote group at query time")
            return self.read_daily(asset_type, partition_key, allow_pending=allow_pending)
        selected_adjustment = Adjustment(adjustment) if adjustment is not None else Adjustment.NONE
        partition = self.partition_directory(dataset, asset_type, partition_key, selected_adjustment)
        if not allow_pending and (partition / "task-commit.json").exists():
            raise InvalidPartitionError("task publication is pending raw finalization; recover the task first")
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
        if dataset is Dataset.DAILY_BAR and manifest.adjustment != selected_adjustment.value:
            raise InvalidPartitionError(f"daily manifest adjustment differs from its partition: {partition}")
        if not manifest.verify(path):
            raise InvalidPartitionError(f"canonical content does not match manifest: {partition}")
        records = [_mapping_to_bar(item) for item in pq.read_table(path).to_pylist()]
        if dataset is Dataset.DAILY_BAR and any(record.adjustment is not selected_adjustment for record in records):
            raise InvalidPartitionError(f"daily adjustment differs from its partition: {partition}")
        return records

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
        replace_instrument_ids: frozenset[str] | None = None,
        replace_adjustment: Adjustment | None = None,
        adjustment: Adjustment | None = None,
    ) -> PublishResult:
        if dataset is Dataset.DAILY_BAR:
            if adjustment is not None or replace_adjustment is not None:
                raise ValueError("daily publication requires a complete aggregate, not an adjustment partition")
            return self.publish_daily(asset_type=asset_type, partition_key=partition_key,
                new_records=new_records, expected_count=expected_count, run_id=run_id,
                item_statuses=item_statuses, replace_instrument_ids=replace_instrument_ids, failure_hook=failure_hook)
        incoming = list(new_records)
        adjustment = Adjustment.NONE
        partition = self.partition_directory(dataset, asset_type, partition_key, adjustment)
        partition.mkdir(parents=True, exist_ok=True)
        marker = partition / "task-commit.json"
        if marker.exists():
            import json
            if json.loads(marker.read_text(encoding="utf-8"))["task_id"] != run_id:
                raise InvalidPartitionError("another task publication is pending")
        with PartitionLock(partition / ".publish.lock", recover_stale=marker.exists()):
            existing = self.read(dataset, asset_type, partition_key, adjustment=adjustment,
                                 allow_pending=True)
            if replace_instrument_ids is not None:
                if replace_adjustment is None:
                    raise ValueError("scoped replacement requires an adjustment")
                existing = [record for record in existing if not (
                    record.instrument_id in replace_instrument_ids and record.adjustment is replace_adjustment)]
            merged, conflicts, quarantined = _merge(existing, incoming)
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

            raw_refs = tuple(dict.fromkeys(path for record in merged for path in record.raw_object_path.split("|") if path))
            manifest = _write_partition(_records_to_table(merged), partition, run_id, dict(
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
                adjustment=adjustment.value,
            ), failure_hook)
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
