from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any


def row_hash(value: Any) -> str:
    payload = json.dumps(
        _jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class Manifest:
    dataset: str
    row_count: int
    first_key: str | None
    last_key: str | None
    content_hash: str
    size_bytes: int
    raw_refs: tuple[str, ...]
    asset_type: str = ""
    partition_key: str = ""
    expected_count: int = 0
    conflict_count: int = 0
    quarantined_count: int = 0
    schema_version: str = "v1"
    item_statuses: dict[str, str] = field(default_factory=dict)
    source_providers: dict[str, str | None] = field(default_factory=dict)

    @classmethod
    def from_file(
        cls,
        path: Path,
        *,
        dataset: str,
        row_count: int,
        first_key: str | None,
        last_key: str | None,
        raw_refs: tuple[str, ...] = (),
        asset_type: str = "",
        partition_key: str = "",
        expected_count: int | None = None,
        conflict_count: int = 0,
        quarantined_count: int = 0,
        schema_version: str = "v1",
        item_statuses: dict[str, str] | None = None,
        source_providers: dict[str, str | None] | None = None,
    ) -> "Manifest":
        if row_count < 0:
            raise ValueError("row_count cannot be negative")
        return cls(
            dataset=dataset,
            row_count=row_count,
            first_key=first_key,
            last_key=last_key,
            content_hash=file_hash(path),
            size_bytes=path.stat().st_size,
            raw_refs=raw_refs,
            asset_type=asset_type,
            partition_key=partition_key,
            expected_count=row_count if expected_count is None else expected_count,
            conflict_count=conflict_count,
            quarantined_count=quarantined_count,
            schema_version=schema_version,
            item_statuses=dict(item_statuses or {}),
            source_providers=dict(source_providers or {}),
        )

    def verify(self, path: Path) -> bool:
        return path.stat().st_size == self.size_bytes and file_hash(path) == self.content_hash

    def write_atomic(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        payload = json.dumps(_jsonable(self), ensure_ascii=False, sort_keys=True).encode("utf-8")
        with temporary.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        payload = json.loads(path.read_text("utf-8"))
        payload["raw_refs"] = tuple(payload.get("raw_refs", ()))
        return cls(**payload)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Enum):
        return value.value
    return value
