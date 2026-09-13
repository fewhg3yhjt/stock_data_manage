from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class RawObjectRef:
    path: Path
    content_hash: str
    size_bytes: int


class RawObjectStore:
    """Immutable, content-verifiable storage for provider responses."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write_json(
        self,
        payload: Any,
        *,
        dataset: str,
        provider: str,
        endpoint: str,
        fetched_at: datetime,
        attempt_id: str,
    ) -> RawObjectRef:
        content = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        relative = Path(
            fetched_at.date().isoformat(), dataset, provider, endpoint, f"{attempt_id}.json"
        )
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        expected_hash = hashlib.sha256(content).hexdigest()
        if target.exists():
            existing_hash = hashlib.sha256(target.read_bytes()).hexdigest()
            if existing_hash != expected_hash:
                raise FileExistsError(f"immutable raw object already exists with different content: {target}")
            return RawObjectRef(target, existing_hash, target.stat().st_size)

        temporary = target.with_name(f"{target.name}.{os.getpid()}.tmp")
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        return RawObjectRef(target, expected_hash, len(content))

    @staticmethod
    def verify(reference: RawObjectRef) -> bool:
        if not reference.path.is_file() or reference.path.stat().st_size != reference.size_bytes:
            return False
        return hashlib.sha256(reference.path.read_bytes()).hexdigest() == reference.content_hash

