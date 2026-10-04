from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from ..storage.integrity import Manifest
from ..storage.metadata import MetadataStore


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    promoted_temporary_partitions: int
    repaired_metadata_partitions: int
    quarantined_files: int
    invalid_final_partitions: tuple[str, ...]


class RecoveryScanner:
    def __init__(self, canonical_root: str | Path, metadata: MetadataStore) -> None:
        self.canonical_root = Path(canonical_root)
        self.metadata = metadata
        self.quarantine_root = self.canonical_root / "_quarantine"

    def recover(self) -> RecoveryReport:
        promoted = 0
        quarantined = 0
        invalid_final: list[str] = []

        for temporary_manifest in self._files("manifest.*.tmp.json"):
            partition = temporary_manifest.parent
            run_id = temporary_manifest.name[len("manifest.") : -len(".tmp.json")]
            temporary_data = partition / f"data.{run_id}.tmp.parquet"
            final_data = partition / "data.parquet"
            final_manifest = partition / "manifest.json"
            try:
                manifest = Manifest.load(temporary_manifest)
            except Exception:
                quarantined += self._quarantine(temporary_manifest, temporary_data)
                continue

            if temporary_data.exists() and manifest.verify(temporary_data):
                os.replace(temporary_data, final_data)
                os.replace(temporary_manifest, final_manifest)
                promoted += 1
            elif final_data.exists() and manifest.verify(final_data):
                os.replace(temporary_manifest, final_manifest)
                promoted += 1
            else:
                quarantined += self._quarantine(temporary_manifest, temporary_data)

        for temporary_data in self._files("data.*.tmp.parquet"):
            quarantined += self._quarantine(temporary_data)

        repaired = 0
        for manifest_path in self._files("manifest.json"):
            data_path = manifest_path.parent / "data.parquet"
            try:
                manifest = Manifest.load(manifest_path)
                if not data_path.exists() or not manifest.verify(data_path):
                    raise ValueError("manifest does not match canonical data")
                self.metadata.record_partition_publish(
                    manifest=manifest,
                    manifest_path=manifest_path,
                )
                repaired += 1
            except Exception:
                invalid_final.append(str(manifest_path.parent))

        return RecoveryReport(promoted, repaired, quarantined, tuple(invalid_final))

    def _files(self, pattern: str) -> list[Path]:
        return sorted(
            path
            for path in self.canonical_root.rglob(pattern)
            if self.quarantine_root not in path.parents
        )

    def _quarantine(self, *paths: Path) -> int:
        moved = 0
        for path in paths:
            if not path.exists():
                continue
            relative = path.relative_to(self.canonical_root)
            destination = self.quarantine_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                destination = destination.with_name(f"{destination.name}.{os.getpid()}")
            shutil.move(str(path), str(destination))
            moved += 1
        return moved
