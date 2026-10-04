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


def archive_published_task(task_directory: str | Path, *, workspace_root: str | Path,
                           archive_root: str | Path, canonical_root: str | Path,
                           raw_root: str | Path, metadata: MetadataStore) -> Path:
    """Archive an existing published task only after checking durable publication evidence."""
    import json
    from ..domain import AttemptStatus
    from ..storage.integrity import file_hash

    task = Path(task_directory).resolve()
    workspace, archive = Path(workspace_root).resolve(), Path(archive_root).resolve()
    canonical, raw = Path(canonical_root).resolve(), Path(raw_root).resolve()
    roots = (workspace, archive, canonical, raw)
    if any(first.is_relative_to(second) or second.is_relative_to(first)
           for index, first in enumerate(roots) for second in roots[index + 1:]):
        raise ValueError("archive storage roots must be disjoint")
    if task == workspace or not task.is_relative_to(workspace) or not task.is_dir():
        raise ValueError("task must be an existing directory inside task_workspace")
    relative = task.relative_to(workspace)
    if len(relative.parts) != 3:
        raise ValueError("task path must be dataset/scope/task-id")
    destination = (archive / relative).resolve()
    if not destination.is_relative_to(archive) or destination.exists():
        raise ValueError("archive destination must be new and inside task_archive")
    manifest = json.loads((task / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("task_id") != task.name or manifest.get("dataset") != relative.parts[0]:
        raise ValueError("task identity does not match directory")
    attempt = metadata.load_attempt(manifest["task_id"])
    if manifest.get("status") != "published" or not attempt or attempt.status != AttemptStatus.PUBLISHED:
        raise ValueError("only published tasks can be archived")
    raw_ref = manifest.get("raw_manifest") or {}
    raw_manifest = (task / raw_ref.get("path", "")).resolve()
    if not raw_manifest.is_relative_to(raw) or not raw_manifest.is_file() or file_hash(raw_manifest) != raw_ref.get("sha256"):
        raise ValueError("raw response reference is invalid")
    if Path(attempt.raw_object_path or "").resolve() != raw_manifest or attempt.raw_content_hash != raw_ref["sha256"]:
        raise ValueError("metadata does not match task raw evidence")
    if (destination / raw_ref["path"]).resolve() != raw_manifest:
        raise ValueError("archive layout would break the relative raw reference")
    for event in (json.loads(line) for line in raw_manifest.read_text(encoding="utf-8").splitlines()):
        if event.get("body_sha256"):
            from ..storage.raw import RawObjectStore
            RawObjectStore.read_response(raw_manifest, event)
    for name in ("output", "report", "quality_report"):
        descriptor = manifest.get(name) or {}
        path = (task / descriptor.get("path", "")).resolve()
        if not path.is_relative_to(task) or not path.is_file() or file_hash(path) != descriptor.get("sha256"):
            raise ValueError(f"task {name} is missing or corrupt")
    refs = manifest.get("canonical_refs") or []
    if not refs:
        raise ValueError("published task needs canonical publication references")
    for ref in refs:
        manifest_path = (canonical / ref["manifest_path"]).resolve()
        if not manifest_path.is_relative_to(canonical) or not manifest_path.is_file() or file_hash(manifest_path) != ref["sha256"]:
            raise ValueError("canonical publication reference is invalid")
        published = Manifest.load(manifest_path)
        if published.dataset != manifest["dataset"] or not published.verify(manifest_path.parent / "data.parquet"):
            raise ValueError("canonical dataset does not match task publication")
    # rename keeps the move atomic; cross-device archives are refused, never copied/deleted.
    destination.parent.mkdir(parents=True, exist_ok=True)
    from ..storage.parquet import PartitionLock
    with PartitionLock(task.with_name(task.name + ".archive.lock")):
        if destination.exists():
            raise ValueError("archive destination already exists")
        os.rename(task, destination)
    return destination
