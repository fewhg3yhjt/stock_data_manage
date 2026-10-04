from pathlib import Path

import pytest

from stock_data_manage.storage.parquet import (
    CanonicalPartitionStore,
    InvalidPartitionError,
    PartitionLock,
    PartitionLockedError,
)
from stock_data_manage.domain import Dataset, ItemStatus
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.worker.recovery import RecoveryScanner


def test_canonical_publish_is_idempotent_and_locked(tmp_path, final_minute_bar) -> None:
    store = CanonicalPartitionStore(tmp_path / "canonical")
    kwargs = dict(
        dataset=Dataset.MINUTE_BAR_1M,
        asset_type="stock",
        partition_key="2026-09-11",
        expected_count=1,
        item_statuses={final_minute_bar.instrument_id: ItemStatus.SUCCESS},
        source_providers={final_minute_bar.instrument_id: "tdx"},
    )
    first = store.publish(new_records=[final_minute_bar], run_id="run-1", **kwargs)
    second = store.publish(new_records=[], run_id="run-2", **kwargs)
    assert not first.no_op
    assert second.no_op
    assert first.manifest.content_hash == second.manifest.content_hash
    assert len(store.read(Dataset.MINUTE_BAR_1M, "stock", "2026-09-11")) == 1

    lock_path = store.partition_directory(
        Dataset.MINUTE_BAR_1M, "stock", "2026-09-11"
    ) / ".publish.lock"
    with PartitionLock(lock_path):
        with pytest.raises(PartitionLockedError):
            with PartitionLock(lock_path):
                pass


def test_recovery_finishes_replace_and_repairs_metadata(tmp_path, final_minute_bar) -> None:
    canonical_root = tmp_path / "canonical"
    store = CanonicalPartitionStore(canonical_root)

    def fail_after_data_replace(stage: str) -> None:
        if stage == "after_data_replace":
            raise RuntimeError("simulated power loss")

    with pytest.raises(RuntimeError, match="power loss"):
        store.publish(
            dataset=Dataset.MINUTE_BAR_1M,
            asset_type="stock",
            partition_key="2026-09-11",
            new_records=[final_minute_bar],
            expected_count=1,
            run_id="interrupted",
            item_statuses={final_minute_bar.instrument_id: ItemStatus.SUCCESS},
            source_providers={final_minute_bar.instrument_id: "tdx"},
            failure_hook=fail_after_data_replace,
        )

    with pytest.raises(InvalidPartitionError, match="incomplete"):
        store.read(Dataset.MINUTE_BAR_1M, "stock", "2026-09-11")

    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        report = RecoveryScanner(canonical_root, metadata).recover()
        assert report.promoted_temporary_partitions == 1
        assert report.repaired_metadata_partitions == 1
        assert report.invalid_final_partitions == ()
        partition = metadata.partition("minute_bar_1m", "stock", "2026-09-11")
        assert partition is not None
        assert partition["status"] == "complete_clean"

    partition_dir = store.partition_directory(
        Dataset.MINUTE_BAR_1M, "stock", "2026-09-11"
    )
    assert (partition_dir / "data.parquet").exists()
    assert (partition_dir / "manifest.json").exists()
    assert not list(partition_dir.glob("*.tmp.*"))


def test_recovery_quarantines_unverifiable_orphan_temp_file(tmp_path) -> None:
    canonical_root = tmp_path / "canonical"
    partition = canonical_root / "daily_bar" / "asset_type=stock" / "trade_date=2026-09-11"
    partition.mkdir(parents=True)
    orphan = partition / "data.crashed.tmp.parquet"
    orphan.write_bytes(b"partial")
    with MetadataStore() as metadata:
        report = RecoveryScanner(canonical_root, metadata).recover()
    assert report.quarantined_files == 1
    assert not orphan.exists()
    assert list((canonical_root / "_quarantine").rglob("data.crashed.tmp.parquet"))



def test_normalized_parquet_roundtrips_precision_time_and_nulls(tmp_path):
    from datetime import date, datetime, timezone
    from decimal import Decimal
    import pyarrow.parquet as pq
    from stock_data_manage.storage.parquet import write_normalized_rows
    fields = {"amount":{"type":"decimal", "required":True}, "day":{"type":"date"},
              "time":{"type":"datetime"}, "count":{"type":"integer"},
              "kind":{"type":"enum", "values":["a","b"]}, "missing":{"type":"decimal"}}
    row = {"amount":Decimal("123456789012345678901234567890123456789.000000000000000001"),
           "day":date(2026,9,30), "time":datetime(2026,9,30,15,10,tzinfo=timezone.utc),
           "count":0, "kind":"a", "missing":None}
    path = tmp_path / "normalized.parquet"
    write_normalized_rows(path, [row], fields)
    assert pq.ParquetFile(path).read().to_pylist() == [row]
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        write_normalized_rows(path, [row], fields)
    assert path.read_bytes() == original and not list(tmp_path.glob("*.tmp"))
    empty = tmp_path / "empty.parquet"
    write_normalized_rows(empty, [], fields)
    assert pq.ParquetFile(empty).read().num_rows == 0
    assert len(pq.ParquetFile(empty).schema_arrow) == len(fields)


@pytest.mark.parametrize("definition,value", [({"type":"decimal"},"1.1"),
    ({"type":"decimal"}, __import__("decimal").Decimal("NaN")),
    ({"type":"decimal"}, __import__("decimal").Decimal("1e-77")),
    ({"type":"integer"}, True), ({"type":"enum", "values":["a"]}, "b"),
    ({"type":"string", "required":True}, None)])
def test_normalized_parquet_refuses_invalid_values_before_writing(tmp_path, definition, value):
    from stock_data_manage.storage.parquet import write_normalized_rows
    path = tmp_path / "normalized.parquet"
    with pytest.raises(ValueError):
        write_normalized_rows(path, [{"value":value}], {"value":definition})
    assert not path.exists()


@pytest.mark.parametrize("mutation", [None, "candidate", "metadata", "canonical", "output", "destination", "escape"])
def test_archive_requires_verified_publication_without_overwriting(tmp_path, final_minute_bar, mutation):
    import json
    import os
    from datetime import datetime, timezone, timedelta
    from stock_data_manage.domain import AttemptStatus
    from stock_data_manage.storage.integrity import file_hash
    from stock_data_manage.worker.attempts import CollectionAttempt
    from stock_data_manage.worker.recovery import archive_published_task
    roots = {name:tmp_path/name for name in ("task_workspace","task_archive","canonical","raw")}
    task = roots["task_workspace"] / "minute_bar_1m/scope-abc/task-1"
    task.mkdir(parents=True)
    raw_manifest = roots["raw"] / "fixture/minute/2026-09-30/batch/manifest.ndjson"
    raw_manifest.parent.mkdir(parents=True)
    raw_manifest.write_bytes(b"")
    canonical = CanonicalPartitionStore(roots["canonical"]).publish(dataset=Dataset.MINUTE_BAR_1M,
        asset_type="stock", partition_key="2026-09-11", new_records=[final_minute_bar], expected_count=1, run_id="offline")
    manifest = {"task_id":task.name, "dataset":"minute_bar_1m", "status":"published",
        "raw_manifest":{"path":Path(os.path.relpath(raw_manifest,task)).as_posix(),"sha256":file_hash(raw_manifest)},
        "canonical_refs":[{"manifest_path":canonical.manifest_path.relative_to(roots["canonical"]).as_posix(),
                           "sha256":file_hash(canonical.manifest_path)}]}
    for name in ("output","report","quality_report"):
        artifact=task/(name+".json");artifact.write_text("{}")
        manifest[name]={"path":artifact.name,"sha256":file_hash(artifact)}
    metadata = MetadataStore(tmp_path / "metadata/metadata.duckdb")
    try:
        now = datetime.now(timezone.utc)
        attempt=CollectionAttempt(task.name).lease(owner="fixture", acquired_at=now,
            expires_at=now+timedelta(hours=1)).transition(AttemptStatus.FETCHING)
        from dataclasses import replace
        attempt=replace(attempt,raw_object_path=str(raw_manifest.resolve()),raw_content_hash=file_hash(raw_manifest))
        for status in (AttemptStatus.RAW_COMMITTED,AttemptStatus.NORMALIZED,AttemptStatus.VALIDATED,AttemptStatus.PUBLISHED):
            attempt=attempt.transition(status)
        if mutation=="metadata": attempt=replace(attempt,status=AttemptStatus.VALIDATED)
        metadata.save_attempt(attempt,updated_at=now)
        destination=roots["task_archive"]/task.relative_to(roots["task_workspace"])
        if mutation=="candidate":manifest["status"]="candidate_complete"
        if mutation=="canonical":(canonical.manifest_path.parent/"data.parquet").write_bytes(b"tampered")
        if mutation=="output":(task/"output.json").write_text("tampered")
        if mutation=="destination":destination.mkdir(parents=True);(destination/"user.txt").write_text("keep")
        (task/"manifest.json").write_text(json.dumps(manifest),encoding="utf-8")
        args=dict(workspace_root=roots["task_workspace"], archive_root=roots["task_archive"],
                  canonical_root=roots["canonical"],raw_root=roots["raw"],metadata=metadata)
        if mutation:
            with pytest.raises(ValueError):archive_published_task(tmp_path if mutation=="escape" else task,**args)
            assert task.exists()
            if mutation=="destination":assert (destination/"user.txt").read_text()=="keep"
        else:
            assert archive_published_task(task,**args)==destination
            assert not task.exists() and (destination/"manifest.json").exists()
            assert (destination/manifest["raw_manifest"]["path"]).resolve()==raw_manifest.resolve()
            assert raw_manifest.exists() and canonical.manifest_path.exists()
    finally:
        metadata.close()
