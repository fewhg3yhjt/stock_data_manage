from pathlib import Path

import pytest

from stock_data_manage.canonical_storage import (
    CanonicalPartitionStore,
    InvalidPartitionError,
    PartitionLock,
    PartitionLockedError,
)
from stock_data_manage.domain import Dataset, ItemStatus
from stock_data_manage.metadata import MetadataStore
from stock_data_manage.recovery import RecoveryScanner


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
