from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from stock_data_manage.domain import AssetType, DailyRecord, Exchange, QuoteGroup, QuoteStatus
from stock_data_manage.service.instruments import (
    SecurityMasterStore,
    SecurityRecord,
    read_published_security_master,
)
from stock_data_manage.storage.integrity import Manifest
from stock_data_manage.storage.parquet import (
    CanonicalPartitionStore,
    InvalidPartitionError,
    PartitionBusyError,
    PartitionLock,
    PartitionLockedError,
)
from stock_data_manage.storage.raw import RawObjectStore, response_body_path


DAY = "2026-11-06"


def test_partition_lock_timeout_is_bounded_and_default_is_nonblocking(tmp_path):
    path = tmp_path / "partition.lock"
    with PartitionLock(path):
        started = time.monotonic()
        with pytest.raises(PartitionLockedError):
            with PartitionLock(path):
                pass
        with pytest.raises(PartitionLockedError):
            with PartitionLock(path, timeout_seconds=0.08):
                pass
        elapsed = time.monotonic() - started
    assert 0.06 <= elapsed < 0.5


def test_dead_process_access_lock_is_reclaimed_without_waiting_for_timeout(tmp_path):
    path = tmp_path / "abandoned.lock"
    script = (
        "import os,sys; from pathlib import Path; "
        "from stock_data_manage.storage.parquet import PartitionLock; "
        "lock=PartitionLock(Path(sys.argv[1])); lock.__enter__(); "
        "sys.stdout.write('locked\\n'); sys.stdout.flush(); os._exit(0)"
    )
    child = subprocess.run([sys.executable, "-c", script, str(path)], cwd=Path(__file__).parents[1],
                           capture_output=True, text=True, timeout=5)
    assert child.returncode == 0 and "locked" in child.stdout
    assert path.exists()
    with PartitionLock(path, recover_stale=True, timeout_seconds=0.2):
        assert path.exists()


def test_security_master_read_only_store_does_not_create_or_change_schema(tmp_path):
    missing = tmp_path / "missing.duckdb"
    with pytest.raises(FileNotFoundError):
        SecurityMasterStore(str(missing), read_only=True)
    assert not missing.exists()
    database = tmp_path / "security_master.duckdb"
    with SecurityMasterStore(str(database)):
        pass
    before = database.read_bytes()
    with SecurityMasterStore(str(database), read_only=True) as store:
        assert store.all() == ()
    assert database.read_bytes() == before
    with pytest.raises(PermissionError):
        with SecurityMasterStore(str(database), read_only=True) as store:
            store.upsert([])


def test_published_security_master_checks_manifest_row_count_and_lock(tmp_path):
    directory = tmp_path / "canonical" / "security_master" / "current"
    directory.mkdir(parents=True)
    database = directory / "security_master.duckdb"
    record = SecurityRecord("XSHG:600000", "600000", Exchange.XSHG, AssetType.STOCK, name="fixture")
    with SecurityMasterStore(str(database)) as store:
        store.publish([record])
    manifest = Manifest.from_file(database, dataset="security_master", asset_type="security", row_count=1,
                                  first_key=record.instrument_id, last_key=record.instrument_id)
    manifest.write_atomic(directory / "manifest.json")
    loaded, records = read_published_security_master(directory)
    assert loaded.row_count == 1 and records == (record,)
    from dataclasses import asdict
    (directory / "manifest.json").write_text(json.dumps({**asdict(manifest), "row_count": 2}), encoding="utf-8")
    with pytest.raises(InvalidPartitionError):
        read_published_security_master(directory)
