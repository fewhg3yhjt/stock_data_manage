from __future__ import annotations

from datetime import datetime, timezone
import json
import multiprocessing
from pathlib import Path

import pytest

from stock_data_manage.storage.metadata import (
    CollectionTaskScopeConflict,
    MetadataStore,
)
from stock_data_manage.storage.daily import initialize_schema


DAY = "2026-10-09"


def _scope(*securities: str, dates=(DAY,), dataset="daily_bar", frequency="1d") -> dict:
    return {"dataset": dataset, "frequency": frequency,
            "asset_types": ["stock"],
            "securities_by_date": {day: list(securities) for day in dates}}


def _task(task_id: str, scope: dict) -> dict:
    return {"task_id": task_id, "dataset": scope["dataset"], "definition_hash": f"definition-{task_id}",
            "status": "waiting", "definition": {"trade_date": next(iter(scope["securities_by_date"]))}}


def _submit_from_process(path: str, barrier, task_id: str, scope: dict, results) -> None:
    with MetadataStore(path) as metadata:
        snapshot = {"registry_hash": metadata.partition_registry_snapshot(scope)}
        barrier.wait(15)
        try:
            accepted = metadata.enqueue_collection_task(_task(task_id, scope), scope, snapshot=snapshot,
                daily_owner_refs=())
            results.put(("accepted", accepted["status"], accepted["queue_order"]))
        except CollectionTaskScopeConflict as exc:
            results.put(("conflict", exc.task_id, exc.conflicts))


def _initialize(path: Path) -> None:
    with MetadataStore(path) as metadata:
        initialize_schema(metadata.connection)


def test_overlapping_submission_from_two_processes_accepts_exactly_one(tmp_path: Path) -> None:
    path = tmp_path / "metadata.sqlite3"
    _initialize(path)
    context = multiprocessing.get_context("spawn")
    barrier, results = context.Barrier(2), context.Queue()
    scope = _scope("XSHG:600001", dates=(DAY, "2026-10-12"))
    processes = [context.Process(target=_submit_from_process, args=(str(path), barrier, task_id, scope, results))
                 for task_id in ("overlap-a", "overlap-b")]
    for process in processes:
        process.start()
    try:
        outcomes = [results.get(timeout=20), results.get(timeout=20)]
    finally:
        for process in processes:
            process.join(20)
            if process.is_alive():
                process.terminate()
                process.join()
    assert sum(result[0] == "accepted" for result in outcomes) == 1
    assert sum(result[0] == "conflict" for result in outcomes) == 1
    assert all(process.exitcode == 0 for process in processes)
    with MetadataStore(path, read_only=True) as metadata:
        assert len(metadata.collection_tasks(status="waiting")) == 1


def test_same_date_different_securities_are_both_waiting_and_ordered(tmp_path: Path) -> None:
    path = tmp_path / "metadata.sqlite3"
    _initialize(path)
    context = multiprocessing.get_context("spawn")
    barrier, results = context.Barrier(2), context.Queue()
    processes = [
        context.Process(target=_submit_from_process,
            args=(str(path), barrier, "shanghai", _scope("XSHG:600001"), results)),
        context.Process(target=_submit_from_process,
            args=(str(path), barrier, "shenzhen", _scope("XSHE:000001"), results)),
    ]
    for process in processes:
        process.start()
    try:
        outcomes = [results.get(timeout=20), results.get(timeout=20)]
    finally:
        for process in processes:
            process.join(20)
            if process.is_alive():
                process.terminate()
                process.join()
    assert all(result[0] == "accepted" and result[1] == "waiting" for result in outcomes)
    assert sorted(result[2] for result in outcomes) == [1, 2]
    assert all(process.exitcode == 0 for process in processes)


def test_cross_date_security_swap_does_not_create_false_overlap() -> None:
    left = {"dataset": "daily_bar", "frequency": "1d",
            "securities_by_date": {DAY: ["XSHG:600001"], "2026-10-12": ["XSHE:000001"]}}
    right = {"dataset": "daily_bar", "frequency": "1d",
             "securities_by_date": {DAY: ["XSHE:000001"], "2026-10-12": ["XSHG:600001"]}}
    assert MetadataStore._scope_overlaps(left, right) == ()
    right["securities_by_date"][DAY] = ["XSHG:600001"]
    assert MetadataStore._scope_overlaps(left, right) == (
        {"trade_date": DAY, "securities": ("XSHG:600001",)},
    )


def test_failed_task_can_requeue_distinct_frozen_scope_subsets(tmp_path: Path) -> None:
    path = tmp_path / "metadata.sqlite3"
    _initialize(path)
    frozen = _scope("XSHG:600001", "XSHE:000001")
    first_subset = _scope("XSHG:600001")
    second_subset = _scope("XSHE:000001")
    with MetadataStore(path) as metadata:
        accepted = metadata.enqueue_collection_task(_task("subset-retry", frozen), frozen,
            snapshot={"registry_hash": metadata.partition_registry_snapshot(frozen)}, daily_owner_refs=())
        state = metadata.load_collection_task("subset-retry")
        state["status"] = "failed"
        metadata.save_collection_task(state)
        first = metadata.enqueue_collection_task(_task("subset-retry", frozen), first_subset,
            snapshot={"registry_hash": metadata.partition_registry_snapshot(first_subset)}, frozen_scope=frozen,
            daily_owner_refs=())
        state = metadata.load_collection_task("subset-retry")
        state["status"] = "failed"
        metadata.save_collection_task(state)
        second = metadata.enqueue_collection_task(_task("subset-retry", frozen), second_subset,
            snapshot={"registry_hash": metadata.partition_registry_snapshot(second_subset)}, frozen_scope=frozen,
            daily_owner_refs=())
        row = metadata.connection.execute(
            "SELECT scope_json, active_scope_json FROM collection_task_context WHERE task_id=?", ["subset-retry"]
        ).fetchone()
        assert first["queue_order"] == 2 and second["queue_order"] == 3
        assert first["first_accepted_at"] == accepted["first_accepted_at"]
        assert row[0] == json.dumps(frozen, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        assert row[1] == json.dumps(second_subset, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def test_fifo_claim_and_requeue_keep_first_acceptance_but_get_new_order(tmp_path: Path) -> None:
    path = tmp_path / "metadata.sqlite3"
    _initialize(path)
    scopes = [_scope(f"XSHG:60000{i}") for i in (1, 2)]
    with MetadataStore(path) as metadata:
        accepted = metadata.enqueue_collection_task(_task("fifo-a", scopes[0]), scopes[0],
            snapshot={"registry_hash": metadata.partition_registry_snapshot(scopes[0])}, daily_owner_refs=())
        metadata.enqueue_collection_task(_task("fifo-b", scopes[1]), scopes[1],
            snapshot={"registry_hash": metadata.partition_registry_snapshot(scopes[1])}, daily_owner_refs=())
        duplicate = metadata.enqueue_collection_task(_task("fifo-a", scopes[0]), scopes[0],
            snapshot={"registry_hash": "stale-is-ignored-for-the-same-active-id"})
        assert duplicate["already_queued"] and duplicate["queue_order"] == 1
        assert metadata.claim_next_collection_task()["task_id"] == "fifo-a"
        assert metadata.claim_next_collection_task()["task_id"] == "fifo-a"
        failed = metadata.load_collection_task("fifo-a")
        failed["status"] = "failed"
        metadata.save_collection_task(failed)
        requeued = metadata.enqueue_collection_task(_task("fifo-a", scopes[0]), scopes[0],
            snapshot={"registry_hash": metadata.partition_registry_snapshot(scopes[0])}, daily_owner_refs=())
        assert requeued["queue_order"] == 3
        assert requeued["first_accepted_at"] == accepted["first_accepted_at"]
        assert metadata.claim_next_collection_task()["task_id"] == "fifo-b"


def test_daily_admission_uses_owner_preview_not_legacy_partition_status(tmp_path: Path) -> None:
    path = tmp_path / "metadata.sqlite3"
    _initialize(path)
    scope = _scope("XSHG:600001")
    with MetadataStore(path) as metadata:
        snapshot = {"registry_hash": metadata.partition_registry_snapshot(scope)}
        now = datetime.now(timezone.utc)
        metadata.connection.execute(
            """INSERT INTO partition_status (
                   dataset, asset_type, partition_key, adjustment, expected_count, actual_count,
                   status, manifest_path, content_hash, conflict_count, quarantined_count, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ["daily_bar", "stock", DAY, "aggregate", 1, 1, "complete_clean", "manifest.json",
             "evidence", 0, 0, now],
        )
        accepted = metadata.enqueue_collection_task(_task("stale-snapshot", scope), scope, snapshot=snapshot,
            daily_owner_refs=())
        assert accepted["status"] == "waiting"
        assert metadata.load_collection_task("stale-snapshot")["daily_overwrite_grant"]["owners"] == []
