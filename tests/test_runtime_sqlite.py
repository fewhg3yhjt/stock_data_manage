from __future__ import annotations

from datetime import date
import multiprocessing
from pathlib import Path

from stock_data_manage.service.calendar import CalendarDay, TradingCalendarStore
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.worker.attempts import CollectionAttempt


def _idle_open_store(path: str, ready, release) -> None:
    with MetadataStore(path):
        ready.set()
        release.wait(10)


def _read_calendar_and_write_task(path: str, result) -> None:
    try:
        with TradingCalendarStore(path, read_only=True) as calendar:
            calendar_result = calendar.is_trading_day(date(2026, 10, 9))
        with MetadataStore(path) as metadata:
            metadata.save_collection_task({
                "task_id": "parallel-task",
                "definition_hash": "definition-v1",
                "dataset": "daily_bar",
                "status": "collecting",
            })
        result.put(("ok", calendar_result))
    except Exception as exc:  # reported to the parent process for a useful assertion
        result.put(("error", repr(exc)))


def _race_claim(path: str, barrier, result) -> None:
    from datetime import datetime, timezone
    with MetadataStore(path) as metadata:
        barrier.wait(10)
        result.put(metadata.claim_attempt(
            CollectionAttempt("same-attempt"), updated_at=datetime(2026, 10, 14, tzinfo=timezone.utc)
        ))


def _race_provider_failure(path: str, barrier, result) -> None:
    from datetime import datetime, timezone
    with MetadataStore(path) as metadata:
        barrier.wait(10)
        health = metadata.record_provider_failure(
            provider="fixture", endpoint="daily", market="XSHG", asset_type="stock",
            dataset="daily_bar", capability_version="v1", failure_class="transport",
            error="fixture failure", now=datetime(2026, 10, 14, tzinfo=timezone.utc),
            cooldown_seconds=30,
        )
        result.put(health.consecutive_failures)


def test_idle_process_connection_does_not_block_other_process_read_and_write(tmp_path: Path) -> None:
    path = str(tmp_path / "metadata.sqlite3")
    with MetadataStore(path):
        pass
    with TradingCalendarStore(path) as calendar:
        calendar.upsert([CalendarDay(date(2026, 10, 9), True, "fixture", 1)])

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    result = context.Queue()
    idle = context.Process(target=_idle_open_store, args=(path, ready, release))
    writer = context.Process(target=_read_calendar_and_write_task, args=(path, result))
    idle.start()
    try:
        assert ready.wait(10)
        writer.start()
        assert result.get(timeout=10) == ("ok", True)
        writer.join(10)
        assert writer.exitcode == 0
    finally:
        release.set()
        idle.join(10)
        if idle.is_alive():
            idle.terminate()
            idle.join()
    assert idle.exitcode == 0
    with MetadataStore(path, read_only=True) as metadata:
        assert metadata.load_collection_task("parallel-task")["status"] == "collecting"


def test_calendar_can_initialize_shared_file_before_metadata(tmp_path: Path) -> None:
    path = tmp_path / "metadata.sqlite3"
    with TradingCalendarStore(str(path)) as calendar:
        calendar.upsert([CalendarDay(date(2026, 10, 12), False, "fixture", 1)])
    with MetadataStore(path) as metadata:
        assert metadata.connection.execute("SELECT count(*) FROM partition_status").fetchone() == (0,)
    with TradingCalendarStore(str(path), read_only=True) as calendar:
        assert calendar.is_trading_day(date(2026, 10, 12)) is False


def test_calendar_batch_upsert_rolls_back_when_input_fails(tmp_path: Path) -> None:
    path = str(tmp_path / "calendar.sqlite3")
    with TradingCalendarStore(path) as calendar:
        try:
            calendar.upsert([CalendarDay(date(2026, 10, 13), True, "fixture", 1), None])
        except AttributeError:
            pass
        else:
            raise AssertionError("expected input failure")
        assert calendar.all() == ()


def test_attempt_claim_is_exclusive_across_processes(tmp_path: Path) -> None:
    path = str(tmp_path / "metadata.sqlite3")
    with MetadataStore(path):
        pass
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    result = context.Queue()
    processes = [context.Process(target=_race_claim, args=(path, barrier, result)) for _ in range(2)]
    for process in processes:
        process.start()
    try:
        outcomes = sorted([result.get(timeout=15), result.get(timeout=15)])
    finally:
        for process in processes:
            process.join(15)
            if process.is_alive():
                process.terminate()
                process.join()
    assert outcomes == [False, True]
    assert all(process.exitcode == 0 for process in processes)


def test_provider_failure_counters_are_atomic_across_processes(tmp_path: Path) -> None:
    path = str(tmp_path / "metadata.sqlite3")
    with MetadataStore(path):
        pass
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    result = context.Queue()
    processes = [context.Process(target=_race_provider_failure, args=(path, barrier, result)) for _ in range(2)]
    for process in processes:
        process.start()
    try:
        outcomes = [result.get(timeout=15), result.get(timeout=15)]
    finally:
        for process in processes:
            process.join(15)
            if process.is_alive():
                process.terminate()
                process.join()
    assert sorted(outcomes) == [1, 2]
    assert all(process.exitcode == 0 for process in processes)
    with MetadataStore(path, read_only=True) as metadata:
        health = metadata.provider_health(
            provider="fixture", endpoint="daily", market="XSHG", asset_type="stock",
            dataset="daily_bar", capability_version="v1",
        )
        assert health is not None
        assert health.consecutive_failures == 2
