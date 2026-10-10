from dataclasses import replace
from datetime import datetime, timezone

import pytest

from stock_data_manage.worker.attempts import CollectionAttempt
from stock_data_manage.domain import AttemptStatus, ItemStatus
from stock_data_manage.storage.integrity import Manifest
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.storage.raw import RawObjectStore
from stock_data_manage.quality.resolution import resolve_records


@pytest.mark.parametrize("data_date", ["20261001", "2026-1-1", "../2026-10-01", "2026-02-30"])
def test_raw_date_paths_reject_invalid_dates(tmp_path, data_date):
    with pytest.raises(ValueError):
        RawObjectStore.task_unit_path(tmp_path, provider="provider", endpoint="endpoint", data_date=data_date)
    with pytest.raises(ValueError):
        RawObjectStore.current_path(tmp_path, "provider", "endpoint", data_date=data_date, parameters={})
    assert list(tmp_path.iterdir()) == []


def test_raw_promotion_rejects_cross_date_destination_before_writing(tmp_path):
    temporary = RawObjectStore.task_unit_path(tmp_path, provider="provider", endpoint="endpoint", data_date="2026-09-30") / "manifest.all-test.ndjson"
    current = RawObjectStore.current_path(tmp_path, "provider", "endpoint", data_date="2026-10-01", parameters={}) / temporary.name
    with pytest.raises(ValueError, match="matching data-date"):
        RawObjectStore.promote_unit(temporary=temporary, current=current, raw_root=tmp_path,
                                   archive_root=tmp_path.parent / "archive", expected_hash="unused")
    assert list(tmp_path.iterdir()) == []


def test_current_catalog_bytes_use_original_day_and_replace_only_own_request(tmp_path):
    import json
    from types import SimpleNamespace
    from stock_data_manage.storage.integrity import file_hash
    root = tmp_path / "raw"
    previous_dir = RawObjectStore.task_unit_path(root, provider="bse", endpoint="security_snapshot", data_date="2026-10-09")
    name = RawObjectStore.request_manifest_name({}, input_id="SECURITY-BSE-001")
    options = dict(url="https://fixture.invalid/catalog", method="GET", request_headers={}, scope={},
                   provider="bse", endpoint="security_snapshot", code_version="fixture", mode="replay")
    def response(body):
        return SimpleNamespace(content=body, status_code=200, headers={}, encoding="utf-8")
    previous = RawObjectStore(previous_dir, manifest_name=name)
    previous.record_response(response=response(b"old catalog"), **options)
    RawObjectStore.refresh_index(previous.manifest)
    old_hash = file_hash(previous.manifest)
    neighbour = RawObjectStore(previous_dir, manifest_name="manifest.other.ndjson")
    neighbour.record_response(response=response(b"other scope"), **options)
    RawObjectStore.refresh_index(neighbour.manifest)
    neighbour_hash = file_hash(neighbour.manifest)
    store = RawObjectStore(RawObjectStore.task_unit_path(root, provider="bse", endpoint="security_snapshot", data_date="2026-10-11"),
                           manifest_name=name, capture_root=root, capture_archive_root=tmp_path / "task_archive")
    event = store.record_response(response=response(b"original dated catalog"), source_ref={"fetched_at_utc": "2026-10-08T16:30:00+00:00"},
                                  fetched_at=datetime(2026, 10, 11, tzinfo=timezone.utc), **options)
    assert store.root == previous_dir
    assert RawObjectStore.read_response(store.manifest, event) == b"original dated catalog"
    assert file_hash(neighbour.manifest) == neighbour_hash
    assert old_hash in {file_hash(p) for p in (tmp_path / "task_archive").rglob("manifest.ndjson")}
    events = [json.loads(line) for line in store.manifest.read_text().splitlines()]
    assert [e["body_sha256"] for e in events if e["event"] == "http_response"] == [event["body_sha256"]]
    RawObjectStore.refresh_index(store.manifest)
    assert RawObjectStore.verify_manifest(previous_dir / "manifest.ndjson")
    assert not (root / "_tmp/2026-10-11/bse/security_snapshot").exists()


def test_raw_objects_are_immutable_and_verifiable(tmp_path) -> None:
    store = RawObjectStore(tmp_path / "raw")
    fetched_at = datetime(2026, 9, 11, 16, tzinfo=timezone.utc)
    reference = store.write_json(
        {"rows": [{"symbol": "sh600519", "close": "10"}]},
        dataset="daily_bar",
        provider="fixture",
        endpoint="history",
        fetched_at=fetched_at,
        attempt_id="attempt-1",
    )
    assert store.verify(reference)
    duplicate = store.write_json(
        {"rows": [{"symbol": "sh600519", "close": "10"}]},
        dataset="daily_bar",
        provider="fixture",
        endpoint="history",
        fetched_at=fetched_at,
        attempt_id="attempt-1",
    )
    assert duplicate == reference
    with pytest.raises(FileExistsError, match="immutable"):
        store.write_json(
            {"rows": []},
            dataset="daily_bar",
            provider="fixture",
            endpoint="history",
            fetched_at=fetched_at,
            attempt_id="attempt-1",
        )


def test_metadata_round_trips_attempt_and_completed_no_trade_partition(tmp_path) -> None:
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    attempt = CollectionAttempt("attempt-1").lease(
        owner="worker", acquired_at=now, expires_at=now.replace(hour=1)
    )
    attempt = attempt.transition(AttemptStatus.FETCHING).transition(AttemptStatus.RAW_COMMITTED)
    attempt = replace(attempt, raw_object_path="raw/a", raw_content_hash="abc")
    data = tmp_path / "data.parquet"
    data.write_bytes(b"partition")
    manifest = Manifest.from_file(
        data,
        dataset="daily_bar",
        asset_type="stock",
        partition_key="2026-09-11",
        row_count=1,
        expected_count=2,
        first_key="a",
        last_key="a",
        item_statuses={"a": "success", "b": "no_trade"},
        source_providers={"a": "fixture", "b": None},
    )
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        metadata.save_attempt(attempt, updated_at=now)
        assert metadata.load_attempt("attempt-1") == attempt
        metadata.record_partition_publish(manifest=manifest, manifest_path=tmp_path / "manifest.json")
        partition = metadata.partition("daily_bar", "stock", "2026-09-11")
        assert partition is not None
        assert partition["status"] == "complete_clean"
        assert metadata.item_statuses("daily_bar", "2026-09-11") == {
            "a": ItemStatus.SUCCESS,
            "b": ItemStatus.NO_TRADE,
        }


def test_conflict_log_is_deduplicated(tmp_path, minute_bar, final_minute_bar) -> None:
    from dataclasses import replace
    from decimal import Decimal

    conflict = replace(final_minute_bar, close=Decimal("120"), high=Decimal("121"))
    conflicts = resolve_records([minute_bar, conflict]).conflicts
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        metadata.record_conflicts(
            dataset="minute_bar_1m", partition_key="2026-09-11", conflicts=conflicts
        )
        metadata.record_conflicts(
            dataset="minute_bar_1m", partition_key="2026-09-11", conflicts=conflicts
        )
        assert metadata.conflict_count("minute_bar_1m", "2026-09-11") == len(conflicts)


@pytest.mark.parametrize("relative", ["../escape.json", "/absolute.json", "nested/../../escape.json", "."])
def test_raw_compact_path_rejects_escape(tmp_path, relative):
    store = RawObjectStore(tmp_path)
    with pytest.raises(ValueError):
        store.write_json({}, dataset="test", provider="fixture", endpoint="test",
                         fetched_at=datetime.now(timezone.utc), attempt_id="a", relative_path=relative)


def test_raw_compact_path_retains_immutable_write_contract(tmp_path):
    store = RawObjectStore(tmp_path)
    kwargs = dict(dataset="test", provider="fixture", endpoint="test", fetched_at=datetime.now(timezone.utc),
                  attempt_id="a", relative_path="sources/fixture/parsed/rows.json")
    first = store.write_json({"value":1}, **kwargs)
    assert store.write_json({"value":1}, **kwargs) == first
    with pytest.raises(FileExistsError):
        store.write_json({"value":2}, **kwargs)
    assert store.verify(first)
