from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from stock_data_manage.worker.attempts import CollectionAttempt
from stock_data_manage.domain import AttemptStatus, ItemStatus
from stock_data_manage.storage.integrity import Manifest
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.storage.raw import RawObjectStore
from stock_data_manage.quality.resolution import resolve_records


def test_http_capture_reuses_source_bytes_across_code_versions(tmp_path, monkeypatch):
    import requests
    from stock_data_manage.providers.transport import captured_requests, RequestPacer
    calls = []
    def send(session, request, **options):
        calls.append(request.url)
        assert options["allow_redirects"] is False
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"retained":true}'
        response.url = request.url
        response.request = request
        return response
    monkeypatch.setattr(requests.Session, "send", send)
    original = RawObjectStore(tmp_path / "original")
    for store, version, roots in ((original, "old-code-version", ()),
                                  (RawObjectStore(tmp_path / "cached"), "new-code-version", (original.root,))):
        with captured_requests(store, provider="fixture", endpoint="daily", scope={"symbol":"sh600519"},
                code_version=version, pacer=RequestPacer(wait=lambda _: None), evidence_roots=roots,
                max_age_seconds=86400) as events:
            response = requests.get("https://fixture.invalid/daily?symbol=sh600519")
            assert response.content == b'{"retained":true}'
        assert RawObjectStore.verify_manifest(store.manifest)
    assert calls == ["https://fixture.invalid/daily?symbol=sh600519"]
    assert events[-1]["mode"] == "cached"


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
    with MetadataStore(tmp_path / "metadata.sqlite3") as metadata:
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


def test_metadata_datetime_storage_normalizes_offsets_to_utc(tmp_path):
    local_time = datetime(2026, 10, 9, 17, 30, tzinfo=timezone(timedelta(hours=8)))
    with MetadataStore(tmp_path / "metadata.sqlite3") as metadata:
        metadata.save_attempt(CollectionAttempt("offset-time"), updated_at=local_time)
        stored = metadata.connection.execute(
            "SELECT updated_at FROM collection_attempt WHERE attempt_id=?", ["offset-time"]
        ).fetchone()[0]
        assert stored == datetime(2026, 10, 9, 9, 30, tzinfo=timezone.utc)
        assert stored.tzinfo == timezone.utc


def test_conflict_log_is_deduplicated(tmp_path, minute_bar, final_minute_bar) -> None:
    from dataclasses import replace
    from decimal import Decimal

    conflict = replace(final_minute_bar, close=Decimal("120"), high=Decimal("121"))
    conflicts = resolve_records([minute_bar, conflict]).conflicts
    with MetadataStore(tmp_path / "metadata.sqlite3") as metadata:
        metadata.record_conflicts(
            dataset="minute_bar_1m", partition_key="2026-09-11", conflicts=conflicts
        )
        metadata.record_conflicts(
            dataset="minute_bar_1m", partition_key="2026-09-11", conflicts=conflicts
        )
        assert metadata.conflict_count("minute_bar_1m", "2026-09-11") == len(conflicts)


def test_partition_batch_updates_preserve_other_items_and_rollback(tmp_path):
    now = datetime(2026, 10, 12, tzinfo=timezone.utc)
    statuses = {f"XSHG:{i:06d}": "success" for i in range(7267)}
    manifest = Manifest(dataset="daily_bar", asset_type="stock", partition_key="2026-10-09",
                        row_count=7267, expected_count=7267, first_key=None, last_key=None,
                        content_hash="original", size_bytes=0, raw_refs=(), item_statuses=statuses,
                        source_providers={key: "tencent" for key in statuses})
    with MetadataStore(tmp_path / "metadata.sqlite3") as metadata:
        metadata.record_partition_publish(manifest=manifest, manifest_path="manifest.json", updated_at=now)
        assert len(metadata.item_statuses("daily_bar", "2026-10-09")) == 7267
        key = "XSHG:000000"
        metadata.record_partition_publish(manifest=manifest, manifest_path="manifest.json",
            item_statuses={key: ItemStatus.NO_TRADE}, source_providers={key: None}, updated_at=now)
        assert metadata.item_statuses("daily_bar", "2026-10-09")[key] == ItemStatus.NO_TRADE
        assert metadata.connection.execute("SELECT source_provider FROM partition_item WHERE instrument_id=?", [key]).fetchone() == (None,)
        assert metadata.item_statuses("daily_bar", "2026-10-09")["XSHG:000001"] == ItemStatus.SUCCESS
        previous = metadata.partition("daily_bar", "stock", "2026-10-09")
        with pytest.raises(AttributeError):
            metadata.record_partition_publish(manifest=replace(manifest, content_hash="must-rollback"),
                manifest_path="new.json", item_statuses={key: "invalid-enum"}, updated_at=now)
        assert metadata.partition("daily_bar", "stock", "2026-10-09") == previous
        assert len(metadata.item_statuses("daily_bar", "2026-10-09")) == 7267
        import sqlite3
        with pytest.raises(sqlite3.IntegrityError):
            metadata.record_partition_publish(manifest=replace(manifest, content_hash="sql-rollback"),
                manifest_path="new.json", item_statuses={None: ItemStatus.SUCCESS}, updated_at=now)
        assert metadata.partition("daily_bar", "stock", "2026-10-09") == previous
        assert len(metadata.item_statuses("daily_bar", "2026-10-09")) == 7267
        # Empty updates must still record a partition, without deleting items.
        metadata.record_partition_publish(manifest=manifest, manifest_path="manifest.json", item_statuses={})
        assert len(metadata.item_statuses("daily_bar", "2026-10-09")) == 7267


def test_metadata_read_only_missing_database_does_not_create_files(tmp_path):
    path = tmp_path / "missing" / "metadata.sqlite3"
    with pytest.raises(FileNotFoundError, match="does not exist"):
        MetadataStore(path, read_only=True)
    assert not path.parent.exists()
    with pytest.raises(FileNotFoundError, match="does not exist"):
        MetadataStore(read_only=True)


def test_metadata_read_only_open_does_not_write_business_data_or_schema(tmp_path):
    path = tmp_path / "metadata.sqlite3"
    with MetadataStore(path):
        pass
    with MetadataStore(path, read_only=True) as metadata:
        assert metadata.connection.execute("SELECT count(*) FROM collection_task").fetchone() == (0,)
        assert metadata.connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
        ).fetchone() == (9,)


def test_metadata_rejects_legacy_database_without_changing_bytes(tmp_path):
    path = tmp_path / "legacy-metadata.duckdb"
    original = b"DuckDB legacy database bytes; migration is not supported"
    path.write_bytes(original)
    with pytest.raises(ValueError, match="legacy DuckDB is not migrated"):
        MetadataStore(path)
    assert path.read_bytes() == original


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


def test_interrupted_raw_copy_rebuilds_index_before_removing_temporary(tmp_path):
    from types import SimpleNamespace
    root=tmp_path/'raw'
    temporary=root/'_tmp/2026-10-29/tencent/kline_daily/manifest.request.ndjson'
    current=root/'2026-10-29/tencent/kline_daily/manifest.request.ndjson'
    store=RawObjectStore(temporary.parent,manifest_name=temporary.name)
    store.record_response(response=SimpleNamespace(content=b'{}',status_code=200,headers={},encoding='utf-8'),
        url='https://fixture.invalid/daily',method='GET',request_headers={},scope={},
        provider='fixture',endpoint='daily',code_version='fixture-v1',mode='fixture')
    expected=RawObjectStore.verify_manifest(temporary)
    RawObjectStore.copy_request(temporary,current)
    assert not current.with_name('manifest.ndjson').exists()
    RawObjectStore.promote_unit(temporary=temporary,current=current,raw_root=root,
        archive_root=tmp_path/'archive',expected_hash=expected)
    assert current.with_name('manifest.ndjson').is_file()
    assert not temporary.exists() and RawObjectStore.verify_manifest(current)==expected


def test_batch_raw_promotion_checks_source_and_destination_once(tmp_path, monkeypatch):
    from types import SimpleNamespace
    root = tmp_path / 'raw'
    temporary = root / '_tmp/2026-10-29/tencent/kline_daily/manifest.request.ndjson'
    current = root / '2026-10-29/tencent/kline_daily/manifest.request.ndjson'
    store = RawObjectStore(temporary.parent, manifest_name=temporary.name)
    store.record_response(response=SimpleNamespace(content=b'{"bars":[1]}', status_code=200,
            headers={}, encoding='utf-8'), url='https://fixture.invalid/daily', method='GET',
        request_headers={}, scope={}, provider='fixture', endpoint='daily',
        code_version='fixture-v1', mode='fixture')
    expected = RawObjectStore.verify_manifest(temporary)
    original_verify, original_copy = RawObjectStore.verify_manifest, RawObjectStore.copy_request
    verified, copy_options = [], []

    def track_verify(path):
        verified.append(str(path))
        return original_verify(path)

    def track_copy(source, destination, *, verify=True):
        copy_options.append(verify)
        return original_copy(source, destination, verify=verify)

    monkeypatch.setattr(RawObjectStore, 'verify_manifest', staticmethod(track_verify))
    monkeypatch.setattr(RawObjectStore, 'copy_request', staticmethod(track_copy))
    promotion = {'temporary': str(temporary), 'current': str(current), 'raw_hash': expected}

    resolved_context = (root.resolve(), temporary.parent, temporary.parent.resolve(),
        current.parent, current.parent.resolve())
    RawObjectStore.promote_unit(temporary=temporary, current=current, raw_root=root,
        archive_root=tmp_path / 'archive', expected_hash=expected, defer_cleanup=True,
        _resolved_path_context=resolved_context)
    assert copy_options == [False]
    assert verified == [str(temporary)]

    RawObjectStore.finalize_promotions([promotion], raw_root=root)
    assert verified == [str(temporary), str(current)]
    assert not temporary.exists()
    assert RawObjectStore.verify_manifest(current) == expected


def test_batch_raw_finalization_reuses_already_promoted_manifest_hash(tmp_path, monkeypatch):
    import hashlib
    from types import SimpleNamespace

    root = tmp_path / 'raw'
    current = root / '2026-10-29/tencent/kline_daily/manifest.request.ndjson'
    temporary = root / '_tmp/2026-10-29/tencent/kline_daily/manifest.request.ndjson'
    store = RawObjectStore(current.parent, manifest_name=current.name)
    store.record_response(response=SimpleNamespace(content=b'{"bars":[1]}', status_code=200,
            headers={}, encoding='utf-8'), url='https://fixture.invalid/daily', method='GET',
        request_headers={}, scope={}, provider='fixture', endpoint='daily',
        code_version='fixture-v1', mode='fixture')
    expected = hashlib.sha256(current.read_bytes()).hexdigest()

    monkeypatch.setattr(RawObjectStore, 'verify_manifest', staticmethod(
        lambda path: pytest.fail('already promoted response bodies must not be reread on task recovery')))

    RawObjectStore.finalize_promotions([{'temporary': str(temporary), 'current': str(current),
        'raw_hash': expected}], raw_root=root)

    assert current.with_name('manifest.ndjson').is_file()
    assert not temporary.exists()
