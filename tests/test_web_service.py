import hashlib
import json
import asyncio
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from stock_data_manage.service.web import DashboardReader, create_app

if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def web_fixture(tmp_path: Path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "presentation.yaml").write_bytes((Path(__file__).resolve().parents[1] / "config/presentation.yaml").read_bytes())
    (config / "collection.yaml").write_text(
        """storage:
  data_root: data
  raw_root: data/raw
  workspace_root: data/task_workspace
  archive_root: data/task_archive
  canonical_root: data/canonical
  metadata_path: data/metadata/metadata.duckdb
realtime_minute:
  hot_store:
    path: data/hot/minute_hot.db
""",
        encoding="utf-8",
    )
    data = tmp_path / "runtime"
    stale = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    summary = {
        "task_id": "daily-2026-10-09",
        "dataset": "daily_bar",
        "status": "collecting",
        "complete_today": False,
        "data_date": None,
        "requested_date": "2026-10-09",
        "publication": {"row_count": 1, "manifest_sha256": "frozen-hash"},
        "generated_at_utc": stale,
        "verification_mode": "retained",
        "missing_units": ["sh600001"],
        "source_units": [
            {
                "key": "sh600519",
                "symbol": "sh600519",
                "status": "candidate_complete",
                "row_count": 1,
                "raw_manifest": str(tmp_path / "must-not-leak.ndjson"),
                "request_scope": {"request": {"symbol": "sh600519", "token": "secret"}},
            },
            {"key": "sh600001", "symbol": "sh600001", "status": "failed", "error": "timeout"},
        ],
        "progress": {
            "version": 2,
            "stages": {
                "raw": {"status": "running", "completed": 1, "total": 2, "percent": 50},
                "processing": {"status": "running", "completed": 1, "total": 2, "percent": 50},
                "publication": {"status": "pending", "completed": 0, "total": 2, "percent": 0},
            },
        },
    }
    write_json(data / "task_workspace/2026-10-09/daily_bar/summary.json", summary)
    write_json(data / "task_workspace/2026-10-09/daily_bar/task.json",
               {"dataset": "daily_bar", "trade_date": "2026-10-09", "asset_type": "stock", "units": []})
    from stock_data_manage.storage.metadata import MetadataStore
    metadata_path = data / "metadata/metadata.duckdb"
    with MetadataStore(metadata_path) as metadata:
        persisted = {**summary, "definition_hash": "fixture-definition"}
        metadata.save_collection_task(persisted)
        metadata.connection.execute("UPDATE collection_task SET updated_at=? WHERE task_id=?",
            [datetime.fromisoformat(stale), summary["task_id"]])
        metadata.connection.execute("""INSERT INTO partition_status (
            dataset,asset_type,partition_key,adjustment,expected_count,actual_count,status,manifest_path,content_hash,
            conflict_count,quarantined_count,no_trade_count,none_available_count,none_unverified_count,
            none_not_applicable_count,forward_available_count,forward_unverified_count,
            forward_not_applicable_count,backward_available_count,backward_unverified_count,
            backward_not_applicable_count,data_revision,coverage_reason,updated_at,last_task_id)
            VALUES ('daily_bar','stock','2026-10-09','aggregate',NULL,1,'published',NULL,'abc123',0,0,0,
                    1,0,0,0,1,0,0,0,0,1,'fixture lacks independent denominator',?,?)""",
            [datetime.now(timezone.utc), summary["task_id"]])

    body = b'{"code":"sh600519","close":1820.0}'
    digest = hashlib.sha256(body).hexdigest()
    raw_dir = data / "raw/2026-10-09/tencent/kline_daily"
    (raw_dir / "bodies").mkdir(parents=True)
    (raw_dir / f"bodies/{digest}.bin").write_bytes(body)
    events = [
        {"event": "stored_artifact", "body_storage": f"bodies/{digest}.bin", "body_sha256": digest},
        {
            "event": "http_response",
            "method": "GET",
            "url": "https://example.test/kline?symbol=sh600519&token=visible-secret",
            "request_headers": {"User-Agent": "fixture", "Authorization": "Bearer secret", "Cookie": "session=secret"},
            "request_options": {"timeout": 10, "proxies": {"https": "http://user:pass@proxy"}},
            "scope": {"request": {"symbol": "sh600519", "api_key": "secret"}},
            "provider": "tencent",
            "endpoint": "kline_daily",
            "fetched_at_utc": "2026-10-09T08:00:00+00:00",
            "mode": "live",
            "outcome": "response",
            "status_code": 200,
            "response_headers": {"Content-Type": "application/json", "Set-Cookie": "source=secret"},
            "content_type": "application/json",
            "encoding": "utf-8",
            "body_bytes": len(body),
            "body_sha256": digest,
            "body_storage": f"bodies/{digest}.bin",
            "persistence_status": "exact_application_payload_saved",
        },
    ]
    manifest = raw_dir / "manifest.sh600519.ndjson"
    manifest.write_text("".join(json.dumps(item) + "\n" for item in events), encoding="utf-8")
    from stock_data_manage.storage.raw import RawObjectStore
    RawObjectStore.refresh_index(manifest)

    write_json(
        data / "canonical/2026-10-09/daily_bar/stock/manifest.json",
        {
            "dataset": "daily_bar",
            "asset_type": "stock",
            "partition_key": "2026-10-09",
            "row_count": 1,
            "expected_count": 2,
            "conflict_count": 0,
            "quarantined_count": 0,
            "content_hash": "abc123",
            "size_bytes": 128,
            "raw_refs": [str(manifest)],
        },
    )
    return DashboardReader(config, data), create_app(config_root=config, data_root=data), data, body


def test_raw_options_filter_names_and_respect_date_directory_boundaries(tmp_path):
    import os
    reader, app, data, _ = web_fixture(tmp_path)
    raw_root = reader.paths["raw_root"]
    (raw_root / "2026-11-10").mkdir()
    (raw_root / "not-a-date").mkdir()
    (raw_root / "2026-11-11").write_text("not a directory", encoding="utf-8")
    outside = tmp_path / "outside-date"
    outside.mkdir()
    linked_inside = raw_root / "2026-11-12"
    escaped = raw_root / "2026-11-13"
    try:
        if os.name == "nt":
            import _winapi
            _winapi.CreateJunction(str(raw_root / "2026-11-10"), str(linked_inside))
            _winapi.CreateJunction(str(outside), str(escaped))
        else:
            linked_inside.symlink_to(raw_root / "2026-11-10", target_is_directory=True)
            escaped.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory link boundary could not be created: {exc}")
    expected = ["2026-11-12", "2026-11-10", "2026-10-09"]
    assert reader.raw_options()["dates"] == expected
    with TestClient(app) as client:
        response = client.get("/api/raw/options")
    assert response.status_code == 200 and response.json()["dates"] == expected
    empty_app = create_app(config_root=tmp_path / "config", data_root=tmp_path / "empty-runtime")
    with TestClient(empty_app) as client:
        empty = client.get("/api/raw/options")
    assert empty.status_code == 200 and empty.json() == {"dates": [], "latest_date": None}


def test_read_only_overview_marks_stale_task_and_lists_publication(tmp_path):
    reader, _, _, _ = web_fixture(tmp_path)

    payload = reader.overview()
    assert payload["task_counts"] == {"stale": 1}
    assert payload["recent_tasks"][0]["display_status"] == "stale"
    assert payload["recent_tasks"][0]["completed_units"] == 1
    assert payload["recent_publications"][0]["coverage_percent"] is None
    assert payload["recent_publications"][0]["coverage_reason"] == "fixture lacks independent denominator"
    assert payload["publication_scope"] == "registered_publications"
    assert payload["recent_publications"][0]["latest_task_id"] == "daily-2026-10-09"


def test_overview_does_not_expand_publication_raw_evidence(tmp_path):
    reader, _, _, _ = web_fixture(tmp_path)

    def evidence_should_not_be_read(_path):
        raise AssertionError("overview must return publication summaries without raw evidence")

    reader._manifest_records = evidence_should_not_be_read
    payload = reader.overview()

    assert payload["recent_publications"][0]["dataset"] == "daily_bar"
    assert payload["recent_publications"][0]["row_count"] == 1
    assert "evidence" not in payload["recent_publications"][0]


def test_publication_listing_returns_registered_summary_only(tmp_path):
    reader, _, _, _ = web_fixture(tmp_path)

    payload = reader.publications()
    detail = reader.publications(dataset="daily_bar", asset_type="stock", partition_key="2026-10-09")

    assert payload["scope"] == "registered_publications"
    assert payload["total"] == detail["total"] == 1
    assert detail["items"][0]["latest_task_id"] == "daily-2026-10-09"
    assert detail["items"][0]["row_count"] == 1
    assert detail["items"][0]["coverage_percent"] is None
    assert not ({"evidence", "tasks", "source_count", "size_bytes", "publication_metadata"}
                & detail["items"][0].keys())


def test_daily_batch_partition_links_to_published_task(tmp_path):
    reader, _, data, _ = web_fixture(tmp_path)
    manifest = data / "canonical/2026-10-09/daily_bar/stock/manifest.json"
    summary_path = data / "task_workspace/2026-10-09/daily_bar/summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["status"] = "published"
    summary["partitions"] = {
        "stock": {"status": "published", "published_manifest": str(manifest)}
    }
    write_json(summary_path, summary)

    payload = reader.publications()

    # Associations come from the persisted summary row, never from scanning summary files.
    assert payload["items"][0]["latest_task_id"] == "daily-2026-10-09"


def test_daily_batch_task_counts_quality_prompts_separately_from_exclusions(tmp_path):
    reader, _, data, _ = web_fixture(tmp_path)
    summary_path = data / "task_workspace/2026-10-09/daily_bar/summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["format_version"] = "daily-batch-v2"
    summary["partitions"] = {"stock": {"status": "published"}}
    write_json(summary_path, summary)
    write_json(summary_path.with_name("coverage.stock.json"), {
        "expected_count": 2,
        "actual_count": 2,
        "issues": [
            {"code": "unverified_applicability"},
            {"asset_type": "stock", "code": "confirmed_suspended"},
            {"asset_type": "index", "code": "manual_lifecycle_exclusion"},
            {"asset_type": "stock", "code": "manual_lifecycle_exclusion"},
            {"asset_type": "stock", "code": "unexpected_quality_issue"},
        ],
    })

    result = reader.task("daily-2026-10-09")["partition_results"][0]

    assert result["other_issue_count"] == 1
    assert result["unverified_count"] == 1
    assert result["excluded_count"] == 2


def test_task_units_are_paginated_and_do_not_expose_managed_paths(tmp_path):
    reader, _, _, _ = web_fixture(tmp_path)

    payload = reader.task_units("daily-2026-10-09", page=1, page_size=1)
    assert payload["total"] == 2 and len(payload["items"]) == 1
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "must-not-leak" not in serialized
    assert "secret" not in serialized
    assert payload["items"][0]["request_scope"]["request"]["token"] == "[已隐藏]"


def test_raw_detail_redacts_secrets_previews_and_downloads_exact_bytes(tmp_path):
    reader, _, _, body = web_fixture(tmp_path)
    listing = reader.raw_records(
        data_date="2026-10-09", provider="tencent", endpoint="kline_daily",
        query=None, page=1, page_size=25,
    )
    evidence_id = listing["items"][0]["id"]

    assert listing["items"][0]["has_body"] is True
    payload = reader.raw_detail(evidence_id, preview_bytes=64 * 1024)
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "visible-secret" not in serialized
    assert "Bearer secret" not in serialized
    assert "session=secret" not in serialized
    assert "user:pass" not in serialized
    assert payload["request"]["headers"]["Authorization"] == "[已隐藏]"
    assert payload["request"]["url"].startswith("https://example.test/kline?")
    assert payload["preview"]["kind"] == "json"
    assert '"close": 1820.0' in payload["preview"]["text"]
    download, content_type, _ = reader.raw_body(evidence_id)
    assert download.read_bytes() == body and content_type == "application/json"


def test_raw_listing_uses_cached_manifest_index_at_production_scale(tmp_path, monkeypatch, record_property):
    """A 17.5k response index is listed without opening the 17.5k request manifests."""
    _, app, data, _ = web_fixture(tmp_path)
    index = data / "raw/2026-10-09/tencent/kline_daily/manifest.ndjson"
    original = json.loads(index.read_text(encoding="utf-8").splitlines()[0])
    response = {
        "event": "http_response", "provider": "tencent", "endpoint": "kline_daily",
        "method": "GET", "url": "https://example.test/kline", "outcome": "response",
        "status_code": 200, "fetched_at_utc": "2026-10-09T08:00:00+00:00",
        "body_bytes": 0, "body_sha256": "0" * 64,
    }
    index.parent.mkdir(parents=True, exist_ok=True)
    with index.open("w", encoding="utf-8") as stream:
        for number in range(17_591):
            item = {
                "event": "request_manifest", "request_manifest": f"manifest.sample-{number:05d}.ndjson",
                "sha256": "0" * 64,
                "responses": [{"event_index": 0, "response": {
                    **response, "fetched_at_utc": (datetime(2026, 10, 9, 8, tzinfo=timezone.utc)
                        + timedelta(seconds=number)).isoformat()}}],
            }
            stream.write(json.dumps(item, separators=(",", ":")) + "\n")
        stream.write(json.dumps(original, separators=(",", ":")) + "\n")

    opened_request_manifests = []
    opened_indexes = []
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        if path.name.startswith("manifest.sample-"):
            opened_request_manifests.append(path)
            raise AssertionError("listing must use the endpoint index, not each request manifest")
        if path == index:
            opened_indexes.append(path)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/api/raw")
    started = time.perf_counter()
    first = endpoint(data_date="2026-10-09", provider=None, endpoint=None,
                     q=None, page=1, page_size=25)
    cold_seconds = time.perf_counter() - started
    assert first["total"] == 17_592 and len(first["items"]) == 25
    started = time.perf_counter()
    second = endpoint(data_date="2026-10-09", provider=None, endpoint=None,
                      q=None, page=2, page_size=25)
    warm_seconds = time.perf_counter() - started
    assert len(second["items"]) == 25
    record_property("raw_listing_cold_seconds_17592_responses", round(cold_seconds, 3))
    record_property("raw_listing_warm_seconds_17592_responses", round(warm_seconds, 3))
    assert cold_seconds < 5.0
    assert warm_seconds < 1.5
    assert opened_request_manifests == []
    assert len(opened_indexes) == 1


def test_raw_query_index_rebuild_is_idempotent_and_excludes_catalog(tmp_path):
    from stock_data_manage.storage.raw import RawObjectStore

    root = tmp_path / "raw"
    endpoint = root / "2026-10-09/tencent/kline_daily"
    endpoint.mkdir(parents=True)
    request = endpoint / "manifest.sample-00001.ndjson"
    request.write_text(json.dumps({"event": "http_response", "provider": "tencent",
        "endpoint": "kline_daily", "outcome": "response", "status_code": 200,
        "url": "https://example.test/kline", "fetched_at_utc": "2026-10-09T08:00:00+00:00",
        "body_bytes": 2, "body_sha256": "ab"}) + "\n", encoding="utf-8")
    RawObjectStore.refresh_index(request)

    assert RawObjectStore.rebuild_indexes(root) == 1
    index = endpoint / "manifest.ndjson"
    records = [json.loads(line) for line in index.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1 and records[0]["request_manifest"] == request.name
    assert RawObjectStore.rebuild_indexes(root) == 1
    records = [json.loads(line) for line in index.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1 and records[0]["request_manifest"] == request.name


def test_source_query_indexes_bound_page_reads_at_current_report_scale(tmp_path, monkeypatch, record_property):
    """23,852 report/row selectors are filtered and paged in SQLite; only page rows are opened."""
    _, app, data, add = source_query_fixture(tmp_path)
    import stock_data_manage.storage.read_index as read_index

    def recursive_scan_forbidden(*args, **kwargs):
        raise AssertionError("source query requests must not walk the evidence tree")

    monkeypatch.setattr(read_index.os, "walk", recursive_scan_forbidden)
    add("sh600519", [{"0": "2026-10-09", "2": "101.00", "5": "7061.00", "symbol": "sh600519"}])
    database = data / read_index.SOURCE_QUERY_DATABASE
    run_root = (data / "task_workspace/2026-10-09/daily_bar").resolve()
    import sqlite3
    with sqlite3.connect(database) as connection:
        # Model the common case where the indexed report already carries its asset type.
        connection.execute("UPDATE source_reports SET report_asset_type='stock' WHERE request_code='600519'")
        report_rows = []
        selector_rows = []
        for number in range(23_851):
            report_path = str(run_root / f"synthetic/report-{number:05d}.report.json")
            descriptor = {"path": f"synthetic/rows-{number:05d}.json", "sha256": "0" * 64, "row_count": 1}
            params = {"code": "sh000000", "adjust": "qfq"}
            report = {"dataset": "daily_bar", "input_id": f"PERF-{number}", "provider": "tencent",
                      "endpoint": "kline_daily", "data_date": "2026-10-09", "parameters": params,
                      "source_rows": descriptor, "normalization_version": "test-v1"}
            report_json = json.dumps(report, separators=(",", ":"))
            identity = json.dumps(["daily_bar", f"PERF-{number}", "2026-10-09", "0" * 64, params],
                                  sort_keys=True, separators=(",", ":"))
            report_rows.append((report_path, str(run_root), report_json, identity, "daily_bar", f"PERF-{number}",
                "tencent", "kline_daily", "2026-10-09", json.dumps(params, sort_keys=True, separators=(",", ":")),
                "sh000000", "sh", "000000", "qfq", "stock", descriptor["path"], "0" * 64, 1, None))
            selector_rows.append((report_path, 0, "sh000000", "", "", "", "", "2026-10-09",
                                  "来源行日期", "sh", "000000"))
        connection.executemany("""INSERT INTO source_reports(report_path,run_root,report_json,report_identity,
            dataset,input_id,provider,endpoint,data_date,parameters_json,request_symbol,request_market,request_code,
            adjustment,report_asset_type,source_rows_path,source_rows_sha256,source_row_count,index_error)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", report_rows)
        selector_rows = [(connection.execute("SELECT report_id FROM source_reports WHERE report_path=?",
                         (row[0],)).fetchone()[0], *row[1:]) for row in selector_rows]
        connection.executemany("""INSERT INTO source_selectors(report_id,source_row_index,raw_symbol,raw_name,
            raw_name_folded,exchange,row_asset_type,record_date,date_basis,row_market,row_code)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)""", selector_rows)

    selected_source = data / "task_workspace/2026-10-09/daily_bar/sources/tencent/kline_daily/sh600519.source-rows.json"
    reads = []
    original_read_bytes = Path.read_bytes

    def tracked_read_bytes(path):
        if path.name.endswith(".source-rows.json"):
            reads.append(path.resolve())
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", tracked_read_bytes)
    options_endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/api/data/options")
    started = time.perf_counter()
    options = options_endpoint()
    options_cold_seconds = time.perf_counter() - started
    assert options["datasets"][0]["id"] == "daily_bar"
    started = time.perf_counter()
    options_warm = options_endpoint()
    options_warm_seconds = time.perf_counter() - started
    assert options_warm["datasets"][0]["id"] == "daily_bar"

    query_params = {"dataset": "daily_bar", "security": "sh600519", "start_date": "2026-10-09",
                    "end_date": "2026-10-09", "page_size": 1}
    data_app = create_app(config_root=tmp_path / "config", data_root=data)
    def frozen_context_scan_forbidden():
        raise AssertionError("date-bounded daily queries with indexed asset context must not scan task definitions")
    data_app.state.reader._task_documents = frozen_context_scan_forbidden
    data_endpoint = next(route.endpoint for route in data_app.routes if getattr(route, "path", "") == "/api/data")
    typed_params = {**query_params, "start_date": date(2026, 10, 9), "end_date": date(2026, 10, 9), "page": 1}
    started = time.perf_counter()
    query = data_endpoint(**typed_params)
    query_cold_seconds = time.perf_counter() - started
    assert query["total"] == 1 and query["items"][0]["values"]["2"] == "101.00"
    started = time.perf_counter()
    warm_query = data_endpoint(**typed_params)
    query_warm_seconds = time.perf_counter() - started
    assert warm_query["total"] == 1
    broad_params = {"dataset": "daily_bar", "security": "", "start_date": date(2026, 10, 9),
                    "end_date": date(2026, 10, 9), "provider": "", "asset_type": "", "adjustment": "",
                    "page": 1, "page_size": 1}
    started = time.perf_counter()
    broad_query = data_endpoint(**broad_params)
    broad_query_cold_seconds = time.perf_counter() - started
    assert broad_query["total"] == 23_852 and broad_query["items"][0]["symbol"] == "sh600519"
    started = time.perf_counter()
    broad_warm_query = data_endpoint(**broad_params)
    broad_query_warm_seconds = time.perf_counter() - started
    assert broad_warm_query["total"] == 23_852
    record_property("source_options_cold_seconds_23852_reports", round(options_cold_seconds, 3))
    record_property("source_options_warm_seconds_23852_reports", round(options_warm_seconds, 3))
    record_property("source_query_cold_seconds_23852_reports", round(query_cold_seconds, 3))
    record_property("source_query_warm_seconds_23852_reports", round(query_warm_seconds, 3))
    record_property("source_default_date_cold_seconds_23852_selectors", round(broad_query_cold_seconds, 3))
    record_property("source_default_date_warm_seconds_23852_selectors", round(broad_query_warm_seconds, 3))
    assert options_cold_seconds < 5.0
    assert options_warm_seconds < 2.5
    assert query_cold_seconds < 5.0
    assert query_warm_seconds < 2.5
    assert broad_query_cold_seconds < 5.0
    assert broad_query_warm_seconds < 2.5
    assert reads == [selected_source.resolve()] * 4


def test_raw_token_cannot_escape_managed_root_and_app_is_read_only(tmp_path):
    reader, app, data, _ = web_fixture(tmp_path)
    before = sorted((path.relative_to(data), path.read_bytes()) for path in data.rglob("*") if path.is_file())

    with pytest.raises(HTTPException) as invalid:
        reader.raw_detail("Li4vLi4vQUdFTlRTLm1k", preview_bytes=1024)
    api_methods = {
        method
        for route in app.routes
        if (getattr(route, "path", "").startswith("/api/")
            and route.path != "/api/tasks/run"
            and not route.path.endswith("/resume"))
        for method in (getattr(route, "methods", set()) or set())
    }
    after = sorted((path.relative_to(data), path.read_bytes()) for path in data.rglob("*") if path.is_file())

    assert invalid.value.status_code == 404
    assert api_methods == {"GET"}
    assert before == after


def test_http_read_only_routes_headers_filters_and_frozen_parameters(tmp_path):
    _, app, data, _ = web_fixture(tmp_path)
    sqlite_coordination_files = {
        Path("metadata/metadata.duckdb-shm"),
        Path("metadata/metadata.duckdb-wal"),
    }
    before = {path.relative_to(data): path.read_bytes() for path in data.rglob("*") if path.is_file()}
    with TestClient(app) as client:
        assert client.get("/").status_code == 200
        assert client.get("/static/app.js").status_code == 200
        response = client.get("/api/tasks?status=stale&page_size=1")
        assert response.status_code == 200 and response.json()["total"] == 1
        assert response.headers["cache-control"] == "no-store"
        assert "script-src 'self'" in response.headers["content-security-policy"]
        assert client.get("/api/tasks?q=does-not-exist").json()["total"] == 0
        publication = client.get("/api/publications?dataset=daily_bar&asset_type=stock&partition_key=2026-10-09")
        assert publication.status_code == 200 and publication.json()["total"] == 1
        assert publication.json()["items"][0]["latest_task_id"] == "daily-2026-10-09"
        assert "evidence" not in publication.json()["items"][0]
        assert client.get("/api/publications?dataset=does_not_exist").json()["total"] == 0
        # The retired query parameter is ignored; it cannot expand source manifests.
        assert "evidence" not in client.get("/api/publications?include_evidence=true").json()["items"][0]
        assert client.get("/api/publications?page_size=101").status_code == 422
        task = client.get("/api/tasks/daily-2026-10-09").json()
        assert task["definition"]["trade_date"] == "2026-10-09"
        assert task["row_count"] == 1 and task["published_hash"] == "frozen-hash"
        assert task["requested_date"] == "2026-10-09" and task["data_date"] is None
        assert client.get("/api/tasks/daily-2026-10-09/units?status=failed").json()["total"] == 1
        assert client.get("/api/tasks?page_size=101").status_code == 422
        assert client.get("/api/tasks/missing").status_code == 404
        assert client.post("/api/tasks").status_code == 405
        listing = client.get("/api/raw").json()
        evidence_id = listing["items"][0]["id"]
        assert client.get(f"/api/raw/{evidence_id}").json()["preview"]["kind"] == "json"
        download = client.get(f"/api/raw/{evidence_id}/download")
        assert download.status_code == 200
        assert "attachment" in download.headers["content-disposition"]
    after = {path.relative_to(data): path.read_bytes() for path in data.rglob("*") if path.is_file()}
    business_before = {path: content for path, content in before.items() if path not in sqlite_coordination_files}
    business_after = {path: content for path, content in after.items() if path not in sqlite_coordination_files}
    assert business_before == business_after
    added = after.keys() - before.keys()
    assert added <= sqlite_coordination_files
    wal_path = Path("metadata/metadata.duckdb-wal")
    if wal_path in added:
        assert after[wal_path] == b""


def test_each_response_in_manifest_is_visible_and_task_links_exact_evidence(tmp_path):
    reader, _, data, body = web_fixture(tmp_path)
    manifest = data / "raw/2026-10-09/tencent/kline_daily/manifest.sh600519.ndjson"
    events = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
    failure = {"event": "http_response", "provider": "tencent", "endpoint": "kline_daily",
               "fetched_at_utc": "2026-10-09T07:59:00+00:00", "outcome": "transport_error",
               "error": "timeout", "url": "https://example.test/kline"}
    manifest.write_text("".join(json.dumps(item) + "\n" for item in [failure, *events]), encoding="utf-8")
    from stock_data_manage.storage.raw import RawObjectStore
    RawObjectStore.refresh_index(manifest)
    summary_path = data / "task_workspace/2026-10-09/daily_bar/summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["source_units"][0]["raw_manifest"] = str(manifest)
    write_json(summary_path, summary)
    listing = reader.raw_records(data_date=None, provider=None, endpoint=None, query=None, page=1, page_size=25)
    assert listing["total"] == 2
    linked = reader.task_units(summary["task_id"], page_size=1)["items"][0]["evidence"]
    assert len(linked) == 2
    assert reader.raw_detail(linked[0]["id"], preview_bytes=1024)["outcome"] == "transport_error"
    assert reader.raw_detail(linked[0]["id"], preview_bytes=1024)["preview"]["kind"] == "unavailable"
    assert reader.raw_body(linked[1]["id"])[0].read_bytes() == body


def test_download_rejects_changed_body_and_scope_traversal(tmp_path):
    reader, app, data, _ = web_fixture(tmp_path)
    listing = reader.raw_records(data_date=None, provider=None, endpoint=None, query=None, page=1, page_size=25)
    evidence_id = listing["items"][0]["id"]
    body_path, _, _ = reader.raw_body(evidence_id)
    body_path.write_bytes(b"changed")
    with TestClient(app) as client:
        assert client.get(f"/api/raw/{evidence_id}/download").status_code == 409
        assert client.get("/api/raw?provider=../../metadata").status_code == 404
        assert client.get("/api/raw/sources?data_date=../../metadata").status_code == 404
        assert client.get("/api/raw/Li4vQUdFTlRTLm1k.0").status_code == 404


def test_empty_root_and_unterminated_multibyte_preview(tmp_path):
    reader, _, data, _ = web_fixture(tmp_path)
    evidence = reader.raw_records(data_date=None, provider=None, endpoint=None, query=None, page=1, page_size=1)["items"][0]
    body_path, _, _ = reader.raw_body(evidence["id"])
    body_path.write_bytes(("你好" * 1000).encode("utf-8"))
    preview = reader.raw_detail(evidence["id"], preview_bytes=1024)["preview"]
    assert preview["kind"] == "text" and preview["truncated"]
    assert preview["text"].startswith("你好")
    empty_app = create_app(config_root=tmp_path / "config", data_root=tmp_path / "empty-runtime")
    with TestClient(empty_app) as client:
        assert client.get("/api/overview").status_code == 503
        assert client.get("/api/raw").json()["total"] == 0
        assert client.get("/api/publications").status_code == 503
    assert not (tmp_path / "empty-runtime").exists()


def test_sdk_payload_is_not_labeled_as_http_or_transport_bytes(tmp_path):
    reader, _, data, _ = web_fixture(tmp_path)
    directory = data / "raw/2026-10-09/baostock/query_all_stock"
    (directory / "bodies").mkdir(parents=True)
    payload = b'{"fields":["code"],"rows":[["sh.600519"]]}'
    digest = hashlib.sha256(payload).hexdigest()
    (directory / "bodies/sdk.bin").write_bytes(payload)
    event = {"event": "source_payload", "provider": "baostock", "endpoint": "query_all_stock",
             "sdk_status_code": "0", "outcome": "response", "payload_bytes": len(payload),
             "body_storage": "bodies/sdk.bin", "body_sha256": digest,
             "representation": "SDK-decoded fields and rows", "original_transport_bytes_available": False}
    (directory / "manifest.sdk.ndjson").write_text(json.dumps(event) + "\n", encoding="utf-8")
    from stock_data_manage.storage.raw import RawObjectStore
    RawObjectStore.refresh_index(directory / "manifest.sdk.ndjson")
    listing = reader.raw_records(data_date=None, provider="baostock", endpoint=None, query=None, page=1, page_size=25)
    assert listing["total"] == 1
    item = reader.raw_detail(listing["items"][0]["id"], preview_bytes=1024)
    assert item["event_kind"] == "source_payload" and item["status_code"] is None
    assert item["sdk_status_code"] == "0" and item["body_bytes"] == len(payload)
    assert item["response"]["original_transport_bytes_available"] is False
    assert reader.raw_body(item["id"])[0].read_bytes() == payload


def source_query_fixture(tmp_path):
    reader, app, data, _ = web_fixture(tmp_path)
    config = tmp_path / "config"
    (config / "normalization").mkdir()
    (config / "normalization/daily_bar.yaml").write_text(
        "rules:\n  - input_id: DAILY-TEST\n    version: test-v1\n"
        "    field_mapping: {trade_date: '0', close: '2', volume: '5'}\n", encoding="utf-8")
    import pyarrow as arrow
    import pyarrow.parquet as parquet
    master = data / "canonical/2026-10-09/security_master/data.parquet"
    master.parent.mkdir(parents=True)
    parquet.write_table(arrow.Table.from_pylist([
        {"instrument_id":"XSHG:600519", "symbol": "600519", "name": "贵州茅台", "asset_type": "stock"},
        {"symbol": "sh000001", "name": "上证指数", "asset_type": "index"},
        {"symbol": "sz000001", "name": "平安银行", "asset_type": "stock"},
    ]), master)
    from stock_data_manage.domain import AssetType, Exchange
    from stock_data_manage.service.instruments import SecurityMasterStore, SecurityRecord
    from stock_data_manage.storage.integrity import Manifest
    legacy_manifest = Manifest.from_file(master, dataset="security_master", asset_type="security",
        partition_key="2026-10-09", row_count=3, first_key=None, last_key=None,
        publication_metadata={"as_of_date": "2026-10-09", "coverage_passed": True})
    legacy_manifest.write_atomic(master.parent / "manifest.json")
    current_dir = data / "canonical/security_master/current"
    current_dir.mkdir(parents=True)
    current_store_path = current_dir / "security_master.duckdb"
    with SecurityMasterStore(str(current_store_path)) as store:
        store.upsert((
            SecurityRecord("XSHG:600519", "600519", Exchange.XSHG, AssetType.STOCK,
                name="贵州茅台", evidence_sources=("fixture",)),
            SecurityRecord("XSHG:000001", "000001", Exchange.XSHG, AssetType.INDEX,
                name="上证指数", evidence_sources=("fixture",)),
            SecurityRecord("XSHE:000001", "000001", Exchange.XSHE, AssetType.STOCK,
                name="平安银行", evidence_sources=("fixture",)),
        ))
    current_manifest = Manifest.from_file(current_store_path, dataset="security_master", asset_type="security",
        partition_key="2026-10-10", row_count=3, first_key=None, last_key=None,
        publication_metadata={"as_of_date": "2026-10-10", "coverage_passed": True})
    current_manifest.write_atomic(current_dir / "manifest.json")

    def add(symbol, rows, *, dataset="daily_bar", parameters=None):
        directory = data / f"task_workspace/2026-10-09/{dataset}/sources/tencent/kline_daily"
        directory.mkdir(parents=True, exist_ok=True)
        source = directory / f"{symbol}.source-rows.json"
        write_json(source, rows)
        descriptor = {"path": f"sources/tencent/kline_daily/{source.name}",
                      "sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "row_count": len(rows)}
        report = {"dataset": dataset, "input_id": "DAILY-TEST", "provider": "tencent", "endpoint": "kline_daily",
                  "data_date": "2026-10-09", "parameters": parameters if parameters is not None else {"code": symbol,"adjust":"qfq"},
                  "source_rows": descriptor, "normalization_version": "test-v1", "code_version": "fixture",
                  "raw_manifest": {"path": str(data / "raw/2026-10-09/tencent/kline_daily/manifest.sh600519.ndjson")}}
        report_path = directory / f"{symbol}.report.json"
        write_json(report_path, report)
        from stock_data_manage.storage.read_index import SOURCE_QUERY_DATABASE, append_source_report
        append_source_report(report_path, run_root=directory.parents[2], config_root=config,
                             query_db_path=data / SOURCE_QUERY_DATABASE,
                             field_mapping={"trade_date": "0", "close": "2", "volume": "5"})
        return source
    add("sh600519", [{"0":day,"2":price,"5":"7061.00","symbol":"sh600519"}
                     for day,price in [("2026-10-08","100.00"),("2026-10-09","101.00"),("2026-10-10","102.00")]])
    return reader, app, data, add


def test_security_catalog_and_publications_use_single_current_master(tmp_path):
    reader, _, _, _ = source_query_fixture(tmp_path)
    catalog = reader._security_catalog()
    assert catalog[("sh", "600519")] == {
        "name": "贵州茅台", "asset_type": "stock", "name_source_date": "2026-10-10"}
    publications = reader.publications(dataset="security_master")
    assert publications["total"] == 0
    assert publications["security_master_status"] == "not_registered"


def test_record_query_filters_business_date_name_and_keeps_source_values(tmp_path):
    _, app, _, _ = source_query_fixture(tmp_path)
    with TestClient(app) as client:
        options = client.get("/api/data/options").json()
        assert [item["id"] for item in options["datasets"]] == ["daily_bar"]
        query = {"dataset":"daily_bar","security":"贵州茅台","start_date":"2026-10-09",
                 "end_date":"2026-10-10","asset_type":"stock","adjustment":"qfq","page_size":1}
        first = client.get("/api/data", params=query).json()
        assert first["total"] == 2
        row = first["items"][0]
        assert row["record_date"] == "2026-10-10"  # Not the workspace date or capture time.
        assert row["values"]["2"] == "102.00" and row["values"]["5"] == "7061.00"
        assert row["symbol"] == "sh600519" and row["evidence"]
        second = client.get("/api/data", params={**query,"page":2}).json()
        assert second["items"][0]["record_date"] == "2026-10-09"
        assert client.get("/api/data",params={**query,"provider":"missing"}).json()["total"] == 0
        assert client.get("/api/data",params={**query,"adjustment":"hfq"}).json()["total"] == 0


def test_source_query_rebuild_keeps_retained_workspace_audit_reports(tmp_path):
    import shutil
    reader, _, data, _ = source_query_fixture(tmp_path)
    source_dir = data / "task_workspace/2026-10-09/daily_bar/sources/tencent/kline_daily"
    report_root = data / "task_archive/_workspace_audit/retained-daily"
    report_dir = report_root / "sources/tencent/kline_daily"
    report_dir.mkdir(parents=True)
    for name in ("sh600519.source-rows.json", "sh600519.report.json"):
        shutil.copy2(source_dir / name, report_dir / name)

    from stock_data_manage.storage.read_index import SOURCE_QUERY_DATABASE, rebuild_source_query_indexes
    rebuilt = rebuild_source_query_indexes(workspace_root=data / "task_workspace",
        archive_root=data / "task_archive", config_root=tmp_path / "config",
        query_db_path=data / SOURCE_QUERY_DATABASE)
    assert rebuilt == {"indexed_reports": 2, "index_databases": 1}
    indexed_paths = {str(path) for path, _ in reader._query_reports()}
    assert str(report_dir / "sh600519.report.json") in indexed_paths
    # The archived copy is retained evidence of the same report, not a second result.
    assert reader.data_records(dataset="daily_bar", security="sh600519")["total"] == 3


def test_record_query_distinguishes_exchanges_and_does_not_use_capture_date(tmp_path):
    reader, _, _, add = source_query_fixture(tmp_path)
    add("sh000001", [{"date":"2026-10-09","symbol":"sh000001","close":"3000"}])
    add("sz000001", [{"date":"2026-10-09","symbol":"sz000001","close":"12"}])
    assert reader.data_records(dataset="daily_bar",security="XSHG:000001")["total"] == 1
    assert reader.data_records(dataset="daily_bar",security="000001")["total"] == 2
    assert reader.data_records(dataset="daily_bar",security="贵州茅台")["total"] == 3
    add("sh600000", [{"symbol":"sh600000","close":"20"}])
    result = reader.data_records(dataset="daily_bar",security="sh600000",start_date=date(2026,10,9))
    assert result["total"] == 0 and result["excluded_unknown_date_count"] == 1


def test_source_type_uses_frozen_task_without_catalog_or_prefix_guess(tmp_path):
    reader, _, data, add = source_query_fixture(tmp_path)
    source = add("bj920001", [{"0": "2026-10-09", "2": "9.59", "5": "11829.00", "symbol": "bj920001"}])
    summary_path = data / "task_workspace/2026-10-09/daily_bar/summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["source_units"] = [{"key": "bj920001", "report_path": str(source.with_name("bj920001.report.json"))}]
    write_json(summary_path, summary)
    result = reader.data_records(dataset="daily_bar", security="bj920001", asset_type="stock")
    assert result["total"] == 1
    assert result["items"][0]["asset_type"] == "stock"
    assert result["items"][0]["values"]["5"] == "11829.00"
    # The same symbol must not be guessed as stock after its explicit context changes.
    write_json(summary_path.with_name("task.json"), {"asset_type": "etf", "units": []})
    assert reader.data_records(dataset="daily_bar", security="bj920001", asset_type="stock")["total"] == 0
    assert reader.data_records(dataset="daily_bar", security="bj920001", asset_type="etf")["total"] == 1


@pytest.mark.parametrize("damage", ["duplicate", "field", "format", "path", "missing_path", "decimals", "source_values"])
def test_presentation_rejects_invalid_contract(tmp_path, damage):
    import yaml
    from stock_data_manage.service.web import _presentation
    _, _, _, _ = source_query_fixture(tmp_path)
    config = tmp_path / "config"
    path = config / "presentation.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if damage == "duplicate": document["views"]["daily"]["columns"].append("symbol")
    if damage == "field": document["views"]["daily"]["columns"].append("secret")
    if damage == "format": document["fields"]["open"]["format"] = "eval"
    if damage == "path": document["views"]["daily"]["values"]["open"] = "__class__"
    if damage == "missing_path": document["views"]["daily"]["values"].pop("open")
    if damage == "decimals": document["fields"]["open"]["min_decimals"] = -1
    if damage == "source_values": document["views"]["daily_source"]["preserve_source_values"] = False
    path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
    with pytest.raises(HTTPException, match="展示配置错误"):
        _presentation(config, "daily")
    if damage == "format":
        document["fields"]["open"]["format"] = []
        path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
        with pytest.raises(HTTPException, match="展示配置错误"):
            _presentation(config, "daily")


def test_source_projection_preserves_values_and_uses_config_order(tmp_path):
    import yaml
    reader, _, data, add = source_query_fixture(tmp_path)
    source = add("sh600519", [{"0":"2026-10-09","2":"10.150","5":"0.00","9":"unchanged","symbol":"sh600519"}])
    report_path = source.with_name("sh600519.report.json")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["applied_unit_contract"] = {"source_units": {"volume":"lot"}}
    write_json(report_path, report)
    from stock_data_manage.storage.read_index import SOURCE_QUERY_DATABASE, append_source_report
    append_source_report(report_path, run_root=report_path.parents[3], config_root=tmp_path / "config",
                         query_db_path=data / SOURCE_QUERY_DATABASE,
                         field_mapping={"trade_date": "0", "close": "2", "volume": "5"})
    result = reader.data_records(dataset="daily_bar")
    assert len(result["columns"]) == 12
    row = result["items"][0]
    assert row["values"]["9"] == "unchanged" and row["display_values"]["close"] == "10.150"
    assert row["source_field_labels"]["9"] == "来源字段9（含义未核验）"
    assert row["display_values"]["volume"] == "0.00" and row["field_metadata"]["volume"]["unit"] == "lot"
    assert row["field_metadata"]["amount"]["status"] == "unverified"
    assert row["name_source_date"] == "2026-10-10"
    config_path = tmp_path / "config/presentation.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["views"]["daily_source"]["columns"] = ["close", "symbol"]
    config_path.write_text(yaml.safe_dump(config, allow_unicode=True), encoding="utf-8")
    assert [column["key"] for column in reader.data_records(dataset="daily_bar")["columns"]] == ["close", "symbol"]


def test_published_optional_field_status_is_not_guessed_from_market():
    from stock_data_manage.service.web import _daily_field_metadata
    row = {"asset_type":"stock","quote_unit":"CNY","values":dict(open="10",high="10",low="10",close="10",volume=None,amount="0"),"flags":[]}
    assert _daily_field_metadata(row)["volume"]["status"] == "missing"
    assert _daily_field_metadata(row)["amount"]["status"] == "available"
    row["flags"] = ["unit_unverified:volume"]
    assert _daily_field_metadata(row)["volume"]["reason"] == "单位未核验"


def test_presentation_edit_does_not_change_frozen_daily_contract(tmp_path):
    import shutil
    import yaml
    from stock_data_manage.pipeline.daily import resolve_daily_definition
    config = tmp_path / "config"
    shutil.copytree(Path(__file__).resolve().parents[1] / "config", config)
    before = resolve_daily_definition({"task":"stock_daily"}, config)
    path = config / "presentation.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["views"]["daily"]["columns"] = ["close", "symbol"]
    path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
    after = resolve_daily_definition({"task":"stock_daily"}, config)
    assert after == before


def test_source_query_rejects_conflicting_asset_metadata(tmp_path):
    reader, _, data, add = source_query_fixture(tmp_path)
    add("sh600519", [{"0":"2026-10-09", "2":"10", "asset_type":"etf", "symbol":"sh600519"}])
    with pytest.raises(HTTPException, match="证券类型上下文冲突"):
        reader.data_records(dataset="daily_bar")


def test_record_query_snapshot_date_is_explicit_and_integrity_failure_visible(tmp_path):
    reader, app, _, add = source_query_fixture(tmp_path)
    add("snapshot", [{"code":"sz.000001","code_name":"平安银行"}], dataset="security_snapshot",
        parameters={"trade_date":"2026-10-09"})
    result = reader.data_records(dataset="security_snapshot",security="000001",start_date=date(2026,10,9))
    assert result["total"] == 1
    assert result["items"][0]["date_basis"].startswith("请求快照日期")
    source = add("sh600001", [{"date":"2026-10-09","code":"sh600001"}])
    source.write_bytes(b"[]")
    with TestClient(app) as client:
        result = client.get("/api/data?dataset=daily_bar&security=sh600001").json()
        assert result["total"] == 0 and "哈希" in result["issues"][0]["reason"]
        assert client.get("/api/data?dataset=daily_bar&start_date=2026-10-10&end_date=2026-10-09").status_code == 422
        assert client.get("/api/data?dataset=daily_bar&start_date=bad").status_code == 422
        all_rows = client.get("/api/data?dataset=daily_bar").json()
        assert all_rows["total"] == 4 and len(all_rows["items"]) == 3
        assert "total_basis" in all_rows and all_rows["issues"]
        assert client.get("/api/data?dataset=../../metadata").status_code == 404


def test_record_query_retains_historical_tencent_date_without_new_unit_mapping(tmp_path):
    reader, _, data, add = source_query_fixture(tmp_path)
    source = add("sh600002", [{"0":"2026-10-09","2":"12.000","5":"77.00","symbol":"sh600002"}])
    report_path = source.with_name("sh600002.report.json")
    report = json.loads(report_path.read_bytes())
    report["normalization_version"] = "tencent-window-input-v1"
    write_json(report_path, report)
    from stock_data_manage.storage.read_index import SOURCE_QUERY_DATABASE, append_source_report
    append_source_report(report_path, run_root=report_path.parents[3], config_root=tmp_path / "config",
                         query_db_path=data / SOURCE_QUERY_DATABASE,
                         field_mapping={})
    result = reader.data_records(dataset="daily_bar",security="sh600002",start_date=date(2026,10,9))
    assert result["total"] == 1
    row = result["items"][0]
    assert row["record_date"] == "2026-10-09" and "字段0" in row["date_basis"]
    assert row["values"]["2"] == "12.000" and row["values"]["5"] == "77.00"


def test_record_query_is_read_only_and_sees_changed_source_report(tmp_path):
    reader, app, data, add = source_query_fixture(tmp_path)
    before = {path.relative_to(data):path.read_bytes() for path in data.rglob("*") if path.is_file()}
    with TestClient(app) as client:
        client.get("/api/data/options")
        response = client.get("/api/data?dataset=daily_bar&security=600519&start_date=2026-10-09&end_date=2026-10-09")
        assert response.json()["total"] == 1
        assert client.post("/api/data").status_code == 405
    after = {path.relative_to(data):path.read_bytes() for path in data.rglob("*") if path.is_file()}
    assert before == after
    add("sh600519", [{"0":"2026-10-09","2":"999.00","symbol":"sh600519"}])
    result = reader.data_records(dataset="daily_bar",security="600519")
    assert result["total"] == 1 and result["items"][0]["values"]["2"] == "999.00"


def _task_api_runtime(tmp_path, calendar_days=(date(2026, 10, 28), date(2026, 10, 29))):
    import shutil
    from stock_data_manage.config.loader import load_storage_paths
    from stock_data_manage.service.calendar import CalendarDay, TradingCalendarStore
    from stock_data_manage.storage.metadata import MetadataStore

    config = tmp_path / "config"
    shutil.copytree(Path(__file__).resolve().parents[1] / "config", config)
    data = tmp_path / "runtime"
    metadata_path = load_storage_paths(config, data_root=data)["metadata_path"]
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with MetadataStore(metadata_path):
        pass
    with TradingCalendarStore(str(metadata_path)) as calendar:
        calendar.upsert(CalendarDay(day, True, "offline-test-calendar", 100) for day in calendar_days)
    return config, data, metadata_path


def _wait_for_web_task_status(metadata_path, task_id, expected, timeout=5.0):
    from stock_data_manage.storage.metadata import MetadataStore

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with MetadataStore(metadata_path, read_only=True) as metadata:
            state = metadata.load_collection_task(task_id)
        if state and state.get("status") == expected:
            return state
        time.sleep(0.01)
    raise AssertionError(f"task {task_id} did not reach {expected!r} within {timeout}s")


def test_task_options_and_unconfirmed_calendar_have_no_runtime_side_effect(tmp_path):
    import shutil

    config = tmp_path / "config"
    shutil.copytree(Path(__file__).resolve().parents[1] / "config", config)
    runtime = tmp_path / "runtime"
    with TestClient(create_app(config_root=config, data_root=runtime),
                    base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client:
        options = client.get("/api/task-options")
        assert options.status_code == 200
        assert {item["dataset"] for item in options.json()["items"] if item["configured"]} == {
            "daily_bar", "security_master"}
        assert options.json()["manual_run_enabled"] is True
        assert options.json()["scheduler_active"] is False
        response = client.post("/api/tasks/run", json={"dataset": "security_master", "trade_date": "2026-10-29"})
        assert response.status_code == 409
        assert "交易日历" in response.json()["detail"]
    assert not runtime.exists()


def test_task_routes_reject_remote_invalid_and_unknown_requests_without_side_effect(tmp_path):
    import shutil

    config = tmp_path / "config"
    shutil.copytree(Path(__file__).resolve().parents[1] / "config", config)
    runtime = tmp_path / "runtime"
    app = create_app(config_root=config, data_root=runtime)
    with TestClient(app, base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client:
        cross_origin = client.post("/api/tasks/run", json={
            "dataset": "security_master", "trade_date": "2026-10-28"},
            headers={"origin": "http://untrusted.example"})
        assert cross_origin.status_code == 403
        wrong_media_type = client.post("/api/tasks/run", content="{}",
            headers={"content-type": "text/plain"})
        assert wrong_media_type.status_code == 415
        unsupported = client.post("/api/tasks/run", json={"dataset": "minute_bar", "trade_date": "2026-10-28"})
        assert unsupported.status_code == 400
        invalid_date = client.post("/api/tasks/run", json={"dataset": "security_master", "trade_date": "not-a-date"})
        assert invalid_date.status_code == 400
        missing_resume = client.post("/api/tasks/not-present/resume", json={})
        assert missing_resume.status_code == 404
    with TestClient(app, base_url="http://127.0.0.1", client=("192.0.2.10", 5000)) as remote_client:
        remote = remote_client.post("/api/tasks/run", json={
            "dataset": "security_master", "trade_date": "2026-10-28"})
        assert remote.status_code == 403
    assert not runtime.exists()


@pytest.mark.parametrize(
    ("task_status", "outside_workspace", "expected_detail"),
    [("published", False, "已发布"), ("failed", True, "归档")],
)
def test_web_resume_rejects_published_and_archived_tasks(
        tmp_path, task_status, outside_workspace, expected_detail):
    from stock_data_manage.storage.metadata import MetadataStore

    config, data, metadata_path = _task_api_runtime(tmp_path)
    task_id = "security-master-20261028" if not outside_workspace else "security-master-20261029"
    if outside_workspace:
        definition_path = tmp_path / "archive" / task_id / "task.json"
    else:
        definition_path = data / "task_workspace" / "2026-10-28" / "security_master" / task_id / "task.json"
    with MetadataStore(metadata_path) as metadata:
        metadata.save_collection_task({
            "task_id": task_id, "dataset": "security_master", "definition_hash": "frozen-hash",
            "definition_ref": {"path": str(definition_path), "sha256": "frozen-file-hash"},
            "status": task_status, "verification_mode": "live",
        })
    app = create_app(config_root=config, data_root=data)
    with TestClient(app, base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client:
        response = client.post(f"/api/tasks/{task_id}/resume", json={})
    assert response.status_code == 409
    assert expected_detail in response.json()["detail"]


def test_web_task_submissions_are_durable_and_worker_consumes_fifo(tmp_path, monkeypatch):
    from stock_data_manage.pipeline import inputs
    from stock_data_manage.storage.metadata import MetadataStore

    calendar_days = []
    candidate = date(2026, 10, 28)
    while len(calendar_days) < 11:
        if candidate.weekday() < 5:
            calendar_days.append(candidate)
        candidate = date.fromordinal(candidate.toordinal() + 1)
    config, data, metadata_path = _task_api_runtime(tmp_path, calendar_days=tuple(calendar_days))
    task_ids = ["security-master-" + day.strftime("%Y%m%d") for day in calendar_days]
    first_id = task_ids[0]
    first_started, release_first = Event(), Event()
    executed, executed_lock = [], Lock()
    worker_starts = []
    enqueue_seconds = []

    actual_worker = inputs.run_collection_worker

    def counted_worker(**kwargs):
        worker_starts.append(True)
        return actual_worker(**kwargs)

    def offline_execute(*, definition, config_root, data_root=None, **_kwargs):
        task_id = definition["task_id"]
        with executed_lock:
            executed.append(task_id)
        if task_id == first_id:
            first_started.set()
            if not release_first.wait(30):
                raise TimeoutError("FIFO fixture did not release the first task")
        paths = __import__("stock_data_manage.config.loader", fromlist=["load_storage_paths"]).load_storage_paths(
            config_root, data_root=data_root)
        with MetadataStore(paths["metadata_path"]) as metadata:
            state = metadata.load_collection_task(task_id)
            state.update(status="published", phase="finished", error=None,
                         updated_at=datetime.now(timezone.utc).isoformat())
            metadata.save_collection_task(state)
        return state

    monkeypatch.setattr(inputs, "_execute_task_body", offline_execute)
    monkeypatch.setattr(inputs, "run_collection_worker", counted_worker)
    app = create_app(config_root=config, data_root=data)
    with TestClient(app, base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client:
        try:
            first_started_at = time.perf_counter()
            first = client.post("/api/tasks/run", json={
                "dataset": "security_master", "trade_date": calendar_days[0].isoformat()})
            enqueue_seconds.append(time.perf_counter() - first_started_at)
            assert first.status_code == 202, first.text
            assert first.json()["status"] == "waiting"
            assert first.json()["queue_order"] == 1
            assert first_started.wait(5), "the application-lifetime worker did not claim the first task"

            for queue_order, (day, task_id) in enumerate(zip(calendar_days[1:], task_ids[1:]), start=2):
                started_at = time.perf_counter()
                response = client.post("/api/tasks/run", json={
                    "dataset": "security_master", "trade_date": day.isoformat()})
                enqueue_seconds.append(time.perf_counter() - started_at)
                assert response.status_code == 202, response.text
                assert response.json()["status"] == "waiting"
                assert response.json()["queue_order"] == queue_order

            repeated = client.post("/api/tasks/run", json={
                "dataset": "security_master", "trade_date": calendar_days[-1].isoformat()})
            assert repeated.status_code == 202, repeated.text
            assert repeated.json()["already_queued"] is True
            assert repeated.json()["resumed"] is False
            assert repeated.json()["queue_order"] == 11
            resumed_waiter = client.post(f"/api/tasks/{task_ids[-1]}/resume", json={})
            assert resumed_waiter.status_code == 202, resumed_waiter.text
            assert resumed_waiter.json()["already_queued"] is True
            assert resumed_waiter.json()["queue_order"] == 11
            assert len(worker_starts) == 1
            with MetadataStore(metadata_path, read_only=True) as metadata:
                persisted = [metadata.load_collection_task(task_id) for task_id in task_ids[1:]]
            assert [state["status"] for state in persisted] == ["waiting"] * 10
            assert [state["queue_order"] for state in persisted] == list(range(2, 12))
        finally:
            release_first.set()

        for task_id in task_ids:
            _wait_for_web_task_status(metadata_path, task_id, "published")
        assert executed == task_ids
    assert len(worker_starts) == 1
    assert max(enqueue_seconds) < 5.0


def _publish_web_test_security_master(config, data_root):
    from stock_data_manage.config.loader import load_storage_paths
    from stock_data_manage.domain import AssetType, Exchange
    from stock_data_manage.service.instruments import SecurityMasterStore, SecurityRecord
    from stock_data_manage.storage.integrity import Manifest

    records = []

    def add_records(exchange, asset_type, start, count):
        for index in range(count):
            symbol = f"{start + index:06d}"
            records.append(SecurityRecord(
                instrument_id=f"{exchange.value}:{symbol}", symbol=symbol, exchange=exchange,
                asset_type=asset_type, name=f"离线证券-{symbol}"))

    add_records(Exchange.XSHG, AssetType.STOCK, 600000, 2787)
    add_records(Exchange.XSHE, AssetType.STOCK, 1, 2786)
    add_records(Exchange.XSHG, AssetType.ETF, 510000, 1694)
    add_records(Exchange.XSHG, AssetType.INDEX, 1, 595)
    directory = load_storage_paths(config, data_root=data_root)["canonical_root"] / "security_master" / "current"
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / "security_master.duckdb"
    with SecurityMasterStore(str(database)) as store:
        store.publish(records)
        actual_count = store.count()
    identities = sorted(record.instrument_id for record in records)
    manifest = Manifest.from_file(database, dataset="security_master", asset_type="security",
        partition_key="2026-10-27", row_count=actual_count, expected_count=actual_count,
        first_key=identities[0], last_key=identities[-1],
        publication_metadata={"as_of_date": "2026-10-27", "required_groups": ["offline-fixture"]})
    manifest.write_atomic(directory / "manifest.json")
    return actual_count


def _seed_web_task_history(metadata_path, *, count=10000):
    from stock_data_manage.storage.metadata import MetadataStore

    now = datetime.now(timezone.utc)
    updated_at = now.isoformat()
    rows = []
    for index in range(count):
        task_id = f"history-{index:05d}"
        state = json.dumps({"task_id": task_id, "dataset": "daily_bar", "status": "published",
                            "updated_at_utc": updated_at}, ensure_ascii=False)
        rows.append((task_id, f"definition-{index:05d}", "daily_bar", "published", state, now))
    with MetadataStore(metadata_path) as metadata:
        metadata.connection.execute("BEGIN IMMEDIATE")
        metadata.connection.executemany(
            "INSERT INTO collection_task (task_id,definition_hash,dataset,status,state_json,updated_at) "
            "VALUES (?,?,?,?,?,?)", rows)
        metadata.connection.execute("COMMIT")


def test_web_task_options_and_daily_admission_meet_documented_budgets(tmp_path, monkeypatch):
    import requests
    from stock_data_manage.pipeline import inputs
    from stock_data_manage.storage.metadata import MetadataStore

    trade_day = date(2026, 10, 28)
    config, data, metadata_path = _task_api_runtime(tmp_path, calendar_days=(trade_day,))
    master_count = _publish_web_test_security_master(config, data)
    historical_count = 10000
    _seed_web_task_history(metadata_path, count=historical_count)

    worker_started = Event()
    worker_starts = []

    def idle_worker(*, stop_event, **_kwargs):
        worker_starts.append(True)
        worker_started.set()
        stop_event.wait()

    def deny_provider_request(*_args, **_kwargs):
        raise AssertionError("Web task admission must not perform a Provider request")

    monkeypatch.setattr(inputs, "run_collection_worker", idle_worker)
    monkeypatch.setattr(requests.Session, "send", deny_provider_request)
    app = create_app(config_root=config, data_root=data)
    with TestClient(app, base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client:
        assert worker_started.wait(2), "the writable app did not start its single queue worker"
        option_times = []
        for _ in range(6):
            started_at = time.perf_counter()
            response = client.get("/api/task-options")
            option_times.append(time.perf_counter() - started_at)
            assert response.status_code == 200
            assert response.json()["manual_run_enabled"] is True
        first_options, warm_options = option_times[0], option_times[1:]
        assert first_options <= 2.0
        assert max(warm_options) <= 1.0

        run_started = time.perf_counter()
        accepted = client.post("/api/tasks/run", json={
            "dataset": "daily_bar", "trade_date": trade_day.isoformat()})
        run_seconds = time.perf_counter() - run_started
        assert accepted.status_code == 202, accepted.text
        assert accepted.json()["status"] == "waiting"
        assert accepted.json()["queue_order"] == 1
        task_id = accepted.json()["task_id"]
        with MetadataStore(metadata_path, read_only=True) as metadata:
            admitted = metadata.load_collection_task(task_id)
        assert admitted["status"] == "waiting"
        frozen_hash = admitted["definition_hash"]
        frozen_reference = dict(admitted["definition_ref"])
        from stock_data_manage.storage.integrity import file_hash, row_hash
        frozen_path = Path(frozen_reference["path"])
        frozen_document = json.loads(frozen_path.read_text(encoding="utf-8"))
        assert row_hash(frozen_document) == frozen_hash
        assert file_hash(frozen_path) == frozen_reference["sha256"] == frozen_hash
        timing = {"task_options_first_seconds": round(first_options, 6),
                  "task_options_warm_max_seconds": round(max(warm_options), 6),
                  "daily_admission_http_seconds": round(run_seconds, 6),
                  "daily_resume_http_seconds": None,
                  "synthetic_security_master_rows": master_count,
                  "historical_task_rows": historical_count,
                  "provider_requests": 0}
        (tmp_path / "web-task-performance.json").write_text(
            json.dumps(timing, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        assert run_seconds <= 5.0

        with MetadataStore(metadata_path) as metadata:
            failed = metadata.load_collection_task(task_id)
            failed.update(status="failed", error="offline admission timing fixture")
            metadata.save_collection_task(failed)
        resume_started = time.perf_counter()
        resumed = client.post(f"/api/tasks/{task_id}/resume", json={})
        resume_seconds = time.perf_counter() - resume_started
        assert resumed.status_code == 202, resumed.text
        assert resumed.json()["status"] == "waiting"
        assert resumed.json()["resumed"] is True
        assert resume_seconds <= 5.0
        with MetadataStore(metadata_path, read_only=True) as metadata:
            requeued = metadata.load_collection_task(task_id)
            history_rows = metadata.connection.execute(
                "SELECT count(*) FROM collection_task WHERE task_id LIKE 'history-%'").fetchone()[0]
        assert requeued["status"] == "waiting"
        assert requeued["definition_hash"] == frozen_hash
        assert requeued["definition_ref"] == frozen_reference
        assert history_rows == historical_count
        assert len(worker_starts) == 1

    performance = {**timing, "daily_resume_http_seconds": round(resume_seconds, 6),
                   "historical_task_rows": history_rows}
    (tmp_path / "web-task-performance.json").write_text(
        json.dumps(performance, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(performance)


def test_read_only_web_instance_leaves_persisted_queue_waiting(tmp_path, monkeypatch):
    import os
    from stock_data_manage.pipeline.inputs import submit_collection_task
    from stock_data_manage.storage.metadata import MetadataStore
    from stock_data_manage.worker.scheduler import build_security_master_definition

    trade_day = date(2026, 10, 28)
    config, data, metadata_path = _task_api_runtime(tmp_path, calendar_days=(trade_day,))
    task_id = "security-master-" + trade_day.strftime("%Y%m%d")
    definition = build_security_master_definition(config, trade_date=trade_day,
        now=datetime(2026, 11, 11, 12, tzinfo=timezone.utc), is_trading_day=True, task_id=task_id)
    submit_collection_task(definition=definition, config_root=config, data_root=data, mode="live")
    real_access = os.access

    def read_only_root(path, mode):
        if Path(path).resolve() == data.resolve():
            return False
        return real_access(path, mode)

    monkeypatch.setattr("stock_data_manage.service.web.os.access", read_only_root)
    app = create_app(config_root=config, data_root=data)
    with TestClient(app, base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client:
        assert app.state.collection_worker_thread is None
        options = client.get("/api/task-options")
        assert options.status_code == 200
        assert options.json()["manual_run_enabled"] is False
        blocked = client.post("/api/tasks/run", json={
            "dataset": "security_master", "trade_date": trade_day.isoformat()})
        assert blocked.status_code == 409
        assert "不可写" in blocked.json()["detail"]
    with MetadataStore(metadata_path, read_only=True) as metadata:
        queued = metadata.load_collection_task(task_id)
    assert queued["status"] == "waiting"
    assert queued["queue_order"] == 1


@pytest.mark.parametrize("failure", ["scope_conflict", "sqlite_busy"])
def test_task_run_reports_admission_conflict_and_busy_without_persisting(
        tmp_path, monkeypatch, failure):
    import sqlite3
    from stock_data_manage.pipeline import inputs
    from stock_data_manage.storage.metadata import CollectionTaskScopeConflict, MetadataStore

    trade_day = date(2026, 10, 28)
    config, data, metadata_path = _task_api_runtime(tmp_path, calendar_days=(trade_day,))

    def fail_admission(**_kwargs):
        if failure == "scope_conflict":
            raise CollectionTaskScopeConflict("other-task", ({"trade_date": trade_day.isoformat()},))
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise error

    monkeypatch.setattr(inputs, "submit_collection_task", fail_admission)
    app = create_app(config_root=config, data_root=data)
    task_id = "security-master-" + trade_day.strftime("%Y%m%d")
    with TestClient(app, base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client:
        response = client.post("/api/tasks/run", json={
            "dataset": "security_master", "trade_date": trade_day.isoformat()})
    expected = 409 if failure == "scope_conflict" else 503
    assert response.status_code == expected
    if failure == "sqlite_busy":
        assert response.headers.get("Retry-After") == "1"
        assert "繁忙" in response.json()["detail"]
    else:
        assert "范围冲突" in response.json()["detail"]
    with MetadataStore(metadata_path, read_only=True) as metadata:
        assert metadata.load_collection_task(task_id) is None


def test_web_resume_requeues_the_frozen_task_definition(tmp_path, monkeypatch):
    from stock_data_manage.pipeline import inputs
    from stock_data_manage.storage.metadata import MetadataStore
    from stock_data_manage.worker.scheduler import build_security_master_definition

    config, data, metadata_path = _task_api_runtime(tmp_path, calendar_days=(date(2026, 10, 28),))
    task_id = "security-master-20261028"
    definition = build_security_master_definition(config, trade_date=date(2026, 10, 28),
        now=datetime(2026, 11, 11, 12, tzinfo=timezone.utc), is_trading_day=True, task_id=task_id)
    from stock_data_manage.pipeline.inputs import submit_collection_task
    submit_collection_task(definition=definition, config_root=config, data_root=data, mode="live")
    with MetadataStore(metadata_path) as metadata:
        failed = metadata.load_collection_task(task_id)
        definition_hash = failed["definition_hash"]
        failed.update(status="failed", error="offline fixture failure")
        metadata.save_collection_task(failed)
    executed = Event()

    def offline_execute(*, definition, config_root, data_root=None, **_kwargs):
        with MetadataStore(metadata_path) as metadata:
            state = metadata.load_collection_task(definition["task_id"])
            state.update(status="published", phase="finished", error=None,
                         updated_at=datetime.now(timezone.utc).isoformat())
            metadata.save_collection_task(state)
        executed.set()
        return state

    monkeypatch.setattr(inputs, "_execute_task_body", offline_execute)
    app = create_app(config_root=config, data_root=data)
    with TestClient(app, base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client:
        response = client.post(f"/api/tasks/{task_id}/resume", json={})
        assert response.status_code == 202, response.text
        assert response.json()["resumed"] is True
        assert response.json()["status"] == "waiting"
        assert executed.wait(5), "the requeued frozen task was not consumed"
        state = _wait_for_web_task_status(metadata_path, task_id, "published")
        assert state["definition_hash"] == definition_hash


def _run_collection_queue_worker_process(
        config_root, data_root, first_task_id, task_count, barrier, release_first,
        first_started, contended, stop, execution_order, outcomes, counter_lock,
        active_count, maximum_active, completed_count):
    import os
    import time
    import requests
    from stock_data_manage.config.loader import load_storage_paths
    from stock_data_manage.pipeline import inputs
    from stock_data_manage.storage.metadata import MetadataStore

    def deny_provider_request(*_args, **_kwargs):
        raise AssertionError("offline queue workers must not perform a Provider request")

    requests.Session.send = deny_provider_request

    def offline_execute(*, definition, config_root, data_root=None, **_kwargs):
        task_id = definition["task_id"]
        with counter_lock:
            active_count.value += 1
            maximum_active.value = max(maximum_active.value, active_count.value)
            if active_count.value > 1:
                outcomes.put(("overlap", task_id, os.getpid()))
                stop.set()
        execution_order.put(task_id)
        try:
            if task_id == first_task_id:
                first_started.set()
                if not release_first.wait(15):
                    raise TimeoutError("first cross-process task was not released")
            paths = load_storage_paths(config_root, data_root=data_root)
            with MetadataStore(paths["metadata_path"]) as metadata:
                state = metadata.load_collection_task(task_id)
                state.update(status="published", phase="finished", error=None,
                             updated_at=datetime.now(timezone.utc).isoformat())
                metadata.save_collection_task(state)
            with counter_lock:
                completed_count.value += 1
            return state
        finally:
            with counter_lock:
                active_count.value -= 1

    inputs._execute_task_body = offline_execute
    try:
        barrier.wait(10)
        while completed_count.value < task_count and not stop.is_set():
            result = inputs.run_collection_queue_once(config_root=config_root, data_root=data_root)
            if result and result.get("_error"):
                outcomes.put(("error", result.get("task_id"), repr(result["_error"])))
                stop.set()
                break
            if result is None:
                if first_started.is_set() and not release_first.is_set():
                    contended.set()
                time.sleep(0.01)
        outcomes.put(("done", os.getpid()))
    except BaseException as exc:
        outcomes.put(("error", os.getpid(), f"{type(exc).__name__}: {exc}"))
        stop.set()


def _submit_collection_task_from_process(definition, config_root, data_root, barrier, results):
    import requests
    from stock_data_manage.pipeline.inputs import submit_collection_task
    from stock_data_manage.storage.metadata import CollectionTaskScopeConflict

    def deny_provider_request(*_args, **_kwargs):
        raise AssertionError("task admission must not perform a Provider request")

    requests.Session.send = deny_provider_request
    try:
        barrier.wait(10)
        accepted = submit_collection_task(
            definition=definition, config_root=config_root, data_root=data_root, mode="live")
        results.put({"task_id": definition["task_id"], "result": "accepted",
                     "status": accepted["status"], "queue_order": accepted["queue_order"]})
    except CollectionTaskScopeConflict as exc:
        results.put({"task_id": definition["task_id"], "result": "conflict",
                     "conflict_task_id": exc.task_id, "conflicts": exc.conflicts})
    except BaseException as exc:
        results.put({"task_id": definition["task_id"], "result": "error",
                     "error": f"{type(exc).__name__}: {exc}"})


@pytest.mark.parametrize("same_date", [False, True], ids=["non-overlapping", "overlapping"])
def test_collection_task_submission_is_serialized_across_processes(tmp_path, same_date):
    import multiprocessing
    from stock_data_manage.worker.scheduler import build_security_master_definition
    from stock_data_manage.storage.metadata import MetadataStore

    days = (date(2026, 10, 28), date(2026, 10, 28) if same_date else date(2026, 10, 29))
    config, data, metadata_path = _task_api_runtime(
        tmp_path, calendar_days=tuple(dict.fromkeys(days)))
    now = datetime(2026, 11, 12, 12, tzinfo=timezone.utc)
    definitions = [build_security_master_definition(
        config, trade_date=day, now=now, is_trading_day=True, task_id=f"process-submit-{index}-{day:%Y%m%d}")
        for index, day in enumerate(days)]

    context = multiprocessing.get_context("spawn")
    barrier, results = context.Barrier(2), context.Queue()
    processes = [context.Process(target=_submit_collection_task_from_process,
        args=(definition, str(config), str(data), barrier, results)) for definition in definitions]
    outcomes = []
    try:
        for process in processes:
            process.start()
        outcomes = [results.get(timeout=20), results.get(timeout=20)]
    finally:
        for process in processes:
            process.join(10)
            if process.is_alive():
                process.terminate()
                process.join(5)

    assert all(process.exitcode == 0 for process in processes)
    assert all(item["result"] != "error" for item in outcomes), outcomes
    with MetadataStore(metadata_path, read_only=True) as metadata:
        waiting = metadata.collection_tasks(status="waiting")
        orders = {item["task_id"]: item["queue_order"] for item in waiting}
    if same_date:
        assert [item["result"] for item in outcomes].count("accepted") == 1
        assert [item["result"] for item in outcomes].count("conflict") == 1
        assert len(waiting) == 1 and list(orders.values()) == [1]
        rejected = next(item for item in outcomes if item["result"] == "conflict")
        accepted = next(item for item in outcomes if item["result"] == "accepted")
        assert rejected["conflict_task_id"] == accepted["task_id"]
    else:
        assert all(item["result"] == "accepted" and item["status"] == "waiting" for item in outcomes)
        assert len(waiting) == 2
        assert sorted(orders.values()) == [1, 2]


def test_collection_queue_worker_processes_claim_fifo_one_at_a_time(tmp_path):
    import multiprocessing
    from stock_data_manage.pipeline.inputs import submit_collection_task
    from stock_data_manage.storage.metadata import MetadataStore
    from stock_data_manage.worker.scheduler import build_security_master_definition

    days = (date(2026, 10, 28), date(2026, 10, 29), date(2026, 10, 30))
    config, data, metadata_path = _task_api_runtime(tmp_path, calendar_days=days)
    now = datetime(2026, 11, 12, 12, tzinfo=timezone.utc)
    definitions = [build_security_master_definition(
        config, trade_date=day, now=now, is_trading_day=True,
        task_id=f"process-worker-{index}-{day:%Y%m%d}") for index, day in enumerate(days)]
    accepted = [submit_collection_task(definition=definition, config_root=config,
        data_root=data, mode="live") for definition in definitions]
    expected_order = [item["task_id"] for item in sorted(accepted, key=lambda item: item["queue_order"])]

    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    release_first, first_started, contended, stop = (context.Event() for _ in range(4))
    execution_order, outcomes = context.Queue(), context.Queue()
    counter_lock = context.Lock()
    active_count, maximum_active, completed_count = (context.Value("i", 0) for _ in range(3))
    process_args = (str(config), str(data), expected_order[0], len(expected_order), barrier,
                    release_first, first_started, contended, stop, execution_order, outcomes,
                    counter_lock, active_count, maximum_active, completed_count)
    processes = [context.Process(target=_run_collection_queue_worker_process, args=process_args)
                 for _ in range(2)]
    outcome_rows = []
    try:
        for process in processes:
            process.start()
        assert first_started.wait(10), "neither process claimed the FIFO head"
        assert contended.wait(10), "the second process did not observe the held execution lock"
        assert completed_count.value == 0
        assert maximum_active.value == 1
        release_first.set()
        for process in processes:
            process.join(20)
            if process.is_alive():
                process.terminate()
                process.join(5)
        outcome_rows = [outcomes.get(timeout=5) for _ in processes]
    finally:
        release_first.set()
        stop.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(5)

    assert all(process.exitcode == 0 for process in processes)
    assert [row[0] for row in outcome_rows].count("done") == 2, outcome_rows
    assert not any(row[0] in {"error", "overlap"} for row in outcome_rows), outcome_rows
    assert completed_count.value == len(expected_order)
    assert active_count.value == 0 and maximum_active.value == 1
    actual_order = [execution_order.get(timeout=5) for _ in expected_order]
    assert actual_order == expected_order
    with MetadataStore(metadata_path, read_only=True) as metadata:
        published = metadata.collection_tasks(status="published")
    assert {state["task_id"] for state in published} == set(expected_order)


def test_two_web_instances_and_cli_share_fifo_and_single_execution(tmp_path, monkeypatch):
    from threading import Barrier, Event, Lock, Thread
    import requests
    from stock_data_manage.pipeline import inputs
    from stock_data_manage.storage.metadata import CollectionTaskScopeConflict, MetadataStore
    from stock_data_manage.worker.scheduler import build_security_master_definition
    from stock_data_manage.cli import main as cli_main

    days = (date(2026, 10, 28), date(2026, 10, 29), date(2026, 10, 30))
    config, data, metadata_path = _task_api_runtime(tmp_path, calendar_days=days)
    first_id, web_id, cli_id = (f"security-master-{day:%Y%m%d}" for day in days)
    first_started, release_first = Event(), Event()
    execution_guard = Lock()
    executed = []
    active_count = 0
    max_active = 0
    overlap_detected = Event()

    def deny_provider_request(*_args, **_kwargs):
        raise AssertionError("offline multi-entry acceptance must not call a Provider")

    def offline_execute(*, definition, config_root, data_root=None, **_kwargs):
        nonlocal active_count, max_active
        task_id = definition["task_id"]
        with execution_guard:
            active_count += 1
            max_active = max(max_active, active_count)
            if active_count > 1:
                overlap_detected.set()
            executed.append(task_id)
        try:
            if task_id == first_id:
                first_started.set()
                if not release_first.wait(15):
                    raise TimeoutError("first offline task was not released")
            paths = __import__("stock_data_manage.config.loader", fromlist=["load_storage_paths"]).load_storage_paths(
                config_root, data_root=data_root)
            with MetadataStore(paths["metadata_path"]) as metadata:
                state = metadata.load_collection_task(task_id)
                state.update(status="published", phase="finished", error=None,
                             updated_at=datetime.now(timezone.utc).isoformat())
                metadata.save_collection_task(state)
            return state
        finally:
            with execution_guard:
                active_count -= 1

    monkeypatch.setattr(requests.Session, "send", deny_provider_request)
    monkeypatch.setattr(inputs, "_execute_task_body", offline_execute)
    app_a = create_app(config_root=config, data_root=data)
    app_b = create_app(config_root=config, data_root=data)
    cli_definition = build_security_master_definition(
        config, trade_date=days[2], now=datetime(2026, 11, 12, 12, tzinfo=timezone.utc),
        is_trading_day=True, task_id=cli_id)
    task_file = tmp_path / "cli-task.json"
    write_json(task_file, cli_definition)
    accepted_elsewhere = {first_id: {"queue_order": 1}}
    accepted_lock = Lock()
    both_submitted = Event()
    actual_submit = inputs.submit_collection_task

    def observe_submission(**kwargs):
        accepted = actual_submit(**kwargs)
        with accepted_lock:
            accepted_elsewhere[accepted["task_id"]] = accepted
            if web_id in accepted_elsewhere and cli_id in accepted_elsewhere:
                both_submitted.set()
        return accepted

    race = Barrier(2)
    web_response = {}
    cli_result = {}
    thread_errors = []

    with TestClient(app_a, base_url="http://127.0.0.1", client=("127.0.0.1", 5000)) as client_a, \
            TestClient(app_b, base_url="http://127.0.0.1", client=("127.0.0.1", 5001)) as client_b:
        monkeypatch.setattr(inputs, "submit_collection_task", observe_submission)
        first = client_a.post("/api/tasks/run", json={
            "dataset": "security_master", "trade_date": days[0].isoformat()})
        assert first.status_code == 202, first.text
        assert first.json()["queue_order"] == 1
        assert first_started.wait(10), "neither Web instance consumed the first queued task"

        duplicate = client_b.post("/api/tasks/run", json={
            "dataset": "security_master", "trade_date": days[0].isoformat()})
        assert duplicate.status_code == 202, duplicate.text
        assert duplicate.json()["already_queued"] is True
        assert duplicate.json()["queue_order"] == 1

        overlapping = build_security_master_definition(
            config, trade_date=days[0], now=datetime(2026, 11, 12, 12, tzinfo=timezone.utc),
            is_trading_day=True, task_id="security-master-overlap-shadow")
        with pytest.raises(CollectionTaskScopeConflict) as conflict:
            actual_submit(definition=overlapping, config_root=config, data_root=data, mode="live")
        assert conflict.value.task_id == first_id
        with MetadataStore(metadata_path, read_only=True) as metadata:
            assert metadata.load_collection_task("security-master-overlap-shadow") is None

        def submit_web():
            try:
                race.wait(10)
                web_response["response"] = client_b.post("/api/tasks/run", json={
                    "dataset": "security_master", "trade_date": days[1].isoformat()})
            except BaseException as exc:
                thread_errors.append(exc)

        def submit_cli():
            try:
                race.wait(10)
                cli_result["exit_code"] = cli_main([
                    "collect-task", "--task-file", str(task_file), "--config-root", str(config),
                    "--data-root", str(data), "--mode", "live", "--no-progress"])
            except BaseException as exc:
                thread_errors.append(exc)

        web_thread, cli_thread = Thread(target=submit_web), Thread(target=submit_cli)
        web_thread.start()
        cli_thread.start()
        try:
            assert both_submitted.wait(20), "the concurrent Web/CLI admissions did not finish"
            web_thread.join(10)
            assert not web_thread.is_alive()
            assert not thread_errors, thread_errors
            assert web_response["response"].status_code == 202, web_response.get("response")
            assert web_response["response"].json()["queue_order"] in {2, 3}
            with MetadataStore(metadata_path, read_only=True) as metadata:
                queued = [metadata.load_collection_task(task_id) for task_id in (first_id, web_id, cli_id)]
            assert sorted(state["queue_order"] for state in queued) == [1, 2, 3]
            assert [state["status"] for state in queued] == ["collecting", "waiting", "waiting"]
        finally:
            release_first.set()
            web_thread.join(15)
            cli_thread.join(20)

        for task_id in (first_id, web_id, cli_id):
            _wait_for_web_task_status(metadata_path, task_id, "published", timeout=20)
        assert not cli_thread.is_alive(), "the synchronous CLI entry did not finish its accepted task"

    assert not thread_errors, thread_errors
    assert cli_result["exit_code"] == 0
    assert max_active == 1 and not overlap_detected.is_set()
    queue_order = {task_id: accepted["queue_order"] for task_id, accepted in accepted_elsewhere.items()}
    assert set(queue_order) == {first_id, web_id, cli_id}
    assert executed == sorted((first_id, web_id, cli_id), key=queue_order.__getitem__)
