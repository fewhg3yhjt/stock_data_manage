import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from stock_data_manage.service.web import DashboardReader, create_app


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


def test_read_only_overview_marks_stale_task_and_lists_publication(tmp_path):
    reader, _, _, _ = web_fixture(tmp_path)

    payload = reader.overview()
    assert payload["task_counts"] == {"stale": 1}
    assert payload["recent_tasks"][0]["display_status"] == "stale"
    assert payload["recent_tasks"][0]["completed_units"] == 1
    assert payload["recent_publications"][0]["coverage_percent"] == 50.0


def test_overview_does_not_expand_publication_raw_evidence(tmp_path):
    reader, _, _, _ = web_fixture(tmp_path)

    def evidence_should_not_be_read(_path):
        raise AssertionError("overview must return publication summaries without raw evidence")

    reader._manifest_records = evidence_should_not_be_read
    payload = reader.overview()

    assert payload["recent_publications"][0]["dataset"] == "daily_bar"
    assert payload["recent_publications"][0]["row_count"] == 1
    assert payload["recent_publications"][0]["evidence"] == []


def test_publication_listing_keeps_raw_evidence_by_default(tmp_path):
    reader, _, _, _ = web_fixture(tmp_path)

    payload = reader.publications()
    summary = reader.publications(include_evidence=False)
    detail = reader.publications(dataset="daily_bar", asset_type="stock", partition_key="2026-10-09")
    detail_summary = reader.publications(dataset="daily_bar", asset_type="stock", partition_key="2026-10-09",
                                         include_evidence=False)

    assert len(payload["items"][0]["evidence"]) == 1
    assert payload["items"][0]["evidence"][0]["provider"] == "tencent"
    assert summary["items"][0]["evidence"] == []
    assert detail["total"] == 1
    assert detail["items"][0]["evidence"][0]["provider"] == "tencent"
    assert detail_summary["items"][0]["source_count"] == 1
    assert detail_summary["items"][0]["evidence"] == []


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

    payload = reader.publications(include_evidence=False)

    assert payload["items"][0]["tasks"] == ["daily-2026-10-09"]


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
        if getattr(route, "path", "").startswith("/api/")
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
        assert client.get("/api/overview").json()["task_counts"] == {}
        assert client.get("/api/raw").json()["total"] == 0
        assert client.get("/api/publications").json()["total"] == 0
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
    publications = reader.publications(dataset="security_master", include_evidence=False)
    assert publications["total"] == 1
    assert publications["items"][0]["partition_key"] == "2026-10-10"


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
