import hashlib
import json
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
    assert before == after


def test_each_response_in_manifest_is_visible_and_task_links_exact_evidence(tmp_path):
    reader, _, data, body = web_fixture(tmp_path)
    manifest = data / "raw/2026-10-09/tencent/kline_daily/manifest.sh600519.ndjson"
    events = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
    failure = {"event": "http_response", "provider": "tencent", "endpoint": "kline_daily",
               "fetched_at_utc": "2026-10-09T07:59:00+00:00", "outcome": "transport_error",
               "error": "timeout", "url": "https://example.test/kline"}
    manifest.write_text("".join(json.dumps(item) + "\n" for item in [failure, *events]), encoding="utf-8")
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
        write_json(directory / f"{symbol}.report.json", report)
        return source
    add("sh600519", [{"0":day,"2":price,"5":"7061.00","symbol":"sh600519"}
                     for day,price in [("2026-10-08","100.00"),("2026-10-09","101.00"),("2026-10-10","102.00")]])
    return reader, app, data, add


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


def test_record_query_distinguishes_exchanges_and_does_not_use_capture_date(tmp_path):
    reader, _, _, add = source_query_fixture(tmp_path)
    add("sh000001", [{"date":"2026-10-09","symbol":"sh000001","close":"3000"}])
    add("sz000001", [{"date":"2026-10-09","symbol":"sz000001","close":"12"}])
    assert reader.data_records(dataset="daily_bar",security="XSHG:000001")["total"] == 1
    assert reader.data_records(dataset="daily_bar",security="000001")["total"] == 2
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
        assert all_rows["total"] == 3
        assert client.get("/api/data?dataset=../../metadata").status_code == 404


def test_record_query_retains_historical_tencent_date_without_new_unit_mapping(tmp_path):
    reader, _, _, add = source_query_fixture(tmp_path)
    source = add("sh600002", [{"0":"2026-10-09","2":"12.000","5":"77.00","symbol":"sh600002"}])
    report_path = source.with_name("sh600002.report.json")
    report = json.loads(report_path.read_bytes())
    report["normalization_version"] = "tencent-window-input-v1"
    write_json(report_path, report)
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
