import hashlib
import json
from datetime import datetime, timedelta, timezone
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
