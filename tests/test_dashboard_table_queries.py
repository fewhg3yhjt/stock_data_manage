from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time

from fastapi.testclient import TestClient
import pytest

from stock_data_manage.service.web import create_app
from stock_data_manage.storage.metadata import MetadataStore


def _make_runtime(root: Path, *, task_count: int = 0) -> tuple[Path, Path, datetime]:
    config = root / "config"
    config.mkdir(parents=True)
    (config / "collection.yaml").write_text(
        """storage:
  data_root: data
  raw_root: data/raw
  workspace_root: data/task_workspace
  archive_root: data/task_archive
  canonical_root: data/canonical
  metadata_path: data/metadata/metadata.sqlite3
realtime_minute:
  hot_store:
    path: data/hot/minute_hot.db
""", encoding="utf-8")
    data = root / "runtime"
    database = data / "metadata/metadata.sqlite3"
    now = datetime.now(timezone.utc)
    with MetadataStore(database) as metadata:
        rows = []
        for index in range(task_count):
            task_id = f"task-{index:05d}"
            status = ("waiting", "collecting", "checking", "committing", "failed", "published")[index % 6]
            age = 600 if index % 2 else 60
            updated = now - timedelta(seconds=age)
            state = {"task_id": task_id, "dataset": "security_master" if index % 2 == 0 else "daily_bar",
                     "definition_hash": f"definition-{index}", "status": status,
                     "requested_date": "2026-11-05", "generated_at_utc": (now - timedelta(seconds=age + 5)).isoformat(),
                     "source_unit_count": 8, "missing_count": index % 3,
                     "progress": {"stages": {"raw": {"completed": index % 9, "total": 8}}}}
            if index == 0:
                state.pop("progress")
            if index == 0:
                # One large task row exercises selected-page JSON parsing without
                # repeating large state or context for every history record.
                state["definition"] = {"trade_date": "2026-11-05", "units": [{"key": f"source-{part}"}
                                                                                for part in range(8)]}
                state["large_report_payload"] = "x" * 1_000_000
            rows.append((task_id, f"definition-{index}", state["dataset"], status,
                         json.dumps(state, separators=(",", ":")), updated,
                         index + 1, updated, updated, index + 1 if status != "waiting" else None))
        if rows:
            metadata.connection.execute("BEGIN")
            metadata.connection.executemany(
                "INSERT INTO collection_task(task_id,definition_hash,dataset,status,state_json,updated_at,"
                "queue_order,first_accepted_at,queued_at,first_execution_order) VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
            # A single large frozen grant belongs to the first task. The task list
            # must not JOIN or decode this 1+ MB context row.
            grant = json.dumps({"daily_overwrite_grant": {"owners": ["owner-" + "x" * 200] * 5_000}})
            metadata.connection.execute(
                "INSERT INTO collection_task_context(task_id,scope_json,snapshot_json,active_scope_json) VALUES(?,?,?,?)",
                ["task-00000", "{}", grant, "{}"])
            metadata.connection.execute("COMMIT")
    return config, data, now


def test_task_http_filters_pages_stale_and_uses_only_persisted_task_rows(tmp_path, monkeypatch):
    config, data, fixture_now = _make_runtime(tmp_path, task_count=10_000)
    app = create_app(config_root=config, data_root=data)
    from stock_data_manage.service.web import DashboardReader
    monkeypatch.setattr(DashboardReader, "_task_documents", lambda self: (_ for _ in ()).throw(
        AssertionError("task list must not scan summaries or manifests")))
    import stock_data_manage.storage.metadata as metadata_module
    traced_sql: list[str] = []
    progress_ticks = {"value": 0}
    open_database = metadata_module.open_runtime_database

    def traced_open(*args, **kwargs):
        connection = open_database(*args, **kwargs)
        connection.set_trace_callback(traced_sql.append)
        connection.set_progress_handler(lambda: progress_ticks.__setitem__(
            "value", progress_ticks["value"] + 1) or 0, 1000)
        return connection

    monkeypatch.setattr(metadata_module, "open_runtime_database", traced_open)
    parsed_pages: list[int] = []
    parsed_views = {"value": 0}
    row_to_view = metadata_module._collection_task_row_to_view

    def counted_view(row):
        parsed_views["value"] += 1
        return row_to_view(row)

    monkeypatch.setattr(metadata_module, "_collection_task_row_to_view", counted_view)
    query_page = MetadataStore.query_collection_tasks

    def measured_query(self, **kwargs):
        before = parsed_views["value"]
        result = query_page(self, **kwargs)
        parsed_pages.append(parsed_views["value"] - before)
        return result

    monkeypatch.setattr(MetadataStore, "query_collection_tasks", measured_query)

    elapsed: list[float] = []
    responses = []
    with TestClient(app) as client:
        started = time.perf_counter()
        all_tasks = client.get("/api/tasks?page=1&page_size=100")
        elapsed.append(time.perf_counter() - started)
        responses.append(all_tasks)
        for _ in range(5):
            started = time.perf_counter()
            repeated = client.get("/api/tasks?page=1&page_size=100")
            elapsed.append(time.perf_counter() - started)
            responses.append(repeated)
        first_six_vm_ticks = progress_ticks["value"]
        first_six_sql = list(traced_sql)
        vm_before_deep_page = progress_ticks["value"]
        started = time.perf_counter()
        deep_page = client.get("/api/tasks?page=100&page_size=100")
        deep_page_elapsed = time.perf_counter() - started
        deep_page_vm_ticks = progress_ticks["value"] - vm_before_deep_page
        responses.append(deep_page)
        page_sql = next(statement for statement in traced_sql if statement.lstrip().upper().startswith(
            "SELECT TASK_ID,DATASET,STATUS,STATE_JSON,UPDATED_AT,QUEUE_ORDER,FIRST_EXECUTION_ORDER "
            "FROM COLLECTION_TASK ORDER BY UPDATED_AT DESC,TASK_ID ASC"))
        deep_page_sql = next(statement for statement in traced_sql if "LIMIT 100 OFFSET 9900" in statement)
        database = data / "metadata/metadata.sqlite3"
        with sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True) as inspect_db:
            plan = inspect_db.execute("EXPLAIN QUERY PLAN " + page_sql).fetchall()
            large_state_bytes = inspect_db.execute(
                "SELECT length(state_json) FROM collection_task WHERE task_id='task-00000'").fetchone()[0]
            large_context_bytes = inspect_db.execute(
                "SELECT length(snapshot_json) FROM collection_task_context WHERE task_id='task-00000'").fetchone()[0]
        evidence = Path(__file__).resolve().parents[1] / "tmp/sqlite-runtime-implementation/dashboard-table-queries"
        evidence.mkdir(parents=True, exist_ok=True)
        report_path = evidence / f"tasks-page-performance-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
        report = {
            "scope": "synthetic isolated collection_task table; no production or provider data",
            "task_rows": 10_000,
            "large_state_rows": 1,
            "large_state_bytes": large_state_bytes,
            "large_context_bytes": large_context_bytes,
            "page_size": 100,
            "first_request_seconds_single_sample_not_cold_p95": round(elapsed[0], 6),
            "first_request_status_total_items": [responses[0].status_code,
                responses[0].json().get("total"), len(responses[0].json().get("items", ()))],
            "reuse_request_seconds_n5": [round(value, 6) for value in elapsed[1:]],
            "reuse_request_status_total_items": [[response.status_code, response.json().get("total"),
                len(response.json().get("items", ()))] for response in responses[1:6]],
            "reuse_p95_nearest_rank": round(sorted(elapsed[1:])[4], 6),
            "deep_page_100_seconds": round(deep_page_elapsed, 6),
            "deep_page_100_status_total_items": [deep_page.status_code,
                deep_page.json().get("total"), len(deep_page.json().get("items", ()))],
            "deep_page_vm_progress_callbacks_per_1000_opcodes": deep_page_vm_ticks,
            "deep_page_query": deep_page_sql,
            "parsed_state_rows_per_request": parsed_pages,
            "parsed_state_view_calls_total": parsed_views["value"],
            "sqlite_vm_progress_callbacks_per_1000_opcodes_first_six_requests": first_six_vm_ticks,
            "actual_sql_statements_first_six_requests": first_six_sql,
            "actual_api_page_query": page_sql,
            "actual_api_page_query_plan": [row[3] for row in plan],
            "work_bound": "COUNT and filtered/deep-page index traversal remain proportional to historical task rows; state JSON projection is limited to page rows",
            "assertions_status": "pending",
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        assert all_tasks.status_code == 200, all_tasks.text
        payload = all_tasks.json()
        assert payload["total"] == 10_000 and payload["pages"] == 100
        assert len(payload["items"]) == 100
        assert payload["items"][0]["updated_at_utc"]
        assert payload["items"][0]["generated_at_utc"]
        assert payload["items"][0]["generated_at_utc"] != payload["items"][0]["updated_at_utc"]
        assert payload["items"][0]["source_unit_count"] == 8
        assert all(response.status_code == 200 and response.json()["total"] == 10_000
                   and len(response.json()["items"]) == 100 for response in responses[1:])
        assert deep_page.status_code == 200 and deep_page.json()["total"] == 10_000
        assert len(deep_page.json()["items"]) == 100 and deep_page_elapsed < 1.0

        stale = client.get("/api/tasks?status=stale&page_size=100").json()
        status_cycle = ("waiting", "collecting", "checking", "committing", "failed", "published")
        assert stale["total"] == sum(status_cycle[index % 6] in {"collecting", "checking", "committing"}
            and index % 2 == 1 for index in range(10_000))
        assert all(item["display_status"] == "stale" for item in stale["items"])

        waiting = client.get("/api/tasks?status=waiting&page_size=2").json()
        assert waiting["total"] == 10_000 // 6 + (1 if 10_000 % 6 else 0)
        assert all(item["status"] == "waiting" and item["display_status"] == "waiting"
                   for item in waiting["items"])
        waiting_first = client.get("/api/tasks?status=waiting&page_size=1").json()["items"][0]
        assert waiting_first["completed_units"] == 0 and waiting_first["total_units"] == 8
        waiting_second = client.get("/api/tasks?status=waiting&page=2&page_size=1").json()["items"][0]
        assert waiting_second["completed_units"] == 6 and waiting_second["total_units"] == 8
        masters = client.get("/api/tasks?dataset=security_master&page=2&page_size=3").json()
        assert masters["total"] == 5_000 and len(masters["items"]) == 3
        assert all(item["dataset"] == "security_master" for item in masters["items"])
        searched = client.get("/api/tasks?q=task-00001&page_size=100").json()
        assert searched["total"] == 1
        assert client.get("/api/tasks?q=does-not-exist").json()["total"] == 0
        assert client.get("/api/tasks?page=200&page_size=100").json()["items"] == []
        assert client.get(f"/api/tasks?page={10**30}&page_size=100").json()["items"] == []
        assert client.get("/api/tasks?page_size=101").status_code == 422

    with MetadataStore(data / "metadata/metadata.sqlite3", read_only=True) as metadata:
        unknown = metadata.collection_task_by_id("missing")
        selected = metadata.collection_task_by_id("task-00000")
        summary = metadata.collection_task_dashboard(now=fixture_now, limit=6)
        assert unknown is None and selected["dataset"] == "security_master"
        assert sum(summary["counts"].values()) == 10_000 and len(summary["items"]) == 6
        assert summary["counts"]["stale"] == sum(
            status_cycle[index % 6] in {"collecting", "checking", "committing"} and index % 2 == 1
            for index in range(10_000))
        plan = metadata.connection.execute("EXPLAIN QUERY PLAN " + page_sql).fetchall()
        assert not any("collection_task_context" in str(row).lower() for row in plan)
        assert any("collection_task_updated_idx" in str(row) for row in plan)
    assert len(parsed_pages) == 16 and max(parsed_pages) <= 100
    assert not any("collection_task_context" in statement.lower() for statement in traced_sql)
    reuse_p95 = sorted(elapsed[1:])[4]
    assert elapsed[0] < 2.0 and reuse_p95 < 1.0
    report.update({
        "reuse_p95_nearest_rank": round(reuse_p95, 6),
        "parsed_state_rows_per_request": parsed_pages,
        "parsed_state_view_calls_total": parsed_views["value"],
        "max_parsed_state_rows": max(parsed_pages),
        "sqlite_vm_progress_callbacks_per_1000_opcodes_all_requests": progress_ticks["value"],
        "filters_and_boundaries": {"stale_total": stale["total"], "waiting_total": waiting["total"],
            "master_total": masters["total"], "exact_query_total": searched["total"],
            "empty_query_total": 0, "out_of_range_page_items": 0, "overflow_page_items": 0},
        "assertions_status": "passed",
    })
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def test_tasks_missing_database_is_503_and_does_not_initialize_runtime(tmp_path):
    config, data, _ = _make_runtime(tmp_path)
    database = data / "metadata/metadata.sqlite3"
    database.unlink()
    app = create_app(config_root=config, data_root=data)
    with TestClient(app) as client:
        response = client.get("/api/tasks")
    assert response.status_code == 503
    assert not database.exists()


def test_task_stale_filter_and_card_share_exact_300_second_boundary(tmp_path, monkeypatch):
    config, data, now = _make_runtime(tmp_path)
    with MetadataStore(data / "metadata/metadata.sqlite3") as metadata:
        rows = []
        for task_id, status, updated in (
            ("exact", "collecting", now - timedelta(seconds=300)),
            ("past", "collecting", now - timedelta(seconds=300, microseconds=1)),
            ("waiting-old", "waiting", now - timedelta(days=1)),
        ):
            state = {"task_id": task_id, "dataset": "daily_bar", "status": status,
                     "definition_hash": task_id, "source_unit_count": 2}
            rows.append((task_id, task_id, "daily_bar", status, json.dumps(state), updated))
        metadata.connection.executemany(
            "INSERT INTO collection_task(task_id,definition_hash,dataset,status,state_json,updated_at) "
            "VALUES(?,?,?,?,?,?)", rows)

    import stock_data_manage.service.web as web_module
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)
    monkeypatch.setattr(web_module, "datetime", FrozenDateTime)
    with TestClient(create_app(config_root=config, data_root=data)) as client:
        stale = client.get("/api/tasks?status=stale&page_size=100").json()
        cards = {item["task_id"]: item for item in client.get("/api/tasks?page_size=100").json()["items"]}
    assert [item["task_id"] for item in stale["items"]] == ["past"]
    assert cards["exact"]["stale"] is False and cards["exact"]["age_seconds"] == 300
    assert cards["past"]["stale"] is True and cards["past"]["age_seconds"] == 300
    assert cards["waiting-old"]["stale"] is False and cards["waiting-old"]["display_status"] == "waiting"


def test_tasks_rejects_corrupt_state_as_conflict_and_busy_as_retryable(tmp_path, monkeypatch):
    config, data, now = _make_runtime(tmp_path)
    with MetadataStore(data / "metadata/metadata.sqlite3") as metadata:
        metadata.connection.executemany(
            "INSERT INTO collection_task(task_id,definition_hash,dataset,status,state_json,updated_at) "
            "VALUES(?,?,?,?,?,?)", [
                ["broken", "broken", "daily_bar", "waiting", "{", now],
                ["non-object", "non-object", "daily_bar", "published", "[]", now],
            ])
    with TestClient(create_app(config_root=config, data_root=data)) as client:
        assert client.get("/api/tasks?q=broken").status_code == 409
        assert client.get("/api/tasks?q=non-object").status_code == 409

    busy_app = create_app(config_root=config, data_root=data)
    def raise_busy(self, **kwargs):
        error = __import__("sqlite3").OperationalError("database is locked")
        error.sqlite_errorcode = __import__("sqlite3").SQLITE_BUSY
        raise error
    monkeypatch.setattr(MetadataStore, "query_collection_tasks", raise_busy)
    with TestClient(busy_app) as client:
        response = client.get("/api/tasks")
    assert response.status_code == 503 and response.headers["retry-after"] == "1"

    def raise_unexpected(self, **kwargs):
        raise __import__("sqlite3").OperationalError("unexpected SQL failure")
    monkeypatch.setattr(MetadataStore, "query_collection_tasks", raise_unexpected)
    with TestClient(create_app(config_root=config, data_root=data)) as client:
        with pytest.raises(__import__("sqlite3").OperationalError, match="unexpected SQL failure"):
            client.get("/api/tasks")


def test_tasks_unsupported_runtime_schema_is_503_without_migration(tmp_path):
    config, data, _ = _make_runtime(tmp_path)
    database = data / "metadata/metadata.sqlite3"
    database.unlink()
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE collection_task(task_id TEXT,scope_json TEXT,snapshot_json TEXT,active_scope_json TEXT)")
    original = database.read_bytes()
    with TestClient(create_app(config_root=config, data_root=data)) as client:
        response = client.get("/api/tasks")
    assert response.status_code == 503
    assert database.read_bytes() == original


def test_submit_waiting_task_is_visible_through_tasks_api_without_running_sources(tmp_path, monkeypatch):
    from test_daily_collection_e2e import CONFIG, DAY, source_task
    from stock_data_manage.pipeline.inputs import submit_collection_task
    from stock_data_manage.providers.tencent.daily import TencentDailyProvider
    from stock_data_manage.providers.transport import RequestPacer
    import requests
    monkeypatch.setattr(TencentDailyProvider, "capture_window", lambda *_a, **_k: (_ for _ in ()).throw(
        AssertionError("task acceptance must not execute provider collection")))
    monkeypatch.setattr(requests.Session, "send", lambda *_a, **_k: (_ for _ in ()).throw(
        AssertionError("task acceptance must not issue network requests")))

    business = tmp_path / "business"
    definition = source_task(tmp_path / "fixture-responses", task_id="api-waiting-task")
    accepted = submit_collection_task(definition=definition, config_root=CONFIG, data_root=business,
        mode="replay", collector_options={"pacer": RequestPacer(wait=lambda _: None)})
    assert accepted["status"] == "waiting" and accepted["task_id"] == "api-waiting-task"
    with MetadataStore(business / "metadata/metadata.sqlite3", read_only=True) as metadata:
        state = metadata.collection_task_by_id("api-waiting-task")
    assert state["status"] == "waiting" and state["state"]["requested_date"] == DAY
    frozen = Path(state["state"]["definition_ref"]["path"])
    assert frozen.is_file() and not frozen.with_name("summary.json").exists()

    with TestClient(create_app(config_root=CONFIG, data_root=business)) as client:
        response = client.get("/api/tasks?status=waiting&page_size=10")
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["total"] == 1 and len(payload["items"]) == 1
    card = payload["items"][0]
    assert card["task_id"] == "api-waiting-task" and card["display_status"] == "waiting"
    assert card["requested_date"] == DAY and card["source_unit_count"] == 3
    assert card["completed_units"] == 0 and card["total_units"] == card["source_unit_count"]
    assert card["queue_order"] == state["queue_order"] and card["first_execution_order"] is None
