from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from stock_data_manage.service.web import create_app
from stock_data_manage.storage.integrity import Manifest
from stock_data_manage.storage.metadata import MetadataStore


def _config(tmp_path: Path) -> tuple[Path, Path]:
    config = tmp_path / "config"
    config.mkdir()
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
    return config, tmp_path / "runtime"


def test_old_partition_schema_is_rejected_before_read_or_write_mutation(tmp_path):
    database = tmp_path / "old.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE partition_status(dataset TEXT)")
    connection.commit()
    connection.close()
    before = database.read_bytes()
    before_files = {p.name for p in tmp_path.iterdir()}

    with pytest.raises(ValueError, match="last_task_id"):
        MetadataStore(database, read_only=True)
    with pytest.raises(ValueError, match="last_task_id"):
        MetadataStore(database)

    assert database.read_bytes() == before
    assert {p.name for p in tmp_path.iterdir()} == before_files


def test_master_summary_and_task_row_respect_outer_transaction_ownership():
    with MetadataStore() as metadata:
        def manifest(day, digest):
            return Manifest(dataset="security_master", asset_type="security", partition_key=day,
                row_count=1, first_key="XSHG:600000", last_key="XSHG:600000", content_hash=digest,
                size_bytes=1, raw_refs=(), expected_count=1)

        metadata.record_current_master_publish(manifest=manifest("2026-11-04", "old"),
            manifest_path="canonical/security_master/current/manifest.json", task_id="old-task")
        metadata.save_collection_task({"task_id": "old-task", "dataset": "security_master",
            "status": "published", "definition_hash": "old-hash", "definition": {}})

        metadata.connection.execute("BEGIN IMMEDIATE")
        metadata.mark_current_master_updating()
        metadata.record_current_master_publish(manifest=manifest("2026-11-05", "new"),
            manifest_path="canonical/security_master/current/manifest.json", task_id="new-task")
        metadata.save_collection_task({"task_id": "new-task", "dataset": "security_master",
            "status": "published", "definition_hash": "new-hash", "definition": {}})
        assert metadata.connection.in_transaction
        metadata.connection.execute("ROLLBACK")

        header = metadata.connection.execute(
            "SELECT partition_key,content_hash,status,last_task_id FROM partition_status WHERE dataset='security_master'"
        ).fetchone()
        assert header == ("2026-11-04", "old", "complete_clean", "old-task")
        assert metadata.collection_task_by_id("new-task") is None
        assert metadata.collection_task_by_id("old-task")["status"] == "published"

        metadata.connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(ValueError, match="definition is immutable"):
            metadata.save_collection_task({"task_id": "old-task", "dataset": "security_master",
                "status": "published", "definition_hash": "changed", "definition": {}})
        assert metadata.connection.in_transaction
        metadata.connection.execute("ROLLBACK")


def test_publication_route_maps_missing_schema_busy_and_corrupt_state(tmp_path, monkeypatch):
    config, runtime = _config(tmp_path)
    app = create_app(config_root=config, data_root=runtime)
    with TestClient(app) as client:
        assert client.get("/api/publications").status_code == 503
        assert client.get("/api/overview").status_code == 503
    assert not runtime.exists()

    with MetadataStore(runtime / "metadata/metadata.sqlite3") as metadata:
        metadata.save_collection_task({"task_id": "broken", "dataset": "daily_bar", "status": "waiting",
                                      "definition_hash": "h", "definition": {}})
        metadata.connection.execute("UPDATE collection_task SET state_json='not-json' WHERE task_id='broken'")
    app = create_app(config_root=config, data_root=runtime)
    with TestClient(app) as client:
        assert client.get("/api/overview").status_code == 409

    def busy(*args, **kwargs):
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise error

    monkeypatch.setattr(MetadataStore, "query_publications", busy)
    with TestClient(app) as client:
        response = client.get("/api/publications")
        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"

    def unexpected(*args, **kwargs):
        raise sqlite3.OperationalError("syntax error in query")

    monkeypatch.setattr(MetadataStore, "query_publications", unexpected)
    with TestClient(app) as client, pytest.raises(sqlite3.OperationalError, match="syntax error"):
        client.get("/api/publications")


def test_publication_api_filter_page_summary_only_and_shared_overview_snapshot(tmp_path, monkeypatch):
    config, runtime = _config(tmp_path)
    with MetadataStore(runtime / "metadata/metadata.sqlite3") as metadata:
        metadata.connection.execute("""INSERT INTO partition_status (
            dataset,asset_type,partition_key,adjustment,expected_count,actual_count,status,
            conflict_count,quarantined_count,updated_at,last_task_id)
            VALUES ('daily_bar','stock','2026-11-05','aggregate',NULL,7,'published',3,2,?, 'task-real')""",
            [datetime.now(timezone.utc)])
        metadata.connection.execute("""INSERT INTO partition_status (
            dataset,asset_type,partition_key,adjustment,expected_count,actual_count,status,
            conflict_count,quarantined_count,updated_at,last_task_id)
            VALUES ('reference_fixture','all','2026-11-04','none',2,1,'complete_clean',0,0,?,NULL)""",
            [datetime.now(timezone.utc)])
        metadata.save_collection_task({"task_id": "task-real", "dataset": "daily_bar", "status": "published",
                                      "definition_hash": "h", "definition": {}, "updated_at_utc": "2026-11-05T00:00:00+00:00"})
    app = create_app(config_root=config, data_root=runtime)
    reader = app.state.reader
    observed = []
    original_dashboard = MetadataStore.collection_task_dashboard
    original_publications = MetadataStore.query_publications

    def dashboard(self, **kwargs):
        observed.append(("tasks", self.connection.in_transaction))
        result = original_dashboard(self, **kwargs)
        if not getattr(dashboard, "writer_committed", False):
            writer = sqlite3.connect(runtime / "metadata/metadata.sqlite3")
            moment = datetime.now(timezone.utc).isoformat()
            writer.execute("""INSERT INTO collection_task (
                task_id,definition_hash,dataset,status,state_json,updated_at)
                VALUES ('task-new','h','daily_bar','published',?,?)""",
                [json.dumps({"task_id": "task-new", "dataset": "daily_bar", "status": "published",
                             "definition_hash": "h", "definition": {}}), moment])
            writer.execute("""INSERT INTO partition_status (
                dataset,asset_type,partition_key,adjustment,expected_count,actual_count,status,
                conflict_count,quarantined_count,updated_at,last_task_id)
                VALUES ('daily_bar','etf','2026-11-06','aggregate',NULL,1,'published',0,0,?,'task-new')""",
                [moment])
            writer.commit()
            writer.close()
            dashboard.writer_committed = True
        return result

    def publications(self, **kwargs):
        observed.append(("publications", self.connection.in_transaction))
        return original_publications(self, **kwargs)

    monkeypatch.setattr(MetadataStore, "collection_task_dashboard", dashboard)
    monkeypatch.setattr(MetadataStore, "query_publications", publications)
    reader._task_documents = lambda: (_ for _ in ()).throw(AssertionError("summary files must not be read"))
    reader._manifest_records = lambda *_: (_ for _ in ()).throw(AssertionError("raw refs must not be expanded"))

    with TestClient(app) as client:
        response = client.get("/api/publications?dataset=daily_bar&asset_type=stock&partition_key=2026-11-05&include_evidence=true")
        assert response.status_code == 200
        body = response.json()
        assert body["scope"] == "registered_publications" and body["total"] == 1
        row = body["items"][0]
        assert row["row_count"] == 7 and row["expected_count"] is None and row["coverage_percent"] is None
        assert row["conflict_count"] == 3 and row["quarantined_count"] == 2
        assert row["latest_task_id"] == "task-real" and row["raw_date"] == "2026-11-05"
        assert not ({"tasks", "evidence", "source_count", "size_bytes", "publication_metadata"} & row.keys())
        assert client.get("/api/publications?dataset=absent").json()["total"] == 0
        second = client.get("/api/publications?page=2&page_size=1").json()
        assert second["total"] == 2 and len(second["items"]) == 1
        assert second["items"][0]["dataset"] == "reference_fixture"
        assert client.get("/api/publications?page=3&page_size=1").json()["items"] == []
        known = client.get("/api/publications?dataset=reference_fixture").json()["items"][0]
        assert known["coverage_percent"] == 50.0
        observed.clear()
        overview = client.get("/api/overview").json()
        assert len(overview["recent_publications"]) == 2
        assert overview["security_master_status"] == "not_registered"
        assert sum(overview["task_counts"].values()) == 1
        assert overview["recent_tasks"][0]["task_id"] == "task-real"
        refreshed = client.get("/api/overview").json()
        assert sum(refreshed["task_counts"].values()) == 2
        assert len(refreshed["recent_publications"]) == 3
        assert refreshed["recent_publications"][0]["partition_key"] == "2026-11-06"
    assert observed == [("tasks", True), ("publications", True)] * 2


def test_committing_master_hides_previous_header_from_count_and_page(tmp_path):
    config, runtime = _config(tmp_path)
    with MetadataStore(runtime / "metadata/metadata.sqlite3") as metadata:
        metadata.record_current_master_publish(
            manifest=__import__("stock_data_manage.storage.integrity", fromlist=["Manifest"]).Manifest(
                dataset="security_master", asset_type="security", partition_key="2026-11-04", adjustment="none",
                expected_count=1, row_count=1, first_key="XSHG:600000", last_key="XSHG:600000",
                content_hash="hash", size_bytes=1, raw_refs=()),
            manifest_path="canonical/security_master/current/manifest.json", task_id="old-task")
        metadata.save_collection_task({"task_id": "current-task", "dataset": "security_master",
            "status": "committing", "definition_hash": "h", "definition": {}})
    app = create_app(config_root=config, data_root=runtime)
    with TestClient(app) as client:
        response = client.get("/api/publications?dataset=security_master")
        assert response.json()["security_master_status"] == "updating"
        assert response.json()["total"] == 0 and response.json()["items"] == []


def test_publication_and_overview_performance_on_synthetic_registered_rows(tmp_path, monkeypatch, record_property):
    config, runtime = _config(tmp_path)
    metadata_path = runtime / "metadata/metadata.sqlite3"
    start = date(2020, 1, 1)
    business_dates = []
    current = start
    while len(business_dates) < 1000:
        if current.weekday() < 5:
            business_dates.append(current.isoformat())
        current += timedelta(days=1)
    now = datetime.now(timezone.utc)
    with MetadataStore(metadata_path) as metadata:
        task_rows = []
        for index in range(10_000):
            task_id = "synthetic-master-large" if index == 9_999 else f"task-{index:05d}"
            dataset = "security_master" if index == 9_999 else "daily_bar"
            status = "published" if index % 4 == 0 else "collecting"
            updated = now - timedelta(seconds=index)
            state = {"task_id": task_id, "dataset": dataset, "status": status,
                     "definition_hash": "synthetic", "requested_date": "2026-11-05",
                     "source_unit_count": 3}
            if index == 9_999:
                state["large_state"] = "x" * 1_000_000
            task_rows.append((task_id, "synthetic", dataset, status, json.dumps(state), updated, index,
                              updated, updated, index if index % 4 == 0 else None))
        metadata.connection.executemany("""INSERT INTO collection_task (
            task_id,definition_hash,dataset,status,state_json,updated_at,queue_order,first_accepted_at,queued_at,first_execution_order)
            VALUES (?,?,?,?,?,?,?,?,?,?)""", task_rows)
        # One unusually large frozen grant/context and one report must stay outside list queries.
        metadata.connection.execute("INSERT INTO collection_task_context VALUES (?,?,?,?)",
            ["synthetic-master-large", "{}", json.dumps({"grant": "g" * 1_050_000}), "{}"])
        summaries = []
        for day in business_dates:
            for asset in ("stock", "etf", "index"):
                summaries.append(("daily_bar", asset, day, "aggregate", None, 2300,
                    "published", 0, 0, now, None))
        metadata.connection.executemany("""INSERT INTO partition_status (
            dataset,asset_type,partition_key,adjustment,expected_count,actual_count,status,
            conflict_count,quarantined_count,updated_at,last_task_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""", summaries)
        large_state_json_bytes = metadata.connection.execute(
            "SELECT length(state_json) FROM collection_task WHERE task_id='synthetic-master-large'").fetchone()[0]
        large_context_json_bytes = metadata.connection.execute(
            "SELECT length(snapshot_json) FROM collection_task_context WHERE task_id='synthetic-master-large'").fetchone()[0]
    raw_root = runtime / "raw"
    raw_root.mkdir(parents=True)
    for index in range(1000):
        (raw_root / business_dates[index]).mkdir()

    import stock_data_manage.storage.metadata as metadata_module
    original_open = metadata_module.open_runtime_database
    traces = []

    def instrumented_open(*args, **kwargs):
        connection = original_open(*args, **kwargs)
        trace = {"sql": [], "vm_callbacks": 0}
        connection.set_trace_callback(trace["sql"].append)
        connection.set_progress_handler(lambda: trace.__setitem__("vm_callbacks", trace["vm_callbacks"] + 1) or 0, 1000)
        traces.append(trace)
        return connection

    monkeypatch.setattr(metadata_module, "open_runtime_database", instrumented_open)
    projection_counts = []
    original_projection = metadata_module._collection_task_row_to_view

    def counted_projection(row):
        if not projection_counts:
            projection_counts.append(0)
        projection_counts[0] += 1
        return original_projection(row)

    monkeypatch.setattr(metadata_module, "_collection_task_row_to_view", counted_projection)
    app = create_app(config_root=config, data_root=runtime)
    reader = app.state.reader
    reader._task_documents = lambda: (_ for _ in ()).throw(AssertionError("task summaries must not be scanned"))
    reader._manifest_records = lambda *_: (_ for _ in ()).throw(AssertionError("raw manifests must not be opened"))
    unregistered = runtime / "canonical" / "legacy" / "manifest.json"
    unregistered.parent.mkdir(parents=True)
    unregistered.write_text("not-json", encoding="utf-8")
    guarded_roots = {reader.paths["canonical_root"].resolve(), reader.paths["workspace_root"].resolve()}
    original_rglob, original_open_path = Path.rglob, Path.open

    def guarded_rglob(path, pattern):
        if path.resolve() in guarded_roots:
            raise AssertionError(f"historical file scan attempted under {path}")
        return original_rglob(path, pattern)

    def guarded_open(path, *args, **kwargs):
        if path.name == "manifest.json" and path.resolve().is_relative_to(reader.paths["canonical_root"].resolve()):
            raise AssertionError(f"publication manifest opened by list/overview: {path}")
        return original_open_path(path, *args, **kwargs)

    monkeypatch.setattr(Path, "rglob", guarded_rglob)
    monkeypatch.setattr(Path, "open", guarded_open)
    raw_resolve_calls = [0]
    original_resolve = Path.resolve

    def counted_resolve(path, *args, **kwargs):
        if path.is_relative_to(raw_root):
            raw_resolve_calls[0] += 1
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", counted_resolve)

    with TestClient(app) as client:
        first_start = time.perf_counter()
        first = client.get("/api/publications?page=1&page_size=100")
        first_seconds = time.perf_counter() - first_start
        first_trace = traces[-1]
        assert first.status_code == 200
        first_body = first.json()
        reuse_seconds = []
        reuse_status = []
        for _ in range(5):
            begun = time.perf_counter()
            response = client.get("/api/publications?page=1&page_size=100")
            reuse_seconds.append(time.perf_counter() - begun)
            reuse_status.append((response.status_code, response.json()["total"], len(response.json()["items"])))
        reuse_trace = traces[-1]
        deep_start = time.perf_counter()
        deep = client.get("/api/publications?page=30&page_size=100")
        deep_seconds = time.perf_counter() - deep_start
        deep_trace = traces[-1]
        overview_start = time.perf_counter()
        overview = client.get("/api/overview")
        overview_seconds = time.perf_counter() - overview_start
        overview_trace = traces[-1]
        overview_projection_calls = projection_counts[0] if projection_counts else 0
        overview_reuse_seconds = []
        overview_reuse_status = []
        overview_reuse_traces = []
        overview_reuse_projection_calls = []
        for _ in range(5):
            begun = time.perf_counter()
            projection_before = projection_counts[0] if projection_counts else 0
            response = client.get("/api/overview")
            overview_reuse_seconds.append(time.perf_counter() - begun)
            projection_after = projection_counts[0] if projection_counts else 0
            overview_reuse_projection_calls.append(projection_after - projection_before)
            overview_reuse_status.append((response.status_code,
                sum(response.json()["task_counts"].values()), len(response.json()["recent_tasks"]),
                len(response.json()["recent_publications"])))
            overview_reuse_traces.append(traces[-1])
        raw_options_start = time.perf_counter()
        raw_options_response = client.get("/api/raw/options")
        raw_options_seconds = time.perf_counter() - raw_options_start

    p95 = sorted(reuse_seconds)[math.ceil(.95 * len(reuse_seconds)) - 1]
    overview_p95 = sorted(overview_reuse_seconds)[math.ceil(.95 * len(overview_reuse_seconds)) - 1]
    publication_select = next(sql for sql in first_trace["sql"] if sql.startswith("SELECT dataset,asset_type,partition_key,adjustment"))
    with MetadataStore(metadata_path, read_only=True) as metadata:
        plan = [row[3] for row in metadata.connection.execute("EXPLAIN QUERY PLAN " + publication_select)]
    overview_projection_counts = overview_reuse_projection_calls
    report = {
        "scope": "synthetic isolated SQLite rows only; not market data publication evidence",
        "task_rows": 10_000,
        "large_state_json_bytes": large_state_json_bytes,
        "large_context_snapshot_json_bytes": large_context_json_bytes,
        "large_state_task_sort_rank": 10_000,
        "large_state_selected_by_overview": any(
            item["task_id"] == "synthetic-master-large" for item in overview.json()["recent_tasks"]),
        "publication_rows": 3_000,
        "business_days": 1_000,
        "asset_types": ["stock", "etf", "index"],
        "raw_date_directories": 1_000,
        "raw_options_seconds": raw_options_seconds,
        "raw_options_resolve_calls_for_normal_date_entries": raw_resolve_calls[0],
        "raw_options_returned_date_count": len(raw_options_response.json()["dates"]),
        "first_page_single_sample_not_cold_seconds": first_seconds,
        "first_page_total_rows": first_body["total"],
        "first_page_returned_rows": len(first_body["items"]),
        "reuse_5_seconds": reuse_seconds,
        "reuse_5_p95_nearest_rank_seconds": p95,
        "reuse_status_total_items": reuse_status,
        "deep_page_30_seconds": deep_seconds,
        "deep_page_total_rows": deep.json()["total"],
        "deep_page_returned_rows": len(deep.json()["items"]),
        "overview_seconds_including_1000_raw_date_directory_listing": overview_seconds,
        "overview_reuse_5_seconds_including_1000_raw_date_directory_listing": overview_reuse_seconds,
        "overview_reuse_5_p95_nearest_rank_seconds": overview_p95,
        "overview_reuse_status_recent_counts": overview_reuse_status,
        "overview_task_counts_total": sum(overview.json()["task_counts"].values()),
        "overview_recent_task_count": len(overview.json()["recent_tasks"]),
        "overview_recent_publication_count": len(overview.json()["recent_publications"]),
        "actual_first_page_sql": publication_select,
        "actual_first_page_sql_plan": plan,
        "actual_first_page_vm_callbacks_per_1000_opcodes": first_trace["vm_callbacks"],
        "reuse_vm_callbacks_per_1000_opcodes": reuse_trace["vm_callbacks"],
        "deep_vm_callbacks_per_1000_opcodes": deep_trace["vm_callbacks"],
        "task_row_projection_calls_first_overview": overview_projection_calls,
        "task_row_projection_calls_per_overview_reuse": overview_projection_counts,
        "overview_trace_excluded_large_context_and_daily_receipts": all(
            "collection_task_context" not in " ".join(trace["sql"])
            and "daily_publication" not in " ".join(trace["sql"])
            and "FROM daily_bar" not in " ".join(trace["sql"])
            and "FROM daily_quote" not in " ".join(trace["sql"])
            for trace in [overview_trace, *overview_reuse_traces]),
        "overview_actual_sql": overview_trace["sql"],
        "work_bound": "COUNT and offset traversal scale with registered summary rows; payload rows are bounded to page size 100; overview reads at most 6 recent rows per section",
    }
    evidence_dir = Path(os.environ.get("PUBLICATION_TABLE_EVIDENCE_DIR", tmp_path))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    report_path = evidence_dir / "publication-table-performance.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    record_property("publication_table_performance_json", str(report_path))

    assert first.status_code == 200 and first_body["total"] == 3_000 and len(first_body["items"]) == 100
    assert deep.status_code == 200 and deep.json()["total"] == 3_000 and len(deep.json()["items"]) == 100
    assert all(status == 200 and total == 3_000 and returned == 100 for status, total, returned in reuse_status)
    assert sum(overview.json()["task_counts"].values()) == 10_000
    assert len(overview.json()["recent_tasks"]) == len(overview.json()["recent_publications"]) == 6
    assert raw_options_response.status_code == 200 and len(raw_options_response.json()["dates"]) == 1000
    assert raw_options_seconds < 1 and raw_resolve_calls[0] == 0
    assert all(status == 200 and count == 10_000 and tasks == 6 and publications == 6
               for status, count, tasks, publications in overview_reuse_status)
    assert overview_seconds < 2 and overview_p95 < 1
    assert overview_projection_calls == 6 and overview_projection_counts == [6] * 5
    assert report["overview_trace_excluded_large_context_and_daily_receipts"]
    assert "partition_status_publication_idx" in " ".join(plan)
    assert first_seconds < 2 and p95 < 1 and deep_seconds < 1
