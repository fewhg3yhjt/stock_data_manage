"""Representative SQL/API work for GET /api/daily; synthetic rows are not production publication evidence."""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import sqlite3
import time

from httpx import ASGITransport, AsyncClient

from stock_data_manage.domain import AssetType, Exchange
from stock_data_manage.service.instruments import SecurityMasterStore, SecurityRecord
from stock_data_manage.storage.daily import initialize_schema
from stock_data_manage.storage.integrity import Manifest
from stock_data_manage.storage.metadata import MetadataStore


def _business_days(start: date, count: int) -> list[date]:
    days = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current = date.fromordinal(current.toordinal() + 1)
    return days


def _universe() -> list[tuple[str, str, Exchange]]:
    return ([(f"XSHG:{600000 + index:06d}", f"{600000 + index:06d}", Exchange.XSHG)
             for index in range(3931)]
            + [(f"XSHE:{index + 1:06d}", f"{index + 1:06d}", Exchange.XSHE)
               for index in range(3931)])


def _build_dataset(root: Path) -> tuple[Path, Path, list[date], list[date], list[str], str]:
    business = root / "business"
    metadata_path = business / "metadata" / "metadata.sqlite3"
    metadata_path.parent.mkdir(parents=True)
    securities = _universe()
    identities = [item[0] for item in securities]
    extended_days = _business_days(date(2021, 1, 4), 1000)
    days = extended_days[-100:]
    all_days = extended_days
    publication_ids = {day: f"perf-{index:04d}" for index, day in enumerate(all_days)}
    now = datetime(2026, 11, 9, 5, tzinfo=timezone.utc).isoformat()

    metadata = MetadataStore(metadata_path)
    initialize_schema(metadata.connection)
    connection = metadata.connection
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("BEGIN IMMEDIATE")
    for day in all_days:
        publication = publication_ids[day]
        connection.execute("INSERT INTO daily_publication VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [publication, f"key-{publication}", f"hash-{publication}", "performance-fixture", 1,
             day.isoformat(), "stock", json.dumps({"fixture": True}), "candidate/performance.parquet",
             "[\"raw/performance.json\"]", len(identities) if day in days else 1, 0, 0, "[]", now])

    quote_sql = "INSERT INTO daily_quote VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    for day in all_days:
        publication = publication_ids[day]
        is_core_day = day in days
        target_identities = identities if is_core_day else identities[:1]
        bars, quotes = [], []
        for identity in target_identities:
            bars.append((identity, day.isoformat(), "stock", publication))
            for adjustment, price in (("none", "12.34"), ("forward", "11.23"), ("backward", "13.45")):
                quotes.append((identity, day.isoformat(), adjustment, "available", price, "14.00", "10.00",
                    price, "123456.78", "987654.32", "fixture", "/daily/performance",
                    "raw/performance.json", now, "norm-v1", "cap-v1", "", "fixture:performance",
                    "[\"performance_fixture\"]", "full_ohlc"))
        connection.executemany("INSERT INTO daily_bar VALUES (?,?,?,?)", bars)
        connection.executemany(quote_sql, quotes)
    connection.execute("COMMIT")
    metadata.close()

    master_dir = business / "canonical" / "security_master" / "current"
    master_dir.mkdir(parents=True)
    master_db = master_dir / "security_master.duckdb"
    master_records = [SecurityRecord(identity, symbol, exchange, AssetType.STOCK, name=f"证券-{symbol}")
                      for identity, symbol, exchange in securities]
    with SecurityMasterStore(str(master_db)) as store:
        store.publish(master_records)
        row_count = store.count()
    manifest = Manifest.from_file(master_db, dataset="security_master", asset_type="security",
        partition_key="2026-11-02", row_count=row_count, first_key=identities[0], last_key=identities[-1],
        publication_metadata={"as_of_date": "2026-11-02", "required_groups": ["performance-fixture"]})
    manifest.write_atomic(master_dir / "manifest.json")
    return business, metadata_path, days, extended_days, identities, str(master_db)


def _fresh_process_request(connection, business: str, config: str) -> None:
    try:
        started = time.perf_counter()
        from stock_data_manage.service.web import create_app
        app = create_app(config_root=config, data_root=business)

        async def get():
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                response = await client.get(
                    f"/api/daily?start_date={os.environ['DAILY_QUERY_START']}&end_date={os.environ['DAILY_QUERY_END']}&page_size=100")
                return response.status_code, response.json()

        status, payload = asyncio.run(get())
        connection.send((status, payload.get("total"), len(payload.get("items", ())),
                         time.perf_counter() - started))
    except BaseException as exc:
        connection.send(("error", repr(exc)))
    finally:
        connection.close()


def test_daily_query_performance_representative_7862_by_100(tmp_path_factory, monkeypatch, record_property):
    """786,200 full quote records, page work, index plan, startup and repeated response timings."""
    import stock_data_manage.storage.daily as daily_storage
    import stock_data_manage.storage.metadata as metadata_storage
    from stock_data_manage.service.web import create_app

    root = tmp_path_factory.mktemp("p")
    business, database, days, history_days, identities, master_db = _build_dataset(root)
    os.environ["DAILY_QUERY_START"], os.environ["DAILY_QUERY_END"] = days[0].isoformat(), days[-1].isoformat()
    app = create_app(config_root=Path(__file__).resolve().parents[1] / "config", data_root=business)
    sql_traces, constructions, vm = [], [], {"ticks": 0}
    open_runtime = metadata_storage.open_runtime_database
    row_to_record = daily_storage._row_to_record

    def instrumented_open(*args, **kwargs):
        connection = open_runtime(*args, **kwargs)
        connection.set_trace_callback(sql_traces.append)
        connection.set_progress_handler(lambda: vm.__setitem__("ticks", vm["ticks"] + 1) or 0, 1000)
        return connection

    def count_record(bar, quotes):
        constructions.append((bar[0], bar[1]))
        return row_to_record(bar, quotes)

    monkeypatch.setattr(metadata_storage, "open_runtime_database", instrumented_open)
    monkeypatch.setattr(daily_storage, "_row_to_record", count_record)

    # First request is made by a newly spawned process. OS file cache is not cleared.
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    worker = context.Process(target=_fresh_process_request, args=(child, str(business),
        str(Path(__file__).resolve().parents[1] / "config")))
    spawn_started = time.perf_counter()
    worker.start()
    child.close()
    first = None
    first_receive_elapsed = None
    try:
        if parent.poll(30):
            first = parent.recv()
            first_receive_elapsed = time.perf_counter() - spawn_started
    finally:
        parent.close()
        worker.join(30)
        if worker.is_alive():
            worker.terminate()
            worker.join(5)

    async def measure_request(client, path):
        sql_start, vm_start, rows_start = len(sql_traces), vm["ticks"], len(constructions)
        started = time.perf_counter()
        response = await asyncio.wait_for(client.get(path), timeout=30)
        elapsed = time.perf_counter() - started
        return response, {"seconds": elapsed, "vm_steps_estimate": (vm["ticks"] - vm_start) * 1000,
            "constructed_records": len(constructions) - rows_start, "sql": sql_traces[sql_start:]}

    date_filter = f"start_date={days[0].isoformat()}&end_date={days[-1].isoformat()}"
    async def repeated_requests():
        samples = []
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            for _ in range(5):
                response, measurement = await measure_request(client, f"/api/daily?{date_filter}&page_size=100")
                measurement.update(status=response.status_code, total=response.json().get("total"),
                    items=len(response.json().get("items", ())),
                    available_adjustments=response.json().get("available_adjustments"))
                samples.append(measurement)
        return samples

    repeated = asyncio.run(repeated_requests())
    repeated_times = [sample["seconds"] for sample in repeated]
    repeated_p95 = sorted(repeated_times)[math.ceil(0.95 * len(repeated_times)) - 1]
    reuse_constructions = sum(sample["constructed_records"] for sample in repeated)

    # Deep OFFSET and a one-security, 1,000-date history exercise their distinct SQL paths.
    async def deep_default_and_growth():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            deep, deep_measurement = await measure_request(client,
                f"/api/daily?{date_filter}&page=7862&page_size=100")
            default, default_measurement = await measure_request(client, "/api/daily?page_size=100")
            growth, growth_measurement = await measure_request(client,
                f"/api/daily?security={identities[0]}&start_date={history_days[0]}&end_date={history_days[-1]}&page=10&page_size=100")
            return (deep, deep_measurement), (default, default_measurement), (growth, growth_measurement)

    (deep, deep_measurement), (default, default_measurement), (growth, growth_measurement) = asyncio.run(deep_default_and_growth())
    with sqlite3.connect(database) as connection:
        connection.execute("""UPDATE daily_quote SET status='unverified',open_text=NULL,high_text=NULL,low_text=NULL,
            close_text=NULL,volume_text=NULL,amount_text=NULL,reason='fixture has no adjusted quote',
            evidence_ref='fixture:unverified-adjustment'
            WHERE adjustment IN ('forward','backward') AND trade_date>=? AND trade_date<=?""",
            (days[0].isoformat(), days[-1].isoformat()))

    async def sparse_adjustment_request():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            return await measure_request(client, f"/api/daily?{date_filter}&page_size=100")

    sparse, sparse_measurement = asyncio.run(sparse_adjustment_request())
    with sqlite3.connect(database) as connection:
        total_bar_rows = connection.execute("SELECT count(*) FROM daily_bar").fetchone()[0]
        total_quote_rows = connection.execute("SELECT count(*) FROM daily_quote").fetchone()[0]
        plans = {}
        plan_sources = {}
        for sample in repeated:
            for statement in sample["sql"]:
                normalized = statement.lstrip().upper()
                if normalized.startswith("SELECT COUNT(*) FROM DAILY_BAR B"):
                    plan_sources.setdefault("count", statement)
                elif normalized.startswith("SELECT 1 FROM DAILY_QUOTE Q JOIN DAILY_BAR B"):
                    for adjustment in ("NONE", "FORWARD", "BACKWARD"):
                        if f"Q.ADJUSTMENT='{adjustment}'" in normalized:
                            plan_sources.setdefault("available_exists_" + adjustment.lower(), statement)
                elif normalized.startswith("SELECT B.INSTRUMENT_ID,B.TRADE_DATE,B.ASSET_TYPE"):
                    plan_sources.setdefault("page_bars_order_limit", statement)
                elif normalized.startswith("SELECT INSTRUMENT_ID,TRADE_DATE,ADJUSTMENT,STATUS"):
                    plan_sources.setdefault("page_quote_groups", statement)
        for statement in deep_measurement["sql"]:
            normalized = statement.lstrip().upper()
            if normalized.startswith("SELECT B.INSTRUMENT_ID,B.TRADE_DATE,B.ASSET_TYPE"):
                plan_sources.setdefault("deep_page_sort_offset", statement)
        for statement in default_measurement["sql"]:
            normalized = statement.lstrip().upper()
            if normalized.startswith("SELECT COUNT(*) FROM DAILY_BAR B"):
                plan_sources.setdefault("default_count", statement)
            elif normalized.startswith("SELECT 1 FROM DAILY_QUOTE Q JOIN DAILY_BAR B") and "Q.ADJUSTMENT='NONE'" in normalized:
                plan_sources.setdefault("default_available_exists_none", statement)
            elif normalized.startswith("SELECT B.INSTRUMENT_ID,B.TRADE_DATE,B.ASSET_TYPE"):
                plan_sources.setdefault("default_page_sort", statement)
        for label, statement in plan_sources.items():
            plans[label] = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + statement).fetchall()]
        for statement in sparse_measurement["sql"]:
            normalized = statement.lstrip().upper()
            if (normalized.startswith("SELECT 1 FROM DAILY_QUOTE Q JOIN DAILY_BAR B")
                    and "Q.ADJUSTMENT='FORWARD'" in normalized):
                plan_sources["sparse_forward_negative_scope"] = statement
                plans["sparse_forward_negative_scope"] = [row[3] for row in connection.execute(
                    "EXPLAIN QUERY PLAN " + statement).fetchall()]

    sample = {
        "data_kind": "synthetic SQL/API workload; not collect_task publication or market coverage evidence",
        "universe_securities": 7862,
        "core_weekday_dates_not_exchange_calendar_verified": 100,
        "core_daily_bar_rows": 786200,
        "core_quote_rows": 2358600,
        "additional_history_bar_rows": 900,
        "total_database_daily_bar_rows": total_bar_rows,
        "total_database_daily_quote_rows": total_quote_rows,
        "quote_density": "all three adjustments available; all OHLC, volume and amount values populated",
        "history_growth": "one existing security has 1,000 available three-adjustment dates",
        "database_bytes": database.stat().st_size,
        "security_master_bytes": Path(master_db).stat().st_size,
        "first_new_process_request_seconds": round(first[3], 6) if first and first[0] == 200 else None,
        "first_new_process_total": first[1] if first and first[0] == 200 else None,
        "first_new_process_items": first[2] if first and first[0] == 200 else None,
        "first_process_spawn_to_receive_seconds": round(first_receive_elapsed, 6) if first_receive_elapsed is not None else None,
        "first_process_exit_code_after_join_or_owned_termination": worker.exitcode,
        "first_process_result": first,
        "first_sample_os_cache_cleared": False,
        "reuse_request_seconds_n5": [round(value, 6) for value in repeated_times],
        "reuse_request_seconds_p95_nearest_rank": round(repeated_p95, 6),
        "reuse_requests": [{key: value for key, value in result.items() if key != "sql"}
                           for result in repeated],
        "requested_page_size": 100,
        "constructed_daily_records_for_reuse_requests": reuse_constructions,
        "page_quote_groups_requested": 100,
        "page_quote_rows_expected_at_three_groups_per_parent": 300,
        "deep_page_measurement": {key: value for key, value in deep_measurement.items() if key != "sql"},
        "growth_query_measurement": {key: value for key, value in growth_measurement.items() if key != "sql"},
        "default_query_measurement": {key: value for key, value in default_measurement.items() if key != "sql"},
        "sparse_adjustment_query_measurement": {key: value for key, value in sparse_measurement.items() if key != "sql"},
        "sqlite_vm_steps_estimate_total_all_in_process_requests": vm["ticks"] * 1000,
        "sqlite_vm_progress_callback_interval": 1000,
        "query_plans": plans,
        "sql_statement_count": len(sql_traces),
        "query_plans_captured_from_actual_api_sql": plans,
        "deep_page": {"page": 7862, "status": deep.status_code, "total": deep.json().get("total"),
            "items": len(deep.json().get("items", ()))},
        "one_security_1000_date_query": {"status": growth.status_code, "total": growth.json().get("total"),
            "page": 10, "items": len(growth.json().get("items", ()))},
        "default_query": {"status": default.status_code, "total": default.json().get("total"),
            "items": len(default.json().get("items", ()))},
        "sparse_adjustment_query": {"status": sparse.status_code, "total": sparse.json().get("total"),
            "items": len(sparse.json().get("items", ())),
            "available_adjustments": sparse.json().get("available_adjustments"),
            "quote_states": sparse.json().get("items", [{}])[0].get("quote_states")},
        "page_quote_key_pairs": max((len(re.findall(r"\('(?:XSHG|XSHE|BSE):[0-9]{6}','[0-9]{4}-[0-9]{2}-[0-9]{2}'\)", statement))
            for result in repeated for statement in result["sql"]
            if statement.lstrip().upper().startswith("SELECT INSTRUMENT_ID,TRADE_DATE,ADJUSTMENT,STATUS")), default=0),
    }
    report_path = Path(os.environ.get("DAILY_QUERY_PERF_REPORT", root / "performance.json"))
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(sample, ensure_ascii=False, indent=2), encoding="utf-8")
    record_property("daily_query_performance_report", str(report_path))
    record_property("daily_query_first_new_process_seconds", sample["first_new_process_request_seconds"])
    record_property("daily_query_reuse_p95_seconds", sample["reuse_request_seconds_p95_nearest_rank"])
    record_property("daily_query_sqlite_vm_steps_estimate", sample["sqlite_vm_steps_estimate_total_all_in_process_requests"])
    assert first and first[0] == 200 and first[1:3] == (786200, 100), first
    assert worker.exitcode == 0 and not worker.is_alive()
    assert (total_bar_rows, total_quote_rows) == (787100, 2361300)
    assert all(result["status"] == 200 and result["total"] == 786200 and result["items"] == 100
               and result["available_adjustments"] == ["none", "forward", "backward"] for result in repeated)
    assert reuse_constructions == 500 and all(sample["constructed_records"] <= 100 for sample in repeated)
    assert deep.status_code == 200 and deep.json().get("total") == 786200 and len(deep.json().get("items", ())) == 100
    assert default.status_code == 200 and default.json().get("total") == 787100 and len(default.json().get("items", ())) == 100
    assert growth.status_code == 200 and growth.json().get("total") == 1000
    assert sparse.status_code == 200 and sparse.json().get("total") == 786200
    assert sparse.json().get("available_adjustments") == ["none"]
    assert sparse.json()["items"][0]["quote_states"] == {
        "none": "available", "forward": "unverified", "backward": "unverified"}
    assert set(plans) == {"count", "available_exists_none", "available_exists_forward", "available_exists_backward",
                          "page_bars_order_limit", "page_quote_groups", "deep_page_sort_offset", "default_count",
                          "default_available_exists_none", "default_page_sort", "sparse_forward_negative_scope"}
    assert sample["page_quote_key_pairs"] == 100
    assert all("TEMP B-TREE" not in detail.upper() for label, details in plans.items()
               if label in {"page_bars_order_limit", "deep_page_sort_offset", "default_page_sort"}
               for detail in details)
    assert all("SCAN Q" not in detail.upper() for label, details in plans.items()
               if label.startswith("available_exists_") or label == "default_available_exists_none"
               for detail in details)
    assert all("SCAN Q" not in detail.upper() for detail in plans["sparse_forward_negative_scope"])
    assert sample["first_new_process_request_seconds"] is not None
    assert sample["first_new_process_request_seconds"] < 2.0
    assert repeated_p95 < 1.0
    assert deep_measurement["seconds"] < 2.0 and default_measurement["seconds"] < 2.0
    assert growth_measurement["seconds"] < 1.0
    assert sparse_measurement["seconds"] < 2.0 and sparse_measurement["constructed_records"] == 100
