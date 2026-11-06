import hashlib
import json
import sqlite3
from datetime import date, timedelta
from time import perf_counter

from stock_data_manage.storage import read_index


DAY = date(2026, 11, 6)


def test_source_report_batch_uses_one_transaction_and_keeps_query_selectors(tmp_path, monkeypatch, record_property):
    run_root = tmp_path / "workspace"
    source_dir = run_root / "sources"
    source_dir.mkdir(parents=True)
    report_specs = []
    for report_number in range(50):
        symbol = f"sh{600000 + report_number:06d}"
        rows = [{"symbol": symbol, "trade_date": (DAY - timedelta(days=offset)).isoformat(),
                 "close": f"{100 + report_number}.25"} for offset in range(641)]
        source_path = source_dir / f"{symbol}.source_rows.json"
        payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        source_path.write_bytes(payload)
        report_path = source_dir / f"{symbol}.report.json"
        report = {
            "dataset": "daily_bar", "input_id": "ASTOCK-002-daily", "provider": "tencent",
            "endpoint": "kline_daily", "data_date": DAY.isoformat(), "asset_type": "stock",
            "status": "candidate_complete", "row_count": 1,
            "parameters": {"code": symbol, "adjust": "none"},
            "source_rows": {"path": source_path.relative_to(run_root).as_posix(),
                            "sha256": hashlib.sha256(payload).hexdigest(), "row_count": len(rows)},
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
        report_specs.append((report_path, {"stock_code": "symbol", "trade_date": "trade_date"}))

    connections = []
    statements = []
    original_connect = read_index._connect_query_database

    def tracked_connect(path, **kwargs):
        connection = original_connect(path, **kwargs)
        connection.set_trace_callback(statements.append)
        connections.append(connection)
        return connection

    monkeypatch.setattr(read_index, "_connect_query_database", tracked_connect)
    database = tmp_path / "source_rows_query_index.sqlite3"
    started = perf_counter()
    outcomes = read_index.append_source_reports(report_specs, run_root=run_root,
        config_root=tmp_path / "config", query_db_path=database)
    elapsed = perf_counter() - started
    record_property("daily_source_index_batch_50_reports_32050_rows_seconds", round(elapsed, 3))

    assert len(outcomes) == 50 and all(indexed and error is None for _, indexed, error in outcomes)
    assert len(connections) == 1
    assert sum(statement.strip().upper() == "BEGIN IMMEDIATE" for statement in statements) == 1
    assert elapsed < 5.0
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_reports").fetchone()[0] == 50
        assert connection.execute("SELECT COUNT(*) FROM source_selectors").fetchone()[0] == 50 * 641

    page = read_index.query_source_data_page(database, dataset="daily_bar", security="sh600000",
        target_market="sh", target_code="600000", start_date=DAY.isoformat(), end_date=DAY.isoformat(),
        provider="tencent", asset_type="stock", adjustment="none", catalog={}, references={},
        page=1, page_size=10)
    assert page["total"] == 1
    assert page["items"][0]["symbol"] == "sh600000"
    assert page["items"][0]["source_row_index"] == 0
