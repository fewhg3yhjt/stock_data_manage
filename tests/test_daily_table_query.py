"""Read-only SQL daily query contracts through the real ASGI route."""
from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import shutil
from datetime import date
from pathlib import Path

from httpx import ASGITransport, AsyncClient
import pytest

from test_daily_collection_e2e import CONFIG, source_task
from stock_data_manage.pipeline.inputs import collect_task
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.service.web import create_app
from stock_data_manage.storage.parquet import CanonicalPartitionStore


@pytest.fixture
def tmp_path(tmp_path_factory):
    return tmp_path_factory.mktemp("q")


def _get_many(app, paths):
    async def run():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            return [await client.get(path) for path in paths]
    return asyncio.run(run())


def _publish_source_task(tmp_path, business, *, day, symbols, task_id, day_only=False, monkeypatch):
    import test_daily_collection_e2e as fixtures

    monkeypatch.setattr(fixtures, "DAY", day)
    definition = source_task(tmp_path / ("source-" + task_id), symbols=symbols,
                             task_id=task_id, day_only_adjustments=day_only)
    result = collect_task(definition=definition, config_root=CONFIG, data_root=business, mode="replay",
        collector_options={"pacer": RequestPacer(wait=lambda _: None)})
    assert result["status"] == "published", result.get("error")
    return result


def _published_master(business, records, *, as_of="2026-11-02"):
    from stock_data_manage.domain import AssetType, Exchange
    from stock_data_manage.service.instruments import SecurityMasterStore
    from stock_data_manage.storage.integrity import Manifest

    directory = business / "canonical" / "security_master" / "current"
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / "security_master.duckdb"
    with SecurityMasterStore(str(database)) as store:
        store.publish(records)
        count = store.count()
        identities = sorted(record.instrument_id for record in records)
    manifest = Manifest.from_file(database, dataset="security_master", asset_type="security",
        partition_key=as_of, row_count=count, first_key=identities[0], last_key=identities[-1],
        publication_metadata={"as_of_date": as_of, "required_groups": ["fixture"]})
    manifest.write_atomic(directory / "manifest.json")
    return directory


def test_published_daily_values_are_read_through_real_asgi_route(tmp_path, monkeypatch):
    import requests
    import pyarrow.parquet as parquet
    import stock_data_manage.storage.daily as daily_storage
    import stock_data_manage.storage.metadata as metadata_storage

    business = tmp_path / "business"
    _publish_source_task(tmp_path, business, day="2026-10-09",
        symbols=("sh600001", "sh600002"), task_id="query-page", monkeypatch=monkeypatch)
    no_network = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("GET attempted a Provider request"))
    monkeypatch.setattr(requests.Session, "send", no_network)

    retired = business / "canonical" / "2026-10-09" / "daily_bar" / "etf" / "data.parquet"
    retired.parent.mkdir(parents=True)
    retired.write_bytes(b"retired daily file must not be read")
    parquet_reads = []
    original_parquet_read = parquet.read_table
    def guard_parquet_read(source, *args, **kwargs):
        if str(source).lower().endswith(".parquet"):
            parquet_reads.append(str(source))
            raise AssertionError("GET opened a formal daily Parquet file")
        return original_parquet_read(source, *args, **kwargs)
    monkeypatch.setattr(parquet, "read_table", guard_parquet_read)
    monkeypatch.setattr(CanonicalPartitionStore, "read_daily",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("GET called read_daily")))

    statements = []
    original_open = metadata_storage.open_runtime_database
    def traced_open(*args, **kwargs):
        connection = original_open(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection
    monkeypatch.setattr(metadata_storage, "open_runtime_database", traced_open)
    constructed = []
    original_convert = daily_storage._row_to_record
    def traced_convert(bar, quotes):
        constructed.append(bar[0])
        return original_convert(bar, quotes)
    monkeypatch.setattr(daily_storage, "_row_to_record", traced_convert)

    app = create_app(config_root=CONFIG, data_root=business)
    database = business / "metadata" / "metadata.sqlite3"
    before_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    responses = _get_many(app, [
        "/api/daily",
        "/api/daily?page=2&page_size=1",
        "/api/daily?security=sh600002&start_date=2026-10-09&end_date=2026-10-09",
        "/api/daily?adjustment=forward",
        "/api/daily?adjustment=backward",
        "/api/daily?security=sh600099&start_date=2026-10-09&end_date=2026-10-09",
        "/api/daily?adjustment=invalid",
        "/api/daily?start_date=2026-10-10&end_date=2026-10-09",
        "/api/daily?start_date=invalid",
        "/api/daily?page=0",
        "/api/daily?page_size=101",
        "/api/daily?asset_type=unknown",
        "/api/daily?page=10&page_size=1",
    ])
    (normal, page_two, selected, forward, backward, empty, invalid_adjustment, invalid_range,
     invalid_date, invalid_page, invalid_page_size, invalid_asset, beyond_last_page) = responses
    assert normal.status_code == 200
    payload = normal.json()
    assert (payload["total"], payload["page"], payload["page_size"], payload["pages"]) == (2, 1, 50, 1)
    assert payload["adjustment"] == "none"
    assert payload["available_adjustments"] == ["none", "forward", "backward"]
    first = payload["items"][0]
    assert first["instrument_id"] == "XSHG:600001" and first["trade_date"] == "2026-10-09"
    assert first["values"]["close"] == "10"
    assert set(first["flags"]) == {"source_volume_rounded_to_100_shares", "source_amount_rounded_to_100_CNY"}
    assert first["quote_states"] == {"none": "available", "forward": "available", "backward": "available"}
    assert first["has_source_evidence"] and "raw_ref" not in first
    assert first["security_name_status"] == "unavailable"
    assert page_two.status_code == 200 and page_two.json()["items"][0]["instrument_id"] == "XSHG:600002"
    assert page_two.json()["total"] == 2
    assert selected.status_code == 200 and selected.json()["total"] == 1
    assert selected.json()["items"][0]["instrument_id"] == "XSHG:600002"
    assert forward.status_code == backward.status_code == 200
    assert forward.json()["items"][0]["values"]["close"] == "9"
    assert backward.json()["items"][0]["values"]["close"] == "11"
    assert empty.status_code == 200 and empty.json()["items"] == [] and empty.json()["total"] == 0
    assert empty.json()["pages"] == 1
    assert invalid_adjustment.status_code == 400
    assert invalid_range.status_code == 400
    assert invalid_date.status_code == 422
    assert invalid_page.status_code == invalid_page_size.status_code == 422
    assert invalid_asset.status_code == 400
    assert beyond_last_page.status_code == 200
    assert beyond_last_page.json()["items"] == [] and beyond_last_page.json()["total"] == 2
    assert not parquet_reads
    assert not any("collection_task" in statement.lower() or "task_summary" in statement.lower()
                   for statement in statements)
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before_hash

    constructed.clear()
    page = _get_many(app, ["/api/daily?page_size=1"])[0]
    assert page.status_code == 200 and len(page.json()["items"]) == 1
    assert constructed == ["XSHG:600001"]


def test_available_adjustments_cover_all_matching_rows_not_only_the_page(tmp_path, monkeypatch):
    business = tmp_path / "business"
    _publish_source_task(tmp_path, business, day="2026-10-09", symbols=("sh600001",),
        task_id="query-sparse", day_only=True, monkeypatch=monkeypatch)
    _publish_source_task(tmp_path, business, day="2026-10-09", symbols=("sh600002",),
        task_id="query-dense", monkeypatch=monkeypatch)
    response = _get_many(create_app(config_root=CONFIG, data_root=business), [
        "/api/daily?page_size=1&adjustment=backward"])[0]
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2 and payload["items"][0]["instrument_id"] == "XSHG:600001"
    assert payload["items"][0]["status"] == "unverified"
    assert payload["available_adjustments"] == ["none", "forward", "backward"]


def test_selected_quote_corruption_returns_conflict_instead_of_false_empty(tmp_path, monkeypatch):
    business = tmp_path / "business"
    _publish_source_task(tmp_path, business, day="2026-10-09", symbols=("sh600001",),
        task_id="query-corrupt", monkeypatch=monkeypatch)
    database = business / "metadata" / "metadata.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM daily_quote WHERE instrument_id=? AND trade_date=? AND adjustment=?",
                           ("XSHG:600001", date(2026, 10, 9), "none"))
    response = _get_many(create_app(config_root=CONFIG, data_root=business), ["/api/daily"])[0]
    assert response.status_code == 409
    assert response.json()["detail"] == "正式日K数据完整性检查失败"


def test_missing_or_old_sqlite_schema_is_not_created_or_read_as_empty(tmp_path):
    app = create_app(config_root=CONFIG, data_root=tmp_path / "missing")
    missing = _get_many(app, ["/api/daily"])[0]
    assert missing.status_code == 503
    assert not (tmp_path / "missing" / "metadata" / "metadata.sqlite3").exists()
    assert not (tmp_path / "missing" / "metadata").exists()

    empty_root = tmp_path / "empty-schema"
    database = empty_root / "metadata" / "metadata.sqlite3"
    database.parent.mkdir(parents=True)
    with sqlite3.connect(database):
        pass
    empty_app = create_app(config_root=CONFIG, data_root=empty_root)
    empty = _get_many(empty_app, ["/api/daily"])[0]
    assert empty.status_code == 503
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []

    legacy_root = tmp_path / "legacy"
    legacy_db = legacy_root / "metadata" / "metadata.sqlite3"
    legacy_db.parent.mkdir(parents=True)
    legacy_db.write_bytes(b"not an SQLite database")
    legacy = _get_many(create_app(config_root=CONFIG, data_root=legacy_root), ["/api/daily"])[0]
    assert legacy.status_code == 503
    assert legacy_db.read_bytes() == b"not an SQLite database"


def test_symbol_successions_are_sql_scoped_by_market_asset_and_effective_date(tmp_path, monkeypatch):
    import yaml
    from stock_data_manage.domain import AssetType, Exchange
    from stock_data_manage.service.instruments import SecurityRecord

    business = tmp_path / "business"
    before = "2026-10-09"
    after = "2026-10-11"
    _publish_source_task(tmp_path, business, day=before,
        symbols=("sh600001", "sz000001", "bj430001"), task_id="successor-before",
        monkeypatch=monkeypatch)
    _publish_source_task(tmp_path, business, day=after,
        symbols=("sh600001", "sz000001", "bj430001"), task_id="successor-after",
        monkeypatch=monkeypatch)
    _published_master(business, [
        SecurityRecord("XSHG:600001", "600002", Exchange.XSHG, AssetType.STOCK, name="沪市承接"),
        SecurityRecord("XSHE:000001", "000002", Exchange.XSHE, AssetType.STOCK, name="深市承接"),
        SecurityRecord("BSE:430001", "430002", Exchange.BSE, AssetType.STOCK, name="北交所承接"),
    ])

    config = tmp_path / "config"
    shutil.copytree(CONFIG, config)
    security_path = config / "datasets" / "security_master.yaml"
    settings = yaml.safe_load(security_path.read_text(encoding="utf-8"))
    settings["symbol_successions"] = list(settings.get("symbol_successions", [])) + [
        {"market": market, "asset_type": "stock", "old_symbol": old, "new_symbol": new,
         "effective_from": "2026-10-10", "evidence_refs": ["fixture:daily-query-successor"]}
        for market, old, new in [("XSHG", "600001", "600002"),
                                 ("XSHE", "000001", "000002"),
                                 ("BSE", "430001", "430002")]
    ]
    security_path.write_text(yaml.safe_dump(settings, allow_unicode=True, sort_keys=False), encoding="utf-8")
    app = create_app(config_root=config, data_root=business)
    paths = [
        f"/api/daily?security={old}&start_date={before}&end_date={before}&asset_type=stock"
        for old in ("sh600001", "sz000001", "bj430001")
    ] + [
        f"/api/daily?security={new}&start_date={before}&end_date={before}&asset_type=stock"
        for new in ("sh600002", "sz000002", "bj430002")
    ] + [
        f"/api/daily?security={new}&start_date={after}&end_date={after}&asset_type=stock"
        for new in ("sh600002", "sz000002", "bj430002")
    ]
    responses = _get_many(app, paths)
    assert [response.json()["total"] for response in responses[:3]] == [1, 1, 1]
    assert [response.json()["total"] for response in responses[3:6]] == [0, 0, 0]
    for response, expected_symbol, expected_id, expected_name in zip(
            responses[6:], ("sh600002", "sz000002", "bj430002"),
            ("XSHG:600001", "XSHE:000001", "BSE:430001"),
            ("沪市承接", "深市承接", "北交所承接")):
        assert response.status_code == 200 and response.json()["total"] == 1
        item = response.json()["items"][0]
        assert item["symbol"] == expected_symbol and item["instrument_id"] == expected_id
        assert item["symbol_relation"]["evidence_refs"] == ["fixture:daily-query-successor"]
        assert item["security_name"] == expected_name and item["security_name_status"] == "available"
        assert item["name_source_date"] == "2026-11-02"

    # The project relation for XSHE:399427 applies only to index. A same-code
    # stock row must not be included through the relation's bounded SQL alias.
    _publish_source_task(tmp_path, business, day="2026-10-29", symbols=("sz399427",),
        task_id="successor-cross-asset", monkeypatch=monkeypatch)
    cross_asset = _get_many(create_app(config_root=CONFIG, data_root=business), [
        "/api/daily?security=sz980070&start_date=2026-10-29&end_date=2026-10-29"])[0]
    assert cross_asset.status_code == 200 and cross_asset.json()["total"] == 0


def test_published_index_successor_and_close_only_values_project_through_asgi(tmp_path):
    from datetime import datetime, timezone
    from decimal import Decimal
    from stock_data_manage.domain import AssetType, DailyRecord, Exchange, QuoteGroup, QuoteStatus
    from stock_data_manage.service.instruments import SecurityRecord
    from stock_data_manage.storage.daily import DailyBarStore, initialize_schema
    from stock_data_manage.storage.metadata import MetadataStore
    import test_daily_table_storage as storage_fixtures

    business = tmp_path / "business"
    date_value = date(2026, 11, 5)
    now = datetime(2026, 11, 9, 5, 0, tzinfo=timezone.utc)
    full = storage_fixtures._record("XSHE:399427", "2061.12345678", asset=AssetType.INDEX)
    close_only = DailyRecord("XSHG:000816", date_value, AssetType.INDEX,
        QuoteGroup(QuoteStatus.AVAILABLE, close=Decimal("12345.12345678"), source_provider="fixture",
            endpoint="index-close", raw_object_path="raw/close-only.json", fetch_time=now,
            value_contract="close_only"),
        QuoteGroup(QuoteStatus.UNVERIFIED, reason="fixture has no forward adjustment"),
        QuoteGroup(QuoteStatus.UNVERIFIED, reason="fixture has no backward adjustment"))
    # Keep the successor row on the same queried date as the close-only row.
    full = DailyRecord(full.instrument_id, date_value, full.asset_type, full.none, full.forward, full.backward)

    database = business / "metadata" / "metadata.sqlite3"
    database.parent.mkdir(parents=True)
    with MetadataStore(database) as metadata:
        initialize_schema(metadata.connection)
        identities = [full.instrument_id, close_only.instrument_id]
        storage_fixtures._seed_task(metadata.connection, "query-index-projection", 1, identities,
                                    asset="index")
        store = DailyBarStore(metadata.connection)
        storage_fixtures._publish(store, task="query-index-projection", order=1,
            records=[full, close_only], scope=identities, key="index-projection-commit",
            publication="index-projection-publication", asset_type=AssetType.INDEX)
    _published_master(business, [
        SecurityRecord("XSHE:399427", "980070", Exchange.XSHE, AssetType.INDEX, name="中证承接指数"),
        SecurityRecord("XSHG:000816", "000816", Exchange.XSHG, AssetType.INDEX, name="中证地产指数"),
    ])

    app = create_app(config_root=CONFIG, data_root=business)
    responses = _get_many(app, [
        "/api/daily?security=sz980070&asset_type=index&start_date=2026-11-05&end_date=2026-11-05",
        "/api/daily?security=sz399427&asset_type=index&start_date=2026-11-05&end_date=2026-11-05",
        "/api/daily?security=sh000816&asset_type=index&start_date=2026-11-05&end_date=2026-11-05",
    ])
    assert [response.status_code for response in responses] == [200, 200, 200]
    successor, legacy, close = [response.json()["items"][0] for response in responses]
    assert successor["instrument_id"] == legacy["instrument_id"] == "XSHE:399427"
    assert successor["symbol"] == legacy["symbol"] == "sz980070"
    assert successor["symbol_relation"]["evidence_refs"]
    assert successor["security_name"] == legacy["security_name"] == "中证承接指数"
    assert successor["name_source_date"] == legacy["name_source_date"] == "2026-11-02"
    assert successor["values"]["close"] == legacy["values"]["close"] == "2061.12345678"
    assert close["instrument_id"] == "XSHG:000816" and close["value_kind"] == "index_points"
    assert close["quote_unit"] == "point"
    assert close["values"] == {"open": None, "high": None, "low": None, "close": "12345.12345678",
                                "volume": None, "amount": None}
    assert close["value_contract"] == "close_only"
    assert close["quote_states"]["none"] == "available"
    assert close["quote_states"]["forward"] == close["quote_states"]["backward"] == "unverified"


def test_live_wal_writer_does_not_block_a_consistent_asgi_read(tmp_path, monkeypatch):
    business = tmp_path / "business"
    _publish_source_task(tmp_path, business, day="2026-10-09", symbols=("sh600001",),
        task_id="query-wal-reader", monkeypatch=monkeypatch)
    database = business / "metadata" / "metadata.sqlite3"
    with sqlite3.connect(database, isolation_level=None) as writer:
        assert writer.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE daily_quote SET close_text='999' WHERE instrument_id=? AND adjustment=?",
                       ("XSHG:600001", "none"))
        response = _get_many(create_app(config_root=CONFIG, data_root=business), ["/api/daily"])[0]
        assert response.status_code == 200
        assert response.json()["items"][0]["values"]["close"] == "10"
        writer.execute("ROLLBACK")


def test_exclusive_sqlite_writer_maps_busy_to_retryable_503(tmp_path, monkeypatch):
    business = tmp_path / "business"
    _publish_source_task(tmp_path, business, day="2026-10-09", symbols=("sh600001",),
        task_id="query-busy", monkeypatch=monkeypatch)
    database = business / "metadata" / "metadata.sqlite3"
    with sqlite3.connect(database, isolation_level=None) as writer:
        assert writer.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower() == "delete"
        writer.execute("BEGIN EXCLUSIVE")
        writer.execute("UPDATE daily_quote SET close_text='999' WHERE instrument_id=? AND adjustment=?",
                       ("XSHG:600001", "none"))
        response = _get_many(create_app(config_root=CONFIG, data_root=business), ["/api/daily"])[0]
        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"
        assert response.json()["detail"] == "日K查询繁忙，请稍后重试"
        writer.execute("ROLLBACK")
