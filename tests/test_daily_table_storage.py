from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
import hashlib
import multiprocessing

import pytest

from stock_data_manage.domain.models import AssetType, DailyRecord, QuoteGroup, QuoteStatus
from stock_data_manage.storage.daily import DailyBarStore, NoTradeFact, initialize_schema
from stock_data_manage.storage.metadata import MetadataStore, open_runtime_database

DAY = date(2026, 11, 5)
NOW = datetime(2026, 11, 9, 5, 0, tzinfo=timezone.utc)
ASSET = AssetType.STOCK


def _record(identity: str, close: str = "12.34000000", *, adjusted: bool = False,
            asset: AssetType = ASSET) -> DailyRecord:
    close_value = Decimal(close)
    maximum = Decimal("999999999999999999999999999999.99999999")
    common = dict(open=Decimal("12.00000000"), high=min(maximum, max(Decimal("13.00000000"), close_value + 1)),
                  low=min(Decimal("11.00000000"), close_value, Decimal("12.00000000")) / 2,
                  close=close_value, volume=Decimal("1.23000000"),
                  amount=Decimal("2.34000000"), source_provider="fixture", endpoint="/daily",
                  raw_object_path="raw/2026-11-05.json", fetch_time=NOW,
                  normalizer_version="norm-v1", capability_version="cap-v1", flags=("verified",))
    none = QuoteGroup(status=QuoteStatus.AVAILABLE, **common)
    if adjusted:
        forward = QuoteGroup(status=QuoteStatus.AVAILABLE, **common)
        backward = QuoteGroup(status=QuoteStatus.AVAILABLE, **common)
    else:
        forward = QuoteGroup(status=QuoteStatus.UNVERIFIED, reason="provider did not supply adjusted quote")
        backward = QuoteGroup(status=QuoteStatus.NOT_APPLICABLE, reason="not applicable", evidence_ref="raw/calendar.json")
    return DailyRecord(identity, DAY, asset, none, forward, backward)


def _scope(identities: list[str], asset: str = "stock") -> dict:
    return {"dataset": "daily_bar", "frequency": "1d", "asset_types": [asset],
            "securities_by_date": {DAY.isoformat(): identities}}


def _seed_task(connection, task_id: str, order: int, identities: list[str], status="collecting", asset="stock") -> None:
    scope = _scope(identities, asset)
    connection.execute("""INSERT INTO collection_task
        (task_id,definition_hash,dataset,status,state_json,updated_at,queue_order,first_execution_order)
        VALUES (?,?,? ,?,?,?,1,?)""",
        [task_id, "frozen", "daily_bar", status,
         json.dumps({"task_id": task_id, "status": status, "first_execution_order": order}), NOW,
         order])
    connection.execute("INSERT INTO collection_task_context VALUES (?,?,?,?)",
        [task_id, json.dumps(scope), json.dumps({}), json.dumps(scope)])


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "daily-tables.sqlite3"
    metadata = MetadataStore(path)
    initialize_schema(metadata.connection)
    yield path, metadata.connection
    metadata.close()


def _publish(store, *, task="task-a", order=1, records=(), no_trade=None, scope=None,
             key="commit-1", publication="pub-1", asset_type=None):
    if asset_type is None:
        asset_type = records[0].asset_type if records else ASSET
    task_row = store.connection.execute("SELECT t.state_json,c.active_scope_json,c.snapshot_json FROM collection_task t "
        "JOIN collection_task_context c USING(task_id) WHERE t.task_id=?", [task]).fetchone()
    task_state = json.loads(task_row[0])
    task_snapshot = json.loads(task_row[2] or "{}")
    if "daily_overwrite_grant" not in task_snapshot:
        active_scope = json.loads(task_row[1])
        owners = store.current_owners(active_scope)
        task_snapshot["daily_overwrite_grant"] = {"scope_hash": hashlib.sha256(
            json.dumps(active_scope, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest(),
            "owners": list(owners)}
        store.connection.execute("UPDATE collection_task_context SET snapshot_json=? WHERE task_id=?",
            [json.dumps(task_snapshot, sort_keys=True, ensure_ascii=False), task])
    authorization = task_snapshot["daily_overwrite_grant"]
    return store.publish_partition(trade_date=DAY, asset_type=asset_type, task_id=task,
        first_execution_order=order, publication_id=publication, idempotency_key=key,
        scope=frozenset(scope or [r.instrument_id for r in records] + list((no_trade or {}).keys())),
        records=tuple(records), no_trade=no_trade or {}, raw_evidence_refs=("raw/2026-11-05.json",),
        candidate_ref="candidate/2026-11-05.parquet", committed_at=NOW, authorization=authorization)


def test_all_quote_fields_decimal_boundaries_nulls_roundtrip(database):
    _, connection = database
    identity = "XSHG:600000"
    _seed_task(connection, "task-a", 1, [identity])
    store = DailyBarStore(connection)
    record = _record(identity, "999999999999999999999999999999.99999999", adjusted=True)
    _publish(store, records=[record])
    result = store.get(identity, DAY)
    assert result == record
    assert result.none.close == Decimal("999999999999999999999999999999.99999999")
    assert connection.execute("SELECT typeof(close_text),close_text FROM daily_quote WHERE instrument_id=? AND adjustment='none'", [identity]).fetchone() == ("text", "999999999999999999999999999999.99999999")
    assert result.forward.flags == ("verified",)
    assert result.none.fetch_time == NOW
    from stock_data_manage.storage.daily import _decimal_text
    assert _decimal_text(Decimal("0E-100")) == "0"
    assert _decimal_text(Decimal("12.34000000")) == "12.34"


def test_nullable_quote_fields_roundtrip(database):
    _, connection = database
    identity = "XSHG:600000"
    _seed_task(connection, "task-a", 1, [identity])
    store = DailyBarStore(connection)
    complete = _record(identity)
    record = DailyRecord(identity, DAY, ASSET,
        QuoteGroup(status=QuoteStatus.AVAILABLE, open=Decimal("1"), high=Decimal("2"), low=Decimal("0.5"),
                   close=Decimal("1.5"), source_provider="p", endpoint="e", raw_object_path="raw/a",
                   fetch_time=NOW, volume=None, amount=None, flags=(), normalizer_version="", capability_version=""),
        complete.forward, complete.backward)
    _publish(store, records=[record])
    assert store.get(identity, DAY) == record


def test_identity_local_overwrite_and_failed_candidate_retains_existing(database):
    _, connection = database
    a, b = "XSHG:600000", "XSHE:000001"
    _seed_task(connection, "task-a", 1, [a, b])
    store = DailyBarStore(connection)
    _publish(store, records=[_record(a), _record(b)])
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-a'")
    _seed_task(connection, "task-b", 2, [a, b])
    _publish(store, task="task-b", order=2, records=[_record(a, "15.00000000")],
             scope=[a, b], key="commit-2", publication="pub-2")
    assert store.get(a, DAY).none.close == Decimal("15.00000000")
    assert store.get(b, DAY).none.close == Decimal("12.34000000")
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-b'")
    _seed_task(connection, "task-c", 3, [a])
    before = connection.execute("SELECT data_revision FROM partition_status WHERE dataset='daily_bar'").fetchone()[0]
    _publish(store, task="task-c", order=3, records=[_record(a, "15.00000000")], key="same-value", publication="pub-same-value")
    assert connection.execute("SELECT last_publication_id FROM daily_bar WHERE instrument_id=?", [a]).fetchone()[0] == "pub-same-value"
    assert connection.execute("SELECT data_revision FROM partition_status WHERE dataset='daily_bar'").fetchone()[0] == before + 1
    with pytest.raises(ValueError, match="empty candidate"):
        _publish(store, task="task-b", order=2, records=(), no_trade={}, scope=[a], key="failed", publication="pub-failed")
    assert store.get(a, DAY).none.close == Decimal("15.00000000")


def test_no_trade_replaces_bar_and_later_bar_clears_no_trade(database):
    _, connection = database
    identity = "XSHG:600000"
    _seed_task(connection, "task-a", 1, [identity])
    store = DailyBarStore(connection)
    _publish(store, records=[_record(identity)])
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-a'")
    _seed_task(connection, "task-b", 2, [identity])
    _publish(store, task="task-b", order=2, records=(), no_trade={identity: NoTradeFact("exchange confirmed no trade", "raw/no-trade.json")}, key="no-trade", publication="pub-no-trade")
    assert store.get(identity, DAY) is None
    assert connection.execute("SELECT status,business_reason,evidence_ref,last_publication_id FROM partition_item WHERE instrument_id=?", [identity]).fetchone() == ("no_trade", "exchange confirmed no trade", "raw/no-trade.json", "pub-no-trade")
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-b'")
    connection.execute("UPDATE collection_task SET status='collecting' WHERE task_id='task-a'")
    stale = _publish(store, records=[_record(identity, "99.00000000")], key="stale-a", publication="pub-stale-a")
    assert stale.superseded_identities == (identity,)
    assert store.get(identity, DAY) is None
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-a'")
    _seed_task(connection, "task-c", 3, [identity])
    _publish(store, task="task-c", order=3, records=[_record(identity, "14.00000000")], key="traded", publication="pub-traded")
    assert store.get(identity, DAY).none.close == Decimal("14.00000000")
    assert connection.execute("SELECT count(*) FROM partition_item WHERE instrument_id=? AND status='no_trade'", [identity]).fetchone()[0] == 0


def test_scope_escape_and_decimal_overflow_fail_before_commit(database):
    _, connection = database
    identity = "XSHG:600000"
    _seed_task(connection, "task-a", 1, [identity])
    store = DailyBarStore(connection)
    with pytest.raises(ValueError, match="outside"):
        _publish(store, records=[_record(identity)], scope=[identity, "XSHE:000001"])
    assert connection.execute("SELECT count(*) FROM daily_publication").fetchone()[0] == 0
    from stock_data_manage.storage.daily import _decimal_text
    with pytest.raises(ValueError, match=r"Decimal\(38,8\)"):
        _decimal_text(Decimal("1000000000000000000000000000000"))


@pytest.mark.parametrize("trigger_name,trigger_sql,use_no_trade", [
    ("fail_publication", "CREATE TRIGGER fail_publication BEFORE INSERT ON daily_publication BEGIN SELECT RAISE(ABORT,'injected'); END", False),
    ("fail_receipt", "CREATE TRIGGER fail_receipt BEFORE UPDATE ON daily_publication BEGIN SELECT RAISE(ABORT,'injected'); END", False),
    ("fail_item", "CREATE TRIGGER fail_item BEFORE INSERT ON partition_item WHEN NEW.dataset='daily_bar' BEGIN SELECT RAISE(ABORT,'injected'); END", True),
    ("fail_bar", "CREATE TRIGGER fail_bar BEFORE INSERT ON daily_bar BEGIN SELECT RAISE(ABORT,'injected'); END", False),
    ("fail_quote", "CREATE TRIGGER fail_quote BEFORE INSERT ON daily_quote BEGIN SELECT RAISE(ABORT,'injected'); END", False),
    ("fail_summary", "CREATE TRIGGER fail_summary BEFORE INSERT ON partition_status WHEN NEW.dataset='daily_bar' BEGIN SELECT RAISE(ABORT,'injected'); END", False),
])
def test_failures_at_transaction_stages_rollback_all(database, trigger_name, trigger_sql, use_no_trade):
    _, connection = database
    identity = "XSHG:600000"
    _seed_task(connection, "task-a", 1, [identity])
    store = DailyBarStore(connection)
    connection.execute(trigger_sql)
    with pytest.raises(Exception, match="injected"):
        _publish(store, records=() if use_no_trade else [_record(identity)],
                 no_trade={identity: NoTradeFact("halted", "raw/no-trade.json")} if use_no_trade else {})
    connection.execute(f"DROP TRIGGER {trigger_name}")
    assert connection.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM daily_quote").fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM daily_publication").fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM partition_status WHERE dataset='daily_bar'").fetchone()[0] == 0


def test_idempotency_and_conflicting_reuse(database):
    _, connection = database
    identity = "XSHG:600000"
    _seed_task(connection, "task-a", 1, [identity])
    store = DailyBarStore(connection)
    first = _publish(store, records=[_record(identity)])
    second = _publish(store, records=[_record(identity, "12.34")], publication="pub-ignored")
    assert not first.idempotent and second.idempotent
    assert connection.execute("SELECT data_revision FROM partition_status WHERE dataset='daily_bar'").fetchone()[0] == 1
    with pytest.raises(ValueError, match="different publication content"):
        _publish(store, records=[_record(identity, "99.00000000")], publication="pub-conflict")
    assert store.get(identity, DAY).none.close == Decimal("12.34000000")


def test_a_b_a_protects_identity_ownership(database):
    _, connection = database
    a, b = "XSHG:600000", "XSHE:000001"
    _seed_task(connection, "task-a", 1, [a, b])
    store = DailyBarStore(connection)
    _publish(store, records=[_record(a)])
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-a'")
    _seed_task(connection, "task-b", 2, [a, b])
    _publish(store, task="task-b", order=2, records=[_record(a, "20.00000000"), _record(b, "21.00000000")], key="B", publication="pub-B")
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-b'")
    connection.execute("UPDATE collection_task SET status='collecting' WHERE task_id='task-a'")
    result = _publish(store, records=[_record(a, "10.00000000"), _record(b, "11.00000000")], key="A-resume", publication="pub-A-resume")
    assert result.superseded_identities == tuple(sorted((a, b)))
    assert connection.execute("SELECT data_revision FROM partition_status WHERE dataset='daily_bar'").fetchone()[0] == 2
    assert (store.get(a, DAY).none.close, store.get(b, DAY).none.close) == (Decimal("20.00000000"), Decimal("21.00000000"))


def test_frozen_grant_blocks_a_resume_after_original_owner_republishes(database):
    path, connection = database
    identity = "XSHG:600000"
    scope = _scope([identity])

    def enqueue(task_id, refs):
        with MetadataStore(path) as metadata:
            state = metadata.enqueue_collection_task(
                {"task_id": task_id, "dataset": "daily_bar", "definition_hash": f"frozen-{task_id}",
                 "status": "waiting"}, scope, snapshot={}, frozen_scope=scope,
                daily_owner_refs=tuple(refs))
            return state

    def publish(task_id, key, publication_id, close):
        with MetadataStore(path) as metadata:
            state = metadata.claim_next_collection_task()
            assert state["task_id"] == task_id
            state = metadata.load_collection_task(task_id)
            store = DailyBarStore(metadata.connection)
            return store.publish_partition(
                trade_date=DAY, asset_type=ASSET, task_id=task_id,
                first_execution_order=state["first_execution_order"], publication_id=publication_id,
                idempotency_key=key, scope=frozenset([identity]), records=(_record(identity, close),),
                no_trade={}, raw_evidence_refs=("raw/2026-11-05.json",),
                candidate_ref=f"candidate/{key}.parquet", committed_at=NOW,
                authorization=state["daily_overwrite_grant"])

    # C's first task is accepted with no owner, then publishes the original value.
    enqueue("task-c", ())
    publish("task-c", "C-1", "pub-C-1", "12.00")
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-c'")

    # A is explicitly admitted against C-1 and freezes that exact owner reference.
    owner_c1 = DailyBarStore(connection).current_owners(scope)
    state_a = enqueue("task-a", owner_c1)
    grant_a = state_a["daily_overwrite_grant"]
    assert grant_a["owners"] == list(owner_c1)
    from stock_data_manage.pipeline.inputs import _save_daily_batch
    summary_root = path.parent / "summary-check"
    summary_root.mkdir()
    task_path = summary_root / "task.json"
    task_path.write_text("{}", encoding="utf-8")
    state_a["definition_ref"] = {"path": str(task_path)}
    with MetadataStore(path) as metadata:
        _save_daily_batch(state_a, {}, metadata)
        persisted = metadata.connection.execute(
            "SELECT t.state_json,c.snapshot_json FROM collection_task t JOIN collection_task_context c USING(task_id) "
            "WHERE t.task_id='task-a'").fetchone()
        assert "daily_overwrite_grant" not in json.loads(persisted[0])
        assert json.loads(persisted[1])["daily_overwrite_grant"] == grant_a
        summary = json.loads((summary_root / "summary.json").read_text(encoding="utf-8"))
        assert "daily_overwrite_grant" not in summary
        claimed = metadata.claim_next_collection_task()
        assert claimed["task_id"] == "task-a"
        loaded = metadata.load_collection_task("task-a")
        loaded["daily_overwrite_grant"] = {"owners": []}  # state projection is not authoritative
        metadata.save_collection_task(loaded)
        assert metadata.load_collection_task("task-a")["daily_overwrite_grant"] == grant_a
        metadata.connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-a'")

    # C resumes at its original execution order and publishes a newer receipt.
    owner_c1 = DailyBarStore(connection).current_owners(scope)
    enqueue("task-c", owner_c1)
    result_c2 = publish("task-c", "C-2", "pub-C-2", "14.00")
    assert result_c2.written_count == 1
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-c'")

    # Re-enqueue A against the current C-2 ref for concurrency safety, but keep its C-1 grant.
    owner_c2 = DailyBarStore(connection).current_owners(scope)
    state_a = enqueue("task-a", owner_c2)
    assert state_a["daily_overwrite_grant"] == grant_a
    result_a = publish("task-a", "A-resume", "pub-A-resume", "99.00")
    assert result_a.written_count == 0
    assert result_a.superseded_identities == (identity,)
    assert DailyBarStore(connection).get(identity, DAY).none.close == Decimal("14.00")


def test_summary_counts_current_rows_no_trade_and_unknown_denominator(database):
    _, connection = database
    a, b = "XSHG:600000", "XSHE:000001"
    _seed_task(connection, "task-a", 1, [a, b])
    store = DailyBarStore(connection)
    _publish(store, records=[_record(a)], no_trade={b: NoTradeFact("closed", "evidence/no-trade.json")})
    summary = store.partition_summary(DAY, ASSET)
    assert summary["actual_count"] == 1
    assert summary["none_available_count"] == 1
    assert summary["forward_unverified_count"] == 1
    assert summary["backward_not_applicable_count"] == 1
    assert summary["expected_count"] is None
    assert summary["no_trade_count"] == 1
    assert summary["coverage_reason"]
    page, total = store.list_records(asset_type=ASSET, date_from=DAY, date_to=DAY, limit=1)
    assert total == 1 and len(page) == 1
    connection.execute("UPDATE partition_status SET expected_count=7 WHERE dataset='daily_bar' AND asset_type='stock'")
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-a'")
    _seed_task(connection, "task-b", 2, [a])
    _publish(store, task="task-b", order=2, records=[_record(a, "15.00000000")],
             key="summary-refresh", publication="pub-summary-refresh")
    refreshed = store.partition_summary(DAY, ASSET)
    assert refreshed["expected_count"] is None
    assert refreshed["actual_count"] == 1 and refreshed["none_available_count"] == 1
    assert refreshed["forward_unverified_count"] == 1 and refreshed["no_trade_count"] == 1
    assert refreshed["data_revision"] == 2


def test_all_asset_query_keeps_global_order_and_no_trade_count_is_asset_scoped(database):
    _, connection = database
    stock, etf = "XSHG:600000", "XSHE:510300"
    _seed_task(connection, "task-stock", 1, [stock, "XSHG:600001"], asset="stock")
    store = DailyBarStore(connection)
    _publish(store, task="task-stock", records=[_record(stock)], no_trade={"XSHG:600001": NoTradeFact("halted", "raw/stock.json")})
    _seed_task(connection, "task-etf", 2, [etf], asset="etf")
    _publish(store, task="task-etf", order=2, records=[_record(etf, asset=AssetType.ETF)], scope=[etf], key="etf", publication="pub-etf", asset_type=AssetType.ETF)
    page1, total = store.list_records(date_from=DAY, date_to=DAY, limit=1, offset=0)
    page2, total2 = store.list_records(date_from=DAY, date_to=DAY, limit=1, offset=1)
    assert total == total2 == 2
    assert [page1[0].instrument_id, page2[0].instrument_id] == sorted([stock, etf])
    stock_summary = store.partition_summary(DAY, AssetType.STOCK)
    etf_summary = store.partition_summary(DAY, AssetType.ETF)
    assert stock_summary["actual_count"] == 1 and stock_summary["no_trade_count"] == 1
    assert etf_summary["actual_count"] == 1 and etf_summary["no_trade_count"] == 0


def _reader_process(path: str, ready, finish, result):
    connection = open_runtime_database(path, read_only=True)
    connection.execute("BEGIN")
    first = connection.execute("SELECT count(*) FROM daily_bar").fetchone()[0]
    ready.set()
    finish.wait(10)
    second = connection.execute("SELECT count(*) FROM daily_bar").fetchone()[0]
    connection.execute("COMMIT")
    result.put((first, second))
    connection.close()


def test_independent_process_read_snapshot_coexists_with_writer(database):
    path, connection = database
    identity = "XSHG:600000"
    _seed_task(connection, "task-a", 1, [identity])
    ctx = multiprocessing.get_context("spawn")
    ready, finish, result = ctx.Event(), ctx.Event(), ctx.Queue()
    process = ctx.Process(target=_reader_process, args=(str(path), ready, finish, result))
    process.start()
    assert ready.wait(10)
    _publish(DailyBarStore(connection), records=[_record(identity)])
    finish.set()
    assert result.get(timeout=10) == (0, 0)
    process.join(10)
    assert process.exitcode == 0
    assert connection.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == 1


def test_list_records_keeps_count_page_and_quotes_in_one_read_snapshot(database):
    path, connection = database
    identity = "XSHG:600000"
    _seed_task(connection, "task-a", 1, [identity])
    store = DailyBarStore(connection)
    _publish(store, records=[_record(identity)])
    connection.execute("UPDATE collection_task SET status='failed' WHERE task_id='task-a'")
    _seed_task(connection, "task-b", 2, [identity])
    writer_metadata = MetadataStore(path)
    writer = DailyBarStore(writer_metadata.connection)
    select_count = 0
    published = False

    def authorizer(action, arg1, arg2, database_name, source):
        nonlocal select_count, published
        if action == sqlite3.SQLITE_SELECT:
            select_count += 1
            if select_count == 2 and not published:
                published = True
                _publish(writer, task="task-b", order=2, records=[_record(identity, "22.00000000")],
                         key="concurrent", publication="pub-concurrent")
        return sqlite3.SQLITE_OK

    import sqlite3
    connection.set_authorizer(authorizer)
    page, total = store.list_records(asset_type=ASSET, date_from=DAY, date_to=DAY, limit=10)
    connection.set_authorizer(None)
    writer_metadata.close()
    assert published
    assert total == 1 and page[0].none.close == Decimal("12.34000000")
    assert store.get(identity, DAY).none.close == Decimal("22.00000000")


def test_claim_assigns_and_resume_preserves_first_execution_order(database):
    path, connection = database
    scope = json.dumps(_scope(["XSHG:600000"]))
    connection.execute("""INSERT INTO collection_task
        (task_id,definition_hash,dataset,status,state_json,updated_at,queue_order)
        VALUES ('claim-a','definition','daily_bar','waiting','{"task_id":"claim-a","status":"waiting"}',?,1)""", [NOW])
    connection.execute("INSERT INTO collection_task_context VALUES ('claim-a',?,?,?)", [scope, "{}", scope])
    metadata = MetadataStore(path)
    first = metadata.claim_next_collection_task()
    first_order = first["first_execution_order"]
    connection.execute("UPDATE collection_task SET status='failed',queue_order=99 WHERE task_id='claim-a'")
    connection.execute("UPDATE collection_task SET status='waiting',queue_order=1 WHERE task_id='claim-a'")
    resumed = metadata.claim_next_collection_task()
    assert resumed["first_execution_order"] == first_order
    assert connection.execute("SELECT first_execution_order FROM collection_task WHERE task_id='claim-a'").fetchone()[0] == first_order
    metadata.close()
