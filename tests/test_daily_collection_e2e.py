"""Aggregate daily contracts using synthetic HTTP fixtures, never live provider evidence."""
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import json

import pytest
import requests

from stock_data_manage.domain import Adjustment, AssetType, DailyRecord, QuoteGroup, QuoteStatus
from stock_data_manage.pipeline.inputs import collect_task
from stock_data_manage.providers.tencent.daily import TencentDailyProvider
from stock_data_manage.providers.contracts import HttpResponse, ProviderContractError
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.routing.retry import execute_with_fallback, RetryPolicy
from stock_data_manage.storage.raw import RawObjectStore
from stock_data_manage.storage.parquet import CanonicalPartitionStore, InvalidPartitionError, DAILY_SCHEMA_V1, _write_partition
from stock_data_manage.storage.integrity import file_hash, Manifest
from stock_data_manage.storage.metadata import MetadataStore

CONFIG = Path(__file__).resolve().parents[1] / "config"
DAY = "2026-10-09"


@pytest.fixture
def tmp_path(tmp_path_factory):
    # Windows file-path length is a recorded constraint; use a short isolated root.
    return tmp_path_factory.mktemp("d")


def quote(value="10"):
    return QuoteGroup(QuoteStatus.AVAILABLE, *[Decimal(value)] * 4,
        source_provider="fixture", endpoint="daily", raw_object_path="fixture-evidence",
        fetch_time=datetime(2026, 10, 9, 16, tzinfo=timezone.utc))


def record(asset=AssetType.STOCK):
    return DailyRecord("XSHG:600001", date.fromisoformat(DAY), asset, quote("10"), quote("9"), quote("11"))


def source_task(tmp_path, *, corrupt=None, day_only_adjustments=False, symbols=("sh600001",), task_id="aggregate"):
    from stock_data_manage.pipeline.daily import resolve_daily_definition

    policy = resolve_daily_definition({"task": "stock_daily"}, CONFIG)["source_policy"]["inputs"][0]["market_adjustment_requests"]
    market_by_prefix = {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}
    units = []
    for symbol in symbols:
        manifests = {}
        for adjustment in Adjustment:
            name = adjustment.value
            request = policy[market_by_prefix[symbol[:2]]][name]
            api, token = request["api"], request["adjust"]
            params = f"{symbol},day,,,1024" if api == "kline" else f"{symbol},day,{DAY},{DAY},640,{token}"
            host = "https://proxy.finance.qq.com/ifzqgtimg" if api == "newfqkline" else "https://web.ifzq.gtimg.cn"
            endpoint = "/appstock/app/kline/kline" if api == "kline" else f"/appstock/app/{api}/get"
            price = {"none": "10", "forward": "9", "backward": "11"}[name]
            row = [DAY, price, price, price, price, "100"]
            if api == "newfqkline":
                row += ["0", "0", "10", "0"]
            series_key = {"none":"day","forward":"qfqday","backward":"hfqday"}[name]
            node = {"day": [row]} if day_only_adjustments and name != "none" else {series_key: [row]}
            body = json.dumps({"code": 0, "data": {symbol: node}}).encode()
            if corrupt == name:
                body = b"<html>source error, still HTTP 200</html>"
            store = RawObjectStore(tmp_path / "fixture-responses" / symbol / name)
            url = requests.Request("GET", host + endpoint, params={"param": params}).prepare().url
            store.record_response(response=SimpleNamespace(content=body, status_code=200,
                headers={"Content-Type":"application/json"}, encoding="utf-8"), url=url,
                method="GET", request_headers={}, scope={"symbol": symbol, "adjustment": name},
                provider="tencent", endpoint="kline_daily", code_version="synthetic-contract-v1", mode="fixture")
            manifests[name] = str(store.manifest)
        units.append({"key": symbol, "symbol": symbol, "input_id": "ASTOCK-002-daily",
            "context": {"request": {"symbol": symbol, "start_date": DAY, "end_date": DAY}},
            "replay_manifests": manifests})
    return {"task_id": task_id, "dataset":"daily_bar", "asset_type":"stock", "trade_date":DAY, "units":units}


def execute(root, definition, **kwargs):
    return collect_task(definition=definition, config_root=CONFIG, data_root=root / "business", mode="replay",
        collector_options={"pacer": RequestPacer(wait=lambda _: None)}, **kwargs)


def batch_entries(state):
    """Read current unit facts from durable files, not the thin checkpoint."""
    work = Path(state['definition_ref']['path']).parent
    entries = {}
    for path in work.rglob('*.manifest.json'):
        entry = json.loads(path.read_text(encoding='utf-8'))
        if entry.get('report_path'):
            entry['report'] = json.loads(Path(entry['report_path']).read_text(encoding='utf-8'))
        entries[entry['unit_key']] = entry
    return entries


def test_aggregate_nested_roundtrip_and_noop(tmp_path):
    store = CanonicalPartitionStore(tmp_path)
    result = store.publish_daily(asset_type="stock", partition_key=DAY, new_records=[record()], expected_count=1,
        run_id="first", item_statuses={"XSHG:600001":"success"})
    read = store.read_daily("stock", DAY)
    assert read == [record()]
    assert read[0].key == ("XSHG:600001", date.fromisoformat(DAY))
    assert result.manifest.adjustment == "aggregate"
    assert result.manifest.schema_version == "daily-aggregate-v2"
    assert store.publish_daily(asset_type="stock", partition_key=DAY, new_records=[], expected_count=1,
        run_id="again", item_statuses={"XSHG:600001":"success"}).no_op


def test_unavailable_quote_cannot_contain_copied_prices():
    with pytest.raises(ValueError, match="copied"):
        QuoteGroup(QuoteStatus.NOT_APPLICABLE, close=Decimal("10"), reason="no", evidence_ref="fixture")


@pytest.mark.parametrize('asset', [AssetType.ETF, AssetType.INDEX])
def test_nonstock_default_quote_publishes_without_certifying_optional_adjustments(tmp_path, asset):
    unknown = QuoteGroup(QuoteStatus.UNVERIFIED, reason="not verified")
    identity = 'XSHG:000001' if asset is AssetType.INDEX else 'XSHG:510300'
    item = DailyRecord(identity, date.fromisoformat(DAY), asset, quote(), unknown, unknown)
    assert item.complete
    store = CanonicalPartitionStore(tmp_path)
    store.publish_daily(asset_type=asset.value, partition_key=DAY, new_records=[item], expected_count=1, run_id=asset.value)
    restored = store.read_daily(asset.value, DAY)[0]
    assert restored == item
    assert restored.forward.status is QuoteStatus.UNVERIFIED and restored.forward.close is None
    assert restored.backward.status is QuoteStatus.UNVERIFIED and restored.backward.close is None
    assert restored.quote_unit == ('point' if asset is AssetType.INDEX else 'CNY')
    missing = QuoteGroup(QuoteStatus.MISSING, reason='required source missing')
    invalid = DailyRecord(identity, date.fromisoformat(DAY), asset, missing, unknown, unknown)
    assert not invalid.complete
    with pytest.raises(ValueError, match="complete"):
        store.publish_daily(asset_type=asset.value, partition_key=DAY, new_records=[invalid], expected_count=1, run_id='missing')


def test_unverified_adjustments_do_not_block_stock_etf_or_lof_daily_records():
    unknown = QuoteGroup(QuoteStatus.UNVERIFIED, reason='not verified')
    for asset in [AssetType.STOCK, AssetType.ETF, AssetType.LOF]:
        assert DailyRecord('XSHG:600001', date.fromisoformat(DAY), asset, quote(), unknown, unknown).complete
    missing_none = QuoteGroup(QuoteStatus.MISSING, reason='required unadjusted quote missing')
    assert not DailyRecord('XSHG:600001', date.fromisoformat(DAY), AssetType.STOCK,
        missing_none, unknown, unknown).complete


def test_verified_nonapplicable_index_roundtrip(tmp_path):
    na = QuoteGroup(QuoteStatus.NOT_APPLICABLE, reason="synthetic semantic fixture", evidence_ref="fixture-only")
    index = DailyRecord("XSHG:000001", date.fromisoformat(DAY), AssetType.INDEX, quote(), na, na)
    store = CanonicalPartitionStore(tmp_path)
    store.publish_daily(asset_type="index", partition_key=DAY, new_records=[index], expected_count=1, run_id="index")
    assert store.read_daily("index", DAY)[0].value_kind == "index_points"


def test_index_close_only_contract_roundtrip_and_v1_partitions_remain_readable(tmp_path):
    import pyarrow as pa
    from stock_data_manage.storage.parquet import DAILY_SCHEMA_VERSION
    close = QuoteGroup(QuoteStatus.AVAILABLE, close=Decimal("2061.10"),
        source_provider="csindex", endpoint="index-perf", raw_object_path="fixture-evidence",
        fetch_time=datetime(2026, 10, 29, 16, tzinfo=timezone.utc), value_contract="close_only")
    unknown = QuoteGroup(QuoteStatus.UNVERIFIED, reason="not verified")
    index = DailyRecord("XSHG:000816", date(2026, 10, 29), AssetType.INDEX, close, unknown, unknown)
    assert index.complete
    assert not DailyRecord("XSHG:600001", date(2026, 10, 29), AssetType.STOCK, close, close, close).complete
    assert not DailyRecord("XSHG:510300", date(2026, 10, 29), AssetType.ETF, close, unknown, unknown).complete

    store = CanonicalPartitionStore(tmp_path / "canonical")
    published = store.publish_daily(asset_type="index", partition_key="2026-10-29", new_records=[index],
        expected_count=1, run_id="close-only")
    assert published.manifest.schema_version == DAILY_SCHEMA_VERSION
    restored = store.read_daily("index", "2026-10-29")[0]
    assert restored.none.value_contract == "close_only"
    assert restored.none.close == Decimal("2061.10") and restored.none.open is None

    for field in ("volume", "amount"):
        with pytest.raises(ValueError, match="close-only quote requires only"):
            QuoteGroup(QuoteStatus.AVAILABLE, close=Decimal("2061.10"), **{field: Decimal("10")},
                source_provider="csindex", endpoint="index-perf", raw_object_path="fixture-evidence",
                fetch_time=datetime(2026, 10, 29, 16, tzinfo=timezone.utc), value_contract="close_only")

    # A real v1 layout has no value_contract field. It must keep its original meaning.
    old = record()
    old_row = __import__("stock_data_manage.storage.parquet", fromlist=["daily_records_to_table"]).daily_records_to_table([old]).to_pylist()
    for group in old_row[0]["quotes"].values():
        group.pop("value_contract", None)
    old_table = pa.Table.from_pylist(old_row, schema=DAILY_SCHEMA_V1)
    old_store = CanonicalPartitionStore(tmp_path / "legacy")
    partition = old_store.daily_directory("stock", DAY)
    partition.mkdir(parents=True)
    _write_partition(old_table, partition, "legacy-v1", dict(dataset="daily_bar", asset_type="stock",
        partition_key=DAY, schema_version="daily-aggregate-v1", adjustment="aggregate", row_count=1,
        expected_count=1, first_key=str(old.key), last_key=str(old.key), raw_refs=(), item_statuses={}, source_providers={}))
    assert old_store.read_daily("stock", DAY) == [old]


def test_formal_task_three_responses_one_record_and_resume(tmp_path):
    definition = source_task(tmp_path)
    state = execute(tmp_path, definition)
    assert state["status"] == "published", state.get("error")
    assert len(batch_entries(state)) == 3 and 'units' not in state
    assert state["row_count"] == 1
    rows = CanonicalPartitionStore(tmp_path / "business/canonical").read_daily("stock", DAY)
    assert [rows[0].quotes[a.value].close for a in Adjustment] == [Decimal("10"), Decimal("9"), Decimal("11")]
    for quote in rows[0].quotes.values():
        assert quote.volume == Decimal('10000') and quote.amount == Decimal('100000')
        assert set(quote.flags) == {'source_volume_rounded_to_100_shares', 'source_amount_rounded_to_100_CNY'}
    assert all(Path(q.raw_object_path).exists() for q in rows[0].quotes.values())
    assert all(q.fetch_time is not None for q in rows[0].quotes.values())
    assert execute(tmp_path, definition)["no_op"]
    with MetadataStore(tmp_path / "business/metadata/metadata.duckdb") as metadata:
        assert metadata.partition("daily_bar", "stock", DAY, adjustment="aggregate")["actual_count"] == 1


def test_stock_day_only_response_publishes_unadjusted_and_keeps_adjustments_unverified(tmp_path):
    from stock_data_manage.service.web import create_app

    definition = source_task(tmp_path, day_only_adjustments=True)
    state = execute(tmp_path, definition)
    assert state["status"] == "published", state.get("error")
    assert state["complete_today"] is True
    assert state["missing_count"] == 0
    assert state["progress"]["stages"]["processing"]["failed"] == 0
    rows = CanonicalPartitionStore(tmp_path / "business/canonical").read_daily("stock", DAY)
    assert len(rows) == 1
    item = rows[0]
    assert item.none.status is QuoteStatus.AVAILABLE and item.none.close == Decimal("10")
    for name in ("forward", "backward"):
        quote_group = item.quotes[name]
        assert quote_group.status is QuoteStatus.UNVERIFIED
        assert quote_group.close is None
        assert "not provide the requested" in quote_group.reason
    entries = batch_entries(state)
    assert entries["sh600001__forward"]["process_status"] == "unverified"
    assert entries["sh600001__backward"]["process_status"] == "unverified"
    coverage = json.loads(Path(state["definition_ref"]["path"]).with_name("coverage.stock.json").read_text(encoding="utf-8"))
    assert coverage["expected_count"] == coverage["actual_count"] == 1
    assert sum(issue["code"] == "optional_adjustment_unavailable" for issue in coverage["issues"]) == 2

    app = create_app(config_root=CONFIG, data_root=tmp_path / "business")
    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/daily")
    result = endpoint(security="sh600001", start_date=None, end_date=None, asset_type="stock",
        adjustment="forward", page=1, page_size=50)
    assert result["total"] == 1
    assert result["items"][0]["status"] == "unverified"
    assert result["items"][0]["values"]["close"] is None
    task_endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/tasks/{task_id}")
    task = task_endpoint(task_id=state["task_id"])
    assert task["partition_results"][0]["status"] == "published"
    assert task["partition_results"][0]["unverified_count"] == 2
    assert task["partition_results"][0]["issue_count"] == 2


@pytest.mark.parametrize('asset_type', [None, 'stock'])
def test_input_units_require_explicit_asset_and_report_actual_conversion(tmp_path, asset_type):
    from stock_data_manage.pipeline.inputs import collect_input
    definition = source_task(tmp_path)
    manifest = definition['units'][0]['replay_manifests']['forward']
    result = collect_input(input_id='ASTOCK-002-daily', config_root=CONFIG,
        context={'request': {'symbol': 'sh600001', 'start_date': DAY, 'end_date': DAY},
                 'config': {'api': 'newfqkline', 'adjust': 'qfq'}},
        output_root=tmp_path/'input', mode='replay', replay_manifest=manifest, security_asset_type=asset_type)
    assert result['status'] == 'candidate_complete'
    report = json.loads(Path(result['report_path']).read_text(encoding='utf-8'))
    output = json.loads((Path(result['run_directory'])/report['output']['path']).read_text(encoding='utf-8'))[0]
    if asset_type is None:
        assert output['volume'] is output['amount'] is None
        assert report['applied_unit_contract'] is None and report['unit_precision_flags'] == []
    else:
        assert Decimal(output['volume']) == 10000 and Decimal(output['amount']) == 100000
        assert report['applied_unit_contract']['id'] == 'tencent-newfqkline-main-growth-stock-units-v1'
        assert report['unverified_fields'] == [] and len(report['unit_precision_flags']) == 2


@pytest.mark.parametrize('damage', ['multiplier', 'overlapping'])
def test_invalid_unit_configuration_is_rejected_before_acquisition(tmp_path, monkeypatch, damage):
    from stock_data_manage.pipeline.inputs import collect_input
    from stock_data_manage.quality.normalization import NormalizationError
    import shutil
    import yaml
    config = tmp_path/'config'
    shutil.copytree(CONFIG, config)
    path = config/'normalization/daily_bar.yaml'
    document = yaml.safe_load(path.read_text(encoding='utf-8'))
    if damage == 'multiplier':
        document['rules'][0]['unit_contracts'][0]['multipliers']['volume'] = 'NaN'
    else:
        from copy import deepcopy
        duplicate = deepcopy(document['rules'][0]['unit_contracts'][0])
        duplicate['id'] = 'overlapping-contract'
        document['rules'][0]['unit_contracts'].append(duplicate)
    path.write_text(yaml.safe_dump(document), encoding='utf-8')
    def forbidden(*args, **kwargs):
        raise AssertionError('invalid configuration must not reach transport')
    monkeypatch.setattr(requests.Session, 'send', forbidden)
    with pytest.raises(NormalizationError, match=damage):
        collect_input(input_id='ASTOCK-002-daily', config_root=config, mode='live',
            output_root=tmp_path/'invalid-input', context={'request': {'symbol':'sh600001', 'start_date':DAY, 'end_date':DAY},
            'config': {'api':'newfqkline', 'adjust':'qfq'}}, security_asset_type='stock')
    assert not (tmp_path/'invalid-input').exists()


def test_http_success_bad_required_body_stops_at_processing_and_does_not_publish(tmp_path):
    definition = source_task(tmp_path, corrupt="none")
    state = execute(tmp_path, definition)
    assert state["status"] == "failed"
    assert not (tmp_path / "business/canonical" / DAY / "daily_bar/stock/data.parquet").exists()
    assert len(batch_entries(state)) == 3
    coverage = json.loads(Path(state['definition_ref']['path']).with_name('coverage.stock.json').read_text(encoding='utf-8'))
    assert any(issue['adjustment'] == 'none' for issue in coverage['issues'])
    assert coverage['actual_count'] == 0
    failed = batch_entries(state)['sh600001__none']
    assert failed['process_status'] == 'failed' and failed['capture_status'] == 'captured'
    assert len(failed['source_response_hashes']) == 1


@pytest.mark.parametrize("stage", ["after_data_replace", "after_publish", "after_raw_promote"])
def test_aggregate_task_recovery_never_fetches_again(tmp_path, stage):
    from stock_data_manage.worker.recovery import recover_collection_tasks
    definition = source_task(tmp_path)
    def interrupt(point):
        if point == stage:
            raise RuntimeError("simulated power loss")
    with pytest.raises(RuntimeError, match="power loss"):
        execute(tmp_path, definition, failure_hook=interrupt)
    assert recover_collection_tasks(config_root=CONFIG, data_root=tmp_path / "business") == ("aggregate",)
    assert len(CanonicalPartitionStore(tmp_path / "business/canonical").read_daily("stock", DAY)) == 1
    assert execute(tmp_path, definition)["no_op"]


@pytest.mark.parametrize("body", [b"<html>error</html>", b"{}", b'{"data":{}}'])
def test_capture_does_not_parse_or_switch_on_bad_business_payload(body):
    class Transport:
        calls = []
        def get(self, *args, **kwargs):
            self.calls.append((args[0], kwargs["params"]["param"]))
            return HttpResponse(200, {}, body)
    transport = Transport()
    provider = TencentDailyProvider(transport)
    captured = provider.capture_window("sh600001", date.fromisoformat(DAY), date.fromisoformat(DAY), adjust="none", api="newfqkline")
    assert transport.calls == [(
        "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get",
        "sh600001,day,2026-10-09,2026-10-09,640,none",
    )]
    with pytest.raises(ProviderContractError):
        provider.parse_window(captured)
    assert len(transport.calls) == 1


def test_failover_only_catches_acquisition_faults():
    calls=[]
    def first():
        calls.append("first")
        raise requests.ConnectionError("synthetic connection")
    def second():
        calls.append("second")
        return "retained bytes"
    assert execute_with_fallback([("a",first),("b",second)], retry_policy=RetryPolicy(1)).value == "retained bytes"
    assert calls == ["first","second"]
    def bug():
        raise TypeError("programming error")
    with pytest.raises(TypeError):
        execute_with_fallback([("a",bug),("b",second)])
    assert calls == ["first","second"]


def test_full_redo_missing_group_preserves_published_data_and_current_raw(tmp_path):
    definition = source_task(tmp_path)
    first = execute(tmp_path, definition)
    data = Path(first["published_manifest"]).with_name("data.parquet")
    before = file_hash(data)
    changed = source_task(tmp_path / "changed", corrupt="none")
    changed['task_id'] = 'changed-source-run'
    assert execute(tmp_path, changed, redo="full")["status"] == "failed"
    assert file_hash(data) == before


def test_selected_redo_keeps_other_security_and_uses_three_units(tmp_path):
    definition = source_task(tmp_path, symbols=("sh600001", "sh600002"))
    first = execute(tmp_path, definition)
    original = CanonicalPartitionStore(tmp_path / "business/canonical").read_daily("stock", DAY)
    second = execute(tmp_path, definition, redo="selected", symbols=["sh600001"])
    assert first["status"] == second["status"] == "published"
    assert second["selected_symbols"] == ["sh600001"]
    rows = CanonicalPartitionStore(tmp_path / "business/canonical").read_daily("stock", DAY)
    assert len(rows) == 2 and rows[1] == original[1]


def test_daily_query_defaults_to_none_and_switches_without_provider_calls(tmp_path, monkeypatch):
    from stock_data_manage.service.market_data import query_daily
    definition = source_task(tmp_path)
    assert execute(tmp_path, definition)["status"] == "published"
    def no_network(*args, **kwargs):
        raise AssertionError("query attempted network")
    monkeypatch.setattr(requests.Session, "send", no_network)
    root = tmp_path / "business/canonical"
    normal = query_daily(root)
    assert normal["adjustment"] == "none" and Decimal(normal["items"][0]["values"]["close"]) == Decimal("10")
    assert normal["items"][0]["has_source_evidence"] and "raw_ref" not in normal["items"][0]
    assert normal["items"][0]["flags"] == ["source_volume_rounded_to_100_shares", "source_amount_rounded_to_100_CNY"]
    assert Decimal(query_daily(root, adjustment="forward")["items"][0]["values"]["close"]) == Decimal("9")
    assert Decimal(query_daily(root, adjustment="backward")["items"][0]["values"]["close"]) == Decimal("11")
    assert query_daily(root, security="600002")["total"] == 0


def test_daily_web_defaults_validation_and_corrupt_partition(tmp_path):
    from fastapi.testclient import TestClient
    from stock_data_manage.service.web import create_app
    definition = source_task(tmp_path)
    state = execute(tmp_path, definition)
    client = TestClient(create_app(config_root=CONFIG, data_root=tmp_path / "business"))
    normal = client.get("/api/daily")
    assert normal.status_code == 200 and normal.json()["adjustment"] == "none"
    assert normal.json()["items"][0]["has_source_evidence"] and "raw_ref" not in normal.json()["items"][0]
    assert client.get("/api/daily?adjustment=backward").json()["items"][0]["values"]["close"] == "11.00000000"
    assert client.get("/api/daily?adjustment=invalid").status_code == 400
    assert client.get("/api/daily?start_date=invalid").status_code == 422
    Path(state["published_manifest"]).with_name("data.parquet").write_bytes(b"corrupt fixture")
    assert client.get("/api/daily").status_code == 409


def test_old_single_adjustment_task_is_explicitly_rejected(tmp_path):
    definition = source_task(tmp_path)
    definition["adjustment"] = "forward"
    with pytest.raises(ValueError, match="aggregate"):
        execute(tmp_path, definition)


def test_daily_route_expansion_uses_market_and_adjustment_policy():
    from stock_data_manage.pipeline.daily import resolve_daily_definition, expand_daily_units

    definition = resolve_daily_definition({"task": "stock_daily"}, CONFIG)
    routes = definition["source_policy"]["inputs"][0]["market_adjustment_requests"]
    units = [{"key": symbol, "symbol": symbol, "input_id": "ASTOCK-002-daily",
              "context": {"request": {"symbol": symbol, "start_date": DAY, "end_date": DAY}}}
             for symbol in ("sh600519", "sz000001", "bj920001")]

    expanded = expand_daily_units(units, definition)
    routes_by_quote = {(unit["symbol"], unit["adjustment"]): unit["context"]["config"]
                       for unit in expanded}
    assert len(expanded) == 9
    assert routes_by_quote[("sh600519", "none")] == {"api": "newfqkline", "adjust": "none"}
    assert routes_by_quote[("sh600519", "forward")] == {"api": "newfqkline", "adjust": "qfq"}
    assert routes_by_quote[("sh600519", "backward")] == {"api": "newfqkline", "adjust": "hfq"}
    assert routes_by_quote[("sz000001", "forward")] == {"api": "newfqkline", "adjust": "qfq"}
    assert routes_by_quote[("bj920001", "forward")] == {"api": "newfqkline", "adjust": "qfq"}
    assert routes_by_quote[("bj920001", "backward")] == {"api": "newfqkline", "adjust": "hfq"}


def test_daily_route_expansion_never_falls_back_for_missing_market_or_conflicting_unit():
    from copy import deepcopy
    from stock_data_manage.pipeline.daily import resolve_daily_definition, expand_daily_units

    definition = resolve_daily_definition({"task": "stock_daily"}, CONFIG)
    unit = {"key": "bj920001", "symbol": "bj920001", "input_id": "ASTOCK-002-daily",
            "context": {"request": {"symbol": "bj920001", "start_date": DAY, "end_date": DAY}}}
    missing_market = deepcopy(definition)
    del missing_market["source_policy"]["inputs"][0]["market_adjustment_requests"]["BSE"]
    with pytest.raises(ValueError, match="no route for market BSE"):
        expand_daily_units([unit], missing_market)

    conflicting = expand_daily_units([unit], definition)
    conflicting[1]["context"]["config"] = {"api": "fqkline", "adjust": "qfq"}
    with pytest.raises(ValueError, match="conflicts with frozen acquisition policy"):
        expand_daily_units(conflicting, definition)


def csi_response_manifest(root, *, symbol="sh000816", day="2026-10-29"):
    code = symbol[2:]
    body = json.dumps({"code": "200", "success": True, "data": [{
        "tradeDate": day.replace("-", ""), "indexCode": code, "indexNameCn": "细分地产",
        "open": None, "high": None, "low": None, "close": 2061.1,
        "tradingVol": 1977524357, "tradingValue": 99.14,
    }]}).encode("utf-8")
    params = {"indexCode": code, "startDate": day.replace("-", ""), "endDate": day.replace("-", "")}
    url = requests.Request("GET", "https://www.csindex.com.cn/csindex-home/perf/index-perf", params=params).prepare().url
    store = RawObjectStore(root)
    store.record_response(response=SimpleNamespace(content=body, status_code=200,
        headers={"Content-Type": "application/json;charset=UTF-8"}, encoding="utf-8"),
        url=url, method="GET", request_headers={}, scope={"parameters": params},
        provider="csindex", endpoint="index-perf", code_version="fixture-v1", mode="fixture")
    return str(store.manifest)


def csi_task(manifest=None, *, task_id="csi-close-only"):
    symbol, day = "sh000816", "2026-10-29"
    unit = {"key": symbol, "symbol": symbol, "asset_type": "index", "instrument_id": "XSHG:000816",
        "input_id": "ASTOCK-002-daily",
        "context": {"request": {"symbol": symbol, "start_date": day, "end_date": day}}}
    if manifest:
        unit["replay_manifests"] = {"none": manifest}
    return {"task_id": task_id, "dataset": "daily_bar", "asset_type": "index", "universe": "static",
        "trade_date": day, "units": [unit]}


def test_csi_close_only_runs_through_existing_daily_batch_and_publishes_index_points(tmp_path):
    from stock_data_manage.service.market_data import query_daily
    manifest = csi_response_manifest(tmp_path / "source")
    result = collect_task(definition=csi_task(manifest), config_root=CONFIG, data_root=tmp_path / "business",
        mode="replay", collector_options={"pacer": RequestPacer(wait=lambda _: None)})
    assert result["status"] == "published" and result["complete_today"]
    records = CanonicalPartitionStore(tmp_path / "business/canonical").read_daily("index", "2026-10-29")
    assert len(records) == 1
    quote = records[0].none
    assert (quote.source_provider, quote.endpoint, quote.value_contract) == ("csindex", "index-perf", "close_only")
    assert quote.close == Decimal("2061.1") and all(getattr(quote, name) is None for name in ("open", "high", "low", "volume", "amount"))
    assert records[0].quote_unit == "point"
    response = query_daily(tmp_path / "business/canonical", asset_type="index")
    assert response["items"][0]["value_contract"] == "close_only"
    assert response["items"][0]["values"]["close"] == "2061.10000000"


def test_csi_live_route_is_blocked_before_any_provider_request(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("unqualified CSI route attempted online capture")
    monkeypatch.setattr(requests.Session, "send", forbidden)
    result = collect_task(definition=csi_task(task_id="csi-live-blocked"), config_root=CONFIG,
        data_root=tmp_path / "business", mode="live", collector_options={"pacer": RequestPacer(wait=lambda _: None)})
    assert result["status"] == "failed"
    work = Path(result["definition_ref"]["path"]).parent
    gate = json.loads((work / "qualification.prepare-1.json").read_text(encoding="utf-8"))
    assert gate["CSI-INDEX-DAILY-001"]["eligible"] is False
    entry = next(json.loads(path.read_text(encoding="utf-8")) for path in work.rglob("*.manifest.json"))
    assert entry["failure_class"] == "qualification_blocked" and entry["capture_status"] == "blocked"


def test_daily_batch_runs_tencent_and_csindex_capture_groups_before_shared_processing(tmp_path, monkeypatch):
    from threading import Barrier, Lock
    import stock_data_manage.pipeline.daily as daily
    from stock_data_manage.providers.csindex.indices import CsindexDailyProvider

    day, tencent_symbol = "2026-10-29", "sh399001"
    csi_manifest = csi_response_manifest(tmp_path / "csi-source")
    tencent_body = json.dumps({"code": 0, "data": {tencent_symbol: {"day": [[
        day, "100", "100", "100", "100", "100", "0", "0", "100", "0"]]}}}).encode("utf-8")
    tencent_params = {"param": f"{tencent_symbol},day,{day},{day},640,none"}
    tencent_url = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
    tencent_url = requests.Request("GET", tencent_url, params=tencent_params).prepare().url
    tencent_store = RawObjectStore(tmp_path / "tencent-source")
    tencent_store.record_response(response=SimpleNamespace(content=tencent_body, status_code=200,
        headers={"Content-Type": "application/json"}, encoding="utf-8"), url=tencent_url,
        method="GET", request_headers={}, scope={"parameters": tencent_params},
        provider="tencent", endpoint="kline_daily", code_version="fixture-v1", mode="fixture")

    definition = csi_task(csi_manifest, task_id="mixed-source-concurrent-capture")
    definition["units"].append({"key": tencent_symbol, "symbol": tencent_symbol, "asset_type": "index",
        "input_id": "ASTOCK-002-daily", "context": {"request": {"symbol": tencent_symbol,
            "start_date": day, "end_date": day}}, "replay_manifests": {"none": str(tencent_store.manifest)}})

    both_sources_entered_capture = Barrier(2, timeout=5)
    completion_lock = Lock()
    completed_sources = set()
    original_tencent_capture = TencentDailyProvider.capture_window
    original_csindex_capture = CsindexDailyProvider.capture_window
    original_process = daily.process_daily_unit

    def record_source_capture(name, original):
        def capture(self, *args, **kwargs):
            both_sources_entered_capture.wait()
            result = original(self, *args, **kwargs)
            with completion_lock:
                completed_sources.add(name)
            return result
        return capture

    def process_after_capture_barrier(**kwargs):
        assert completed_sources == {"tencent", "csindex"}
        return original_process(**kwargs)

    monkeypatch.setattr(TencentDailyProvider, "capture_window", record_source_capture("tencent", original_tencent_capture))
    monkeypatch.setattr(CsindexDailyProvider, "capture_window", record_source_capture("csindex", original_csindex_capture))
    monkeypatch.setattr(daily, "process_daily_unit", process_after_capture_barrier)

    result = collect_task(definition=definition, config_root=CONFIG, data_root=tmp_path / "business",
        mode="replay", collector_options={"pacer": RequestPacer(wait=lambda _: None)})

    assert result["status"] == "published" and result["row_count"] == 2
    records = CanonicalPartitionStore(tmp_path / "business/canonical").read_daily("index", day)
    by_id = {record.instrument_id: record for record in records}
    assert by_id["XSHG:000816"].none.source_provider == "csindex"
    assert by_id["XSHG:000816"].none.value_contract == "close_only"
    assert by_id["XSHG:399001"].none.source_provider == "tencent"
    assert completed_sources == {"tencent", "csindex"}
