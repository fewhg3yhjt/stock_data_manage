import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
import yaml

from stock_data_manage.pipeline.inputs import collect_task
from stock_data_manage.providers.contracts import FailureClass, HttpResponse, ProviderContractError
from stock_data_manage.providers.cnindex.daily import CnindexDailyProvider
from stock_data_manage.providers.csindex.indices import CsindexDailyProvider
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.storage.parquet import CanonicalPartitionStore
from stock_data_manage.storage.raw import RawObjectStore


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
DAY = "2026-10-29"


def _response_manifest(tmp_path, *, provider, endpoint, url, body, parameters):
    store = RawObjectStore(tmp_path / "raw")
    store.record_response(
        response=SimpleNamespace(content=body, status_code=200,
            headers={"Content-Type": "application/json;charset=UTF-8"}, encoding="utf-8"),
        url=url, method="GET", request_headers={}, scope={"parameters": parameters},
        provider=provider, endpoint=endpoint, code_version="fixture-v1", mode="fixture")
    return str(store.manifest)


def _task(task_id, symbol, manifest, *, source_input="ASTOCK-002-daily"):
    unit = {"key": symbol, "symbol": symbol, "asset_type": "index", "input_id": source_input,
        "context": {"request": {"symbol": symbol, "start_date": DAY, "end_date": DAY}},
        "replay_manifests": {"none": manifest}}
    return {"task_id": task_id, "dataset": "daily_bar", "asset_type": "index", "universe": "static",
        "trade_date": DAY, "units": [unit]}


def _csi_manifest(tmp_path, *, code="000904", body_row=None):
    row = body_row or {"tradeDate": "20261029", "indexCode": code, "indexNameCn": "中证中盘200",
        "open": 4920.55, "high": 4920.55, "low": 4840.32, "close": 4843.23,
        "change": -97.54, "changePct": -1.97, "tradingVol": 8593328372, "tradingValue": 1759.99}
    body = json.dumps({"code": "200", "msg": "Success", "data": [row], "success": True}).encode()
    params = {"indexCode": code, "startDate": DAY.replace("-", ""), "endDate": DAY.replace("-", "")}
    url = requests.Request("GET", "https://www.csindex.com.cn/csindex-home/perf/index-perf", params=params).prepare().url
    return _response_manifest(tmp_path, provider="csindex", endpoint="index-perf", url=url,
        body=body, parameters=params)


def _cni_manifest(tmp_path):
    values = [DAY, 1.03, 1.05, 1.01, 1.00, 1.03, 0.02, 1.98, 1500000, 120000, 1.02]
    body = json.dumps({"code": 200, "message": "", "data": {"indexCode": "399299",
        "item": list(CnindexDailyProvider.expected_fields), "data": [values]}}).encode()
    params = {"indexCode": "399299", "startDate": DAY, "endDate": DAY, "frequency": "day"}
    url = requests.Request("GET", CnindexDailyProvider.url, params=params).prepare().url
    return _response_manifest(tmp_path, provider="cnindex", endpoint="index-daily", url=url,
        body=body, parameters=params)


def test_daily_routes_map_only_explicit_parallel_codes_and_keep_the_list_identity():
    from stock_data_manage.pipeline.daily import expand_daily_units, resolve_daily_definition
    from stock_data_manage.config.loader import load_input_capabilities

    definition = resolve_daily_definition({"asset_type": "index", "universe": "static", "trade_date": DAY}, CONFIG)
    symbols = ("sz399904", "sz399920", "sz399939", "sz399941", "sz399961", "sz399962", "sz399980", "sz399299")
    units = [{"key": symbol, "symbol": symbol, "instrument_id": f"XSHE:{symbol[2:]}",
        "asset_type": "index", "input_id": "ASTOCK-002-daily",
        "context": {"request": {"symbol": symbol, "start_date": DAY, "end_date": DAY}}} for symbol in symbols]
    expanded = expand_daily_units(units, definition)
    by_symbol = {unit["symbol"]: unit for unit in expanded}

    assert by_symbol["sz399904"]["input_id"] == "CSI-INDEX-DAILY-OHLC-001"
    assert by_symbol["sz399904"]["symbol"] == "sz399904"
    assert by_symbol["sz399904"]["source_symbol"] == "sh000904"
    assert by_symbol["sz399904"]["context"]["request"]["symbol"] == "sz399904"
    assert by_symbol["sz399904"]["context"]["request"]["source_symbol"] == "sh000904"
    assert by_symbol["sz399904"]["value_contract"] == "full_ohlc"
    assert by_symbol["sz399941"]["input_id"] == "CSI-INDEX-DAILY-OHLC-001"
    assert by_symbol["sz399941"]["symbol"] == "sz399941"
    assert by_symbol["sz399941"]["source_symbol"] == "sh000941"
    for logical, source_code in (("sz399920", "sh000920"), ("sz399939", "sh000939")):
        assert by_symbol[logical]["input_id"] == "CSI-INDEX-DAILY-CLOSE-PAIR-001"
        assert by_symbol[logical]["symbol"] == logical
        assert by_symbol[logical]["source_symbol"] == source_code
        assert by_symbol[logical]["value_contract"] == "close_only"
    assert by_symbol["sz399962"]["input_id"] == "CSI-INDEX-DAILY-CLOSE-PAIR-001"
    assert by_symbol["sz399962"]["symbol"] == "sz399962"
    assert by_symbol["sz399962"]["instrument_id"] == "XSHE:399962"
    assert by_symbol["sz399962"]["source_symbol"] == "sh000962"
    assert by_symbol["sz399962"]["value_contract"] == "close_only"
    assert by_symbol["sz399961"]["input_id"] == "ASTOCK-002-daily"
    assert by_symbol["sz399980"]["input_id"] == "CSI-INDEX-DAILY-OHLC-001"
    assert by_symbol["sz399980"]["symbol"] == "sz399980"
    assert by_symbol["sz399980"]["instrument_id"] == "XSHE:399980"
    assert by_symbol["sz399980"]["source_symbol"] == "sh000980"
    assert by_symbol["sz399980"]["value_contract"] == "full_ohlc"
    assert by_symbol["sz399299"]["input_id"] == "CNI-INDEX-DAILY-001"

    contracts = {item.input_id: item for item in load_input_capabilities(CONFIG / "providers.yaml")}
    parameters = contracts["CSI-INDEX-DAILY-OHLC-001"].bind_parameters(by_symbol["sz399904"]["context"])
    assert parameters["code"] == "sh000904"
    assert parameters["start"] == date.fromisoformat(DAY)
    close_parameters = contracts["CSI-INDEX-DAILY-CLOSE-PAIR-001"].bind_parameters(by_symbol["sz399920"]["context"])
    assert close_parameters["code"] == "sh000920"
    new_close_parameters = contracts["CSI-INDEX-DAILY-CLOSE-PAIR-001"].bind_parameters(by_symbol["sz399962"]["context"])
    assert new_close_parameters["code"] == "sh000962"
    super_cap_parameters = contracts["CSI-INDEX-DAILY-OHLC-001"].bind_parameters(by_symbol["sz399980"]["context"])
    assert super_cap_parameters["code"] == "sh000980"

    from stock_data_manage.routing.factory import build_input_provider
    from stock_data_manage.routing.factory import _configured_index_daily_symbols
    close_provider = build_input_provider(contracts["CSI-INDEX-DAILY-CLOSE-PAIR-001"],
        providers_path=CONFIG / "providers.yaml")
    assert close_provider.value_contract == "close_only"

    daily_routes = {item["input_id"]: item for item in definition["source_policy"]["inputs"]}
    assert len(daily_routes["CSI-INDEX-DAILY-OHLC-001"]["symbols"]) == 35
    assert len(daily_routes["CSI-INDEX-DAILY-CLOSE-PAIR-001"]["symbols"]) == 14
    assert _configured_index_daily_symbols(CONFIG, "CSI-INDEX-DAILY-001")["sh000816"] == "sh000816"
    assert _configured_index_daily_symbols(CONFIG, "CSI-INDEX-DAILY-OHLC-001")["sz399904"] == "sh000904"
    assert _configured_index_daily_symbols(CONFIG, "CSI-INDEX-DAILY-CLOSE-PAIR-001")["sz399962"] == "sh000962"
    assert _configured_index_daily_symbols(CONFIG, "CNI-INDEX-DAILY-001")["sz399299"] == "sz399299"


def test_csindex_399962_close_only_replay_publishes_with_logical_identity_and_unknown_units_empty(tmp_path):
    row = {"tradeDate": "20261029", "indexCode": "000962", "indexNameCnAll": "中证中游制造产业指数",
        "indexNameCn": "中证中游", "open": None, "high": None, "low": None, "close": 4044.15,
        "tradingVol": 9685107219.0, "tradingValue": 3663.44}
    manifest = _csi_manifest(tmp_path / "csi-962-source", code="000962", body_row=row)
    result = collect_task(definition=_task("csi-399962-close-only", "sz399962", manifest),
        config_root=CONFIG, data_root=tmp_path / "csi-962-business", mode="replay",
        collector_options={"pacer": RequestPacer(wait=lambda _: None)})

    assert result["status"] == "published" and result["complete_today"]
    record = CanonicalPartitionStore(tmp_path / "csi-962-business/canonical").read_daily("index", DAY)[0]
    assert record.instrument_id == "XSHE:399962"
    assert record.none.source_provider == "csindex" and record.none.value_contract == "close_only"
    assert record.none.open is None and record.none.high is None and record.none.low is None
    assert record.none.close == Decimal("4044.15")
    assert record.none.volume is None and record.none.amount is None


def test_saved_csindex_399962_response_replays_through_the_daily_publication(tmp_path):
    manifest = ROOT / "tmp/batch-remediation-20261029/interface-review/raw/csi-daily-sh000962/manifest.ndjson"
    if not manifest.exists():
        pytest.skip("the retained target-date CSI response is not present in this checkout")
    result = collect_task(definition=_task("csi-399962-saved-response", "sz399962", str(manifest)),
        config_root=CONFIG, data_root=tmp_path / "csi-962-saved-response", mode="replay",
        collector_options={"pacer": RequestPacer(wait=lambda _: None)})

    assert result["status"] == "published" and result["complete_today"]
    assert result["verification_mode"] == "replay"
    assert result["progress"]["stages"]["raw"]["reused"] == 1
    record = CanonicalPartitionStore(tmp_path / "csi-962-saved-response/canonical").read_daily("index", DAY)[0]
    assert record.instrument_id == "XSHE:399962"
    assert record.none.source_provider == "csindex" and record.none.value_contract == "close_only"
    assert record.none.open is None and record.none.high is None and record.none.low is None
    assert record.none.close == Decimal("4044.15")
    assert record.none.volume is None and record.none.amount is None


def test_csindex_full_ohlc_parser_checks_prices_and_close_only_contract_stays_separate():
    provider = CsindexDailyProvider(object(), value_contract="full_ohlc")
    body = json.dumps({"code": "200", "success": True, "data": [{"tradeDate": "20261029",
        "indexCode": "000904", "open": 4920.55, "high": 4920.55, "low": 4840.32, "close": 4843.23}]}).encode()
    response = HttpResponse(200, {"Content-Type": "application/json"}, body)
    captured = {"symbol": "sh000904", "start": date.fromisoformat(DAY), "end": date.fromisoformat(DAY),
        "segments": ((date.fromisoformat(DAY), date.fromisoformat(DAY), response, "www.csindex.com.cn"),)}
    fetched = provider.parse_window(captured)
    assert fetched.mapping_context["value_contract"] == "full_ohlc"
    assert fetched.rows[0]["open"] == 4920.55 and fetched.rows[0]["close"] == 4843.23

    bad = dict(captured)
    bad_response = HttpResponse(200, {}, json.dumps({"code": "200", "success": True, "data": [{
        "tradeDate": "20261029", "indexCode": "000904", "open": 4900, "high": 4800,
        "low": 4700, "close": 4850}]}).encode())
    bad["segments"] = ((date.fromisoformat(DAY), date.fromisoformat(DAY), bad_response, "www.csindex.com.cn"),)
    with pytest.raises(ProviderContractError) as error:
        provider.parse_window(bad)
    assert error.value.failure_class is FailureClass.SCHEMA_CHANGED

    close_provider = CsindexDailyProvider(object())
    close_response = HttpResponse(200, {}, json.dumps({"code": "200", "success": True, "data": [{
        "tradeDate": "20261029", "indexCode": "000816", "open": None, "high": None,
        "low": None, "close": 2061.1}]}).encode())
    close = close_provider.parse_window({**captured, "symbol": "sh000816", "segments": ((
        date.fromisoformat(DAY), date.fromisoformat(DAY), close_response, "www.csindex.com.cn"),)})
    assert close.mapping_context["value_contract"] == "close_only"
    assert close.rows[0]["open"] is None and close.rows[0]["close"] == 2061.1


def test_csindex_successful_empty_response_is_classified_as_temporary_empty():
    provider = CsindexDailyProvider(object(), value_contract="full_ohlc")
    day = date.fromisoformat(DAY)
    response = HttpResponse(200, {"Content-Type": "application/json"},
        json.dumps({"code": "200", "msg": "Success", "data": [], "success": True}).encode())
    captured = {"symbol": "sh000904", "start": day, "end": day,
        "segments": ((day, day, response, "www.csindex.com.cn"),)}

    with pytest.raises(ProviderContractError) as error:
        provider.parse_window(captured)

    assert error.value.failure_class is FailureClass.TEMPORARY_EMPTY


def test_csindex_paired_code_replay_publishes_full_ohlc_under_the_retained_list_identity(tmp_path):
    manifest = _csi_manifest(tmp_path / "csi-source")
    result = collect_task(definition=_task("csi-paired-ohlc", "sz399904", manifest),
        config_root=CONFIG, data_root=tmp_path / "csi-business", mode="replay",
        collector_options={"pacer": RequestPacer(wait=lambda _: None)})

    assert result["status"] == "published" and result["complete_today"]
    records = CanonicalPartitionStore(tmp_path / "csi-business/canonical").read_daily("index", DAY)
    assert len(records) == 1
    record = records[0]
    assert record.instrument_id == "XSHE:399904"
    assert record.none.source_provider == "csindex"
    assert record.none.value_contract == "full_ohlc"
    assert (record.none.open, record.none.high, record.none.low, record.none.close) == tuple(
        Decimal(value) for value in ("4920.55", "4920.55", "4840.32", "4843.23"))
    events = [json.loads(line) for line in Path(record.none.raw_object_path).read_text(encoding="utf-8").splitlines()]
    assert any("indexCode=000904" in event.get("url", "") for event in events)


def test_cnindex_replay_maps_four_prices_and_leaves_unverified_amount_volume_empty(tmp_path):
    manifest = _cni_manifest(tmp_path / "cni-source")
    result = collect_task(definition=_task("cni-index-daily", "sz399299", manifest),
        config_root=CONFIG, data_root=tmp_path / "cni-business", mode="replay",
        collector_options={"pacer": RequestPacer(wait=lambda _: None)})

    assert result["status"] == "published" and result["complete_today"]
    record = CanonicalPartitionStore(tmp_path / "cni-business/canonical").read_daily("index", DAY)[0]
    assert record.instrument_id == "XSHE:399299"
    assert record.none.source_provider == "cnindex" and record.none.value_contract == "full_ohlc"
    assert (record.none.open, record.none.high, record.none.low, record.none.close) == tuple(
        Decimal(value) for value in ("1.01", "1.05", "1.00", "1.03"))
    assert record.none.volume is None and record.none.amount is None


def test_csindex_and_cnindex_candidates_aggregate_into_one_index_publication(tmp_path):
    csi_manifest = _csi_manifest(tmp_path / "csi-source")
    cni_manifest = _cni_manifest(tmp_path / "cni-source")
    units = [
        {"key": "sz399904", "symbol": "sz399904", "asset_type": "index", "input_id": "ASTOCK-002-daily",
         "context": {"request": {"symbol": "sz399904", "start_date": DAY, "end_date": DAY}},
         "replay_manifests": {"none": csi_manifest}},
        {"key": "sz399299", "symbol": "sz399299", "asset_type": "index", "input_id": "ASTOCK-002-daily",
         "context": {"request": {"symbol": "sz399299", "start_date": DAY, "end_date": DAY}},
         "replay_manifests": {"none": cni_manifest}},
    ]
    definition = {"task_id": "mixed-csi-cnindex-20261029", "dataset": "daily_bar", "asset_type": "index",
        "universe": "static", "trade_date": DAY, "units": units}
    result = collect_task(definition=definition, config_root=CONFIG, data_root=tmp_path / "mixed-business",
        mode="replay", collector_options={"pacer": RequestPacer(wait=lambda _: None)})

    assert result["status"] == "published" and result["complete_today"]
    rows = CanonicalPartitionStore(tmp_path / "mixed-business/canonical").read_daily("index", DAY)
    assert {row.instrument_id for row in rows} == {"XSHE:399904", "XSHE:399299"}
    assert {row.none.source_provider for row in rows} == {"csindex", "cnindex"}
