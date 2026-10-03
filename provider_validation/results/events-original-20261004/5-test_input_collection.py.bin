"""YAML execution and migration checks against independently archived source responses."""
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil

import pytest
import requests
import yaml

from stock_data_manage.config.loader import load_dataset_normalization_rules
from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange, QualityStatus
from stock_data_manage.pipeline.inputs import collect_input
from stock_data_manage.providers.transport import captured_requests, RequestPacer
from stock_data_manage.quality.normalization import Normalizer, NormalizationError
from stock_data_manage.storage.raw import RawObjectStore

ROOT = Path(__file__).resolve().parents[1]
TENCENT_ARCHIVE = ROOT / "provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson"
SDK_ARCHIVE = ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson"
CASES = [
    ("ASTOCK-002-daily", {"request": {"symbol": "600519", "start_date": date(2026, 9, 1), "end_date": date(2026, 9, 18)}}, TENCENT_ARCHIVE, 14),
    ("ASTOCK-002-5m", {"request": {"symbol": "300750"}, "config": {"count": 96}}, TENCENT_ARCHIVE, 96),
    ("ASTOCK-045", {"request": {"trade_date": date(2026, 9, 30)}, "calendar": {"trading_dates": [date(2026, 9, 30)]}}, SDK_ARCHIVE, 52),
    ("ASTOCK-070", {}, SDK_ARCHIVE, 8797),
]
THS_ARCHIVE = ROOT / "provider_validation/results/raw/2026-10-02-sector-capabilities-network-retry/manifest.ndjson"
THS_CASES = [
    ("SDA-BOARD-001", {}, THS_ARCHIVE, 90),
    ("SDA-BOARD-002", {"request": {"board_name": "半导体", "start_date": "2026-09-01", "end_date": "2026-10-02"},
                       "dependency": {"board_code": "881121"}}, THS_ARCHIVE, 21),
    ("SDA-BOARD-003", {}, THS_ARCHIVE, 90),
    ("SDA-BOARD-004", {}, THS_ARCHIVE, 387),
]
BAO_ARCHIVE = ROOT / "provider_validation/results/live-probes/baostock-industry-20260930-20261003/_raw/baostock-industry/manifest.ndjson"
BAO_EMPTY_ARCHIVE = ROOT / "provider_validation/results/live-probes/baostock-industry-20261003/_raw/baostock-industry/manifest.ndjson"
BAO_CONTEXT = {"request": {"trade_date": "2026-09-30"}, "calendar": {"trading_dates": [date(2026, 9, 30)]}}
BAO_CASES = [("SDA-BOARD-005", BAO_CONTEXT, BAO_ARCHIVE, 5223), ("SDA-BOARD-006", BAO_CONTEXT, BAO_ARCHIVE, 5221)]
QUOTE_ARCHIVE = ROOT / "provider_validation/results/live-probes/pilot-20261003-tencent-quote-network/_raw/missing-capabilities-20261003T173649/manifest.ndjson"
QUOTE_CONTEXT = {"request": {"symbols": ["sh600519"], "as_of": "2026-09-30T15:10:00+08:00"}}
EM_CONTEXT = {"request": {"symbol": "600519"}}
EM_CASES = [("ASTOCK-026", EM_CONTEXT, SDK_ARCHIVE, 63), ("ASTOCK-027", EM_CONTEXT, SDK_ARCHIVE, 27),
            ("ASTOCK-028", EM_CONTEXT, SDK_ARCHIVE, 120)]
POOL_CONTEXT = {"request": {"trade_date": date(2026, 9, 30)}, "calendar": {"trading_dates": [date(2026, 9, 30)]}}
POOL_CASES = [("ASTOCK-046", POOL_CONTEXT, SDK_ARCHIVE, 12), ("ASTOCK-047", POOL_CONTEXT, SDK_ARCHIVE, 9),
              ("ASTOCK-048", POOL_CONTEXT, SDK_ARCHIVE, 57), ("ASTOCK-050", POOL_CONTEXT, SDK_ARCHIVE, 199)]
POOL_SOURCE = {"ASTOCK-046": ("fetch_broken_board_pool", "stock_zt_pool_zbgc_em", "getTopicZBPool"),
               "ASTOCK-047": ("fetch_limit_down_pool", "stock_zt_pool_dtgc_em", "getTopicDTPool"),
               "ASTOCK-048": ("fetch_previous_limit_pool", "stock_zt_pool_previous_em", "getYesterdayZTPool"),
               "ASTOCK-050": ("fetch_strong_pool", "stock_zt_pool_strong_em", "getTopicQSPool")}


def compare_pool_original(tmp_path, input_id, context, manifest, count):
    import ast
    import inspect
    import types
    import pandas as pd
    import akshare as ak
    from typing import Any, Callable
    from stock_data_manage.providers.eastmoney.limit_pool import pool_replay_clock
    method, sdk_name, endpoint = POOL_SOURCE[input_id]
    probe = ROOT / "provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py"
    nodes = [n for n in ast.parse(probe.read_text(encoding="utf-8")).body
             if isinstance(n, ast.FunctionDef) and n.name in {"ak_function", "call_ak", method}]
    namespace = dict(inspect=inspect, Any=Any, Callable=Callable, Config=types.SimpleNamespace, pd=pd, ensure_ak=lambda: ak)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(probe), "exec"), namespace)
    store = RawObjectStore(tmp_path / "original")
    with captured_requests(store, provider="eastmoney", endpoint=endpoint, scope=context, code_version="original-probe",
                           pacer=RequestPacer(), replay_manifest=manifest, sdk_retry_policy=True) as events:
        with pool_replay_clock(getattr(ak, sdk_name), manifest, endpoint):
            frame = namespace[method](types.SimpleNamespace(trade_date="20260930"))
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config",
                           output_root=tmp_path / "candidate", replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    source = read_artifact(report, "source_rows")
    original_rows = [{k: None if pd.isna(v) else v for k, v in row.items()} for row in frame.to_dict(orient="records")]
    assert source == original_rows and len(source) == report["row_count"] == report["coverage_denominator"] == count
    record_path = ROOT / f"provider_validation/results/interface-records/{input_id}.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    csv_path = ROOT / record["parsed_output_ref"]
    golden = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    assert list(golden.columns) == list(frame.columns) and len(golden) == count
    for index, row in enumerate(source):
        for column, value in row.items():
            expected = golden.iloc[index][column]
            if value is None: assert expected == ""
            elif isinstance(value, (int, float)): assert float(value) == pytest.approx(float(expected), rel=1e-14, abs=1e-12)
            else: assert str(value) == expected
    keys = ("url", "method", "outcome", "status_code", "body_sha256", "request_headers", "request_options")
    assert len(events) == len(report["responses"]) == 1
    assert {k: events[0].get(k) for k in keys} == {k: report["responses"][0].get(k) for k in keys}
    mapped = read_artifact(report, "output")
    assert report["source_quote_date"] == "2026-09-30" and report["source_total_count"] == count
    assert all(row["trade_date"] == "2026-09-30" and row["snapshot_at"] == report["source_capture_window"]["last"] for row in mapped)
    assert [r["source_security_code"] for r in mapped] == [r["代码"] for r in source]
    assert [float(r["price"]) for r in mapped] == [r["最新价"] for r in source]
    assert all(r[field] is None for r in mapped for field in report["unverified_fields"])
    specific = {"ASTOCK-046": ("broken_count", "炸板次数"), "ASTOCK-047": ("consecutive_limit_down_count", "连续跌停"),
                "ASTOCK-048": ("previous_consecutive_limit_count", "昨日连板数"), "ASTOCK-050": ("selection_reason", "入选理由")}
    left, right = specific[input_id]
    assert [r[left] for r in mapped] == [r[right] for r in source]
    assert report["live_http_calls"] == report["production_writes"] == 0 and not report["eligible_for_production_routing"]
    comparison = dict(input_id=input_id, original_csv=str(csv_path.resolve()), original_csv_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        original_probe_sha256=hashlib.sha256(probe.read_bytes()).hexdigest(), original_manifest=str((store.root/"manifest.ndjson").resolve()),
        original_manifest_sha256=hashlib.sha256((store.root/"manifest.ndjson").read_bytes()).hexdigest(),
        report_path=report["report_path"], report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        rows_compared=count, all_sdk_source_columns_equal=True, all_csv_columns_equal=True,
        request_comparison=[{label:{k:event.get(k) for k in keys} for label,event in (("original",events[0]),("provider",report["responses"][0]))}],
        returned_date_equal=True, standard_numeric_units_unverified=report["unverified_fields"], result="passed")
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count", POOL_CASES)
def test_pool_original_script_csv_and_yaml_candidate_agree(tmp_path, no_network, input_id, context, manifest, count):
    compare_pool_original(tmp_path, input_id, context, manifest, count)


def pool_fixture(tmp_path, mutation):
    from urllib.parse import urlsplit
    original = next(json.loads(line) for line in SDK_ARCHIVE.read_text(encoding="utf-8").splitlines()
                    if urlsplit(json.loads(line).get("url", "")).path == "/getTopicZBPool")
    body = json.loads(RawObjectStore.read_response(SDK_ARCHIVE, original))
    if mutation == "date": body["data"]["qdate"] = 20260929
    elif mutation == "count": body["data"]["tc"] += 1
    elif mutation == "duplicate": body["data"]["pool"][1]["c"] = body["data"]["pool"][0]["c"]
    elif mutation == "code": body["data"]["pool"][0]["c"] = "262"
    elif mutation == "numeric": body["data"]["pool"][0]["p"] = None
    elif mutation == "zero": body["data"]["pool"][0]["p"] = 0
    elif mutation == "business": body["rc"] = 1
    elif mutation == "empty": body["data"].update(tc=0,pool=[])
    elif mutation == "null": body["data"] = None
    response = requests.Response()
    response.status_code, response.encoding = (429 if mutation=="http_429" else 200), "utf-8"
    response._content = b"<html>login</html>" if mutation=="html" else json.dumps(body).encode("utf-8")
    store = RawObjectStore(tmp_path/"fixture")
    event = store.record_response(response=response,url=original["url"],method="GET",request_headers=original["request_headers"],
        scope={"mutation":mutation,"synthetic":True,"source_sha256":original["body_sha256"]},provider="eastmoney",endpoint="broken_limit_pool",code_version="offline-fixture")
    if mutation=="transport":
        event.update(outcome="transport_error",error_type="ConnectionError")
        (store.root/"manifest.ndjson").write_text(json.dumps(event)+"\n",encoding="utf-8")
    return store.root/"manifest.ndjson"


POOL_FAILURES = [("date","schema_changed"),("count","truncated"),("duplicate","NormalizationError"),
                 ("code","schema_changed"),("numeric","NormalizationError"),("zero","NormalizationError"),
                 ("business","schema_changed"),("empty","temporary_empty"),("null","temporary_empty"),
                 ("http_429","rate_limited"),("html","JSONDecodeError"),("transport","ConnectionError")]


@pytest.mark.parametrize("mutation,expected", POOL_FAILURES)
def test_pool_invalid_response_keeps_evidence_and_rejects_output(tmp_path, no_network, mutation, expected):
    manifest = pool_fixture(tmp_path,mutation)
    report = collect_input(input_id="ASTOCK-046",context=POOL_CONTEXT,config_root=ROOT/"config",output_root=tmp_path/"out",replay_manifest=manifest)
    assert report["status"]=="failed" and report["failure_class"]==expected,report
    assert "output" not in report and report["production_writes"]==report["live_http_calls"]==0
    original=json.loads(manifest.read_text(encoding="utf-8").splitlines()[0]);saved=report["responses"][0]
    if mutation=="transport": assert saved["outcome"]=="transport_error"
    else:
        assert saved["body_sha256"]==original["body_sha256"]
        assert RawObjectStore.read_response(Path(report["run_directory"])/report["raw_manifest"]["path"],saved)==RawObjectStore.read_response(manifest,original)


def test_pool_mapping_selection_clock_and_replay_miss(tmp_path, no_network):
    import akshare as ak
    from unittest.mock import patch
    config = tmp_path/"config"; shutil.copytree(ROOT/"config",config)
    path=config/"normalization/broken_limit_pool.yaml";document=yaml.safe_load(path.read_text(encoding="utf-8"))
    document["rules"][0]["field_mapping"]["price"]="涨停价"
    path.write_text(yaml.safe_dump(document,allow_unicode=True),encoding="utf-8")
    fields=yaml.safe_load((config/"datasets/broken_limit_pool.yaml").read_text(encoding="utf-8"))["fields"]
    required=[name for name,definition in fields.items() if definition.get("required")]
    namespace=ak.stock_zt_pool_zbgc_em.__globals__
    class FutureDateTime(datetime):
        @classmethod
        def now(cls,tz=None): return datetime(2030,1,1,tzinfo=tz)
    with patch.dict(namespace,{"datetime":FutureDateTime}):
        report=collect_input(input_id="ASTOCK-046",context=POOL_CONTEXT,config_root=config,output_root=tmp_path/"out",
                             replay_manifest=SDK_ARCHIVE,fields=required)
        assert namespace["datetime"] is FutureDateTime
    assert report["status"]=="candidate_complete"
    source,mapped=read_artifact(report,"source_rows"),read_artifact(report,"output")
    assert set(mapped[0])==set(required) and [float(r["price"]) for r in mapped]==[r["涨停价"] for r in source]
    failed=collect_input(input_id="ASTOCK-046",context={"request":{"trade_date":date(2026,9,29)},"calendar":{"trading_dates":[date(2026,9,29)]}},
        config_root=config,output_root=tmp_path/"out",replay_manifest=SDK_ARCHIVE)
    assert failed["status"]=="failed" and failed["failure_class"]=="ValueError" and "never falls back" in failed["error"]
    with pytest.raises(ValueError,match="does not support"):
        collect_input(input_id="ASTOCK-046",context={**POOL_CONTEXT,"request":{**POOL_CONTEXT["request"],"symbol":"600519"}},
            config_root=config,output_root=tmp_path/"out",replay_manifest=SDK_ARCHIVE)


def test_sector_flow_alias_uses_successful_ths_scope(tmp_path,no_network):
    import pandas as pd
    from stock_data_manage.config.loader import load_input_capabilities
    contracts={c.input_id:c for c in load_input_capabilities(ROOT/"config/providers.yaml")}
    assert contracts["ASTOCK-023"].implementation_status=="alias" and contracts["ASTOCK-023"].canonical_input=="SDA-BOARD-003"
    report=collect_input(input_id="SDA-BOARD-003",context={},config_root=ROOT/"config",output_root=tmp_path,
        replay_manifest=SDK_ARCHIVE)
    assert report["status"]=="candidate_complete" and report["row_count"]==90
    source=read_artifact(report,"source_rows")
    csv_path=SDK_ARCHIVE.parents[2]/"21_板块资金流向/data.csv"
    golden=pd.read_csv(csv_path,keep_default_na=False)
    assert set(golden.columns)==set(source[0]) and len(golden)==len(source)
    for actual,expected in zip(source,golden.to_dict(orient="records")):
        for key,value in actual.items():
            if isinstance(value,(float,int)):assert float(value)==pytest.approx(float(expected[key]),rel=1e-14,abs=1e-12)
            else:assert value==expected[key]
    assert all("10jqka.com.cn" in event["url"] for event in report["responses"])
    result=dict(alias="ASTOCK-023",canonical_input="SDA-BOARD-003",source_rows_compared=90,
        source_csv=str(csv_path.resolve()),source_csv_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        report_path=report["report_path"],report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        limitations="successful THS variant only; failed EastMoney rank variant is not inherited",result="passed")
    (tmp_path/"alias-comparison.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")


def test_existing_limit_up_provider_default_contract_unchanged(tmp_path,no_network):
    import types
    import sys
    import akshare as ak
    from stock_data_manage.providers.eastmoney.limit_pool import EastMoneyLimitUpProvider
    snapshot=ROOT/"provider_validation/results/pools-original-20261004/1-limit_pool.py.bin"
    module=types.ModuleType("stock_data_manage.providers.eastmoney.original_limit_pool")
    module.__package__="stock_data_manage.providers.eastmoney"
    sys.modules[module.__name__]=module
    try: exec(compile(snapshot.read_bytes(),str(snapshot),"exec"),module.__dict__)
    finally: del sys.modules[module.__name__]
    results,requests_made=[],[]
    for label,adapter in (("original",module.EastMoneyLimitUpProvider),("provider",EastMoneyLimitUpProvider)):
        store=RawObjectStore(tmp_path/label)
        with captured_requests(store,provider="eastmoney",endpoint="limit_up_pool",scope=POOL_CONTEXT,
            code_version="legacy-default-check",pacer=RequestPacer(),replay_manifest=SDK_ARCHIVE,sdk_retry_policy=True) as events:
            results.append(adapter(client=ak).fetch(date(2026,9,30)))
        requests_made.append(events)
    assert results[0]==results[1] and len(results[1].rows)==52
    keys=("url","method","request_headers","status_code","body_sha256","request_options")
    assert [{k:r.get(k) for k in keys} for r in requests_made[0]]==[{k:r.get(k) for k in keys} for r in requests_made[1]]
    (tmp_path/"legacy-comparison.json").write_text(json.dumps(dict(rows_compared=52,all_fields_equal=True,
        requests_equal=True,original_snapshot_sha256=hashlib.sha256(snapshot.read_bytes()).hexdigest(),result="passed"),indent=2),encoding="utf-8")


def test_pool_injected_live_session_and_cache_preserve_sdk(tmp_path,monkeypatch):
    from urllib.parse import urlsplit
    original=next(json.loads(line) for line in SDK_ARCHIVE.read_text(encoding="utf-8").splitlines()
        if urlsplit(json.loads(line).get("url","")).path=="/getTopicZBPool")
    body=RawObjectStore.read_response(SDK_ARCHIVE,original)
    calls=[]
    def send(session,request,**kwargs):
        policy=session.adapters["https://"].max_retries
        calls.append(dict(timeout=kwargs.get("timeout"),trust_env=session.trust_env,retry_total=policy.total))
        assert policy.total==2 and not policy.is_retry("GET",429,True)
        response=requests.Response();response._content=body;response.status_code=200;response.encoding="utf-8"
        response.url,response.request=request.url,request
        return response
    from unittest.mock import patch
    with patch.object(requests.Session,"send",send):
        kwargs=dict(input_id="ASTOCK-046",context=POOL_CONTEXT,config_root=ROOT/"config",output_root=tmp_path/"out",
                    mode="live",evidence_root=tmp_path)
        first=collect_input(**kwargs);second=collect_input(**kwargs)
    assert first["status"]==second["status"]=="candidate_complete"
    assert calls==[{"timeout":None,"trust_env":True,"retry_total":2}]
    assert first["live_http_calls"]==1 and second["live_http_calls"]==0 and second["responses"][0]["mode"]=="cached"
    (tmp_path/"session-comparison.json").write_text(json.dumps(dict(mode="injected Session fixture; no network",
        calls=calls,cache_reused=True,report_paths=[first["report_path"],second["report_path"]],result="passed"),indent=2),encoding="utf-8")


def compare_em_original(tmp_path, input_id, context, manifest, count):
    """Run the saved probe functions and compare every SDK column against independent CSVs."""
    import ast
    import inspect
    import re
    import types
    import pandas as pd
    import akshare as ak
    from typing import Any, Callable, List, Tuple, Dict
    from stock_data_manage.providers.eastmoney.realtime import history_replay_clock
    probe = ROOT / "provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py"
    names = {"only_digits", "market_of", "em_market", "ak_function", "call_ak", "try_ak_variants",
             "fetch_shareholder_count", "fetch_dividend_history", "fetch_em_fund_flow", "fetch_fund_flow_120"}
    nodes = [node for node in ast.parse(probe.read_text(encoding="utf-8")).body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = dict(re=re, inspect=inspect, pd=pd, Config=types.SimpleNamespace, Any=Any, Callable=Callable,
                     List=List, Tuple=Tuple, Dict=Dict, ensure_ak=lambda: ak)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(probe), "exec"), namespace)
    method, folder = {"ASTOCK-026": ("fetch_shareholder_count", "24_股东户数变化"),
                      "ASTOCK-027": ("fetch_dividend_history", "25_分红送转历史"),
                      "ASTOCK-028": ("fetch_fund_flow_120", "26_个股资金流近100~120日")}[input_id]
    from contextlib import ExitStack
    original_store = RawObjectStore(tmp_path / "original")
    with ExitStack() as stack:
        events = stack.enter_context(captured_requests(original_store, provider="eastmoney", endpoint=method,
            scope=context, code_version="original-probe", pacer=RequestPacer(), replay_manifest=manifest, sdk_retry_policy=True))
        if input_id == "ASTOCK-028":
            stack.enter_context(history_replay_clock(ak.stock_individual_fund_flow, manifest, "600519"))
        frame = namespace[method](types.SimpleNamespace(code="600519"))
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "candidate",
                           replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    source = read_artifact(report, "source_rows")
    expected_count = 28 if input_id == "ASTOCK-027" else count
    assert len(frame) == len(source) == expected_count
    csv_path = manifest.parents[2] / folder / "data.csv"
    golden = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    assert len(golden) == expected_count and list(golden.columns) == list(frame.columns)
    original_rows = [{k: None if pd.isna(v) else v.isoformat() if hasattr(v, "isoformat") else v for k, v in r.items()}
                     for r in frame.to_dict(orient="records")]
    assert source == original_rows
    for index, raw in enumerate(source):
        for column, value in raw.items():
            saved = golden.iloc[index][column]
            if value is None:
                assert saved == ""
            elif isinstance(value, (float, int)):
                # CSV and SDK may differ only in shortest floating point serialization.
                assert float(value) == pytest.approx(float(saved), rel=1e-14, abs=1e-12)
            else:
                assert str(value) == saved
    keys = ["url", "method", "request_headers", "outcome", "status_code", "body_sha256", "error_type", "request_options"]
    assert len(events) == len(report["responses"]) == (1 if input_id == "ASTOCK-028" else 2)
    for left, right in zip(events, report["responses"]):
        assert {k: left.get(k) for k in keys} == {k: right.get(k) for k in keys}
    mapped = read_artifact(report, "output")
    excluded = read_artifact(report, "excluded_rows")
    assert len(mapped) == report["coverage_denominator"] == count
    assert report["original_row_count"] == expected_count and report["selected_row_count"] == count
    assert report["live_http_calls"] == report["production_writes"] == 0
    assert report["eligible_for_production_routing"] is False
    assert all(row[field] is None for row in mapped for field in report["unverified_fields"])
    if input_id == "ASTOCK-027":
        selected = [row for row in source if row["方案进度"] == "实施分配"]
        assert len(excluded) == 1 and excluded[0]["row"]["方案进度"] == "预披露"
        assert excluded[0]["reason"] == "not_implemented_dividend_plan"
        assert [r["ex_dividend_date"] for r in mapped] == [r["除权除息日"] for r in selected]
        assert [r["notice_date"] for r in mapped] == [r["最新公告日期"] for r in selected]
    elif input_id == "ASTOCK-026":
        assert excluded == []
        assert [float(r["holder_count"]) for r in mapped] == [r["股东户数-本次"] for r in source]
        assert all(r["previous_end_date"] is None for r in mapped)
    else:
        assert excluded == []
        assert [float(r["close"]) for r in mapped] == [r["收盘价"] for r in source]
    comparison = dict(input_id=input_id, original_count=expected_count, selected_count=count, excluded_count=len(excluded),
        all_sdk_columns_and_rows_equal=True, all_csv_columns_and_rows_equal=True, requests_equal=True,
        original_csv=str(csv_path.resolve()), original_csv_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        original_probe=str(probe.resolve()), original_probe_sha256=hashlib.sha256(probe.read_bytes()).hexdigest(),
        original_manifest=str((original_store.root / "manifest.ndjson").resolve()),
        original_manifest_sha256=hashlib.sha256((original_store.root / "manifest.ndjson").read_bytes()).hexdigest(),
        provider_report=report["report_path"], provider_report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        request_comparison=[{"original": {k: l.get(k) for k in keys}, "provider": {k: r.get(k) for k in keys}}
                            for l, r in zip(events, report["responses"])])
    (tmp_path / "comparison.json").write_text(json.dumps(comparison, ensure_ascii=False, indent=2), encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count", EM_CASES)
def test_em_original_script_sdk_csv_and_candidate_agree(tmp_path, no_network, input_id, context, manifest, count):
    compare_em_original(tmp_path, input_id, context, manifest, count)


def em_fixture(tmp_path, input_id, mutation):
    """Synthetic mutation of retained source bytes; metadata is computed from the new payload."""
    target = {"ASTOCK-026": "RPT_HOLDERNUM_DET", "ASTOCK-027": "RPT_SHAREBONUS_DET",
              "ASTOCK-028": "/fflow/daykline/get"}[input_id]
    record = next(json.loads(line) for line in reversed(SDK_ARCHIVE.read_text(encoding="utf-8").splitlines())
                  if target in json.loads(line).get("url", ""))
    body = json.loads(RawObjectStore.read_response(SDK_ARCHIVE, record))
    if mutation == "identity":
        if input_id == "ASTOCK-028": body["data"]["code"] = "000001"
        else: body["result"]["data"][0]["SECURITY_CODE"] = "000001"
    elif mutation == "count": body["result"]["count"] += 1
    elif mutation == "duplicate":
        if input_id == "ASTOCK-028": body["data"]["klines"][1] = body["data"]["klines"][0]
        else: body["result"]["data"][1] = dict(body["result"]["data"][0])
    elif mutation == "numeric":
        values=body["data"]["klines"][0].split(","); values[1]="broken"; body["data"]["klines"][0]=",".join(values)
    elif mutation == "all_pending":
        for row in body["result"]["data"]: row["ASSIGN_PROGRESS"] = "预披露"
    elif mutation == "business": body["success"] = False
    elif mutation == "empty": body["data"]["klines"] = []
    elif mutation == "schema":
        for row in body["result"]["data"]: row.pop("END_DATE", None)
    response = requests.Response()
    response.status_code = 429 if mutation == "http_429" else 200
    response._content, response.encoding = json.dumps(body).encode("utf-8"), "utf-8"
    store = RawObjectStore(tmp_path / "fixture")
    # Shareholder/dividend SDK performs two identical calls; archive both.
    for index in range(1 if input_id == "ASTOCK-028" else 2):
        store.record_response(response=response, url=record["url"], method="GET", request_headers=record["request_headers"],
            scope={"synthetic_mutation": mutation, "original_source_sha256": record["body_sha256"]},
            provider="eastmoney", endpoint=input_id, code_version="offline-fixture")
    if mutation in {"transport", "corrupt_hash"}:
        path = store.root / "manifest.ndjson"
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        for event in records:
            if mutation == "transport": event.update(outcome="transport_error", error_type="ConnectionError")
            else: event["body_sha256"] = "0" * 64
        path.write_text("\n".join(json.dumps(event) for event in records)+"\n",encoding="utf-8")
    return store.root / "manifest.ndjson"


EM_FAILURES = [("ASTOCK-026", "identity", "schema_changed"), ("ASTOCK-027", "identity", "schema_changed"),
               ("ASTOCK-028", "identity", "schema_changed"), ("ASTOCK-026", "count", "schema_changed"),
               ("ASTOCK-027", "count", "schema_changed"), ("ASTOCK-028", "duplicate", "NormalizationError"),
               ("ASTOCK-026", "duplicate", "NormalizationError"), ("ASTOCK-028", "numeric", "NormalizationError"),
               ("ASTOCK-027", "all_pending", "NormalizationError"), ("ASTOCK-028", "http_429", "rate_limited"),
               ("ASTOCK-026", "business", "schema_changed"), ("ASTOCK-028", "empty", "ValueError"),
               ("ASTOCK-028", "transport", "ConnectionError"), ("ASTOCK-028", "corrupt_hash", "ValueError")]


@pytest.mark.parametrize("input_id,mutation,expected", EM_FAILURES)
def test_em_invalid_source_keeps_bytes_and_rejects_candidate(tmp_path, no_network, input_id, mutation, expected):
    manifest = em_fixture(tmp_path, input_id, mutation)
    report = collect_input(input_id=input_id, context=EM_CONTEXT, config_root=ROOT / "config", output_root=tmp_path / "out",
                           replay_manifest=manifest)
    assert report["status"] == "failed" and report["failure_class"] == expected, report
    assert "output" not in report and report["live_http_calls"] == 0 and report["production_writes"] == 0
    original = json.loads(manifest.read_text(encoding="utf-8").splitlines()[0])
    if mutation == "corrupt_hash":
        assert "responses" not in report
        return
    event = report["responses"][0]
    if mutation == "transport":
        assert event["outcome"] == "transport_error" and event["error_type"] == "ConnectionError"
        return
    assert event["body_sha256"] == original["body_sha256"]
    assert RawObjectStore.read_response(Path(report["run_directory"]) / report["raw_manifest"]["path"], event) == RawObjectStore.read_response(manifest, original)


def test_em_legacy_fund_flow_columns_match_independent_csv(tmp_path):
    import csv
    from stock_data_manage.providers.contracts import HttpResponse
    from stock_data_manage.providers.eastmoney.fund_flow import EastMoneyStockFundFlowProvider
    record = next(json.loads(line) for line in reversed(SDK_ARCHIVE.read_text(encoding="utf-8").splitlines())
                  if "/fflow/daykline/get" in json.loads(line).get("url", ""))
    body = RawObjectStore.read_response(SDK_ARCHIVE, record)
    calls = []
    class Transport:
        def get(self, url, *, params, timeout_seconds):
            calls.append(dict(url=url, params=params, timeout_seconds=timeout_seconds))
            return HttpResponse(200, {}, body)
    rows = EastMoneyStockFundFlowProvider(Transport()).fetch_daily(["sh600519"]).records
    csv_path = SDK_ARCHIVE.parents[2] / "26_个股资金流近100~120日/data.csv"
    golden = list(csv.DictReader(csv_path.open(encoding="utf-8-sig", newline="")))
    mapping = {"main":"主力", "super_large":"超大单", "large":"大单", "medium":"中单", "small":"小单"}
    assert len(rows) == len(golden) == 120
    for row, reference in zip(rows, golden):
        assert row.trade_date == reference["日期"]
        for key, source in mapping.items():
            assert getattr(row, key+"_net_inflow") == float(reference[source+"净流入-净额"])
            assert getattr(row, key+"_net_inflow_pct") == float(reference[source+"净流入-净占比"])
    assert calls[0]["params"]["lmt"] == "20" and calls[0]["timeout_seconds"] == 20
    original_cls = em_original_class("2-fund_flow.py.bin", "EastMoneyStockFundFlowProvider")
    old_rows = original_cls(Transport()).fetch_daily(["sh600519"]).records
    assert calls[0] == calls[1]
    assert len(old_rows) == len(rows)
    assert all(old.main_net_inflow == new.main_net_inflow and old.close == new.close and old.change_pct == new.change_pct
               for old, new in zip(old_rows, rows))
    saved = dict(rows_compared=120, fields_compared=10, source_csv=str(csv_path.resolve()),
                 source_sha256=hashlib.sha256(body).hexdigest(), legacy_request=calls[0], result="passed")
    tmp_path.mkdir(exist_ok=True, parents=True)
    (tmp_path / "legacy-flow-comparison.json").write_text(json.dumps(saved, ensure_ascii=False, indent=2),encoding="utf-8")


def em_original_class(snapshot, name):
    import sys
    import types
    module = types.ModuleType("em_original_" + name)
    module.__package__ = "stock_data_manage.providers.eastmoney"
    sys.modules[module.__name__] = module
    try:
        path = ROOT / "provider_validation/results/em-original-20261004" / snapshot
        exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
        return getattr(module, name)
    finally:
        del sys.modules[module.__name__]


@pytest.mark.parametrize("input_id,snapshot,class_name,method", [
    ("ASTOCK-026", "1-shareholder.py.bin", "EastMoneyShareholderCountProvider", "fetch"),
    ("ASTOCK-027", "3-dividend.py.bin", "EastMoneyDividendProvider", "fetch_events")])
def test_em_existing_methods_preserve_requests_and_returns(tmp_path, input_id, snapshot, class_name, method):
    from dataclasses import asdict
    from stock_data_manage.providers.contracts import HttpResponse
    from stock_data_manage.providers.eastmoney.shareholder import EastMoneyShareholderCountProvider
    from stock_data_manage.providers.eastmoney.dividend import EastMoneyDividendProvider
    token = "RPT_HOLDERNUM_DET" if input_id == "ASTOCK-026" else "RPT_SHAREBONUS_DET"
    event = next(json.loads(line) for line in SDK_ARCHIVE.read_text(encoding="utf-8").splitlines()
                 if token in json.loads(line).get("url", ""))
    body = RawObjectStore.read_response(SDK_ARCHIVE, event)
    requests_made = []
    class Transport:
        def get(self, url, *, params, timeout_seconds):
            requests_made.append(dict(url=url, params=params, timeout_seconds=timeout_seconds))
            return HttpResponse(200, {}, body)
    current = EastMoneyShareholderCountProvider if input_id == "ASTOCK-026" else EastMoneyDividendProvider
    old = em_original_class(snapshot, class_name)
    args, kwargs = ((["sh600519"],), {}) if input_id == "ASTOCK-026" else (
        (date(2001,1,1), date(2026,9,30)), {"fetch_time":datetime(2026,10,3,tzinfo=timezone.utc)})
    previous = getattr(old(Transport()), method)(*args, **kwargs)
    migrated = getattr(current(Transport()), method)(*args, **kwargs)
    assert asdict(previous) == asdict(migrated) and requests_made[0] == requests_made[1]
    tmp_path.mkdir(exist_ok=True, parents=True)
    result = dict(input_id=input_id, source_sha256=hashlib.sha256(body).hexdigest(), requests=requests_made,
        rows_compared=len(migrated.records if input_id == "ASTOCK-026" else migrated.events),
        all_legacy_fields_equal=True, mode="offline parser/default-request regression using injected archived response")
    (tmp_path / "legacy-comparison.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")


def test_em_yaml_selection_mapping_and_unsupported_parameters(tmp_path, no_network):
    config = tmp_path / "config"
    shutil.copytree(ROOT / "config", config)
    path = config / "normalization/shareholder_count.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["rules"][-1]["field_mapping"]["holder_count"] = "股东户数-上次"
    path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
    report = collect_input(input_id="ASTOCK-026", context=EM_CONTEXT, config_root=config, output_root=tmp_path / "out",
                           replay_manifest=SDK_ARCHIVE, fields=["instrument_id", "end_date", "holder_count"])
    assert report["status"] == "candidate_complete"
    source, mapped = read_artifact(report, "source_rows"), read_artifact(report, "output")
    assert set(mapped[0]) == {"instrument_id", "end_date", "holder_count"}
    assert [float(row["holder_count"]) for row in mapped] == [row["股东户数-上次"] for row in source]
    for ident, _, _, _ in EM_CASES:
        with pytest.raises(ValueError):
            collect_input(input_id=ident, context={"request":{"symbol":"600519", "start_date":"2026-01-01"}},
                config_root=config, output_root=tmp_path / "out", replay_manifest=SDK_ARCHIVE)
        failed = collect_input(input_id=ident, context={"request":{"symbol":"sz600519"}},config_root=config,
            output_root=tmp_path / "out", replay_manifest=SDK_ARCHIVE)
        assert failed["status"] == "failed" and failed["failure_class"] == "ValueError"


def test_em_replay_miss_and_clock_are_isolated(tmp_path, no_network):
    import akshare as ak
    clock = ak.stock_individual_fund_flow.__globals__["time"]
    report = collect_input(input_id="ASTOCK-028", context={"request":{"symbol":"000001"}},config_root=ROOT / "config",
        output_root=tmp_path, replay_manifest=em_fixture(tmp_path, "ASTOCK-028", "none"))
    assert report["status"] == "failed" and "never falls back" in report["error"]
    assert ak.stock_individual_fund_flow.__globals__["time"] is clock


def quote_source():
    event = next(json.loads(line) for line in QUOTE_ARCHIVE.read_text(encoding="utf-8").splitlines()
                 if json.loads(line).get("url") == "https://qt.gtimg.cn/q=sh600519")
    return event, RawObjectStore.read_response(QUOTE_ARCHIVE, event)


def quote_payload(symbols):
    _, body = quote_source()
    return b"\n".join(body.strip().replace(b"v_sh600519=", ("v_" + symbol + "=").encode())
                       .replace(b"~600519~", ("~" + symbol[2:] + "~").encode()) for symbol in symbols)


def compare_quote_original(tmp_path):
    import ast
    import csv
    import re
    import types
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from stock_data_manage.config.loader import load_input_capabilities
    from stock_data_manage.routing.factory import build_input_provider
    from stock_data_manage.providers.contracts import HttpResponse
    probe = ROOT / "provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py"
    nodes = [node for node in ast.parse(probe.read_text(encoding="utf-8")).body
             if isinstance(node, ast.FunctionDef) and node.name in {"build_session", "only_digits", "market_of", "sina_symbol", "fetch_tencent_finance"}
             or isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "UA" for t in node.targets)]
    namespace = {"requests": requests, "re": re, "pd": pd, "HTTPAdapter": HTTPAdapter, "Retry": Retry, "Config": types.SimpleNamespace}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(probe), "exec"), namespace)
    runner_path = ROOT / "provider_validation/tests/run_a_stock_rate_limited_probes.py"
    spec = importlib.util.spec_from_file_location("original_quote_probe_policy", runner_path)
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    store = RawObjectStore(tmp_path / "original")
    with captured_requests(store, provider="tencent", endpoint="single_quote", scope=QUOTE_CONTEXT, code_version="original-probe",
                           pacer=RequestPacer(), replay_manifest=QUOTE_ARCHIVE, sdk_retry_policy=True) as events:
        session = namespace["build_session"]()
        for scheme in ("http://", "https://"):
            session.mount(scheme, HTTPAdapter(max_retries=runner.retry_policy()))
        namespace["SESSION"] = session
        try:
            frame = namespace["fetch_tencent_finance"](types.SimpleNamespace(code="600519"))
        finally:
            session.close()
    original_rows = frame.to_dict("records")
    golden = ROOT / "provider_validation/results/live-probes/pilot-20261003-tencent-quote-network/01_腾讯财经/data.csv"
    with golden.open(encoding="utf-8-sig", newline="") as stream:
        assert original_rows == list(csv.DictReader(stream))
    parsed_ref = store.write_json(original_rows, dataset="original_parsed", provider="tencent", endpoint="single_quote",
                                 fetched_at=datetime.now(timezone.utc), attempt_id="parsed")
    report = collect_input(input_id="ASTOCK-001", context=QUOTE_CONTEXT, config_root=ROOT / "config",
                           output_root=tmp_path / "adapter", replay_manifest=QUOTE_ARCHIVE)
    assert report["status"] == "candidate_complete", report
    source, mapped = read_artifact(report, "source_rows"), read_artifact(report, "output")
    assert {key: source[0][key] for key in original_rows[0]} == original_rows[0]
    assert mapped[0]["instrument_id"] == "XSHG:600519"
    for target, column in {"price": "price", "change_pct": "change_pct", "change_amount": "change",
                           "open": "open", "high": "high", "low": "low", "pre_close": "prev_close"}.items():
        assert Decimal(mapped[0][target]) == Decimal(original_rows[0][column])
    assert mapped[0]["quote_time"] == "2026-09-30T16:14:58+08:00"
    assert mapped[0]["volume"] is mapped[0]["amount"] is None
    keys = ("url", "method", "request_headers", "outcome", "status_code", "body_sha256", "error_type", "request_options")
    original_request, migrated_request = ({key: event.get(key) for key in keys} for event in (events[0], report["responses"][0]))
    assert original_request == migrated_request
    archived, body = quote_source()
    assert migrated_request["request_headers"] == archived["request_headers"]
    assert migrated_request["request_options"]["timeout"] == 20
    assert "Referer" not in migrated_request["request_headers"]
    snapshot_path = ROOT / "provider_validation/results/tencent-original-20261004/1-snapshot.py.bin"
    module = types.ModuleType("stock_data_manage.providers.tencent.original_quote")
    module.__package__ = "stock_data_manage.providers.tencent"
    import sys
    sys.modules[module.__name__] = module
    exec(compile(snapshot_path.read_bytes(), str(snapshot_path), "exec"), module.__dict__)
    class ArchivedTransport:
        def get(self, *args, **kwargs):
            return HttpResponse(200, archived["response_headers"], body)
    contract = next(c for c in load_input_capabilities(ROOT / "config/providers.yaml") if c.input_id == "ASTOCK-001")
    provider = build_input_provider(contract, providers_path=ROOT / "config/providers.yaml")
    provider.transport = ArchivedTransport()
    stamp = datetime.fromisoformat(QUOTE_CONTEXT["request"]["as_of"])
    legacy = module.TencentSnapshotProvider(ArchivedTransport(), provider.capability).fetch_snapshot(["sh600519"], stamp)
    current = provider.fetch_snapshot(["sh600519"], stamp)
    assert current.rows == legacy.rows
    comparison = {"input_id": "ASTOCK-001", "mode": "offline_replay", "source_probe": str(probe.relative_to(ROOT)),
        "source_response_sha256": archived["body_sha256"], "original_csv_sha256": hashlib.sha256(golden.read_bytes()).hexdigest(),
        "original_parsed_sha256": parsed_ref.content_hash, "original_parsed_path": str(parsed_ref.path),
        "provider_report": report["report_path"], "all_source_fields_equal": True, "all_mapped_prices_equal": True,
        "original_request": original_request, "provider_request": migrated_request, "request_equal": True,
        "legacy_rows_equal": True, "production_writes": 0, "eligible_for_production_routing": False}
    (tmp_path / "comparison.json").write_text(json.dumps(comparison, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return comparison


def test_quote_matches_original_script_csv_and_existing_provider(tmp_path, no_network):
    compare_quote_original(tmp_path)


def quote_fixture_manifest(tmp_path, symbols, *, mutation=None):
    from copy import deepcopy
    store = RawObjectStore(tmp_path / "fixture")
    original, _ = quote_source()
    for offset in range(0, len(symbols), 100):
        batch = symbols[offset:offset + 100]
        body = quote_payload(batch)
        if mutation == "duplicate":
            body += b"\n" + body
        elif mutation == "identity":
            body = body.replace(b"~600519~", b"~600000~")
        elif mutation == "timestamp":
            body = body.replace(b"20260930161458", b"20261399161458")
        elif mutation == "stale":
            body = body.replace(b"20260930161458", b"20260929161458")
        elif mutation == "numeric":
            body = body.replace(b"~1258.62~", b"~NaN~", 1)
        elif mutation == "unexpected":
            body = quote_payload(["sz300750"])
        elif mutation == "html":
            body = b"<html>not quote data</html>"
        elif mutation == "empty":
            body = b'v_sh600519="";'
        event = deepcopy(original)
        ref = store.write_bytes(body, dataset="fixture_payload", provider="tencent", endpoint="single_quote",
                                fetched_at=datetime.now(timezone.utc), attempt_id=str(offset), content_addressed=True)
        event.update(url="https://qt.gtimg.cn/q=" + ",".join(batch), mode="offline_fixture", synthetic=True,
                     body_storage=ref.path.relative_to(store.root).as_posix(), body_sha256=ref.content_hash,
                     requests_decoded_body_bytes=len(body), fixture_parent_sha256=original["body_sha256"], mutation=mutation)
        if mutation == "http_429":
            event["status_code"] = 429
        elif mutation == "transport":
            event.update(outcome="transport_error", error_type="ConnectionError")
        elif mutation == "corrupt_hash":
            event["body_sha256"] = "0" * 64
        store.append_event(event)
    return store.root / "manifest.ndjson"


def test_quote_multi_batch_yaml_and_partial_scope(tmp_path, no_network):
    symbols = tuple(f"sh{600000+i}" for i in range(201)) + ("sz300750", "bj920001")
    manifest = quote_fixture_manifest(tmp_path, symbols)
    context = {"request": {**QUOTE_CONTEXT["request"], "symbols": symbols}}
    report = collect_input(input_id="ASTOCK-001", context=context, config_root=ROOT / "config", output_root=tmp_path / "out",
                           replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    assert report["row_count"] == report["coverage_denominator"] == 203
    assert report["coverage_complete"] and not report["missing_symbols"]
    assert [len(event["url"].split("q=")[1].split(",")) for event in report["responses"]] == [100, 100, 3]
    assert {row["instrument_id"] for row in read_artifact(report, "output")} == {
        {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[s[:2]] + ":" + s[2:] for s in symbols}
    assert report["live_http_calls"] == report["production_writes"] == 0
    assert not report["online_batch_validation"] and not report["universe_completeness_verified"]
    partial_symbols = ["sh600519", "sz300750"]
    partial_manifest = quote_fixture_manifest(tmp_path / "p", partial_symbols)
    event = json.loads(partial_manifest.read_text(encoding="utf-8"))
    from copy import deepcopy
    partial_event = deepcopy(event)
    _, body = quote_source()
    ref = RawObjectStore(partial_manifest.parent).write_bytes(body, dataset="partial", provider="tencent", endpoint="single_quote",
        fetched_at=datetime.now(timezone.utc), attempt_id="partial", content_addressed=True)
    partial_event.update(body_storage=ref.path.relative_to(partial_manifest.parent).as_posix(), body_sha256=ref.content_hash,
                         requests_decoded_body_bytes=len(body))
    assert partial_event["requests_decoded_body_bytes"] == len(RawObjectStore.read_response(partial_manifest, partial_event))
    # A separate immutable manifest models a successful response missing one requested security.
    missing = partial_manifest.parent / "partial.ndjson"
    missing.write_text(json.dumps(partial_event) + "\n", encoding="utf-8")
    partial = collect_input(input_id="ASTOCK-001", context={"request": {**QUOTE_CONTEXT["request"], "symbols": partial_symbols}},
        config_root=ROOT / "config", output_root=tmp_path / "partial", replay_manifest=missing)
    assert partial["status"] == "candidate_complete", partial
    assert partial["row_count"] == 1 and partial["coverage_denominator"] == 2
    assert partial["missing_symbols"] == ["sz300750"] and not partial["coverage_complete"]


@pytest.mark.parametrize("mutation,expected", [("duplicate", "schema_changed"), ("identity", "schema_changed"),
    ("timestamp", "schema_changed"), ("stale", "NormalizationError"), ("numeric", "NormalizationError"),
    ("unexpected", "schema_changed"), ("html", "schema_changed"), ("empty", "temporary_empty"),
    ("http_429", "HTTPError"), ("transport", "ConnectionError"), ("corrupt_hash", "ValueError")])
def test_quote_failure_retains_evidence(tmp_path, no_network, mutation, expected):
    manifest = quote_fixture_manifest(tmp_path, ["sh600519"], mutation=mutation)
    report = collect_input(input_id="ASTOCK-001", context=QUOTE_CONTEXT, config_root=ROOT / "config",
        output_root=tmp_path / "out", replay_manifest=manifest)
    assert report["status"] == "failed" and report["failure_class"] == expected, report
    assert report["production_writes"] == report["live_http_calls"] == 0
    if mutation not in {"transport", "corrupt_hash"}:
        assert report["responses"][0]["body_sha256"] == json.loads(manifest.read_text(encoding="utf-8"))["body_sha256"]
        RawObjectStore.read_response(Path(report["run_directory"]) / report["raw_manifest"]["path"], report["responses"][0])


def test_quote_yaml_mapping_selection_and_scope_restrictions(tmp_path, no_network):
    config = tmp_path / "config"
    shutil.copytree(ROOT / "config", config)
    path = config / "normalization/realtime_quote.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["rules"][0]["field_mapping"]["price"] = "prev_close"
    path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
    report = collect_input(input_id="ASTOCK-001", context=QUOTE_CONTEXT, config_root=config, output_root=tmp_path / "out",
        replay_manifest=QUOTE_ARCHIVE, fields=["instrument_id", "quote_time", "price"])
    assert report["status"] == "candidate_complete", report
    assert read_artifact(report, "output") == [{"instrument_id": "XSHG:600519", "quote_time": "2026-09-30T16:14:58+08:00", "price": "1235.58"}]
    for request in ({"symbol": "600519"}, {"symbols": ["sh600519"], "as_of": "2026-09-30T15:10:00"}):
        with pytest.raises(ValueError):
            collect_input(input_id="ASTOCK-001", context={"request": request}, config_root=config,
                output_root=tmp_path / "out", replay_manifest=QUOTE_ARCHIVE)
    for symbols in (["sh600519", "sh600519"], ["sh510300"], ["sh600519&other=1"]):
        failed = collect_input(input_id="ASTOCK-001", context={"request": {**QUOTE_CONTEXT["request"], "symbols": symbols}},
            config_root=config, output_root=tmp_path / "out", replay_manifest=QUOTE_ARCHIVE)
        assert failed["status"] == "failed" and failed["failure_class"] == "ValueError", failed


def test_quote_injected_session_pacing_retry_and_cache(tmp_path, no_network):
    from unittest.mock import patch
    calls, sessions, starts = [], [], []
    clock = [0.0]
    pacer = RequestPacer(clock=lambda: clock[0], wait=lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    symbols = tuple(f"sh{600000+i}" for i in range(201))
    def fake_send(session, request, **kwargs):
        starts.append(clock[0]); calls.append(request.url); sessions.append(session)
        policy = session.adapters["https://"].max_retries
        assert policy.total == policy.connect == policy.read == policy.status == 2
        assert not policy.is_retry("GET", 429) and not policy.is_retry("GET", 403)
        assert policy.increment(method="GET", error=requests.ConnectionError()).get_backoff_time() >= 5
        assert session.trust_env and kwargs["timeout"] == 20 and kwargs["allow_redirects"]
        assert "Referer" not in request.headers
        response = requests.Response(); response.status_code = 200
        response.headers["Content-Type"] = "text/html; charset=GBK"; response.encoding = "gbk"
        response._content = quote_payload(request.url.split("q=", 1)[1].split(",")); response._content_consumed = True
        response.url = request.url; response.request = request
        return response
    context = {"request": {**QUOTE_CONTEXT["request"], "symbols": symbols}}
    with patch("requests.Session.send", fake_send):
        report = collect_input(input_id="ASTOCK-001", context=context, config_root=ROOT / "config", output_root=tmp_path / "out",
            mode="live", evidence_root=tmp_path / "empty", pacer=pacer)
        assert report["status"] == "candidate_complete", report
        assert starts == [0, 3, 6] and len({id(s) for s in sessions}) == 1
        cached = collect_input(input_id="ASTOCK-001", context=context, config_root=ROOT / "config", output_root=tmp_path / "out",
            mode="live", evidence_root=tmp_path / "empty", pacer=pacer)
    assert cached["status"] == "candidate_complete", cached
    assert len(calls) == 3 and cached["live_http_calls"] == 0
    assert all(event["mode"] == "cached" for event in cached["responses"])
    (tmp_path / "fixture-validation.json").write_text(json.dumps({"mode": "injected Session fixture; no real live source calls",
        "requests": calls, "start_seconds": starts, "shared_session": True, "cache_reused": True,
        "retry_policy_equal_to_source_runner": True, "external_network_requests": 0}, indent=2) + "\n", encoding="utf-8")


@pytest.mark.parametrize("input_id,context,manifest,count", BAO_CASES)
def test_bao_archived_inputs_preserve_sdk_rows_and_coverage(tmp_path, no_network, input_id, context, manifest, count):
    import csv
    import re
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path, replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    assert report["row_count"] == count and report["coverage_denominator"] == 5223
    assert report["production_writes"] == report["live_sdk_calls"] == report["live_http_calls"] == 0
    assert not report["eligible_for_production_routing"] and not report["sdk_dependency"]["live_sdk_executed"]
    assert report["sdk_query_count"] == 2
    assert report["source_sdk_row_counts"] == {"query_all_stock": 7423, "query_stock_industry": 5556}
    assert report["source_capture_window"]["last"] == "2026-10-03T10:16:32.498000+00:00"
    assert report["missing_industry_symbols"] == ["sz001246", "sz301716"]
    raw_manifest = Path(report["run_directory"]) / report["raw_manifest"]["path"]
    payloads = []
    for event in report["responses"]:
        assert event["event"] == "source_payload" and event["mode"] == "replay"
        assert not event["request_options"]["tcp_wire_bytes_visible"]
        assert event["sdk_status_code"] == "0"
        payloads.append(json.loads(RawObjectStore.read_response(raw_manifest, event)))
    assert report["responses"][0]["request_parameters"] == {"day": "2026-09-30"}
    assert report["responses"][1]["request_parameters"] == {"date": "2026-09-30"}
    source, mapped = read_artifact(report, "source_rows"), read_artifact(report, "output")
    assert len(source) == len(mapped) == count
    assert "code_name" in source[0] and "canonical_stock_code" in source[0]
    if input_id.endswith("005"):
        pattern = re.compile(r"^(?:sh\.(?:60|68)\d{4}|sz\.(?:000|001|002|003|300|301)\d{3})$")
        stocks = {row["code"]: row for row in payloads[0]["rows"] if pattern.fullmatch(row["code"])}
        assert len(stocks) == 5223
        assert report["coverage_complete"] and report["missing_symbols"] == []
        for row in mapped:
            code = ("sh." if row["exchange"] == "XSHG" else "sz.") + row["stock_code"]
            original = stocks.pop(code)
            assert row["stock_name"] == original["code_name"]
            assert row["status"] == ("active" if original["tradeStatus"] == "1" else "suspended")
            assert row["trade_date"] == "2026-09-30"
        assert not stocks
    else:
        assert not report["coverage_complete"]
        assert report["missing_symbols"] == ["sz001246", "sz301716"]
        csv_path = manifest.parents[2] / "industry-membership.csv"
        with csv_path.open(encoding="utf-8-sig", newline="") as stream:
            assert mapped == list(csv.DictReader(stream))
        assert all(row["classification_update_date"] == "2026-09-28" for row in mapped)


def compare_bao_original(tmp_path):
    import sys
    import types
    from contextlib import contextmanager
    from unittest.mock import patch
    from stock_data_manage.providers.baostock.industry import BaoStockIndustryMembershipProvider
    from stock_data_manage.providers.baostock.session import captured_sdk_queries, logged_in_session
    snapshot = ROOT / "provider_validation/results/bao-original-20261004/industry.py.bin"
    metadata = json.loads((snapshot.parent / "manifest.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(snapshot.read_bytes()).hexdigest() == metadata["sources"][0]["sha256"]
    module = types.ModuleType("stock_data_manage.providers.baostock.original_industry_contract")
    module.__package__ = "stock_data_manage.providers.baostock"
    sys.modules[module.__name__] = module
    exec(compile(snapshot.read_bytes(), str(snapshot), "exec"), module.__dict__)
    outputs, requests = [], []
    for label, provider_class in (("original", module.BaoStockIndustryMembershipProvider), ("provider", BaoStockIndustryMembershipProvider)):
        store = RawObjectStore(tmp_path / label)
        with captured_sdk_queries(store, mode="replay", replay_manifest=BAO_ARCHIVE, evidence_roots=(), scope=BAO_CONTEXT,
                                  code_version="original-contract-comparison", trade_date=date(2026, 9, 30),
                                  pacer=RequestPacer(), interval_seconds=3, max_age_seconds=86400) as (client, archive, events):
            @contextmanager
            def session():
                with logged_in_session(client=client) as bs:
                    yield bs
            with patch.object(module if label == "original" else sys.modules[provider_class.__module__], "logged_in_session", session):
                result = provider_class().fetch_snapshot(date(2026, 9, 30), raw_archive=archive)
        store.write_json(list(result.rows), dataset="legacy_rows", provider="baostock", endpoint="industry_membership",
                         fetched_at=datetime.now(timezone.utc), attempt_id="parsed")
        outputs.append(result)
        requests.append([{key: event[key] for key in ("method", "request_parameters", "sdk_status_code", "body_sha256", "outcome")} for event in events])
    assert outputs[0].rows == outputs[1].rows
    assert outputs[0].requested_symbols == outputs[1].requested_symbols
    assert outputs[0].missing_symbols == outputs[1].missing_symbols == ("sz001246", "sz301716")
    assert outputs[0].coverage_denominator == outputs[1].coverage_denominator == 5223
    assert requests[0] == requests[1]
    comparison = {"original_source_sha256": metadata["sources"][0]["sha256"], "legacy_rows_compared": 5221,
                  "security_universe_compared": 5223, "missing_symbols": list(outputs[1].missing_symbols),
                  "all_legacy_fields_equal": True, "query_parameters_status_payload_equal": True, "request_comparison": requests,
                  "boundary": "SDK methods/decoded payloads; URL/headers/TCP wire bytes are not exposed",
                  "production_writes": 0, "network_requests": 0}
    (tmp_path / "comparison.json").write_text(json.dumps(comparison, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return comparison


def test_bao_matches_original_provider(tmp_path, no_network):
    compare_bao_original(tmp_path)


@pytest.mark.parametrize("input_id,count,missing", [("SDA-BOARD-005", 2, []), ("SDA-BOARD-006", 1, ["sz001246"])])
def test_bao_symbol_subset_is_local_and_preserves_missing_denominator(tmp_path, no_network, input_id, count, missing):
    context = {**BAO_CONTEXT, "request": {"trade_date": "2026-09-30", "symbols": ["sh600519", "sz001246"]}}
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path, replay_manifest=BAO_ARCHIVE)
    assert report["status"] == "candidate_complete", report
    assert report["row_count"] == count and report["coverage_denominator"] == 2
    assert report["missing_symbols"] == missing
    assert report["source_sdk_row_counts"]["query_all_stock"] == 7423


def test_bao_source_yaml_mapping_is_executed(tmp_path, no_network):
    config = tmp_path / "config"
    shutil.copytree(ROOT / "config", config)
    path = config / "normalization/industry_membership.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["rules"][0]["field_mapping"]["industry_name"] = "industryClassification"
    path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
    report = collect_input(input_id="SDA-BOARD-006", context=BAO_CONTEXT, config_root=config, output_root=tmp_path / "out", replay_manifest=BAO_ARCHIVE)
    assert report["status"] == "candidate_complete", report
    assert all(row["industry_name"] == row["classification"] for row in read_artifact(report, "output"))


def test_bao_requires_positive_calendar_and_rejects_ignored_single_symbol(tmp_path, no_network):
    for context in ({"request": {"trade_date": "2026-09-30"}},
                    {"request": {"trade_date": "2026-10-02"}, "calendar": BAO_CONTEXT["calendar"]},
                    {**BAO_CONTEXT, "request": {"trade_date": "2026-09-30", "symbol": "600519"}}):
        with pytest.raises(ValueError):
            collect_input(input_id="SDA-BOARD-005", context=context, config_root=ROOT / "config", output_root=tmp_path, replay_manifest=BAO_ARCHIVE)


def bao_empty_scope_check(tmp_path):
    from stock_data_manage.providers.baostock.industry import BaoStockIndustryMembershipProvider
    from stock_data_manage.providers.baostock.session import captured_sdk_queries
    from stock_data_manage.providers.contracts import ProviderContractError, FailureClass
    store = RawObjectStore(tmp_path)
    with captured_sdk_queries(store, mode="replay", replay_manifest=BAO_EMPTY_ARCHIVE, evidence_roots=(), scope={"date": "2026-10-02"},
                              code_version="empty-sdk-contract", trade_date=date(2026, 10, 2),
                              pacer=RequestPacer(), interval_seconds=3, max_age_seconds=86400) as (client, archive, events):
        with pytest.raises(ProviderContractError) as exc:
            BaoStockIndustryMembershipProvider(client=client).fetch_snapshot(date(2026, 10, 2), raw_archive=archive)
    assert exc.value.failure_class is FailureClass.TEMPORARY_EMPTY
    assert len(events) == 1 and events[0]["metadata"]["row_count"] == 0
    result = {"failure_class": exc.value.failure_class.value, "valid_empty_dataset": False,
              "sdk_status_code": events[0]["sdk_status_code"], "source_response_sha256": events[0]["body_sha256"],
              "mode": "offline_replay", "query_count": 1}
    (tmp_path / "validation.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def test_bao_archived_empty_response_is_not_valid_empty_dataset(tmp_path, no_network):
    bao_empty_scope_check(tmp_path)


def _sdk_fixture_archive(path, documents):
    store = RawObjectStore(path)
    for name, document in documents.items():
        payload = json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode()
        ref = store.write_bytes(payload, dataset="sdk_response", provider="baostock", endpoint=name,
                                fetched_at=datetime.now(timezone.utc), attempt_id="fixture", content_addressed=True)
        store.append_event({"event": "source_payload", "provider": "baostock", "endpoint": name,
                            "metadata": {"trade_date": "2026-09-30", "row_count": len(document["rows"]),
                                         "outcome": document.get("result", "success")},
                            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                            "body_storage": ref.path.relative_to(store.root).as_posix(), "body_sha256": ref.content_hash})
    return store.root / "manifest.ndjson"


@pytest.mark.parametrize("mutation,expected", [("sdk_error", "connection"), ("missing_query", "ValueError"),
                                               ("schema", "schema_changed"), ("duplicate", "schema_changed"),
                                               ("corrupt_hash", "ValueError"), ("secret_payload", "ValueError")])
def test_bao_failed_contract_retains_source_evidence(tmp_path, no_network, mutation, expected):
    documents = {}
    for line in BAO_ARCHIVE.read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        documents[event["endpoint"]] = json.loads(RawObjectStore.read_response(BAO_ARCHIVE, event))
    if mutation == "sdk_error":
        documents = {"query_all_stock": {"fields": [], "rows": [], "result": {"error_code": "10001001", "error_msg": "connection failed"}}}
    elif mutation == "missing_query":
        documents.pop("query_stock_industry")
    elif mutation == "schema":
        documents["query_stock_industry"]["fields"].remove("industryClassification")
        for row in documents["query_stock_industry"]["rows"]:
            row.pop("industryClassification")
    elif mutation == "duplicate":
        row = next(row for row in documents["query_all_stock"]["rows"] if row["code"] == "sh.600519")
        documents["query_all_stock"]["rows"].append(row)
    elif mutation == "secret_payload":
        documents["query_all_stock"]["fields"].append("password")
        for row in documents["query_all_stock"]["rows"]:
            row["password"] = "<fixture-secret>"
    manifest = _sdk_fixture_archive(tmp_path / "archive", documents)
    if mutation == "corrupt_hash":
        event = json.loads(manifest.read_text(encoding="utf-8").splitlines()[0])
        (manifest.parent / event["body_storage"]).write_bytes(b"corrupt-fixture")
    report = collect_input(input_id="SDA-BOARD-006", context=BAO_CONTEXT, config_root=ROOT / "config",
                           output_root=tmp_path / "out", replay_manifest=manifest)
    assert report["status"] == "failed" and report["failure_class"] == expected, report
    assert "output" not in report and report["live_sdk_calls"] == report["production_writes"] == 0
    assert report["responses"] and Path(report["report_path"]).exists()
    if mutation == "secret_payload":
        assert report["responses"][0]["redacted"]
        candidate = Path(report["run_directory"])
        assert all(b"<fixture-secret>" not in path.read_bytes() for path in candidate.rglob("*") if path.is_file())


def test_bao_live_fixture_preserves_session_pacing_and_reuses_cache(tmp_path, no_network):
    from types import SimpleNamespace
    from stock_data_manage.providers.baostock.session import _decoded_result_set
    clock = [0.0]
    calls = []
    class FixtureSDK:
        __version__ = "fixture-sdk-v1"
        def login(self):
            calls.append(("login", {}, clock[0]))
            return SimpleNamespace(error_code="0", error_msg="")
        def logout(self):
            calls.append(("logout", {}, clock[0]))
            return SimpleNamespace(error_code="0", error_msg="")
        def query_all_stock(self, **kwargs):
            calls.append(("query_all_stock", kwargs, clock[0]))
            clock[0] += 10  # A long first query must still retain the post-payload three-second gap.
            return _decoded_result_set({"fields": ["code", "tradeStatus", "code_name"], "rows": [
                {"code": "sh.600519", "tradeStatus": "1", "code_name": "样本股"}]})
        def query_stock_industry(self, **kwargs):
            calls.append(("query_stock_industry", kwargs, clock[0]))
            return _decoded_result_set({"fields": ["code", "code_name", "industry", "industryClassification", "updateDate"],
                "rows": [{"code": "sh.600519", "code_name": "样本股", "industry": "样本行业", "industryClassification": "证监会行业分类", "updateDate": "2026-09-28"}]})
    def advance(delay):
        clock[0] += delay
    sdk = FixtureSDK()
    kwargs = dict(context=BAO_CONTEXT, config_root=ROOT / "config", output_root=tmp_path / "out", evidence_root=tmp_path / "out",
                  mode="live", client=sdk, pacer=RequestPacer(clock=lambda: clock[0], wait=advance))
    first = collect_input(input_id="SDA-BOARD-005", **kwargs)
    assert first["status"] == "candidate_complete", first
    assert calls == [("login", {}, 0), ("query_all_stock", {"day": "2026-09-30"}, 0),
                     ("query_stock_industry", {"date": "2026-09-30"}, 13), ("logout", {}, 13)]
    assert first["live_sdk_calls"] == 2
    second = collect_input(input_id="SDA-BOARD-006", **kwargs)
    assert second["status"] == "candidate_complete", second
    assert len(calls) == 4 and second["live_sdk_calls"] == 0
    assert all(event["mode"] == "cached" for event in second["responses"])
    assert not second["sdk_dependency"]["live_sdk_executed"]


@pytest.mark.parametrize("input_id,context,manifest,count", THS_CASES)
def test_ths_archived_inputs_execute_source_yaml(tmp_path, no_network, input_id, context, manifest, count):
    pytest.importorskip("akshare")
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config",
                           output_root=tmp_path, replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    assert report["row_count"] == report["coverage_denominator"] == count
    assert report["production_writes"] == report["live_http_calls"] == 0
    assert not report["eligible_for_production_routing"]
    source, mapped = read_artifact(report, "source_rows"), read_artifact(report, "output")
    assert len(source) == len(mapped) == count
    assert report["source_capture_window"]["last"].startswith("2026-10-02")
    assert all(not event["request_options"]["sdk_retry_policy"] for event in report["responses"])
    raw_manifest = Path(report["run_directory"]) / report["raw_manifest"]["path"]
    for event in report["responses"]:
        assert event["mode"] == "replay"
        assert hashlib.sha256(RawObjectStore.read_response(raw_manifest, event)).hexdigest() == event["body_sha256"]
    if input_id == "SDA-BOARD-001":
        assert set(source[0]) == {"name", "code"}
        assert mapped[0]["board_code"] == source[0]["code"]
        assert mapped[0]["board_type"] == "industry"
    elif input_id == "SDA-BOARD-002":
        assert mapped[0]["board_code"] == "881121"
        assert all(row["volume"] is None and row["amount"] is None for row in mapped)
        assert all(Decimal(row["close"]) == Decimal(str(raw["收盘价"])) for row, raw in zip(mapped, source))
    else:
        assert "行业" in source[0] and "snapshot_at" not in source[0]
        assert all(row["snapshot_at"] == report["source_capture_window"]["last"] for row in mapped)
        assert all(row["money_inflow"] is None and row["money_outflow"] is None and row["net_inflow"] is None for row in mapped)
        assert all(row["board_type"] == ("industry" if input_id.endswith("003") else "concept") for row in mapped)


def compare_ths_original(tmp_path, input_id, context, manifest, count):
    """Save original and modified legacy returns plus an explicit request/row comparison."""
    import sys
    import types
    from functools import lru_cache
    from unittest.mock import patch
    import akshare
    from stock_data_manage.config.loader import load_input_capabilities
    from stock_data_manage.routing.factory import build_input_provider
    snapshot_root = ROOT / "provider_validation/results/ths-original-20261004"
    snapshot = snapshot_root / "boards.py.bin"
    metadata = json.loads((snapshot_root / "manifest.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(snapshot.read_bytes()).hexdigest() == metadata["sha256"]
    module = types.ModuleType("stock_data_manage.providers.akshare.original_board_contract")
    module.__package__ = "stock_data_manage.providers.akshare"
    sys.modules[module.__name__] = module
    exec(compile(snapshot.read_bytes(), str(snapshot), "exec"), module.__dict__)
    contract = next(c for c in load_input_capabilities(ROOT / "config/providers.yaml") if c.input_id == input_id)
    parameters = contract.bind_parameters(context)
    if input_id in {"SDA-BOARD-003", "SDA-BOARD-004"}:
        parameters["snapshot_at"] = datetime(2026, 10, 2, 16, tzinfo=timezone.utc)
    providers = (module.AkShareBoardProvider(client=akshare), build_input_provider(contract, providers_path=ROOT / "config/providers.yaml", client=akshare))
    outputs, events = [], []
    namespace = akshare.stock_board_industry_index_ths.__globals__
    helper = namespace["_get_stock_board_industry_name_ths"]
    for label, provider in zip(("original", "provider"), providers):
        store = RawObjectStore(tmp_path / label)
        with patch.dict(namespace, {"_get_stock_board_industry_name_ths": lru_cache()(helper.__wrapped__)}):
            with captured_requests(store, provider="ths", endpoint=contract.endpoint, scope=context, code_version="original-contract-comparison",
                                   pacer=RequestPacer(), replay_manifest=manifest):
                result = getattr(provider, contract.runtime_method)(**parameters)
        saved = store.write_json(list(result.rows), dataset="legacy_rows", provider="ths", endpoint=contract.endpoint,
                                 fetched_at=datetime.now(timezone.utc), attempt_id="parsed")
        outputs.append(result)
        events.append([json.loads(line) for line in (store.root / "manifest.ndjson").read_text(encoding="utf-8").splitlines()])
    assert outputs[0].rows == outputs[1].rows
    assert len(outputs[0].rows) == count
    assert outputs[0].returned_first_key == outputs[1].returned_first_key
    assert outputs[0].returned_last_key == outputs[1].returned_last_key
    import csv
    filenames = {"SDA-BOARD-001": "ths-industry-directory.csv", "SDA-BOARD-002": "ths-semiconductor-index-daily.csv",
                 "SDA-BOARD-003": "ths-industry-fund-flow-now.csv", "SDA-BOARD-004": "ths-concept-fund-flow-now.csv"}
    csv_path = ROOT / "provider_validation/results/2026-10-02-sector-derived" / filenames[input_id]
    with csv_path.open(encoding="utf-8-sig", newline="") as stream:
        golden_rows = list(csv.DictReader(stream))
    assert len(golden_rows) == count
    for golden, actual in zip(golden_rows, outputs[1].rows):
        assert set(golden) == set(actual)
        for key in golden:
            if key == "snapshot_at":
                continue  # Explicit legacy caller timestamp above; actual archive time is checked on candidate outputs.
            if isinstance(actual[key], (int, float)):
                assert Decimal(golden[key]) == Decimal(str(actual[key]))
            else:
                assert golden[key] == str(actual[key])
    # SDK's dynamic anti-bot value varies per invocation. Compare redacted header shape,
    # and record the known difference without claiming replay proves current acceptance.
    keys = ("url", "method", "outcome", "status_code", "body_sha256", "error_type", "request_options")
    requests_compared = []
    assert len(events[0]) == len(events[1])
    for left, right in zip(*events):
        assert all(left.get(key) == right.get(key) for key in keys)
        left_headers, right_headers = left["request_headers"].copy(), right["request_headers"].copy()
        for headers in (left_headers, right_headers):
            if "hexin-v" in headers:
                headers["hexin-v"] = "<dynamic-anti-bot-value>"
        assert left_headers == right_headers
        requests_compared.append({"original": {**{key: left.get(key) for key in keys}, "request_headers": left_headers},
                                  "provider": {**{key: right.get(key) for key in keys}, "request_headers": right_headers}, "equal_except_dynamic_value": True})
    comparison = {"input_id": input_id, "original_source_sha256": metadata["sha256"], "rows_compared": count,
                  "all_legacy_fields_equal": True, "returned_window_equal": True,
                  "original_parsed_csv": csv_path.relative_to(ROOT).as_posix(),
                  "original_parsed_csv_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
                  "all_archived_source_values_equal": True,
                  "first_key": outputs[1].returned_first_key, "last_key": outputs[1].returned_last_key,
                  "request_comparison": requests_compared, "failure_class": None,
                  "limitations": ["离线原响应；动态反爬值只比较头名称和生成方式，不证明当前可用性。"]}
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "comparison.json").write_text(json.dumps(comparison, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count", THS_CASES)
def test_ths_matches_original_provider(tmp_path, no_network, input_id, context, manifest, count):
    pytest.importorskip("akshare")
    compare_ths_original(tmp_path, input_id, context, manifest, count)


def test_ths_mapping_uses_source_columns_and_rejects_identity_mismatch(tmp_path, no_network):
    pytest.importorskip("akshare")
    config = tmp_path / "config"
    shutil.copytree(ROOT / "config", config)
    input_id, context, manifest, _ = THS_CASES[1]
    path = config / "normalization/industry_index_daily.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["rules"][0]["field_mapping"]["open"] = "收盘价"
    path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
    report = collect_input(input_id=input_id, context=context, config_root=config, output_root=tmp_path / "out", replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    assert all(row["open"] == row["close"] for row in read_artifact(report, "output"))
    failed = collect_input(input_id=input_id, context={**context, "dependency": {"board_code": "wrong"}}, config_root=config,
                           output_root=tmp_path / "out", replay_manifest=manifest)
    assert failed["status"] == "failed" and "source directory" in failed["error"]


def test_ths_replay_miss_is_migration_failure_and_period_is_bounded(tmp_path, no_network):
    pytest.importorskip("akshare")
    missing = tmp_path / "empty.ndjson"
    missing.write_text("", encoding="utf-8")
    failed = collect_input(input_id="SDA-BOARD-001", context={}, config_root=ROOT / "config", output_root=tmp_path, replay_manifest=missing)
    assert failed["status"] == "failed" and failed["failure_class"] == "ValueError"
    assert "never falls back" in failed["error"]
    with pytest.raises(ValueError, match="period"):
        collect_input(input_id="SDA-BOARD-003", context={"config": {"period": "5日排行"}}, config_root=ROOT / "config",
                      output_root=tmp_path, replay_manifest=THS_ARCHIVE)


def read_artifact(report, key):
    path = Path(report["run_directory"]) / report[key]["path"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == report[key]["sha256"]
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def no_network(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("offline validation must never issue a network request")
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", fail)
    import socket
    original_connect = socket.socket.connect
    def guard_connect(sock, address):
        # Windows asyncio builds its self-pipe through a loopback socket pair.
        if isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}:
            return original_connect(sock, address)
        fail()
    monkeypatch.setattr(socket.socket, "connect", guard_connect)


@pytest.mark.parametrize("input_id,context,manifest,count", CASES)
def test_archived_inputs_execute_yaml_and_preserve_evidence(tmp_path, no_network, input_id, context, manifest, count):
    if input_id in {"ASTOCK-045", "ASTOCK-070"}:
        pytest.importorskip("akshare")
    original_send, original_init = requests.Session.send, requests.Session.__init__
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config",
                           output_root=tmp_path, replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    assert report["row_count"] == report["coverage_denominator"] == count
    assert not report["eligible_for_production_routing"] and report["production_writes"] == report["live_http_calls"] == 0
    assert requests.Session.send is original_send and requests.Session.__init__ is original_init
    assert report["normalization_status"] == "pending_validation"
    source, mapped = read_artifact(report, "source_rows"), read_artifact(report, "output")
    raw_manifest = Path(report["run_directory"]) / report["raw_manifest"]["path"]
    for event in report["responses"]:
        assert event["mode"] == "replay"
        if event["outcome"] == "response":
            body = RawObjectStore.read_response(raw_manifest, event)
            assert len(body) == event["body_bytes"]
            assert event["source_ref"]["manifest"] == str(manifest.resolve())
    if input_id.startswith("ASTOCK-002"):
        assert all(row["volume"] is None for row in mapped)
        assert all(row["5"] is not None for row in source)
        assert all(Decimal(row["close"]) == Decimal(raw["2"]) for row, raw in zip(mapped, source))
        assert all(row["amount"] is None for row in mapped)
    elif input_id == "ASTOCK-045":
        pandas = pytest.importorskip("pandas")
        expected = pandas.read_csv(ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003/43_东财涨停池/data.csv", dtype=str)
        assert set(source[0]) == set(expected.columns)
        for actual, golden in zip(source, expected.to_dict("records")):
            for key, value in golden.items():
                if key in {"序号", "涨跌幅", "最新价", "成交额", "流通市值", "总市值", "换手率", "封板资金", "炸板次数", "连板数"}:
                    assert float(actual[key]) == pytest.approx(float(value), rel=1e-12)
                else:
                    assert str(actual[key]) == value
        assert mapped[0]["source_security_code"] == source[0]["代码"]
        assert all(row["amount"] is None for row in mapped)
    else:
        expected = (ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003/68_交易日历/data.csv").read_text(encoding="utf-8-sig").splitlines()[1:]
        assert [row["trade_date"] for row in source] == expected
        assert [row["trade_date"] for row in mapped] == expected


@pytest.mark.parametrize("input_id,context,manifest,count", CASES[:2])
def test_tencent_matches_original_shipped_script(tmp_path, no_network, input_id, context, manifest, count):
    path = ROOT / "provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py"
    spec = importlib.util.spec_from_file_location("original_v39_input_comparison", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    namespace = module.load_shipped_code()
    store = RawObjectStore(tmp_path / "original")
    with captured_requests(store, provider="tencent", endpoint=input_id, scope=context, code_version="source-snapshot",
                           pacer=RequestPacer(), replay_manifest=manifest) as events:
        if input_id.endswith("daily"):
            frame = namespace["tencent_kline"]("600519", period="day", adjust="qfq", start="2026-09-01", end="2026-09-18")
        else:
            frame = namespace["tencent_kline"]("300750", period="m5", count=96)
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "adapter", replay_manifest=manifest)
    assert report["status"] == "candidate_complete"
    mapped, source = read_artifact(report, "output"), read_artifact(report, "source_rows")
    assert len(frame) == len(mapped) == count
    for golden, actual, raw in zip(frame.to_dict("records"), mapped, source):
        for name in ("open", "high", "low", "close"):
            assert Decimal(actual[name]) == Decimal(str(golden[name]))
        assert Decimal(raw["5"]) == Decimal(str(golden["volume"]))
        assert actual["trade_date"] == (golden["date"] if input_id.endswith("daily") else golden["datetime"][:10])
        if input_id.endswith("5m"):
            assert actual["bar_time"].replace("T", " ")[:16] == golden["datetime"]
            assert float(actual["turnover_rate_pct"]) == pytest.approx(golden["turnover_rate_pct"], rel=1e-12)
            assert Decimal(actual["turnover_rate_pct"]) == Decimal(raw["7"]) / 100
    assert report["source_url"] == frame["source_url"].iloc[0]
    # Compare original and migrated requests, statuses, failures and returned body hashes.
    original_events = [json.loads(line) for line in (store.root / "manifest.ndjson").read_text(encoding="utf-8").splitlines()]
    for left, right in zip(original_events, report["responses"]):
        for key in ("url", "method", "request_headers", "outcome", "status_code", "body_sha256", "error_type"):
            assert left.get(key) == right.get(key), key
    assert len(original_events) == len(report["responses"])


def test_yaml_alias_mapping_status_and_code_scope_are_executed(tmp_path):
    document = {"dataset": "daily_bar", "rules": [{"provider": "fixture", "endpoint": "daily", "exchange": "XSHG", "asset_type": "stock", "frequency": None,
        "adjustment": "none", "volume_multiplier": "100", "amount_multiplier": "10000", "version": "alias-test",
        "status": "pending_validation", "code_prefixes": ["sh"], "null_values": [None, "--"],
        "field_mapping": {"symbol": "ticker", "trade_date": "date", "open": "O", "high": "H", "low": "L", "close": "C", "volume": "V", "amount": "A"}}]}
    path = tmp_path / "daily_bar.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    rules = load_dataset_normalization_rules(tmp_path, "daily_bar")
    raw = {"ticker": "sh600519", "date": "2026-09-18", "O": "10", "H": "11", "L": "9", "C": "10.5", "V": "2", "A": "--"}
    kwargs = dict(dataset=Dataset.DAILY_BAR, instrument_id="XSHG:600519", exchange=Exchange.XSHG, asset_type=AssetType.STOCK,
                  provider="fixture", endpoint="daily", adjustment=Adjustment.NONE, capability_priority=1, capability_version="fixture", raw_object_path="fixture.json",
                  fetch_time=datetime.now(timezone.utc), quality_status=QualityStatus.PROVISIONAL)
    with pytest.raises(NormalizationError, match="not validated"):
        Normalizer(rules).normalize_bar(raw, **kwargs)
    record = Normalizer(rules, allow_pending=True).normalize_bar(raw, **kwargs)
    assert record.close == Decimal("10.5") and record.volume == 200 and record.amount is None
    with pytest.raises(NormalizationError, match="found 0"):
        Normalizer(rules, allow_pending=True).normalize_bar({**raw, "ticker": "sz000001"}, **kwargs)


def test_yaml_types_units_dates_timezone_and_nullable_values():
    fields = {"date": {"type": "date", "required": True}, "time": {"type": "datetime", "required": True},
              "value": {"type": "decimal"}, "count": {"type": "integer"}, "nullable": {"type": "decimal"}}
    rule = {"status": "validated", "field_mapping": {"date": "D", "time": "T", "value": "V", "count": "N", "nullable": "X"},
            "null_values": [None, "--"], "transforms": {"date": {"format": "%Y%m%d"}, "time": {"format": "%Y%m%d%H%M", "timezone": "UTC"},
                                                      "value": {"remove_commas": True, "multiplier": "100"}}}
    row = Normalizer.normalize_fields({"D": "20260918", "T": "202609180930", "V": "1,234.50", "N": "2", "X": "--"}, rule=rule, fields=fields)
    assert row["date"] == date(2026, 9, 18) and row["time"].utcoffset().total_seconds() == 0
    assert row["value"] == Decimal("123450") and row["count"] == 2 and row["nullable"] is None


@pytest.mark.parametrize("value", [True, "nan", "inf", None, "garbage"])
def test_unverified_units_still_require_valid_source_numbers(value):
    rule = {"status": "pending_validation", "field_mapping": {"volume": "5"}, "unverified_fields": ["volume"], "source_required_numeric": ["5"]}
    with pytest.raises(NormalizationError, match="source number"):
        Normalizer.normalize_fields({"5": value}, rule=rule, fields={"volume": {"type": "decimal"}}, allow_pending=True)


@pytest.mark.parametrize("status", ["pending_validation", "disabled", "expired"])
def test_unvalidated_generic_rules_cannot_publish(status):
    with pytest.raises(NormalizationError, match="not validated"):
        Normalizer.normalize_fields({}, rule={"status": status}, fields={})


def test_field_selection_uses_yaml_and_requires_mandatory_fields(tmp_path, no_network):
    config = tmp_path / "config"
    shutil.copytree(ROOT / "config", config)
    fields = yaml.safe_load((config / "datasets/daily_bar.yaml").read_text(encoding="utf-8"))["fields"]
    required = [name for name, definition in fields.items() if definition.get("required")]
    input_id, context, manifest, _ = CASES[0]
    report = collect_input(input_id=input_id, context=context, config_root=config, output_root=tmp_path / "out", replay_manifest=manifest, fields=required)
    assert report["status"] == "candidate_complete"
    assert set(read_artifact(report, "output")[0]) == set(required)
    with pytest.raises(ValueError, match="required fields"):
        collect_input(input_id=input_id, context=context, config_root=config, output_root=tmp_path / "out", replay_manifest=manifest, fields=["close"])
    # Changing only the YAML mapping changes the actual collected field.
    path = config / "normalization/daily_bar.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["rules"][0]["field_mapping"]["open"] = "2"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    changed = collect_input(input_id=input_id, context=context, config_root=config, output_root=tmp_path / "out", replay_manifest=manifest)
    assert changed["status"] == "candidate_complete"
    assert all(row["open"] == row["close"] for row in read_artifact(changed, "output"))


def test_replay_missing_request_is_failure_with_no_network_fallback(tmp_path, no_network):
    input_id, context, manifest, _ = CASES[0]
    original = requests.Session.send
    report = collect_input(input_id=input_id, context={"request": {**context["request"], "symbol": "000001"}},
                           config_root=ROOT / "config", output_root=tmp_path, replay_manifest=manifest)
    assert report["status"] == "failed" and report["failure_class"] == "ValueError"
    assert "never falls back" in report["error"]
    assert requests.Session.send is original
    assert Path(report["report_path"]).is_file()


def test_date_snapshot_requires_explicit_calendar_and_blocks_production_output(tmp_path):
    with pytest.raises(ValueError, match="calendar coverage"):
        collect_input(input_id="ASTOCK-045", context={"request": {"trade_date": date(2026, 9, 30)}}, config_root=ROOT / "config", output_root=tmp_path, replay_manifest=SDK_ARCHIVE)
    with pytest.raises(ValueError, match="production"):
        collect_input(input_id="ASTOCK-070", context={}, config_root=ROOT / "config", output_root=ROOT / "data/raw", replay_manifest=SDK_ARCHIVE)


def test_exact_response_is_saved_before_json_parser_and_secrets_are_redacted(tmp_path, no_network):
    archive = RawObjectStore(tmp_path / "archive")
    response = requests.Response()
    response.status_code, response._content, response.encoding = 200, b"not-json\x00\xff", "utf-8"
    request = requests.Request("GET", "https://example.test/input?token=hidden", headers={"Authorization": "hidden", "hexin-v": "hidden"}).prepare()
    original_event = archive.record_response(response=response, url=request.url, method="GET", request_headers=request.headers,
        scope={"token": "hidden"}, provider="fixture", endpoint="input", code_version="fixture")
    destination = RawObjectStore(tmp_path / "destination")
    with captured_requests(destination, provider="fixture", endpoint="input", scope={}, code_version="fixture", pacer=RequestPacer(), replay_manifest=archive.root / "manifest.ndjson"):
        result = requests.get(request.url)
        assert RawObjectStore.read_response(destination.root / "manifest.ndjson", original_event) == response.content
        with pytest.raises(requests.exceptions.JSONDecodeError):
            result.json()
    assert "hidden" not in (archive.root / "manifest.ndjson").read_text(encoding="utf-8")
    response._content = b'{"access_token":"sensitive"}'
    with pytest.raises(ValueError, match="retention suppressed"):
        destination.record_response(response=response, url=request.url, method="GET", request_headers={}, scope={}, provider="fixture", endpoint="input", code_version="fixture")
    assert not (destination.root / "bodies" / (hashlib.sha256(response.content).hexdigest() + ".bin")).exists()


@pytest.mark.parametrize("mutation", ["duplicate", "outside_window", "empty_adjusted", "malformed_volume"])
def test_daily_source_contract_rejects_invalid_rows_and_keeps_original_bytes(tmp_path, no_network, mutation):
    original = next(json.loads(line) for line in TENCENT_ARCHIVE.read_text(encoding="utf-8").splitlines() if "fqkline" in line)
    body = json.loads(RawObjectStore.read_response(TENCENT_ARCHIVE, original))
    bars = body["data"]["sh600519"]["qfqday"]
    if mutation == "duplicate":
        bars.append(bars[0])
    elif mutation == "outside_window":
        bars[0][0] = "2026-08-31"
    elif mutation == "empty_adjusted":
        body["data"]["sh600519"]["day"] = bars
        body["data"]["sh600519"]["qfqday"] = []
    else:
        bars[0][5] = True
    response = requests.Response()
    response.status_code, response._content, response.encoding = 200, json.dumps(body).encode(), "utf-8"
    archive = RawObjectStore(tmp_path / "source")
    event = archive.record_response(response=response, url=original["url"], method="GET", request_headers={}, scope={}, provider="tencent", endpoint="kline_daily", code_version="fixture")
    input_id, context, _, _ = CASES[0]
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "candidate", replay_manifest=archive.root / "manifest.ndjson")
    assert report["status"] == "failed"
    captured = report["responses"][0]
    assert captured["body_sha256"] == event["body_sha256"]
    assert RawObjectStore.read_response(Path(report["run_directory"]) / report["raw_manifest"]["path"], captured) == response.content
    assert "output" not in report


def test_live_path_reuses_matching_fresh_response_before_repeating_request(tmp_path, monkeypatch):
    original = next(json.loads(line) for line in TENCENT_ARCHIVE.read_text(encoding="utf-8").splitlines() if "fqkline" in line)
    body = RawObjectStore.read_response(TENCENT_ARCHIVE, original)
    calls = []
    def simulated_send(session, request, **kwargs):
        calls.append((request, kwargs))
        response = requests.Response()
        response.status_code, response._content, response.encoding = 200, body, "utf-8"
        response.url, response.request = request.url, request
        return response
    monkeypatch.setattr(requests.Session, "send", simulated_send)
    input_id, context, _, _ = CASES[0]
    kwargs = dict(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "out", evidence_root=tmp_path, mode="live")
    first = collect_input(**kwargs)
    second = collect_input(**kwargs)
    assert first["status"] == second["status"] == "candidate_complete"
    assert len(calls) == 1 and first["live_http_calls"] == 1 and second["live_http_calls"] == 0
    assert calls[0][1]["timeout"] == (8, 20)
    assert second["responses"][0]["mode"] == "cached"
    assert second["responses"][0]["source_ref"]["sha256"] == first["responses"][0]["body_sha256"]


def test_sdk_session_retry_policy_and_restoration(tmp_path):
    original_init = requests.Session.__init__
    with captured_requests(RawObjectStore(tmp_path), provider="fixture", endpoint="sdk", scope={}, code_version="fixture", pacer=RequestPacer(), sdk_retry_policy=True):
        with requests.Session() as session:
            policy = session.adapters["https://"].max_retries
            assert policy.total == 2 and policy.respect_retry_after_header
            assert not policy.is_retry("GET", 403, True) and not policy.is_retry("GET", 429, True)
            assert policy.is_retry("GET", 503, False)
            assert policy.increment(method="GET", error=ConnectionError()).get_backoff_time() >= 5
    assert requests.Session.__init__ is original_init



def test_candidate_response_archive_fits_nested_windows_path(tmp_path, no_network):
    padding = max(1, 130 - len(str(tmp_path.resolve())) - 1)
    output = tmp_path / ("x" * padding)
    report = collect_input(input_id="ASTOCK-002-5m", context={"request": {"symbol": "sz300750"}},
                           config_root=ROOT / "config", output_root=output, replay_manifest=TENCENT_ARCHIVE)
    assert report["status"] == "candidate_complete", report
    assert len(Path(report["run_directory"]).name) == len("ASTOCK-002-5m-") + 12
    manifest = Path(report["run_directory"]) / report["raw_manifest"]["path"]
    for event in report["responses"]:
        if event["outcome"] == "response":
            assert len(str((manifest.parent / event["body_storage"]).resolve())) < 250
            RawObjectStore.read_response(manifest, event)
