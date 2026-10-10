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
V310_ARCHIVE = ROOT / "provider_validation/results/raw/2026-10-01-v310-live/manifest.ndjson"
NEWS_NATIVE_ARCHIVE = ROOT / "provider_validation/results/remaining-evidence-direct-20261004/ASTOCK-031/_raw/manifest.ndjson"
VALUE_NATIVE_ARCHIVE = ROOT / "provider_validation/results/remaining-evidence-direct-20261004/ASTOCK-069/_raw/manifest.ndjson"


def remaining_day(day, **parameters):
    return {"request": {"trade_date": day, **parameters}, "calendar": {"trading_dates": [date.fromisoformat(day)]}}


REMAINING_CASES = [
    ("ASTOCK-031", {"request": {"symbol": "600519"}}, NEWS_NATIVE_ARCHIVE, 10),
    ("ASTOCK-069", {"request": {"index_code": "000300"}}, VALUE_NATIVE_ARCHIVE, 20),
    ("ASTOCK-039", {"request": {"symbol": "600519"}}, SDK_ARCHIVE, 305),
    ("ASTOCK-055", remaining_day("2026-09-30"), SDK_ARCHIVE, 580),
    ("ASTOCK-067", {"request": {"index_code": "000300"}}, SDK_ARCHIVE, 300),
    ("ASTOCK-068", {"request": {"index_code": "000300"}}, SDK_ARCHIVE, 300),
    ("ASTOCK-085", remaining_day("2026-09-30"), SDK_ARCHIVE, 83),
    ("ASTOCK-003", remaining_day("2026-09-18"), TENCENT_ARCHIVE, 52314),
    ("ASTOCK-004", {"request": {"symbol": "000001"}}, V310_ARCHIVE, 4445),
    ("ASTOCK-030", remaining_day("2026-09-18", exchange="SH"), TENCENT_ARCHIVE, 912),
    ("ASTOCK-063", {"request": {"start_date": "2026-09-01", "end_date": "2026-09-18", "curve": "all"}}, TENCENT_ARCHIVE, 42),
    ("ASTOCK-077", {"request": {"instrument": "Au99.99"}}, TENCENT_ARCHIVE, 2375),
    ("ASTOCK-051", {}, SDK_ARCHIVE, 7831),
] + [("ASTOCK-071", remaining_day("2026-09-18", exchange=exchange), TENCENT_ARCHIVE, count)
     for exchange, count in (("SHFE", 238), ("INE", 63), ("CZCE", 243), ("CFFEX", 28), ("GFEX", 48))] + [
    ("ASTOCK-072", remaining_day("2026-09-18", exchange="SHFE"), TENCENT_ARCHIVE, 6546),
    ("ASTOCK-072", remaining_day("2026-09-18", exchange="CFFEX"), TENCENT_ARCHIVE, 692),
    ("ASTOCK-073", remaining_day("2026-09-18", exchange="CFFEX"), TENCENT_ARCHIVE, 440)]


def compare_remaining_original(tmp_path, input_id, context, manifest, count):
    """Run independent shipped functions before comparing every candidate business field."""
    import sys
    import pandas as pd
    import akshare as sdk
    from stock_data_manage.pipeline.inputs import _json_value
    is_sdk = input_id in {"ASTOCK-031", "ASTOCK-039", "ASTOCK-055", "ASTOCK-067", "ASTOCK-068", "ASTOCK-069", "ASTOCK-085", "ASTOCK-051"}
    request = context.get("request", {})
    ns = None
    source = ROOT / "provider_validation/tests/source_snapshots/a-stock-data"
    if not is_sdk:
        tests = source / "tests"
        sys.path.insert(0, str(tests))
        name = "test_v310_sources" if input_id == "ASTOCK-004" else "test_v39_sources"
        spec = importlib.util.spec_from_file_location(name, tests / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        ns = module.load_v310() if input_id == "ASTOCK-004" else module.load_shipped_code()
    store = RawObjectStore(tmp_path / "original")
    frames = []
    try:
        with captured_requests(store, provider="original", endpoint=input_id, scope=context,
                code_version=hashlib.sha256((source / "SKILL.md").read_bytes()).hexdigest(),
                pacer=RequestPacer(wait=lambda _: None), replay_manifest=manifest,
                sdk_retry_policy=is_sdk and input_id not in {'ASTOCK-031','ASTOCK-069'}, probe_host_pause=is_sdk,
                native_transport='curl_cffi' if input_id=='ASTOCK-031' else 'pandas_urllib' if input_id=='ASTOCK-069' else None) as events:
            if input_id == 'ASTOCK-031':
                f=sdk.stock_news_em(symbol='600519')
                body=RawObjectStore.read_response(store.root/'manifest.ndjson',events[0]).decode('utf-8')
                items=json.loads(body[body.index('(')+1:body.rindex(')')])['result']['cmsArticleWebOld']
                frames=[f.assign(source_article_code=[item['code'] for item in items])]
            elif input_id == 'ASTOCK-069': frames=[sdk.stock_zh_index_value_csindex(symbol='000300')]
            elif input_id == "ASTOCK-039":
                for table in ("资产负债表", "利润表", "现金流量表"):
                    f = sdk.stock_financial_report_sina(stock="sh600519", symbol=table)
                    frames.append(f.assign(report_type=table, source_security_code="600519"))
            elif input_id == "ASTOCK-055": frames = [sdk.option_risk_indicator_sse(date="20260930")]
            elif input_id == "ASTOCK-067": frames = [sdk.index_stock_cons_csindex(symbol="000300")]
            elif input_id == "ASTOCK-068": frames = [sdk.index_stock_cons_weight_csindex(symbol="000300")]
            elif input_id == "ASTOCK-085":
                f = sdk.stock_lhb_detail_daily_sina(date="20260930")
                frames = [f.assign(trade_date="2026-09-30", source_row_number=range(1, len(f)+1))]
            elif input_id == "ASTOCK-051":
                kinds = ("火箭发射", "快速反弹", "大笔买入", "封涨停板", "打开跌停板", "有大买盘", "竞价上涨", "高开5日线", "向上缺口", "60日新高", "60日大幅上涨", "加速下跌", "高台跳水", "大笔卖出", "封跌停板", "打开涨停板", "有大卖盘", "竞价下跌", "低开5日线", "向下缺口", "60日新低", "60日大幅下跌")
                offset = 0
                for kind in kinds:
                    f = sdk.stock_changes_em(symbol=kind)
                    frames.append(f.assign(异动类型=kind, source_row_number=range(offset+1, offset+len(f)+1)))
                    offset += len(f)
            elif input_id == "ASTOCK-003": frames = [ns["tdx_daily_package"]("2026-09-18")]
            elif input_id == "ASTOCK-004": frames = [ns["tencent_ticks"]("sz000001")]
            elif input_id == "ASTOCK-030": frames = [ns["etf_shares"]("2026-09-18", "SH")]
            elif input_id == "ASTOCK-063": frames = [ns["chinabond_yield_curve"]("2026-09-01", "2026-09-18", "all")]
            elif input_id == "ASTOCK-077": frames = [ns["sge_spot"]("Au99.99")]
            else:
                function = {"ASTOCK-071": "futures_daily", "ASTOCK-072": "options_daily", "ASTOCK-073": "futures_position_rank"}[input_id]
                frames = [ns[function]("2026-09-18", request["exchange"])]
    finally:
        if ns is not None: ns["EM_SESSION"].close()
    parsed = [r for f in frames for r in _json_value(f.astype(object).where(f.notna(), None).to_dict(orient="records"))]
    path = tmp_path / "original-parsed.json"
    path.write_bytes((json.dumps(parsed, ensure_ascii=False, indent=2) + "\n").encode())
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "candidate", replay_manifest=manifest)
    assert report["status"] == "candidate_complete", {k: report.get(k) for k in ("status", "failure_class", "failure_message")}
    assert report["row_count"] == len(parsed)
    if count: assert len(parsed) == count
    candidate = read_artifact(report, "parsed_rows")
    assert len(candidate) == len(parsed)
    for row, old in zip(candidate, parsed):
        assert row == {key: old[key] for key in row}
        assert set(old) - set(row) <= {"source", "source_url", "fetched_at"}
    keys = ("url", "method", "outcome", "status_code", "body_sha256", "request_headers", "request_options")
    assert [{k: r.get(k) for k in keys} for r in events] == [{k: r.get(k) for k in keys} for r in report["responses"]]
    assert report["production_writes"] == report["live_http_calls"] == 0
    assert not report["eligible_for_production_routing"] and not report["source_fallback_enabled"]
    for row in read_artifact(report, "output"):
        assert row["snapshot_at"] == report["source_capture_window"]["last"]
        assert all(row[field] is None for field in report["unverified_fields"])
        if input_id=='ASTOCK-069':assert row['index_code']=='000300'
    if input_id in {'ASTOCK-031','ASTOCK-069'}:
        golden=ROOT/'provider_validation/results/remaining-evidence-direct-20261004'/input_id/'parsed.json'
        assert [{k:v for k,v in row.items() if k!='source_article_code'} for row in parsed]==json.loads(golden.read_text(encoding='utf-8'))
    comparison = dict(input_id=input_id, scope=report["parameters"], row_count=len(parsed), mode="offline_replay",
        all_business_fields_equal=True, request_comparison_equal=True, production_writes=0, network_requests=0,
        original_parsed_path=str(path.resolve()), original_parsed_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        report_path=report["report_path"], report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        original_manifest=str(store.root / "manifest.ndjson"), source_response_hashes=[r.get("body_sha256") for r in events])
    (tmp_path / "comparison.json").write_bytes((json.dumps(comparison, ensure_ascii=False, indent=2)+"\n").encode())
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count", REMAINING_CASES)
def test_remaining_original_requests_business_fields_and_yaml(tmp_path, no_network, input_id, context, manifest, count):
    compare_remaining_original(tmp_path, input_id, context, manifest, count)


REMAINING_FAILURES = [("ASTOCK-055", m) for m in ("empty", "date", "numeric", "duplicate", "http403")] + [
    ("ASTOCK-030", "count"), ("ASTOCK-030", "date"), ("ASTOCK-039", "numeric"), ("ASTOCK-039", "date")]


def remaining_invalid_fixture(tmp_path, input_id, mutation):
    """Derived response fixtures retain their parent response hash and mutation description."""
    sdk = input_id != "ASTOCK-030"
    summary = ROOT / ("provider_validation/results/remaining-sdk-smoke-20261004/summary.json" if sdk
        else "provider_validation/results/remaining-original-smoke-20261004/summary.json")
    original = next(r for r in json.loads(summary.read_text(encoding="utf-8")) if r["input_id"] == input_id)
    manifest = SDK_ARCHIVE if sdk else TENCENT_ARCHIVE
    records = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
    store = RawObjectStore(tmp_path / "fixture")
    for i, response in enumerate(original["responses"]):
        parent = records[response["source_ref"]["line"]-1]
        body = RawObjectStore.read_response(manifest, parent)
        payload = json.loads(body)
        if i == 0 and mutation != "http403":
            if input_id == "ASTOCK-055":
                rows = payload["result"]
                if mutation == "empty": payload["result"] = []
                elif mutation == "date": rows[0]["TRADE_DATE"] = "1999-01-01"
                elif mutation == "numeric": rows[0]["DELTA_VALUE"] = "not-a-number"
                elif mutation == "duplicate": rows[1] = dict(rows[0])
            elif input_id == "ASTOCK-030":
                if mutation == "count": payload["pageHelp"]["total"] = len(payload["result"])+1
                else: payload["result"][0]["STAT_DATE"] = "1999-01-01"
            else:
                data = payload["result"]["data"]
                if mutation == "date": data["report_date"][0]["date_value"] = "20261350"
                else:
                    key = str(data["report_date"][0]["date_value"])
                    data["report_list"][key]["data"][0]["item_value"] = "not-a-number"
            body = json.dumps(payload, ensure_ascii=False).encode()
        ref = store.write_bytes(body, dataset="fixture", provider="source", endpoint=input_id,
            fetched_at=datetime.now(timezone.utc), attempt_id=str(i), content_addressed=True)
        store.append_event({**parent, "body_storage": ref.path.relative_to(store.root).as_posix(),
            "body_sha256": ref.content_hash, "body_bytes": len(body), "mode": "offline_mutation_fixture",
            "status_code": 403 if mutation=="http403" and i==0 else parent["status_code"],
            "derived_from_sha256": parent["body_sha256"], "mutation": mutation,
            "transformation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    return store.root / "manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation", REMAINING_FAILURES)
def test_remaining_invalid_response_retains_evidence_without_fallback(tmp_path, no_network, input_id, mutation):
    manifest = remaining_invalid_fixture(tmp_path, input_id, mutation)
    context = next(c[1] for c in REMAINING_CASES if c[0]==input_id)
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "candidate", replay_manifest=manifest)
    assert report["status"] == "failed" and report["responses"]
    assert "output" not in report and report["production_writes"] == report["live_http_calls"] == 0
    for response in report["responses"]:
        assert hashlib.sha256(RawObjectStore.read_response(Path(report["run_directory"])/"_raw/manifest.ndjson", response)).hexdigest()==response["body_sha256"]
    assert not any("eastmoney.com" in r["url"] for r in report["responses"])


def test_remaining_migration_preserves_existing_provider_methods():
    import ast
    before = ROOT / "provider_validation/results/remaining-original-20261004"
    pairs = [("3-financial.py.bin", "eastmoney/financial.py"), ("4-news.py.bin", "sina/news.py"),
             ("8-snapshot.py.bin", "tencent/snapshot.py"), ("9-minute.py.bin", "tdx/minute.py")]
    for snapshot, relative in pairs:
        old = ast.parse((before/snapshot).read_text(encoding="utf-8"))
        new = ast.parse((ROOT/"src/providers"/relative).read_text(encoding="utf-8"))
        classes = {n.name: n for n in new.body if isinstance(n, ast.ClassDef)}
        for cls in (n for n in old.body if isinstance(n, ast.ClassDef)):
            methods = {n.name: n for n in classes[cls.name].body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
            for method in (n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))):
                assert ast.dump(method, include_attributes=False) == ast.dump(methods[method.name], include_attributes=False)


def test_remaining_unsupported_exchange_and_calendar_rejected_before_requests(tmp_path, no_network):
    for i, (id, exchange) in enumerate((("ASTOCK-030", "SZ"), ("ASTOCK-071", "DCE"), ("ASTOCK-072", "GFEX"), ("ASTOCK-073", "SHFE"))):
        report = collect_input(input_id=id, context=remaining_day("2026-09-18", exchange=exchange), config_root=ROOT/"config", output_root=tmp_path/str(i), replay_manifest=TENCENT_ARCHIVE)
        assert report["status"] == "failed" and not report.get("responses", []) and "output" not in report
    with pytest.raises(ValueError, match="known trading day"):
        collect_input(input_id="ASTOCK-071", context={"request": {"trade_date": "2026-09-18", "exchange": "SHFE"}, "calendar": {"trading_dates": []}}, config_root=ROOT/"config", output_root=tmp_path/"calendar", replay_manifest=TENCENT_ARCHIVE)


@pytest.mark.parametrize('id,mutation',[('ASTOCK-031','empty'),('ASTOCK-031','business'),('ASTOCK-031','duplicate'),('ASTOCK-069','index'),('ASTOCK-069','inconsistent')])
def test_remaining_native_invalid_payload_rejects_candidate(tmp_path,no_network,id,mutation):
    import io
    import pandas as pd
    archive=NEWS_NATIVE_ARCHIVE if id=='ASTOCK-031' else VALUE_NATIVE_ARCHIVE
    store=RawObjectStore(tmp_path/'fixture')
    for i,line in enumerate(archive.read_text(encoding='utf-8').splitlines()):
        parent=json.loads(line);body=RawObjectStore.read_response(archive,parent)
        if id=='ASTOCK-031':
            text=body.decode();payload=json.loads(text[text.index('(')+1:text.rindex(')')])
            if mutation=='empty':payload['result']['cmsArticleWebOld']=[]
            elif mutation=='business':payload['code']=1
            else:payload['result']['cmsArticleWebOld'][1]=dict(payload['result']['cmsArticleWebOld'][0])
            body=(text[:text.index('(')+1]+json.dumps(payload,ensure_ascii=False)+')').encode()
        elif mutation=='index' or (mutation=='inconsistent' and i==1):
            frame=pd.read_excel(io.BytesIO(body));frame.iloc[0,1]=905
            buffer=io.BytesIO();frame.to_excel(buffer,index=False);body=buffer.getvalue()
        ref=store.write_bytes(body,dataset='fixture',provider='native',endpoint=id,fetched_at=datetime.now(timezone.utc),attempt_id=str(i),content_addressed=True)
        store.append_event({**parent,'body_storage':ref.path.relative_to(store.root).as_posix(),'body_sha256':ref.content_hash,'body_bytes':len(body),
            'derived_from_sha256':parent['body_sha256'],'mutation':mutation,'transformation_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    context=next(c[1] for c in REMAINING_CASES if c[0]==id)
    report=collect_input(input_id=id,context=context,config_root=ROOT/'config',output_root=tmp_path/'candidate',replay_manifest=store.root/'manifest.ndjson')
    assert report['status']=='failed' and report['responses'] and 'output' not in report
    assert report['production_writes']==report['live_http_calls']==0


def test_remaining_native_transport_delegation_and_restoration(tmp_path,monkeypatch):
    import io
    from types import SimpleNamespace
    import curl_cffi.requests as curl_requests
    import pandas.io.common as common
    body=b'{"ok":true}'
    calls=[]
    def curl_original(session,method,url,*args,**kwargs):
        calls.append((session,method,url,kwargs))
        return SimpleNamespace(content=body,status_code=200,headers={'Content-Type':'application/json'},encoding='utf-8')
    monkeypatch.setattr(curl_requests.Session,'request',curl_original)
    session=curl_requests.Session()
    arguments=dict(params={'keyword':'600519'},headers={'Cookie':'test-only-private-value','Referer':'https://example.invalid/news'},timeout=17)
    with captured_requests(RawObjectStore(tmp_path/'curl'),provider='fixture',endpoint='news',scope={},code_version='test',pacer=RequestPacer(),native_transport='curl_cffi') as events:
        response=session.request('GET','https://example.invalid/api',**arguments)
        assert response.content==body and len(events)==1
        assert events[0]['request_headers']['Cookie']=='<redacted>'
        assert RawObjectStore.read_response(tmp_path/'curl/manifest.ndjson',events[0])==body
    assert calls==[(session,'GET','https://example.invalid/api',arguments)] and curl_requests.Session.request is curl_original
    session.close()
    def urllib_original(request):
        calls.append(request)
        stream=io.BytesIO(body);stream.status=200;stream.headers={'Content-Type':'application/octet-stream'}
        return stream
    monkeypatch.setattr(common,'urlopen',urllib_original)
    from urllib.request import Request
    request=Request('https://example.invalid/book.xls',headers={'User-Agent':'pandas-fixture'})
    with pytest.raises(RuntimeError,match='after-save'):
        with captured_requests(RawObjectStore(tmp_path/'urllib'),provider='fixture',endpoint='valuation',scope={},code_version='test',pacer=RequestPacer(),native_transport='pandas_urllib') as events:
            assert common.urlopen(request).read()==body and len(events)==1
            assert RawObjectStore.read_response(tmp_path/'urllib/manifest.ndjson',events[0])==body
            raise RuntimeError('after-save')
    assert calls[-1] is request and common.urlopen is urllib_original


def test_remaining_news_yaml_projection_mapping_and_scope(tmp_path,no_network):
    config=tmp_path/'config'
    for name in ['providers.yaml','collection.yaml','datasets/stock_news.yaml','normalization/stock_news.yaml']:
        target=config/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/'config'/name,target)
    path=config/'normalization/stock_news.yaml';document=yaml.safe_load(path.read_text(encoding='utf-8'))
    document['rules'][0]['field_mapping']['title']='文章来源'
    path.write_bytes(yaml.safe_dump(document,allow_unicode=True,sort_keys=False).encode())
    fields=['snapshot_at','source_article_code','title']
    report=collect_input(input_id='ASTOCK-031',context={'request':{'symbol':'600519'}},config_root=config,output_root=tmp_path/'candidate',replay_manifest=NEWS_NATIVE_ARCHIVE,fields=fields)
    assert report['status']=='candidate_complete'
    rows=read_artifact(report,'output');source=read_artifact(report,'parsed_rows')
    assert all(set(row)==set(fields) and row['title']==old['文章来源'] for row,old in zip(rows,source))
    with pytest.raises(ValueError,match='does not support'):
        collect_input(input_id='ASTOCK-031',context={'request':{'symbol':'600519','start_date':'2026-01-01'}},config_root=config,output_root=tmp_path/'bad',replay_manifest=NEWS_NATIVE_ARCHIVE)
SDK_NEWS_CASES = [("ASTOCK-032", {}, SDK_ARCHIVE, 20), ("ASTOCK-033", {}, SDK_ARCHIVE, 20)]

ACTUAL_DATA_CASES = [
    ('ASTOCK-014', {}, SDK_ARCHIVE, 375),
    ('ASTOCK-037-profile', {'request': {'symbol': '600519'}}, SDK_ARCHIVE, 1),
    ('ASTOCK-037-events', {'request': {'date': '2023-08-08'}}, SDK_ARCHIVE, 75),
    ('ASTOCK-044', {}, ROOT/'provider_validation/results/actual-data-original-20261004/st/manifest.ndjson', 198),
    ('ASTOCK-087', {'request': {'date': '2026-10-01'}}, ROOT/'provider_validation/results/actual-data-network-20261004/ASTOCK-087/_raw/manifest.ndjson', 718),
]


def clear_concept_sdk_cache():
    import akshare as sdk
    helper = sdk.stock_board_concept_name_ths.__globals__['_get_stock_board_concept_name_ths']
    helper.cache_clear()
    helper.__wrapped__.__globals__['__stock_board_concept_summary_ths'].cache_clear()


def compare_actual_data_original(tmp_path, input_id, context, manifest, count):
    import akshare as sdk
    from stock_data_manage.pipeline.inputs import _json_value
    from stock_data_manage.providers.baostock.session import captured_sdk_queries
    from unittest.mock import patch
    store = RawObjectStore(tmp_path/'original')
    function = {'ASTOCK-014': 'stock_board_concept_name_ths', 'ASTOCK-037-profile': 'stock_profile_cninfo',
                'ASTOCK-037-events': 'stock_gsrl_gsdt_em', 'ASTOCK-087': 'stock_notice_report'}
    if input_id == 'ASTOCK-044':
        import sys, baostock
        tests = ROOT/'provider_validation/tests/source_snapshots/a-stock-data/tests'
        sys.path.insert(0, str(tests))
        spec = importlib.util.spec_from_file_location('st_original_v39', tests/'test_v39_sources.py')
        module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        ns = module.load_shipped_code()
        try:
            with captured_sdk_queries(store, mode='replay', replay_manifest=manifest, evidence_roots=(), scope={},
                    code_version='original-v39-comparison', trade_date=None, pacer=RequestPacer(wait=lambda _:None),
                    interval_seconds=3, max_age_seconds=0, query_requests={'query_stock_basic':{'code_name':'ST'}}) as (client, _, events):
                with patch.object(baostock,'query_stock_basic',client.query_stock_basic), patch.object(baostock,'login',client.login), patch.object(baostock,'logout',client.logout):
                    frame = ns['_st_list_baostock']('independent original branch replay')
        finally:
            ns['EM_SESSION'].close()
    else:
        if input_id=='ASTOCK-014':clear_concept_sdk_cache()
        with captured_requests(store, provider='original-sdk', endpoint=function[input_id], scope=context,
                code_version='original-sdk-comparison', pacer=RequestPacer(wait=lambda _:None),
                replay_manifest=manifest, sdk_retry_policy=True, probe_host_pause=True) as events:
            kwargs = {} if input_id=='ASTOCK-014' else {'symbol':'600519'} if input_id=='ASTOCK-037-profile' else {'date':'20230808'} if input_id=='ASTOCK-037-events' else {'symbol':'全部','date':'20261001'}
            frame = getattr(sdk,function[input_id])(**kwargs)
            if input_id=='ASTOCK-087':
                items = [item for e in events[1:] for item in json.loads(RawObjectStore.read_response(store.root/'manifest.ndjson', e))['data']['list']]
                frame = frame.assign(source_article_code=[item['art_code'] for item in items])
    original = _json_value(frame.astype(object).where(frame.notna(),None).to_dict('records'))
    original = [{k:v for k,v in r.items() if k not in {'source','source_url','fetched_at'}} for r in original]
    assert len(original)==count
    path = tmp_path/'original-parsed.json';path.write_bytes(json.dumps(original,ensure_ascii=False,indent=2).encode())
    if input_id=='ASTOCK-014':clear_concept_sdk_cache()
    report = collect_input(input_id=input_id, context=context, config_root=ROOT/'config', output_root=tmp_path/'candidate',
        replay_manifest=manifest, pacer=RequestPacer(wait=lambda _:None))
    assert report['status']=='candidate_complete', {k:report.get(k) for k in ['status','error','failure_class']}
    assert read_artifact(report,'parsed_rows') == original
    assert report['row_count']==count and report['production_writes']==report['live_http_calls']==0
    assert not report['eligible_for_production_routing'] and not report['source_fallback_enabled']
    keys = ('endpoint','request_parameters','outcome','body_sha256') if input_id=='ASTOCK-044' else ('url','method','status_code','body_sha256','request_headers','request_options')
    assert [{k:e.get(k) for k in keys} for e in events] == [{k:e.get(k) for k in keys} for e in report['responses']]
    for row in read_artifact(report,'output'):
        assert row['snapshot_at']==report['source_capture_window']['last']
        assert all(row[f] is None for f in report['unverified_fields'])
    if input_id=='ASTOCK-014':
        assert any(e['status_code']==401 for e in report['responses'])
        assert report['source_metadata']['pagination_completeness_verified'] is False
        clear_concept_sdk_cache()
    if input_id=='ASTOCK-037-events':
        assert report['returned_window']['first']==report['returned_window']['last']=='2023-08-08'
        assert len({r['代码'] for r in original}) > 1
    if input_id=='ASTOCK-044':
        assert report['original_row_count']==317 and report['selected_row_count']==198
        assert not report['sdk_dependency']['live_sdk_executed']
    summary = dict(input_id=input_id, scope=report['parameters'], row_count=count, original_parsed_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        response_hashes=[e['body_sha256'] for e in report['responses']], original_fields_equal=True, original_transport_equal=True,
        production_writes=0, mode='offline_replay', universe_completeness_verified=False, report_path=report['report_path'])
    (tmp_path/'comparison.json').write_bytes(json.dumps(summary,ensure_ascii=False,indent=2).encode())
    return report


@pytest.mark.parametrize('input_id,context,manifest,count',ACTUAL_DATA_CASES)
def test_actual_data_original_scope_transport_and_fields(tmp_path,no_network,input_id,context,manifest,count):
    compare_actual_data_original(tmp_path,input_id,context,manifest,count)


def test_actual_data_yaml_projection_and_mapping(tmp_path,no_network):
    config=tmp_path/'config'
    for name in ['providers.yaml','collection.yaml','datasets/company_profile.yaml','normalization/company_profile.yaml']:
        target=config/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/'config'/name,target)
    path=config/'normalization/company_profile.yaml';doc=yaml.safe_load(path.read_text(encoding='utf-8'))
    doc['rules'][0]['field_mapping']['company_name']='英文名称';path.write_bytes(yaml.safe_dump(doc,allow_unicode=True,sort_keys=False).encode())
    fields=['snapshot_at','stock_code','stock_name','company_name']
    report=collect_input(input_id='ASTOCK-037-profile',context={'request':{'symbol':'600519'}},config_root=config,
        output_root=tmp_path/'candidate',replay_manifest=SDK_ARCHIVE,fields=fields,pacer=RequestPacer(wait=lambda _:None))
    assert report['status']=='candidate_complete',report.get('error')
    row=read_artifact(report,'output')[0]
    assert set(row)==set(fields) and row['company_name']=='Kweichow Moutai Co., Ltd.'


@pytest.mark.parametrize('input_id',['ASTOCK-014','ASTOCK-044','ASTOCK-037-events','ASTOCK-087'])
def test_actual_data_full_scope_rejects_stock_filter(tmp_path,no_network,input_id):
    with pytest.raises(ValueError,match='does not support'):
        collect_input(input_id=input_id,context={'request':{'symbol':'600519','date':'2026-10-01'}},config_root=ROOT/'config',
            output_root=tmp_path/'candidate',replay_manifest=SDK_ARCHIVE)


def test_actual_data_generated_auth_header_is_redacted():
    from stock_data_manage.storage.raw import sanitized_headers
    assert sanitized_headers({'Accept-Enckey':'test-only-auth','Cookie':'test-only-cookie'}) == {'Accept-Enckey':'<redacted>','Cookie':'<redacted>'}


ACTUAL_DATA_FAILURES = [('ASTOCK-037-profile', kind) for kind in ['status','order','code','count']] + [
    ('ASTOCK-037-events', kind) for kind in ['status','date','pages','count']] + [
    ('ASTOCK-087', kind) for kind in ['status','date','count','missing_page','missing_issuer']] + [
    ('ASTOCK-044', kind) for kind in ['empty','missing_field','sdk_status']] + [('ASTOCK-014','missing_directory')]


def actual_data_invalid_fixture(tmp_path,input_id,mutation):
    manifest=next(c[2] for c in ACTUAL_DATA_CASES if c[0]==input_id)
    parents=[json.loads(l) for l in manifest.read_text(encoding='utf-8').splitlines()]
    needle={'ASTOCK-014':'q.10jqka.com.cn/gn/','ASTOCK-037-profile':'p_sysapi1133',
            'ASTOCK-037-events':'RPT_ORGOP_ALL','ASTOCK-087':'np-anotice-stock','ASTOCK-044':'query_stock_basic'}[input_id]
    parents=[e for e in parents if needle in e.get('url',e.get('endpoint',''))]
    store=RawObjectStore(tmp_path/'fixture')
    for i,e in enumerate(parents):
        if mutation=='missing_page' and i==len(parents)-1:continue
        body=RawObjectStore.read_response(manifest,e)
        if i==0 or input_id=='ASTOCK-087':
            if input_id=='ASTOCK-014':
                body=body.replace(b'cate_inner',b'missing_category') if i==0 else body
            else:
                p=json.loads(body)
                if input_id=='ASTOCK-037-profile':
                    if mutation=='status':p['resultcode']=500
                    elif mutation=='count':p['count']=2
                    elif mutation=='code':p['records'][0]['ASECCODE']='000001'
                    else:p['records'][0]=dict(reversed(list(p['records'][0].items())))
                elif input_id=='ASTOCK-037-events':
                    if mutation=='status':p['success']=False
                    elif mutation=='pages':p['result']['pages']=2
                    elif mutation=='count':p['result']['count']+=1
                    else:p['result']['data'][0]['TRADE_DATE']='1999-01-01 00:00:00'
                elif input_id=='ASTOCK-087':
                    if mutation=='status':p['success']=0
                    elif mutation=='count':p['data']['total_hits']+=1
                    elif mutation=='date':p['data']['list'][0]['notice_date']='1999-01-01'
                    elif mutation=='missing_issuer':p['data']['list'][0]['codes']=[]
                else:
                    if mutation=='empty':p['rows']=[]
                    elif mutation=='sdk_status':p['result']={'error_code':'10001001','error_msg':'fixture SDK failure'}
                    else:
                        p['fields'].remove('code_name')
                        for r in p['rows']:r.pop('code_name')
                body=json.dumps(p,ensure_ascii=False,separators=(',',':')).encode()
        ref=store.write_bytes(body,dataset='fixture',provider='source',endpoint=input_id,
            fetched_at=datetime.now(timezone.utc),attempt_id=str(i),content_addressed=True)
        store.append_event({**e,'body_storage':ref.path.relative_to(store.root).as_posix(),'body_sha256':ref.content_hash,
            'body_bytes':len(body),'derived_from_sha256':e['body_sha256'],'mutation':mutation,'mode':'offline_mutation_fixture',
            'transformation_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    return store.root/'manifest.ndjson'


@pytest.mark.parametrize('input_id,mutation',ACTUAL_DATA_FAILURES)
def test_actual_data_invalid_response_does_not_produce_output(tmp_path,no_network,input_id,mutation):
    if input_id=='ASTOCK-014':clear_concept_sdk_cache()
    manifest=actual_data_invalid_fixture(tmp_path,input_id,mutation)
    context=next(c[1] for c in ACTUAL_DATA_CASES if c[0]==input_id)
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/'config',output_root=tmp_path/'candidate',
        replay_manifest=manifest,pacer=RequestPacer(wait=lambda _:None))
    assert report['status']=='failed' and 'output' not in report and report['responses'],report
    assert report['production_writes']==report['live_http_calls']==0 and not report['eligible_for_production_routing']
    for e in report['responses']:
        assert hashlib.sha256(RawObjectStore.read_response(Path(report['run_directory'])/'_raw/manifest.ndjson',e)).hexdigest()==e['body_sha256']
    if input_id=='ASTOCK-014':clear_concept_sdk_cache()
MACRO_CASES = [("ASTOCK-061", {}, SDK_ARCHIVE, 136), ("ASTOCK-062", {}, SDK_ARCHIVE, 225)]

REPORTS_SEATS_CASES = [
    ("ASTOCK-008", {"request":{"symbol":"600519", "start_date":"2026-09-01", "end_date":"2026-10-03"}}, SDK_ARCHIVE, 1),
    ("ASTOCK-019", {"request":{"symbol":"600519", "trade_date":"2013-01-28"}, "calendar":{"trading_dates":[date(2013,1,28)]}}, SDK_ARCHIVE, 10)]
REPORTS_SEATS_FAILURES = [(id, mutation) for id in ("ASTOCK-008", "ASTOCK-019")
    for mutation in ("empty", "schema", "numeric", "date", "code", "duplicate", "count", "pages", "http403")] + [
    ("ASTOCK-008","page"), ("ASTOCK-008","missing"), ("ASTOCK-019","order"), ("ASTOCK-019","business"), ("ASTOCK-019","missing_sell")]


def reports_seats_records(input_id):
    records = [json.loads(line) for line in SDK_ARCHIVE.read_text(encoding="utf-8").splitlines()]
    return [r for r in records if ("reportapi.eastmoney.com/report/list" in r.get("url", "") if input_id=="ASTOCK-008"
        else "RPT_BILLBOARD_DAILYDETAILS" in r.get("url", ""))][:1 if input_id=="ASTOCK-008" else 2]


def compare_reports_seats_original(tmp_path, input_id, context, manifest, count):
    import ast, csv, inspect, re
    from types import SimpleNamespace
    from typing import Any, Callable, List, Tuple, Dict
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import akshare as sdk
    import pandas as pd
    from stock_data_manage.pipeline.inputs import _json_value
    from stock_data_manage.providers.eastmoney.financial import reportapi_replay_clock
    from contextlib import ExitStack
    source = ROOT/"provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py"
    names={"only_digits","date_dash","build_session","fetch_eastmoney_reportapi"} if input_id=="ASTOCK-008" else {"ak_function","call_ak","try_ak_variants","only_digits","fetch_lhb_seats"}
    nodes=[n for n in ast.parse(source.read_text(encoding="utf-8")).body if isinstance(n,ast.FunctionDef) and n.name in names]
    ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
    namespace=dict(re=re,inspect=inspect,requests=requests,json=json,pd=pd,datetime=datetime,Any=Any,Callable=Callable,List=List,Tuple=Tuple,Dict=Dict,
        Config=SimpleNamespace,ensure_ak=lambda:sdk,UA=ua,Retry=Retry,HTTPAdapter=HTTPAdapter)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source),"exec"),namespace)
    cfg=SimpleNamespace(code="600519",trade_date="20260930",start="20260901",end="20261003",report_pages=3,sleep=0.25)
    with ExitStack() as stack:
        events=stack.enter_context(captured_requests(RawObjectStore(tmp_path/"original"),provider="eastmoney",endpoint="original-reports-seats",scope={},
            code_version="original-source",pacer=RequestPacer(),replay_manifest=manifest,sdk_retry_policy=True,probe_host_pause=True,
            cache_ignored_query_parameters=("_",) if input_id=="ASTOCK-008" else ()))
        if input_id=="ASTOCK-008":
            # The successful rate-limited probe explicitly remounted the original module Session after loading it.
            probe_path=ROOT/"provider_validation/tests/run_a_stock_rate_limited_probes.py"
            spec=importlib.util.spec_from_file_location("original_report_probe_policy",probe_path)
            probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
            session=namespace["build_session"]()
            for scheme in ("http://","https://"):session.mount(scheme,HTTPAdapter(max_retries=probe.retry_policy()))
            namespace["SESSION"]=session;stack.callback(session.close)
            stack.enter_context(reportapi_replay_clock(namespace["fetch_eastmoney_reportapi"],manifest,code="600519",start=date(2026,9,1),end=date(2026,10,3),clock_name="time"))
            frames={"reports":namespace["fetch_eastmoney_reportapi"](cfg)};chosen_date=None
        else:
            result=namespace["fetch_lhb_seats"](cfg);frames={k:result[k] for k in ("dates","buy","sell")};chosen_date=result["chosen_date"]
            assert chosen_date=="20130128"
    parsed={k:_json_value(frame.astype(object).where(pd.notna(frame),None).to_dict(orient="records")) for k,frame in frames.items()}
    path=tmp_path/"original-parsed.json";path.write_text(json.dumps(dict(frames=parsed,original_requested_date=cfg.trade_date,chosen_date=chosen_date),ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    csv_refs=[]
    for kind, rows in parsed.items():
        csv_path=SDK_ARCHIVE.parents[2]/("08_东财reportapi/data.csv" if input_id=="ASTOCK-008" else "17_龙虎榜席位/data/"+kind+".csv")
        with csv_path.open(encoding="utf-8-sig",newline="") as stream: golden=list(csv.DictReader(stream))
        assert len(rows)==len(golden)
        for row, old in zip(rows,golden):
            assert tuple(row)==tuple(old)
            for key,value in row.items():
                if value is None:assert old[key]==""
                elif isinstance(value,(int,float)):assert float(value)==pytest.approx(float(old[key]),rel=1e-12)
                else:assert str(value)==old[key]
        csv_refs.append(dict(path=str(csv_path),sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest()))
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==count,report
    candidate=read_artifact(report,"parsed_rows");source_rows=read_artifact(report,"source_rows")
    if input_id=="ASTOCK-008":
        assert candidate==source_rows==parsed["reports"]
        assert source_rows[0]["predictNextTwoYearEps"]=="71.3200000000"
        expected_events=events
    else:
        expected_events=events[1:]
        for side in ("buy","sell"):
            assert [{k:v for k,v in r.items() if k not in {"side","source_security_code","trade_date"}} for r in candidate if r["side"]==side]==parsed[side]
        expected_raw=[{"side":side,**row} for side,record in zip(("buy","sell"),reports_seats_records(input_id))
            for row in json.loads(RawObjectStore.read_response(manifest,record))["result"]["data"]]
        assert source_rows==expected_raw and report["date_discovery_requests"]==0
        assert report["returned_window"]=={"first":"2013-01-28","last":"2013-01-28"}
        assert len(events)==3 and len(report["responses"])==2
        assert report["side_counts"]=={"buy":5,"sell":5}
    keys=("url","method","status_code","body_sha256","request_headers","request_options")
    assert [{k:r.get(k) for k in keys} for r in expected_events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    assert not report["source_fallback_enabled"] and not report["implicit_date_selection"]
    assert report["production_writes"]==report["live_http_calls"]==0 and not report["eligible_for_production_routing"]
    for row in read_artifact(report,"output"):
        assert all(row[key] is None for key in report["unverified_fields"])
        assert row["snapshot_at"]==report["source_capture_window"]["last"]
    comparison=dict(input_id=input_id,all_business_fields_equal=True,all_retained_source_fields_equal=True,request_comparison_equal=True,
        original_request_count=len(events),provider_request_count=len(report["responses"]),original_requested_date=cfg.trade_date if chosen_date else None,
        actual_seat_day=chosen_date,original_csv=csv_refs,original_parsed_path=str(path.resolve()),original_parsed_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        report_path=report["report_path"],report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        known_difference="explicit selected historical day; original date discovery retained only as evidence, no date/source fallback" if chosen_date else "no PDF download; numeric source strings retained without unit certification")
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count",REPORTS_SEATS_CASES)
def test_reports_seats_original_csv_requests_and_actual_day(tmp_path,no_network,input_id,context,manifest,count):
    compare_reports_seats_original(tmp_path,input_id,context,manifest,count)


def reports_seats_fixture(tmp_path,input_id,mutation):
    from copy import deepcopy
    records=reports_seats_records(input_id)
    payloads=[json.loads(RawObjectStore.read_response(SDK_ARCHIVE,r)) for r in records]
    payload=payloads[0];result=payload if input_id=="ASTOCK-008" else payload["result"];rows=result["data"]
    code="stockCode" if input_id=="ASTOCK-008" else "SECURITY_CODE"
    day="publishDate" if input_id=="ASTOCK-008" else "TRADE_DATE"
    numeric="predictThisYearEps" if input_id=="ASTOCK-008" else "BUY"
    if mutation=="empty":rows.clear()
    elif mutation=="schema":rows[0].pop("infoCode" if input_id=="ASTOCK-008" else numeric)
    elif mutation=="numeric":rows[0][numeric]="bad"
    elif mutation=="date":rows[0][day]="2020-01-01 00:00:00.000"
    elif mutation=="code":rows[0][code]="000001"
    elif mutation=="duplicate":
        if input_id=="ASTOCK-008":rows.append(deepcopy(rows[0]));result["hits"]=result["size"]=2
        else:rows[1]=deepcopy(rows[0])
    elif mutation=="count":result["hits" if input_id=="ASTOCK-008" else "count"]+=1
    elif mutation=="pages":result["TotalPage" if input_id=="ASTOCK-008" else "pages"]+=1
    elif mutation=="order":rows[0]={k:rows[0][k] for k in reversed(rows[0])}
    elif mutation=="business":payload["success"]=False
    elif mutation=="page":payload["pageNo"]=2
    elif mutation=="missing_sell":payloads=payloads[:1];records=records[:1]
    elif mutation=="zero_negative":
        if input_id=="ASTOCK-008":rows[0].update(predictThisYearEps="-1.0000000000",predictNextYearEps="0.0000000000",predictNextTwoYearEps=None)
        else:rows[0][numeric]=-1;rows[1][numeric]=0;rows[2][numeric]=None
    store=RawObjectStore(tmp_path/"fixture")
    if mutation=="missing":store.root.mkdir(parents=True,exist_ok=True);(store.root/"manifest.ndjson").write_text("",encoding="utf-8")
    else:
        for record,item in zip(records,payloads):
            response=requests.Response();response.status_code=403 if mutation=="http403" else 200;response.encoding="utf-8"
            text=json.dumps(item,ensure_ascii=False);response._content=("datatable("+text+")" if mutation=="jsonp" else text).encode()
            store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],provider="fixture",endpoint="reports-seats",scope={"fixture":"synthetic mutation"},code_version="fixture",mode="fixture")
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation",REPORTS_SEATS_FAILURES)
def test_reports_seats_invalid_source_stops_without_source_or_date_fallback(tmp_path,no_network,input_id,mutation):
    context=next(c[1] for c in REPORTS_SEATS_CASES if c[0]==input_id)
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=reports_seats_fixture(tmp_path,input_id,mutation))
    assert report["status"]=="failed" and "output" not in report,report
    assert report["production_writes"]==report["live_http_calls"]==0 and not report["eligible_for_production_routing"]
    if mutation!="missing":
        assert report["responses"] and len(report["responses"])<=2
        for event in report["responses"]:assert RawObjectStore.read_response(Path(report["run_directory"])/report["raw_manifest"]["path"],event)


@pytest.mark.parametrize("input_id",["ASTOCK-008","ASTOCK-019"])
def test_reports_seats_zero_negative_null_and_jsonp_preservation(tmp_path,no_network,input_id):
    context=next(c[1] for c in REPORTS_SEATS_CASES if c[0]==input_id)
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=reports_seats_fixture(tmp_path,input_id,"zero_negative"))
    assert report["status"]=="candidate_complete",report
    parsed=read_artifact(report,"parsed_rows")
    if input_id=="ASTOCK-008":
        assert [parsed[0][k] for k in ("predictThisYearEps","predictNextYearEps","predictNextTwoYearEps")]==["-1.0000000000","0.0000000000",None]
        jsonp=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=reports_seats_fixture(tmp_path/"jsonp",input_id,"jsonp"))
        assert jsonp["status"]=="candidate_complete" and read_artifact(jsonp,"source_rows")[0]["infoCode"]=="AP202609211829719676",jsonp
    else:assert [r["买入金额"] for r in parsed[:3]]==[-1,0,None]


def test_reports_seats_yaml_projection_scope_and_old_methods(tmp_path,no_network):
    import ast
    def methods(path):
        cls=next(n for n in ast.parse(path.read_text(encoding="utf-8")).body if isinstance(n,ast.ClassDef) and n.name=="EastMoneyFinancialMainProvider")
        return {n.name:ast.dump(n,include_attributes=False) for n in cls.body if isinstance(n,ast.FunctionDef)}
    before=methods(ROOT/"provider_validation/results/reports-seats-original-20261004/0-financial.py.bin")
    current=methods(ROOT/"src/providers/eastmoney/financial.py")
    assert all(current[name]==body for name,body in before.items())
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    path=config/"normalization/eastmoney_reports.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"));rule["rules"][0]["field_mapping"]["title"]="orgName"
    path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    fields=["snapshot_at","source_security_code","report_id","report_date","source_publish_text","title"]
    report=collect_input(input_id="ASTOCK-008",context=REPORTS_SEATS_CASES[0][1],config_root=config,output_root=tmp_path/"candidate",replay_manifest=SDK_ARCHIVE,fields=fields)
    assert report["status"]=="candidate_complete" and all(set(r)==set(fields) and r["title"]=="诚通证券股份有限公司" for r in read_artifact(report,"output")),report
    for input_id,context,manifest,count in REPORTS_SEATS_CASES:
        with pytest.raises(ValueError,match="does not support"):
            collect_input(input_id=input_id,context={**context,"request":{**context["request"],"symbols":["600519"]}},config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=manifest)
    with pytest.raises(ValueError,match="calendar"):
        collect_input(input_id="ASTOCK-019",context={"request":{"symbol":"600519","trade_date":"2013-01-28"}},config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=SDK_ARCHIVE)
    wrong=collect_input(input_id="ASTOCK-019",context={"request":{"symbol":"600519","trade_date":"2026-09-30"},"calendar":{"trading_dates":[date(2026,9,30)]}},config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=SDK_ARCHIVE)
    assert wrong["status"]=="failed" and wrong["failure_class"]=="ValueError" and not wrong.get("responses")


def test_reports_seats_session_headers_timeout_retry_cache_and_local_clock(tmp_path,no_network):
    from unittest.mock import patch
    from urllib.parse import parse_qs,urlsplit
    import time
    from stock_data_manage.providers.eastmoney import financial
    seen=[];process_clock=time.time;report_clock=financial._report_clock
    def send(session,request,**kwargs):
        input_id="ASTOCK-008" if urlsplit(request.url).hostname=="reportapi.eastmoney.com" else "ASTOCK-019"
        record=next(r for r in reports_seats_records(input_id) if input_id=="ASTOCK-008" or parse_qs(urlsplit(request.url).query)["reportName"]==parse_qs(urlsplit(r["url"]).query)["reportName"])
        seen.append(request.url)
        assert kwargs["timeout"]==(30 if input_id=="ASTOCK-008" else None) and kwargs["allow_redirects"] and session.trust_env
        assert request.headers["User-Agent"]==record["request_headers"]["User-Agent"]
        if input_id=="ASTOCK-008":
            assert request.headers["Referer"]=="https://data.eastmoney.com/" and request.headers["Accept-Language"]==record["request_headers"]["Accept-Language"]
        retry=session.get_adapter(request.url).max_retries
        assert retry.total==retry.connect==retry.read==retry.status==2 and retry.backoff_factor==5
        assert not retry.is_retry("GET",403) and not retry.is_retry("GET",429,True)
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=RawObjectStore.read_response(SDK_ARCHIVE,record);response.url=request.url
        return response
    with patch("requests.Session.send",send):
        for input_id,context,_,_ in REPORTS_SEATS_CASES:
            first=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
            second=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
            assert first["status"]==second["status"]=="candidate_complete",(first,second)
            assert first["live_http_calls"]==(1 if input_id=="ASTOCK-008" else 2) and second["live_http_calls"]==0
            assert read_artifact(first,"output")==read_artifact(second,"output")
    assert len(seen)==3 and time.time is process_clock and financial._report_clock is report_clock
    (tmp_path/"fixture-mode.json").write_text(json.dumps(dict(mode="injected Session; not real live validation",real_http_calls=0,fixture_send_calls=3)),encoding="utf-8")


def test_report_cache_nonce_does_not_relax_scope_window_version_or_age(tmp_path,no_network):
    from datetime import timedelta
    original=reports_seats_records("ASTOCK-008")[0]
    response=requests.Response();response.status_code=200;response._content=RawObjectStore.read_response(SDK_ARCHIVE,original);response.encoding="utf-8"
    store=RawObjectStore(tmp_path/"cache");store.record_response(response=response,url=original["url"],method="GET",request_headers={},provider="eastmoney",endpoint="report_list",scope={"input_id":"ASTOCK-008"},code_version="current",mode="live")
    url=original["url"].replace("1791020798080","1791020800000")
    kwargs=dict(roots=(store.root,),url=url,method="GET",scope={"input_id":"ASTOCK-008"},code_version="current",max_age_seconds=86400)
    assert RawObjectStore.find_cached_response(**kwargs) is None
    assert RawObjectStore.find_cached_response(**kwargs,ignored_query_parameters=("_",)) is not None
    for change in ({"code_version":"different"},{"scope":{"input_id":"other"}},{"method":"POST"},{"max_age_seconds":0},
        {"url":url.replace("600519","000001")},{"url":url.replace("2026-09-01","2026-09-02")},{"url":url.replace("pageNo=1","pageNo=2")},
        {"url":url.replace("reportapi.eastmoney.com","other.example.com")}):
        assert RawObjectStore.find_cached_response(**{**kwargs,**change},ignored_query_parameters=("_",)) is None


def report_pages_fixture(tmp_path, mutation):
    from copy import deepcopy
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
    record=reports_seats_records("ASTOCK-008")[0]
    payload=json.loads(RawObjectStore.read_response(SDK_ARCHIVE,record));base=payload["data"][0]
    count=301 if mutation=="limited" else 101
    store=RawObjectStore(tmp_path/"fixture")
    for page in (1,2):
        item=deepcopy(payload);size=min(100,count-(page-1)*100)
        item.update(hits=count,TotalPage=(count+99)//100,pageNo=page,size=size)
        item["data"]=[{**deepcopy(base),"infoCode":base["infoCode"]+str(i)} for i in range((page-1)*100,(page-1)*100+size)]
        if page==2 and mutation=="duplicate_cross_page":item["data"][0]["infoCode"]=base["infoCode"]+"0"
        if page==2 and mutation=="totals_drift":
            item["hits"]=102;item["size"]=2;item["data"].append({**deepcopy(base),"infoCode":"extra"})
        parts=urlsplit(record["url"]);query=dict(parse_qsl(parts.query,keep_blank_values=True))
        for key in ("p","pageNo","pageNum","pageNumber"):query[key]=str(page)
        query["_"]=str(int(query["_"])+1000*(page-1))
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=json.dumps(item,ensure_ascii=False).encode()
        store.record_response(response=response,url=urlunsplit((parts.scheme,parts.netloc,parts.path,urlencode(query),"")),method="GET",request_headers=record["request_headers"],
            provider="fixture",endpoint="report-list",scope={"fixture":"synthetic report pages"},code_version="fixture",mode="fixture")
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("mutation",["complete","limited","totals_drift","duplicate_cross_page"])
def test_report_pages_complete_bounded_or_inconsistent(tmp_path,no_network,mutation):
    context={**REPORTS_SEATS_CASES[0][1],"config":{"pages":2}}
    report=collect_input(input_id="ASTOCK-008",context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=report_pages_fixture(tmp_path,mutation))
    if mutation in {"complete","limited"}:
        assert report["status"]=="candidate_complete" and report["row_count"]==(101 if mutation=="complete" else 200),report
        assert report["result_limited"] is (mutation=="limited") and report["coverage_complete"] is (mutation=="complete")
        assert report["source_total_count"]==(101 if mutation=="complete" else 301) and report["retrieved_pages"]==[1,2]
    else:assert report["status"]=="failed" and "output" not in report,report
    assert report["live_http_calls"]==report["production_writes"]==0 and len(report["responses"])==2

MARKET_EVENT_CASES = [
    ("ASTOCK-020", {"request": {"trade_date": "2026-09-30"}, "calendar": {"trading_dates": [date(2026, 9, 30)]}}, SDK_ARCHIVE, 84),
    ("ASTOCK-021", {"request": {"start_date": "2026-09-01", "end_date": "2026-10-03"}}, SDK_ARCHIVE, 167)]
MARKET_EVENT_FAILURES = [(id, mutation) for id in ("ASTOCK-020", "ASTOCK-021")
    for mutation in ("business", "empty", "schema", "order", "numeric", "date", "code", "identity", "duplicate", "count", "pages", "http403", "missing_page", "changed_page")]


def market_event_records(input_id):
    from urllib.parse import parse_qs, urlsplit
    report = "RPT_DAILYBILLBOARD_DETAILSNEW" if input_id == "ASTOCK-020" else "RPT_LIFT_STAGE"
    return [record for line in SDK_ARCHIVE.read_text(encoding="utf-8").splitlines()
        if (record := json.loads(line)).get("event") == "http_response"
        and (query := parse_qs(urlsplit(record.get("url", "")).query)).get("reportName") == [report]
        and "SECURITY_CODE=" not in query.get("filter", [""])[0]]


def compare_market_event_original(tmp_path, input_id, context, manifest, count):
    import ast, csv, inspect, re
    import akshare as sdk
    import pandas as pd
    from types import SimpleNamespace
    from typing import Any, Callable, List, Tuple, Dict
    from stock_data_manage.pipeline.inputs import _json_value
    source = ROOT / "provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py"
    method = "fetch_lhb_all" if input_id == "ASTOCK-020" else "fetch_restricted_release"
    nodes = [n for n in ast.parse(source.read_text(encoding="utf-8")).body if isinstance(n, ast.FunctionDef)
        and n.name in {"ak_function", "call_ak", "try_ak_variants", "only_digits", method}]
    namespace = dict(inspect=inspect, re=re, Any=Any, Callable=Callable, List=List, Tuple=Tuple, Dict=Dict,
        Config=SimpleNamespace, pd=pd, ensure_ak=lambda:sdk)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    with captured_requests(RawObjectStore(tmp_path / "original"), provider="eastmoney", endpoint="original-market-event",
        scope={}, code_version="original-source-script", pacer=RequestPacer(), replay_manifest=manifest,
        sdk_retry_policy=True, probe_host_pause=True) as events:
        frame = namespace[method](SimpleNamespace(code="600519", trade_date="20260930", start="20260901", end="20261003"))
    frame = frame.astype(object).where(pd.notna(frame), None)
    parsed = _json_value(frame.to_dict(orient="records"))
    path = tmp_path / "original-parsed.json"
    path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    csv_path = SDK_ARCHIVE.parents[2] / ("18_全市场龙虎榜" if input_id == "ASTOCK-020" else "19_限售解禁日历") / "data.csv"
    with csv_path.open(encoding="utf-8-sig", newline="") as stream: golden = list(csv.DictReader(stream))
    assert len(parsed) == len(golden) == count
    for row, old in zip(parsed, golden):
        assert tuple(row) == tuple(old)
        for key, value in row.items():
            if value is None: assert old[key] == ""
            elif isinstance(value, (int, float)): assert float(value) == pytest.approx(float(old[key]), rel=1e-12)
            else: assert str(value) == old[key]
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config",
        output_root=tmp_path / "candidate", replay_manifest=manifest)
    assert report["status"] == "candidate_complete" and report["row_count"] == count, report
    assert read_artifact(report, "parsed_rows") == parsed
    assert read_artifact(report, "source_rows") == json.loads(RawObjectStore.read_response(manifest, market_event_records(input_id)[0]))["result"]["data"]
    selected_events = events if input_id == "ASTOCK-020" else events[1:]
    keys = ("url", "method", "status_code", "body_sha256", "request_headers", "request_options")
    assert [{k:r.get(k) for k in keys} for r in selected_events] == [{k:r.get(k) for k in keys} for r in report["responses"]]
    assert len(selected_events) == len(report["responses"]) == 2
    assert len(events) == (2 if input_id == "ASTOCK-020" else 3)
    outputs = read_artifact(report, "output")
    for output, row in zip(outputs, parsed):
        assert output["source_security_code"] == row["代码" if input_id == "ASTOCK-020" else "股票代码"]
        assert output["trade_date" if input_id == "ASTOCK-020" else "release_date"] == row["上榜日" if input_id == "ASTOCK-020" else "解禁时间"]
        assert all(output[key] is None for key in report["unverified_fields"])
    if input_id == "ASTOCK-020": assert len({r["代码"] for r in parsed}) < count  # Keep distinct reasons for one stock.
    else:
        for raw, row in zip(read_artifact(report, "source_rows"), parsed):
            for key, source_key in (("解禁数量", "ABLE_FREE_SHARES"), ("实际解禁数量", "CURRENT_FREE_SHARES"), ("实际解禁市值", "LIFT_MARKET_CAP")):
                assert row[key] == pytest.approx(raw[source_key] * 10000, rel=1e-12)
    assert report["source_total_count"] == report["coverage_denominator"] == count
    assert not report["universe_completeness_verified"] and not report["eligible_for_production_routing"]
    assert report["live_http_calls"] == report["production_writes"] == 0
    assert not report["source_fallback_enabled"] and not report["implicit_date_selection"]
    comparison = dict(input_id=input_id, original_csv_path=str(csv_path), original_csv_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        original_parsed_path=str(path.resolve()), original_parsed_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        all_business_fields_equal=True, all_retained_source_fields_equal=True, request_comparison_equal=True,
        original_request_count=len(events), selected_successful_request_count=len(selected_events), provider_request_count=2,
        known_difference="failed single-stock queue retained only in original evidence; independent successful market-window detail endpoint" if input_id == "ASTOCK-021" else "original fallback alternatives are not embedded in Provider",
        report_path=report["report_path"], report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest())
    (tmp_path / "comparison.json").write_text(json.dumps(comparison, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count", MARKET_EVENT_CASES)
def test_market_event_original_sdk_csv_and_exact_requests(tmp_path, no_network, input_id, context, manifest, count):
    compare_market_event_original(tmp_path, input_id, context, manifest, count)


def market_event_fixture(tmp_path, input_id, mutation):
    from copy import deepcopy
    records = market_event_records(input_id)
    payload = json.loads(RawObjectStore.read_response(SDK_ARCHIVE, records[0]))
    result = payload["result"]; rows = result["data"]
    numeric = "CLOSE_PRICE" if input_id == "ASTOCK-020" else "CURRENT_FREE_SHARES"
    day = "TRADE_DATE" if input_id == "ASTOCK-020" else "FREE_DATE"
    identity = "EXPLANATION" if input_id == "ASTOCK-020" else "FREE_SHARES_TYPE"
    if mutation == "business": payload["success"] = False
    elif mutation == "empty": rows.clear(); result["count"] = 0
    elif mutation == "schema": rows[0].pop(numeric)
    elif mutation == "order": rows[0] = {k:rows[0][k] for k in reversed(rows[0])}
    elif mutation == "numeric": rows[0][numeric] = "bad"
    elif mutation == "date": rows[0][day] = "2020-01-01 00:00:00"
    elif mutation == "code": rows[0]["SECURITY_CODE"] = "bad"
    elif mutation == "identity": rows[0][identity] = ""
    elif mutation == "duplicate": rows[1] = rows[0].copy()
    elif mutation == "count": result["count"] += 1
    elif mutation == "pages": result["pages"] += 1
    elif mutation == "zero_negative": rows[0][numeric] = -1; rows[1][numeric] = 0; rows[2][numeric] = None
    payloads = [payload, deepcopy(payload)]
    if mutation == "changed_page": payloads[1]["result"]["data"][0][numeric] += 1
    if mutation == "missing_page": payloads = payloads[:1]
    if mutation == "multi_page":
        assert input_id == "ASTOCK-021"
        new_rows = [{**deepcopy(rows[0]), "SECURITY_CODE":f"{i+1:06}"} for i in range(501)]
        result.update(count=501, pages=2, data=new_rows[:500])
        last = deepcopy(payload); last["result"]["data"] = new_rows[500:]
        payloads = [payload, deepcopy(payload), last]
    store = RawObjectStore(tmp_path / "fixture")
    for index, item in enumerate(payloads):
        record = records[min(index, 1)]
        response = requests.Response(); response.status_code = 403 if mutation == "http403" else 200
        response.encoding = "utf-8"; response._content = json.dumps(item, ensure_ascii=False).encode()
        url = record["url"] if index < 2 else record["url"].replace("pageNumber=1", "pageNumber=2")
        store.record_response(response=response, url=url, method="GET", request_headers=record["request_headers"],
            provider="fixture", endpoint="market-event", scope={"fixture":"synthetic mutation"}, code_version="fixture", mode="fixture")
    return store.root / "manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation", MARKET_EVENT_FAILURES)
def test_market_event_invalid_response_retained_without_fallback(tmp_path, no_network, input_id, mutation):
    case = next(c for c in MARKET_EVENT_CASES if c[0] == input_id)
    report = collect_input(input_id=input_id, context=case[1], config_root=ROOT / "config", output_root=tmp_path / "candidate",
        replay_manifest=market_event_fixture(tmp_path, input_id, mutation))
    assert report["status"] == "failed" and "output" not in report and report["responses"], report
    assert report["live_http_calls"] == report["production_writes"] == 0 and not report["eligible_for_production_routing"]
    assert len(report["responses"]) <= 2
    raw_manifest = Path(report["run_directory"]) / report["raw_manifest"]["path"]
    for event in report["responses"]: assert RawObjectStore.read_response(raw_manifest, event)


@pytest.mark.parametrize("input_id", ["ASTOCK-020", "ASTOCK-021"])
def test_market_event_numeric_null_negative_zero_remain_raw_and_parsed(tmp_path, no_network, input_id):
    case = next(c for c in MARKET_EVENT_CASES if c[0] == input_id)
    report = collect_input(input_id=input_id, context=case[1], config_root=ROOT / "config", output_root=tmp_path / "candidate",
        replay_manifest=market_event_fixture(tmp_path, input_id, "zero_negative"))
    assert report["status"] == "candidate_complete", report
    key = "收盘价" if input_id == "ASTOCK-020" else "实际解禁数量"
    multiplier = 1 if input_id == "ASTOCK-020" else 10000
    assert [r[key] for r in read_artifact(report, "parsed_rows")][:3] == [-multiplier, 0, None]


def test_market_event_multi_page_retrieval_and_source_totals(tmp_path, no_network):
    report = collect_input(input_id="ASTOCK-021", context=MARKET_EVENT_CASES[1][1], config_root=ROOT / "config", output_root=tmp_path / "candidate",
        replay_manifest=market_event_fixture(tmp_path, "ASTOCK-021", "multi_page"))
    assert report["status"] == "candidate_complete" and report["row_count"] == report["source_total_count"] == 501, report
    assert len(report["responses"]) == 3 and report["source_page_count"] == 2


def test_market_event_yaml_fields_scope_and_existing_methods(tmp_path, no_network):
    import ast
    baseline = ROOT / "provider_validation/results/market-events-original-20261004/0-financial.py.bin"
    def methods(p):
        tree = ast.parse(p.read_text(encoding="utf-8"))
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "EastMoneyFinancialMainProvider")
        return {n.name:ast.dump(n, include_attributes=False) for n in cls.body if isinstance(n, ast.FunctionDef)}
    old = methods(baseline); current = methods(ROOT / "src/providers/eastmoney/financial.py")
    assert all(current[name] == body for name, body in old.items())
    config = tmp_path / "config"; shutil.copytree(ROOT / "config", config)
    path = config / "normalization/lockup_expiry.yaml"; rule = yaml.safe_load(path.read_text(encoding="utf-8"))
    rule["rules"][0]["field_mapping"]["name"] = "限售股类型"
    path.write_text(yaml.safe_dump(rule, allow_unicode=True), encoding="utf-8")
    fields = ["snapshot_at", "source_security_code", "release_date", "restricted_share_type", "name"]
    report = collect_input(input_id="ASTOCK-021", context=MARKET_EVENT_CASES[1][1], config_root=config,
        output_root=tmp_path / "candidate", replay_manifest=SDK_ARCHIVE, fields=fields)
    assert report["status"] == "candidate_complete", report
    assert all(set(row) == set(fields) and row["name"] == row["restricted_share_type"] for row in read_artifact(report, "output"))
    for input_id, context, manifest, count in MARKET_EVENT_CASES:
        for extra in ({"symbol":"600519"}, {"symbols":["600519"]}):
            with pytest.raises(ValueError, match="does not support"):
                collect_input(input_id=input_id, context={**context, "request":{**context["request"], **extra}}, config_root=ROOT / "config", output_root=tmp_path / "bad", replay_manifest=manifest)
    with pytest.raises(ValueError, match="end must not precede start"):
        collect_input(input_id="ASTOCK-021", context={"request":{"start_date":"2026-10-03", "end_date":"2026-09-01"}}, config_root=ROOT / "config", output_root=tmp_path / "bad", replay_manifest=SDK_ARCHIVE)
    with pytest.raises(ValueError, match="calendar"):
        collect_input(input_id="ASTOCK-020", context={"request":{"trade_date":"2026-09-30"}}, config_root=ROOT / "config", output_root=tmp_path / "bad", replay_manifest=SDK_ARCHIVE)


def test_market_event_original_session_cache_and_no_source_switch(tmp_path, no_network):
    from unittest.mock import patch
    seen = []
    def send(session, request, **kwargs):
        record = next(r for id in ("ASTOCK-020", "ASTOCK-021") for r in market_event_records(id) if r["url"] == request.url)
        seen.append(request.url)
        assert request.method == "GET" and kwargs["timeout"] is None and kwargs["allow_redirects"] and session.trust_env
        assert request.headers["User-Agent"] == record["request_headers"]["User-Agent"]
        retry = session.get_adapter(request.url).max_retries
        assert retry.total == retry.connect == retry.read == retry.status == 2 and retry.backoff_factor == 5
        assert not retry.is_retry("GET", 403) and not retry.is_retry("GET", 429, True)
        response = requests.Response(); response.status_code = 200; response.encoding = "utf-8"
        response._content = RawObjectStore.read_response(SDK_ARCHIVE, record); response.url = request.url
        return response
    with patch("requests.Session.send", send):
        for input_id, context, _, _ in MARKET_EVENT_CASES:
            first = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "candidate", mode="live", evidence_root=tmp_path / "unused")
            second = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "candidate", mode="live", evidence_root=tmp_path / "candidate")
            assert first["status"] == second["status"] == "candidate_complete", (first, second)
            assert first["live_http_calls"] == 1 and second["live_http_calls"] == 0
            assert read_artifact(first, "output") == read_artifact(second, "output")
    assert len(seen) == 2
    (tmp_path / "fixture-mode.json").write_text(json.dumps(dict(mode="injected Session, not real live validation", real_http_calls=0, fixture_send_calls=2)), encoding="utf-8")

FACTOR_CASES = [("ASTOCK-006", {"request":{"symbol":"600519"},"config":{"kind":kind}}, SDK_ARCHIVE, 33) for kind in ("qfq","hfq")]
FACTOR_FAILURES = ["empty","schema","order","numeric","date","duplicate","count","variable","executable","trailing_code","http403","missing_hfq"]


def factor_records():
    return [json.loads(line) for line in SDK_ARCHIVE.read_text(encoding="utf-8").splitlines()
            if "/sh600519/qfq.js" in line or "/sh600519/hfq.js" in line]


def compare_factor_original(tmp_path,input_id,context,manifest,count):
    import ast,inspect,csv,re
    import akshare as sdk
    import pandas as pd
    from typing import Any,Callable,List,Tuple,Dict
    from types import SimpleNamespace
    from stock_data_manage.pipeline.inputs import _json_value
    from stock_data_manage.providers.sina.daily import parse_adjustment_payload
    source=ROOT/"provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py"
    names={"ak_function","call_ak","fetch_sina_adjust_factor","sina_symbol","only_digits","market_of"}
    nodes=[node for node in ast.parse(source.read_text(encoding="utf-8")).body if isinstance(node,ast.FunctionDef) and node.name in names]
    namespace=dict(inspect=inspect,re=re,Any=Any,Callable=Callable,List=List,Tuple=Tuple,Dict=Dict,Config=SimpleNamespace,pd=pd,ensure_ak=lambda:sdk)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source),"exec"),namespace)
    with captured_requests(RawObjectStore(tmp_path/"original"),provider="sina",endpoint="factor",scope={},code_version="original-factor",
        pacer=RequestPacer(),replay_manifest=manifest,sdk_retry_policy=True,probe_host_pause=True) as events:
        frames=namespace["fetch_sina_adjust_factor"](SimpleNamespace(code="600519"))
    parsed={key:_json_value(frame.to_dict(orient="records")) for key,frame in frames.items()}
    path=tmp_path/"original-parsed.json";path.write_text(json.dumps(parsed,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    csv_refs=[]
    for key,rows in parsed.items():
        csv_path=SDK_ARCHIVE.parents[2]/"06_新浪复权因子/data"/(key+".csv")
        with csv_path.open(encoding="utf-8-sig",newline="") as stream:golden=list(csv.DictReader(stream))
        assert len(golden)==len(rows)==33
        assert [{**row,"date":row["date"][:10]} for row in rows]==golden
        csv_refs.append(dict(path=str(csv_path),sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest()))
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==count,report
    source=[]
    for record in factor_records():
        kind,symbol,items=parse_adjustment_payload(RawObjectStore.read_response(manifest,record),record["url"])
        source.extend({"factor_kind":kind,**row} for row in items)
    assert read_artifact(report,"source_rows")==source and len(source)==66
    kind=context["config"]["kind"];candidate=read_artifact(report,"parsed_rows");excluded=read_artifact(report,"excluded_rows")
    for name,rows in ((kind,candidate),("hfq" if kind=="qfq" else "qfq",excluded)):
        assert [{"date":row["source_date"],name+"_factor":row["source_factor"]} for row in rows]==parsed[name+"_factor"]
        assert all(row["raw_factor"]==row["source_factor"] and row["factor_kind"]==name for row in rows)
    assert report["original_row_count"]==66 and report["selected_row_count"]==len(excluded)==33
    for output,row in zip(read_artifact(report,"output"),candidate):
        assert output["raw_factor"]==row["source_factor"] and output["factor_date"]==row["source_date"][:10]
        assert output["snapshot_at"]==report["source_capture_window"]["last"] and output["factor_value"] is None
        assert output["instrument_id"]=="XSHG:600519" and output["factor_kind"]==kind
    assert read_artifact(report,"output")[-1]["factor_date"]=="1900-01-01"
    keys=("url","method","request_headers","request_options","status_code","body_sha256")
    assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    comparison=dict(input_id=input_id,kind=kind,row_count=count,mode="offline_replay",all_business_fields_equal=True,
        all_retained_source_fields_equal=True,request_comparison_equal=True,original_csv=csv_refs,
        original_parsed_path=str(path.resolve()),original_parsed_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        report_path=report["report_path"],report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest())
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count",FACTOR_CASES)
def test_factor_original_sdk_csv_precision_and_baseline(tmp_path,no_network,input_id,context,manifest,count):
    compare_factor_original(tmp_path,input_id,context,manifest,count)


def factor_fixture(tmp_path,mutation):
    store=RawObjectStore(tmp_path/"fixture")
    for index,record in enumerate(factor_records()):
        if index==1 and mutation=="missing_hfq":continue
        original=RawObjectStore.read_response(SDK_ARCHIVE,record)
        body=original;status=200
        if index==0:
            payload=json.loads(original.decode().split("=",1)[1].split("\n")[0]);items=payload["data"]
            if mutation=="empty":items.clear();payload["total"]=0
            elif mutation=="schema":items[0].pop("f")
            elif mutation=="order":items[0]={"f":items[0]["f"],"d":items[0]["d"]}
            elif mutation=="numeric":items[0]["f"]="NaN"
            elif mutation=="date":items[0]["d"]="2026-99-01"
            elif mutation=="duplicate":items[1]=items[0].copy()
            elif mutation=="count":payload["total"]+=1
            body=("var sh600519qfq="+json.dumps(payload,ensure_ascii=False)).encode()
            if mutation=="variable":body=body.replace(b"sh600519qfq",b"sz000001qfq")
            elif mutation=="executable":body=b"var sh600519qfq=__import__('builtins').print('executed')"
            elif mutation=="trailing_code":body+=b"\nprint('executed')"
            elif mutation=="http403":body=original;status=403
        response=requests.Response();response._content=body;response.status_code=status;response.encoding="utf-8"
        store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],scope={"fixture":mutation},
            provider="sina",endpoint="factor",code_version="synthetic-factor",mode="fixture",source_ref={"manifest":str(SDK_ARCHIVE),"sha256":record["body_sha256"]})
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("mutation",FACTOR_FAILURES)
def test_factor_failures_saved_before_sdk_eval(tmp_path,no_network,mutation):
    from unittest.mock import patch
    manifest=factor_fixture(tmp_path,mutation)
    # No response expression may execute: the original SDK cannot reach eval on invalid data.
    with patch("builtins.eval",side_effect=AssertionError("SDK eval reached before response gate")) if mutation not in {"missing_hfq"} else patch("builtins.eval",wraps=eval):
        report=collect_input(input_id="ASTOCK-006",context={"request":{"symbol":"600519"}},config_root=ROOT/"config",
            output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="failed" and "output" not in report and report["production_writes"]==0,report
    assert report["failure_class"]==("ValueError" if mutation=="missing_hfq" else "RuntimeError"),report
    assert len(report["responses"])==1 and report["responses"][0]["body_sha256"]==json.loads(manifest.read_text(encoding="utf-8").splitlines()[0])["body_sha256"]
    body=RawObjectStore.read_response(Path(report["run_directory"])/report["raw_manifest"]["path"],report["responses"][0])
    assert body==RawObjectStore.read_response(manifest,json.loads(manifest.read_text(encoding="utf-8").splitlines()[0]))


def test_factor_yaml_selection_mapping_and_scope(tmp_path,no_network):
    from stock_data_manage.config.loader import load_input_capabilities
    contract=next(c for c in load_input_capabilities(ROOT/"config/providers.yaml") if c.input_id=="ASTOCK-006")
    assert contract.base_requests_per_fetch==2 and contract.collection_profile=="market_after_close"
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    mapping=config/"normalization/adjustment_factor.yaml";doc=yaml.safe_load(mapping.read_text(encoding="utf-8"));doc["rules"][0]["field_mapping"]["raw_factor"]="source_date";mapping.write_text(yaml.safe_dump(doc,allow_unicode=True),encoding="utf-8")
    fields=["snapshot_at","instrument_id","factor_kind","factor_date","raw_factor"]
    report=collect_input(input_id="ASTOCK-006",context={"request":{"symbol":"sh600519"},"config":{"kind":"hfq"}},config_root=config,
        output_root=tmp_path/"candidate",replay_manifest=SDK_ARCHIVE,fields=fields)
    assert report["status"]=="candidate_complete" and set(read_artifact(report,"output")[0])==set(fields),report
    assert read_artifact(report,"output")[0]["raw_factor"]==read_artifact(report,"parsed_rows")[0]["source_date"].replace("T"," ")
    for context in [{"request":{"symbol":"600519","start_date":"2026-01-01"}},{"request":{"symbol":"600519","end_date":"2026-09-30"}},
                    {"request":{"symbol":"600519"},"config":{"kind":"raw"}}]:
        with pytest.raises(ValueError):contract.bind_parameters(context)
    for symbol in ("sh000001","sz600519","510300"):
        report=collect_input(input_id="ASTOCK-006",context={"request":{"symbol":symbol}},config_root=ROOT/"config",output_root=tmp_path/"invalid",replay_manifest=SDK_ARCHIVE)
        assert report["status"]=="failed" and not report.get("responses"),report


def test_factor_sdk_original_session_cache_and_guard(tmp_path,no_network):
    from unittest.mock import patch
    seen=[];records=factor_records()
    def send(session,request,**kwargs):
        record=next(r for r in records if r["url"]==request.url);seen.append(request.url)
        assert request.method=="GET" and kwargs["timeout"] is None and kwargs["allow_redirects"] and session.trust_env
        assert request.headers["User-Agent"]==record["request_headers"]["User-Agent"]
        retry=session.get_adapter(request.url).max_retries
        assert retry.total==retry.connect==retry.read==retry.status==2 and retry.backoff_factor==5
        assert not retry.is_retry("GET",403) and not retry.is_retry("GET",429,True)
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=RawObjectStore.read_response(SDK_ARCHIVE,record);response.url=request.url
        return response
    with patch("requests.Session.send",send):
        first=collect_input(input_id="ASTOCK-006",context={"request":{"symbol":"600519"}},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
        second=collect_input(input_id="ASTOCK-006",context={"request":{"symbol":"600519"}},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
    assert first["status"]==second["status"]=="candidate_complete",(first,second)
    assert first["live_http_calls"]==2 and second["live_http_calls"]==0 and len(seen)==2
    assert read_artifact(first,"output")==read_artifact(second,"output")
    (tmp_path/"fixture-mode.json").write_text(json.dumps(dict(mode="injected Session; not real live validation",real_http_calls=0,fixture_send_calls=2)),encoding="utf-8")


def macro_record(input_id):
    needle="shrzgmQuery" if input_id=="ASTOCK-061" else "reportName=RPT_ECONOMY_PMI"
    return next(json.loads(line) for line in SDK_ARCHIVE.read_text(encoding="utf-8").splitlines() if needle in json.loads(line).get("url",""))


def compare_macro_original(tmp_path,input_id,context,manifest,count):
    import ast,inspect,csv
    import akshare as sdk
    import pandas as pd
    from typing import Any,Callable,List,Tuple,Dict
    from types import SimpleNamespace
    from stock_data_manage.pipeline.inputs import _json_value
    source=ROOT/"provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py"
    method="fetch_social_financing" if input_id=="ASTOCK-061" else "fetch_pmi"
    nodes=[n for n in ast.parse(source.read_text(encoding="utf-8")).body if isinstance(n,ast.FunctionDef)
        and n.name in {"ak_function","call_ak","try_ak_variants",method}]
    namespace=dict(inspect=inspect,Any=Any,Callable=Callable,List=List,Tuple=Tuple,Dict=Dict,Config=SimpleNamespace,pd=pd,ensure_ak=lambda:sdk)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source),"exec"),namespace)
    store=RawObjectStore(tmp_path/"original")
    with captured_requests(store,provider="mofcom" if input_id=="ASTOCK-061" else "eastmoney",endpoint="macro",scope={},
        code_version="original-sdk-macro",pacer=RequestPacer(),replay_manifest=manifest,sdk_retry_policy=True,
        probe_host_pause=True,require_empty_post_body=input_id=="ASTOCK-061") as events:
        frame=namespace[method](None)
    parsed=_json_value(frame.to_dict(orient="records"))
    parsed_path=tmp_path/"original-parsed.json";parsed_path.write_text(json.dumps(parsed,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    csv_path=ROOT/"provider_validation/results/live-probes/rate-limited-all-20261003"/("59_人民银行社融" if input_id=="ASTOCK-061" else "60_PMI")/"data.csv"
    with csv_path.open(encoding="utf-8-sig",newline="") as stream:golden=list(csv.DictReader(stream))
    assert len(golden)==len(parsed)==count
    for old,row in zip(parsed,golden):
        assert list(old)==list(row)
        for key,value in old.items():
            if key=="月份":assert str(value)==row[key]
            else:assert float(value)==float(row[key])
    report=collect_input(input_id=input_id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==count,report
    raw=json.loads(RawObjectStore.read_response(manifest,macro_record(input_id)));raw=raw if input_id=="ASTOCK-061" else raw["result"]["data"]
    assert read_artifact(report,"source_rows")==raw
    candidate=read_artifact(report,"parsed_rows");assert [{k:v for k,v in row.items() if k!="statistical_month"} for row in candidate]==parsed
    keys=("url","method","status_code","body_sha256","request_headers","request_options")
    assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    source_keys={"社会融资规模增量":"tiosfs","其中-人民币贷款":"rmblaon","其中-委托贷款外币贷款":"forcloan","其中-委托贷款":"entrustloan",
        "其中-信托贷款":"trustloan","其中-未贴现银行承兑汇票":"ndbab","其中-企业债券":"bibae","其中-非金融企业境内股票融资":"sfinfe"} if input_id=="ASTOCK-061" else {"制造业-指数":"MAKE_INDEX","制造业-同比增长":"MAKE_SAME","非制造业-指数":"NMAKE_INDEX","非制造业-同比增长":"NMAKE_SAME"}
    by_period={row["date"] if input_id=="ASTOCK-061" else row["TIME"]:row for row in raw}
    for row in parsed:
        for key,raw_key in source_keys.items():assert float(row[key])==float(by_period[row["月份"]][raw_key])
    for row,old in zip(read_artifact(report,"output"),parsed):
        expected=datetime.strptime(old["月份"],"%Y%m" if input_id=="ASTOCK-061" else "%Y年%m月份").strftime("%Y-%m")
        assert row["statistical_month"]==expected and row["snapshot_at"]==report["source_capture_window"]["last"] and row["publication_time"] is None
        assert all(row[key] is None for key in report["unverified_fields"])
    comparison=dict(input_id=input_id,row_count=count,mode="offline_replay",all_business_fields_equal=True,
        all_retained_source_fields_equal=True,request_comparison_equal=True,original_csv_path=str(csv_path),original_csv_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        original_parsed_path=str(parsed_path.resolve()),original_parsed_sha256=hashlib.sha256(parsed_path.read_bytes()).hexdigest(),
        report_path=report["report_path"],report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest())
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count",MACRO_CASES)
def test_macro_original_sdk_csv_numeric_and_months(tmp_path,no_network,input_id,context,manifest,count):
    compare_macro_original(tmp_path,input_id,context,manifest,count)


MACRO_FAILURES=[(id,mutation) for id in ("ASTOCK-061","ASTOCK-062")
    for mutation in ("empty","schema","order","numeric","month","duplicate","http403")]+[("ASTOCK-062","business"),("ASTOCK-062","count"),("ASTOCK-062","period_mismatch")]


def macro_fixture(tmp_path,input_id,mutation):
    record=macro_record(input_id);payload=json.loads(RawObjectStore.read_response(SDK_ARCHIVE,record))
    items=payload if input_id=="ASTOCK-061" else payload["result"]["data"]
    month_key="date" if input_id=="ASTOCK-061" else "TIME";numeric_key="tiosfs" if input_id=="ASTOCK-061" else "MAKE_INDEX"
    if mutation=="empty":items.clear()
    elif mutation=="schema":items[0].pop(numeric_key)
    elif mutation=="order":items[0]={k:items[0][k] for k in reversed(items[0])}
    elif mutation=="numeric":items[0][numeric_key]="bad"
    elif mutation=="null":items[0][numeric_key]=None
    elif mutation=="month":items[0][month_key]="invalid"
    elif mutation=="duplicate":items[1]=items[0].copy()
    elif mutation=="business":payload["success"]=False
    elif mutation=="count":payload["result"]["count"]+=1
    elif mutation=="period_mismatch":items[0]["REPORT_DATE"]="2025-01-01 00:00:00"
    elif mutation=="post_body":record=dict(record,request_body_sha256="a"*64)
    elif mutation=="zero_negative":items[0][numeric_key]=-1;items[1][numeric_key]=0
    store=RawObjectStore(tmp_path/"fixture");response=requests.Response();response.status_code=403 if mutation=="http403" else 200;response.encoding="utf-8"
    response._content=json.dumps(payload,ensure_ascii=False).encode()
    store.record_response(response=response,url=record["url"],method=record["method"],request_headers=record["request_headers"],
        provider="fixture",endpoint="macro",scope={"fixture":"synthetic mutation"},code_version="fixture",mode="fixture",
        request_options={"request_body_bytes":record["request_body_bytes"],"request_body_sha256":record["request_body_sha256"]})
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation",MACRO_FAILURES)
def test_macro_invalid_source_never_publishes_and_keeps_raw(tmp_path,no_network,input_id,mutation):
    report=collect_input(input_id=input_id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=macro_fixture(tmp_path,input_id,mutation))
    assert report["status"]=="failed" and "output" not in report and report["responses"],report
    assert report["production_writes"]==report["live_http_calls"]==0


def test_macro_negative_and_zero_values_remain_in_parsed_evidence(tmp_path,no_network):
    for id in ("ASTOCK-061","ASTOCK-062"):
        report=collect_input(input_id=id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=macro_fixture(tmp_path/id,id,"zero_negative"))
        assert report["status"]=="candidate_complete",report
        key="社会融资规模增量" if id=="ASTOCK-061" else "制造业-指数"
        assert {-1,0}<={row[key] for row in read_artifact(report,"parsed_rows")}


@pytest.mark.parametrize("input_id",["ASTOCK-061","ASTOCK-062"])
def test_macro_optional_null_numeric_preserved(tmp_path,no_network,input_id):
    report=collect_input(input_id=input_id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=macro_fixture(tmp_path,input_id,"null"))
    assert report["status"]=="candidate_complete",report
    key="社会融资规模增量" if input_id=="ASTOCK-061" else "制造业-指数"
    assert any(row[key] is None for row in read_artifact(report,"parsed_rows"))


def test_macro_bodyless_post_matching_is_strict(tmp_path,no_network):
    report=collect_input(input_id="ASTOCK-061",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=macro_fixture(tmp_path,"ASTOCK-061","post_body"))
    assert report["status"]=="failed" and report["failure_class"]=="ValueError" and not report.get("responses") and report["live_http_calls"]==0
    store=RawObjectStore(tmp_path/"guard")
    with captured_requests(store,provider="fixture",endpoint="macro",scope={},code_version="fixture",pacer=RequestPacer(),require_empty_post_body=True):
        with pytest.raises(ValueError):requests.post("https://data.mofcom.gov.cn/datamofcom/front/gnmy/shrzgmQuery",data="unexpected")
        with pytest.raises(ValueError):requests.get("https://data.mofcom.gov.cn/datamofcom/front/gnmy/shrzgmQuery")


def test_macro_yaml_projection_mapping_and_forbidden_parameters(tmp_path,no_network):
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    path=config/"normalization/pmi_history.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"));rule["rules"][0]["field_mapping"]["statistical_month"]="月份"
    path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    report=collect_input(input_id="ASTOCK-062",context={},config_root=config,output_root=tmp_path/"candidate",replay_manifest=SDK_ARCHIVE,fields=["snapshot_at","statistical_month"])
    assert report["status"]=="candidate_complete" and read_artifact(report,"output")[0]["statistical_month"]=="2026年09月份",report
    for id in ("ASTOCK-061","ASTOCK-062"):
        for bad in ({"request":{"symbol":"600519"}},{"request":{"start_date":"2026-01-01"}},{"request":{"end_date":"2026-10-01"}}):
            with pytest.raises(ValueError):collect_input(input_id=id,context=bad,config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=SDK_ARCHIVE)


def test_macro_original_sdk_adapter_timeout_headers_and_cache(tmp_path,monkeypatch):
    from unittest.mock import patch
    from stock_data_manage.storage.raw import sanitized_url
    seen=[]
    def send(session,request,**kwargs):
        id="ASTOCK-061" if request.method=="POST" else "ASTOCK-062";record=macro_record(id);seen.append(request.url)
        assert sanitized_url(request.url)==sanitized_url(record["url"]) and session.trust_env is True
        assert kwargs["timeout"] is None and kwargs["allow_redirects"] is True and request.headers["User-Agent"]==record["request_headers"]["User-Agent"]
        adapter=session.get_adapter(request.url)
        if id=="ASTOCK-061":
            assert type(adapter).__name__=="TLSAdapter" and adapter.max_retries.total==0 and request.body is None
            assert adapter.poolmanager.connection_pool_kw["ssl_context"] is not None
        else:assert adapter.max_retries.total==2 and adapter.max_retries.backoff_factor==5
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=RawObjectStore.read_response(SDK_ARCHIVE,record);response.url=request.url;return response
    with patch("requests.Session.send",send):
        for id in ("ASTOCK-061","ASTOCK-062"):
            first=collect_input(input_id=id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
            second=collect_input(input_id=id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
            assert first["status"]==second["status"]=="candidate_complete",(first,second)
            assert first["live_http_calls"]==1 and second["live_http_calls"]==0 and read_artifact(first,"output")==read_artifact(second,"output")
    assert len(seen)==2
    (tmp_path/"fixture-mode.json").write_text(json.dumps(dict(mode="injected SDK Session; no real live validation",real_http_calls=0,fixture_send_calls=2)),encoding="utf-8")


def sdk_news_record(input_id):
    host = "www.cls.cn" if input_id == "ASTOCK-032" else "zhibo.sina.com.cn"
    return next(json.loads(line) for line in SDK_ARCHIVE.read_text(encoding="utf-8").splitlines()
                if host in json.loads(line).get("url", ""))


def compare_sdk_news_original(tmp_path, input_id, context, manifest, count):
    import csv
    from contextlib import ExitStack
    from stock_data_manage.providers.cls.news import telegraph_replay_clock
    from stock_data_manage.pipeline.inputs import _json_value
    import ast
    import inspect
    import akshare as sdk
    import pandas as pd
    from types import SimpleNamespace
    from typing import Any, Callable, List, Tuple, Dict
    source = ROOT / "provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py"
    nodes = [n for n in ast.parse(source.read_text(encoding="utf-8")).body if isinstance(n, ast.FunctionDef)
             and n.name in {"ak_function", "call_ak", "try_ak_variants", "fetch_cls_telegraph", "fetch_global_news"}]
    namespace = dict(inspect=inspect, Any=Any, Callable=Callable, List=List, Tuple=Tuple, Dict=Dict,
                     Config=SimpleNamespace, pd=pd, ensure_ak=lambda:sdk)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source),"exec"),namespace)
    function = sdk.stock_info_global_cls if input_id == "ASTOCK-032" else sdk.stock_info_global_sina
    store = RawObjectStore(tmp_path / "original")
    with ExitStack() as stack:
        events = stack.enter_context(captured_requests(store, provider="cls" if input_id == "ASTOCK-032" else "sina",
            endpoint="sdk-news", scope={}, code_version="original-sdk-news", pacer=RequestPacer(),
            replay_manifest=manifest, sdk_retry_policy=True, probe_host_pause=True))
        if input_id == "ASTOCK-032": stack.enter_context(telegraph_replay_clock(function, manifest))
        frame = namespace["fetch_cls_telegraph"](None) if input_id == "ASTOCK-032" else namespace["fetch_global_news"](None)
    parsed = _json_value(frame.to_dict(orient="records"))
    original_path = tmp_path / "original-parsed.json"
    original_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    csv_path = ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003" / ("30_财联社电报" if input_id == "ASTOCK-032" else "31_全球资讯") / "data.csv"
    with csv_path.open(encoding="utf-8-sig", newline="") as stream: csv_rows = list(csv.DictReader(stream))
    assert [{k: str(v) for k, v in row.items()} for row in parsed] == csv_rows
    report = collect_input(input_id=input_id, context=context, config_root=ROOT/"config", output_root=tmp_path/"candidate", replay_manifest=manifest)
    assert report["status"] == "candidate_complete" and report["row_count"] == len(parsed) == count, report
    assert read_artifact(report, "source_rows") == parsed
    keys = ("url", "method", "status_code", "body_sha256", "request_headers", "request_options")
    assert [{k: r.get(k) for k in keys} for r in events] == [{k: r.get(k) for k in keys} for r in report["responses"]]
    for row, old in zip(read_artifact(report, "output"), parsed):
        assert row["snapshot_at"] == report["source_capture_window"]["last"]
        assert row["content"] == old["内容"]
        expected_time = old["发布日期"]+"T"+old["发布时间"] if input_id == "ASTOCK-032" else old["时间"].replace(" ", "T")
        assert row["news_time"] == expected_time+"+08:00"
        if input_id == "ASTOCK-032": assert row["title"] == old["标题"]
    comparison = dict(input_id=input_id, row_count=count, mode="offline_replay", all_business_fields_equal=True,
        all_retained_source_fields_equal=True, request_comparison_equal=True, original_csv_path=str(csv_path),
        original_csv_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(), original_parsed_path=str(original_path.resolve()),
        original_parsed_sha256=hashlib.sha256(original_path.read_bytes()).hexdigest(), report_path=report["report_path"],
        report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest())
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count", SDK_NEWS_CASES)
def test_sdk_news_preserve_original_requests_csv_and_business_fields(tmp_path,no_network,input_id,context,manifest,count):
    compare_sdk_news_original(tmp_path,input_id,context,manifest,count)


SDK_NEWS_FAILURES = [(id, mutation) for id in ("ASTOCK-032", "ASTOCK-033")
                     for mutation in ("business", "empty", "content", "time", "duplicate", "schema", "http403")]


def sdk_news_fixture(tmp_path, input_id, mutation):
    record = sdk_news_record(input_id); payload = json.loads(RawObjectStore.read_response(SDK_ARCHIVE,record))
    items = payload["data"]["roll_data"] if input_id == "ASTOCK-032" else payload["result"]["data"]["feed"]["list"]
    if mutation == "business":
        if input_id == "ASTOCK-032": payload["errno"] = 1
        else: payload["result"]["status"]["code"] = 1
    elif mutation == "empty": items.clear()
    elif mutation == "content": items[0]["content" if input_id=="ASTOCK-032" else "rich_text"] = None
    elif mutation == "time": items[0]["ctime" if input_id=="ASTOCK-032" else "create_time"] = "bad"
    elif mutation == "duplicate": items[1] = items[0].copy()
    elif mutation == "schema": items[0].pop("content" if input_id=="ASTOCK-032" else "rich_text")
    store = RawObjectStore(tmp_path/"fixture")
    status = 403 if mutation=="http403" else 200
    body = json.dumps(payload,ensure_ascii=False).encode()
    response = requests.Response(); response.status_code=status; response.encoding="utf-8"; response._content=body
    store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],
        scope={"fixture":"synthetic mutation"},provider="cls" if input_id=="ASTOCK-032" else "sina",endpoint="fixture",code_version="fixture",
        mode="fixture",source_ref={"manifest":str(SDK_ARCHIVE),"sha256":record["body_sha256"]})
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation",SDK_NEWS_FAILURES)
def test_sdk_news_failures_keep_raw_and_never_publish(tmp_path,no_network,input_id,mutation):
    import akshare
    from types import SimpleNamespace
    from unittest.mock import patch
    helper = akshare.stock_info_global_cls.__globals__["make_request_with_retry_json"]
    waits = []
    with patch.dict(helper.__globals__, {"time":SimpleNamespace(sleep=waits.append)}):
        report=collect_input(input_id=input_id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",
            replay_manifest=sdk_news_fixture(tmp_path,input_id,mutation))
    assert report["status"]=="failed" and "output" not in report,report
    assert report["responses"] and report["production_writes"]==report["live_http_calls"]==0
    if input_id=="ASTOCK-032" and mutation=="http403":
        assert report["failure_class"]=="HostPausedError" and waits==[1] and len(report["host_pause_events"])==1


def test_sdk_news_yaml_projection_and_scope(tmp_path,no_network):
    config=tmp_path/"config"; shutil.copytree(ROOT/"config",config)
    path=config/"normalization/cls_telegraph.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"))
    rule["rules"][0]["field_mapping"]["title"]="内容";path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    report=collect_input(input_id="ASTOCK-032",context={},config_root=config,output_root=tmp_path/"candidate",replay_manifest=SDK_ARCHIVE)
    assert report["status"]=="candidate_complete",report
    assert read_artifact(report,"output")[0]["title"]==read_artifact(report,"source_rows")[0]["内容"]
    report=collect_input(input_id="ASTOCK-032",context={},config_root=ROOT/"config",output_root=tmp_path/"projection",
        replay_manifest=SDK_ARCHIVE,fields=["snapshot_at","news_time","content"])
    assert report["status"]=="candidate_complete" and set(read_artifact(report,"output")[0])=={"snapshot_at","news_time","content"}
    for id in ("ASTOCK-032","ASTOCK-033"):
        for context in ({"request":{"symbol":"600519"}},{"metadata":{"cursor":"older"}},{"request":{"start_date":"2026-01-01"}}):
            with pytest.raises(ValueError):collect_input(input_id=id,context=context,config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=SDK_ARCHIVE)


def test_sdk_news_original_session_policy_and_cache(tmp_path,monkeypatch):
    import akshare
    from unittest.mock import patch
    from stock_data_manage.providers.cls.news import telegraph_replay_clock
    from stock_data_manage.storage.raw import sanitized_url
    seen=[]
    def send(session,request,**kwargs):
        record=next(sdk_news_record(id) for id in ("ASTOCK-032","ASTOCK-033") if sanitized_url(sdk_news_record(id)["url"])==sanitized_url(request.url))
        seen.append(request.url)
        assert session.trust_env is True and kwargs["timeout"] is None and kwargs["allow_redirects"] is True
        retry=session.get_adapter(request.url).max_retries
        assert retry.total==retry.connect==retry.read==retry.status==2 and retry.backoff_factor==5
        assert not retry.is_retry("GET",403) and not retry.is_retry("GET",429,True) and retry.is_retry("GET",503)
        assert request.headers["User-Agent"]==record["request_headers"]["User-Agent"]
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=RawObjectStore.read_response(SDK_ARCHIVE,record)
        response.url=request.url;return response
    with patch("requests.Session.send",send),telegraph_replay_clock(akshare.stock_info_global_cls,SDK_ARCHIVE):
        for id in ("ASTOCK-032","ASTOCK-033"):
            first=collect_input(input_id=id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
            second=collect_input(input_id=id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
            assert first["status"]==second["status"]=="candidate_complete",(first,second)
            assert first["live_http_calls"]==1 and second["live_http_calls"]==0 and read_artifact(first,"output")==read_artifact(second,"output")
    assert len(seen)==2
    (tmp_path/"fixture-mode.json").write_text(json.dumps(dict(mode="injected Session and fixed CLS query clock; no real live validation",real_http_calls=0,fixture_send_calls=2)),encoding="utf-8")


def test_sdk_news_cls_retry_retains_failed_response_and_sdk_backoff(tmp_path,no_network):
    import akshare
    from types import SimpleNamespace
    from unittest.mock import patch
    record=sdk_news_record("ASTOCK-032");store=RawObjectStore(tmp_path/"fixture")
    for status in (503,200):
        response=requests.Response();response.status_code=status;response.encoding="utf-8"
        response._content=b'{"fixture":"temporary server failure"}' if status==503 else RawObjectStore.read_response(SDK_ARCHIVE,record)
        store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],
            provider="cls",endpoint="fixture",scope={"fixture":"failed request then successful retry"},code_version="fixture",mode="fixture")
    helper=akshare.stock_info_global_cls.__globals__["make_request_with_retry_json"];waits=[]
    with patch.dict(helper.__globals__,{"time":SimpleNamespace(sleep=waits.append)}):
        report=collect_input(input_id="ASTOCK-032",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=store.root/"manifest.ndjson")
    assert report["status"]=="candidate_complete" and report["row_count"]==20,report
    assert waits==[1] and [r["status_code"] for r in report["responses"]]==[503,200] and not report["host_pause_events"]
    assert len(report["output"]["source_response_hashes"])==2
    (tmp_path/"sdk-retry-proof.json").write_text(json.dumps(dict(sdk_delays=waits,statuses=[503,200],all_responses_retained=True,mode="synthetic offline")),encoding="utf-8")


def test_sdk_news_replay_miss_never_contacts_network(tmp_path,no_network):
    wrong=sdk_news_fixture(tmp_path,"ASTOCK-033","business")
    report=collect_input(input_id="ASTOCK-032",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=wrong)
    assert report["status"]=="failed" and report["failure_class"]=="ValueError" and report["live_http_calls"]==0 and not report.get("responses")


def test_sdk_news_signature_redaction():
    from stock_data_manage.storage.raw import sanitized_url,sanitized_metadata
    assert "secret-sign" not in sanitized_url("https://www.cls.cn/feed?last_time=123&sign=secret-sign")
    assert sanitized_metadata({"sign":"secret-sign","last_time":123})=={"sign":"<redacted>","last_time":123}


@pytest.mark.parametrize("sequence",[(429,200),(403,200),(503,503,200),(503,200,503,200),("error","error",200)])
def test_sdk_news_host_pause_matches_original_probe(tmp_path,sequence):
    from types import SimpleNamespace
    from unittest.mock import patch
    from stock_data_manage.providers.transport import HostPausedError
    spec=importlib.util.spec_from_file_location("news_rate_probe",ROOT/"provider_validation/tests/run_a_stock_rate_limited_probes.py")
    original=importlib.util.module_from_spec(spec);spec.loader.exec_module(original)
    results=[]
    for candidate in (False,True):
        calls=[];outcomes=[];store=RawObjectStore(tmp_path/str(candidate));clock=[100.0]
        def fake_send(session,request,**kwargs):
            value=sequence[len(calls)];calls.append(value)
            if value=="error":raise requests.ConnectionError("fixture transport error")
            response=requests.Response();response.status_code=value;response._content=b'{}';response.url=request.url;return response
        if candidate:
            with patch("requests.Session.send",fake_send),captured_requests(store,provider="fixture",endpoint="news",scope={},code_version="fixture",
                pacer=RequestPacer(clock=lambda:clock[0],wait=lambda n:clock.__setitem__(0,clock[0]+n)),probe_host_pause=True):
                for _ in sequence:
                    try:outcomes.append(requests.get("https://news.fixture.test/feed").status_code)
                    except (HostPausedError,requests.ConnectionError) as exc:outcomes.append(type(exc).__name__)
        else:
            with patch.object(original,"time",SimpleNamespace(monotonic=lambda:clock[0],sleep=lambda n:clock.__setitem__(0,clock[0]+n))),patch("requests.Session.send",original.paced_send_wrapper(fake_send)):
                for _ in sequence:
                    try:outcomes.append(requests.get("https://news.fixture.test/feed").status_code)
                    except (original.HostPausedError,requests.ConnectionError) as exc:outcomes.append(type(exc).__name__)
        results.append(dict(calls=calls,outcomes=outcomes))
    assert results[0]==results[1]
    (tmp_path/"original-vs-provider-pause.json").write_text(json.dumps(dict(sequence=sequence,original=results[0],provider=results[1],equal=True)),encoding="utf-8")

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
BAO_CASES = [("SDA-BOARD-005", BAO_CONTEXT, BAO_ARCHIVE, 5224), ("SDA-BOARD-006", BAO_CONTEXT, BAO_ARCHIVE, 5221)]
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

EVENT_ARCHIVE = ROOT / "provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson"
FUTURES_ARCHIVE = ROOT / "provider_validation/results/raw/2026-10-01-v310-live/manifest.ndjson"
REPORTS_CALENDAR_CONTEXT={"request":{"start_date":"2026-09-01","end_date":"2026-09-18"}}
REPORTS_CALENDAR_CASES=[("ASTOCK-013",{},EVENT_ARCHIVE,40),("ASTOCK-066",REPORTS_CALENDAR_CONTEXT,EVENT_ARCHIVE,552),
    ("ASTOCK-066",{**REPORTS_CALENDAR_CONTEXT,"config":{"country":"中国","min_importance":3}},EVENT_ARCHIVE,48)]


class ReportTestClock:
    def __init__(self):self.now=1000.0;self.waits=[]
    def time(self):return self.now
    def sleep(self,seconds):self.waits.append(seconds);self.now+=seconds


def reports_calendar_records(input_id):
    needle="https://vip.stock.finance.sina.com.cn/q/go.php/vReport_List/" if input_id=="ASTOCK-013" else "/apiv1/finance/macrodatas?"
    return [json.loads(line) for line in EVENT_ARCHIVE.read_text(encoding="utf-8").splitlines() if needle in json.loads(line).get("url","")]


def compare_reports_calendar_original(tmp_path,input_id,context,manifest,count):
    import pandas as pd
    from unittest.mock import patch
    from stock_data_manage.providers.sina import news as sina_news
    spec=importlib.util.spec_from_file_location("reports_calendar_original",ROOT/"provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);ns=module.load_shipped_code();store=RawObjectStore(tmp_path/"original")
    ns["time"]=ReportTestClock()
    try:
        with captured_requests(store,provider="sina" if input_id=="ASTOCK-013" else "wallstreetcn",endpoint="reports-calendar",scope=context,
            code_version="original-reports-calendar",pacer=RequestPacer(),replay_manifest=manifest) as events:
            request=context.get("request",{});params=context.get("config",{})
            frame=ns["sina_research_reports"]() if input_id=="ASTOCK-013" else ns["macro_calendar"](request["start_date"],request["end_date"],**params)
    finally:ns["EM_SESSION"].close()
    parsed=[{k:None if pd.isna(v) else v for k,v in row.items()} for row in frame.to_dict(orient="records")]
    original_path=tmp_path/"original-parsed.json";original_path.write_text(json.dumps(parsed,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    with patch.object(sina_news,"_report_clock",ReportTestClock()),patch.object(sina_news,"_report_last",[0.0]):
        report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==len(parsed)==count,report
    keys=("url","method","status_code","body_sha256","request_headers","request_options")
    assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    candidate=read_artifact(report,"parsed_rows")
    for row,old in zip(candidate,parsed):
        for key,value in row.items():
            if key!="source_event_id":assert value==old[key],(key,value,old[key])
    if input_id=="ASTOCK-013":assert read_artifact(report,"source_rows")==candidate
    else:
        raw=[]
        for record in reports_calendar_records(input_id):raw.extend(json.loads(RawObjectStore.read_response(manifest,record))["data"]["items"])
        assert read_artifact(report,"source_rows")==raw
        selected_ids={row["source_event_id"] for row in candidate}
        assert read_artifact(report,"excluded_rows")==[row for row in raw if str(row["id"]) not in selected_ids]
        by_id={str(row["id"]):row for row in raw}
        for row in candidate:
            old=by_id[row["source_event_id"]]
            assert row["title"]==old["title"] and row["importance"]==old["importance"]
    for row,old in zip(read_artifact(report,"output"),candidate):
        assert row["snapshot_at"]==report["source_capture_window"]["last"]
        if input_id=="ASTOCK-013":assert row["source_report_id"]==old["report_id"] and row["report_date"]==old["date"] and row["title"]==old["title"]
        else:
            assert row["event_time"]==old["time"].replace(" ","T")+":00+08:00"
            assert row["source_event_id"]==old["source_event_id"] and row["title"]==old["title"]
            for key in ("actual","forecast","previous","revised"):
                assert row[key+"_text"]==(str(old[key]) if old[key] is not None else None)
    comparison=dict(input_id=input_id,row_count=count,mode="offline_replay",all_business_fields_equal=True,all_retained_source_fields_equal=True,
        request_comparison_equal=True,original_parsed_path=str(original_path.resolve()),original_parsed_sha256=hashlib.sha256(original_path.read_bytes()).hexdigest(),
        report_path=report["report_path"],report_sha256=hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest())
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count",REPORTS_CALENDAR_CASES)
def test_reports_calendar_original_requests_fields_and_filters(tmp_path,no_network,input_id,context,manifest,count):
    compare_reports_calendar_original(tmp_path,input_id,context,manifest,count)


REPORTS_CALENDAR_FAILURES=[("ASTOCK-013",m) for m in ("table","missing_row","day","duplicate","http403")]+[
    ("ASTOCK-066",m) for m in ("business","structure","week_empty","time","outside","importance","duplicate","http403")]


def reports_calendar_fixture(tmp_path,input_id,mutation):
    import re
    store=RawObjectStore(tmp_path/"fixture")
    for index,record in enumerate(reports_calendar_records(input_id)):
        body=RawObjectStore.read_response(EVENT_ARCHIVE,record)
        if index==0:
            if input_id=="ASTOCK-013":
                text=body.decode("gbk","replace")
                if mutation=="table":text=text.replace("研究员","changed")
                elif mutation=="missing_row":text=text.replace('class="tal f14"','class="changed"',1)
                elif mutation=="day":text=re.sub(r'(<td>)\d{4}-\d{2}-\d{2}(</td>)',r'\1bad\2',text,count=1)
                elif mutation=="duplicate":
                    ids=re.findall(r'/rptid/(\d+)/',text);text=text.replace('/rptid/'+ids[1]+'/', '/rptid/'+ids[0]+'/',1)
                elif mutation in {"false_empty","valid_empty"}:text='<table class="tb_01">研究员</table>没有找到相关内容'
                body=text.encode("gbk",errors="replace")
            else:
                payload=json.loads(body);items=payload["data"]["items"]
                if mutation=="business":payload["code"]=0
                elif mutation=="structure":payload["data"]["items"]={}
                elif mutation=="week_empty":items.clear()
                elif mutation=="time":items[0]["public_date"]="bad"
                elif mutation=="outside":items[0]["public_date"]=0
                elif mutation=="importance":items[0]["importance"]=True
                elif mutation=="duplicate":items[1]["id"]=items[0]["id"]
                elif mutation=="display_values":items[0].update(actual=0,forecast="0",previous="--",revised=" null ")
                body=json.dumps(payload,ensure_ascii=False).encode()
        response=requests.Response();response.status_code=403 if mutation=="http403" and index==0 else 200;response.encoding=record.get("response_encoding") or "utf-8";response._content=body
        store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],provider="fixture",endpoint="reports-calendar",
            scope={"fixture":"synthetic mutation"},code_version="fixture",mode="fixture")
        if input_id=="ASTOCK-013" and mutation in {"false_empty","valid_empty"}:
            response._content=RawObjectStore.read_response(EVENT_ARCHIVE,record) if mutation=="false_empty" else body
            store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],provider="fixture",endpoint="reports-calendar",scope={"fixture":"second empty-page attempt"},code_version="fixture",mode="fixture")
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation",REPORTS_CALENDAR_FAILURES)
def test_reports_calendar_failures_keep_evidence(tmp_path,no_network,input_id,mutation):
    from stock_data_manage.providers.sina import news as sina_news
    from unittest.mock import patch
    context={} if input_id=="ASTOCK-013" else REPORTS_CALENDAR_CONTEXT
    with patch.object(sina_news,"_report_clock",ReportTestClock()),patch.object(sina_news,"_report_last",[0.0]):
        report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=reports_calendar_fixture(tmp_path,input_id,mutation))
    assert report["status"]=="failed" and "output" not in report and report["responses"],report
    assert report["production_writes"]==report["live_http_calls"]==0


@pytest.mark.parametrize("mutation,count",[("false_empty",40),("valid_empty",0)])
def test_reports_calendar_original_empty_retry_and_six_seconds(tmp_path,no_network,mutation,count):
    from stock_data_manage.providers.sina import news as sina_news
    from unittest.mock import patch
    clock=ReportTestClock()
    with patch.object(sina_news,"_report_clock",clock),patch.object(sina_news,"_report_last",[0.0]):
        report=collect_input(input_id="ASTOCK-013",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=reports_calendar_fixture(tmp_path,"ASTOCK-013",mutation))
    assert report["status"]=="candidate_complete" and report["row_count"]==count and report["empty_page_attempts"]==2,report
    assert len(report["responses"])==2 and clock.waits==[6.0] and report["valid_empty_dataset"]==(count==0)
    (tmp_path/"retry-proof.json").write_text(json.dumps(dict(mode="synthetic clock and empty HTML fixture",waits=clock.waits,attempts=2,rows=count)),encoding="utf-8")


def test_reports_calendar_source_display_values_not_reinterpreted(tmp_path,no_network):
    report=collect_input(input_id="ASTOCK-066",context=REPORTS_CALENDAR_CONTEXT,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=reports_calendar_fixture(tmp_path,"ASTOCK-066","display_values"))
    assert report["status"]=="candidate_complete",report
    id=str(read_artifact(report,"source_rows")[0]["id"]);row=next(row for row in read_artifact(report,"output") if row["source_event_id"]==id)
    assert [row[key+"_text"] for key in ("actual","forecast","previous","revised")]==["0","0","--"," null "]


def test_reports_calendar_yaml_and_scope(tmp_path,no_network):
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config);path=config/"normalization/macro_calendar.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"))
    rule["rules"][0]["field_mapping"]["title"]="country";path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    report=collect_input(input_id="ASTOCK-066",context=REPORTS_CALENDAR_CONTEXT,config_root=config,output_root=tmp_path/"candidate",replay_manifest=EVENT_ARCHIVE,
        fields=["snapshot_at","source_event_id","event_time","importance","title"])
    assert report["status"]=="candidate_complete" and read_artifact(report,"output")[0]["title"]==read_artifact(report,"parsed_rows")[0]["country"],report
    for id,bad in [("ASTOCK-013",{"metadata":{"page":2}}),("ASTOCK-013",{"request":{"symbol":"600519"}}),
        ("ASTOCK-066",{**REPORTS_CALENDAR_CONTEXT,"config":{"min_importance":0}}),("ASTOCK-066",{**REPORTS_CALENDAR_CONTEXT,"config":{"min_importance":True}})]:
        with pytest.raises(ValueError):collect_input(input_id=id,context=bad,config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)
    for context in [{"request":{"start_date":"2026-01-01","end_date":"2026-09-18"}},{**REPORTS_CALENDAR_CONTEXT,"config":{"country":"not a country"}}]:
        report=collect_input(input_id="ASTOCK-066",context=context,config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)
        assert report["status"]=="failed" and report["failure_class"]=="ValueError" and "output" not in report


def test_reports_calendar_direct_http_policy_and_cache(tmp_path,monkeypatch):
    from stock_data_manage.providers.sina import news as sina_news
    from stock_data_manage.storage.raw import sanitized_url
    from unittest.mock import patch
    seen=[]
    def send(session,request,**kwargs):
        record=next(record for id in ("ASTOCK-013","ASTOCK-066") for record in reports_calendar_records(id) if sanitized_url(record["url"])==sanitized_url(request.url));seen.append(request.url)
        assert session.trust_env is True and kwargs["timeout"]==(10,40) and kwargs["allow_redirects"] is True and session.get_adapter(request.url).max_retries.total==0
        assert request.headers["User-Agent"]==record["request_headers"]["User-Agent"]
        if "vReport_List" in request.url:assert request.headers["Referer"]=="https://finance.sina.com.cn/"
        response=requests.Response();response.status_code=200;response.encoding=record.get("response_encoding") or "utf-8";response._content=RawObjectStore.read_response(EVENT_ARCHIVE,record);response.url=request.url;return response
    with patch("requests.Session.send",send),patch.object(sina_news,"_report_clock",ReportTestClock()),patch.object(sina_news,"_report_last",[0.0]):
        for id,context,_,count in REPORTS_CALENDAR_CASES[:2]:
            first=collect_input(input_id=id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused",pacer=RequestPacer(clock=lambda:1000,wait=lambda n:None))
            second=collect_input(input_id=id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
            assert first["status"]==second["status"]=="candidate_complete" and first["row_count"]==count,(first,second)
            assert first["live_http_calls"]==(1 if id=="ASTOCK-013" else 3) and second["live_http_calls"]==0 and read_artifact(first,"output")==read_artifact(second,"output")
    assert len(seen)==4
    (tmp_path/"fixture-mode.json").write_text(json.dumps(dict(mode="injected Session and clock; no real live validation",real_http_calls=0,fixture_send_calls=4)),encoding="utf-8")

SINA_FUTURES_CASES = [("ASTOCK-074",{"request":{"contracts":["RB0","M0","IF0"]}},EVENT_ARCHIVE,3),
    ("ASTOCK-075",{"request":{"contract":"RB0","start_date":"2026-01-01","end_date":"2026-09-30"}},FUTURES_ARCHIVE,181),
    ("ASTOCK-075",{"request":{"contract":"M0","start_date":"2026-01-01","end_date":"2026-09-30"}},FUTURES_ARCHIVE,181),
    ("ASTOCK-076",{},EVENT_ARCHIVE,1)]


def sina_futures_record(input_id,symbol="RB0"):
    manifest=FUTURES_ARCHIVE if input_id=="ASTOCK-075" else EVENT_ARCHIVE
    needle="getDailyKLine?symbol="+symbol if input_id=="ASTOCK-075" else "list=nf_RB0,nf_M0,nf_IF0" if input_id=="ASTOCK-074" else "list=hf_CHA50CFD"
    return manifest,next(json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if needle in json.loads(line).get("url",""))


def compare_sina_futures_original(tmp_path,input_id,context,manifest,count):
    import pandas as pd
    spec=importlib.util.spec_from_file_location("sina_futures_original",ROOT/"provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);ns=module.load_shipped_code();store=RawObjectStore(tmp_path/"original")
    try:
        with captured_requests(store,provider="sina",endpoint="futures",scope=context,code_version="original-sina-futures",pacer=RequestPacer(),replay_manifest=manifest) as events:
            request=context.get("request",{})
            frame=ns["futures_realtime"](request["contracts"]) if input_id=="ASTOCK-074" else ns["a50_futures"]() if input_id=="ASTOCK-076" else ns["futures_kline"](request["contract"],start=request.get("start_date"),end=request.get("end_date"))
    finally:ns["EM_SESSION"].close()
    parsed=[{k:None if pd.isna(v) else v for k,v in row.items()} for row in frame.to_dict(orient="records")]
    original_path=tmp_path/"original-parsed.json";original_path.write_text(json.dumps(parsed,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==len(parsed)==count,report
    keys=("url","method","outcome","status_code","body_sha256","request_headers","request_options")
    assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    candidate=read_artifact(report,"parsed_rows");assert len(candidate)==len(parsed)
    assert candidate==[{k:old[k] for k in row} for row,old in zip(candidate,parsed)]
    rows=read_artifact(report,"output")
    for row,old in zip(rows,parsed):
        assert row["snapshot_at"]==report["source_capture_window"]["last"]
        for name,value in row.items():
            if name=="snapshot_at":continue
            if name in report["unverified_fields"]:assert value is None;continue
            if name=="series_identity":assert value=="main_continuous";continue
            if name=="source_contract" and input_id=="ASTOCK-076":assert value=="hf_CHA50CFD";continue
            expected=old[{"source_contract":"symbol","bar_date":"date","quote_time":"datetime"}.get(name,name)]
            if name=="quote_time":expected=expected.replace(" ","T")+"+08:00"
            assert value==expected
    source=read_artifact(report,"source_rows")
    if input_id=="ASTOCK-075":
        import re
        _,record=sina_futures_record(input_id,context["request"]["contract"])
        text=RawObjectStore.read_response(manifest,record).decode("gbk","replace")
        raw=json.loads(re.search(r'var _[A-Z0-9]+=\((.*)\);?\s*$',text,re.S).group(1));assert raw==source
        assert len(raw)==(4254 if context["request"]["contract"]=="RB0" else 5293)
        assert read_artifact(report,"excluded_rows")==[r for r in raw if not "2026-01-01"<=r["d"]<="2026-09-30"]
    else:
        import re
        _,record=sina_futures_record(input_id)
        text=RawObjectStore.read_response(manifest,record).decode("gbk","replace")
        assert source==[{"source_variable":key,"fields":body.split(",") if body else []} for key,body in re.findall(r'var hq_str_([^=]+)="([^"]*)"',text)]
    comparison={"input_id":input_id,"mode":"offline_replay","row_count":count,"all_business_fields_equal":True,"all_retained_source_fields_equal":True,
        "request_comparison_equal":True,"source_response_hashes":[r["body_sha256"] for r in events],"report_path":report["report_path"],
        "report_sha256":hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),"original_parsed_path":str(original_path.resolve()),
        "original_parsed_sha256":hashlib.sha256(original_path.read_bytes()).hexdigest()}
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count",SINA_FUTURES_CASES)
def test_sina_futures_original_contract(tmp_path,no_network,input_id,context,manifest,count):
    compare_sina_futures_original(tmp_path,input_id,context,manifest,count)


SINA_FUTURES_FAILURES=[("ASTOCK-074","structure","RuntimeError"),("ASTOCK-074","short","RuntimeError"),("ASTOCK-074","missing","ValueError"),
    ("ASTOCK-074","numeric","RuntimeError"),("ASTOCK-074","time","NormalizationError"),("ASTOCK-076","zero","RuntimeError"),
    ("ASTOCK-075","wrapper","RuntimeError"),("ASTOCK-075","null","ValueError"),("ASTOCK-075","duplicate","RuntimeError"),
    ("ASTOCK-075","date","RuntimeError"),("ASTOCK-075","numeric","RuntimeError"),("ASTOCK-075","settle_field","RuntimeError")]


def sina_futures_fixture(tmp_path,input_id,mutation):
    import re
    archive,record=sina_futures_record(input_id);text=RawObjectStore.read_response(archive,record).decode("gbk","replace");url=record["url"]
    if input_id=="ASTOCK-075":
        rows=json.loads(re.search(r'var _RB0=\((.*)\);?\s*$',text,re.S).group(1))
        row=next(r for r in rows if r["d"]=="2026-09-18")
        if mutation=="duplicate":rows.append(rows[0].copy())
        elif mutation=="date":rows[0]["d"]="bad"
        elif mutation=="numeric":row["o"]="bad"
        elif mutation=="settle_field":row.pop("s")
        elif mutation=="zero":row["s"]="0"
        elif mutation=="actual_contract":url=url.replace("RB0","RB2610")
        elif mutation=="empty_window":pass
        text="var _"+("RB2610" if mutation=="actual_contract" else "WRONG" if mutation=="wrapper" else "RB0")+"=("+("null" if mutation=="null" else json.dumps(rows))+");"
    else:
        data={key:body.split(",") if body else [] for key,body in re.findall(r'var hq_str_([^=]+)="([^"]*)"',text)}
        key="hf_CHA50CFD" if input_id=="ASTOCK-076" else "nf_RB0";fields=data[key]
        if mutation=="short":data[key]=fields[:10]
        elif mutation=="missing":data[key]=[]
        elif mutation=="numeric":fields[8]="bad"
        elif mutation=="time":fields[17]="bad"
        elif mutation=="zero":fields[0]="0"
        elif mutation=="zero_fields":fields[2]="0";fields[14]="0"
        text="<html>changed</html>" if mutation=="structure" else "\n".join(f'var hq_str_{key}="{",".join(values)}";' for key,values in data.items())
    response=requests.Response();response.status_code=200;response.encoding="gbk";response._content=text.encode("gbk")
    store=RawObjectStore(tmp_path/"fixture");store.record_response(response=response,url=url,method="GET",request_headers=record["request_headers"],provider="sina",endpoint="futures",code_version="offline-fixture",
        scope={"synthetic":True,"mutation":mutation,"parent_sha256":record["body_sha256"]})
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation,expected",SINA_FUTURES_FAILURES)
def test_sina_futures_invalid_response_retains_evidence(tmp_path,no_network,input_id,mutation,expected):
    manifest=sina_futures_fixture(tmp_path,input_id,mutation);case=next(c for c in SINA_FUTURES_CASES if c[0]==input_id)
    report=collect_input(input_id=input_id,context=case[1],config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="failed" and report["failure_class"]==expected and "output" not in report,report
    assert report["responses"] and report["production_writes"]==report["live_http_calls"]==0


def test_sina_futures_zero_price_quantity_and_contract_identity(tmp_path,no_network):
    for input_id,mutation in [("ASTOCK-074","zero_fields"),("ASTOCK-075","zero"),("ASTOCK-075","actual_contract")]:
        manifest=sina_futures_fixture(tmp_path/mutation,input_id,mutation);case=next(c for c in SINA_FUTURES_CASES if c[0]==input_id);context=case[1]
        if mutation=="actual_contract":context={"request":{"contract":"RB2610","start_date":"2026-09-18","end_date":"2026-09-18"}}
        report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/mutation/"candidate",replay_manifest=manifest)
        assert report["status"]=="candidate_complete",report
        parsed=read_artifact(report,"parsed_rows")
        if mutation=="zero_fields":assert parsed[0]["open"] is None and parsed[0]["volume"]==0
        elif mutation=="zero":assert next(r for r in parsed if r["date"]=="2026-09-18")["settle"] is None
        else:assert report["series_identity"]=="contract" and read_artifact(report,"output")[0]["source_contract"]=="RB2610"


def test_sina_futures_yaml_and_date_window_scope(tmp_path,no_network):
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    path=config/"normalization/futures_quote.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"));rule["rules"][0]["field_mapping"]["name"]="symbol"
    path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    report=collect_input(input_id="ASTOCK-074",context=SINA_FUTURES_CASES[0][1],config_root=config,output_root=tmp_path/"candidate",replay_manifest=EVENT_ARCHIVE,
        fields=["snapshot_at","source_contract","quote_time","name"])
    assert report["status"]=="candidate_complete" and read_artifact(report,"output")[0]["name"]=="RB0",report
    empty=collect_input(input_id="ASTOCK-075",context={"request":{"contract":"RB0","start_date":"2030-01-01"}},config_root=ROOT/"config",output_root=tmp_path/"empty",replay_manifest=FUTURES_ARCHIVE)
    assert empty["status"]=="failed" and empty["failure_class"]=="ValueError" and "output" not in empty,empty
    for input_id,context in [("ASTOCK-074",{"request":{"contracts":["bad" ]}}),("ASTOCK-076",{"request":{"as_of":"2026-09-30"}})]:
        try:
            report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)
        except ValueError:continue
        assert report["status"]=="failed" and report["failure_class"]=="ValueError" and not report.get("responses"),report


def test_sina_futures_original_http_and_cache(tmp_path,monkeypatch):
    from unittest.mock import patch
    from stock_data_manage.storage.raw import sanitized_url
    seen=[]
    def send(session,request,**kwargs):
        pair=next((sina_futures_record(case[0],case[1].get("request",{}).get("contract","RB0")) for case in SINA_FUTURES_CASES
            if sanitized_url(sina_futures_record(case[0],case[1].get("request",{}).get("contract","RB0"))[1]["url"])==sanitized_url(request.url)))
        archive,record=pair;seen.append(request.url)
        assert session.trust_env and kwargs["timeout"]==(10,40) and kwargs["allow_redirects"] is True
        assert session.get_adapter(request.url).max_retries.total==0 and request.headers["Referer"]=="https://finance.sina.com.cn/"
        assert request.headers["User-Agent"]==record["request_headers"]["User-Agent"]
        response=requests.Response();response.status_code=200;response.encoding="gbk";response.headers["Content-Type"]=record["content_type"]
        response._content=RawObjectStore.read_response(archive,record);response.url=request.url;return response
    with patch("requests.Session.send",send):
        for input_id,context,_,count in SINA_FUTURES_CASES:
            first=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
            second=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
            assert first["status"]==second["status"]=="candidate_complete" and first["row_count"]==count,(first,second)
            assert first["live_http_calls"]==1 and second["live_http_calls"]==0 and read_artifact(first,"output")==read_artifact(second,"output")
    assert len(seen)==4
    (tmp_path/"fixture-mode.json").write_text(json.dumps({"mode":"injected Session; not real live source validation","real_http_calls":0,"fixture_send_calls":4}),encoding="utf-8")


def test_sina_existing_stock_methods_unchanged(tmp_path,no_network):
    import sys,types
    from dataclasses import asdict
    from stock_data_manage.providers.sina import SinaDailyProvider,SinaSnapshotProvider
    from stock_data_manage.config.loader import load_provider_configs
    from stock_data_manage.providers.contracts import HttpResponse
    capability=next(c.capability() for c in load_provider_configs(ROOT/"config/providers.yaml") if c.provider=="sina")
    class Transport:
        def __init__(self,snapshot):self.snapshot=snapshot;self.calls=[]
        def get(self,url,params,timeout_seconds):
            self.calls.append({"url":url,"params":params,"timeout":timeout_seconds})
            body='var hq_str_sh600519="样本,10,9,11,12,8,0,0,100,200,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-09-18,15:00:00";' if self.snapshot else json.dumps([{"day":"2026-09-18","open":"10","high":"12","low":"8","close":"11","volume":"100","amount":"200"}])
            return HttpResponse(200,{"Content-Type":"text/plain" if self.snapshot else "application/json"},body.encode())
    proof=[]
    for index,(cls,filename,snapshot) in enumerate([(SinaSnapshotProvider,"1-snapshot.py.bin",True),(SinaDailyProvider,"2-daily.py.bin",False)]):
        path=ROOT/"provider_validation/results/sina-futures-original-20261004"/filename;name="stock_data_manage.providers.sina.old_fixture"+str(index);module=types.ModuleType(name);module.__package__="stock_data_manage.providers.sina";sys.modules[name]=module
        try:exec(compile(path.read_bytes(),str(path),"exec"),module.__dict__)
        finally:sys.modules.pop(name,None)
        old_transport,new_transport=Transport(snapshot),Transport(snapshot)
        old_cls=getattr(module,cls.__name__);old=old_cls(old_transport,capability) if snapshot else old_cls(old_transport);new=cls(new_transport,capability) if snapshot else cls(new_transport)
        before=old.fetch_snapshot(["sh600519"],datetime(2026,9,18)) if snapshot else old.fetch_daily(["sh600519"],date(2026,9,18))
        after=new.fetch_snapshot(["sh600519"],datetime(2026,9,18)) if snapshot else new.fetch_daily(["sh600519"],date(2026,9,18))
        assert asdict(before)==asdict(after) and old_transport.calls==new_transport.calls
        proof.append({"method":"stock snapshot" if snapshot else "stock daily","original_code_sha256":hashlib.sha256(path.read_bytes()).hexdigest(),"request_and_result_equal":True})
    tmp_path.mkdir(parents=True,exist_ok=True);(tmp_path/"legacy-comparison.json").write_text(json.dumps(proof,indent=2),encoding="utf-8")
NEWS_CASES = [("ASTOCK-034", {"config":{"channel":"a-stock-channel","limit":50}}, EVENT_ARCHIVE, 50),
              ("ASTOCK-035", {"request":{"trade_date":"2026-09-18"},"config":{"with_content":False}}, EVENT_ARCHIVE, 14)]
RATES_BONDS_CASES = [("ASTOCK-064",{},EVENT_ARCHIVE,747),("ASTOCK-084",{},EVENT_ARCHIVE,322)]


def rates_bonds_records(input_id):
    needle="frr-chrt.csv" if input_id=="ASTOCK-064" else "reportName=RPT_BOND_CB_LIST"
    return [json.loads(line) for line in EVENT_ARCHIVE.read_text(encoding="utf-8").splitlines() if needle in json.loads(line).get("url","")]


def compare_rates_bonds_original(tmp_path,input_id,context,manifest,count):
    import pandas as pd
    from unittest.mock import patch
    spec=importlib.util.spec_from_file_location("rates_bonds_original",ROOT/"provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);ns=module.load_shipped_code()
    stamp=datetime.fromisoformat(rates_bonds_records(input_id)[-1]["fetched_at_utc"])
    class SourceClock(datetime):
        @classmethod
        def now(cls,tz=None):return stamp.astimezone(tz) if tz else stamp.replace(tzinfo=None)
    store=RawObjectStore(tmp_path/"original")
    try:
        with patch.dict(ns,{"datetime":SourceClock}),captured_requests(store,provider="chinamoney" if input_id=="ASTOCK-064" else "eastmoney",endpoint="rates-bonds",scope=context,code_version="original-v39-rates-bonds",pacer=RequestPacer(),replay_manifest=manifest) as events:
            frame=ns["repo_fixing_rates"]("FR") if input_id=="ASTOCK-064" else ns["convertible_bonds"]()
    finally:ns["EM_SESSION"].close()
    parsed=[{k:None if pd.isna(v) else v for k,v in row.items()} for row in frame.to_dict(orient="records")]
    original_path=tmp_path/"original-parsed.json";original_path.write_text(json.dumps(parsed,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==len(parsed)==count,report
    keys=("url","method","outcome","status_code","body_sha256","request_headers","request_options")
    assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    candidate_parsed=read_artifact(report,"parsed_rows")
    assert candidate_parsed==[{name:old[name] for name in candidate.keys()} for candidate,old in zip(candidate_parsed,parsed)]
    assert len(candidate_parsed)==len(parsed)
    rows=read_artifact(report,"output")
    for row,old in zip(rows,parsed):
        assert row["snapshot_at"]==report["source_capture_window"]["last"]
        for name,value in row.items():
            if name=="snapshot_at":continue
            if name=="rate_kind":assert value=="FR";continue
            if name in report["unverified_fields"]:assert value is None;continue
            assert value==old[{"rate_date":"date","source_bond_code":"code"}.get(name,name)]
    source=read_artifact(report,"source_rows")
    if input_id=="ASTOCK-064":
        lines=[line.split(",") for line in RawObjectStore.read_response(manifest,rates_bonds_records(input_id)[0]).decode("utf-8-sig").splitlines() if line.strip()]
        assert len(lines)==len(source)==747
        assert source==[{"date":parts[0],"FR001":parts[6],"FR007":parts[7],"FR014":parts[8]} for parts in lines]
        original_by_date={row["date"]:row for row in parsed}
        for row in source:
            assert all(float(row[name])==original_by_date[row["date"]][name] for name in ("FR001","FR007","FR014"))
    else:
        raw=[]
        for record in rates_bonds_records(input_id):raw.extend(json.loads(RawObjectStore.read_response(manifest,record))["result"]["data"])
        assert source==raw and len(raw)==report["source_total_count"]==1059
        assert report["classification_reference_date"]=="2026-10-01" and report["excluded_rows"]["row_count"]==737
        by_code={row["SECURITY_CODE"]:row for row in source}
        assert {row["SECURITY_CODE"] for row in read_artifact(report,"excluded_rows")}==set(by_code)-{row["code"] for row in parsed}
        numeric={"issue_size_100m":"ACTUAL_ISSUE_SCALE","initial_convert_price":"INITIAL_TRANSFER_PRICE","convert_price":"TRANSFER_PRICE",
            "bond_price":"CURRENT_BOND_PRICE","stock_price":"CONVERT_STOCK_PRICE","convert_value":"TRANSFER_VALUE","premium_pct":"TRANSFER_PREMIUM_RATIO"}
        for old in parsed:
            for name,key in numeric.items():
                value=by_code[old["code"]].get(key)
                expected=None if value in (None,"","-","--") else float(value)
                assert old[name]==expected
    comparison={"input_id":input_id,"mode":"offline_replay","row_count":count,"all_business_fields_equal":True,
        "all_retained_source_fields_equal":True,"request_comparison_equal":True,
        "source_response_hashes":[r["body_sha256"] for r in events],"report_path":report["report_path"],
        "report_sha256":hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        "original_parsed_path":str(original_path.resolve()),"original_parsed_sha256":hashlib.sha256(original_path.read_bytes()).hexdigest()}
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count",RATES_BONDS_CASES)
def test_rates_bonds_preserve_original_fields_requests_and_clock(tmp_path,no_network,input_id,context,manifest,count):
    compare_rates_bonds_original(tmp_path,input_id,context,manifest,count)


RATES_BONDS_FAILURES=[("ASTOCK-064","layout"),("ASTOCK-064","spacer"),("ASTOCK-064","date"),
    ("ASTOCK-064","numeric"),("ASTOCK-064","empty_rate"),("ASTOCK-064","nonfinite"),("ASTOCK-064","duplicate"),("ASTOCK-064","empty"),
    ("ASTOCK-084","date"),("ASTOCK-084","numeric"),("ASTOCK-084","code"),("ASTOCK-084","duplicate"),
    ("ASTOCK-084","short_page"),("ASTOCK-084","count_changed"),("ASTOCK-084","empty")]


def rates_bonds_fixture(tmp_path,input_id,mutation):
    store=RawObjectStore(tmp_path/"fixture")
    for page,record in enumerate(rates_bonds_records(input_id),1):
        body=RawObjectStore.read_response(EVENT_ARCHIVE,record)
        if input_id=="ASTOCK-064":
            lines=[line for line in body.decode("utf-8-sig").splitlines() if line.strip()];parts=lines[0].split(",")
            if mutation=="layout":parts.pop()
            elif mutation=="spacer":parts[2]="changed"
            elif mutation=="date":parts[0]="bad"
            elif mutation=="numeric":parts[6]="bad"
            elif mutation=="empty_rate":parts[6]=""
            elif mutation=="nonfinite":parts[6]="NaN"
            elif mutation=="zero_negative":parts[6:9]=["0","-0.25","1.5"]
            lines[0]=",".join(parts)
            if mutation=="duplicate":lines[-1]=lines[0]
            if mutation=="empty":lines=[]
            body=("\ufeff\n"+"\n".join(lines)).encode()
        else:
            payload=json.loads(body);rows=payload["result"]["data"]
            if mutation=="all_delisted":
                for row in rows:row["TRADE_MARKET"]="STAS00"
            if page==1:
                if mutation=="empty":payload["code"]=9201;payload["result"]=None
                elif mutation=="date":rows[0]["LISTING_DATE"]="bad"
                elif mutation=="numeric":rows[0]["TRANSFER_PRICE"]="bad"
                elif mutation=="code":rows[0].pop("SECURITY_CODE")
                elif mutation=="duplicate":rows[1]=rows[0].copy()
                elif mutation=="short_page":rows.pop()
                elif mutation=="statuses":
                    rows[0]["LISTING_DATE"]="2026-10-02 00:00:00";rows[0]["DELIST_DATE"]="2026-10-20 00:00:00"
                    rows[1]["TRADE_MARKET"]="unrecognized-market";rows[2]["TRADE_MARKET"]="STAS00"
            if page==2 and mutation=="count_changed":payload["result"]["count"]+=1
            body=json.dumps(payload).encode()
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=body
        store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],
            provider="chinamoney" if input_id=="ASTOCK-064" else "eastmoney",endpoint="rates-bonds",code_version="offline-fixture",
            fetched_at=datetime.fromisoformat(record["fetched_at_utc"]),
            scope={"synthetic":True,"mutation":mutation,"clock":"original archive capture time for offline simulation",
                "validation_time_utc":datetime.now(timezone.utc).isoformat(),"parent_sha256":record["body_sha256"]})
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation",RATES_BONDS_FAILURES)
def test_rates_bonds_failures_retain_evidence(tmp_path,no_network,input_id,mutation):
    manifest=rates_bonds_fixture(tmp_path,input_id,mutation)
    report=collect_input(input_id=input_id,context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="failed" and report["failure_class"]=="RuntimeError" and "output" not in report,report
    assert report["responses"] and report["production_writes"]==report["live_http_calls"]==0


def test_rates_bonds_include_delisted_and_future_status_semantics(tmp_path,no_network):
    rate_manifest=rates_bonds_fixture(tmp_path/"zero_negative","ASTOCK-064","zero_negative")
    rates=collect_input(input_id="ASTOCK-064",context={},config_root=ROOT/"config",output_root=tmp_path/"zero_negative"/"candidate",replay_manifest=rate_manifest)
    assert rates["status"]=="candidate_complete",rates
    latest=read_artifact(rates,"parsed_rows")[-1];assert latest["FR001"]==0 and latest["FR007"]==-0.25 and latest["FR014"]==1.5
    full=collect_input(input_id="ASTOCK-084",context={"config":{"include_delisted":True}},config_root=ROOT/"config",output_root=tmp_path/"all",replay_manifest=EVENT_ARCHIVE)
    assert full["status"]=="candidate_complete" and full["row_count"]==1059 and full["excluded_rows"]["row_count"]==0,full
    rows=read_artifact(full,"output");assert sum(row["status"]=="delisted" for row in rows)==737
    for mutation in ["statuses","all_delisted"]:
        manifest=rates_bonds_fixture(tmp_path/mutation,"ASTOCK-084",mutation)
        report=collect_input(input_id="ASTOCK-084",context={},config_root=ROOT/"config",output_root=tmp_path/mutation/"candidate",replay_manifest=manifest)
        assert report["status"]=="candidate_complete" and report["classification_reference_date"]=="2026-10-01",report
        if mutation=="all_delisted":assert report["row_count"]==0 and report["excluded_rows"]["row_count"]==1059
        else:
            source=json.loads(RawObjectStore.read_response(manifest,json.loads(manifest.read_text().splitlines()[0])))["result"]["data"]
            selected={r["source_bond_code"]:r for r in read_artifact(report,"output")}
            assert selected[source[0]["SECURITY_CODE"]]["status"]=="upcoming" and selected[source[1]["SECURITY_CODE"]]["status"]=="unknown"
            assert source[2]["SECURITY_CODE"] not in selected


def test_rates_bonds_yaml_projection_and_scope(tmp_path,no_network):
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    path=config/"normalization/convertible_bonds.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"));rule["rules"][0]["field_mapping"]["name"]="stock_name"
    path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    report=collect_input(input_id="ASTOCK-084",context={},config_root=config,output_root=tmp_path/"candidate",replay_manifest=EVENT_ARCHIVE,
        fields=["snapshot_at","source_bond_code","status","name"])
    assert report["status"]=="candidate_complete",report
    rows=read_artifact(report,"output");source=read_artifact(report,"source_rows");assert rows[0]["name"]==source[0]["SECURITY_SHORT_NAME"] and len(rows[0])==4
    with pytest.raises(ValueError):collect_input(input_id="ASTOCK-064",context={"config":{"kind":"FDR"}},config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)
    for input_id in ["ASTOCK-064","ASTOCK-084"]:
        for context in [{"request":{"trade_date":"2026-10-01"}},{"config":{"limit":50}},{"request":{"as_of":"2026-10-01"}}]:
            with pytest.raises(ValueError):collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)


def test_rates_bonds_http_policy_and_cache(tmp_path,monkeypatch):
    from unittest.mock import patch
    from stock_data_manage.storage.raw import sanitized_url
    originals=[r for case in RATES_BONDS_CASES for r in rates_bonds_records(case[0])];seen=[];bond_sessions=[]
    def send(session,request,**kwargs):
        record=next(r for r in originals if sanitized_url(r["url"])==sanitized_url(request.url));seen.append(request.url)
        assert session.trust_env is True
        if "frr-chrt.csv" in request.url:
            assert kwargs["timeout"]==(10,40) and request.headers["Referer"]=="https://www.chinamoney.com.cn/chinese/bkfrr/"
            assert session.get_adapter(request.url).max_retries.total==0
        else:
            bond_sessions.append(session);assert kwargs["timeout"]==20 and session.get_adapter(request.url).max_retries.total==3
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response.headers["Content-Type"]=record["content_type"]
        response._content=RawObjectStore.read_response(EVENT_ARCHIVE,record);response.url=request.url;return response
    with patch("requests.Session.send",send):
        for input_id,context,_,_ in RATES_BONDS_CASES:
            first=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
            second=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
            assert first["status"]==second["status"]=="candidate_complete",(first,second)
            assert first["live_http_calls"]==(1 if input_id=="ASTOCK-064" else 3) and second["live_http_calls"]==0
            assert read_artifact(first,"output")==read_artifact(second,"output")
    assert len(seen)==4 and len({id(session) for session in bond_sessions})==1
    (tmp_path/"fixture-mode.json").write_text(json.dumps({"mode":"injected Session; not real live source validation","real_http_calls":0,"fixture_send_calls":4}),encoding="utf-8")


def test_cb_replay_clock_uses_actual_matched_pages(tmp_path,no_network):
    store=RawObjectStore(tmp_path/"fixture")
    for later in (False,True):
        for record in rates_bonds_records("ASTOCK-084"):
            response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=RawObjectStore.read_response(EVENT_ARCHIVE,record)
            store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],provider="eastmoney",endpoint="convertible_bonds",code_version="offline-fixture",
                fetched_at=datetime(2027,12,31,tzinfo=timezone.utc) if later else datetime.fromisoformat(record["fetched_at_utc"]),
                scope={"synthetic":True,"scenario":"later unrelated capture with identical query", "later":later,"parent_sha256":record["body_sha256"]})
    report=collect_input(input_id="ASTOCK-084",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=store.root/"manifest.ndjson")
    assert report["status"]=="candidate_complete" and report["row_count"]==322 and report["classification_reference_date"]=="2026-10-01",report
    assert len(report["responses"])==3 and all(r["source_ref"]["line"]<=3 for r in report["responses"])


def news_source_record(input_id):
    needle="/content/lives?" if input_id=="ASTOCK-034" else "/lm/xwlb/day/20260918.shtml"
    return next(json.loads(line) for line in EVENT_ARCHIVE.read_text(encoding="utf-8").splitlines() if needle in json.loads(line).get("url",""))


def compare_news_original(tmp_path,input_id,context,manifest,count):
    spec=importlib.util.spec_from_file_location("news_original",ROOT/"provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);ns=module.load_shipped_code()
    store=RawObjectStore(tmp_path/"original")
    try:
        with captured_requests(store,provider="wallstreetcn" if input_id=="ASTOCK-034" else "cctv",endpoint="news",scope=context,code_version="original-v39-news",pacer=RequestPacer(),replay_manifest=manifest) as events:
            frame=ns["wallstreetcn_lives"](channel="a-stock-channel",limit=50) if input_id=="ASTOCK-034" else ns["cctv_news"]("2026-09-18",with_content=False)
    finally:ns["EM_SESSION"].close()
    parsed=frame.to_dict(orient="records");original_path=tmp_path/"original-parsed.json"
    original_path.write_text(json.dumps(parsed,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==len(parsed)==count,report
    keys=("url","method","outcome","status_code","body_sha256","request_headers","request_options")
    assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    output=read_artifact(report,"output")
    for row,old in zip(output,parsed):
        assert row["snapshot_at"]==report["source_capture_window"]["last"]
        if input_id=="ASTOCK-034":
            assert row["source_news_id"]==str(old["id"]) and row["news_time"]==old["time"].replace(" ","T")+"+08:00"
            assert all(row[name]==old[name] for name in ("title","content","importance","channels","url"))
        else:
            assert row["broadcast_date"]==old["date"] and row["title"]==old["title"] and row["url"]==old["url"]
    if input_id=="ASTOCK-034":
        payload=json.loads(RawObjectStore.read_response(manifest,news_source_record(input_id)))
        assert read_artifact(report,"source_rows")==payload["data"]["items"] and report["next_cursor"]==frame.attrs["next_cursor"]
        assert not report["pagination_completeness_verified"]
    else:
        assert read_artifact(report,"source_rows")==[{name:row[name] for name in ("date","title","url")} for row in parsed]
        assert report["content_requests"]==0 and report["article_content_verified"] is False
    comparison={"input_id":input_id,"mode":"offline_replay","row_count":count,"all_business_fields_equal":True,
        "all_retained_source_fields_equal":True,"request_comparison_equal":True,
        "source_response_hashes":[r["body_sha256"] for r in events],"report_path":report["report_path"],
        "report_sha256":hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        "original_parsed_path":str(original_path.resolve()),"original_parsed_sha256":hashlib.sha256(original_path.read_bytes()).hexdigest()}
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count",NEWS_CASES)
def test_news_preserves_original_parser_and_request(tmp_path,no_network,input_id,context,manifest,count):
    compare_news_original(tmp_path,input_id,context,manifest,count)


NEWS_FAILURES=[("ASTOCK-034","business","RuntimeError"),("ASTOCK-034","empty","RuntimeError"),
    ("ASTOCK-034","json","RuntimeError"),("ASTOCK-034","time","RuntimeError"),("ASTOCK-034","channels","RuntimeError"),
    ("ASTOCK-034","id","RuntimeError"),("ASTOCK-034","score","NormalizationError"),("ASTOCK-034","duplicate","NormalizationError"),
    ("ASTOCK-034","http429","RuntimeError"),("ASTOCK-035","missing","ValueError"),
    ("ASTOCK-035","structure","RuntimeError"),("ASTOCK-035","duplicate","NormalizationError")]


def news_fixture(tmp_path,input_id,mutation):
    record=news_source_record(input_id);body=RawObjectStore.read_response(EVENT_ARCHIVE,record);status=200
    if input_id=="ASTOCK-034":
        data=json.loads(body)
        if mutation=="business":data["code"]=50000
        elif mutation=="empty":data["data"]["items"]=[]
        elif mutation=="time":data["data"]["items"][0]["display_time"]=True
        elif mutation=="channels":data["data"]["items"][0]["channels"]="a-stock-channel"
        elif mutation=="id":data["data"]["items"][0].pop("id")
        elif mutation=="score":data["data"]["items"][0]["score"]=True
        elif mutation=="duplicate":data["data"]["items"][-1]=data["data"]["items"][0].copy()
        elif mutation=="cursor":data["data"]["next_cursor"]="synthetic-cursor"
        elif mutation=="http429":status=429
        body=b"<html>invalid</html>" if mutation=="json" else json.dumps(data).encode()
    elif mutation=="missing":status=404;body=b"<html>not published</html>"
    elif mutation=="structure":body=b"<html>source changed</html>"
    elif mutation=="duplicate":body+=body
    elif mutation=="old_title":body='<li><a href="//tv.cctv.com/2026/09/19/VIDE-example.shtml"><div class="title">[视频]样本新闻 &amp; 提示</div></a></li><li><a href="//tv.cctv.com/2026/09/19/VIDE-full.shtml" title="新闻联播完整版"></a></li>'.encode()
    url=record["url"]
    if mutation=="cursor":url+="&cursor=synthetic-cursor"
    if mutation=="old_title":url=url.replace("20260918","20260919")
    response=requests.Response();response.status_code=status;response.encoding=record["response_encoding"];response._content=body
    store=RawObjectStore(tmp_path/"fixture");store.record_response(response=response,url=url,method="GET",request_headers=record["request_headers"],
        provider="wallstreetcn" if input_id=="ASTOCK-034" else "cctv",endpoint="news",code_version="offline-fixture",
        scope={"synthetic":True,"mutation":mutation,"parent_sha256":record["body_sha256"]})
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("input_id,mutation,expected",NEWS_FAILURES)
def test_news_failures_retain_evidence(tmp_path,no_network,input_id,mutation,expected):
    case=next(case for case in NEWS_CASES if case[0]==input_id);manifest=news_fixture(tmp_path,input_id,mutation)
    report=collect_input(input_id=input_id,context=case[1],config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="failed" and report["failure_class"]==expected and "output" not in report,report
    assert report["responses"] and report["live_http_calls"]==report["production_writes"]==0


def test_news_yaml_projection_scope_and_mapping(tmp_path,no_network):
    input_id,context,manifest,_=NEWS_CASES[0];config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    path=config/"normalization/wallstreetcn_news.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"))
    rule["rules"][0]["field_mapping"]["title"]="content";path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    report=collect_input(input_id=input_id,context=context,config_root=config,output_root=tmp_path/"candidate",replay_manifest=manifest,
        fields=["snapshot_at","source_news_id","news_time","title"])
    assert report["status"]=="candidate_complete",report
    source=json.loads(RawObjectStore.read_response(manifest,news_source_record(input_id)))["data"]["items"]
    rows=read_artifact(report,"output");assert rows[0]["title"]==source[0]["content_text"].strip() and len(rows[0])==4
    for bad in [{"config":{"channel":"global"}},{"config":{"channel":"global-channel"}},{"config":{"limit":101}},{"config":{"limit":True}},{"request":{"symbol":"600519"}}]:
        with pytest.raises(ValueError):collect_input(input_id=input_id,context=bad,config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=manifest)
    with pytest.raises(ValueError):collect_input(input_id="ASTOCK-035",context={"request":{"trade_date":"2026-09-18"},"config":{"with_content":True}},config_root=ROOT/"config",output_root=tmp_path/"bad",replay_manifest=manifest)


def test_news_cursor_and_weekend_broadcast_preserve_source_semantics(tmp_path,no_network):
    manifest=news_fixture(tmp_path/"cursor","ASTOCK-034","cursor")
    report=collect_input(input_id="ASTOCK-034",context={"metadata":{"cursor":"synthetic-cursor"}},config_root=ROOT/"config",output_root=tmp_path/"cursor-candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["next_cursor"]=="synthetic-cursor",report
    manifest=news_fixture(tmp_path/"weekend","ASTOCK-035","old_title")
    report=collect_input(input_id="ASTOCK-035",context={"request":{"trade_date":"2026-09-19"}},config_root=ROOT/"config",output_root=tmp_path/"weekend-candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==1,report
    row=read_artifact(report,"output")[0];assert row["broadcast_date"]=="2026-09-19" and row["title"]=="样本新闻 & 提示" and row["url"].startswith("https://")


def test_news_original_http_policy_and_cache(tmp_path,monkeypatch):
    from unittest.mock import patch
    originals=[news_source_record(case[0]) for case in NEWS_CASES];seen=[]
    def send(session,request,**kwargs):
        record=next(record for record in originals if record["url"]==request.url);seen.append(request.url)
        assert session.trust_env is True and kwargs["timeout"]==(10,40) and kwargs["allow_redirects"] is True
        assert session.get_adapter(request.url).max_retries.total==0 and request.headers["User-Agent"]==record["request_headers"]["User-Agent"]
        response=requests.Response();response.status_code=200;response.encoding=record["response_encoding"];response.headers["Content-Type"]=record["content_type"]
        response._content=RawObjectStore.read_response(EVENT_ARCHIVE,record);response.url=request.url;return response
    with patch("requests.Session.send",send):
        for input_id,context,_,count in NEWS_CASES:
            first=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
            second=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
            assert first["status"]==second["status"]=="candidate_complete" and first["row_count"]==count,(first,second)
            assert first["live_http_calls"]==1 and second["live_http_calls"]==0 and read_artifact(first,"output")==read_artifact(second,"output")
    assert len(seen)==2
    (tmp_path/"fixture-mode.json").write_text(json.dumps({"mode":"injected Session; not real live source validation","real_http_calls":0,"fixture_send_calls":2}),encoding="utf-8")
EVENT_CASES = [("ASTOCK-078", {"config": {"limit": 50}}, EVENT_ARCHIVE, 50),
               ("ASTOCK-079", {"config": {"limit": 50}}, EVENT_ARCHIVE, 50)]
EVENT_REPORTS = {"ASTOCK-078": "RPT_PUBLIC_OP_NEWPREDICT", "ASTOCK-079": "RPT_ORG_SURVEYNEW"}
ACTION_CASES = [("ASTOCK-080", {"config":{"limit":50}}, EVENT_ARCHIVE, 50),
                ("ASTOCK-081", {"config":{"limit":50}}, EVENT_ARCHIVE, 50),
                ("ASTOCK-082", {"config":{"limit":50}}, EVENT_ARCHIVE, 50),
                ("ASTOCK-083", {"config":{"limit":30}}, EVENT_ARCHIVE, 30)]
ACTION_REPORTS = {"ASTOCK-080":"RPT_SHARE_HOLDER_INCREASE", "ASTOCK-081":"RPTA_WEB_GETHGLIST_NEW",
                  "ASTOCK-082":"RPT_CSDC_LIST", "ASTOCK-083":"RPTA_APP_IPOAPPLY"}
ACTION_FUNCTIONS = {"ASTOCK-080":"holder_trades", "ASTOCK-081":"share_buyback", "ASTOCK-082":"equity_pledge", "ASTOCK-083":"ipo_calendar"}


def lpr_source_records():
    from urllib.parse import parse_qs,urlsplit
    return [json.loads(line) for line in EVENT_ARCHIVE.read_text(encoding="utf-8").splitlines()
            if parse_qs(urlsplit(json.loads(line)["url"]).query).get("reportName")==["RPTA_WEB_RATE"]]


def compare_lpr_original(tmp_path):
    import pandas as pd
    spec=importlib.util.spec_from_file_location("lpr_original",ROOT/"provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);ns=module.load_shipped_code()
    store=RawObjectStore(tmp_path/"original")
    try:
        with captured_requests(store,provider="eastmoney",endpoint="lpr",scope={},code_version="original-v39-lpr",pacer=RequestPacer(),replay_manifest=EVENT_ARCHIVE) as events:
            frame=ns["lpr_history"]()
    finally:ns["EM_SESSION"].close()
    parsed=[{k:None if pd.isna(v) else v for k,v in row.items()} for row in frame.to_dict(orient="records")]
    original_path=tmp_path/"original-parsed.json";original_path.write_text(json.dumps(parsed,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    report=collect_input(input_id="ASTOCK-065",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=EVENT_ARCHIVE)
    assert report["status"]=="candidate_complete",report
    source=[]
    for record in lpr_source_records():source.extend(json.loads(RawObjectStore.read_response(EVENT_ARCHIVE,record))["result"]["data"])
    assert read_artifact(report,"source_rows")==source and len(source)==report["original_row_count"]==1576
    selected=[r for r in source if r.get("LPR1Y") is not None];excluded=[r for r in source if r.get("LPR1Y") is None]
    assert len(selected)==len(parsed)==report["row_count"]==1538 and len(excluded)==38
    assert read_artifact(report,"excluded_rows")==excluded and report["source_total_count"]==1576 and not report["result_limited"]
    for row,old in zip(selected,parsed):
        assert row["TRADE_DATE"][:10]==old["date"] and float(row["LPR1Y"])==old["lpr_1y"]
        assert row.get("LPR5Y")==old["lpr_5y"]
    keys=("url","method","outcome","status_code","body_sha256","request_headers","request_options")
    assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    output=read_artifact(report,"output")
    assert [r["rate_date"] for r in output]==[r["date"] for r in parsed]
    assert all(r["snapshot_at"]==report["source_capture_window"]["last"] and r["lpr_1y"] is None and r["lpr_5y"] is None for r in output)
    comparison={"input_id":"ASTOCK-065","mode":"offline_replay","source_row_count":1576,"row_count":1538,"excluded_row_count":38,
        "all_business_fields_equal":True,"all_retained_source_fields_equal":True,"request_comparison_equal":True,
        "source_response_hashes":[r["body_sha256"] for r in events],"report_path":report["report_path"],
        "report_sha256":hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        "original_parsed_path":str(original_path.resolve()),"original_parsed_sha256":hashlib.sha256(original_path.read_bytes()).hexdigest()}
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


def test_lpr_preserves_original_history_and_exclusions(tmp_path,no_network):
    compare_lpr_original(tmp_path)


def lpr_fixture(tmp_path,mutation):
    store=RawObjectStore(tmp_path/"fixture")
    for page,record in enumerate(lpr_source_records(),1):
        body=json.loads(RawObjectStore.read_response(EVENT_ARCHIVE,record));data=body["result"]["data"]
        target=next((r for r in data if r.get("LPR1Y") is not None),None)
        if page==1:
            if mutation=="numeric":target["LPR1Y"]="broken"
            elif mutation=="missing_required":target["LPR1Y"]="--"
            elif mutation=="date":target["TRADE_DATE"]="bad"
            elif mutation=="duplicate":data[-1]=data[-2].copy()
            elif mutation=="short_page":data.pop()
        if page==2 and mutation=="count_changed":body["result"]["count"]+=1
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=json.dumps(body).encode()
        store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],provider="eastmoney",endpoint="lpr",code_version="offline-fixture",
            scope={"synthetic":True,"mutation":mutation,"parent_sha256":record["body_sha256"]})
    return store.root/"manifest.ndjson"


LPR_FAILURES=[("numeric","RuntimeError"),("missing_required","RuntimeError"),("date","RuntimeError"),("duplicate","NormalizationError"),("short_page","RuntimeError"),("count_changed","RuntimeError")]


@pytest.mark.parametrize("mutation,expected",LPR_FAILURES)
def test_lpr_invalid_history_is_not_published(tmp_path,no_network,mutation,expected):
    manifest=lpr_fixture(tmp_path,mutation)
    report=collect_input(input_id="ASTOCK-065",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="failed" and report["failure_class"]==expected and "output" not in report,report
    assert report["responses"] and report["production_writes"]==report["live_http_calls"]==0
    for record in report["responses"]:
        assert hashlib.sha256(RawObjectStore.read_response(Path(report["run_directory"])/report["raw_manifest"]["path"],record)).hexdigest()==record["body_sha256"]


def test_lpr_yaml_projection_and_forbidden_window(tmp_path,no_network):
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    path=config/"normalization/lpr_history.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"));rule["rules"][0]["field_mapping"]["lpr_1y"]="LPR5Y"
    path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    report=collect_input(input_id="ASTOCK-065",context={},config_root=config,output_root=tmp_path/"candidate",replay_manifest=EVENT_ARCHIVE,fields=["rate_date","snapshot_at"])
    assert report["status"]=="candidate_complete" and all(set(r)=={"rate_date","snapshot_at"} for r in read_artifact(report,"output")),report
    invalid_config=tmp_path/"invalid-config";shutil.copytree(config,invalid_config)
    rule["rules"][0]["field_mapping"]["rate_date"]="LPR1Y"
    (invalid_config/"normalization/lpr_history.yaml").write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    invalid=collect_input(input_id="ASTOCK-065",context={},config_root=invalid_config,output_root=tmp_path/"invalid",replay_manifest=EVENT_ARCHIVE)
    assert invalid["status"]=="failed" and invalid["failure_class"]=="NormalizationError" and "output" not in invalid,invalid
    for context in [{"request":{"symbol":"600519"}},{"request":{"start_date":"2026-09-01"}},{"config":{"limit":50}}]:
        with pytest.raises(ValueError):collect_input(input_id="ASTOCK-065",context=context,config_root=config,output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)


def test_lpr_four_page_session_and_cache(tmp_path,monkeypatch):
    from unittest.mock import patch
    from stock_data_manage.storage.raw import sanitized_url
    originals=lpr_source_records();seen=[]
    def send(session,request,**kwargs):
        seen.append(session);record=next(r for r in originals if sanitized_url(r["url"])==sanitized_url(request.url))
        assert session.trust_env is True and kwargs["timeout"]==20 and session.get_adapter(request.url).max_retries.total==3
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response.headers["Content-Type"]="application/json"
        response._content=RawObjectStore.read_response(EVENT_ARCHIVE,record);return response
    with patch("requests.Session.send",send):
        first=collect_input(input_id="ASTOCK-065",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
        second=collect_input(input_id="ASTOCK-065",context={},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
    assert first["status"]==second["status"]=="candidate_complete",(first,second)
    assert len(seen)==4 and len({id(session) for session in seen})==1
    assert first["live_http_calls"]==4 and second["live_http_calls"]==0 and len(second["responses"])==4
    assert read_artifact(first,"output")==read_artifact(second,"output")
    (tmp_path/"fixture-mode.json").write_text(json.dumps({"mode":"injected Session; not real live source validation",
        "real_http_calls":0,"fixture_send_calls":4,"cached_send_calls":0}),encoding="utf-8")


def action_source_records(input_id):
    from urllib.parse import parse_qs,urlsplit
    return [json.loads(line) for line in EVENT_ARCHIVE.read_text(encoding="utf-8").splitlines()
            if parse_qs(urlsplit(json.loads(line)["url"]).query).get("reportName")==[ACTION_REPORTS[input_id]]]


def compare_action_original(tmp_path,input_id,context,manifest,count):
    import pandas as pd
    spec=importlib.util.spec_from_file_location("action_original",ROOT/"provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);ns=module.load_shipped_code()
    original=RawObjectStore(tmp_path/"original")
    try:
        with captured_requests(original,provider="eastmoney",endpoint=input_id,scope=context,code_version="original-v39-actions",pacer=RequestPacer(),replay_manifest=manifest) as events:
            frame=ns[ACTION_FUNCTIONS[input_id]](limit=count)
    finally:ns["EM_SESSION"].close()
    parsed=[{k:None if pd.isna(v) else v for k,v in row.items()} for row in frame.to_dict(orient="records")]
    tmp_path.mkdir(parents=True,exist_ok=True);original_path=tmp_path/"original-parsed.json"
    original_path.write_text(json.dumps(parsed,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["row_count"]==count,report
    source=read_artifact(report,"source_rows");raw=json.loads(RawObjectStore.read_response(manifest,action_source_records(input_id)[-1]))
    assert source==raw["result"]["data"] and report["source_total_count"]==raw["result"]["count"]
    keys=("url","method","outcome","status_code","body_sha256","request_headers","request_options")
    assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
    rows=read_artifact(report,"output")
    for row,prior,raw_row in zip(rows,parsed,source):
        assert row["snapshot_at"]==report["source_capture_window"]["last"]
        for name,value in row.items():
            if name=="snapshot_at":continue
            if name=="source_plan_code":assert value==str(raw_row["REPURCODE"]);continue
            expected=prior[{"source_security_code":"code","statistic_date":"date"}.get(name,name)]
            if name in report["unverified_fields"]:assert value is None
            elif expected is None:assert value is None
            elif isinstance(expected,(float,int)):assert float(value)==pytest.approx(expected,rel=1e-14)
            else:assert value==expected
    comparison={"input_id":input_id,"mode":"offline_replay","row_count":count,"all_retained_source_fields_equal":True,
        "all_business_fields_equal":True,"request_comparison_equal":True,"source_total_count":report["source_total_count"],
        "source_response_hashes":[r["body_sha256"] for r in events],"report_path":report["report_path"],
        "report_sha256":hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        "original_parsed_path":str(original_path.resolve()),"original_parsed_sha256":hashlib.sha256(original_path.read_bytes()).hexdigest()}
    (tmp_path/"comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count",ACTION_CASES)
def test_action_inputs_preserve_original_business_fields(tmp_path,no_network,input_id,context,manifest,count):
    compare_action_original(tmp_path,input_id,context,manifest,count)


def action_fixture(tmp_path,input_id,mutation):
    records=action_source_records(input_id);store=RawObjectStore(tmp_path/"fixture")
    for index,record in enumerate(records):
        body=json.loads(RawObjectStore.read_response(EVENT_ARCHIVE,record));data=body["result"]["data"]
        if index==len(records)-1:
            if mutation=="direction":data[0]["DIRECTION"]="减持"
            elif mutation=="pledge_sum":data[0]["REPURCHASE_LIMITED_BALANCE"]=10000
            elif mutation=="pledge_date":data[0]["TRADE_DATE"]="2026-09-29 00:00:00"
            elif mutation=="numeric":data[0]["REPURAMOUNTLOWER"]="broken"
            elif mutation=="plan_missing":data[0].pop("REPURCODE")
            elif mutation=="code":data[0]["SECURITY_CODE"]="broken"
            elif mutation=="duplicate":data[1]=data[0].copy()
            elif mutation=="progress_unknown":data[0]["REPURPROGRESS"]="007"
            elif mutation=="board_fallback":data[0]["MARKET"]=None;data[0]["MARKET_TYPE_NEW"]="北交所"
            elif mutation=="future_date":data[0]["LISTING_DATE"]="2030-01-01 00:00:00";data[0]["ISSUE_PRICE"]=0
        elif mutation=="latest_date_missing":data[0].pop("TRADE_DATE")
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=json.dumps(body,ensure_ascii=False).encode()
        store.record_response(response=response,url=record["url"],method="GET",request_headers=record["request_headers"],provider="eastmoney",endpoint=input_id,
            code_version="offline-fixture",scope={"synthetic":True,"mutation":mutation,"parent_sha256":record["body_sha256"]})
    return store.root/"manifest.ndjson"


ACTION_FAILURES=[("ASTOCK-080","direction","RuntimeError"),("ASTOCK-082","pledge_sum","RuntimeError"),
    ("ASTOCK-082","pledge_date","RuntimeError"),("ASTOCK-082","latest_date_missing","RuntimeError"),
    ("ASTOCK-081","numeric","RuntimeError"),("ASTOCK-081","plan_missing","NormalizationError"),
    ("ASTOCK-083","code","RuntimeError"),("ASTOCK-083","duplicate","RuntimeError")]


@pytest.mark.parametrize("input_id,mutation,expected",ACTION_FAILURES)
def test_action_semantics_failures_retain_evidence(tmp_path,no_network,input_id,mutation,expected):
    manifest=action_fixture(tmp_path,input_id,mutation);count=30 if input_id=="ASTOCK-083" else 50
    report=collect_input(input_id=input_id,context={"config":{"limit":count}},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="failed" and report["failure_class"]==expected and "output" not in report,report
    for event in report["responses"]:
        body=RawObjectStore.read_response(Path(report["run_directory"])/report["raw_manifest"]["path"],event)
        assert hashlib.sha256(body).hexdigest()==event["body_sha256"]


@pytest.mark.parametrize("input_id,mutation",[("ASTOCK-081","progress_unknown"),("ASTOCK-083","board_fallback"),("ASTOCK-083","future_date")])
def test_action_unknown_progress_and_future_ipo_are_preserved(tmp_path,no_network,input_id,mutation):
    manifest=action_fixture(tmp_path,input_id,mutation);count=30 if input_id=="ASTOCK-083" else 50
    report=collect_input(input_id=input_id,context={"config":{"limit":count}},config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete",report
    row=read_artifact(report,"output")[0]
    if mutation=="progress_unknown":assert row["progress"] is None and row["progress_code"]=="007"
    elif mutation=="board_fallback":assert row["board"]=="北交所"
    else:assert row["listing_date"]=="2030-01-01" and row["issue_price"] is None


def test_action_yaml_mapping_scope_and_empty_statistics(tmp_path,no_network):
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    path=config/"normalization/buyback.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"))
    rule["rules"][0]["transforms"]["progress"]["value_mapping"]["006"]="自定义标签"
    path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    report=collect_input(input_id="ASTOCK-081",context={"config":{"limit":50}},config_root=config,output_root=tmp_path/"candidate",replay_manifest=EVENT_ARCHIVE)
    assert report["status"]=="candidate_complete" and read_artifact(report,"output")[0]["progress"]=="自定义标签",report
    for input_id,context in [("ASTOCK-080",{"config":{"direction":"买入"}}),("ASTOCK-081",{"config":{"progress":"未知"}}),
                            ("ASTOCK-083",{"request":{"symbol":"600519"}}),("ASTOCK-082",{"request":{"trade_date":"2026-09-30"}})]:
        with pytest.raises(ValueError):collect_input(input_id=input_id,context=context,config_root=config,output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)
    for context in [{"request":{"symbol":"bj920000"}},{"request":{"symbol":"600519","statistic_date":"2026-09-30"}}]:
        failed=collect_input(input_id="ASTOCK-082",context=context,config_root=config,output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)
        assert failed["status"]=="failed" and failed["failure_class"]=="ValueError" and "responses" not in failed,failed


def test_action_normalization_transform_safety():
    fields={"board":{"type":"string","required":False},"progress":{"type":"string","required":False},"price":{"type":"decimal","required":False}}
    rule={"status":"pending_validation","field_mapping":{"board":"MARKET","progress":"CODE","price":"PRICE"},
        "transforms":{"board":{"fallback_source":"BOARD"},"progress":{"value_mapping":{"006":"完成"}},"price":{"zero_is_null":True}}}
    result=Normalizer.normalize_fields({"MARKET":None,"BOARD":"北交所","CODE":"009","PRICE":0},rule=rule,fields=fields,allow_pending=True)
    assert result=={"board":"北交所","progress":None,"price":None}
    with pytest.raises(NormalizationError):Normalizer.normalize_fields({"PRICE":True},rule=rule,fields=fields,allow_pending=True)


def test_action_pledge_session_and_cache_use_original_two_queries(tmp_path,monkeypatch):
    from unittest.mock import patch
    from stock_data_manage.storage.raw import sanitized_url
    originals=action_source_records("ASTOCK-082");seen=[]
    def send(session,request,**kwargs):
        seen.append(session)
        record=next(r for r in originals if sanitized_url(r["url"])==sanitized_url(request.url))
        assert kwargs["timeout"]==20 and session.trust_env is True
        assert session.get_adapter(request.url).max_retries.total==3
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response.headers["Content-Type"]="application/json"
        response._content=RawObjectStore.read_response(EVENT_ARCHIVE,record);return response
    with patch("requests.Session.send",send):
        first=collect_input(input_id="ASTOCK-082",context={"config":{"limit":50}},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
        second=collect_input(input_id="ASTOCK-082",context={"config":{"limit":50}},config_root=ROOT/"config",output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
    assert first["status"]==second["status"]=="candidate_complete",(first,second)
    assert len(seen)==2 and seen[0] is seen[1]
    assert first["live_http_calls"]==2 and second["live_http_calls"]==0
    assert [r["mode"] for r in second["responses"]]==["cached","cached"]
    assert read_artifact(first,"output")==read_artifact(second,"output")
    (tmp_path/"fixture-mode.json").write_text(json.dumps({"mode":"injected Session; not real live source verification",
        "real_http_calls":0,"fixture_send_calls":2,"cached_send_calls":0}),encoding="utf-8")


def test_action_pledge_unpublished_date_and_filtered_empty_are_distinct(tmp_path,no_network):
    from urllib.parse import urlsplit,urlunsplit,parse_qs,urlencode
    for input_id in ("ASTOCK-080","ASTOCK-082"):
        record=action_source_records(input_id)[-1]
        parts=urlsplit(record["url"]);query={k:v[0] for k,v in parse_qs(parts.query,keep_blank_values=True).items()}
        context={"config":{"limit":50}}
        if input_id=="ASTOCK-080":
            query["filter"]='(SECURITY_CODE="600519")';context["request"]={"symbol":"600519"}
        else:context["request"]={"statistic_date":"2026-09-30"}
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=b'{"code":9201,"result":null}'
        store=RawObjectStore(tmp_path/input_id/"fixture")
        store.record_response(response=response,url=urlunsplit((parts.scheme,parts.netloc,parts.path,urlencode(query),"")),method="GET",
            request_headers=record["request_headers"],provider="eastmoney",endpoint=input_id,code_version="offline-fixture",
            scope={"synthetic":True,"mutation":"empty","parent_sha256":record["body_sha256"]})
        report=collect_input(input_id=input_id,context=context,config_root=ROOT/"config",output_root=tmp_path/input_id/"candidate",replay_manifest=store.root/"manifest.ndjson")
        assert len(report["responses"])==1,report
        if input_id=="ASTOCK-080":assert report["status"]=="candidate_complete" and report["valid_empty_dataset"] and read_artifact(report,"output")==[],report
        else:assert report["status"]=="failed" and report["failure_class"]=="ValueError" and "output" not in report,report


def event_source_record(input_id):
    from urllib.parse import parse_qs, urlsplit
    return next(json.loads(line) for line in EVENT_ARCHIVE.read_text(encoding="utf-8").splitlines()
                if parse_qs(urlsplit(json.loads(line)["url"]).query).get("reportName") == [EVENT_REPORTS[input_id]])


def compare_event_original(tmp_path, input_id, context, manifest, count):
    import pandas as pd
    test = ROOT / "provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py"
    spec = importlib.util.spec_from_file_location("event_original", test)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    ns = module.load_shipped_code()
    store = RawObjectStore(tmp_path / "original")
    try:
        with captured_requests(store, provider="eastmoney", endpoint=input_id, scope=context, code_version="original-v39-script",
                               pacer=RequestPacer(), replay_manifest=manifest) as original_events:
            frame = ns["earnings_forecast" if input_id == "ASTOCK-078" else "institution_survey"](limit=50)
    finally:
        ns["EM_SESSION"].close()
    original_rows = [{k: None if pd.isna(v) else v for k, v in row.items()} for row in frame.to_dict(orient="records")]
    tmp_path.mkdir(parents=True, exist_ok=True)
    original_path = tmp_path / "original-parsed.json"
    original_path.write_text(json.dumps(original_rows, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config",
                           output_root=tmp_path / "candidate", replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    retained = json.loads(RawObjectStore.read_response(manifest, event_source_record(input_id)))
    assert read_artifact(report, "source_rows") == retained["result"]["data"]
    assert report["row_count"] == report["coverage_denominator"] == len(frame) == count
    assert report["source_total_count"] == retained["result"]["count"] and report["result_limited"]
    assert report["live_http_calls"] == report["production_writes"] == 0
    keys = ("url", "method", "outcome", "status_code", "body_sha256", "request_headers", "request_options")
    assert [{k: e.get(k) for k in keys} for e in original_events] == [{k: e.get(k) for k in keys} for e in report["responses"]]
    output = read_artifact(report, "output")
    for row, original in zip(output, original_rows):
        for name, value in row.items():
            if name in {"snapshot_at", "indicator_code"}: continue
            expected = original["code" if name == "source_security_code" else name]
            if name in report["unverified_fields"]: assert value is None
            elif expected is None: assert value is None
            elif isinstance(expected, (int, float)): assert float(value) == pytest.approx(expected, rel=1e-14)
            else: assert value == expected
        assert row["snapshot_at"] == report["source_capture_window"]["last"]
    comparison = {"input_id":input_id, "mode":"offline_replay", "row_count":count,
        "original_parsed_path":str(original_path.resolve()), "original_parsed_sha256":hashlib.sha256(original_path.read_bytes()).hexdigest(),
        "source_response_sha256":event_source_record(input_id)["body_sha256"],
        "source_total_count":report["source_total_count"], "all_retained_source_fields_equal":True,
        "all_business_fields_equal":True, "request_comparison_equal":True, "report_path":report["report_path"],
        "report_sha256":hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
        "limitations":["bounded latest 50 events; not whole-market completeness", "currency units pending",
                       "archived redacted cookies cannot reconstruct the original cross-endpoint Session cookie jar"]}
    (tmp_path / "comparison.json").write_text(json.dumps(comparison,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return comparison


@pytest.mark.parametrize("input_id,context,manifest,count", EVENT_CASES)
def test_event_inputs_preserve_original_business_fields(tmp_path, no_network, input_id, context, manifest, count):
    compare_event_original(tmp_path,input_id,context,manifest,count)


def event_fixture(tmp_path, mutation, input_id="ASTOCK-078", *, narrowed=False):
    record = event_source_record(input_id)
    body = json.loads(RawObjectStore.read_response(EVENT_ARCHIVE,record))
    data = body["result"]["data"]
    if mutation == "pages": body["result"]["pages"] = True
    elif mutation == "count": body["result"].update(count=49, pages=1)
    elif mutation == "short_page": data.pop()
    elif mutation == "duplicate": data[1] = data[0].copy()
    elif mutation == "code": data[0]["SECURITY_CODE"] = "1"
    elif mutation == "numeric": data[0]["PREDICT_AMT_LOWER"] = "broken"
    elif mutation == "date": data[0]["NOTICE_DATE"] = "2026/09/30"
    elif mutation == "business": body["code"] = 9501
    elif mutation == "empty": body = {"code":9201,"result":None}
    elif mutation == "ignored_filter": data[0]["NUMBERNEW"] = "2"
    response = requests.Response()
    response.status_code, response.encoding = (429 if mutation=="http_429" else 200), "utf-8"
    response._content = b"<html>login</html>" if mutation=="html" else json.dumps(body,ensure_ascii=False).encode()
    url = record["url"]
    if narrowed:
        from urllib.parse import urlsplit, parse_qs, urlencode, urlunsplit
        parts = urlsplit(url); query = {k:v[0] for k,v in parse_qs(parts.query,keep_blank_values=True).items()}
        query["filter"] += '(SECURITY_CODE="600519")'
        url = urlunsplit((parts.scheme,parts.netloc,parts.path,urlencode(query),""))
    store = RawObjectStore(tmp_path/"fixture")
    event = store.record_response(response=response,url=url,method="GET",request_headers=record["request_headers"],
        scope={"synthetic":True,"mutation":mutation,"parent_sha256":record["body_sha256"]},provider="eastmoney",
        endpoint=input_id,code_version="offline-fixture")
    if mutation=="transport":
        event.update(outcome="transport_error",error_type="ConnectionError")
        (store.root/"manifest.ndjson").write_text(json.dumps(event)+"\n",encoding="utf-8")
    return store.root/"manifest.ndjson"


EVENT_FAILURES = [("pages","RuntimeError"),("count","RuntimeError"),("short_page","RuntimeError"),
                  ("duplicate","RuntimeError"),("code","RuntimeError"),("numeric","RuntimeError"),
                  ("date","RuntimeError"),("business","RuntimeError"),("empty","RuntimeError"),
                  ("ignored_filter","RuntimeError"),("http_429","RuntimeError"),("html","RuntimeError"),("transport","RuntimeError")]


@pytest.mark.parametrize("mutation,expected", EVENT_FAILURES)
def test_event_invalid_response_is_not_a_complete_dataset(tmp_path,no_network,mutation,expected):
    input_id = "ASTOCK-079" if mutation=="ignored_filter" else "ASTOCK-078"
    manifest = event_fixture(tmp_path,mutation,input_id)
    report = collect_input(input_id=input_id,context={"config":{"limit":50}},config_root=ROOT/"config",
                           output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="failed" and report["failure_class"]==expected and "output" not in report, report
    assert len(report["responses"])==1
    if mutation!="transport":
        event=report["responses"][0]
        body=RawObjectStore.read_response(Path(report["run_directory"])/report["raw_manifest"]["path"],event)
        assert hashlib.sha256(body).hexdigest()==event["body_sha256"]


def test_event_filtered_empty_is_valid_but_full_list_empty_is_not(tmp_path,no_network):
    manifest=event_fixture(tmp_path,"empty",narrowed=True)
    report=collect_input(input_id="ASTOCK-078",context={"request":{"symbol":"600519"},"config":{"limit":50}},
        config_root=ROOT/"config",output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert report["status"]=="candidate_complete" and report["valid_empty_dataset"] and report["row_count"]==0,report
    assert read_artifact(report,"output")==[] and read_artifact(report,"source_rows")==[]
    assert report["first_key"] is None and report["source_total_count"] is None


def test_event_mapping_projection_and_unsupported_scope(tmp_path,no_network):
    config=tmp_path/"config";shutil.copytree(ROOT/"config",config)
    path=config/"normalization/earnings_forecast.yaml";rule=yaml.safe_load(path.read_text(encoding="utf-8"))
    rule["rules"][0]["field_mapping"]["reason"]="PREDICT_CONTENT";path.write_text(yaml.safe_dump(rule,allow_unicode=True),encoding="utf-8")
    fields={k for k,v in yaml.safe_load((config/"datasets/earnings_forecast.yaml").read_text(encoding="utf-8"))["fields"].items() if v.get("required")}|{"reason"}
    report=collect_input(input_id="ASTOCK-078",context={"config":{"limit":50}},config_root=config,
        output_root=tmp_path/"candidate",replay_manifest=EVENT_ARCHIVE,fields=fields)
    assert report["status"]=="candidate_complete",report
    source=read_artifact(report,"source_rows");rows=read_artifact(report,"output")
    assert all(set(row)==fields and row["reason"]==raw["PREDICT_CONTENT"] for row,raw in zip(rows,source))
    for input_id,context in [("ASTOCK-078",{"request":{"start_date":"2026-09-01"}}),
                             ("ASTOCK-079",{"config":{"detail":True}}),("ASTOCK-078",{"config":{"limit":5001}})]:
        with pytest.raises(ValueError):
            collect_input(input_id=input_id,context=context,config_root=config,output_root=tmp_path/"bad",replay_manifest=EVENT_ARCHIVE)


def event_page_fixture(tmp_path, mutation=None):
    from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode
    record=event_source_record("ASTOCK-078")
    original=json.loads(RawObjectStore.read_response(EVENT_ARCHIVE,record))
    store=RawObjectStore(tmp_path/"fixture")
    for page,count in [(1,500),(2,1)]:
        rows=[]
        for index in range(count):
            row=original["result"]["data"][0].copy()
            row["SECURITY_CODE"]=str(600000+(page-1)*500+index)
            rows.append(row)
        body={**original,"result":{"data":rows,"pages":2,"count":501}}
        if page==2:
            if mutation=="empty_page":body["result"]["data"]=[]
            elif mutation=="changed_total":body["result"]["count"]=502
            elif mutation=="repeat_page":body["result"]["data"][0]["SECURITY_CODE"]="600000"
        parts=urlsplit(record["url"]);query={k:v[0] for k,v in parse_qs(parts.query,keep_blank_values=True).items()}
        query.update(pageNumber=str(page),pageSize="500")
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response._content=json.dumps(body).encode()
        store.record_response(response=response,url=urlunsplit((parts.scheme,parts.netloc,parts.path,urlencode(query),"")),
            method="GET",request_headers=record["request_headers"],provider="eastmoney",endpoint="earnings_forecast",
            code_version="offline-fixture",scope={"synthetic":True,"mutation":mutation,"parent_sha256":record["body_sha256"]})
    return store.root/"manifest.ndjson"


@pytest.mark.parametrize("mutation",[None,"empty_page","changed_total","repeat_page"])
def test_event_complete_paging_and_changed_total(tmp_path,no_network,mutation):
    manifest=event_page_fixture(tmp_path,mutation)
    report=collect_input(input_id="ASTOCK-078",context={"config":{"limit":600}},config_root=ROOT/"config",
        output_root=tmp_path/"candidate",replay_manifest=manifest)
    assert len(report["responses"])==2 and report["live_http_calls"]==0,report
    if mutation is not None:
        assert report["status"]=="failed" and report["failure_class"]=="RuntimeError" and "output" not in report,report
    else:
        assert report["status"]=="candidate_complete" and report["row_count"]==501 and not report["result_limited"],report
        assert report["source_page_count"]==2 and report["source_total_count"]==501
        assert len({r["source_security_code"] for r in read_artifact(report,"output")})==501


def test_event_existing_financial_method_unchanged(tmp_path,no_network):
    import sys
    import types
    from stock_data_manage.providers.eastmoney.financial import EastMoneyFinancialMainProvider
    from stock_data_manage.providers.contracts import HttpResponse
    snapshot=ROOT/"provider_validation/results/events-original-20261004/1-financial.py.bin"
    module=types.ModuleType("stock_data_manage.providers.eastmoney.original_financial")
    sys.modules[module.__name__]=module
    try:exec(compile(snapshot.read_bytes(),str(snapshot),"exec"),module.__dict__)
    finally:sys.modules.pop(module.__name__)
    payload={"result":{"data":[{"REPORT_DATE":"2026-06-30","NOTICE_DATE":"2026-08-22","EPSJB":3.1,"CURRENCY":"CNY"}]}}
    class Transport:
        def __init__(self):self.calls=[]
        def get(self,url,**kwargs):
            self.calls.append((url,kwargs));return HttpResponse(200,{"Content-Type":"application/json"},json.dumps(payload).encode())
    old,new=Transport(),Transport()
    prior=module.EastMoneyFinancialMainProvider(old).fetch(["sh600519"])
    current=EastMoneyFinancialMainProvider(new).fetch(["sh600519"])
    from dataclasses import asdict
    assert asdict(prior)==asdict(current) and old.calls==new.calls
    tmp_path.mkdir(parents=True,exist_ok=True)
    (tmp_path/"legacy-comparison.json").write_text(json.dumps({"same_request":True,"same_return":True,
        "mode":"offline fixture","original_source_sha256":hashlib.sha256(snapshot.read_bytes()).hexdigest()}),encoding="utf-8")


def test_event_session_policy_and_cache_before_fetch(tmp_path,monkeypatch):
    from unittest.mock import patch
    body=RawObjectStore.read_response(EVENT_ARCHIVE,event_source_record("ASTOCK-078"))
    seen=[]
    def send(session,request,**kwargs):
        seen.append((session,request,kwargs))
        retry=session.get_adapter(request.url).max_retries
        assert session.trust_env is True and retry.total==retry.connect==3 and retry.backoff_factor==0.6
        assert retry.status_forcelist==[429,500,502,503,504] and retry.allowed_methods==["GET"]
        assert request.headers["User-Agent"].startswith("python-requests/") and "Referer" not in request.headers
        assert kwargs["timeout"]==20
        response=requests.Response();response.status_code=200;response.encoding="utf-8";response.headers["Content-Type"]="application/json"
        response._content=body;return response
    with patch("requests.Session.send",send):
        first=collect_input(input_id="ASTOCK-078",context={"config":{"limit":50}},config_root=ROOT/"config",
            output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"unused")
        second=collect_input(input_id="ASTOCK-078",context={"config":{"limit":50}},config_root=ROOT/"config",
            output_root=tmp_path/"candidate",mode="live",evidence_root=tmp_path/"candidate")
    assert first["status"]==second["status"]=="candidate_complete" and len(seen)==1,(first,second)
    assert first["live_http_calls"]==1 and second["live_http_calls"]==0 and second["responses"][0]["mode"]=="cached"
    assert read_artifact(first,"output")==read_artifact(second,"output")
    (tmp_path/"fixture-mode.json").write_text(json.dumps({"mode":"injected Session fixture; not a real live source call",
        "source_http_calls":0,"fixture_send_calls":1,"cached_send_calls":0}),encoding="utf-8")


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
    assert report["row_count"] == count and report["coverage_denominator"] == (5224 if input_id == "SDA-BOARD-005" else 5223)
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
        pattern = re.compile(r"^(?:sh\.(?:60|68)\d{4}|sz\.(?:000|001|002|003|300|301|302)\d{3})$")
        stocks = {row["code"]: row for row in payloads[0]["rows"] if pattern.fullmatch(row["code"])}
        assert len(stocks) == 5224
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
    try:
        import curl_cffi.requests as curl_requests
        import pandas.io.common as common
    except ImportError:
        pass
    else:
        monkeypatch.setattr(curl_requests.Session, 'request', fail)
        monkeypatch.setattr(common, 'urlopen', fail)
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


# Reuse the independently checked request contexts and immutable source archives above.
RUNTIME_CASES = {}
for _case_group in (REMAINING_CASES, SDK_NEWS_CASES, ACTUAL_DATA_CASES, MACRO_CASES,
                    REPORTS_SEATS_CASES, MARKET_EVENT_CASES, FACTOR_CASES, CASES,
                    THS_CASES, BAO_CASES, EM_CASES, POOL_CASES, REPORTS_CALENDAR_CASES,
                    SINA_FUTURES_CASES, NEWS_CASES, RATES_BONDS_CASES, EVENT_CASES, ACTION_CASES):
    for _case in _case_group:
        RUNTIME_CASES.setdefault(_case[0], _case)
RUNTIME_CASES["ASTOCK-001"] = ("ASTOCK-001", QUOTE_CONTEXT, QUOTE_ARCHIVE, 1)
RUNTIME_CASES["ASTOCK-065"] = ("ASTOCK-065", {}, EVENT_ARCHIVE, 1538)


@pytest.mark.parametrize("input_id,context,manifest,count", list(RUNTIME_CASES.values()),
                         ids=list(RUNTIME_CASES))
def test_runtime_layered_storage_preserves_source_and_mapping(tmp_path, no_network, input_id, context, manifest, count):
    import pyarrow.parquet as pq
    from stock_data_manage.config.loader import load_storage_paths
    from stock_data_manage.storage.metadata import MetadataStore
    from stock_data_manage.domain import AttemptStatus

    import os
    audit_root = os.environ.get("STOCKDATA_LAYOUT_EVIDENCE_ROOT")
    data_root = Path(audit_root).resolve() if audit_root else tmp_path / "data"
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config",
                           data_root=data_root, replay_manifest=manifest)
    assert report["status"] == "candidate_complete", {k: report.get(k) for k in ("input_id", "status", "error")}
    assert report["row_count"] == count
    paths = load_storage_paths(ROOT / "config", data_root=data_root)
    task = Path(report["run_directory"])
    assert task == paths["workspace_root"] / report["data_date"] / report["dataset"]
    raw_manifest = (task / report["raw_manifest"]["path"]).resolve()
    assert raw_manifest.is_relative_to(paths["raw_root"] / "_tmp" / report["data_date"])
    assert not (paths["raw_root"] / report["provider"] / report["endpoint"]).exists()
    raw_events = [json.loads(line) for line in raw_manifest.read_text(encoding="utf-8").splitlines()]
    assert next(event for event in raw_events if event.get("event") == "raw_partition")["data_date"] == report["data_date"]
    assert not (task / "_raw").exists()
    assert not paths["canonical_root"].exists() and not paths["archive_root"].exists()
    assert report["live_http_calls"] == report["production_writes"] == 0
    assert not report["eligible_for_production_routing"]
    for event in report["responses"]:
        if event.get("body_sha256"):
            body = RawObjectStore.read_response(raw_manifest, event)
            assert hashlib.sha256(body).hexdigest() == event["body_sha256"]
    parquet = task / report["normalized_parquet"]["path"]
    assert hashlib.sha256(parquet.read_bytes()).hexdigest() == report["normalized_parquet"]["sha256"]
    records = pq.ParquetFile(parquet).read().to_pylist()
    from stock_data_manage.pipeline.inputs import _json_value
    # Every mapped value, including exact decimals/nulls and timestamp instants, must survive.
    mapped = read_artifact(report, "output")
    for row, expected in zip(records, mapped):
        for name, value in row.items():
            if isinstance(value, datetime):
                assert value == datetime.fromisoformat(expected[name])
            elif isinstance(value, Decimal):
                assert value == Decimal(expected[name])
            else:
                assert _json_value(value) == expected[name]
    source = parquet.parent
    request_key = raw_manifest.name.removeprefix("manifest.").removesuffix(".ndjson")
    task_manifest = json.loads((source / (request_key + ".manifest.json")).read_text(encoding="utf-8"))
    quality = json.loads((source / (request_key + ".quality.json")).read_text(encoding="utf-8"))
    assert task_manifest["status"] == quality["status"] == "candidate_complete"
    assert not task_manifest["publication_permitted"] and not task_manifest["canonical_refs"]
    assert task_manifest["source_response_hashes"] == report["output"]["source_response_hashes"]
    assert quality["coverage_denominator"] == report["coverage_denominator"]
    source_refs = json.loads((source / (request_key + ".raw_refs.json")).read_text(encoding="utf-8"))
    assert source_refs["raw_manifest"] == report["raw_manifest"]
    metadata = MetadataStore(paths["metadata_path"])
    try:
        attempt = metadata.load_attempt(task_manifest["task_id"])
        assert attempt.status == AttemptStatus.VALIDATED
        assert Path(attempt.raw_object_path) == raw_manifest
        assert attempt.raw_content_hash == report["raw_manifest"]["sha256"]
    finally:
        metadata.close()
    if audit_root:
        evidence = {"input_id":input_id, "report_path":report["report_path"],
            "report_sha256":hashlib.sha256(Path(report["report_path"]).read_bytes()).hexdigest(),
            "source_manifest":str(manifest.resolve()), "source_manifest_sha256":hashlib.sha256(manifest.read_bytes()).hexdigest(),
            "source_response_hashes":report["output"]["source_response_hashes"],
            "code_version":report["code_version"], "row_count":report["row_count"],
            "coverage_denominator":report["coverage_denominator"],
            "validation_time_utc":report["validation_time_utc"],
            "mode":"offline archive replay into isolated runtime layers", "canonical_writes":0,
            "field_roundtrip_verified":True, "raw_hashes_verified":True}
        target=data_root.parent / (input_id+"-verification.json")
        target.write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding="utf-8")


def test_runtime_failed_task_keeps_raw_without_publishing(tmp_path, no_network):
    manifest = quote_fixture_manifest(tmp_path / "probe", ["sh600519"], mutation="empty")
    result = collect_input(input_id="ASTOCK-001", context=QUOTE_CONTEXT, config_root=ROOT / "config",
                           data_root=tmp_path / "data", replay_manifest=manifest)
    assert result["status"] == "failed"
    task = Path(result["run_directory"])
    raw_manifest = (task / result["raw_manifest"]["path"]).resolve()
    assert raw_manifest.is_file()
    request_key = raw_manifest.name.removeprefix("manifest.").removesuffix(".ndjson")
    source = task / "sources" / result["provider"] / result["endpoint"]
    assert json.loads((source / (request_key + ".manifest.json")).read_text())["status"] == "failed"
    assert not (tmp_path / "data/canonical").exists()


@pytest.mark.parametrize("argument", ["output_root", "data_root"])
@pytest.mark.parametrize("mode", ["live", "replay"])
def test_formal_input_rejects_validation_output_before_collection(argument, mode, no_network):
    with pytest.raises(ValueError, match="outside provider_validation"):
        collect_input(input_id="ASTOCK-001", context=QUOTE_CONTEXT, config_root=ROOT / "config",
                      mode=mode, replay_manifest=QUOTE_ARCHIVE,
                      **{argument: ROOT / "provider_validation/results/forbidden-business-output"})


def test_input_replay_rejects_default_production_root(no_network):
    with pytest.raises(ValueError, match="isolated"):
        collect_input(input_id="ASTOCK-001", context=QUOTE_CONTEXT, config_root=ROOT / "config",
                      replay_manifest=QUOTE_ARCHIVE)


def test_input_date_uses_history_target_and_original_shanghai_capture(tmp_path):
    from stock_data_manage.pipeline.inputs import _input_data_date
    manifest = tmp_path / "manifest.ndjson"
    manifest.write_text(json.dumps({"event": "http_response", "provider": "fixture", "endpoint": "current",
        "fetched_at_utc": "2026-09-30T16:30:00+00:00"}) + "\n", encoding="utf-8")
    assert _input_data_date({}, manifest, provider="fixture", endpoint="current") == "2026-10-01"
    assert _input_data_date({"request": {"end_date": "2026-09-18"}}, manifest,
                            provider="fixture", endpoint="daily") == "2026-09-18"
    assert _input_data_date({"request": {"end_date": "2026-09-18"}}, manifest,
                            provider="fixture", endpoint="daily", data_date="2026-09-15") == "2026-09-15"
    manifest.write_text(json.dumps({"event": "http_response", "provider": "fixture", "endpoint": "current",
        "fetched_at_utc": "2026-10-10T00:00:00+00:00", "source_ref": {
            "fetched_at_utc": "2026-09-30T16:30:00+00:00"}}) + "\n", encoding="utf-8")
    assert _input_data_date({}, manifest, provider="fixture", endpoint="current") == "2026-10-01"


@pytest.mark.parametrize("input_id", ["ASTOCK-001", "ASTOCK-032"])
def test_runtime_relocation_retains_original_day_and_dependency_references(tmp_path, monkeypatch, no_network, input_id):
    # Simulate a current input starting today and discovering older cached bytes.
    # Replay uses retained responses; this is an offline path/reference test.
    import stock_data_manage.pipeline.inputs as inputs
    original = inputs._input_data_date
    calls = []
    provisional = "2099-01-01"
    def provisional_then_source(*args, **kwargs):
        calls.append(True)
        return provisional if len(calls) == 1 else original(*args, **kwargs)
    monkeypatch.setattr(inputs, "_input_data_date", provisional_then_source)
    _, context, manifest, count = RUNTIME_CASES[input_id]
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config",
                           data_root=tmp_path / "data", replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report.get("error")
    assert report["row_count"] == count and report["data_date"] != provisional
    work = Path(report["run_directory"])
    raw_manifest = (work / report["raw_manifest"]["path"]).resolve()
    assert raw_manifest.is_relative_to(tmp_path / "data/raw/_tmp" / report["data_date"])
    dependency = report.get("sdk_dependency", {})
    for item in [dependency, *dependency.get("dependencies", [])]:
        name = item.get("source_path") or item.get("path")
        if name:
            assert hashlib.sha256((work / name).resolve().read_bytes()).hexdigest() == item["sha256"]
