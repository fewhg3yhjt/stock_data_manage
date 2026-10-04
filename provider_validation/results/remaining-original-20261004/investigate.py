"""Persist pending input inventory and exact implementation/source baselines; no network."""
from pathlib import Path
import hashlib, inspect, json, re, sys
from datetime import datetime, timezone
import yaml
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'src'))
from stock_data_manage.storage.raw import RawObjectStore

def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    out = Path(__file__).parent
    current = yaml.safe_load((ROOT/'config/providers.yaml').read_text(encoding='utf-8'))
    pending = {k:v for k,v in current['input_capabilities'].items() if v['implementation_status']=='unimplemented'}
    catalog_path = ROOT/'provider_validation/coverage/successful-input-capabilities.json'
    catalog = json.loads(catalog_path.read_text(encoding='utf-8'))['catalog']
    evidence = [row for row in catalog if row['contract']['input_id'] in pending]
    files = ['config/providers.yaml','src/stock_data_manage/pipeline/inputs.py','src/stock_data_manage/routing/factory.py',
        'src/stock_data_manage/providers/eastmoney/financial.py','src/stock_data_manage/providers/sina/news.py',
        'src/stock_data_manage/providers/ths/boards.py','src/stock_data_manage/providers/eastmoney/realtime.py',
        'src/stock_data_manage/providers/eastmoney/security_list.py','src/stock_data_manage/providers/tencent/snapshot.py',
        'src/stock_data_manage/providers/tdx/minute.py','tests/test_input_collection.py','CODE_STRUCTURE.md','PROJECT_PROGRESS.md','.gitattributes']
    baselines = []
    for i,name in enumerate(files):
        source = ROOT/name
        target = out/f'{i}-{source.name}.bin'
        assert not target.exists()
        target.write_bytes(source.read_bytes())
        baselines.append(dict(path=name,snapshot=target.relative_to(ROOT).as_posix(),sha256=sha(target)))
    refs = {catalog_path}
    for row in evidence:
        for e in row['evidence']:
            refs.update(ROOT/r['path'] for key in ('derived_artifacts','source_code_artifacts') for r in e.get(key,[]))
            if e.get('manifest_ref'): refs.add(ROOT/e['manifest_ref'])
    report = dict(time_utc=datetime.now(timezone.utc).isoformat(),pending=pending,catalog=evidence,before=baselines,
        source_refs=[dict(path=p.relative_to(ROOT).as_posix(),sha256=sha(p)) for p in sorted(refs)],
        network_requests=0,production_writes=0,eligible_for_production_routing=False)
    import akshare
    functions = ['stock_profit_forecast_ths','stock_news_em','stock_changes_em','option_risk_indicator_sse',
        'index_stock_cons_csindex','index_stock_cons_weight_csindex','stock_zh_index_value_csindex','stock_lhb_detail_daily_sina',
        'stock_financial_report_sina','stock_zyjs_ths','stock_profile_cninfo','stock_gsrl_gsdt_em']
    sdk=[]
    for name in functions:
        function=getattr(akshare,name)
        target=out/(name+'.py.bin')
        assert not target.exists()
        source=inspect.getsource(function).encode('utf-8')
        snapshot=re.sub(rb'''(?i)(["'](?:cookie|ut|token|access_token|api_key|password|secret)["']\s*:\s*["'])[^"']*(["'])''',rb'\1<redacted>\2',source)
        target.write_bytes(snapshot)
        sdk.append(dict(function=name,path=target.relative_to(ROOT).as_posix(),sha256=sha(target),
            original_source_sha256=hashlib.sha256(source).hexdigest(),redacted=snapshot!=source))
    report['sdk_version']=akshare.__version__
    report['sdk_sources']=sdk
    (out/'inventory.json').write_bytes((json.dumps(report,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))
    print(json.dumps(dict(pending=len(pending),references=len(refs),sdk_functions=len(sdk))))

if __name__=='__main__':main()
