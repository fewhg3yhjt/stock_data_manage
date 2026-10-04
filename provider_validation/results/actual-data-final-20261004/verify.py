"""Independent audit of source bytes, code/config versions, source scopes and delivery."""
import ast
import collections
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET
import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'src'))
from stock_data_manage.storage.raw import RawObjectStore, _SECRET_BODY
FINAL = Path(__file__).parent
RESULTS = ROOT/'provider_validation/results'
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))

baseline=read(RESULTS/'actual-data-original-20261004/baseline.json')
for ref in baseline['before']:assert sha(ROOT/ref['snapshot'])==ref['sha256']
before=yaml.safe_load((RESULTS/'actual-data-original-20261004/0-providers.yaml.bin').read_text(encoding='utf-8'))
after=yaml.safe_load((ROOT/'config/providers.yaml').read_text(encoding='utf-8'))
assert before['providers']==after['providers']
assert {k for k,v in before['input_capabilities'].items() if v!=after['input_capabilities'][k]}=={
    'ASTOCK-011','ASTOCK-014','ASTOCK-037','ASTOCK-044','ASTOCK-087'}
assert set(after['input_capabilities'])-set(before['input_capabilities'])=={'ASTOCK-037-profile','ASTOCK-037-events','ASTOCK-037-business'}
counts=dict(collections.Counter(v['implementation_status'] for v in after['input_capabilities'].values()))
assert counts==dict(implemented_validation_only=64,unimplemented=2,blocked=8,alias=3)
tree=ast.parse((ROOT/'src/stock_data_manage/routing/factory.py').read_text(encoding='utf-8'))
methods=next(ast.literal_eval(n.value) for n in ast.walk(tree) if isinstance(n,ast.Assign)
             and any(isinstance(t,ast.Name) and t.id=='expected_methods' for t in n.targets))
assert set(methods)=={k for k,v in after['input_capabilities'].items() if v['implementation_status']=='implemented_validation_only'}
legacy=[]
for snapshot,relative in [('3-boards.py.bin','akshare/boards.py'),('4-industry.py.bin','baostock/industry.py'),
                          ('6-news.py.bin','eastmoney/news.py'),('7-financial.py.bin','eastmoney/financial.py')]:
    old=ast.parse((RESULTS/'actual-data-original-20261004'/snapshot).read_text(encoding='utf-8'))
    current=ast.parse((ROOT/'src/stock_data_manage/providers'/relative).read_text(encoding='utf-8'))
    classes={n.name:n for n in current.body if isinstance(n,ast.ClassDef)}
    for cls in (n for n in old.body if isinstance(n,ast.ClassDef)):
        functions={n.name:n for n in classes[cls.name].body if isinstance(n,ast.FunctionDef)}
        for fn in (n for n in cls.body if isinstance(n,ast.FunctionDef)):
            assert ast.dump(fn,include_attributes=False)==ast.dump(functions[fn.name],include_attributes=False),(relative,fn.name)
            legacy.append(relative+':'+fn.name)
summary=read(FINAL/'comparison.json')
assert len(summary['inputs'])==5 and len(summary['negative_checks'])==17
for key in ['verification_code','runner']:assert sha(ROOT/summary[key]['path'])==summary[key]['sha256']
assert summary['network_requests']==summary['production_writes']==0
cli=read(RESULTS/'actual-data-cli-v2-20261004/summary.json');assert len(cli)==5
reports=[]
for directory in [FINAL, RESULTS/'actual-data-cli-v2-20261004']:
    for path in directory.rglob('report.json'):
        r=read(path)
        for ref in r['code_files']+r['config_files']:assert sha(ref['path'])==ref['sha256'],ref
        assert r['production_writes']==r['live_http_calls']==0 and not r['eligible_for_production_routing']
        run=path.parent
        while not (run/'_raw').exists():run=run.parent;assert run!=ROOT
        manifest=run/r['raw_manifest']['path'];assert sha(manifest)==r['raw_manifest']['sha256']
        for event in r['responses']:
            if event['outcome']!='response':continue
            body=RawObjectStore.read_response(manifest,event)
            assert len(body)==event.get('body_bytes',event.get('payload_bytes'))
            source=event.get('source_ref') or {}
            if source.get('manifest'):
                parent=read_line=Path(source['manifest']).read_text(encoding='utf-8').splitlines()[source['line']-1]
                assert RawObjectStore.read_response(source['manifest'],json.loads(parent))==body
            assert not _SECRET_BODY.search(body)
            assert all(v=='<redacted>' for k,v in event.get('request_headers',{}).items()
                       if k.lower() in {'cookie','authorization','accept-enckey','hexin-v'})
        sdk=r.get('sdk_dependency',{})
        if sdk.get('source_path'):assert sha(run/sdk['source_path'])==sdk['sha256']
        for dep in sdk.get('dependencies',[]):assert sha(run/dep['path'])==dep['sha256']
        for key in ['source_rows','parsed_rows','output','excluded_rows']:
            if key not in r:continue
            ref=r[key];data=read(run/ref['path']);assert sha(run/ref['path'])==ref['sha256'] and len(data)==ref['row_count']
            if key=='output':assert all(row[f] is None for row in data for f in r['unverified_fields'] if f in row)
        if r['status']=='failed':assert 'output' not in r
        reports.append(dict(path=path.relative_to(ROOT).as_posix(),sha256=sha(path),status=r['status'],row_count=r.get('row_count')))
for ref in summary['inputs']+summary['negative_checks']+cli:assert sha(ref['report_path'])==ref['report_sha256']
suites=ET.parse(RESULTS/'actual-data-final-20261004-tests.xml').getroot().findall('testsuite')
assert sum(int(s.attrib['tests']) for s in suites)==645
assert all(int(s.attrib[k])==0 for s in suites for k in ['failures','errors','skipped'])
probe=RESULTS/'actual-data-network-20261004/ASTOCK-087';result=read(probe/'result.json')
assert result['row_count']==718 and result['status']=='parsed'
assert sha(probe/'_raw/manifest.ndjson')==result['manifest_sha256'] and sha(probe/'parsed.json')==result['parsed_sha256']
assert len((probe/'_raw/manifest.ndjson').read_text(encoding='utf-8').splitlines())==9
for id in ['ASTOCK-011','ASTOCK-037-business']:
    cancellation=read(RESULTS/'actual-data-network-20261004'/id/'cancellation.json')
    assert cancellation['source_unavailable'] is False and cancellation['transport_timeout_changed'] is False
catalog=read(ROOT/'provider_validation/coverage/successful-input-capabilities.json')
assert {r['contract']['input_id'] for r in catalog['catalog']}==set(after['input_capabilities'])
assert all(not r['eligible_for_production_routing'] for r in catalog['catalog'])
target=FINAL/'verification.json';assert not target.exists()
target.write_bytes(json.dumps(dict(result='passed',validation_time_utc=datetime.now(timezone.utc).isoformat(),
    counts=counts,full_regression_tests=645,reports=reports,legacy_methods_preserved=legacy,cli_inputs_verified=5,
    original_vs_provider_scopes=5,negative_checks=17,production_writes=0,eligible_for_production_routing=False),ensure_ascii=False,indent=2).encode())
print('audit passed:',len(reports),'reports;',len(legacy),'existing methods unchanged; 645 tests')
