"""Independent source, version, response, mapping and delivery audit."""
from pathlib import Path
import ast,collections,hashlib,importlib.util,inspect,json,subprocess,sys
from datetime import datetime,timezone
import xml.etree.ElementTree as ET
import yaml
ROOT=Path(__file__).resolve().parents[3]
RESULTS=ROOT/'provider_validation/results'
sys.path.insert(0,str(ROOT/'src'))
from stock_data_manage.storage.raw import RawObjectStore
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))

def audit_report(path,versions=None):
    report=read(path)
    for ref in report['code_files']+report['config_files']:
        if versions is None:assert sha(ref['path'])==ref['sha256'],ref
        else:assert (ref['path'],ref['sha256']) in versions,ref
    assert report['production_writes']==0 and not report['eligible_for_production_routing']
    if 'raw_manifest' not in report:
        assert report['status']=='failed' and not report.get('responses') and 'output' not in report
        return dict(path=str(path.relative_to(ROOT)),status=report['status'],scope='before-request')
    run=path.parent
    while not (run/'_raw').exists():run=run.parent;assert run!=ROOT
    manifest=run/report['raw_manifest']['path'];assert sha(manifest)==report['raw_manifest']['sha256']
    for event in report.get('responses',[]):
        if event.get('outcome')!='response':continue
        body=RawObjectStore.read_response(manifest,event)
        assert len(body)==event['body_bytes']
        original=event.get('source_ref') or {}
        if 'manifest' in original:
            rows=[json.loads(l) for l in Path(original['manifest']).read_text(encoding='utf-8').splitlines()]
            parent=rows[original['line']-1]
            assert RawObjectStore.read_response(original['manifest'],parent)==body
        assert all(v=='<redacted>' for k,v in event['request_headers'].items() if k.lower() in {'cookie','authorization','hexin-v'})
    sdk=report.get('sdk_dependency',{})
    if sdk.get('source_path'):assert sha(run/sdk['source_path'])==sdk['sha256']
    for name in ['source_rows','parsed_rows','output','excluded_rows']:
        if name not in report:continue
        ref=report[name];p=run/ref['path'];assert sha(p)==ref['sha256']
        rows=read(p);assert len(rows)==ref['row_count']
        if versions is None:
            json.loads(p.read_text(encoding='utf-8'),parse_constant=lambda _:(_ for _ in ()).throw(AssertionError('nonfinite JSON')))
        if name=='output':
            assert all(row[f] is None for row in rows for f in report['unverified_fields'])
    if report['status']=='failed':assert 'output' not in report
    return dict(path=str(path.relative_to(ROOT)),sha256=sha(path),status=report['status'],row_count=report.get('row_count'))

def main():
    original=RESULTS/'remaining-original-20261004';inventory=read(original/'inventory.json')
    for ref in inventory['before']:assert sha(ROOT/ref['snapshot'])==ref['sha256']
    for ref in inventory['source_refs']:assert sha(ROOT/ref['path'])==ref['sha256']
    for ref in inventory['sdk_sources']:assert sha(ROOT/ref['path'])==ref['sha256']
    before=yaml.safe_load((original/'0-providers.yaml.bin').read_text(encoding='utf-8'))
    after=yaml.safe_load((ROOT/'config/providers.yaml').read_text(encoding='utf-8'))
    assert before['providers']==after['providers']
    changed={id for id,v in after['input_capabilities'].items() if before['input_capabilities'][id]!=v}
    assert changed=={'ASTOCK-'+s for s in ['003','004','030','031','039','051','055','063','067','068','069','071','072','073','077','085']}
    counts=dict(collections.Counter(v['implementation_status'] for v in after['input_capabilities'].values()))
    assert counts==dict(implemented_validation_only=59,unimplemented=5,blocked=7,alias=3)
    tree=ast.parse((ROOT/'src/stock_data_manage/routing/factory.py').read_text(encoding='utf-8'))
    methods=next(ast.literal_eval(n.value) for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='expected_methods' for t in n.targets))
    assert set(methods)=={id for id,v in after['input_capabilities'].items() if v['implementation_status']=='implemented_validation_only'}
    nodes=collections.defaultdict(list)
    for p in (ROOT/'src/stock_data_manage/providers').rglob('*.py'):
        for n in ast.parse(p.read_text(encoding='utf-8')).body:
            if isinstance(n,ast.FunctionDef):nodes[n.name].append(n)
    provenance=read(original/'migration-provenance.json')
    for ref in provenance['original_functions']:
        candidates=nodes[ref['function']]
        if ref['function']=='_v39_http':
            for n in candidates:n.body=[item for item in n.body if not isinstance(item,ast.Import)]
        assert any(hashlib.sha256(ast.dump(n,include_attributes=False).encode()).hexdigest()==ref['ast_sha256'] for n in candidates),ref['function']
    historical=[]
    for name in ['remaining-sdk-smoke-20261004','remaining-original-smoke-20261004','remaining-final-20261004']:
        directory=RESULTS/name
        refs=read(directory/'version/index.json')
        if (directory/'version/index-extra.json').exists():refs+=read(directory/'version/index-extra.json')
        versions={(ref['path'],ref['sha256']) for ref in refs}
        for ref in refs:assert sha(ROOT/ref['snapshot'])==ref['sha256']
        historical += [audit_report(p,versions) for p in directory.rglob('report.json')]
    final=Path(__file__).parent;summary=read(final/'comparison.json')
    assert len(summary['inputs'])==21 and len(summary['negative_checks'])==14
    assert summary['network_requests']==summary['production_writes']==0 and not summary['eligible_for_production_routing']
    for name in ['verification_code','runner']:assert sha(ROOT/summary[name]['path'])==summary[name]['sha256']
    for ref in summary['original_vs_provider']:
        assert ref['all_business_fields_equal'] and ref['request_comparison_equal']
        assert sha(ref['original_parsed_path'])==ref['original_parsed_sha256']
        assert sha(ref['report_path'])==ref['report_sha256']
    current=[audit_report(p) for p in final.rglob('report.json')]
    suites=ET.parse(RESULTS/'remaining-tests-20261004/full-dev.xml').getroot().findall('testsuite')
    assert sum(int(s.attrib['tests']) for s in suites)==617
    assert all(int(s.attrib[k])==0 for s in suites for k in ['failures','errors','skipped'])
    for id,count in [('ASTOCK-031',10),('ASTOCK-069',20)]:
        root=RESULTS/'remaining-evidence-direct-20261004'/id;probe=read(root/'result.json')
        assert probe['row_count']==count and probe['status']=='parsed'
        assert sha(ROOT/probe['manifest'])==probe['manifest_sha256'] and sha(ROOT/probe['parsed_path'])==probe['parsed_sha256']
        for line in (ROOT/probe['manifest']).read_text(encoding='utf-8').splitlines():
            e=json.loads(line);RawObjectStore.read_response(ROOT/probe['manifest'],e)
    st=read(RESULTS/'remaining-st-evidence-20261004/result.json')
    assert st['source_row_count']==317 and st['row_count']==198
    for name in ['source_payload','parsed']:assert sha(ROOT/st[name+'_path'])==st[name+'_sha256']
    result=dict(result='passed',validation_time_utc=datetime.now(timezone.utc).isoformat(),counts=counts,changed_inputs=sorted(changed),
        current_reports=current,historical_reports=historical,full_tests=617,live_probe_scopes=['news 600519 keyword page 1:10','CSIndex 000300:20 dates','BaoStock ST-name query:317 -> 198 SDK rows; still unimplemented'],
        production_writes=0,eligible_for_production_routing=False,remaining_unimplemented=['ASTOCK-011','ASTOCK-014','ASTOCK-037','ASTOCK-044','ASTOCK-087'])
    (final/'audit.json').write_bytes((json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode())
    print('audit passed',len(current),'current reports',len(historical),'historical reports',counts)

if __name__=='__main__':main()
