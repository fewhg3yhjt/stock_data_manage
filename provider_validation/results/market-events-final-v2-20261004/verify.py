"""Independent immutable audit of retained responses, semantics, tests and staged bytes."""
from pathlib import Path
import ast, collections, hashlib, importlib.util, json, subprocess, sys
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import yaml
ROOT = Path(__file__).resolve().parents[3]
results = ROOT / 'provider_validation/results'
sys.path.insert(0, str(ROOT/'src'))
from stock_data_manage.storage.raw import RawObjectStore

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def main():
    prior = results/'reports-calendar-final-20261004/verify.py'
    spec = importlib.util.spec_from_file_location('prior_artifact_audit', prior)
    audit_module = importlib.util.module_from_spec(spec); spec.loader.exec_module(audit_module)
    original = json.loads((results/'market-events-original-20261004/investigation.json').read_text(encoding='utf-8'))
    for ref in original['before']: assert sha(ROOT/ref['snapshot']) == ref['sha256']
    for key in ('source_code','raw_manifest'): assert sha(ROOT/original[key]) == original[key+'_sha256']
    baseline = yaml.safe_load((results/'market-events-original-20261004/3-providers.yaml.bin').read_text(encoding='utf-8'))
    current = yaml.safe_load((ROOT/'config/providers.yaml').read_text(encoding='utf-8'))
    assert baseline['providers'] == current['providers']
    for name, contract in baseline['input_capabilities'].items():
        if name not in {'ASTOCK-020','ASTOCK-021'}: assert contract == current['input_capabilities'][name], name
    counts = dict(collections.Counter(v['implementation_status'] for v in current['input_capabilities'].values()))
    assert counts == {'implemented_validation_only':41,'unimplemented':23,'blocked':7,'alias':3}
    methods = next(ast.literal_eval(n.value) for n in ast.walk(ast.parse((ROOT/'src/stock_data_manage/routing/factory.py').read_text(encoding='utf-8')))
        if isinstance(n, ast.Assign) and any(isinstance(t,ast.Name) and t.id=='expected_methods' for t in n.targets))
    assert set(methods) == {k for k,v in current['input_capabilities'].items() if v['implementation_status']=='implemented_validation_only'}
    def class_methods(path):
        cls = next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.ClassDef) and n.name=='EastMoneyFinancialMainProvider')
        return {n.name:ast.dump(n,include_attributes=False) for n in cls.body if isinstance(n,ast.FunctionDef)}
    legacy = class_methods(results/'market-events-original-20261004/0-financial.py.bin')
    new = class_methods(ROOT/'src/stock_data_manage/providers/eastmoney/financial.py')
    assert all(new[name]==body for name,body in legacy.items())
    prefixes = ('market-events-final-20261004','market-events-final-v2-20261004')
    for prefix in prefixes:
        summary = json.loads((results/prefix/'comparison.json').read_text(encoding='utf-8'))
        assert summary['network_requests']==summary['production_writes']==0 and len(summary['inputs'])==2
        assert [r['row_count'] for r in summary['inputs']]==[84,167] and len(summary['negative_checks'])==28
        assert sha(ROOT/summary['verification_code']['path'])==summary['verification_code']['sha256']
        runner = ROOT/summary['runner']['path'] if prefix==prefixes[1] else results/'market-events-original-20261004/runner-first-final.py.bin'
        assert sha(runner)==summary['runner']['sha256']
        for ref in summary['original_vs_provider']:
            assert ref['all_business_fields_equal'] and ref['all_retained_source_fields_equal'] and ref['request_comparison_equal']
            for path_key, hash_key in (('original_csv_path','original_csv_sha256'),('original_parsed_path','original_parsed_sha256'),('report_path','report_sha256')):
                assert sha(ref[path_key])==ref[hash_key]
        for ref in summary['negative_checks']:
            assert sha(ROOT/ref['report_path'])==ref['report_sha256']
            report=json.loads((ROOT/ref['report_path']).read_text(encoding='utf-8'))
            assert report['status']=='failed' and report['failure_class']==ref['failure_class'] and 'output' not in report
        fixture=json.loads((results/prefix/'session/fixture-mode.json').read_text(encoding='utf-8'))
        assert fixture['real_http_calls']==0 and fixture['fixture_send_calls']==2
        for report_path in (results/prefix/'pages').rglob('report.json'):
            report=json.loads(report_path.read_text(encoding='utf-8'))
            assert report['row_count']==report['source_total_count']==501 and report['source_page_count']==2
    cli=json.loads((results/'market-events-cli-20261004/verification.json').read_text(encoding='utf-8'))
    assert sha(results/'market-events-cli-20261004/verify.py')==cli['script_sha256']
    assert [r['count'] for r in cli['results']]==[84,167]
    for ref in cli['results']:
        assert ref['exit_code']==0
        for key in ('stdout','stderr'): assert sha(ROOT/ref[key])==ref[key+'_sha256']
    audited=[audit_module.audit(p) for prefix in (*prefixes,'market-events-cli-20261004') for p in sorted((results/prefix).rglob('report.json'))]
    assert len(audited)==78
    for ref in audited:
        if ref['live_http_calls']: assert '/session/' in ref['path'] and ref['live_http_calls']==1
    # Inspect raw archives for all original-script requests, including failed single-stock queue.
    original_requests=[]
    for prefix in prefixes:
        for manifest in (results/prefix).glob('ASTOCK-*/original/manifest.ndjson'):
            for line in manifest.read_text(encoding='utf-8').splitlines():
                event=json.loads(line)
                if event.get('event')!='http_response': continue
                body=RawObjectStore.read_response(manifest,event)
                assert hashlib.sha256(body).hexdigest()==event['body_sha256']
                reference=event['source_ref']; archive=Path(reference['manifest'])
                source_record=json.loads(archive.read_text(encoding='utf-8').splitlines()[reference['line']-1])
                assert RawObjectStore.read_response(archive,source_record)==body
                original_requests.append(event['body_sha256'])
    assert len(original_requests)==10 and original_requests.count('ea6528a2204b61a7ee34683a73ca00f8afdc86aadf3f765d4dae39bbfb19dbe7')==2
    suites=ET.parse(results/'market-events-final-20261004-tests.xml').getroot().findall('testsuite')
    tests=sum(int(s.get('tests')) for s in suites)
    assert tests==544 and all(int(s.get(k,'0'))==0 for s in suites for k in ('failures','errors','skipped'))
    dev=json.loads((results/'market-events-dev-20261004/index.json').read_text(encoding='utf-8'))
    assert len(dev['investigations'])==7
    for ref in dev['investigations']:
        for file in ref['files']: assert sha(ROOT/file['saved_path'])==file['sha256']
    paths={p for directory in results.glob('market-events-*-20261004') if directory.is_dir() for p in directory.rglob('*')
        if p.is_file() and not p.is_symlink() and '__pycache__' not in p.parts}
    paths.update(results.glob('market-events-*20261004*.xml'))
    paths.add(prior)
    names=['.gitattributes','CODE_STRUCTURE.md','PROJECT_PROGRESS.md','config/providers.yaml','tests/test_input_collection.py',
        'provider_validation/tests/replay_input_capabilities.py','provider_validation/docs/2026-10-04-market-events-input-collection.md',
        'config/datasets/dragon_tiger_daily.yaml','config/normalization/dragon_tiger_daily.yaml','config/datasets/lockup_expiry.yaml','config/normalization/lockup_expiry.yaml']
    paths.update(ROOT/name for name in names)
    report=json.loads(Path(cli['results'][0]['report_path']).read_text(encoding='utf-8'))
    paths.update(Path(ref['path']) for ref in report['code_files']+report['config_files'])
    index=[dict(path=p.relative_to(ROOT).as_posix(),sha256=sha(p),bytes=p.stat().st_size) for p in sorted(paths)]
    target=results/'market-events-final-20261004-verification.json'
    if target.exists(): assert json.loads(target.read_text(encoding='utf-8'))['files']==index
    else: target.write_text(json.dumps(dict(validation_time_utc=datetime.now(timezone.utc).isoformat(),result='passed',tests=tests,counts=counts,
        real_network_requests=0,production_writes=0,eligible_for_production_routing=False,legacy_financial_methods_ast_equal=True,
        audited_reports=audited,original_requests_checked=len(original_requests),files=index),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    if '--check-staged' in sys.argv:
        for item in subprocess.check_output(['git','diff','--cached','--name-only','-z'],cwd=ROOT).split(b'\0'):
            if not item: continue
            name=item.decode('utf-8')
            assert name not in {'AGENTS.md','开发记录.md'} and '~$' not in name and '.env' not in name
            assert subprocess.check_output(['git','show',':'+name],cwd=ROOT)==(ROOT/name).read_bytes(), name
    print(json.dumps(dict(result='passed',tests=tests,audited_reports=len(audited),indexed_files=len(index))))

if __name__=='__main__': main()
