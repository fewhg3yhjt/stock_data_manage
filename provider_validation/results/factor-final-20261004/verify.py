"""Independent audit of source preservation, candidate scope and saved evidence."""
from pathlib import Path
import ast,collections,hashlib,importlib.util,json,subprocess,sys
from datetime import datetime,timezone
import xml.etree.ElementTree as ET
import yaml
ROOT=Path(__file__).resolve().parents[3]
results=ROOT/'provider_validation/results'
sys.path.insert(0,str(ROOT/'src'))

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def main():
    prior=results/'reports-calendar-final-20261004/verify.py'
    spec=importlib.util.spec_from_file_location('previous_evidence_audit',prior)
    audit_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit_module)
    original=json.loads((results/'factor-original-20261004/investigation.json').read_text(encoding='utf-8'))
    for ref in original['before']:assert sha(ROOT/ref['snapshot'])==ref['sha256']
    for name in ('source_code','raw_manifest'):assert sha(ROOT/original[name])==original[name+'_sha256']
    baseline=yaml.safe_load((results/'factor-original-20261004/4-providers.yaml.bin').read_text(encoding='utf-8'))
    current=yaml.safe_load((ROOT/'config/providers.yaml').read_text(encoding='utf-8'))
    assert baseline['providers']==current['providers']
    for name,contract in baseline['input_capabilities'].items():
        if name!='ASTOCK-006':assert contract==current['input_capabilities'][name],name
    counts=dict(collections.Counter(v['implementation_status'] for v in current['input_capabilities'].values()))
    assert counts=={'implemented_validation_only':39,'unimplemented':25,'blocked':7,'alias':3}
    methods=next(ast.literal_eval(n.value) for n in ast.walk(ast.parse((ROOT/'src/stock_data_manage/routing/factory.py').read_text(encoding='utf-8')))
        if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='expected_methods' for t in n.targets))
    assert set(methods)=={key for key,value in current['input_capabilities'].items() if value['implementation_status']=='implemented_validation_only'}
    def legacy_methods(path):
        tree=ast.parse(path.read_text(encoding='utf-8'))
        return {n.name:ast.dump(n,include_attributes=False) for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name in {'fetch_daily','fetch_futures_kline'}}
    assert legacy_methods(results/'factor-original-20261004/0-daily.py.bin')==legacy_methods(ROOT/'src/stock_data_manage/providers/sina/daily.py')
    summary=json.loads((results/'factor-final-20261004/comparison.json').read_text(encoding='utf-8'))
    assert len(summary['inputs'])==2 and summary['network_requests']==summary['production_writes']==0
    for field in ('runner','verification_code'):assert sha(ROOT/summary[field]['path'])==summary[field]['sha256']
    for ref in summary['original_vs_provider']:
        assert ref['all_business_fields_equal'] and ref['all_retained_source_fields_equal'] and ref['request_comparison_equal']
        assert sha(ref['original_parsed_path'])==ref['original_parsed_sha256'] and sha(ref['report_path'])==ref['report_sha256']
        for csv in ref['original_csv']:assert sha(csv['path'])==csv['sha256']
    for ref in summary['negative_checks']:
        assert sha(ROOT/ref['report_path'])==ref['report_sha256']
        report=json.loads((ROOT/ref['report_path']).read_text(encoding='utf-8'))
        assert report['status']=='failed' and report['failure_class']==ref['failure_class']
    cli=json.loads((results/'factor-cli-20261004/verification.json').read_text(encoding='utf-8'))
    assert sha(results/'factor-cli-20261004/verify.py')==cli['script_sha256']
    for ref in cli['results']:
        assert ref['exit_code']==0 and ref['count']==33
        for field in ('stdout','stderr'):assert sha(ROOT/ref[field])==ref[field+'_sha256']
    audited=[audit_module.audit(p) for prefix in ('factor-final-20261004','factor-cli-20261004') for p in sorted((results/prefix).rglob('report.json'))]
    for ref in audited:
        if ref['live_http_calls']:assert '/session/' in ref['path'] and ref['live_http_calls']==2
    fixture=json.loads((results/'factor-final-20261004/session/fixture-mode.json').read_text(encoding='utf-8'))
    assert fixture['real_http_calls']==0 and fixture['fixture_send_calls']==2
    proof=json.loads((results/'factor-final-20261004/path-comparison.json').read_text(encoding='utf-8'))
    assert proof['result']=='passed' and sha(results/'factor-final-20261004/path_check.py')==proof['script_sha256']
    for ref in proof['xml_refs']+proof['local_failed_reports']:assert sha(ROOT/ref['path'])==ref['sha256']
    suites=ET.parse(results/'factor-final-v2-20261004-tests.xml').getroot().findall('testsuite')
    tests=sum(int(s.get('tests')) for s in suites)
    assert tests==509 and all(int(s.get(k,'0'))==0 for s in suites for k in ('failures','errors','skipped'))
    # Development and failed long-path test evidence stays distinct from final reports.
    paths={p for prefix in ('factor-original-20261004','factor-final-20261004','factor-cli-20261004','factor-smoke-20261004',
        'factor-tests-dev-20261004','factor-tests-dev2-20261004') for p in (results/prefix).rglob('*') if p.is_file() and '__pycache__' not in p.parts}
    paths.update(p for p in results.glob('factor-*.xml'))
    paths.update(ROOT/ref['path'] for ref in proof['local_failed_reports'])
    paths.add(prior)
    names=['.gitattributes','CODE_STRUCTURE.md','PROJECT_PROGRESS.md','config/providers.yaml','tests/test_input_collection.py',
        'provider_validation/tests/replay_input_capabilities.py','provider_validation/docs/2026-10-04-adjustment-factor-input-collection.md',
        'config/datasets/adjustment_factor.yaml','config/normalization/adjustment_factor.yaml']
    paths.update(ROOT/name for name in names)
    report=json.loads(Path(summary['original_vs_provider'][0]['report_path']).read_text(encoding='utf-8'))
    paths.update(Path(ref['path']) for ref in report['code_files']+report['config_files'])
    index=[dict(path=p.relative_to(ROOT).as_posix(),sha256=sha(p),bytes=p.stat().st_size) for p in sorted(paths)]
    target=results/'factor-final-20261004-verification.json'
    if target.exists():assert json.loads(target.read_text(encoding='utf-8'))['files']==index
    else:target.write_text(json.dumps(dict(validation_time_utc=datetime.now(timezone.utc).isoformat(),result='passed',tests=tests,counts=counts,
        real_network_requests=0,production_writes=0,eligible_for_production_routing=False,legacy_sina_methods_ast_equal=True,audited_reports=audited,files=index),
        ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    if '--check-staged' in sys.argv:
        for item in subprocess.check_output(['git','diff','--cached','--name-only','-z'],cwd=ROOT).split(b'\0'):
            if not item:continue
            name=item.decode('utf-8')
            assert name not in {'AGENTS.md','开发记录.md'} and '~$' not in name and '.env' not in name
            assert subprocess.check_output(['git','show',':'+name],cwd=ROOT)==(ROOT/name).read_bytes(),name
    print(json.dumps(dict(result='passed',tests=tests,audited_reports=len(audited),indexed_files=len(index))))

if __name__=='__main__':main()
