"""Independently audit retained responses, source scope, version and staged bytes."""
from pathlib import Path
import ast, collections, hashlib, importlib.util, json, subprocess, sys
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import yaml

ROOT = Path(__file__).resolve().parents[3]
RESULTS = ROOT / 'provider_validation/results'
sys.path.insert(0, str(ROOT/'src'))
from stock_data_manage.storage.raw import RawObjectStore


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def suites(path, expected, failures=0):
    rows = ET.parse(path).getroot().findall('testsuite')
    assert sum(int(r.get('tests')) for r in rows) == expected
    assert sum(int(r.get('failures','0')) for r in rows) == failures
    assert all(int(r.get(k,'0')) == 0 for r in rows for k in ('errors','skipped'))


def main():
    prior = RESULTS/'reports-calendar-final-20261004/verify.py'
    spec = importlib.util.spec_from_file_location('prior_artifact_audit', prior)
    auditor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(auditor)
    original_dir = RESULTS/'reports-seats-original-20261004'
    original = read(original_dir/'investigation.json')
    discovery = read(original_dir/'date-evidence.json')
    for ref in original['before'] + discovery['before_additional']:
        assert sha(ROOT/ref['snapshot']) == ref['sha256']
    for key in ('source_code','raw_manifest'):
        assert sha(ROOT/original[key]) == original[key+'_sha256']
    assert hashlib.sha256(RawObjectStore.read_response(ROOT/discovery['raw_manifest'], discovery['record'])).hexdigest() == discovery['record']['body_sha256']
    assert discovery['decoded']['result']['count'] == 2
    baseline = yaml.safe_load((original_dir/'3-providers.yaml.bin').read_text(encoding='utf-8'))
    current = yaml.safe_load((ROOT/'config/providers.yaml').read_text(encoding='utf-8'))
    assert baseline['providers'] == current['providers']
    for name, contract in baseline['input_capabilities'].items():
        if name not in {'ASTOCK-008','ASTOCK-019'}:
            assert contract == current['input_capabilities'][name], name
    counts = dict(collections.Counter(v['implementation_status'] for v in current['input_capabilities'].values()))
    assert counts == {'implemented_validation_only':43,'unimplemented':21,'blocked':7,'alias':3}
    methods = next(ast.literal_eval(n.value) for n in ast.walk(ast.parse((ROOT/'src/stock_data_manage/routing/factory.py').read_text(encoding='utf-8')))
        if isinstance(n, ast.Assign) and any(isinstance(t,ast.Name) and t.id=='expected_methods' for t in n.targets))
    assert set(methods) == {k for k,v in current['input_capabilities'].items() if v['implementation_status']=='implemented_validation_only'}
    def class_methods(path):
        cls = next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.ClassDef) and n.name=='EastMoneyFinancialMainProvider')
        return {n.name:ast.dump(n,include_attributes=False) for n in cls.body if isinstance(n,ast.FunctionDef)}
    legacy = class_methods(original_dir/'0-financial.py.bin')
    new = class_methods(ROOT/'src/stock_data_manage/providers/eastmoney/financial.py')
    assert all(new[name] == body for name,body in legacy.items())
    final = RESULTS/'reports-seats-final-20261004'
    summary = read(final/'comparison.json')
    assert summary['network_requests'] == summary['production_writes'] == 0
    assert summary['eligible_for_production_routing'] is False
    assert [r['row_count'] for r in summary['inputs']] == [1,10]
    assert len(summary['negative_checks']) == 23
    for key in ('verification_code','runner'):
        assert sha(ROOT/summary[key]['path']) == summary[key]['sha256']
    for ref in summary['inputs']:
        assert sha(ROOT/ref['source_manifest']) == ref['source_manifest_sha256']
        assert sha(ROOT/ref['report_path']) == ref['report_sha256']
    for ref in summary['original_vs_provider']:
        assert ref['all_business_fields_equal'] and ref['all_retained_source_fields_equal'] and ref['request_comparison_equal']
        for csv in ref['original_csv']:
            assert sha(csv['path']) == csv['sha256']
        for key in ('original_parsed','report'):
            assert sha(ref[key+'_path']) == ref[key+'_sha256']
        if ref['input_id'] == 'ASTOCK-019':
            assert ref['original_requested_date'] == '20260930' and ref['actual_seat_day'] == '20130128'
            assert ref['original_request_count'] == 3 and ref['provider_request_count'] == 2
        else:
            assert ref['original_request_count'] == ref['provider_request_count'] == 1
        report = read(ref['report_path'])
        assert report['source_fallback_enabled'] is False and report['implicit_date_selection'] is False
        if ref['input_id'] == 'ASTOCK-008':
            assert report['pdf_requests'] == 0 and report['publication_time_precision_verified'] is False
            assert report['source_total_count'] == 1 and report['coverage_complete'] is True
        else:
            assert report['date_discovery_requests'] == 0 and report['parameters']['date'] == '2013-01-28'
    for ref in summary['negative_checks']:
        assert sha(ROOT/ref['report_path']) == ref['report_sha256']
        report = read(ROOT/ref['report_path'])
        assert report['status'] == 'failed' and report['failure_class'] == ref['failure_class'] and 'output' not in report
    assert read(final/'session/fixture-mode.json') == {'mode':'injected Session; not real live validation','real_http_calls':0,'fixture_send_calls':3}
    for mode, count, limited in (('complete',101,False),('limited',200,True)):
        report = read(next((final/('pages-'+mode)).rglob('report.json')))
        assert report['row_count'] == count and report['result_limited'] is limited
        assert report['coverage_complete'] is (not limited) and report['retrieved_pages'] == [1,2]
    for mode in ('totals_drift','duplicate_cross_page'):
        report = read(next((final/('pages-'+mode)).rglob('report.json')))
        assert report['status'] == 'failed' and 'output' not in report
    cli_dir = RESULTS/'reports-seats-cli-20261004'
    cli = read(cli_dir/'verification.json')
    assert sha(cli_dir/'verify.py') == cli['script_sha256']
    assert [r['count'] for r in cli['results']] == [1,10]
    for ref in cli['results']:
        assert ref['exit_code'] == 0
        for key in ('stdout','stderr'):
            assert sha(ROOT/ref[key]) == ref[key+'_sha256']
    prefixes = ('reports-seats-final-20261004','reports-seats-cli-20261004','reports-seats-smoke-20261004')
    audited = [auditor.audit(p) for prefix in prefixes for p in sorted((RESULTS/prefix).rglob('report.json'))]
    assert len(audited) == 42
    for ref in audited:
        if ref['live_http_calls']:
            assert '/session/' in ref['path'] and ref['live_http_calls'] in {1,2}
    original_requests = []
    for manifest in final.glob('ASTOCK-*/original/manifest.ndjson'):
        for line in manifest.read_text(encoding='utf-8').splitlines():
            event = json.loads(line)
            if event.get('event') != 'http_response':
                continue
            body = RawObjectStore.read_response(manifest,event)
            assert hashlib.sha256(body).hexdigest() == event['body_sha256']
            reference = event['source_ref']
            archive = Path(reference['manifest'])
            source_record = json.loads(archive.read_text(encoding='utf-8').splitlines()[reference['line']-1])
            assert RawObjectStore.read_response(archive,source_record) == body
            original_requests.append(event['body_sha256'])
    assert len(original_requests) == 4 and discovery['record']['body_sha256'] in original_requests
    suites(RESULTS/'reports-seats-final-20261004-tests.xml',578)
    suites(RESULTS/'reports-seats-dev3-20261004-tests.xml',34)
    suites(RESULTS/'reports-seats-dev-20261004-tests.xml',30,1)
    suites(RESULTS/'reports-seats-dev2-20261004-tests.xml',34,1)
    dev = read(RESULTS/'reports-seats-dev-20261004/index.json')
    assert len(dev['investigations']) == 2 and sum(len(r['files']) for r in dev['investigations']) == 194
    for ref in dev['investigations']:
        for file in ref['files']:
            assert sha(ROOT/file['saved_path']) == file['sha256']
    ownership = read(original_dir/'ownership-check.json')
    assert ownership['owner'] == '书非の主机\\sp181' and ownership['verified'] > 0
    paths = {p for directory in RESULTS.glob('reports-seats-*-20261004') if directory.is_dir() for p in directory.rglob('*')
        if p.is_file() and not p.is_symlink() and '__pycache__' not in p.parts}
    paths.update(RESULTS.glob('reports-seats-*20261004*.xml'))
    paths.add(prior)
    names = ['.gitattributes','CODE_STRUCTURE.md','PROJECT_PROGRESS.md','config/providers.yaml','tests/test_input_collection.py',
        'provider_validation/tests/replay_input_capabilities.py','provider_validation/docs/2026-10-04-reports-seats-input-collection.md']
    names += [f'config/{folder}/{dataset}.yaml' for folder in ('datasets','normalization') for dataset in ('eastmoney_reports','dragon_tiger_seats')]
    paths.update(ROOT/name for name in names)
    report = read(cli['results'][0]['report_path'])
    paths.update(Path(ref['path']) for ref in report['code_files']+report['config_files'])
    index = [dict(path=p.relative_to(ROOT).as_posix(),sha256=sha(p),bytes=p.stat().st_size) for p in sorted(paths)]
    target = RESULTS/'reports-seats-final-20261004-verification.json'
    if target.exists():
        assert read(target)['files'] == index
    else:
        target.write_bytes((json.dumps(dict(validation_time_utc=datetime.now(timezone.utc).isoformat(),result='passed',tests=578,counts=counts,
            real_network_requests=0,production_writes=0,eligible_for_production_routing=False,legacy_financial_methods_ast_equal=True,
            audited_reports=audited,original_requests_checked=4,files=index),ensure_ascii=False,indent=2)+'\n').encode('utf-8'))
    if '--check-staged' in sys.argv:
        for item in subprocess.check_output(['git','diff','--cached','--name-only','-z'],cwd=ROOT).split(b'\0'):
            if not item:
                continue
            name = item.decode('utf-8')
            assert name not in {'AGENTS.md','开发记录.md'} and '~$' not in name and '.env' not in name
            assert subprocess.check_output(['git','show',':'+name],cwd=ROOT) == (ROOT/name).read_bytes(), name
    print(json.dumps(dict(result='passed',tests=578,audited_reports=len(audited),indexed_files=len(index))))


if __name__ == '__main__':
    main()
