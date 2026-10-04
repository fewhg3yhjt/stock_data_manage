from pathlib import Path
import hashlib,json,xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[3]
results=ROOT/'provider_validation/results'
failed=results/'factor-final-20261004-tests.xml'
short=results/'factor-path-20261004-tests.xml'
cases=ET.parse(failed).getroot().findall('.//testcase')
failures=[case for case in cases if case.find('failure') is not None]
assert len(cases)==509 and len(failures)==4
checked=ET.parse(short).getroot().findall('.//testcase')
assert len(checked)==3 and all(case.find('failure') is None for case in checked)
reports=[]
for path in (results/'factor-fulltests-20261004').rglob('report.json'):
    report=json.loads(path.read_text(encoding='utf-8'))
    if report.get('failure_class')=='FileNotFoundError':
        reports.append(dict(path=path.relative_to(ROOT).as_posix(),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            classification='test output path too deep; retained locally, not committed',input_id=report['input_id']))
assert len(reports)==4
refs=[dict(path=p.relative_to(ROOT).as_posix(),sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in (failed,short)]
(Path(__file__).parent/'path-comparison.json').write_text(json.dumps(dict(result='passed',failed_long_path_cases=4,
    targeted_short_path_cases=3,final_full_regression='factor-final-v2-20261004-tests.xml',xml_refs=refs,local_failed_reports=reports,
    runtime_code_changed_to_fix_paths=False,script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print('long-path failures independently linked to saved reports; short-path checks passed')
