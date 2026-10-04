"""Preserve rejected incomplete field-selection investigations without rewriting evidence."""
from pathlib import Path
import hashlib,json,shutil
ROOT=Path(__file__).resolve().parents[3]
output=ROOT/'provider_validation/results/reports-seats-dev-20261004'
assert not output.exists();output.mkdir()
refs=[]
for index,root in enumerate(('rs1','rs2')):
    source=ROOT/'tmp'/root/'test_reports_seats_yaml_projec0'
    target=output/('d'+str(index));shutil.copytree(source,target,ignore=shutil.ignore_patterns('__pycache__'))
    files=[dict(original_path=str(source/p.relative_to(target)),saved_path=p.relative_to(ROOT).as_posix(),sha256=hashlib.sha256(p.read_bytes()).hexdigest())
        for p in target.rglob('*') if p.is_file()]
    refs.append(dict(original_directory=str(source),saved_directory=target.relative_to(ROOT).as_posix(),files=files))
(output/'index.json').write_text(json.dumps(dict(reason='test projection omitted mandatory report_date/source_publish_text; input correctly rejected before requests; fixed test fields, no runtime defect',
    selected_fields=['snapshot_at','source_security_code','report_id','title'],investigations=refs),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(dict(investigations=len(refs),saved_files=sum(len(r['files']) for r in refs))))
