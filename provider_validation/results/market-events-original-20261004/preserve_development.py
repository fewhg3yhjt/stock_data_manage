"""Keep failed development reports and their exact source evidence outside ignored test space."""
from pathlib import Path
import hashlib, json, shutil
ROOT = Path(__file__).resolve().parents[3]
output = ROOT / 'provider_validation/results/market-events-dev-20261004'
assert not output.exists()
output.mkdir()
refs = []
for index, source in enumerate(sorted((ROOT/'tmp/me1').iterdir())):
    if not source.is_dir() or source.is_symlink() or source.name.endswith('current'): continue
    # Save only investigations which failed, preserving all dependent payloads/configuration.
    reports = list(source.rglob('report.json'))
    if not any(json.loads(p.read_text(encoding='utf-8')).get('failure_class') == 'AttributeError' for p in reports): continue
    target = output / ('d'+str(index))
    shutil.copytree(source, target, ignore=shutil.ignore_patterns('__pycache__'))
    files = [dict(original_path=str(source/p.relative_to(target)), saved_path=p.relative_to(ROOT).as_posix(),
        sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in target.rglob('*') if p.is_file()]
    refs.append(dict(original_directory=str(source), saved_directory=target.relative_to(ROOT).as_posix(), files=files))
(output/'index.json').write_text(json.dumps(dict(reason='initial pipeline branch accessed legacy board result attributes for new InputFetchResult; fixed explicit branch exclusion',
    original_test_xml='provider_validation/results/market-events-dev-20261004-tests.xml', investigations=refs), ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
print(json.dumps(dict(saved_investigations=len(refs), saved_files=sum(len(r['files']) for r in refs))))
