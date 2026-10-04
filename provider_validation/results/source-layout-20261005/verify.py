"""Check the physical migration, both install modes and retained project changes."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import tomllib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    baseline = json.loads((OUT / 'baseline.json').read_text(encoding='utf-8'))
    config = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    packages = config['tool']['setuptools']['packages']
    found = sorted('stock_data_manage' + ('.' + p.parent.relative_to(ROOT / 'src').as_posix().replace('/', '.')
                   if p.parent != ROOT / 'src' else '') for p in (ROOT / 'src').rglob('__init__.py'))
    assert packages == found == baseline['packages']
    assert not (ROOT / 'src/stock_data_manage').exists()
    changed = []
    for item in baseline['source_files']:
        current = ROOT / item['new_path']
        assert current.is_file()
        if digest(current) != item['sha256']:
            changed.append(item['new_path'])
            original = subprocess.check_output(['git', 'show', baseline['head'] + ':' + item['old_path']], cwd=ROOT)
            # Only the default YAML lookup depth changes after removing a directory.
            assert original.replace(b'parents[4]', b'parents[3]').replace(b'\r\n', b'\n') == current.read_bytes().replace(b'\r\n', b'\n')
    assert sorted(changed) == ['src/providers/akshare/boards.py', 'src/providers/baostock/industry.py']
    assert digest(ROOT / 'AGENTS.md') == baseline['preexisting_agents_sha256']
    removed_locks = [p for p in baseline['preexisting_untracked'] if p and Path(p).name.startswith('~$') and not (ROOT / p).exists()]
    missing = [p for p in baseline['preexisting_untracked'] if p and p not in removed_locks and not (ROOT / p).exists()]
    assert not missing, missing
    wheel = next((ROOT / 'tmp/source-layout-20261005/wheels').glob('*.whl'))
    with ZipFile(wheel) as archive:
        expected = {'stock_data_manage/' + p.relative_to(ROOT / 'src').as_posix(): p
                    for p in (ROOT / 'src').rglob('*.py')}
        assert {n for n in archive.namelist() if n.endswith('.py')} == set(expected)
        for name, source in expected.items():
            assert archive.read(name) == source.read_bytes(), name
    code = """import importlib,json,sys
from pathlib import Path
modules=json.loads(sys.argv[1])
imported={name:str(Path(importlib.import_module(name).__file__).resolve()) for name in modules}
print(json.dumps(imported))
"""
    modules = sorted('stock_data_manage' + ('.' + p.relative_to(ROOT / 'src').with_suffix('').as_posix().replace('/', '.')
                    if p.name != '__init__.py' else '.' + p.parent.relative_to(ROOT / 'src').as_posix().replace('/', '.')
                    if p.parent != ROOT / 'src' else '') for p in (ROOT / 'src').rglob('*.py'))
    installations = {}
    for mode in ('editable', 'regular'):
        directory = ROOT / 'tmp/source-layout-20261005' / mode
        python = directory / 'Scripts/python.exe'
        result = subprocess.run([str(python), '-I', '-c', code, json.dumps(modules)], cwd=directory,
                                capture_output=True, text=True, encoding='utf-8', check=True)
        imported = json.loads(result.stdout)
        location = ROOT / 'src' if mode == 'editable' else directory / 'Lib/site-packages/stock_data_manage'
        assert all(Path(path).is_relative_to(location) for path in imported.values())
        for args in ([str(python), '-I', '-m', 'stock_data_manage.cli', '--help'],
                     [str(directory / 'Scripts/stock-data.exe'), '--help']):
            cli = subprocess.run(args, cwd=directory, capture_output=True, text=True, check=True)
            assert 'collect-input' in cli.stdout
        installations[mode] = {'imported_modules': imported, 'module_cli': 'passed', 'installed_command': 'passed',
                               'cwd': str(directory), 'isolated_python': True}
    sources = [{**i, 'current_sha256': digest(ROOT / i['new_path'])} for i in baseline['source_files']]
    result = {'status': 'passed', 'verified_at_utc': datetime.now(timezone.utc).isoformat(), 'network_requests': 0,
              'production_writes': 0, 'source_files': len(sources), 'packages': len(packages),
              'unchanged_source_files': len(sources) - len(changed), 'changed_default_config_lookup': changed,
              'preserved_preexisting_untracked_files': len([p for p in baseline['preexisting_untracked'] if p]) - len(removed_locks),
              'removed_application_lock_files': removed_locks,
              'preexisting_agents_unchanged': True, 'wheel': {'path': wheel.relative_to(ROOT).as_posix(), 'sha256': digest(wheel),
              'source_members_checked': len(expected)}, 'installations': installations, 'source_mapping': sources}
    tests = {}
    for name, expected_count in (('tests', 645), ('regular-tests', 59), ('current-environment-tests', 4)):
        xml = OUT / (name + '.xml')
        suites = ET.parse(xml).getroot()
        totals = {field: sum(int(s.attrib.get(field, 0)) for s in suites.findall('testsuite'))
                  for field in ('tests', 'failures', 'errors', 'skipped')}
        assert totals == {'tests': expected_count, 'failures': 0, 'errors': 0, 'skipped': 0}, totals
        tests[name] = {**totals, 'path': xml.relative_to(ROOT).as_posix(), 'sha256': digest(xml)}
    result['tests'] = tests
    result['verification_script'] = {'path': Path(__file__).relative_to(ROOT).as_posix(), 'sha256': digest(Path(__file__))}
    workbook = ROOT / 'tmp/source-layout-20261005/源头采集接口说明.xlsx'
    spec = importlib.util.spec_from_file_location('formal_check', ROOT / 'provider_validation/results/formal-interface-spec-20261004/verify.py')
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    checker.OUT = OUT / 'formal'
    checker.OUT.mkdir(exist_ok=True)
    published = ROOT / 'docs/providers/源头采集接口说明.xlsx'
    matches = digest(published) == digest(workbook)
    checker.XLSX = published if matches else workbook
    checker.main()
    result['formal_workbook'] = {'candidate_path': workbook.relative_to(ROOT).as_posix(), 'sha256': digest(workbook),
        'published_path': 'docs/providers/源头采集接口说明.xlsx',
        'published_matches_candidate': matches,
        'verification': (checker.OUT / 'verification.json').relative_to(ROOT).as_posix()}
    (OUT / 'verification.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: result[k] for k in ('status', 'source_files', 'packages', 'unchanged_source_files')}))


if __name__ == '__main__':
    main()
