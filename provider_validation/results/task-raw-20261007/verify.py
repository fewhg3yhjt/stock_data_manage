import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET
import pyarrow.parquet as pq
from stock_data_manage.storage.integrity import Manifest, file_hash
from stock_data_manage.storage.raw import RawObjectStore

root = Path('provider_validation/results/task-raw-20261007')
checks = []
for name, expected in [('master', 5223), ('daily', 1)]:
    result = json.loads((root / (name + '-result.json')).read_text(encoding='utf-8'))
    assert result['status'] == 'published' and result['row_count'] == expected
    manifest_path = Path(result['published_manifest'])
    manifest = Manifest.load(manifest_path)
    assert manifest.verify(manifest_path.parent / 'data.parquet')
    assert not (manifest_path.parent / 'task-commit.json').exists()
    rows = pq.read_table(manifest_path.parent / 'data.parquet').to_pylist()
    assert len(rows) == expected
    key = lambda row: row['instrument_id'] if name == 'master' else (row['instrument_id'], row['trade_date'], row['adjustment'])
    assert len({key(row) for row in rows}) == expected
    units = []
    for unit in result['units'].values():
        report = unit['report']
        assert RawObjectStore.verify_manifest(unit['raw_manifest']) == unit['raw_hash']
        assert RawObjectStore.verify_manifest(Path(unit['current_raw']) / 'manifest.ndjson') == unit['raw_hash']
        assert file_hash(Path(unit['artifact'])) == unit['artifact_hash']
        assert report.get('live_http_calls', 0) == 0 and report.get('live_sdk_calls', 0) == 0
        raw_events = [json.loads(line) for line in Path(unit['raw_manifest']).read_text(encoding='utf-8').splitlines()]
        units.append({'input_id': unit['input_id'], 'parameters': report['parameters'],
            'source_response_hashes': [event.get('body_sha256') for event in raw_events if event.get('body_sha256')],
            'source_references': [event.get('source_ref') for event in raw_events if event.get('source_ref')],
            'code_version': report['code_version'], 'normalization_version': report['normalization_version'],
            'source_normalized_rows': report['normalized_parquet']['row_count'],
            'normalized_path': unit['artifact'], 'normalized_hash': unit['artifact_hash'],
            'raw_manifest': unit['raw_manifest'], 'raw_hash': unit['raw_hash']})
    checks.append({'task': name, 'published_rows': expected, 'published_manifest': str(manifest_path),
        'manifest_sha256': file_hash(manifest_path), 'data_sha256': manifest.content_hash,
        'coverage_denominator': expected if name == 'daily' else None,
        'coverage_basis': 'one requested security on one trading date' if name == 'daily' else 'returned SH/SZ stock rows; no independent full-market census',
        'units': units})
assert all(not path.exists() or not list(path.iterdir()) for path in [root / 'data-proof/raw/_tmp'])
xml = ET.parse(root / 'regression.xml').getroot()
suites = [xml] if xml.tag == 'testsuite' else list(xml.iter('testsuite'))
test_result = {field: sum(int(s.get(field, '0')) for s in suites) for field in ['tests', 'failures', 'errors', 'skipped']}
assert test_result['failures'] == test_result['errors'] == 0
inventory = [{'path': str(path.relative_to(root)).replace('\\', '/'), 'sha256': file_hash(path), 'bytes': path.stat().st_size}
             for path in sorted(root.rglob('*')) if path.is_file() and path.name != 'verification.json']
sources = ['src/cli.py', 'src/pipeline/inputs.py', 'src/pipeline/daily.py', 'src/service/instruments_update.py',
           'src/storage/raw.py', 'src/storage/parquet.py', 'src/storage/metadata.py', 'src/worker/recovery.py',
           'config/datasets/security_master.yaml', 'tests/test_collection_tasks.py']
summary = {'validation_time_utc': datetime.now(timezone.utc).isoformat(), 'mode': 'offline_archive_replay',
    'live_requests': 0, 'production_writes': 0, 'routing_eligibility': False,
    'scope': 'SH/SZ stock-list snapshot 2026-09-30 and XSHG:600519 daily bar 2026-09-18',
    'not_verified': ['ETF full coverage', 'BSE full coverage', 'all-market daily bars', 'online task publication', 'background scheduling'],
    'regression': test_result, 'regression_manifest': 'regression.xml', 'tasks': checks,
    'code_files': [{'path': path, 'sha256': file_hash(Path(path))} for path in sources], 'artifacts': inventory}
temporary = root / 'verification.tmp.json'
temporary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
os.replace(temporary, root / 'verification.json')
print(json.dumps({'tasks': [(c['task'], c['published_rows']) for c in checks], 'regression': test_result, 'artifact_count': len(inventory)}))
