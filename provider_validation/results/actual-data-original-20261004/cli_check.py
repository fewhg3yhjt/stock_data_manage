"""Verify the existing user-facing CLI with actual source scopes, replay only."""
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('actual_data_cases', ROOT/'tests/test_input_collection.py')
checks = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(ROOT/'src'))
spec.loader.exec_module(checks)
out = ROOT/'provider_validation/results/actual-data-cli-v2-20261004'
out.mkdir(exist_ok=False)
results = []
env = dict(os.environ, PYTHONPATH=str(ROOT/'src'), PYTHONIOENCODING='utf-8')
for i, (input_id, context, manifest, count) in enumerate(checks.ACTUAL_DATA_CASES, 1):
    directory = out/str(i);directory.mkdir()
    context_path = directory/'context.json'
    context_path.write_bytes(json.dumps(context, ensure_ascii=False).encode())
    command = [sys.executable, '-m', 'stock_data_manage.cli', 'collect-input', '--input', input_id,
        '--config-root', str(ROOT/'config'), '--output-root', str(directory/'candidate'),
        '--mode', 'replay', '--replay-manifest', str(manifest), '--context-file', str(context_path)]
    run = subprocess.run(command, cwd=ROOT, env=env, capture_output=True)
    (directory/'stdout.json').write_bytes(run.stdout)
    (directory/'stderr.log').write_bytes(run.stderr)
    assert run.returncode==0, (input_id, run.stderr.decode('utf-8'), run.stdout.decode('utf-8')[-400:])
    report = json.loads(run.stdout.decode('utf-8'))
    assert report['status']=='candidate_complete' and report['row_count']==count
    assert report['production_writes']==report['live_http_calls']==0 and not report['eligible_for_production_routing']
    results.append(dict(input_id=input_id,row_count=count,result='passed',command=command,report_path=report['report_path'],
        report_sha256=hashlib.sha256(Path(report['report_path']).read_bytes()).hexdigest()))
(out/'summary.json').write_bytes(json.dumps(results, ensure_ascii=False, indent=2).encode())
print('CLI replay passed:', len(results))
