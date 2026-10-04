from pathlib import Path
import hashlib,json,subprocess,sys,os
from datetime import datetime,timezone
ROOT=Path(__file__).resolve().parents[3]
output=Path(__file__).resolve().parent
archive=ROOT/'provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson'
results=[]
for index,kind in enumerate(('qfq','hfq')):
    context_file=output/(str(index)+'-context.json')
    context_file.write_text(json.dumps({'request':{'symbol':'600519'},'config':{'kind':kind}},indent=2)+'\n',encoding='utf-8')
    command=[sys.executable,'-m','stock_data_manage.cli','collect-input','--input','ASTOCK-006','--config-root',str(ROOT/'config'),
             '--output-root',str(output/'candidate'),'--mode','replay','--replay-manifest',str(archive),'--context-file',str(context_file)]
    env=dict(os.environ,PYTHONPATH=str(ROOT/'src'),PYTHONIOENCODING='utf-8')
    completed=subprocess.run(command,cwd=ROOT,env=env,capture_output=True)
    stdout=output/(str(index)+'-stdout.json');stderr=output/(str(index)+'-stderr.bin')
    assert not stdout.exists() and not stderr.exists()
    stdout.write_bytes(completed.stdout);stderr.write_bytes(completed.stderr)
    report=json.loads(completed.stdout)
    assert completed.returncode==0 and report['status']=='candidate_complete' and report['row_count']==33
    assert report['live_http_calls']==report['production_writes']==0 and report['original_row_count']==66
    results.append(dict(input_id='ASTOCK-006',kind=kind,command=command,exit_code=completed.returncode,count=33,
        stdout=stdout.relative_to(ROOT).as_posix(),stderr=stderr.relative_to(ROOT).as_posix(),stdout_sha256=hashlib.sha256(completed.stdout).hexdigest(),
        stderr_sha256=hashlib.sha256(completed.stderr).hexdigest(),report_path=report['report_path']))
(output/'verification.json').write_text(json.dumps(dict(validation_time_utc=datetime.now(timezone.utc).isoformat(),results=results,
    script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(dict(result='passed',input_counts=[33,33])))
