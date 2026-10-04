"""Exercise the real candidate CLI with original report and explicit historical seat evidence."""
from pathlib import Path
import hashlib, json, os, subprocess, sys
from datetime import datetime, timezone
ROOT=Path(__file__).resolve().parents[3]
output=Path(__file__).parent
archive=ROOT/'provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson'
cases=[('ASTOCK-008',{'request':{'symbol':'600519','start_date':'2026-09-01','end_date':'2026-10-03'}},1),
       ('ASTOCK-019',{'request':{'symbol':'600519','trade_date':'2013-01-28'},'calendar':{'trading_dates':['2013-01-28']}},10)]
results=[]
for index,(input_id,context,count) in enumerate(cases):
    context_file=output/(str(index)+'-context.json')
    context_file.write_text(json.dumps(context,indent=2)+'\n',encoding='utf-8')
    command=[sys.executable,'-m','stock_data_manage.cli','collect-input','--input',input_id,'--config-root',str(ROOT/'config'),
        '--output-root',str(output/'candidate'),'--mode','replay','--replay-manifest',str(archive),'--context-file',str(context_file)]
    completed=subprocess.run(command,cwd=ROOT,env=dict(os.environ,PYTHONPATH=str(ROOT/'src'),PYTHONIOENCODING='utf-8'),capture_output=True)
    stdout,stderr=output/(str(index)+'-stdout.json'),output/(str(index)+'-stderr.bin')
    assert not stdout.exists() and not stderr.exists()
    stdout.write_bytes(completed.stdout);stderr.write_bytes(completed.stderr)
    report=json.loads(completed.stdout)
    assert completed.returncode==0 and report['status']=='candidate_complete' and report['row_count']==count
    assert report['live_http_calls']==report['production_writes']==0
    assert report['returned_window']['first']==('2026-09-21' if input_id=='ASTOCK-008' else '2013-01-28')
    results.append(dict(input_id=input_id,command=command,exit_code=completed.returncode,count=count,
        stdout=stdout.relative_to(ROOT).as_posix(),stderr=stderr.relative_to(ROOT).as_posix(),stdout_sha256=hashlib.sha256(completed.stdout).hexdigest(),
        stderr_sha256=hashlib.sha256(completed.stderr).hexdigest(),report_path=report['report_path']))
(output/'verification.json').write_text(json.dumps(dict(validation_time_utc=datetime.now(timezone.utc).isoformat(),results=results,
    script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(dict(result='passed',input_counts=[1,10])))
