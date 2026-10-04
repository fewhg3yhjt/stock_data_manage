from pathlib import Path
import hashlib,json,subprocess,sys,os
from datetime import datetime,timezone
ROOT=Path(__file__).resolve().parents[3]
output=Path(__file__).resolve().parent
archive=ROOT/"provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson"
results=[]
for input_id in ("ASTOCK-061","ASTOCK-062"):
 command=[sys.executable,"-m","stock_data_manage.cli","collect-input","--input",input_id,"--config-root",str(ROOT/"config"),"--output-root",str(output/"candidate"),"--mode","replay","--replay-manifest",str(archive)]
 env=dict(os.environ,PYTHONPATH=str(ROOT/"src"),PYTHONIOENCODING="utf-8")
 result=subprocess.run(command,cwd=ROOT,env=env,capture_output=True)
 stdout=output/(input_id+"-stdout.json");stderr=output/(input_id+"-stderr.bin")
 assert not stdout.exists() and not stderr.exists()
 stdout.write_bytes(result.stdout);stderr.write_bytes(result.stderr)
 report=json.loads(result.stdout)
 assert result.returncode==0 and report["status"]=="candidate_complete" and report["row_count"]==({"ASTOCK-061":136,"ASTOCK-062":225}[input_id]) and report["live_http_calls"]==report["production_writes"]==0
 results.append(dict(input_id=input_id,command=command,exit_code=result.returncode,count=report["row_count"],stdout=stdout.relative_to(ROOT).as_posix(),stderr=stderr.relative_to(ROOT).as_posix(),stdout_sha256=hashlib.sha256(result.stdout).hexdigest(),stderr_sha256=hashlib.sha256(result.stderr).hexdigest(),report_path=report["report_path"]))
(output/"verification.json").write_text(json.dumps(dict(validation_time_utc=datetime.now(timezone.utc).isoformat(),results=results,script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print(json.dumps(dict(result="passed",input_counts=[136,225])))
