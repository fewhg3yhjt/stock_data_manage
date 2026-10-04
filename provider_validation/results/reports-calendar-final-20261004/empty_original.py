from pathlib import Path
import hashlib,importlib.util,json,sys
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/"src"))
from stock_data_manage.providers.transport import captured_requests,RequestPacer
from stock_data_manage.storage.raw import RawObjectStore
spec=importlib.util.spec_from_file_location("empty_source",ROOT/"provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py")
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class Clock:
 def __init__(self):self.now=1000.;self.waits=[]
 def time(self):return self.now
 def sleep(self,n):self.waits.append(n);self.now+=n
result=[]
for name in ("false_empty","valid_empty"):
 directory=Path(__file__).parent/name
 candidate_path=next(directory.rglob("report.json"));report=json.loads(candidate_path.read_text(encoding="utf-8"))
 manifest=directory/"fixture/manifest.ndjson";ns=module.load_shipped_code();clock=Clock();ns["time"]=clock
 try:
  with patch("requests.adapters.HTTPAdapter.send",side_effect=AssertionError("offline only")),captured_requests(RawObjectStore(directory/"original-rule"),provider="sina",endpoint="reports",scope={"fixture":name},code_version="original-empty-page-rule",pacer=RequestPacer(),replay_manifest=manifest) as events:
   frame=ns["sina_research_reports"]()
 finally:ns["EM_SESSION"].close()
 assert len(frame)==report["row_count"] and clock.waits==[6.0]
 keys=("url","method","body_sha256","status_code","request_headers","request_options")
 assert [{k:r.get(k) for k in keys} for r in events]==[{k:r.get(k) for k in keys} for r in report["responses"]]
 result.append(dict(fixture=name,original_count=len(frame),candidate_count=report["row_count"],original_waits=clock.waits,requests_equal=True,report_path=candidate_path.relative_to(ROOT).as_posix(),report_sha256=hashlib.sha256(candidate_path.read_bytes()).hexdigest(),source_manifest=manifest.relative_to(ROOT).as_posix(),source_manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest()))
(Path(__file__).parent/"empty-original-comparison.json").write_text(json.dumps(dict(result="passed",script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),mode="synthetic HTML and clock; original source executed offline",comparisons=result),ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print(json.dumps(dict(result="passed",rows=[40,0],original_waits=[6.0])))
