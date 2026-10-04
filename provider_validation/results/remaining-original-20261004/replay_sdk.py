"""Check installed SDK behavior against retained exact requests, with network forbidden."""
from pathlib import Path
import hashlib, json, sys
from datetime import datetime, timezone
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'src'))
from stock_data_manage.storage.raw import RawObjectStore
from stock_data_manage.providers.transport import captured_requests, RequestPacer
from stock_data_manage.pipeline.inputs import _json_value
import akshare

def main():
    archive=ROOT/'provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson'
    out=Path(__file__).parent/'sdk-replay'
    out.mkdir(exist_ok=False)
    cases=[('risk','option_risk_indicator_sse',{'date':'20260930'}),
        ('constituents','index_stock_cons_csindex',{'symbol':'000300'}),
        ('weights','index_stock_cons_weight_csindex',{'symbol':'000300'}),
        ('billboard','stock_lhb_detail_daily_sina',{'date':'20260930'}),
        ('balance','stock_financial_report_sina',{'stock':'sh600519','symbol':'资产负债表'}),
        ('income','stock_financial_report_sina',{'stock':'sh600519','symbol':'利润表'}),
        ('cashflow','stock_financial_report_sina',{'stock':'sh600519','symbol':'现金流量表'}),
        ('changes','stock_changes_em',{'symbol':'火箭发射'})]
    summary=[]
    def forbidden(*a,**k):raise AssertionError('investigation cannot issue live requests')
    with patch('requests.adapters.HTTPAdapter.send',forbidden),patch('socket.socket.connect',forbidden):
        for name,function,params in cases:
            store=RawObjectStore(out/name/'_raw')
            result=dict(function=function,parameters=params,sdk_version=akshare.__version__,network_requests=0)
            with captured_requests(store,provider='original-sdk',endpoint=function,scope=params,
                code_version=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),pacer=RequestPacer(wait=lambda _:None),
                replay_manifest=archive,sdk_retry_policy=True,probe_host_pause=True) as events:
                try:
                    frame=getattr(akshare,function)(**params)
                    rows=frame.astype(object).where(frame.notna(),None).to_dict(orient='records')
                    target=out/name/'parsed.json'
                    target.write_bytes((json.dumps(_json_value(rows),ensure_ascii=False,indent=2)+'\n').encode())
                    result.update(status='passed',row_count=len(frame),fields=list(frame.columns),
                        parsed_path=target.relative_to(ROOT).as_posix(),parsed_sha256=hashlib.sha256(target.read_bytes()).hexdigest())
                except Exception as exc:
                    result.update(status='failed',failure_class=type(exc).__name__,message=str(exc))
            manifest=store.root/'manifest.ndjson'
            result.update(manifest=manifest.relative_to(ROOT).as_posix(),manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),request_count=len(events))
            summary.append(result)
            print(name,result['status'],result.get('row_count'),result.get('message',''))
    (out/'summary.json').write_bytes((json.dumps(summary,ensure_ascii=False,indent=2)+'\n').encode())

if __name__=='__main__':main()
