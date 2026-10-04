"""Check the original successful ST source directly, without EastMoney fallback."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,importlib.util,json,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'src'))
from stock_data_manage.providers.baostock.session import _decoded_result_set
import baostock

def main():
    out=ROOT/'provider_validation/results/remaining-st-evidence-20261004';out.mkdir(exist_ok=False)
    source=ROOT/'provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py'
    spec=importlib.util.spec_from_file_location('st_original',source);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    ns=module.load_shipped_code()
    original=baostock.query_stock_basic
    result=dict(provider='baostock',endpoint='query_stock_basic',parameters={'code_name':'ST'},production_writes=0,
        eligible_for_production_routing=False,representation='SDK-decoded fields and rows; exact TCP frames not exposed',
        source_sha256=hashlib.sha256((source.parents[1]/'SKILL.md').read_bytes()).hexdigest())
    def captured(*args,**kwargs):
        rs=original(*args,**kwargs)
        rows=[]
        while rs.next():rows.append(dict(zip(rs.fields,rs.get_row_data())))
        payload=dict(fields=rs.fields,rows=rows,result=dict(error_code=rs.error_code,error_msg=rs.error_msg))
        raw=(json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
        path=out/'sdk-payload.json';path.write_bytes(raw)
        result.update(source_payload_path=str(path.relative_to(ROOT)),source_payload_sha256=hashlib.sha256(raw).hexdigest(),source_row_count=len(rows))
        return _decoded_result_set(payload)
    baostock.query_stock_basic=captured
    try:
        frame=ns['_st_list_baostock']('explicit independent probe of the original successful branch')
        rows=frame.astype(object).where(frame.notna(),None).to_dict(orient='records')
        path=out/'parsed.json';path.write_bytes((json.dumps(rows,ensure_ascii=False,indent=2)+'\n').encode())
        result.update(status='parsed',row_count=len(rows),coverage=frame.attrs['coverage'],parsed_path=str(path.relative_to(ROOT)),parsed_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    except Exception as exc:result.update(status='blocked',failure_class=type(exc).__name__,message=str(exc))
    finally:baostock.query_stock_basic=original;ns['EM_SESSION'].close()
    result['validation_time_utc']=datetime.now(timezone.utc).isoformat()
    (out/'result.json').write_bytes((json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode())
    print(result['status'],result.get('row_count'),result.get('failure_class',''),flush=True)
if __name__=='__main__':main()
