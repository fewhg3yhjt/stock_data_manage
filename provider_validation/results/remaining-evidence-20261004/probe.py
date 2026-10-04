"""Bounded original SDK probes for gaps identified by the saved archive inventory."""
from pathlib import Path
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
import argparse, hashlib, inspect, io, json, sys
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'src'))
from stock_data_manage.storage.raw import RawObjectStore, sanitized_headers, sanitized_url, sanitized_metadata
from stock_data_manage.providers.transport import captured_requests, RequestPacer
from stock_data_manage.pipeline.inputs import _json_value
import akshare

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output-root',type=Path,default=Path(__file__).parent)
    parser.add_argument('--inputs',nargs='+')
    args=parser.parse_args()
    out=args.output_root.resolve()
    assert out.is_relative_to(ROOT/'provider_validation/results')
    out.mkdir(parents=True,exist_ok=True)
    source=ROOT/'provider_validation/results/remaining-original-20261004/inventory.json'
    summary=[]
    for id,function,parameters in [('ASTOCK-011','stock_profit_forecast_ths',{'symbol':'600519'}),
                                   ('ASTOCK-031','stock_news_em',{'symbol':'600519'}),
                                   ('ASTOCK-069','stock_zh_index_value_csindex',{'symbol':'000300'})]:
        if args.inputs and id not in args.inputs:continue
        directory=out/id;directory.mkdir(exist_ok=False)
        store=RawObjectStore(directory/'_raw')
        result=dict(input_id=id,function=function,parameters=parameters,
            archive_search_inventory=str(source.relative_to(ROOT)),archive_search_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
            scope='one original symbol; manual evidence only',production_writes=0,eligible_for_production_routing=False)
        # Keep pandas' own urllib transport, request headers and proxy behavior. Capture the
        # exact application payload before handing it back to the original Excel parser.
        import pandas.io.common as common
        original_urlopen=common.urlopen
        def recorded_urlopen(request):
            try: response=original_urlopen(request)
            except Exception as exc:
                store.append_event(dict(event='transport_failure',url=sanitized_url(request.full_url),method=request.get_method(),
                    request_headers=sanitized_headers(dict(request.header_items())),scope=parameters,
                    failure_class=type(exc).__name__,message=str(exc),fetched_at_utc=datetime.now(timezone.utc).isoformat()))
                raise
            with response:
                body=response.read()
                url=request.full_url if hasattr(request,'full_url') else str(request)
                headers=dict(request.header_items()) if hasattr(request,'header_items') else {}
                record=SimpleNamespace(content=body,status_code=response.status,headers=dict(response.headers),encoding=None)
                store.record_response(response=record,url=url,method=request.get_method() if hasattr(request,'get_method') else 'GET',
                    request_headers=headers,scope=parameters,provider='csindex',endpoint=function,
                    code_version=hashlib.sha256(inspect.getsource(getattr(akshare,function)).encode()).hexdigest())
                buffered=io.BytesIO(body);buffered.headers=response.headers
                return buffered
        import curl_cffi.requests as curl_requests
        original_curl_request=curl_requests.Session.request
        def recorded_curl_request(session,method,url,*args,**kwargs):
            try: response=original_curl_request(session,method,url,*args,**kwargs)
            except Exception as exc:
                store.append_event(dict(event='transport_failure',method=method,url=sanitized_url(url),
                    request_headers=sanitized_headers(kwargs.get('headers') or {}),request_options=sanitized_metadata(kwargs),
                    scope=parameters,failure_class=type(exc).__name__,message=str(exc),fetched_at_utc=datetime.now(timezone.utc).isoformat()))
                raise
            store.record_response(response=response,url=response.url,method=method,
                request_headers=dict(response.request.headers),scope=parameters,provider='eastmoney',endpoint=function,
                code_version=hashlib.sha256(inspect.getsource(getattr(akshare,function)).encode()).hexdigest(),
                request_options=dict(transport='original curl_cffi SDK request boundary',**sanitized_metadata(kwargs)))
            return response
        try:
            with captured_requests(store,provider='original-sdk',endpoint=function,scope=parameters,
                    code_version=hashlib.sha256(inspect.getsource(getattr(akshare,function)).encode()).hexdigest(),
                    pacer=RequestPacer(),sdk_retry_policy=True,probe_host_pause=True), patch.object(common,'urlopen',recorded_urlopen), patch.object(curl_requests.Session,'request',recorded_curl_request):
                frame=getattr(akshare,function)(**parameters)
                rows=_json_value(frame.astype(object).where(frame.notna(),None).to_dict(orient='records'))
                target=directory/'parsed.json';target.write_bytes((json.dumps(rows,ensure_ascii=False,indent=2)+'\n').encode())
                result.update(status='parsed',row_count=len(rows),fields=list(frame.columns),parsed_path=str(target.relative_to(ROOT)),parsed_sha256=hashlib.sha256(target.read_bytes()).hexdigest())
        except Exception as exc:result.update(status='blocked',failure_class=type(exc).__name__,message=str(exc))
        manifest=store.root/'manifest.ndjson'
        result.update(manifest=str(manifest.relative_to(ROOT)),manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest() if manifest.exists() else None)
        (directory/'result.json').write_bytes((json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode())
        summary.append(result)
        print(id,result['status'],result.get('row_count'),result.get('failure_class',''),flush=True)
    (out/'summary.json').write_bytes((json.dumps(summary,ensure_ascii=False,indent=2)+'\n').encode())

if __name__=='__main__':main()
