"""Move the observed source parsers into their existing architecture boundaries."""
import ast, hashlib, json, re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'provider_validation/tests/source_snapshots/a-stock-data/SKILL.md'

def main():
    text=SOURCE.read_text(encoding='utf-8')
    markers=('v39-helpers','v39-tdx-package','v39-etf-shares','v39-chinabond','v39-futures','v310-tencent-ticks')
    nodes={}; imports=[]
    for marker in markers:
        body=text.split(f'<!-- {marker}:start -->',1)[1].split(f'<!-- {marker}:end -->',1)[0]
        code=re.search(r'```python\n(.*?)\n```',body,re.S).group(1)
        for n in ast.parse(code).body:
            if isinstance(n,ast.FunctionDef):nodes[n.name]=n
            elif isinstance(n,(ast.Assign,ast.AnnAssign)):
                names=[v.id for t in (n.targets if isinstance(n,ast.Assign) else [n.target]) for v in ast.walk(t) if isinstance(v,ast.Name)]
                for name in names:nodes[name]=n
            elif isinstance(n,(ast.Import,ast.ImportFrom)):imports.append(ast.unparse(n))
    for name in ('get_prefix','norm_ticker'):
        block=next(b for b in re.findall(r'```python\n(.*?)```',text,re.S) if re.search(rf'^def {name}\(',b,re.M))
        for n in ast.parse(block).body:
            if isinstance(n,ast.FunctionDef):nodes[n.name]=n
            elif isinstance(n,ast.Assign) and not any(isinstance(c,ast.Call) and isinstance(c.func,ast.Name) and c.func.id=='norm_ticker' for c in ast.walk(n)):
                for t in n.targets:
                    if isinstance(t,ast.Name):nodes[t.id]=n
            elif isinstance(n,(ast.Import,ast.ImportFrom)):imports.append(ast.unparse(n))
    common={'_v39_http','_v39_json','_v39_date','_v39_src_date','_v39_num','_v39_req_num','_v39_contract','_v39_count','_v39_rows','_v39_frame'}
    def closure(names,skip=()):
        found=set(names); todo=list(names)
        while todo:
            for n in ast.walk(nodes[todo.pop()]):
                if isinstance(n,ast.Name) and isinstance(n.ctx,ast.Load) and n.id in nodes and n.id not in found and n.id not in skip:
                    found.add(n.id);todo.append(n.id)
        unique=[]
        for name,node in nodes.items():
            if name in found and node not in unique:unique.append(node)
        return unique
    provenance=[]
    def render(names,skip=()):
        selected=closure(names,skip)
        for node in selected:
            if isinstance(node,ast.FunctionDef):provenance.append(dict(function=node.name,ast_sha256=hashlib.sha256(ast.dump(node,include_attributes=False).encode()).hexdigest()))
        return '\n\n'.join(ast.unparse(n) for n in selected)+'\n'
    def append(path,body):
        p=ROOT/path;s=p.read_text(encoding='utf-8')
        p.write_bytes((s.rstrip()+'\n\n'+body).encode('utf-8'))
    http=ast.unparse(nodes['_v39_http']).replace('    merged =','    import requests\n    merged =',1)
    append('src/stock_data_manage/providers/transport.py',ast.unparse(nodes['V39_UA'])+'\n\n'+http+'\n')
    append('src/stock_data_manage/providers/contracts.py','import math, functools, re\nimport pandas as pd\nfrom datetime import date as _date_cls, datetime, timezone\n\n'+render(common-{'_v39_http'}))
    header='import math, re, io, csv, time, struct, zipfile, zlib, json\nfrom xml.etree import ElementTree\nfrom datetime import date, datetime, timedelta\nimport pandas as pd\nfrom ..contracts import InputFetchResult, '+', '.join(sorted(common-{'_v39_http'}))+'\nfrom ..transport import _v39_http\n\n'
    groups=[('chinabond/yield_curve.py',['chinabond_yield_curve'],'''class ChinaBondYieldProvider:
    capability_version='chinabond-yield-input-v1'
    input_hosts=('https://yield.chinabond.com.cn',)
    def fetch(self,*,start,end,curve):
        frame=chinabond_yield_curve(start,end,curve)
        rows=tuple(frame.drop(columns=['source','source_url','fetched_at']).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=rows,source_url=frame['source_url'].iloc[0])
'''),('sge/spot.py',['sge_spot'],'''class SgeSpotProvider:
    capability_version='sge-spot-input-v1'
    input_hosts=('https://www.sge.com.cn',)
    def fetch(self,*,instrument):
        if instrument!='Au99.99':raise ValueError('only the archived Au99.99 input is enabled')
        frame=sge_spot(instrument)
        rows=tuple(frame.drop(columns=['source','source_url','fetched_at']).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=rows,source_url=frame['source_url'].iloc[0],mapping_context={'ohlc_anomaly_dates':frame.attrs['ohlc_anomaly_dates']})
'''),('exchanges/daily.py',['futures_daily','options_daily','futures_position_rank'],'''class ExchangeDailyProvider:
    capability_version='official-derivatives-input-v1'
    input_hosts=('https://www.shfe.com.cn','https://www.ine.cn','https://www.czce.com.cn','http://www.cffex.com.cn','http://www.gfex.com.cn')
    @staticmethod
    def result(frame):
        rows=tuple(frame.drop(columns=['source','source_url','fetched_at']).astype(object).where(frame.drop(columns=['source','source_url','fetched_at']).notna(),None).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=rows,source_url=frame['source_url'].iloc[0])
    def fetch_futures(self,*,date,exchange):
        if exchange not in ('SHFE','INE','CZCE','CFFEX','GFEX'):raise ValueError('exchange lacks matching source evidence')
        return self.result(futures_daily(date,exchange))
    def fetch_options(self,*,date,exchange):
        if exchange not in ('SHFE','CFFEX'):raise ValueError('only observed SHFE/CFFEX option inputs are enabled')
        return self.result(options_daily(date,exchange))
    def fetch_rank(self,*,date,exchange,symbol=None):
        if exchange!='CFFEX':raise ValueError('only observed CFFEX ranking input is enabled')
        frame=futures_position_rank(date,exchange,symbol)
        if frame.empty:raise ValueError('requested contract is absent; never substitutes another contract')
        return self.result(frame)
''')]
    for name,functions,wrapper in groups:
        p=ROOT/'src/stock_data_manage/providers'/name
        p.parent.mkdir(exist_ok=True)
        assert not p.exists()
        p.write_bytes((header+render(functions,common)+ '\n'+wrapper).encode('utf-8'))
        (p.parent/'__init__.py').write_bytes(b'"""Explicit original-source candidate inputs."""\n')
    tdx=render(['tdx_daily_package'],common)
    append('src/stock_data_manage/providers/tdx/minute.py',header.replace('from ..contracts','from ..contracts').replace('from ..transport','from ..transport')+tdx)
    p=ROOT/'src/stock_data_manage/providers/tdx/minute.py';s=p.read_text(encoding='utf-8')
    s=s.replace('    def fetch_realtime_minute(','''    input_hosts = ('https://www.tdx.com.cn',)

    def fetch_daily_package(self, *, date):
        frame=tdx_daily_package(date)
        rows=tuple(frame.drop(columns=['source','source_url','fetched_at']).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=rows,source_url=frame['source_url'].iloc[0])

    def fetch_realtime_minute(''',1);p.write_bytes(s.encode())
    # Extend the existing Tencent snapshot adapter; the original paginated source uses two snapshots as a witness.
    append('src/stock_data_manage/providers/tencent/snapshot.py',header+render(['tencent_ticks'],common))
    p=ROOT/'src/stock_data_manage/providers/tencent/snapshot.py';s=p.read_text(encoding='utf-8')
    s=s.replace('    def fetch_snapshot(','''    def fetch_ticks(self, *, code):
        frame=tencent_ticks(code)
        rows=tuple(frame.drop(columns=['source','source_url','fetched_at']).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=rows,source_url=frame['source_url'].iloc[0],mapping_context={'missing_seq':frame.attrs['missing_seq']})

    def fetch_snapshot(''',1);p.write_bytes(s.encode())
    # SSE supports only the archived SH ETF input; SZ remains explicitly unavailable here.
    append('src/stock_data_manage/providers/sse/market.py',header+render(['_etf_shares_sse'],common))
    p=ROOT/'src/stock_data_manage/providers/sse/market.py';s=p.read_text(encoding='utf-8')
    s=s.replace('    def fetch_risk(','''    def fetch_etf(self, *, date, exchange):
        if exchange!='SH':raise ValueError('only the archived SH ETF share input is enabled')
        rows,url=_etf_shares_sse(date.isoformat())
        if len({r['code'] for r in rows})!=len(rows):raise RuntimeError('duplicate ETF code')
        return InputFetchResult(tuple(rows),source_rows=tuple(rows),source_url=url)

    def fetch_risk(''',1);p.write_bytes(s.encode())
    (Path(__file__).parent/'migration-provenance.json').write_bytes((json.dumps(dict(source=str(SOURCE),source_sha256=hashlib.sha256(SOURCE.read_bytes()).hexdigest(),original_functions=provenance),ensure_ascii=False,indent=2)+'\n').encode())
    print('migrated original parser closures',len(provenance))

if __name__=='__main__':main()
