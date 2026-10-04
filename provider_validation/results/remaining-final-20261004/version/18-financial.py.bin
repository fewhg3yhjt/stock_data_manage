from datetime import datetime
import math

from ..akshare.session import load_client
from ..contracts import InputFetchResult
from ..eastmoney.realtime import history_stock_identity


class SinaFinancialReportProvider:
    """Three original wide reports, preserving report and disclosure dates separately."""
    input_hosts=('https://quotes.sina.cn',)
    capability_version='sina-statements-input-v1'

    def __init__(self,client=None):
        self.client=client

    def fetch(self,*,code,source_responses):
        import json
        bare,market,_=history_stock_identity(code)
        symbol=market+bare
        self.client=self.client or load_client()
        tables=('资产负债表','利润表','现金流量表')
        frames=[self.client.stock_financial_report_sina(stock=symbol,symbol=name) for name in tables]
        responses=list(source_responses())
        if len(responses)!=3:
            raise ValueError('three explicit financial statement responses are required')
        rows,source_rows,counts=[],[],{}
        for name,frame,response in zip(tables,frames,responses):
            payload=json.loads(response['body'])
            result=payload.get('result',{})
            if result.get('status',{}).get('code')!=0:
                raise ValueError('financial statement business status changed')
            data=result.get('data',{})
            dates=data.get('report_date')
            reports=data.get('report_list')
            if not isinstance(dates,list) or not dates or not isinstance(reports,dict) or len(frame)!=len(dates):
                raise ValueError('financial statement coverage changed')
            if [str(row['报告日']) for row in frame.to_dict(orient='records')]!=[str(d['date_value']) for d in dates]:
                raise ValueError('financial statement report dates changed')
            for day in dates:
                period=str(day['date_value'])
                datetime.strptime(period,'%Y%m%d')
                report=reports[period]
                values=report.get('data')
                if not isinstance(values,list) or not values:
                    raise ValueError('financial statement items missing')
                for value in values:
                    if not {'item_title','item_value'}<=set(value) or not value['item_title']:
                        raise ValueError('financial statement source item changed')
                    number=value['item_value']
                    if number not in (None,'','--','-') and (isinstance(number,bool) or not math.isfinite(float(number))):
                        raise ValueError('financial statement source number changed')
                source_rows.append({'report_type':name,'source_security_code':bare,'report_date':period,**report})
            parsed=frame.astype(object).where(frame.notna(),None).to_dict(orient='records')
            rows.extend({'report_type':name,'source_security_code':bare,**r} for r in parsed)
            counts[name]=len(parsed)
        return InputFetchResult(tuple(rows),source_rows=tuple(source_rows),source_url=responses[0]['url'],mapping_context={'table_counts':counts})
