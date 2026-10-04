from datetime import date, datetime
from io import BytesIO
import math
import re
import pandas as pd

from ..akshare.session import load_client
from ..contracts import InputFetchResult


class CsindexConstituentProvider:
    """Original SDK constituent and weight workbooks; each endpoint is independent."""
    input_hosts = ("https://oss-ch.csindex.com.cn",)
    capability_version = "csindex-constituents-input-v1"

    def __init__(self, endpoint, client=None):
        self.endpoint, self.client = endpoint, client

    def fetch(self, *, index_code, source_responses):
        if not re.fullmatch(r'\d{6}', index_code):
            raise ValueError('an explicit six-digit index code is required')
        self.client = self.client or load_client()
        if self.endpoint == 'index_valuation':
            frame=self.client.stock_zh_index_value_csindex(symbol=index_code)
            responses=list(source_responses())
            if len(responses)!=2 or responses[0]['body']!=responses[1]['body'] or responses[0]['url']!=responses[1]['url']:
                raise ValueError('original Excel format-check and parse requests must return the same workbook')
            raw=pd.read_excel(BytesIO(responses[0]['body']))
            columns=('日期','指数代码','指数中文全称','指数中文简称','指数英文全称','指数英文简称','市盈率1','市盈率2','股息率1','股息率2')
            if raw.empty or len(raw)!=len(frame) or raw.shape[1]!=10 or tuple(frame.columns)!=columns:
                raise ValueError('valuation workbook fields or coverage changed')
            raw.columns=columns
            for row in raw.to_dict(orient='records'):
                datetime.strptime(str(row['日期']),'%Y%m%d')
                if str(row['指数代码']).zfill(6)!=index_code:raise ValueError('valuation returned another index')
                for field in columns[6:]:
                    if pd.isna(row[field]):continue
                    if isinstance(row[field],bool) or not math.isfinite(float(row[field])):raise ValueError('invalid valuation source number')
            if frame['日期'].isna().any() or frame.duplicated(['日期','指数代码']).any():raise ValueError('invalid valuation date or duplicate')
            rows=tuple(frame.astype(object).where(frame.notna(),None).to_dict(orient='records'))
            return InputFetchResult(rows,source_rows=tuple(raw.astype(object).where(raw.notna(),None).to_dict(orient='records')),
                source_url=responses[0]['url'],mapping_context={'index_code':index_code,'pagination_completeness_verified':False,'source_total_count':len(raw)})
        weighted = self.endpoint == 'weights'
        function = self.client.index_stock_cons_weight_csindex if weighted else self.client.index_stock_cons_csindex
        frame = function(symbol=index_code)
        responses = list(source_responses())
        if len(responses) != 1:
            raise ValueError('one exact constituent workbook is required')
        raw = pd.read_excel(BytesIO(responses[0]['body']))
        columns = ('日期','指数代码','指数名称','指数英文名称','成分券代码','成分券名称','成分券英文名称','交易所','交易所英文名称') + (('权重',) if weighted else ())
        if raw.empty or raw.shape[1] != len(columns) or tuple(frame.columns) != columns or len(raw) != len(frame):
            raise ValueError('constituent workbook fields or coverage changed')
        raw.columns = columns
        for row in raw.to_dict(orient='records'):
            datetime.strptime(str(row['日期']), '%Y%m%d')
            if str(row['指数代码']).zfill(6) != index_code or not re.fullmatch(r'\d{6}',str(row['成分券代码']).zfill(6)):
                raise ValueError('constituent workbook ignored requested index or invalid constituent')
            if weighted and (isinstance(row['权重'],bool) or not math.isfinite(float(row['权重']))):
                raise ValueError('source weight is not a finite number')
        if frame.duplicated(['日期','成分券代码']).any() or frame['日期'].isna().any():
            raise ValueError('duplicate constituent or invalid source date')
        rows = tuple(frame.astype(object).where(frame.notna(),None).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=tuple(raw.to_dict(orient='records')),source_url=responses[0]['url'],
            mapping_context={'index_code':index_code,'source_total_count':len(raw),'coverage_complete':True})
