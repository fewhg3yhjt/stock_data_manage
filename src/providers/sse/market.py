from datetime import date
import math

from ..akshare.session import load_client
from ..contracts import InputFetchResult


class SseMarketProvider:
    input_hosts = ('http://query.sse.com.cn','https://query.sse.com.cn')
    capability_version = 'sse-risk-input-v1'

    def __init__(self, client=None):
        self.client = client

    def fetch_etf(self, *, date, exchange):
        if exchange!='SH':raise ValueError('only the archived SH ETF share input is enabled')
        rows,url=_etf_shares_sse(date.isoformat())
        if len({r['code'] for r in rows})!=len(rows):raise RuntimeError('duplicate ETF code')
        return InputFetchResult(tuple(rows),source_rows=tuple(rows),source_url=url)

    def fetch_risk(self, *, date, source_responses):
        import json
        self.client = self.client or load_client()
        frame = self.client.option_risk_indicator_sse(date=date.strftime('%Y%m%d'))
        responses = list(source_responses())
        if len(responses) != 1:
            raise ValueError('one explicit-date SSE risk response is required')
        payload=json.loads(responses[0]['body'])
        items=payload.get('result')
        columns=('TRADE_DATE','SECURITY_ID','CONTRACT_ID','CONTRACT_SYMBOL','DELTA_VALUE','THETA_VALUE','GAMMA_VALUE','VEGA_VALUE','RHO_VALUE','IMPLC_VOLATLTY')
        if not isinstance(items,list) or not items or len(items)!=len(frame) or tuple(frame.columns)!=columns:
            raise ValueError('SSE risk fields or count changed')
        for row in items:
            if not set(columns)<=set(row) or str(row['TRADE_DATE']).replace('-','') != date.strftime('%Y%m%d'):
                raise ValueError('SSE risk response ignored explicit day or fields changed')
            for key in columns[4:]:
                value=row[key]
                if value not in (None,'','-','--') and (isinstance(value,bool) or not math.isfinite(float(value))):
                    raise ValueError('SSE risk source number changed')
        if frame.duplicated(['SECURITY_ID']).any():
            raise ValueError('duplicate SSE risk contract')
        rows=tuple(frame.astype(object).where(frame.notna(),None).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=tuple(items),source_url=responses[0]['url'],mapping_context={'source_total_count':len(items)})

import math, re, io, csv, time, struct, zipfile, zlib
from xml.etree import ElementTree
from datetime import date, datetime, timedelta
import pandas as pd
from ..contracts import InputFetchResult, _v39_contract, _v39_count, _v39_date, _v39_frame, _v39_json, _v39_num, _v39_req_num, _v39_rows, _v39_src_date
from ..transport import _v39_http

SSE_ETF_SHARES_URL = 'https://query.sse.com.cn/commonQuery.do'

def _etf_shares_sse(day):
    params = {'sqlId': 'COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L', 'STAT_DATE': day, 'isPagination': 'true', 'pageHelp.pageSize': 10000, 'pageHelp.pageNo': 1, 'pageHelp.beginPage': 1, 'pageHelp.cacheSize': 1, 'pageHelp.endPage': 1}
    response = _v39_http(SSE_ETF_SHARES_URL, params=params, headers={'Referer': 'https://www.sse.com.cn/'})
    payload = _v39_json(response)
    result = payload.get('result') if isinstance(payload, dict) else None
    page_help = payload.get('pageHelp') if isinstance(payload, dict) else None
    if not isinstance(result, list) or not all((isinstance(r, dict) for r in result)) or (not isinstance(page_help, dict)):
        raise RuntimeError('上交所 ETF 规模接口返回结构改变')
    total = _v39_count(page_help.get('total'), '上交所 ETF 规模 pageHelp.total')
    if len(result) != total:
        raise RuntimeError(f'上交所 ETF 规模返回 {len(result)} 条，与自报总数 {total} 不符，结果不完整')
    if not result:
        raise ValueError(f'上交所 {day} 没有 ETF 份额数据：非交易日或尚未发布')
    rows = []
    for rec in result:
        if rec.get('STAT_DATE') != day:
            raise RuntimeError('上交所返回了其他日期的数据')
        try:
            rows.append({'date': day, 'exchange': 'SH', 'code': rec['SEC_CODE'], 'name': rec['SEC_NAME'], 'etf_type': rec.get('ETF_TYPE'), 'shares_10k': _v39_req_num(rec['TOT_VOL'], '上交所 ETF 份额')})
        except KeyError as exc:
            raise RuntimeError(f'上交所 ETF 规模字段缺失: {exc!r}') from exc
    return (rows, response.url)
