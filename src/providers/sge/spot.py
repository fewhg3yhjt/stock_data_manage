import math, re, io, csv, time, struct, zipfile, zlib
from xml.etree import ElementTree
from datetime import date, datetime, timedelta
import pandas as pd
from ..contracts import InputFetchResult, _v39_contract, _v39_count, _v39_date, _v39_frame, _v39_json, _v39_num, _v39_req_num, _v39_rows, _v39_src_date
from ..transport import _v39_http

SGE_DAILY_URL = 'https://www.sge.com.cn/graph/Dailyhq'

@_v39_contract
def sge_spot(instrument='Au99.99'):
    """上海黄金交易所现货日线（官方）— 2016-12 至今。

    instrument 例: 'Au99.99' / 'Au(T+D)' / 'mAu(T+D)' / 'Ag(T+D)' / 'Ag99.99' / 'Pt99.95'。
    黄金单位 元/克，白银 元/千克。无成交日交易所填 0，这里剔除；代码不存在时上金所返回全 0，抛 ValueError。
    少数日子收盘价略超出高低区间是原始数据如此，日期列在 attrs['ohlc_anomaly_dates']。
    """
    response = _v39_http(SGE_DAILY_URL, method='POST', data={'instid': instrument}, headers={'Referer': 'https://www.sge.com.cn/'})
    payload = _v39_json(response)
    series = payload.get('time') if isinstance(payload, dict) else None
    if not isinstance(series, list):
        raise RuntimeError('上金所日线返回结构改变')
    rows = []
    for rec in series:
        if not isinstance(rec, list) or len(rec) != 5:
            raise RuntimeError(f'上金所日线字段数不对: {rec}')
        prices = [_v39_num(v) for v in rec[1:]]
        if None in prices:
            raise RuntimeError(f'上金所日线出现空值或非有限数值: {rec}')
        if not all(prices):
            continue
        open_, close, low, high = prices
        rows.append({'date': _v39_src_date(rec[0]), 'instrument': instrument, 'open': open_, 'high': high, 'low': low, 'close': close})
    if not rows:
        raise ValueError(f'上金所没有 {instrument} 的有效行情（代码不存在时返回全 0）')
    bad = [r['date'] for r in rows if not r['low'] <= min(r['open'], r['close']) <= max(r['open'], r['close']) <= r['high']]
    if len(bad) > 0.05 * len(rows):
        raise RuntimeError(f'上金所日线 {len(bad)}/{len(rows)} 天高低开收关系不成立，字段顺序可能已变')
    frame = _v39_frame(rows, 'sge', response.url)
    frame.attrs['ohlc_anomaly_dates'] = bad
    return frame

class SgeSpotProvider:
    capability_version='sge-spot-input-v1'
    input_hosts=('https://www.sge.com.cn',)
    def fetch(self,*,instrument):
        if instrument!='Au99.99':raise ValueError('only the archived Au99.99 input is enabled')
        frame=sge_spot(instrument)
        rows=tuple(frame.drop(columns=['source','source_url','fetched_at']).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=rows,source_url=frame['source_url'].iloc[0],mapping_context={'ohlc_anomaly_dates':frame.attrs['ohlc_anomaly_dates']})
