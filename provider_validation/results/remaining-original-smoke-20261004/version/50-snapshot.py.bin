from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence
import re

from ...domain import QualityStatus
from ...routing.capabilities import ProviderCapability
from ..base import FetchResult
from ..contracts import EndpointContract, FailureClass, ProviderContractError
from ..transport import HttpTransport, SnapshotFetchResponse, parse_tencent_snapshot_line


@dataclass(slots=True)
class TencentSnapshotProvider:
    transport: HttpTransport
    capability: ProviderCapability
    name: str = "tencent"
    endpoint: str = "bulk_snapshot"
    timeout_seconds: float = 10.0
    max_symbols_per_request: int = 100
    url: str = "https://qt.gtimg.cn/q="

    @property
    def capability_version(self) -> str:
        return self.capability.version

    @property
    def capability_priority(self) -> int:
        return self.capability.priority

    @property
    def quality_status(self) -> QualityStatus:
        return QualityStatus.PROVISIONAL

    source_method: str = "snapshot"

    @property
    def input_hosts(self) -> tuple[str, ...]:
        return (self.url,)

    def fetch_ticks(self, *, code):
        frame=tencent_ticks(code)
        rows=tuple(frame.drop(columns=['source','source_url','fetched_at']).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=rows,source_url=frame['source_url'].iloc[0],mapping_context={'missing_seq':frame.attrs['missing_seq']})

    def fetch_snapshot(self, symbols: Sequence[str], as_of: datetime) -> SnapshotFetchResponse:
        requested = tuple(symbols)
        if self.max_symbols_per_request < 1:
            raise ValueError("snapshot batch size must be positive")
        if not requested or len(set(requested)) != len(requested):
            raise ValueError("snapshot requires a non-empty, unique security scope")
        if any(not isinstance(symbol, str) or not re.fullmatch(r"(?:sh|sz|bj)\d{6}", symbol) for symbol in requested):
            raise ValueError("snapshot requires explicit canonical sh/sz/bj security codes")
        rows: list[dict[str, Any]] = []
        source_rows: list[dict[str, Any]] = []
        seen = set()
        for offset in range(0, len(requested), max(1, self.max_symbols_per_request)):
            batch = requested[offset : offset + self.max_symbols_per_request]
            response = self.transport.get(self.url + ",".join(batch), params={}, timeout_seconds=self.timeout_seconds)
            if response.status_code >= 400:
                EndpointContract(frozenset()).parse_json(response)
            # The archived successful quote probe explicitly decodes Tencent bytes as GBK.
            try:
                lines = response.body.decode("gbk").splitlines()
                for line in lines:
                    if not line.strip():
                        continue
                    match = re.fullmatch(r'\s*v_([^=]+)="(.*?)";?\s*', line)
                    if match is None:
                        raise ValueError("quote response envelope changed")
                    symbol, payload = match.groups()
                    if symbol not in batch:
                        raise ValueError("quote returned an unrequested security")
                    if symbol in seen:
                        raise ValueError("duplicate quote security")
                    seen.add(symbol)
                    if not payload or payload == "1":
                        continue  # Explicit no-quote marker, retained in response bytes.
                    parts = payload.split("~")
                    if len(parts) < 35 or parts[2] != symbol[2:]:
                        raise ValueError("quote fields or security identity changed")
                    if not re.fullmatch(r"\d{14}", parts[30]):
                        raise ValueError("quote timestamp format changed")
                    parsed = parse_tencent_snapshot_line(line)
                    if parsed is None:
                        raise ValueError("quote timestamp is missing")
                    raw = {name: parts[index] for name, index in (
                        ("name", 1), ("code", 2), ("price", 3), ("prev_close", 4), ("open", 5),
                        ("volume_lot", 6), ("datetime", 30), ("change", 31), ("change_pct", 32), ("high", 33), ("low", 34))}
                    exchange = {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[symbol[:2]]
                    raw.update(raw=payload, symbol=symbol,
                               instrument_id=f"{exchange}:{symbol[2:]}",
                               source_amount_37=parts[37] if len(parts) > 37 else None)
                    rows.append(parsed)
                    source_rows.append(raw)
            except (UnicodeDecodeError, ValueError) as exc:
                raise ProviderContractError(str(exc), FailureClass.SCHEMA_CHANGED, retryable=False) from exc
        if not rows:
            raise ProviderContractError("snapshot has no valid quotes", FailureClass.TEMPORARY_EMPTY, retryable=True)
        return SnapshotFetchResponse(tuple(rows), requested, tuple(source_rows), self.url)

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        response = self.fetch_snapshot(symbols, datetime(trade_date.year, trade_date.month, trade_date.day, 15, 16))
        rows = tuple(row for row in response.rows if row.get("trade_date") == trade_date.isoformat())
        return FetchResult(rows, response.requested_symbols)

import math, re, io, csv, time, struct, zipfile, zlib
from xml.etree import ElementTree
from datetime import date, datetime, timedelta
import pandas as pd
from ..contracts import InputFetchResult, _v39_contract, _v39_count, _v39_date, _v39_frame, _v39_json, _v39_num, _v39_req_num, _v39_rows, _v39_src_date
from ..transport import _v39_http

TENCENT_TICK_URL = 'https://stock.gtimg.cn/data/index.php'

TENCENT_QT_URL = 'https://qt.gtimg.cn/q='

_TICK_MAX_PAGES = 300

_TICK_SESSION_END = '15:00:59'

def _tencent_qt_snapshot(symbol):
    """腾讯行情快照 → (交易日 'YYYY-MM-DD', 时刻 'HHMMSS', 当日成交额 元)。代码不存在抛 ValueError。"""
    response = _v39_http(TENCENT_QT_URL + symbol)
    text = response.content.decode('gbk', 'replace')
    if 'v_pv_none_match' in text:
        raise ValueError(f'腾讯没有 {symbol} 这个代码')
    match = re.search(f'v_{symbol}="([^"]*)"', text)
    if not match:
        raise RuntimeError(f'腾讯行情快照 {symbol} 的返回里没有 v_{symbol} 变量，格式可能已变')
    fields = match.group(1).split('~')
    if len(fields) < 36 or not re.fullmatch('[0-9]{14}', fields[30]):
        raise RuntimeError(f'腾讯行情快照 {symbol} 字段数 {len(fields)} 或时间字段不对，格式可能已变')
    parts = fields[35].split('/')
    if len(parts) != 3:
        raise RuntimeError(f'腾讯行情快照 {symbol} 的价/量/额字段是 {fields[35]!r}，格式可能已变')
    return (_v39_src_date(fields[30][:8]), fields[30][8:], _v39_req_num(parts[2], '成交额'))

def _tencent_tick_page(symbol, page):
    """第 page 页逐笔（0 起）→ 记录列表；翻过最后一页时腾讯返回空内容，返回 None。"""
    response = _v39_http(TENCENT_TICK_URL, params={'appn': 'detail', 'action': 'data', 'c': symbol, 'p': page})
    text = response.content.decode('gbk', 'replace').strip()
    if not text:
        return None
    match = re.fullmatch(f'v_detail_data_{symbol}=\\[(\\d+),"([^"]*)"\\];?', text)
    if not match or int(match.group(1)) != page:
        raise RuntimeError(f'腾讯逐笔 {symbol} 第 {page} 页不是预期格式: {text[:80]!r}')
    if not match.group(2):
        return None
    records = []
    try:
        for item in match.group(2).split('|'):
            seq, clock, price, change, volume, amount, side = item.split('/')
            if not re.fullmatch('\\d\\d:\\d\\d:\\d\\d', clock) or side not in ('B', 'S', 'M'):
                raise ValueError(item)
            records.append({'seq': int(seq), 'time': clock, 'price': _v39_req_num(price, 'price'), 'change': _v39_req_num(change, 'change'), 'volume': _v39_req_num(volume, 'volume'), 'amount': _v39_req_num(amount, 'amount'), 'side': side})
    except ValueError as exc:
        raise RuntimeError(f'腾讯逐笔 {symbol} 第 {page} 页记录格式改变: {exc}') from exc
    return records

@_v39_contract
def tencent_ticks(code):
    """腾讯逐笔成交（分笔）— 最近一个交易日的全部成交明细，沪深个股与 ETF。

    一行一笔：date / code / time / seq（腾讯序号）/ price / change（较上一笔）/ volume（手）/ amount（元）/
    side（B 主动买 · S 主动卖 · M 中性）。约 3 秒一笔的分笔，不是 Level-2 逐笔。
    北交所、指数、代码不存在、当日没有成交抛 ValueError。收盘后调用会用行情快照的当日成交额核对连续竞价段，
    对不上抛 RuntimeError；盘后定价段腾讯偶尔缺几笔，缺的序号在 frame.attrs["missing_seq"]。
    """
    prefix, ticker = (get_prefix(code), norm_ticker(code))
    if prefix == 'bj':
        raise ValueError('腾讯逐笔不支持北交所（返回空）；北交所日线见 §1.3 tdx_daily_package')
    if (prefix, ticker[:3]) in (('sh', '000'), ('sz', '399')):
        raise ValueError(f'{prefix}{ticker} 是指数，没有逐笔成交')
    symbol = prefix + ticker
    day, clock, amount_before = _tencent_qt_snapshot(symbol)
    if amount_before == 0:
        raise ValueError(f'{symbol} 在 {day} 没有成交（停牌、尚未开盘或集合竞价未撮合）')
    rows, missing = ([], [])
    for page in range(_TICK_MAX_PAGES):
        records = _tencent_tick_page(symbol, page)
        if records is None:
            break
        for r in records:
            expected = rows[-1]['seq'] + 1 if rows else 0
            if r['seq'] < expected or (rows and r['time'] < rows[-1]['time']):
                raise RuntimeError(f'腾讯逐笔 {symbol} 序号或时间倒退（第 {page} 页 {r['seq']} {r['time']}），结果不可信')
            if r['seq'] > expected:
                if r['time'] <= _TICK_SESSION_END:
                    raise RuntimeError(f'腾讯逐笔 {symbol} 缺序号 {expected}–{r['seq'] - 1}（{r['time']} 之前，第 {page} 页），腾讯该页缓存不完整，稍后重试')
                missing.extend(range(expected, r['seq']))
            rows.append(r)
        time.sleep(0.1)
    else:
        raise RuntimeError(f'腾讯逐笔 {symbol} 翻到第 {_TICK_MAX_PAGES} 页仍未结束，格式可能已变')
    if not rows:
        if clock < '092500':
            raise ValueError(f'{symbol} 集合竞价尚未撮合（{clock}），还没有逐笔')
        raise RuntimeError(f'{symbol} 在 {day} 成交 {amount_before:.0f} 元，腾讯逐笔却为空：开盘前腾讯可能已清空上一交易日的明细，否则是接口变了')
    day_after, _, amount_after = _tencent_qt_snapshot(symbol)
    if day_after != day:
        raise RuntimeError(f'取数期间交易日从 {day} 变成 {day_after}，请重试')
    session = sum((r['amount'] for r in rows if r['time'] <= _TICK_SESSION_END))
    if amount_after == amount_before and abs(session - amount_before) > amount_before * 0.001 + 1000:
        raise RuntimeError(f'腾讯逐笔 {symbol} 连续竞价段成交额 {session:.0f} 元，与行情快照 {amount_before:.0f} 元对不上，逐笔可能不全')
    frame = _v39_frame(rows, 'tencent', f'{TENCENT_TICK_URL}?appn=detail&action=data&c={symbol}', ['time', 'seq', 'price', 'change', 'volume', 'amount', 'side'])
    frame.insert(0, 'date', day)
    frame.insert(1, 'code', symbol)
    frame.attrs['missing_seq'] = missing
    return frame

SH_INDEX = {'000300', '000905', '000016', '000688', '000852', '000010'}

def get_prefix(code: str) -> str:
    """6位代码 → 市场前缀（sh/sz/bj）。支持显式前缀/后缀（sh000016 / 000016.SH）透传以解决歧义。"""
    c = code.lower().strip()
    if c.endswith(('.sh', '.sz', '.bj')):
        return c[-2:]
    if c.endswith(('.xshg', '.xshe')):
        return 'sh' if c.endswith('.xshg') else 'sz'
    if c.startswith(('sh', 'sz', 'bj')):
        return c[:2]
    if c.startswith('92'):
        return 'bj'
    if c.startswith(('5', '6', '9')):
        return 'sh'
    if c.startswith(('4', '8')):
        return 'bj'
    if c in SH_INDEX:
        return 'sh'
    return 'sz'

_TICKER_RE = re.compile('^(?:(sh|sz|bj)(\\d{6})|(\\d{6})(?:\\.(sh|sz|bj|xshg|xshe))?)$', re.IGNORECASE)

_JQ_SUFFIX = {'xshg': 'sh', 'xshe': 'sz'}

def _natural_market(digits: str) -> str:
    """6 位码的自然归属市场。仅用于校验显式前缀是否自相矛盾。
    注意 000xxx 是沪指数/深个股共用的歧义段，由调用处单独处理，不走这里。"""
    if digits.startswith(('4', '8', '92')):
        return 'bj'
    if digits[0] in ('5', '6', '9'):
        return 'sh'
    return 'sz'

def norm_ticker(code: str, stock_only: bool=False) -> str:
    """任意受支持写法 → 纯 6 位数字代码。

    支持 600519 / SH600519 / sh600519 / 600519.SH / BJ920982 等。
    stock_only=True：个股专用接口（研报、一致预期等）传这个，会拒绝显式指数写法。
    ⚠️ 不匹配时**抛 ValueError，绝不静默返回空串或猜一个代码**——
    否则调用方会把「代码格式写错」误读成「这只票没有数据」，
    或者更糟：拿到另一只股票的数据还以为是对的。
    """
    raw = str(code).strip()
    m = _TICKER_RE.match(raw)
    if not m:
        raise ValueError(f'无法把 {code!r} 解析为 6 位股票代码；支持格式：600519 / SH600519 / sh600519 / 600519.SH / 600519.XSHG（聚宽）（前缀与后缀二选一，不能同时写）')
    digits = m.group(2) or m.group(3)
    market = (m.group(1) or m.group(4) or '').lower()
    market = _JQ_SUFFIX.get(market, market)
    if market:
        if digits.startswith('000'):
            if market == 'bj':
                raise ValueError(f'{code!r} 市场标识与号段矛盾：000xxx 不属北交所。')
            if stock_only and market == 'sh':
                raise ValueError(f'{code!r} 指向沪市指数而非个股（沪市无 000xxx 个股），本接口只服务个股。要查同号段的深市个股请显式传 sz{digits}。')
        else:
            nat = _natural_market(digits)
            if market != nat:
                raise ValueError(f'{code!r} 的市场标识与号段矛盾：{digits} 属 {nat} 市，而不是 {market} 市。（改用 {nat}{digits} 或去掉市场标识）')
    return digits
