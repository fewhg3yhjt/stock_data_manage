from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Mapping, Sequence

from ...routing.capabilities import ProviderCapability
from ..contracts import FailureClass, ProviderContractError
from ..transport import RealtimeFetchResponse, call_tdx_bars, parse_provider_datetime, tdx_code, tdx_records, tdx_value


@dataclass(slots=True)
class TdxMinuteProvider:
    """Adapter around an injected TDX-compatible client."""

    client_factory: Callable[[], object]
    capability: ProviderCapability
    name: str = "tdx"
    endpoint: str = "delayed_1m"
    timeout_seconds: float = 10.0
    count: int = 240
    frequency: int = 1
    frequency_codes: Mapping[int, int] = field(default_factory=lambda: {1: 8, 5: 0})

    @property
    def capability_version(self) -> str:
        return self.capability.version

    input_hosts = ('https://www.tdx.com.cn',)

    def fetch_daily_package(self, *, date):
        frame=tdx_daily_package(date)
        rows=tuple(frame.drop(columns=['source','source_url','fetched_at']).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=rows,source_url=frame['source_url'].iloc[0])

    def fetch_realtime_minute(self, symbols: Sequence[str], as_of: datetime) -> RealtimeFetchResponse:
        requested = tuple(symbols)
        rows = []
        client = self.client_factory()
        try:
            category = self.frequency_codes.get(self.frequency, self.frequency)
            for source_symbol in requested:
                code = tdx_code(source_symbol)
                try:
                    bars = call_tdx_bars(client, code, category, self.count)
                except ProviderContractError:
                    raise
                except (TimeoutError, OSError, ConnectionError) as exc:
                    raise ProviderContractError(
                        f"TDX minute request failed for {source_symbol}", FailureClass.CONNECTION, retryable=True
                    ) from exc
                for item in tdx_records(bars):
                    timestamp = tdx_value(item, "datetime", "bar_time", "time", "date")
                    if timestamp in (None, ""):
                        continue
                    parsed_time = parse_provider_datetime(timestamp)
                    values = {
                        "open": tdx_value(item, "open", "opening"),
                        "close": tdx_value(item, "close", "price"),
                        "high": tdx_value(item, "high"),
                        "low": tdx_value(item, "low"),
                        "volume": tdx_value(item, "volume", "vol"),
                    }
                    if any(value is None for value in values.values()):
                        raise ProviderContractError(
                            f"TDX minute row schema changed for {source_symbol}", FailureClass.SCHEMA_CHANGED, retryable=False
                        )
                    rows.append({
                        "symbol": source_symbol, "trade_date": parsed_time.date().isoformat(),
                        "bar_time": parsed_time.isoformat(), **values,
                        "amount": tdx_value(item, "amount", "turnover"),
                    })
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        return RealtimeFetchResponse(
            tuple(rows), requested, (),
            ("trade_date", "bar_time", "open", "high", "low", "close", "volume", "amount"),
            ("volume:client_defined", "amount:client_defined"), len(rows),
            str(rows[0]["bar_time"]) if rows else None, str(rows[-1]["bar_time"]) if rows else None,
        )

import math, re, io, csv, time, struct, zipfile, zlib
from xml.etree import ElementTree
from datetime import date, datetime, timedelta
import pandas as pd
from ..contracts import InputFetchResult, _v39_contract, _v39_count, _v39_date, _v39_frame, _v39_json, _v39_num, _v39_req_num, _v39_rows, _v39_src_date
from ..transport import _v39_http

TDX_PACKAGE_URL = 'https://www.tdx.com.cn/products/data/data/g4day/{ymd}.zip'

TDX_BJ_FIRST_DAY = '20220506'

TDX_MIN_PRICED = {'sh': 10000, 'sz': 3000, 'bj': 50}

def _tdx_parse_package(content, ymd):
    """解析通达信每日增量包：每个市场一对 .cod（代码表，150 字节/条）+ .md1（行情块，512 字节/块）。
    布局参考 jing2uo/tdx2db（MIT）的 tdx/merge.go，已用 600519/000001/920000 与腾讯收盘价对拍。"""
    archive = zipfile.ZipFile(io.BytesIO(content))
    names = set(archive.namelist())
    rows = []
    for market in ('sh', 'sz', 'bj'):
        cod_name, md1_name = (f'{market}{ymd[2:]}.cod', f'{market}{ymd[2:]}.md1')
        if market == 'bj' and ymd < TDX_BJ_FIRST_DAY and (cod_name not in names) and (md1_name not in names):
            continue
        if cod_name not in names or md1_name not in names:
            raise RuntimeError(f'通达信盘后包缺少 {cod_name}/{md1_name}，格式可能已变')
        cod, md1 = (archive.read(cod_name), archive.read(md1_name))
        if len(cod) % 150 or len(md1) % 512:
            raise RuntimeError(f'{market} 代码表或行情块长度不是整块，文件可能被截断')
        if len(cod) // 150 != len(md1) // 512:
            raise RuntimeError(f'{market} 代码表 {len(cod) // 150} 条、行情块 {len(md1) // 512} 块，对不上')
        before, codes, seqs = (len(rows), set(), set())
        for offset in range(0, len(cod), 150):
            record = cod[offset:offset + 150]
            code = record[0:6].rstrip(b'\x00 ').decode('ascii', 'replace')
            seq = struct.unpack('<H', record[32:34])[0]
            if not re.fullmatch('[0-9]{6}', code):
                raise RuntimeError(f'通达信盘后包 {market} 代码表出现非 6 位数字代码 {code!r}，格式可能已变')
            if code in codes or seq in seqs:
                raise RuntimeError(f'通达信盘后包 {market} 代码表有重复的代码 / 行情块序号（{code!r}, seq={seq}）')
            codes.add(code)
            seqs.add(seq)
            block = md1[seq * 512:(seq + 1) * 512]
            if len(block) != 512:
                raise RuntimeError(f'{market}{code} 行情块越界（seq={seq}）')
            prev_close = struct.unpack('<d', block[4:12])[0]
            open_, high, low, close = struct.unpack('<4d', block[12:44])
            amount = struct.unpack('<d', block[72:80])[0]
            if not all((math.isfinite(v) for v in (prev_close, open_, high, low, close, amount))):
                raise RuntimeError(f'通达信盘后包 {market}{code} 行情块出现非有限数值，文件可能已损坏')
            if close <= 0:
                continue
            volume = struct.unpack('<Q', block[56:64])[0]
            raw_name = record[40:72].split(b'\x00')[0]
            try:
                name = raw_name.decode('gbk').strip()
            except UnicodeDecodeError as exc:
                raise RuntimeError(f'通达信盘后包 {market}{code} 的名称不是 GBK，文件可能已损坏') from exc
            if not name:
                raise RuntimeError(f'通达信盘后包 {market}{code} 有价格却没有名称，文件可能已损坏')
            rows.append({'date': f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}', 'market': market, 'code': code, 'name': name, 'prev_close': round(prev_close, 4), 'open': round(open_, 4), 'high': round(high, 4), 'low': round(low, 4), 'close': round(close, 4), 'volume': volume, 'amount': round(amount, 2)})
        if len(rows) - before < TDX_MIN_PRICED[market]:
            raise RuntimeError(f'通达信盘后包 {market} 市场只有 {len(rows) - before} 条有价记录（实测下限 {TDX_MIN_PRICED[market]}），文件可能残缺或格式已变')
    return rows

@_v39_contract
def tdx_daily_package(date):
    """通达信官网每日盘后包 — 某一交易日沪深北全部证券的日线（含成交额）。

    走 HTTP 下载（约 2.7MB），与 #52 失效的 TCP 行情命令是两条路。
    个股 volume 单位是「股」、amount 单位是「元」；指数等特殊代码的 volume 为通达信原值。
    非交易日或当日包尚未发布时官网返回 404，本函数抛 ValueError，不返回空表。
    历史包实测 2022-01-04、2023-01-03 可取，2021-01-04 已 404，未逐日验证；
    2022-05-06 之前的包没有北交所文件，只返回沪深，之后缺北交所文件会报错。
    某个市场有价记录少于 TDX_MIN_PRICED 的实测下限、代码不是 6 位数字或重复、行情块序号重复、
    代码表与行情块条数对不上、价格 / 成交额不是有限数，
    都按文件残缺抛 RuntimeError，不把部分市场当全市场返回。
    """
    ymd = _v39_date(date).replace('-', '')
    url = TDX_PACKAGE_URL.format(ymd=ymd)
    response = _v39_http(url, timeout=(10, 90), allow_status=(404,))
    if response.status_code == 404:
        raise ValueError(f'{date} 没有通达信盘后包：非交易日、当日包尚未发布（通常收盘后数小时），或早于官网保留范围（实测 2021-01-04 已没有）')
    if not response.content.startswith(b'PK'):
        raise RuntimeError('通达信盘后包不是 zip 文件，可能是错误页')
    try:
        rows = _tdx_parse_package(response.content, ymd)
    except (zipfile.BadZipFile, zlib.error, EOFError) as exc:
        raise RuntimeError(f'通达信盘后包 {url} 无法解压: {type(exc).__name__}: {exc}') from exc
    return _v39_frame(rows, 'tdx', url)
