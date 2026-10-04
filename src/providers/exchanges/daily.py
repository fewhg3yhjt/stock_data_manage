import math, re, io, csv, time, struct, zipfile, zlib, json
from xml.etree import ElementTree
from datetime import date, datetime, timedelta
import pandas as pd
from ..contracts import InputFetchResult, _v39_contract, _v39_count, _v39_date, _v39_frame, _v39_json, _v39_num, _v39_req_num, _v39_rows, _v39_src_date
from ..transport import _v39_http

FUTURES_EXCHANGES = ('SHFE', 'INE', 'CZCE', 'CFFEX', 'GFEX')

_SHFE_HOSTS = {'SHFE': 'https://www.shfe.com.cn', 'INE': 'https://www.ine.cn'}

CZCE_FILE_URL = 'https://www.czce.com.cn/cn/DFSStaticFiles/{kind}/{year}/{ymd}/{name}.txt'

CZCE_FIRST_DAY = '20150921'

CFFEX_DAILY_URL = 'http://www.cffex.com.cn/fzjy/mrhq/{ym}/{dd}/{ymd}_1.csv'

CFFEX_DAILY_XML = 'http://www.cffex.com.cn/fzjy/mrhq/{ym}/{dd}/index.xml'

CFFEX_RANK_URL = 'http://www.cffex.com.cn/sj/ccpm/{ym}/{dd}/{product}_1.csv'

CFFEX_RANK_FIRST_DAY = {'IF': '20100416', 'IH': '20150416', 'IC': '20150416', 'IM': '20220722', 'TS': '20180817', 'TF': '20130906', 'T': '20150320', 'TL': '20230421'}

GFEX_DAILY_URL = 'http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList'

_DCE_HINT = "大商所官网有 JS 反爬（纯 HTTP 返回 412），不提供官方日行情；大商所品种（豆粕 M、铁矿 I、塑料 L…）请用 futures_kline('M0') 取逐日 K 线（新浪），futures_realtime('M0') 取实时/收盘快照"

_FUT_COLUMNS = ['date', 'exchange', 'symbol', 'product', 'open', 'high', 'low', 'close', 'settle', 'pre_settle', 'volume', 'open_interest', 'oi_change', 'turnover_10k']

_OPT_COLUMNS = ['date', 'exchange', 'symbol', 'series', 'option_type', 'strike', 'open', 'high', 'low', 'close', 'settle', 'pre_settle', 'volume', 'open_interest', 'oi_change', 'turnover_10k', 'delta', 'iv_pct', 'series_iv_pct']

_RANK_COLUMNS = ['date', 'exchange', 'level', 'symbol', 'rank', 'volume_member', 'volume', 'volume_chg', 'long_member', 'long_oi', 'long_chg', 'short_member', 'short_oi', 'short_chg']

def _fut_price(value):
    """期货/期权价格：0 不是有效价格（无成交时交易所填 0 或空），统一成 None。"""
    number = _v39_num(value)
    return None if number == 0 else number

def _fut_product(code):
    """合约代码的品种字母（rb2610 -> rb、IF2609 -> IF）；不是字母开头说明来源格式变了。"""
    match = re.match('[A-Za-z]+', str(code))
    if not match:
        raise RuntimeError(f'合约代码 {code!r} 不是字母开头，格式可能已变')
    return match.group(0)

def _fut_exchange(exchange):
    exchange = str(exchange).upper()
    if exchange == 'DCE':
        raise ValueError(_DCE_HINT)
    if exchange not in FUTURES_EXCHANGES:
        raise ValueError('exchange 只能是 ' + ' / '.join(FUTURES_EXCHANGES) + '（大商所见 futures_kline / futures_realtime）')
    return exchange

def _shfe_json(exchange, path, ymd, key, allow_missing=False):
    """上期所 / 上期能源的 .dat（实为 JSON）。非交易日官网 404；allow_missing 时返回 (None, url)。
    key 是调用方要读的行列表字段（o_curinstrument / o_cursor）；顶层不是对象、它不是由对象组成的列表，抛 RuntimeError。"""
    url = f'{_SHFE_HOSTS[exchange]}/data/tradedata/{path}{ymd}.dat'
    first = _INE_FIRST_DAY.get(path) if exchange == 'INE' else None
    if first and ymd < first:
        if allow_missing:
            return (None, url)
        raise ValueError(f'上期能源该类数据从 {first} 起才有（{ymd} 早于首日）')
    response = _v39_http(url, timeout=(10, 60), allow_status=(404,))
    payload = None
    if response.status_code != 404:
        try:
            payload = json.loads(response.content.decode('utf-8'))
        except ValueError as exc:
            raise RuntimeError(f'{exchange} {url} 返回的不是 JSON，可能是错误页') from exc
        rows = payload.get(key) if isinstance(payload, dict) else None
        if not isinstance(rows, list) or not all((isinstance(r, dict) for r in rows)):
            raise RuntimeError(f'{exchange} {url} 的 {key} 不是由对象组成的列表，格式可能已变')
        reported = payload.get('report_date')
        if reported is not None and str(reported) != ymd:
            raise RuntimeError(f'{exchange} 返回的 report_date={reported}，不是 {ymd}')
        if reported is None and (not any((v for v in payload.values() if isinstance(v, list)))):
            payload = None
    if payload is None:
        if allow_missing:
            return (None, url)
        raise ValueError(f'{exchange} {ymd} 没有数据：非交易日、尚未发布或该品种当时未上市')
    return (payload, url)

def _czce_text(kind, name, ymd):
    if ymd < CZCE_FIRST_DAY:
        raise ValueError(f'郑商所数据从 {CZCE_FIRST_DAY} 起接入（更早的文件是另一套格式）')
    url = CZCE_FILE_URL.format(kind=kind, year=ymd[:4], ymd=ymd, name=name)
    response = _v39_http(url, timeout=(10, 60), allow_status=(404,))
    if response.status_code == 404:
        raise ValueError(f'郑商所 {ymd} 没有 {name}：非交易日或尚未发布')
    try:
        text = response.content.decode('utf-8-sig')
    except UnicodeDecodeError:
        text = response.content.decode('gbk')
    if f'({ymd[:4]}-{ymd[4:6]}-{ymd[6:]})' not in text.split('\n', 1)[0] + text[:200]:
        raise RuntimeError(f'郑商所 {name} 标题里的日期不是 {ymd}')
    return (text, url)

_CZCE_HEADER_ALIAS = {'品种月份': '合约代码', '品种代码': '合约代码', '空盘量': '持仓量'}

def _czce_table(text, header_prefix):
    """郑商所竖线分隔表：返回 (表头, 数据行)，千分位逗号已去掉，小计/总计已剔除。"""
    lines = [line for line in text.splitlines() if '|' in line]
    header = [_CZCE_HEADER_ALIAS.get(c.strip(), c.strip()) for c in lines[0].split('|')] if lines else []
    if not header or header[0] != header_prefix:
        raise RuntimeError('郑商所文件表头改变')
    rows = []
    for line in lines[1:]:
        cells = [c.strip().replace(',', '') for c in line.split('|')]
        if not cells[0] or cells[0].endswith(('小计', '总计', '合计')):
            continue
        rows.append(dict(zip(header, cells)))
    return (header, rows)

def _cffex_csv(url, allow_missing=False):
    response = _v39_http(url, timeout=(10, 60), allow_status=(302, 404), allow_redirects=False)
    if response.status_code in (302, 404):
        if allow_missing:
            return None
        raise ValueError(f'中金所没有该文件（非交易日或尚未发布）: {url}')
    return list(csv.reader(io.StringIO(response.content.decode('gbk'))))

def _cffex_daily_table(ymd):
    """中金所日行情 CSV（期货 + 期权同一个文件）。CSV 里没有交易日列，同目录的 index.xml 每行都带
    tradingday；用它核对交易日，并逐合约核对成交量 / 收盘价 / 持仓量，对不上就抛 RuntimeError，
    不把别的交易日的文件标成这一天（2010–2026 抽 5 天实测两者逐合约一致）。"""
    url = CFFEX_DAILY_URL.format(ym=ymd[:6], dd=ymd[6:], ymd=ymd)
    table = _cffex_csv(url)
    if not table or table[0][:3] != ['合约代码', '今开盘', '最高价']:
        raise RuntimeError('中金所日行情表头改变')
    xml_url = CFFEX_DAILY_XML.format(ym=ymd[:6], dd=ymd[6:])
    response = _v39_http(xml_url, timeout=(10, 60), allow_status=(302, 404), allow_redirects=False)
    if response.status_code in (302, 404):
        raise RuntimeError(f'中金所 {ymd} 有行情 CSV 却没有 index.xml，无法核对交易日: {xml_url}')
    if b'<!DOCTYPE' in response.content or b'<!ENTITY' in response.content:
        raise RuntimeError(f'中金所 index.xml 含 DOCTYPE / ENTITY，拒绝解析: {xml_url}')
    try:
        nodes = ElementTree.fromstring(response.content).findall('dailydata')
    except ElementTree.ParseError as exc:
        raise RuntimeError(f'中金所 index.xml 无法解析: {exc}') from exc
    witness = {}
    for node in nodes:
        values = {k: (node.findtext(k) or '').strip() for k in ('instrumentid', 'tradingday', 'volume', 'closeprice', 'openinterest')}
        if values['tradingday'] != ymd:
            raise RuntimeError(f'中金所 index.xml 的交易日是 {values['tradingday']}，不是 {ymd}')
        witness[values['instrumentid']] = tuple((_v39_num(values[k]) for k in ('volume', 'closeprice', 'openinterest')))
    header = [c.strip() for c in table[0]]
    csv_rows = {}
    for rec in table[1:]:
        code = rec[0].strip() if rec else ''
        if not code or code in ('小计', '合计', '总计'):
            continue
        r = dict(zip(header, rec))
        csv_rows[code] = tuple((_v39_num(r.get(k)) for k in ('成交量', '今收盘', '持仓量')))
    if not witness or len(witness) != len(nodes) or csv_rows != witness:
        diff = sorted(set(csv_rows) ^ set(witness)) or sorted((k for k in csv_rows if csv_rows[k] != witness.get(k)))
        raise RuntimeError(f'中金所 {ymd} 行情 CSV 与 index.xml 对不上（{len(csv_rows)} / {len(witness)} 个合约，例如 {diff[:3]}），不能确认 CSV 属于这一天')
    return (table, url)

_CFFEX_RANK_SUB = ['会员简称', '成交量', '比上一交易日增减', '会员简称', '持买单量', '比上一交易日增减', '会员简称', '持卖单量', '比上一交易日增减']

def _cffex_rank_rows(table, product, ymd):
    """中金所持仓排名 CSV → 行。只在核对过两行表头的「排名」段里取数，列顺序不对就抛错。
    2015 年前后的旧文件在排名段前面还有一段「会员类别」合计（表头同样以 交易日,合约 开头），跳过。"""
    out, in_rank, i = ([], False, 0)
    while i < len(table):
        cells = [c.strip() for c in table[i]]
        if cells[:2] == ['交易日', '合约']:
            in_rank = cells[2:3] == ['排名']
            if in_rank:
                sub = [c.strip() for c in table[i + 1][3:12]] if i + 1 < len(table) else []
                if cells[3:12:3] != ['成交量排名', '持买单量排名', '持卖单量排名'] or sub != _CFFEX_RANK_SUB:
                    raise RuntimeError(f'中金所 {product} 持仓排名表头变了: {cells} / {sub}')
                i += 1
        elif cells[2:3] and cells[2].isdigit():
            if not in_rank:
                raise RuntimeError(f'中金所 {product} 持仓排名在排名表头之前出现数据行: {cells}')
            if len(cells) < 12:
                raise RuntimeError(f'中金所 {product} 持仓排名行缺列: {cells}')
            if cells[0] != ymd:
                raise RuntimeError(f'中金所 {product} 持仓排名交易日是 {cells[0]}，不是 {ymd}')
            out.append({'level': 'contract', 'symbol': cells[1], 'rank': int(cells[2]), 'volume_member': _rank_member(cells[3]), 'volume': _v39_num(cells[4]), 'volume_chg': _v39_num(cells[5]), 'long_member': _rank_member(cells[6]), 'long_oi': _v39_num(cells[7]), 'long_chg': _v39_num(cells[8]), 'short_member': _rank_member(cells[9]), 'short_oi': _v39_num(cells[10]), 'short_chg': _v39_num(cells[11])})
        i += 1
    return out

def _gfex_rows(ymd, trade_type):
    response = _v39_http(GFEX_DAILY_URL, method='POST', data={'trade_date': ymd, 'trade_type': trade_type}, headers={'Referer': 'http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml'})
    payload = _v39_json(response)
    if not isinstance(payload, dict) or str(payload.get('code')) != '0':
        raise RuntimeError(f'广期所返回错误: {str(payload)[:200]}')
    param = payload.get('param')
    if not isinstance(param, dict):
        raise RuntimeError(f'广期所没有回显请求参数（param 应为对象）: {str(payload)[:200]}')
    data = _v39_rows(payload.get('data'), '广期所 data')
    if param.get('trade_date') not in ([ymd], ymd) or param.get('trade_type') not in ([str(trade_type)], str(trade_type)):
        raise RuntimeError(f'广期所回显的请求参数 {param} 与请求的 {ymd}/{trade_type} 不符')
    rows = []
    for r in data:
        if str(r.get('variety', '')).endswith(('小计', '总计')):
            continue
        if not r.get('delivMonth'):
            raise RuntimeError(f'广期所合约行缺 delivMonth，格式可能已变: {str(r)[:120]}')
        rows.append(r)
    if not rows:
        raise ValueError(f'广期所 {ymd} 没有行情：非交易日或尚未发布')
    return (rows, response.url)

_INE_FIRST_DAY = {'future/dailydata/kx': '20180326', 'future/dailydata/pm': '20200703', 'option/dailydata/kx': '20210621'}

def _shfe_ine_ids(path, ymd, key, field):
    """上期所文件里混有上期能源的品种；取能源中心同一天的 ID 集合用来剔除。
    能源中心该文件第一天之前没有文件（或是空壳），上期所文件里也没有能源品种，返回空集合；
    第一天起缺文件不能当成「没有能源品种」，否则 sc 等合约会被标成上期所，直接抛 RuntimeError。"""
    payload, url = _shfe_json('INE', path, ymd, key, allow_missing=True)
    ids = {str(r[field]).strip() for r in payload[key]} if payload else set()
    if not ids and ymd >= _INE_FIRST_DAY[path]:
        raise RuntimeError(f'上期能源 {ymd} 的对照文件缺失或为空（{url}），无法从上期所数据里剔除能源品种；可能尚未发布，稍后重试')
    return ids

@_v39_contract
def futures_daily(date, exchange):
    """期货日行情（交易所官方收盘数据）— 上期所 / 上期能源 / 郑商所 / 中金所 / 广期所。

    exchange: 'SHFE' / 'INE' / 'CZCE' / 'CFFEX' / 'GFEX'；大商所（DCE）官网有反爬，见 futures_kline / futures_realtime。
    一行一个合约（不含小计），settle 为当日结算价，turnover_10k 单位万元，价格为 0 的统一成 None。
    上期所的官方文件里也包含上期能源的品种（原油、20号胶等），这里按能源中心同日文件剔除，
    所以 SHFE 与 INE 两次调用不会重复。非交易日抛 ValueError。
    实测可用起点：上期所 2002-01-07 起（2021 年及以前没有成交额，turnover_10k 为 None）；
    上期能源 2018-03 开业；郑商所 2015-09-21 起（更早是另一套格式，未接入）；中金所 2010-04-16 开业即有；广期所 2022-12 开业。
    """
    exchange = _fut_exchange(exchange)
    day = _v39_date(date)
    ymd = day.replace('-', '')
    rows = []
    if exchange in ('SHFE', 'INE'):
        payload, url = _shfe_json(exchange, 'future/dailydata/kx', ymd, 'o_curinstrument')
        skip = _shfe_ine_ids('future/dailydata/kx', ymd, 'o_curinstrument', 'PRODUCTID') if exchange == 'SHFE' else set()
        for r in payload['o_curinstrument']:
            month = str(r.get('DELIVERYMONTH', '')).strip()
            product_id = r['PRODUCTID'].strip()
            if not product_id.endswith('_f') or not month.isdigit() or product_id in skip:
                continue
            rows.append({'symbol': product_id[:-2] + month, 'product': r['PRODUCTNAME'].strip(), 'open': _fut_price(r['OPENPRICE']), 'high': _fut_price(r['HIGHESTPRICE']), 'low': _fut_price(r['LOWESTPRICE']), 'close': _fut_price(r['CLOSEPRICE']), 'settle': _fut_price(r['SETTLEMENTPRICE']), 'pre_settle': _fut_price(r['PRESETTLEMENTPRICE']), 'volume': _v39_num(r['VOLUME']), 'open_interest': _v39_num(r['OPENINTEREST']), 'oi_change': _v39_num(r['OPENINTERESTCHG']), 'turnover_10k': _v39_num(r.get('TURNOVER'))})
    elif exchange == 'CZCE':
        text, url = _czce_text('Future', 'FutureDataDaily', ymd)
        _, table = _czce_table(text, '合约代码')
        for r in table:
            rows.append({'symbol': r['合约代码'], 'product': _fut_product(r['合约代码']), 'open': _fut_price(r['今开盘']), 'high': _fut_price(r['最高价']), 'low': _fut_price(r['最低价']), 'close': _fut_price(r['今收盘']), 'settle': _fut_price(r['今结算']), 'pre_settle': _fut_price(r['昨结算']), 'volume': _v39_num(r['成交量(手)']), 'open_interest': _v39_num(r['持仓量']), 'oi_change': _v39_num(r['增减量']), 'turnover_10k': _v39_num(r['成交额(万元)'])})
    elif exchange == 'CFFEX':
        table, url = _cffex_daily_table(ymd)
        for rec in table[1:]:
            code = rec[0].strip()
            if not code or code in ('小计', '合计', '总计') or '-C-' in code or ('-P-' in code):
                continue
            r = dict(zip(table[0], rec))
            rows.append({'symbol': code, 'product': _fut_product(code), 'open': _fut_price(r['今开盘']), 'high': _fut_price(r['最高价']), 'low': _fut_price(r['最低价']), 'close': _fut_price(r['今收盘']), 'settle': _fut_price(r['今结算']), 'pre_settle': _fut_price(r['前结算']), 'volume': _v39_num(r['成交量']), 'open_interest': _v39_num(r['持仓量']), 'oi_change': _v39_num(r['持仓变化']), 'turnover_10k': _v39_num(r['成交金额'])})
    else:
        data, url = _gfex_rows(ymd, 0)
        for r in data:
            rows.append({'symbol': r['varietyOrder'] + r['delivMonth'], 'product': r['variety'], 'open': _fut_price(r['open']), 'high': _fut_price(r['high']), 'low': _fut_price(r['low']), 'close': _fut_price(r['close']), 'settle': _fut_price(r['clearPrice']), 'pre_settle': _fut_price(r['lastClear']), 'volume': _v39_num(r['volumn']), 'open_interest': _v39_num(r['openInterest']), 'oi_change': _v39_num(r['diffI']), 'turnover_10k': _v39_num(r['turnover'])})
    if not rows:
        raise RuntimeError(f'{exchange} {day} 解析出 0 个期货合约，格式可能已变')
    for row in rows:
        row['date'], row['exchange'] = (day, exchange)
    frame = _v39_frame(rows, exchange.lower(), url, _FUT_COLUMNS)
    if frame.duplicated(['symbol']).any():
        raise RuntimeError(f'{exchange} {day} 期货合约代码重复')
    return frame

_OPTION_CODE = re.compile('^([A-Za-z]+\\d{3,4}(?:[A-Z]{2})?)-?([CP])-?(\\d+(?:\\.\\d+)?)$')

@_v39_contract
def options_daily(date, exchange):
    """商品期权 / 股指期权日行情（交易所官方）— 上期所 / 上期能源 / 郑商所 / 中金所 / 广期所。

    series：期权系列（商品期权 = 标的期货合约，如 cu2610；中金所 = HO/IO/MO + 月份；
    郑商所部分品种另有 CF701MS 这类带后缀的系列，按官方代码原样保留，与 CF701 分开）。
    delta：交易所公布值（中金所不公布，为 None）。
    iv_pct：逐合约隐含波动率 %（郑商所、广期所公布）；series_iv_pct：上期所/能源中心按系列公布的
    隐含波动率（官方 SIGMA × 100）。ETF 期权不在这里，见 Layer 9。非交易日抛 ValueError。
    各所期权上市时间不同（上期所铜期权 2018-09、郑商所白糖期权 2017-04），之前的日期抛 ValueError。
    """
    exchange = _fut_exchange(exchange)
    day = _v39_date(date)
    ymd = day.replace('-', '')
    rows = []
    if exchange in ('SHFE', 'INE'):
        payload, url = _shfe_json(exchange, 'option/dailydata/kx', ymd, 'o_curinstrument')
        skip = _shfe_ine_ids('option/dailydata/kx', ymd, 'o_curinstrument', 'PRODUCTID') if exchange == 'SHFE' else set()
        sigma_rows = _v39_rows(payload.get('o_cursigma'), f'{exchange} {url} 的 o_cursigma')
        if not all(('INSTRUMENTID' in r for r in sigma_rows)):
            raise RuntimeError(f'{exchange} {url} 的 o_cursigma 行没有 INSTRUMENTID')
        sigma = {}
        for r in sigma_rows:
            series_id = str(r['INSTRUMENTID']).strip()
            if series_id in ('小计', '合计', '总计'):
                continue
            if not series_id:
                raise RuntimeError(f'{exchange} {url} 的 o_cursigma 有空的 INSTRUMENTID')
            if series_id in sigma:
                raise RuntimeError(f'{exchange} {url} 的 o_cursigma 里 {series_id} 出现两次，隐含波动率会互相覆盖')
            sigma[series_id] = _v39_req_num(r.get('SIGMA'), f'{exchange} {series_id} 的 SIGMA')
        for r in payload['o_curinstrument']:
            code = str(r.get('INSTRUMENTID', '')).strip()
            kind = {'1': 'C', '2': 'P'}.get(str(r.get('OPTIONSTYPE')))
            if kind is None or r['PRODUCTID'].strip() in skip:
                continue
            parsed = _OPTION_CODE.match(code)
            if not parsed or parsed.group(2) != kind:
                raise RuntimeError(f'{exchange} 期权 {code} 的代码与 OPTIONSTYPE={r.get('OPTIONSTYPE')} 不一致')
            series = str(r['UNDERLYINGINSTRID']).strip()
            if series not in sigma:
                raise RuntimeError(f'{exchange} {url} 的 o_cursigma 里没有系列 {series}，结果不完整')
            iv = sigma[series]
            rows.append({'symbol': code, 'series': series, 'option_type': kind, 'strike': _v39_num(r['STRIKEPRICE']), 'open': _fut_price(r['OPENPRICE']), 'high': _fut_price(r['HIGHESTPRICE']), 'low': _fut_price(r['LOWESTPRICE']), 'close': _fut_price(r['CLOSEPRICE']), 'settle': _fut_price(r['SETTLEMENTPRICE']), 'pre_settle': _fut_price(r['PRESETTLEMENTPRICE']), 'volume': _v39_num(r['VOLUME']), 'open_interest': _v39_num(r['OPENINTEREST']), 'oi_change': _v39_num(r['OPENINTERESTCHG']), 'turnover_10k': _v39_num(r['TURNOVER']), 'delta': _v39_num(r.get('DELTA')), 'iv_pct': None, 'series_iv_pct': round(iv * 100, 4) if iv is not None else None})
    elif exchange == 'CZCE':
        text, url = _czce_text('Option', 'OptionDataDaily', ymd)
        if '无交易记录' in text:
            raise ValueError(f'郑商所 {ymd} 没有期权成交记录（郑商所期权 2017-04-19 起上市）')
        _, table = _czce_table(text, '合约代码')
        for r in table:
            parsed = _OPTION_CODE.match(r['合约代码'])
            if not parsed:
                raise RuntimeError(f'郑商所期权代码无法解析: {r['合约代码']}')
            rows.append({'symbol': r['合约代码'], 'series': parsed.group(1), 'option_type': parsed.group(2), 'strike': _v39_num(parsed.group(3)), 'open': _fut_price(r['今开盘']), 'high': _fut_price(r['最高价']), 'low': _fut_price(r['最低价']), 'close': _fut_price(r['今收盘']), 'settle': _fut_price(r['今结算']), 'pre_settle': _fut_price(r['昨结算']), 'volume': _v39_num(r['成交量(手)']), 'open_interest': _v39_num(r['持仓量']), 'oi_change': _v39_num(r['增减量']), 'turnover_10k': _v39_num(r['成交额(万元)']), 'delta': _v39_num(r['DELTA']), 'iv_pct': _v39_num(r['隐含波动率']), 'series_iv_pct': None})
    elif exchange == 'CFFEX':
        table, url = _cffex_daily_table(ymd)
        for rec in table[1:]:
            code = rec[0].strip()
            if '-C-' not in code and '-P-' not in code:
                continue
            parsed = _OPTION_CODE.match(code)
            if not parsed:
                raise RuntimeError(f'中金所期权代码无法解析: {code}')
            r = dict(zip(table[0], rec))
            rows.append({'symbol': code, 'series': parsed.group(1), 'option_type': parsed.group(2), 'strike': _v39_num(parsed.group(3)), 'open': _fut_price(r['今开盘']), 'high': _fut_price(r['最高价']), 'low': _fut_price(r['最低价']), 'close': _fut_price(r['今收盘']), 'settle': _fut_price(r['今结算']), 'pre_settle': _fut_price(r['前结算']), 'volume': _v39_num(r['成交量']), 'open_interest': _v39_num(r['持仓量']), 'oi_change': _v39_num(r['持仓变化']), 'turnover_10k': _v39_num(r['成交金额']), 'delta': None, 'iv_pct': None, 'series_iv_pct': None})
    else:
        data, url = _gfex_rows(ymd, 1)
        for r in data:
            parsed = _OPTION_CODE.match(r['delivMonth'])
            if not parsed:
                raise RuntimeError(f'广期所期权代码无法解析: {r['delivMonth']}')
            rows.append({'symbol': r['delivMonth'], 'series': parsed.group(1), 'option_type': parsed.group(2), 'strike': _v39_num(parsed.group(3)), 'open': _fut_price(r['open']), 'high': _fut_price(r['high']), 'low': _fut_price(r['low']), 'close': _fut_price(r['close']), 'settle': _fut_price(r['clearPrice']), 'pre_settle': _fut_price(r['lastClear']), 'volume': _v39_num(r['volumn']), 'open_interest': _v39_num(r['openInterest']), 'oi_change': _v39_num(r['diffI']), 'turnover_10k': _v39_num(r['turnover']), 'delta': _v39_num(r['delta']), 'iv_pct': _v39_num(r['impliedVolatility']), 'series_iv_pct': None})
    if not rows:
        raise RuntimeError(f'{exchange} {day} 解析出 0 个期权合约，格式可能已变')
    for row in rows:
        row['date'], row['exchange'] = (day, exchange)
    frame = _v39_frame(rows, exchange.lower(), url, _OPT_COLUMNS)
    if frame.duplicated(['symbol']).any():
        raise RuntimeError(f'{exchange} {day} 期权合约代码重复')
    return frame

def _rank_member(value):
    value = str(value or '').strip()
    return value if value and value != '-' else None

@_v39_contract
def futures_position_rank(date, exchange, symbol=None):
    """期货会员成交量 / 持买单 / 持卖单前 20 名（交易所官方持仓排名）。

    exchange: 'SHFE' / 'INE' / 'CZCE' / 'CFFEX'（广期所、大商所未接入）。
    level='contract' 为单个合约；level='product' 为品种合计，只有郑商所公布（symbol 为品种字母，如 AP）。
    上期所 / 能源中心文件里的 cuall 行是按会员类型的汇总、没有名次，已剔除。
    symbol 可选，按合约或品种过滤（不区分大小写）。中金所按 IF/IH/IC/IM/TS/TF/T/TL 各取一个文件，
    当天已上市的品种缺任何一个都抛 RuntimeError（不返回部分品种）；source_url 列出实际读取的文件。
    上期能源 2019 年的排名文件是空的（抛 ValueError），实测 2021 年起有数据。
    """
    exchange = _fut_exchange(exchange)
    if exchange == 'GFEX':
        raise ValueError('广期所持仓排名未接入')
    day = _v39_date(date)
    ymd = day.replace('-', '')
    rows = []
    if exchange in ('SHFE', 'INE'):
        payload, url = _shfe_json(exchange, 'future/dailydata/pm', ymd, 'o_cursor')
        skip = _shfe_ine_ids('future/dailydata/pm', ymd, 'o_cursor', 'INSTRUMENTID') if exchange == 'SHFE' else set()
        for r in payload['o_cursor']:
            code = str(r['INSTRUMENTID']).strip()
            rank = _v39_num(r['RANK'])
            if rank is None:
                raise RuntimeError(f'{exchange} {code} 持仓排名缺名次字段')
            if not 1 <= rank <= 20 or code in skip:
                continue
            rows.append({'level': 'contract', 'symbol': code, 'rank': int(rank), 'volume_member': _rank_member(r['PARTICIPANTABBR1']), 'volume': _v39_num(r['CJ1']), 'volume_chg': _v39_num(r['CJ1_CHG']), 'long_member': _rank_member(r['PARTICIPANTABBR2']), 'long_oi': _v39_num(r['CJ2']), 'long_chg': _v39_num(r['CJ2_CHG']), 'short_member': _rank_member(r['PARTICIPANTABBR3']), 'short_oi': _v39_num(r['CJ3']), 'short_chg': _v39_num(r['CJ3_CHG'])})
    elif exchange == 'CZCE':
        text, url = _czce_text('Future', 'FutureDataHolding', ymd)
        level = code = None
        for line in text.splitlines():
            head = re.match('^(品种|合约)：\\s*(\\S+)\\s+日期：', line)
            if head:
                level = 'product' if head.group(1) == '品种' else 'contract'
                found = re.search('[A-Za-z]+\\d*$', head.group(2))
                if not found:
                    raise RuntimeError(f'郑商所持仓排名表头认不出品种 / 合约: {line[:60]}')
                code = found.group(0)
                continue
            cells = [c.strip().replace(',', '') for c in line.split('|')]
            if len(cells) < 10 or not cells[0].isdigit():
                continue
            if code is None:
                raise RuntimeError('郑商所持仓排名在品种/合约标题之前出现数据行')
            rows.append({'level': level, 'symbol': code, 'rank': int(cells[0]), 'volume_member': _rank_member(cells[1]), 'volume': _v39_num(cells[2]), 'volume_chg': _v39_num(cells[3]), 'long_member': _rank_member(cells[4]), 'long_oi': _v39_num(cells[5]), 'long_chg': _v39_num(cells[6]), 'short_member': _rank_member(cells[7]), 'short_oi': _v39_num(cells[8]), 'short_chg': _v39_num(cells[9])})
    else:
        expected = [p for p, first in CFFEX_RANK_FIRST_DAY.items() if ymd >= first]
        if not expected:
            raise ValueError('中金所持仓排名从 2010-04-16（沪深300 期货上市）起才有')
        urls, missing = ([], [])
        for product in expected:
            file_url = CFFEX_RANK_URL.format(ym=ymd[:6], dd=ymd[6:], product=product)
            table = _cffex_csv(file_url, allow_missing=True)
            if table is None:
                missing.append(product)
                continue
            urls.append(file_url)
            parsed = _cffex_rank_rows(table, product, ymd)
            rows.extend(parsed)
            if not parsed:
                raise RuntimeError(f'中金所 {product} {day} 持仓排名文件解析出 0 行，格式可能已变')
        if len(missing) == len(expected):
            raise ValueError(f'中金所 {day} 没有持仓排名：非交易日或尚未发布')
        if missing:
            raise RuntimeError(f'中金所 {day} 缺少已上市品种 {'/'.join(missing)} 的持仓排名，结果不完整（可能尚未全部发布，稍后重试）')
        url = ' | '.join(urls)
    if not rows:
        raise RuntimeError(f'{exchange} {day} 持仓排名解析出 0 行，格式可能已变')
    for row in rows:
        row['date'], row['exchange'] = (day, exchange)
    frame = _v39_frame(rows, exchange.lower(), url, _RANK_COLUMNS)
    if frame.duplicated(['level', 'symbol', 'rank']).any():
        raise RuntimeError(f'{exchange} {day} 持仓排名 合约+名次 重复')
    if symbol:
        frame = frame[frame['symbol'].str.upper() == str(symbol).upper()].reset_index(drop=True)
    return frame

class ExchangeDailyProvider:
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
