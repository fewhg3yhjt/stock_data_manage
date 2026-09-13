from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import traceback
from pathlib import Path

import pandas as pd
import requests


# ============================================================
# 测试对象
# ============================================================

STOCK = "600519"
STOCK_EM = "1.600519"
STOCK_TX = "sh600519"

INDEX = "000001"
INDEX_EM = "1.000001"
INDEX_TX = "sh000001"

# 东方财富行业板块：银行Ⅱ
BOARD_CODE = "BK0475"
BOARD_EM = f"90.{BOARD_CODE}"

OUT = Path("market_source_probe")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/152 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
})


# ============================================================
# 通用
# ============================================================

RESULTS = []


def as_df(obj):
    if obj is None:
        return pd.DataFrame()

    if isinstance(obj, pd.DataFrame):
        return obj.copy()

    if isinstance(obj, list):
        return pd.DataFrame(obj)

    if isinstance(obj, dict):
        return pd.DataFrame([obj])

    return pd.DataFrame({"value": [obj]})


def safe_float(v):
    try:
        return float(v)
    except Exception:
        return v


def http_json(url, params=None, timeout=15):
    r = SESSION.get(
        url,
        params=params,
        timeout=timeout,
    )

    r.raise_for_status()

    text = r.text.strip()

    # 部分腾讯接口可能返回 JSONP / var=JSON
    if not text.startswith("{") and "{" in text:
        text = text[text.find("{"): text.rfind("}") + 1]

    return json.loads(text)


def find_time_range(df):
    if df.empty:
        return None, None

    candidates = [
        "datetime",
        "日期时间",
        "date",
        "日期",
        "time",
        "时间",
        "day",
    ]

    for col in candidates:
        if col in df.columns:
            values = df[col].dropna().astype(str)

            if not values.empty:
                return values.iloc[0], values.iloc[-1]

    if isinstance(df.index, pd.DatetimeIndex):
        return str(df.index.min()), str(df.index.max())

    return None, None


def run_case(provider, test, fn):
    print()
    print("=" * 100)
    print(f"[{provider}] {test}")

    begin = time.perf_counter()

    try:
        df = as_df(fn())

        elapsed = time.perf_counter() - begin

        start, end = find_time_range(df)

        status = "OK" if not df.empty else "EMPTY"

        print(
            f"status={status} "
            f"rows={len(df)} "
            f"elapsed={elapsed:.2f}s "
            f"range={start} -> {end}"
        )

        print("columns:")
        print(list(df.columns))

        if not df.empty:
            print("\nHEAD:")
            print(df.head(3).to_string(index=False))

            print("\nTAIL:")
            print(df.tail(3).to_string(index=False))

            filename = (
                f"{provider}__{test}"
                .replace("/", "_")
                .replace(" ", "_")
            )

            df.to_csv(
                OUT / f"{filename}.csv",
                index=False,
                encoding="utf-8-sig",
            )

        RESULTS.append({
            "provider": provider,
            "test": test,
            "status": status,
            "elapsed_sec": round(elapsed, 3),
            "rows": len(df),
            "start": start,
            "end": end,
            "columns": "|".join(map(str, df.columns)),
            "error": "",
        })

    except Exception as e:

        elapsed = time.perf_counter() - begin

        error = f"{type(e).__name__}: {e}"

        print(
            f"status=ERROR "
            f"elapsed={elapsed:.2f}s"
        )

        print(error)

        print(
            traceback.format_exc(
                limit=3
            )
        )

        RESULTS.append({
            "provider": provider,
            "test": test,
            "status": "ERROR",
            "elapsed_sec": round(elapsed, 3),
            "rows": 0,
            "start": None,
            "end": None,
            "columns": "",
            "error": error,
        })


def skip(provider, test, reason):
    print()
    print(
        f"[{provider}] {test}: "
        f"SKIP - {reason}"
    )

    RESULTS.append({
        "provider": provider,
        "test": test,
        "status": "SKIP",
        "elapsed_sec": 0,
        "rows": 0,
        "start": None,
        "end": None,
        "columns": "",
        "error": reason,
    })


# ============================================================
# 东方财富：原始 HTTP
# ============================================================

def em_kline(
    secid,
    klt="101",
    beg="20260801",
    end="20500101",
    fqt="0",
):
    url = (
        "https://push2his.eastmoney.com/"
        "api/qt/stock/kline/get"
    )

    params = {
        "secid": secid,

        "fields1":
            "f1,f2,f3,f4,f5,f6",

        "fields2":
            "f51,f52,f53,f54,f55,"
            "f56,f57,f58,f59,f60,f61",

        "klt": klt,

        # 0 不复权
        # 1 前复权
        # 2 后复权
        "fqt": fqt,

        "beg": beg,
        "end": end,

        "smplmt": "10000",
        "lmt": "1000000",
    }

    data = http_json(
        url,
        params,
    )

    klines = (
        (data.get("data") or {})
        .get("klines")
        or []
    )

    rows = [
        x.split(",")
        for x in klines
    ]

    columns = [
        "datetime",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "pct_chg",
        "chg",
        "turnover",
    ]

    df = pd.DataFrame(
        rows,
        columns=columns,
    )

    for col in columns[1:]:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce",
        )

    return df


def em_1m(
    secid,
    ndays=5,
):
    url = (
        "https://push2his.eastmoney.com/"
        "api/qt/stock/trends2/get"
    )

    params = {
        "fields1":
            "f1,f2,f3,f4,f5,f6,f7,"
            "f8,f9,f10,f11,f12,f13",

        "fields2":
            "f51,f52,f53,f54,"
            "f55,f56,f57,f58",

        "iscr": "0",

        "ndays": str(ndays),

        "secid": secid,
    }

    data = http_json(
        url,
        params,
    )

    trends = (
        (data.get("data") or {})
        .get("trends")
        or []
    )

    rows = [
        x.split(",")
        for x in trends
    ]

    columns = [
        "datetime",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "avg",
    ]

    df = pd.DataFrame(
        rows,
        columns=columns,
    )

    for col in columns[1:]:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce",
        )

    return df


# ============================================================
# AKShare：东方财富封装
#
# 注意：
# 这是为了比较
# 东财原始接口 VS AKShare封装
# 它们不是独立数据源
# ============================================================

def ak_stock_daily():
    import akshare as ak

    return ak.stock_zh_a_hist(
        symbol=STOCK,
        period="daily",
        start_date="20260801",
        end_date="20260912",
        adjust="",
    )


def ak_stock_1m():
    import akshare as ak

    return ak.stock_zh_a_hist_min_em(
        symbol=STOCK,
        period="1",
        start_date="2026-09-01 09:30:00",
        end_date="2026-09-12 15:00:00",
        adjust="",
    )


def ak_board_daily():
    import akshare as ak

    return ak.stock_board_industry_hist_em(
        symbol=BOARD_CODE,
        start_date="20260801",
        end_date="20260912",
        period="日k",
        adjust="",
    )


def ak_board_5m():
    import akshare as ak

    return ak.stock_board_industry_hist_min_em(
        symbol=BOARD_CODE,
        period="5",
    )


# ============================================================
# 腾讯财经
# ============================================================

def tx_day(
    symbol,
    count=120,
):
    url = (
        "https://web.ifzq.gtimg.cn/"
        "appstock/app/fqkline/get"
    )

    params = {
        "param":
            f"{symbol},day,,,"
            f"{count},qfq"
    }

    data = http_json(
        url,
        params,
    )

    stock = (
        (data.get("data") or {})
        .get(symbol)
        or {}
    )

    bars = (
        stock.get("qfqday")
        or stock.get("day")
        or []
    )

    rows = []

    for x in bars:

        if len(x) < 6:
            continue

        rows.append({
            "date": x[0],
            "open": safe_float(x[1]),
            "close": safe_float(x[2]),
            "high": safe_float(x[3]),
            "low": safe_float(x[4]),
            "volume": safe_float(x[5]),
        })

    return pd.DataFrame(rows)


def tx_minute(
    symbol,
    period=1,
    count=120,
):
    url = (
        "https://ifzq.gtimg.cn/"
        "appstock/app/kline/mkline"
    )

    params = {
        "param":
            f"{symbol},m{period},,"
            f"{count}"
    }

    data = http_json(
        url,
        params,
    )

    stock = (
        (data.get("data") or {})
        .get(symbol)
        or {}
    )

    bars = (
        stock.get(
            f"m{period}"
        )
        or []
    )

    rows = []

    for x in bars:

        if len(x) < 6:
            continue

        rows.append({
            "datetime": x[0],
            "open": safe_float(x[1]),
            "close": safe_float(x[2]),
            "high": safe_float(x[3]),
            "low": safe_float(x[4]),
            "volume": safe_float(x[5]),
        })

    return pd.DataFrame(rows)


# ============================================================
# 新浪财经
# ============================================================

def sina_kline(
    symbol,
    scale,
    datalen=120,
):
    url = (
        "https://money.finance.sina.com.cn/"
        "quotes_service/api/json_v2.php/"
        "CN_MarketData.getKLineData"
    )

    params = {
        "symbol": symbol,
        "scale": str(scale),
        "ma": "no",
        "datalen": str(datalen),
    }

    r = SESSION.get(
        url,
        params=params,
        timeout=15,
    )

    r.raise_for_status()

    return pd.DataFrame(
        json.loads(
            r.text
        )
    )


# ============================================================
# BaoStock
# ============================================================

def baostock_query(
    code,
    frequency,
    start_date,
    end_date,
):
    import baostock as bs

    login = bs.login()

    if login.error_code != "0":
        raise RuntimeError(
            "BaoStock login failed: "
            f"{login.error_code} "
            f"{login.error_msg}"
        )

    try:

        if frequency == "d":

            fields = (
                "date,code,"
                "open,high,low,close,"
                "preclose,"
                "volume,amount,"
                "adjustflag,"
                "turn,"
                "tradestatus,"
                "pctChg,"
                "peTTM,"
                "pbMRQ,"
                "isST"
            )

        else:

            fields = (
                "date,time,code,"
                "open,high,low,close,"
                "volume,amount,"
                "adjustflag"
            )

        rs = bs.query_history_k_data_plus(
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,

            # 3 = 不复权
            adjustflag="3",
        )

        if rs.error_code != "0":
            raise RuntimeError(
                "BaoStock query failed: "
                f"{rs.error_code} "
                f"{rs.error_msg}"
            )

        rows = []

        while (
            rs.error_code == "0"
            and rs.next()
        ):
            rows.append(
                rs.get_row_data()
            )

        return pd.DataFrame(
            rows,
            columns=rs.fields,
        )

    finally:

        bs.logout()


# ============================================================
# 通达信 TCP / Mootdx
# ============================================================

def create_tdx_client():
    from mootdx.quotes import Quotes

    # 不同版本 mootdx 参数略有差异
    try:

        return Quotes.factory(
            market="std",
            multithread=False,
            heartbeat=False,
            bestip=True,
            timeout=8,
            quiet=True,
        )

    except TypeError:

        return Quotes.factory(
            market="std",
            multithread=False,
            heartbeat=False,
        )


def tdx_bars(
    symbol,
    frequency,
    offset=120,
):
    old_timeout = socket.getdefaulttimeout()

    socket.setdefaulttimeout(8)

    client = None

    try:

        client = create_tdx_client()

        try:

            return client.bars(
                symbol=symbol,
                frequency=frequency,
                offset=offset,
            )

        except TypeError:

            # 兼容旧版本
            return client.bars(
                symbol=symbol,
                category=frequency,
                offset=offset,
            )

    finally:

        socket.setdefaulttimeout(
            old_timeout
        )

        if client is not None:

            try:
                client.close()
            except Exception:
                pass


def tdx_index(
    symbol,
    frequency=9,
):
    old_timeout = socket.getdefaulttimeout()

    socket.setdefaulttimeout(8)

    client = None

    try:

        client = create_tdx_client()

        return client.index(
            symbol=symbol,
            frequency=frequency,
        )

    finally:

        socket.setdefaulttimeout(
            old_timeout
        )

        if client is not None:

            try:
                client.close()
            except Exception:
                pass


# ============================================================
# 通达信本地文件
# ============================================================

def tdx_local_daily(
    tdx_dir,
):
    from mootdx.reader import Reader

    reader = Reader.factory(
        market="std",
        tdxdir=tdx_dir,
    )

    return reader.daily(
        symbol=STOCK
    )


def tdx_local_1m(
    tdx_dir,
):
    from mootdx.reader import Reader

    reader = Reader.factory(
        market="std",
        tdxdir=tdx_dir,
    )

    return reader.minute(
        symbol=STOCK,
        suffix="1",
    )


def tdx_local_5m(
    tdx_dir,
):
    from mootdx.reader import Reader

    reader = Reader.factory(
        market="std",
        tdxdir=tdx_dir,
    )

    return reader.minute(
        symbol=STOCK,
        suffix="5",
    )


def tdx_local_blocks(
    tdx_dir,
):
    from mootdx.reader import Reader

    reader = Reader.factory(
        market="std",
        tdxdir=tdx_dir,
    )

    return reader.block(
        symbol="block_zs",
        group=False,
    )


# ============================================================
# 通达信官方全量日线包
#
# 不真的把整个 ZIP 下载下来
# 这里只检测：
# URL
# HTTP
# 文件大小
# ZIP 文件头
# ============================================================

def tdx_zip_probe():

    url = (
        "https://data.tdx.com.cn/"
        "vipdoc/hsjday.zip"
    )

    with SESSION.get(
        url,
        stream=True,
        timeout=20,
    ) as r:

        r.raise_for_status()

        first = next(
            r.iter_content(
                chunk_size=64
            ),
            b"",
        )

        return pd.DataFrame([{
            "url": url,

            "http_status":
                r.status_code,

            "content_length":
                r.headers.get(
                    "Content-Length"
                ),

            "content_type":
                r.headers.get(
                    "Content-Type"
                ),

            "first_bytes_hex":
                first[:8].hex(),

            "is_zip_header":
                first[:2] == b"PK",
        }])


# ============================================================
# 环境信息
# ============================================================

def print_environment():

    print(
        "Python:",
        sys.version.replace(
            "\n",
            " ",
        ),
    )

    modules = [
        "pandas",
        "requests",
        "akshare",
        "baostock",
        "mootdx",
        "tdxpy",
    ]

    for name in modules:

        try:

            module = __import__(name)

            print(
                f"{name}: "
                f"{getattr(module, '__version__', 'installed')}"
            )

        except Exception as e:

            print(
                f"{name}: "
                f"NOT INSTALLED "
                f"({e})"
            )


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tdx-dir",
        default=os.environ.get(
            "TDX_DIR",
            "",
        ),
        help=(
            r"通达信安装目录，如 C:\new_tdx；"
            r"留空则跳过本地文件测试"
        ),
    )

    args = parser.parse_args()

    OUT.mkdir(
        parents=True,
        exist_ok=True,
    )

    print_environment()

    # --------------------------------------------------------
    # 1 东方财富 原始HTTP
    # --------------------------------------------------------

    run_case(
        "eastmoney_raw",
        "stock_daily_600519",
        lambda:
            em_kline(
                STOCK_EM,
                "101",
            ),
    )

    run_case(
        "eastmoney_raw",
        "stock_1m_600519",
        lambda:
            em_1m(
                STOCK_EM,
                5,
            ),
    )

    run_case(
        "eastmoney_raw",
        "index_daily_000001",
        lambda:
            em_kline(
                INDEX_EM,
                "101",
            ),
    )

    run_case(
        "eastmoney_raw",
        "index_1m_000001",
        lambda:
            em_1m(
                INDEX_EM,
                5,
            ),
    )

    run_case(
        "eastmoney_raw",
        f"board_daily_{BOARD_CODE}",
        lambda:
            em_kline(
                BOARD_EM,
                "101",
            ),
    )

    run_case(
        "eastmoney_raw",
        f"board_5m_{BOARD_CODE}",
        lambda:
            em_kline(
                BOARD_EM,
                "5",
            ),
    )

    run_case(
        "eastmoney_raw",
        f"board_1m_{BOARD_CODE}",
        lambda:
            em_1m(
                BOARD_EM,
                1,
            ),
    )

    # --------------------------------------------------------
    # 2 AKShare 东方财富
    # --------------------------------------------------------

    run_case(
        "akshare_em",
        "stock_daily_600519",
        ak_stock_daily,
    )

    run_case(
        "akshare_em",
        "stock_1m_600519",
        ak_stock_1m,
    )

    run_case(
        "akshare_em",
        f"board_daily_{BOARD_CODE}",
        ak_board_daily,
    )

    run_case(
        "akshare_em",
        f"board_5m_{BOARD_CODE}",
        ak_board_5m,
    )

    # --------------------------------------------------------
    # 3 腾讯
    # --------------------------------------------------------

    run_case(
        "tencent",
        "stock_daily_600519",
        lambda:
            tx_day(
                STOCK_TX
            ),
    )

    run_case(
        "tencent",
        "stock_1m_600519",
        lambda:
            tx_minute(
                STOCK_TX,
                1,
            ),
    )

    run_case(
        "tencent",
        "stock_5m_600519",
        lambda:
            tx_minute(
                STOCK_TX,
                5,
            ),
    )

    run_case(
        "tencent",
        "index_daily_000001",
        lambda:
            tx_day(
                INDEX_TX
            ),
    )

    run_case(
        "tencent",
        "index_1m_000001",
        lambda:
            tx_minute(
                INDEX_TX,
                1,
            ),
    )

    skip(
        "tencent",
        "industry_board",
        "暂不作为统一行业板块历史源",
    )

    # --------------------------------------------------------
    # 4 新浪
    # --------------------------------------------------------

    # 240 = 日K
    run_case(
        "sina",
        "stock_daily_600519",
        lambda:
            sina_kline(
                STOCK_TX,
                240,
            ),
    )

    run_case(
        "sina",
        "stock_5m_600519",
        lambda:
            sina_kline(
                STOCK_TX,
                5,
            ),
    )

    run_case(
        "sina",
        "index_daily_000001",
        lambda:
            sina_kline(
                INDEX_TX,
                240,
            ),
    )

    skip(
        "sina",
        "industry_board",
        "板块覆盖较弱",
    )

    # --------------------------------------------------------
    # 5 BaoStock
    # --------------------------------------------------------

    run_case(
        "baostock",
        "stock_daily_600519",
        lambda:
            baostock_query(
                "sh.600519",
                "d",
                "2026-08-01",
                "2026-09-12",
            ),
    )

    run_case(
        "baostock",
        "stock_5m_600519",
        lambda:
            baostock_query(
                "sh.600519",
                "5",
                "2026-09-01",
                "2026-09-12",
            ),
    )

    run_case(
        "baostock",
        "index_daily_000001",
        lambda:
            baostock_query(
                "sh.000001",
                "d",
                "2026-08-01",
                "2026-09-12",
            ),
    )

    skip(
        "baostock",
        "index_minute",
        "BaoStock指数没有分钟线",
    )

    skip(
        "baostock",
        "industry_board",
        "不是板块指数分钟行情主源",
    )

    # --------------------------------------------------------
    # 6 TDX TCP
    #
    # frequency:
    # 9 日线
    # 8 1分钟
    # --------------------------------------------------------

    run_case(
        "tdx_tcp",
        "stock_daily_600519",
        lambda:
            tdx_bars(
                STOCK,
                9,
                120,
            ),
    )

    run_case(
        "tdx_tcp",
        "stock_1m_600519",
        lambda:
            tdx_bars(
                STOCK,
                8,
                120,
            ),
    )

    run_case(
        "tdx_tcp",
        "index_daily_000001",
        lambda:
            tdx_index(
                INDEX,
                9,
            ),
    )

    # --------------------------------------------------------
    # 7 通达信官方历史文件
    # --------------------------------------------------------

    run_case(
        "tdx_official_zip",
        "hsjday_zip_probe",
        tdx_zip_probe,
    )

    # --------------------------------------------------------
    # 8 通达信本地文件
    # --------------------------------------------------------

    if args.tdx_dir:

        tdx_dir = args.tdx_dir

        run_case(
            "tdx_local",
            "stock_daily_600519",
            lambda:
                tdx_local_daily(
                    tdx_dir
                ),
        )

        run_case(
            "tdx_local",
            "stock_1m_600519",
            lambda:
                tdx_local_1m(
                    tdx_dir
                ),
        )

        run_case(
            "tdx_local",
            "stock_5m_600519",
            lambda:
                tdx_local_5m(
                    tdx_dir
                ),
        )

        run_case(
            "tdx_local",
            "block_zs",
            lambda:
                tdx_local_blocks(
                    tdx_dir
                ),
        )

    else:

        skip(
            "tdx_local",
            "all",
            (
                "未提供 --tdx-dir，"
                "因此未读取本地 vipdoc"
            ),
        )

    # ========================================================
    # 汇总
    # ========================================================

    summary = pd.DataFrame(
        RESULTS
    )

    summary.to_csv(
        OUT / "summary.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print()
    print("=" * 100)
    print("SUMMARY")

    show_cols = [
        "provider",
        "test",
        "status",
        "elapsed_sec",
        "rows",
        "start",
        "end",
        "error",
    ]

    print(
        summary[
            show_cols
        ].to_string(
            index=False
        )
    )

    print()
    print(
        "结果目录:",
        OUT.resolve(),
    )

    print(
        "汇总文件:",
        OUT / "summary.csv",
    )


if __name__ == "__main__":
    main()