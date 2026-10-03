#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股“未通过能力”补齐采集脚本（单文件版）

目标：
- 对 a-stock-data-capability-results.xlsx 中原先“未通过/未验证”的能力，尽量给出可直接运行的获取方式。
- 已经通过的能力不重复实现。
- 对 2026 年仍存在上游失效、需要凭据或披露规则变化的项目，明确记录为 unavailable/credential_required，
  不伪装成成功。
- 每个能力独立保存 CSV/JSON/TXT/PDF，方便后续自行拆成 provider/collector。

建议环境：
    Python >= 3.10
    pip install -U "akshare>=1.19.1" pandas requests beautifulsoup4 lxml

示例：
    python a_stock_missing_capabilities.py --code 600519 --run all
    python a_stock_missing_capabilities.py --code 600519 --run 15,16,25,26,29,36,39
    python a_stock_missing_capabilities.py --code 000001 --run 38 --start 20250101 --end 20261001
    python a_stock_missing_capabilities.py --run 43,44,45,46 --date 20260930
    python a_stock_missing_capabilities.py --run 65,66,67 --index 000300

注意：
1. 公开网页接口没有稳定 SLA，站点随时可能改字段/风控；脚本做了重试和多源 fallback，但不能保证永久有效。
2. 同花顺/iWencai 对抓取频率更敏感，不建议大并发。
3. 北向资金“实时净买入”披露规则已变化，历史接口和当前实时口径不是一回事。
4. 本脚本默认只拉取一个示例股票，避免误操作造成全市场高频请求。
"""

from __future__ import annotations

import argparse
import inspect
import json
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import akshare as ak
except ImportError:
    ak = None


# ---------------------------------------------------------------------------
# 基础配置
# ---------------------------------------------------------------------------

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/154.0.0.0 Safari/537.36"
)


@dataclass
class Config:
    code: str = "600519"
    start: str = "20250101"
    end: str = datetime.now().strftime("%Y%m%d")
    trade_date: str = datetime.now().strftime("%Y%m%d")
    report_date: str = "20260930"
    index_code: str = "000300"
    output_dir: str = "a_stock_missing_output"
    report_pages: int = 3
    pdf_limit: int = 3
    sleep: float = 0.25
    option_name: str = "华夏上证50ETF期权"


class CapabilityUnavailable(RuntimeError):
    pass


class CredentialRequired(RuntimeError):
    pass


def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
    )
    return s


SESSION = build_session()


def ensure_ak() -> Any:
    if ak is None:
        raise RuntimeError(
            '未安装 AKShare。请先执行: pip install -U "akshare>=1.19.1" pandas requests beautifulsoup4 lxml'
        )
    return ak


def only_digits(code: str) -> str:
    m = re.search(r"(\d{6})", str(code))
    if not m:
        raise ValueError(f"无法识别 A 股代码: {code}")
    return m.group(1)


def market_of(code: str) -> str:
    c = only_digits(code)
    if c.startswith(("4", "8", "92")):
        return "bj"
    if c.startswith(("5", "6", "9")):
        return "sh"
    return "sz"


def sina_symbol(code: str) -> str:
    return f"{market_of(code)}{only_digits(code)}"


def em_market(code: str) -> str:
    m = market_of(code)
    if m == "sh":
        return "沪"
    if m == "sz":
        return "深"
    return "京"


def em_secid(code: str) -> str:
    c = only_digits(code)
    return f"{1 if market_of(c) == 'sh' else 0}.{c}"


def date_dash(d: str) -> str:
    s = re.sub(r"\D", "", d)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def previous_dates(d: str, n: int = 12) -> Iterable[str]:
    dt = datetime.strptime(re.sub(r"\D", "", d), "%Y%m%d")
    for i in range(n):
        yield (dt - timedelta(days=i)).strftime("%Y%m%d")


def ak_function(name: str) -> Callable:
    a = ensure_ak()
    fn = getattr(a, name, None)
    if fn is None:
        raise AttributeError(
            f"当前 AKShare 不包含 {name}；请先升级 AKShare，再检查该接口是否被上游移除。"
        )
    return fn


def call_ak(name: str, **kwargs: Any) -> Any:
    """
    根据当前安装版本的函数签名自动过滤 kwargs，减少 AKShare 版本差异导致的 TypeError。
    """
    fn = ak_function(name)
    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            accepted = kwargs
        else:
            accepted = {k: v for k, v in kwargs.items() if k in params}
    except (TypeError, ValueError):
        accepted = kwargs
    return fn(**accepted)


def try_ak_variants(
    variants: List[Tuple[str, Dict[str, Any]]],
    *,
    require_nonempty: bool = True,
) -> Any:
    errors: List[str] = []
    for name, kwargs in variants:
        try:
            result = call_ak(name, **kwargs)
            if require_nonempty and isinstance(result, pd.DataFrame) and result.empty:
                errors.append(f"{name}: empty")
                continue
            return result
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {e}")
    raise RuntimeError(" | ".join(errors))


def jsonable(obj: Any) -> Any:
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if pd.isna(obj) if not isinstance(obj, (dict, list, tuple, set)) else False:
        return None
    return str(obj)


def save_result(base: Path, result: Any) -> List[str]:
    """
    递归保存：
    - DataFrame -> csv
    - dict -> 若含复杂对象则拆子目录，否则 json
    - list/tuple -> json
    - bytes -> bin
    - str/其他 -> txt/json
    """
    paths: List[str] = []
    base.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(result, pd.DataFrame):
        p = base.with_suffix(".csv")
        result.to_csv(p, index=False, encoding="utf-8-sig")
        paths.append(str(p))
        return paths

    if isinstance(result, pd.Series):
        p = base.with_suffix(".csv")
        result.to_frame().to_csv(p, encoding="utf-8-sig")
        paths.append(str(p))
        return paths

    if isinstance(result, dict):
        has_complex = any(
            isinstance(v, (pd.DataFrame, pd.Series, dict, list, tuple, bytes, bytearray))
            for v in result.values()
        )
        if has_complex:
            folder = base
            folder.mkdir(parents=True, exist_ok=True)
            meta: Dict[str, Any] = {}
            for k, v in result.items():
                safe = re.sub(r'[\\/:*?"<>|]+', "_", str(k))
                if isinstance(v, (pd.DataFrame, pd.Series, dict, list, tuple, bytes, bytearray)):
                    paths.extend(save_result(folder / safe, v))
                else:
                    meta[k] = v
            if meta:
                p = folder / "_meta.json"
                p.write_text(
                    json.dumps(meta, ensure_ascii=False, indent=2, default=jsonable),
                    encoding="utf-8",
                )
                paths.append(str(p))
            return paths

        p = base.with_suffix(".json")
        p.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=jsonable),
            encoding="utf-8",
        )
        paths.append(str(p))
        return paths

    if isinstance(result, (list, tuple)):
        p = base.with_suffix(".json")
        p.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=jsonable),
            encoding="utf-8",
        )
        paths.append(str(p))
        return paths

    if isinstance(result, (bytes, bytearray)):
        p = base.with_suffix(".bin")
        p.write_bytes(bytes(result))
        paths.append(str(p))
        return paths

    p = base.with_suffix(".txt")
    p.write_text(str(result), encoding="utf-8")
    paths.append(str(p))
    return paths


def filter_code(df: pd.DataFrame, code: str) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df
    c = only_digits(code)
    candidates = [
        "代码", "股票代码", "证券代码", "code", "SECURITY_CODE", "股票",
        "股票代码A股", "证券代码A股"
    ]
    for col in candidates:
        if col in df.columns:
            s = df[col].astype(str).str.extract(r"(\d{6})", expand=False)
            out = df[s == c]
            return out if not out.empty else df
    return df


# ---------------------------------------------------------------------------
# 直接网页接口
# ---------------------------------------------------------------------------

def fetch_tencent_finance(cfg: Config) -> pd.DataFrame:
    """
    #1 腾讯财经：实时行情/盘口基本信息。
    这是 qt.gtimg.cn 的公开行情入口，不等同于你已经通过的“腾讯逐笔成交”。
    """
    symbol = sina_symbol(cfg.code)
    url = f"https://qt.gtimg.cn/q={symbol}"
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    r.encoding = "gbk"
    text = r.text.strip()
    if "~" not in text:
        raise RuntimeError(f"腾讯返回异常: {text[:200]}")
    payload = text.split('"', 1)[1].rsplit('"', 1)[0]
    arr = payload.split("~")
    # 腾讯字段很多，先保留 raw + 常用字段；后续拆模块时可按索引进一步映射。
    common = {
        "name": arr[1] if len(arr) > 1 else None,
        "code": arr[2] if len(arr) > 2 else None,
        "price": arr[3] if len(arr) > 3 else None,
        "prev_close": arr[4] if len(arr) > 4 else None,
        "open": arr[5] if len(arr) > 5 else None,
        "volume_lot": arr[6] if len(arr) > 6 else None,
        "datetime": arr[30] if len(arr) > 30 else None,
        "change": arr[31] if len(arr) > 31 else None,
        "change_pct": arr[32] if len(arr) > 32 else None,
        "high": arr[33] if len(arr) > 33 else None,
        "low": arr[34] if len(arr) > 34 else None,
        "raw": payload,
    }
    return pd.DataFrame([common])


def fetch_baidu_kline(cfg: Config) -> pd.DataFrame:
    """
    #5 百度 K 线。
    百度接口有概率按 IP/环境返回 403；代码能直接验证是否仍可用。
    """
    url = "https://finance.pae.baidu.com/selfselect/getstockquotation"
    params = {
        "all": "1",
        "isIndex": "false",
        "isBk": "false",
        "isBlock": "false",
        "isFutures": "false",
        "isStock": "true",
        "newFormat": "1",
        "group": "quotation_kline_ab",
        "finClientType": "pc",
        "code": only_digits(cfg.code),
        "start_time": "",
        "ktype": "1",   # 1 日K；若网页口径变化可调整
    }
    headers = {
        "User-Agent": UA,
        "Accept": "application/vnd.finance-web.v1+json",
        "Referer": "https://gushitong.baidu.com/",
        "Origin": "https://gushitong.baidu.com",
    }
    r = SESSION.get(url, params=params, headers=headers, timeout=25)
    if r.status_code == 403:
        raise CapabilityUnavailable("百度当前对该运行环境返回 403；不是代码解析错误。")
    r.raise_for_status()
    j = r.json()
    result = j.get("Result") or j.get("result") or {}
    md = result.get("newMarketData") or result.get("marketData") or {}
    keys = md.get("keys") or []
    rows = md.get("marketData") or md.get("data") or []
    if isinstance(keys, str):
        keys = [x.strip() for x in keys.split(",")]
    if isinstance(rows, str):
        rows = [x for x in rows.split(";") if x.strip()]
    parsed: List[List[Any]] = []
    for row in rows:
        if isinstance(row, str):
            parsed.append(row.split(","))
        elif isinstance(row, (list, tuple)):
            parsed.append(list(row))
        elif isinstance(row, dict):
            parsed.append(row)
    if parsed and isinstance(parsed[0], dict):
        return pd.DataFrame(parsed)
    if parsed and keys and len(keys) == len(parsed[0]):
        return pd.DataFrame(parsed, columns=keys)
    return pd.DataFrame({"raw": rows})


def fetch_eastmoney_reportapi(cfg: Config) -> pd.DataFrame:
    """
    #8 东财 reportapi：研报列表。
    """
    url = "https://reportapi.eastmoney.com/report/list"
    all_rows: List[dict] = []
    for page in range(1, cfg.report_pages + 1):
        params = {
            "industryCode": "*",
            "pageSize": "100",
            "industry": "*",
            "rating": "*",
            "ratingChange": "*",
            "beginTime": date_dash(cfg.start),
            "endTime": date_dash(cfg.end),
            "pageNo": str(page),
            "fields": "",
            "qType": "0",
            "orgCode": "",
            "code": only_digits(cfg.code),
            "rcode": "",
            "p": str(page),
            "pageNum": str(page),
            "pageNumber": str(page),
            "_": str(int(time.time() * 1000)),
        }
        r = SESSION.get(
            url,
            params=params,
            headers={"User-Agent": UA, "Referer": "https://data.eastmoney.com/"},
            timeout=30,
        )
        r.raise_for_status()
        text = r.text.strip()
        if text.startswith("datatable") and "(" in text:
            text = text[text.find("(") + 1 : text.rfind(")")]
            j = json.loads(text)
        else:
            j = r.json()
        rows = j.get("data") or []
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < 100:
            break
        time.sleep(cfg.sleep)
    return pd.DataFrame(all_rows)


def download_eastmoney_report_pdfs(cfg: Config) -> Dict[str, bytes]:
    """
    #9 东财研报 PDF。
    优先使用研报记录中已有 PDF 链接；否则按 infoCode 构造 dfcfw 地址。
    """
    df = fetch_eastmoney_reportapi(cfg)
    if df.empty:
        raise RuntimeError("没有研报数据，无法下载 PDF")

    out: Dict[str, bytes] = {}
    for _, row in df.head(cfg.pdf_limit).iterrows():
        info_code = (
            row.get("infoCode")
            or row.get("info_code")
            or row.get("INFOCODE")
            or row.get("infoCodeStr")
        )
        title = str(row.get("title") or row.get("TITLE") or info_code or "report")
        url = (
            row.get("pdfUrl")
            or row.get("pdf_url")
            or row.get("attachUrl")
            or (f"https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf" if info_code else None)
        )
        if not url:
            continue
        if str(url).startswith("//"):
            url = "https:" + str(url)
        r = SESSION.get(
            str(url),
            headers={"User-Agent": UA, "Referer": "https://data.eastmoney.com/"},
            timeout=40,
        )
        if r.ok and r.content.startswith(b"%PDF"):
            safe = re.sub(r'[\\/:*?"<>|]+', "_", title)[:80]
            out[f"{safe}.pdf"] = r.content
        time.sleep(cfg.sleep)

    if not out:
        raise RuntimeError("研报列表可获取，但本次未成功下载 PDF")
    return out


# ---------------------------------------------------------------------------
# AKShare / 多源函数
# ---------------------------------------------------------------------------

def fetch_sina_adjust_factor(cfg: Config) -> Dict[str, pd.DataFrame]:
    """#6 新浪前/后复权因子。"""
    symbol = sina_symbol(cfg.code)
    return {
        "qfq_factor": call_ak("stock_zh_a_daily", symbol=symbol, adjust="qfq-factor"),
        "hfq_factor": call_ak("stock_zh_a_daily", symbol=symbol, adjust="hfq-factor"),
    }


def fetch_ths_profit_forecast(cfg: Config) -> pd.DataFrame:
    """#10 同花顺一致预期/盈利预测。"""
    return call_ak("stock_profit_forecast_ths", symbol=only_digits(cfg.code))


def fetch_ths_hotspot(cfg: Config) -> Any:
    """#13 同花顺热点。优先同花顺热点/热榜相关接口。"""
    return try_ak_variants(
        [
            ("stock_wc_hot_top", {}),
            ("stock_hot_rank_wc", {"date": cfg.trade_date}),
            ("stock_board_concept_name_ths", {}),
        ]
    )


def fetch_northbound_realtime(cfg: Config) -> Any:
    """
    #14 同花顺/沪深港通北向实时。
    2024-05 后披露口径变化；先尝试当前公开汇总接口。
    """
    return try_ak_variants(
        [
            ("stock_hsgt_fund_flow_summary_em", {}),
            ("stock_hsgt_fund_min_em", {}),
        ]
    )


def fetch_em_board_membership(cfg: Config) -> Dict[str, Any]:
    """
    #15 东财板块归属：
    个股信息能给行业；概念则遍历概念板块成分，成本较高，所以先尝试热门关键词/相关概念，
    再给出行业 + 概念板块全集，便于后续离线建映射。
    """
    info = call_ak("stock_individual_info_em", symbol=only_digits(cfg.code))
    concept_names = call_ak("stock_board_concept_name_em")
    industry_names = call_ak("stock_board_industry_name_em")

    concept_hits: List[dict] = []
    # 避免默认扫全部几百个概念导致长时间请求；只在 --all-concepts 环境变量开启时做完整命中。
    if os.getenv("FULL_CONCEPT_SCAN", "0") == "1":
        name_col = next(
            (c for c in ["板块名称", "名称", "概念名称"] if c in concept_names.columns),
            None,
        )
        if name_col:
            for name in concept_names[name_col].dropna().astype(str).tolist():
                try:
                    cons = call_ak("stock_board_concept_cons_em", symbol=name)
                    hit = filter_code(cons, cfg.code)
                    # filter_code 找不到代码时会返回原表，因此再做严格判断
                    found = False
                    for col in ["代码", "股票代码", "证券代码"]:
                        if col in cons.columns:
                            found = (
                                cons[col]
                                .astype(str)
                                .str.extract(r"(\d{6})", expand=False)
                                .eq(only_digits(cfg.code))
                                .any()
                            )
                            break
                    if found:
                        concept_hits.append({"concept": name})
                except Exception:
                    pass
                time.sleep(cfg.sleep)

    return {
        "individual_info": info,
        "industry_board_list": industry_names,
        "concept_board_list": concept_names,
        "concept_hits": concept_hits,
    }


def fetch_em_fund_flow(cfg: Config) -> pd.DataFrame:
    """#16 东财个股资金流向。"""
    return call_ak(
        "stock_individual_fund_flow",
        stock=only_digits(cfg.code),
        market=em_market(cfg.code),
    )


def fetch_lhb_seats(cfg: Config) -> Dict[str, Any]:
    """#17 龙虎榜席位：个股上榜日期 + 买/卖营业部。"""
    code = only_digits(cfg.code)
    dates = call_ak("stock_lhb_stock_detail_date_em", symbol=code)
    date_value = cfg.trade_date

    # 优先从返回日期里选最近一个
    if isinstance(dates, pd.DataFrame) and not dates.empty:
        for col in ["交易日", "上榜日", "日期"]:
            if col in dates.columns:
                vals = pd.to_datetime(dates[col], errors="coerce").dropna()
                if not vals.empty:
                    target = pd.Timestamp(datetime.strptime(cfg.trade_date, "%Y%m%d"))
                    vals = vals[vals <= target]
                    if not vals.empty:
                        date_value = vals.max().strftime("%Y%m%d")
                break

    buy = try_ak_variants(
        [
            ("stock_lhb_stock_detail_em", {"symbol": code, "date": date_value, "flag": "买入"}),
            ("stock_lhb_jgmx_sina", {}),
        ],
        require_nonempty=False,
    )
    sell = try_ak_variants(
        [
            ("stock_lhb_stock_detail_em", {"symbol": code, "date": date_value, "flag": "卖出"}),
            ("stock_lhb_yytj_sina", {}),
        ],
        require_nonempty=False,
    )
    return {"dates": dates, "buy": buy, "sell": sell, "chosen_date": date_value}


def fetch_lhb_all(cfg: Config) -> pd.DataFrame:
    """#18 全市场龙虎榜。"""
    return try_ak_variants(
        [
            ("stock_lhb_detail_em", {"start_date": cfg.trade_date, "end_date": cfg.trade_date}),
            ("stock_lhb_detail_daily_sina", {"date": cfg.trade_date}),
        ]
    )


def fetch_restricted_release(cfg: Config) -> pd.DataFrame:
    """#19 限售解禁日历/明细。"""
    return try_ak_variants(
        [
            ("stock_restricted_release_queue_em", {"symbol": only_digits(cfg.code)}),
            (
                "stock_restricted_release_detail_em",
                {"start_date": cfg.start, "end_date": cfg.end},
            ),
        ]
    )


def fetch_industry_rank(cfg: Config) -> pd.DataFrame:
    """#20 行业板块排名。"""
    df = call_ak("stock_board_industry_name_em")
    for col in ["涨跌幅", "涨跌幅%", "涨幅"]:
        if col in df.columns:
            return df.sort_values(col, ascending=False, na_position="last").reset_index(drop=True)
    return df


def fetch_sector_fund_flow(cfg: Config) -> pd.DataFrame:
    """#21 板块资金流向。"""
    return try_ak_variants(
        [
            (
                "stock_sector_fund_flow_rank",
                {"indicator": "今日", "sector_type": "行业资金流"},
            ),
            ("stock_fund_flow_industry", {"symbol": "即时"}),
        ]
    )


def fetch_margin_detail(cfg: Config) -> Dict[str, Any]:
    """
    #22 + #86 两融明细/官方交易所备胎。
    自动向前找最近可返回数据的日期。
    """
    errors = []
    for d in previous_dates(cfg.trade_date, 12):
        try:
            sh = call_ak("stock_margin_detail_sse", date=d)
            sz = call_ak("stock_margin_detail_szse", date=d)
            if (isinstance(sh, pd.DataFrame) and not sh.empty) or (
                isinstance(sz, pd.DataFrame) and not sz.empty
            ):
                return {
                    "date": d,
                    "sse_all": sh,
                    "szse_all": sz,
                    "sse_filtered": filter_code(sh, cfg.code),
                    "szse_filtered": filter_code(sz, cfg.code),
                }
        except Exception as e:
            errors.append(f"{d}:{e}")
    raise RuntimeError("最近 12 个自然日均未取得两融明细: " + " | ".join(errors[-3:]))


def fetch_block_trade(cfg: Config) -> pd.DataFrame:
    """#23 大宗交易。"""
    df = call_ak(
        "stock_dzjy_mrmx",
        symbol="A股",
        start_date=cfg.start,
        end_date=cfg.end,
    )
    return filter_code(df, cfg.code)


def fetch_shareholder_count(cfg: Config) -> pd.DataFrame:
    """#24 股东户数变化。"""
    return call_ak("stock_zh_a_gdhs_detail_em", symbol=only_digits(cfg.code))


def fetch_dividend_history(cfg: Config) -> pd.DataFrame:
    """#25 分红送转历史。"""
    return try_ak_variants(
        [
            ("stock_fhps_detail_em", {"symbol": only_digits(cfg.code)}),
            ("stock_dividend_cninfo", {"symbol": only_digits(cfg.code)}),
        ]
    )


def fetch_fund_flow_120(cfg: Config) -> pd.DataFrame:
    """
    #26 个股资金流近约 100~120 个交易日。
    东财接口实际返回长度由上游控制，不能强行保证正好 120 条。
    """
    return fetch_em_fund_flow(cfg)


def fetch_cyq(cfg: Config) -> pd.DataFrame:
    """#27 筹码分布 CYQ。该接口历史上有过上游字段变化，失败时会明确报错。"""
    return try_ak_variants(
        [
            ("stock_cyq_em", {"symbol": only_digits(cfg.code), "adjust": ""}),
            ("stock_cyq_em", {"symbol": only_digits(cfg.code), "adjust": "qfq"}),
        ]
    )


def fetch_stock_news(cfg: Config) -> pd.DataFrame:
    """#29 个股新闻。"""
    return call_ak("stock_news_em", symbol=only_digits(cfg.code))


def fetch_cls_telegraph(cfg: Config) -> pd.DataFrame:
    """
    #30 财联社电报。
    AKShare 的 CLS 入口曾在 2026 出现 404；保留调用用于当前环境验证。
    """
    return call_ak("stock_info_global_cls")


def fetch_global_news(cfg: Config) -> pd.DataFrame:
    """#31 全球资讯。"""
    return try_ak_variants(
        [
            ("stock_info_global_sina", {}),
            ("stock_info_global_em", {}),
        ]
    )


def fetch_quarter_snapshot(cfg: Config) -> pd.DataFrame:
    """#34 季报/业绩报表快照。"""
    return try_ak_variants(
        [
            ("stock_yjbb_em", {"date": cfg.report_date}),
            ("stock_yjkb_em", {"date": cfg.report_date}),
        ]
    )


def fetch_f10_composite(cfg: Config) -> Dict[str, Any]:
    """
    #35 F10 文本/结构化资料。
    不依赖网页全文 HTML，一次组合个股信息、主营介绍、主营构成、公司资料。
    """
    c = only_digits(cfg.code)
    result: Dict[str, Any] = {}
    funcs = [
        ("individual_info_em", "stock_individual_info_em", {"symbol": c}),
        ("main_business_ths", "stock_zyjs_ths", {"symbol": c}),
        ("business_composition_em", "stock_zygc_em", {"symbol": c}),
        ("profile_cninfo", "stock_profile_cninfo", {"symbol": c}),
        ("company_events_em", "stock_gsrl_gsdt_em", {"symbol": c}),
    ]
    for key, fn, kwargs in funcs:
        try:
            result[key] = call_ak(fn, **kwargs)
        except Exception as e:
            result[key] = {"error": f"{type(e).__name__}: {e}"}
    return result


def fetch_individual_info(cfg: Config) -> pd.DataFrame:
    """#36 东财个股信息。"""
    return call_ak("stock_individual_info_em", symbol=only_digits(cfg.code))


def fetch_sina_financial_statements(cfg: Config) -> Dict[str, pd.DataFrame]:
    """#37 新浪财报三表。"""
    stock = sina_symbol(cfg.code)
    result = {}
    for indicator in ["资产负债表", "利润表", "现金流量表"]:
        result[indicator] = call_ak(
            "stock_financial_report_sina",
            stock=stock,
            indicator=indicator,
            symbol=indicator,  # 老版本曾使用 symbol；call_ak 会按签名自动过滤
        )
    return result


def fetch_cninfo_announcements(cfg: Config) -> pd.DataFrame:
    """#38 巨潮公告。"""
    return call_ak(
        "stock_zh_a_disclosure_report_cninfo",
        symbol=only_digits(cfg.code),
        market="沪深京",
        keyword="",
        category="",
        start_date=cfg.start,
        end_date=cfg.end,
    )


def fetch_valuation_history(cfg: Config) -> Dict[str, pd.DataFrame]:
    """#39 估值历史：总市值/PE/PB/PS。"""
    c = only_digits(cfg.code)
    result: Dict[str, pd.DataFrame] = {}
    for indicator in ["总市值", "市盈率", "市净率", "市销率"]:
        result[indicator] = call_ak(
            "stock_zh_valuation_baidu", symbol=c, indicator=indicator
        )
    return result


def fetch_listing_delisting(cfg: Config) -> Dict[str, Any]:
    """#40 上市/退市日。上市日期来自个股信息；退市表来自沪深交易所接口。"""
    c = only_digits(cfg.code)
    info = call_ak("stock_individual_info_em", symbol=c)
    result: Dict[str, Any] = {"individual_info": info}
    for name in ["stock_info_sh_delist", "stock_info_sz_delist"]:
        try:
            df = call_ak(name)
            result[name] = filter_code(df, c)
        except Exception as e:
            result[name] = {"error": str(e)}
    return result


def fetch_sw_industry_history(cfg: Config) -> Any:
    """
    #41 申万行业变迁史。
    不同 AKShare 版本函数签名不同，先尝试个股代码，再尝试无参数返回全集。
    """
    return try_ak_variants(
        [
            ("stock_industry_clf_hist_sw", {"symbol": only_digits(cfg.code)}),
            ("stock_industry_clf_hist_sw", {}),
        ]
    )


def fetch_limit_up_pool(cfg: Config) -> pd.DataFrame:
    """#43 东财涨停池。"""
    return call_ak("stock_zt_pool_em", date=cfg.trade_date)


def fetch_broken_board_pool(cfg: Config) -> pd.DataFrame:
    """#44 东财炸板池。"""
    return call_ak("stock_zt_pool_zbgc_em", date=cfg.trade_date)


def fetch_limit_down_pool(cfg: Config) -> pd.DataFrame:
    """#45 东财跌停池。"""
    return call_ak("stock_zt_pool_dtgc_em", date=cfg.trade_date)


def fetch_previous_limit_pool(cfg: Config) -> pd.DataFrame:
    """#46 东财昨日涨停池。"""
    return call_ak("stock_zt_pool_previous_em", date=cfg.trade_date)


def fetch_ths_limit_reason(cfg: Config) -> Any:
    """
    #47 同花顺涨停揭秘。
    AKShare 对“同花顺涨停原因”没有长期稳定统一入口；优先尝试当前版本中可能存在的涨停池接口，
    否则返回东财涨停池（含涨停原因）作为可用替代。
    """
    candidates = [
        ("stock_zt_pool_ths", {"date": cfg.trade_date}),
        ("stock_zt_pool_em", {"date": cfg.trade_date}),
    ]
    return try_ak_variants(candidates)


def fetch_strong_pool(cfg: Config) -> pd.DataFrame:
    """
    #48 东财重点监控/强势池。
    表中“重点监控池”若指东财强势股池，可直接使用此接口。
    """
    return call_ak("stock_zt_pool_strong_em", date=cfg.trade_date)


def fetch_intraday_changes(cfg: Config) -> pd.DataFrame:
    """
    #49 东财日内异动池（明细）。
    盘口异动接口通常按异动类型查询；这里遍历常见异动类型并合并。
    """
    change_types = [
        "火箭发射", "快速反弹", "大笔买入", "封涨停板", "打开跌停板",
        "有大买盘", "竞价上涨", "高开5日线", "向上缺口", "60日新高",
        "60日大幅上涨", "加速下跌", "高台跳水", "大笔卖出", "封跌停板",
        "打开涨停板", "有大卖盘", "竞价下跌", "低开5日线", "向下缺口",
        "60日新低", "60日大幅下跌",
    ]
    frames: List[pd.DataFrame] = []
    errors: List[str] = []
    for t in change_types:
        try:
            df = call_ak("stock_changes_em", symbol=t)
            if isinstance(df, pd.DataFrame) and not df.empty:
                df = df.copy()
                if "异动类型" not in df.columns:
                    df["异动类型"] = t
                frames.append(df)
        except Exception as e:
            errors.append(f"{t}:{e}")
        time.sleep(min(cfg.sleep, 0.15))
    if frames:
        return pd.concat(frames, ignore_index=True)
    # 某些版本可能无参数直接返回
    try:
        return call_ak("stock_market_activity_em")
    except Exception:
        raise RuntimeError("stock_changes_em 全部异动类型均失败: " + " | ".join(errors[-5:]))


def fetch_intraday_change_stats(cfg: Config) -> pd.DataFrame:
    """#50 东财日内异动按标的统计，本地基于 #49 明细聚合。"""
    df = fetch_intraday_changes(cfg)
    if df.empty:
        return df
    code_col = next(
        (c for c in ["代码", "股票代码", "证券代码"] if c in df.columns),
        None,
    )
    name_col = next((c for c in ["名称", "股票名称", "证券简称"] if c in df.columns), None)
    if not code_col:
        return df
    group_cols = [code_col] + ([name_col] if name_col else [])
    out = (
        df.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name="异动次数")
        .sort_values("异动次数", ascending=False)
        .reset_index(drop=True)
    )
    return out


def fetch_option_contracts(cfg: Config) -> Any:
    """#51 ETF 期权合约清单。"""
    # 直接拿上交所期权到期月份/合约列表；若版本差异则用 T 型报价反推。
    return try_ak_variants(
        [
            ("option_sse_list_sina", {"symbol": "50ETF", "exchange": "null"}),
            ("option_finance_board", {"symbol": cfg.option_name}),
        ]
    )


def fetch_option_t_board(cfg: Config) -> pd.DataFrame:
    """#52 T 型报价。"""
    # end_month 在不同版本中可选；不硬编码，交给 call_ak 按签名过滤。
    yy_mm = datetime.strptime(cfg.trade_date, "%Y%m%d").strftime("%y%m")
    return try_ak_variants(
        [
            ("option_finance_board", {"symbol": cfg.option_name, "end_month": yy_mm}),
            ("option_finance_board", {"symbol": cfg.option_name}),
        ]
    )


def fetch_option_greeks(cfg: Config) -> Any:
    """
    #53 Greeks + IV。
    优先交易所风险指标；若当前版本不支持 date，则自动过滤参数。
    """
    errors = []
    for d in previous_dates(cfg.trade_date, 12):
        try:
            df = call_ak("option_risk_indicator_sse", date=d)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df
        except Exception as e:
            errors.append(f"{d}:{e}")
    raise RuntimeError("最近日期期权风险指标获取失败: " + " | ".join(errors[-3:]))


def fetch_irm_cninfo(cfg: Config) -> Any:
    """#54 互动易问答。"""
    q = call_ak("stock_irm_cninfo", symbol=only_digits(cfg.code))
    result: Dict[str, Any] = {"questions": q}
    if isinstance(q, pd.DataFrame) and not q.empty:
        id_col = next(
            (
                c
                for c in [
                    "问题ID", "question_id", "questionId", "id",
                ]
                if c in q.columns
            ),
            None,
        )
        if id_col:
            qid = str(q.iloc[0][id_col])
            try:
                result["first_answer"] = call_ak("stock_irm_ans_cninfo", question_id=qid)
            except Exception as e:
                result["first_answer_error"] = str(e)
    return result


def fetch_sse_interaction(cfg: Config) -> pd.DataFrame:
    """#55 上证 e 互动。"""
    return call_ak("stock_sns_sseinfo", symbol=only_digits(cfg.code))


def fetch_ths_hot_rank(cfg: Config) -> Any:
    """#56 同花顺热榜/问财热榜。"""
    return try_ak_variants(
        [
            ("stock_wc_hot_top", {}),
            ("stock_hot_rank_wc", {"date": cfg.trade_date}),
            ("stock_hot_rank_wc", {}),
        ]
    )


def fetch_em_hot_rank(cfg: Config) -> pd.DataFrame:
    """#57 东财人气榜。"""
    return try_ak_variants(
        [
            ("stock_hot_rank_latest_em", {}),
            ("stock_hot_rank_em", {}),
        ]
    )


def fetch_em_stock_concept_hit(cfg: Config) -> Any:
    """
    #58 东财个股概念命中。
    先取个股热门关键词/相关股票；完整概念归属可通过 #15 的 FULL_CONCEPT_SCAN=1 全扫。
    """
    c = only_digits(cfg.code)
    result: Dict[str, Any] = {}
    for name, variants in {
        "hot_keyword": [
            ("stock_hot_keyword_em", {"symbol": c}),
            ("stock_hot_keyword_em", {}),
        ],
        "related": [("stock_hot_related_em", {"symbol": c})],
    }.items():
        try:
            result[name] = try_ak_variants(variants, require_nonempty=False)
        except Exception as e:
            result[name] = {"error": str(e)}
    return result


def fetch_social_financing(cfg: Config) -> pd.DataFrame:
    """#59 人民银行社融。"""
    return call_ak("macro_china_shrzgm")


def fetch_pmi(cfg: Config) -> pd.DataFrame:
    """#60 PMI。"""
    return try_ak_variants(
        [
            ("macro_china_pmi", {}),
            ("macro_china_pmi_yearly", {}),
        ]
    )


def fetch_index_constituents(cfg: Config) -> pd.DataFrame:
    """#65 指数成分。"""
    return try_ak_variants(
        [
            ("index_stock_cons_csindex", {"symbol": cfg.index_code}),
            ("index_stock_cons_sina", {"symbol": cfg.index_code}),
        ]
    )


def fetch_index_weights(cfg: Config) -> pd.DataFrame:
    """#66 指数权重。"""
    return call_ak("index_stock_cons_weight_csindex", symbol=cfg.index_code)


def fetch_index_valuation(cfg: Config) -> Any:
    """#67 指数估值。"""
    return try_ak_variants(
        [
            ("index_value_hist_funddb", {"symbol": cfg.index_code}),
            ("stock_index_pe_lg", {"symbol": cfg.index_code}),
            ("stock_zh_index_value_csindex", {"symbol": cfg.index_code}),
        ]
    )


def fetch_trade_calendar(cfg: Config) -> pd.DataFrame:
    """#68 交易日历。"""
    return call_ak("tool_trade_date_hist_sina")


def fetch_official_lhb_backup(cfg: Config) -> pd.DataFrame:
    """#83 龙虎榜备胎：新浪龙虎榜日榜。"""
    return try_ak_variants(
        [
            ("stock_lhb_detail_daily_sina", {"date": cfg.trade_date}),
            ("stock_lhb_detail_em", {"start_date": cfg.trade_date, "end_date": cfg.trade_date}),
        ]
    )


def fetch_fund_flow_backup(cfg: Config) -> pd.DataFrame:
    """#84 资金流备胎：同花顺资金流。"""
    return call_ak("stock_fund_flow_individual", symbol="即时")


def fetch_notice_backup(cfg: Config) -> pd.DataFrame:
    """#85 公告备胎：东方财富公告。"""
    c = only_digits(cfg.code)
    return try_ak_variants(
        [
            (
                "stock_individual_notice_report",
                {
                    "symbol": c,
                    "security": c,
                    "begin_date": cfg.start,
                    "end_date": cfg.end,
                },
            ),
            ("stock_notice_report", {"symbol": "全部", "date": cfg.trade_date}),
        ]
    )


def fetch_margin_backup(cfg: Config) -> Dict[str, Any]:
    """#86 官方两融备胎：直接复用上交所/深交所明细。"""
    return fetch_margin_detail(cfg)


def fetch_bse_quote_backup(cfg: Config) -> pd.DataFrame:
    """#87 北交所行情备胎。"""
    return call_ak("stock_bj_a_spot_em")


# ---------------------------------------------------------------------------
# 明确不能无条件获取的项目
# ---------------------------------------------------------------------------

def unavailable_mootdx(cfg: Config) -> Any:
    """#7 mootdx 行情：表中已经记录 2026-09 起空数据，不当作可用行情源。"""
    raise CapabilityUnavailable(
        "mootdx/通达信在线行情在你表中已记录 2026-09 起返回空数据；"
        "盘后包能力已通过，因此这里不重复包装成“成功”。"
    )


def unavailable_iwencai_nl(cfg: Config) -> Any:
    """#11 iwencai 自然语言搜索：需要可用凭据/会话。"""
    if not os.getenv("IWENCAI_API_KEY"):
        raise CredentialRequired(
            "未设置 IWENCAI_API_KEY；自然语言问财接口不应在无凭据时伪装成可用。"
        )
    raise CapabilityUnavailable(
        "已检测到 IWENCAI_API_KEY，但脚本不硬编码非公开/易变的私有 endpoint；"
        "建议使用你现有授权 SDK/接口接入。"
    )


# ---------------------------------------------------------------------------
# 能力注册表
# ---------------------------------------------------------------------------

Capability = Tuple[str, Callable[[Config], Any]]

CAPABILITIES: Dict[int, Capability] = {
    1: ("腾讯财经", fetch_tencent_finance),
    5: ("百度K线", fetch_baidu_kline),
    6: ("新浪复权因子", fetch_sina_adjust_factor),
    7: ("mootdx行情", unavailable_mootdx),
    8: ("东财reportapi", fetch_eastmoney_reportapi),
    9: ("东财PDF下载", download_eastmoney_report_pdfs),
    10: ("同花顺一致预期", fetch_ths_profit_forecast),
    11: ("iwencai自然语言搜索", unavailable_iwencai_nl),
    13: ("同花顺热点", fetch_ths_hotspot),
    14: ("北向/沪深港通当前资金汇总", fetch_northbound_realtime),
    15: ("东财板块归属", fetch_em_board_membership),
    16: ("东财个股资金流向", fetch_em_fund_flow),
    17: ("龙虎榜席位", fetch_lhb_seats),
    18: ("全市场龙虎榜", fetch_lhb_all),
    19: ("限售解禁日历", fetch_restricted_release),
    20: ("行业板块排名", fetch_industry_rank),
    21: ("板块资金流向", fetch_sector_fund_flow),
    22: ("融资融券明细", fetch_margin_detail),
    23: ("大宗交易", fetch_block_trade),
    24: ("股东户数变化", fetch_shareholder_count),
    25: ("分红送转历史", fetch_dividend_history),
    26: ("个股资金流近100~120日", fetch_fund_flow_120),
    27: ("筹码分布CYQ", fetch_cyq),
    29: ("个股新闻", fetch_stock_news),
    30: ("财联社电报", fetch_cls_telegraph),
    31: ("全球资讯", fetch_global_news),
    34: ("季报快照", fetch_quarter_snapshot),
    35: ("F10结构化资料", fetch_f10_composite),
    36: ("东财个股信息", fetch_individual_info),
    37: ("新浪财报三表", fetch_sina_financial_statements),
    38: ("巨潮公告", fetch_cninfo_announcements),
    39: ("估值历史", fetch_valuation_history),
    40: ("上市/退市日", fetch_listing_delisting),
    41: ("申万行业变迁史", fetch_sw_industry_history),
    43: ("东财涨停池", fetch_limit_up_pool),
    44: ("东财炸板池", fetch_broken_board_pool),
    45: ("东财跌停池", fetch_limit_down_pool),
    46: ("东财昨日涨停池", fetch_previous_limit_pool),
    47: ("同花顺涨停揭秘/涨停原因替代", fetch_ths_limit_reason),
    48: ("东财强势/重点监控替代池", fetch_strong_pool),
    49: ("东财日内异动池明细", fetch_intraday_changes),
    50: ("东财日内异动按标的统计", fetch_intraday_change_stats),
    51: ("期权合约清单", fetch_option_contracts),
    52: ("期权T型报价", fetch_option_t_board),
    53: ("期权Greeks/风险指标", fetch_option_greeks),
    54: ("互动易问答", fetch_irm_cninfo),
    55: ("上证e互动", fetch_sse_interaction),
    56: ("同花顺/问财热榜", fetch_ths_hot_rank),
    57: ("东财人气榜", fetch_em_hot_rank),
    58: ("东财个股概念/关键词命中", fetch_em_stock_concept_hit),
    59: ("人民银行社融", fetch_social_financing),
    60: ("PMI", fetch_pmi),
    65: ("指数成分", fetch_index_constituents),
    66: ("指数权重", fetch_index_weights),
    67: ("指数估值", fetch_index_valuation),
    68: ("交易日历", fetch_trade_calendar),
    83: ("龙虎榜备胎", fetch_official_lhb_backup),
    84: ("资金流备胎", fetch_fund_flow_backup),
    85: ("公告备胎", fetch_notice_backup),
    86: ("官方两融备胎", fetch_margin_backup),
    87: ("北交所行情备胎", fetch_bse_quote_backup),
}


# ---------------------------------------------------------------------------
# 执行器
# ---------------------------------------------------------------------------

def safe_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", name)


def save_capability_result(out_root: Path, cap_id: int, name: str, result: Any) -> List[str]:
    folder = out_root / f"{cap_id:02d}_{safe_name(name)}"
    folder.mkdir(parents=True, exist_ok=True)

    # PDF 字典特殊处理
    if isinstance(result, dict) and result and all(
        isinstance(k, str) and k.lower().endswith(".pdf") and isinstance(v, (bytes, bytearray))
        for k, v in result.items()
    ):
        paths = []
        for filename, content in result.items():
            p = folder / safe_name(filename)
            p.write_bytes(bytes(content))
            paths.append(str(p))
        return paths

    return save_result(folder / "data", result)


def run_capability(cap_id: int, cfg: Config, out_root: Path) -> Dict[str, Any]:
    name, fn = CAPABILITIES[cap_id]
    print(f"\n[{cap_id:02d}] {name}")
    started = time.time()
    try:
        result = fn(cfg)
        paths = save_capability_result(out_root, cap_id, name, result)
        status = "success"
        message = f"saved {len(paths)} file(s)"
        print(f"  OK  {message}")
        for p in paths[:5]:
            print(f"      {p}")
        if len(paths) > 5:
            print(f"      ... +{len(paths)-5}")
        return {
            "id": cap_id,
            "name": name,
            "status": status,
            "seconds": round(time.time() - started, 3),
            "message": message,
            "files": ";".join(paths),
        }
    except CredentialRequired as e:
        print(f"  CREDENTIAL_REQUIRED  {e}")
        return {
            "id": cap_id,
            "name": name,
            "status": "credential_required",
            "seconds": round(time.time() - started, 3),
            "message": str(e),
            "files": "",
        }
    except CapabilityUnavailable as e:
        print(f"  UNAVAILABLE  {e}")
        return {
            "id": cap_id,
            "name": name,
            "status": "unavailable",
            "seconds": round(time.time() - started, 3),
            "message": str(e),
            "files": "",
        }
    except Exception as e:
        print(f"  FAIL  {type(e).__name__}: {e}")
        if os.getenv("DEBUG", "0") == "1":
            traceback.print_exc()
        return {
            "id": cap_id,
            "name": name,
            "status": "failed",
            "seconds": round(time.time() - started, 3),
            "message": f"{type(e).__name__}: {e}",
            "files": "",
        }


def parse_ids(raw: str) -> List[int]:
    if raw.lower() == "all":
        return sorted(CAPABILITIES)
    ids = []
    for x in raw.split(","):
        x = x.strip()
        if not x:
            continue
        n = int(x)
        if n not in CAPABILITIES:
            raise ValueError(
                f"能力编号 {n} 不在本脚本范围。可用编号: {','.join(map(str, sorted(CAPABILITIES)))}"
            )
        ids.append(n)
    return ids


def main() -> int:
    parser = argparse.ArgumentParser(
        description="补齐 A 股数据能力清单中原未通过项的单文件采集脚本"
    )
    parser.add_argument("--code", default="600519", help="A股代码，默认 600519")
    parser.add_argument("--start", default="20250101", help="开始日期 YYYYMMDD")
    parser.add_argument("--end", default=datetime.now().strftime("%Y%m%d"), help="结束日期 YYYYMMDD")
    parser.add_argument("--date", dest="trade_date", default=datetime.now().strftime("%Y%m%d"), help="交易日 YYYYMMDD")
    parser.add_argument("--report-date", default="20260930", help="季报期 YYYYMMDD")
    parser.add_argument("--index", dest="index_code", default="000300", help="指数代码")
    parser.add_argument("--output", default="a_stock_missing_output", help="输出目录")
    parser.add_argument("--run", default="all", help='执行能力编号，如 "15,16,25" 或 all')
    parser.add_argument("--report-pages", type=int, default=3, help="研报最大分页")
    parser.add_argument("--pdf-limit", type=int, default=3, help="最多下载研报PDF数")
    parser.add_argument("--sleep", type=float, default=0.25, help="分页/批量接口间隔秒")
    parser.add_argument("--option-name", default="华夏上证50ETF期权", help="期权品种名")
    args = parser.parse_args()

    cfg = Config(
        code=only_digits(args.code),
        start=re.sub(r"\D", "", args.start),
        end=re.sub(r"\D", "", args.end),
        trade_date=re.sub(r"\D", "", args.trade_date),
        report_date=re.sub(r"\D", "", args.report_date),
        index_code=str(args.index_code),
        output_dir=args.output,
        report_pages=args.report_pages,
        pdf_limit=args.pdf_limit,
        sleep=args.sleep,
        option_name=args.option_name,
    )

    ids = parse_ids(args.run)
    out_root = Path(cfg.output_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    print("=" * 78)
    print("A股未通过能力补齐采集")
    print(f"code={cfg.code} date={cfg.trade_date} start={cfg.start} end={cfg.end}")
    print(f"run={ids}")
    if ak is not None:
        print(f"akshare={getattr(ak, '__version__', 'unknown')}")
    else:
        print("akshare=NOT_INSTALLED（仅直接 requests 接口可运行）")
    print("=" * 78)

    rows = []
    for cap_id in ids:
        rows.append(run_capability(cap_id, cfg, out_root))
        time.sleep(cfg.sleep)

    summary = pd.DataFrame(rows)
    summary_path = out_root / "_summary.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 78)
    print(summary[["id", "name", "status", "seconds", "message"]].to_string(index=False))
    print(f"\n汇总文件: {summary_path}")
    print("=" * 78)

    # 只要脚本本身执行完就返回 0；具体能力失败看 summary，不阻断批量采集。
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
