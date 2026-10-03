# -*- coding: utf-8 -*-
"""数据源适配器 — 同花顺资金流（行业/概念/个股 × 即时/多日排行）

同花顺口径说明:
    - 即时表: 流入/流出/净额 多带「亿/万」后缀；净额极小的票返回**纯数字=元**
      （这是同花顺接口的坑，必须按 context 解析）
    - 多日排行表(3日/5日/10日/20日): 全部为**纯数字=亿**，列名与即时表不一致
      （即时「行业-涨跌幅」→ 多日「阶段涨跌幅」）

统一列约定:
    - 板块类(行业/概念): name / index / chg / in / out / net / count / leader / leader_chg
    - 个股类: code / name / price / chg / turnover / in / out / net / amount
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# 同花顺排行周期 → akshare symbol 参数
PERIODS = {"now": "即时", "3d": "3日排行", "5d": "5日排行", "10d": "10日排行", "20d": "20日排行"}

SECTOR_COLS = {  # 即时板块表
    "行业": "name", "行业指数": "index", "行业-涨跌幅": "chg",
    "流入资金": "in", "流出资金": "out", "净额": "net",
    "公司家数": "count", "领涨股": "leader", "领涨股-涨跌幅": "leader_chg",
}
SECTOR_COLS_DAYS = {  # 多日板块表
    "行业": "name", "行业指数": "index", "阶段涨跌幅": "chg",
    "流入资金": "in", "流出资金": "out", "净额": "net", "公司家数": "count",
}
STOCK_COLS = {
    "股票代码": "code", "股票简称": "name", "最新价": "price", "涨跌幅": "chg",
    "换手率": "turnover", "流入资金": "in", "流出资金": "out",
    "净额": "net", "成交额": "amount",
}
STOCK_COLS_DAYS = {  # 多日个股排行（列名与即时不一致）
    "股票代码": "code", "股票简称": "name", "最新价": "price",
    "阶段涨跌幅": "chg_days", "连续换手率": "turnover_days",
    "资金流入净额": "net_days",
}


def tonum(value, default_unit="亿"):
    """把同花顺金额字符串转成「亿」为单位的 float。

    Parameters
    ----------
    default_unit : str
        无后缀纯数字时的单位: 即时表净额是「元」，多日表是「亿」。
    """
    s = str(value).replace(",", "").strip()
    if not s or s in ("-", "--"):
        return None
    if s.endswith("亿"):
        return float(s[:-1])
    if s.endswith("万"):
        return float(s[:-1]) / 1e4
    try:
        v = float(s)
    except ValueError:
        return None
    if default_unit == "元":
        return v / 1e8
    return v  # 亿


def _to_pct(s):
    return float(str(s).replace("%", "")) if str(s).replace("%", "").replace(".", "").strip() else None


def _standardize_sector(df: pd.DataFrame, is_days: bool) -> pd.DataFrame:
    mapping = SECTOR_COLS_DAYS if is_days else SECTOR_COLS
    keep = {k: v for k, v in mapping.items() if k in df.columns}
    out = df[list(keep)].rename(columns=keep)
    # 板块表(行业/概念): 无论即时/多日，无后缀数字单位都是「亿」
    # （注意与个股即时表不同——个股即时无后缀=元）
    for c in ("in", "out", "net"):
        if c in out.columns:
            out[c] = out[c].map(lambda s: tonum(s, "亿"))
    if "chg" in out.columns:
        out["chg"] = out["chg"].map(_to_pct)
    return out


def fetch_sector(kind: str, period: str = "now") -> pd.DataFrame:
    """行业(industry)/概念(concept) 资金流排行。

    period: now / 3d / 5d / 10d / 20d
    """
    import akshare as ak

    symbol = PERIODS[period]
    if kind == "industry":
        raw = ak.stock_fund_flow_industry(symbol=symbol)
    elif kind == "concept":
        raw = ak.stock_fund_flow_concept(symbol=symbol)
    else:
        raise ValueError(f"未知板块类型: {kind}")
    is_days = period != "now"
    out = _standardize_sector(raw, is_days)
    out["kind"] = kind
    out["period"] = period
    return out


def _standardize_stock(df: pd.DataFrame, is_days: bool) -> pd.DataFrame:
    mapping = STOCK_COLS_DAYS if is_days else STOCK_COLS
    keep = {k: v for k, v in mapping.items() if k in df.columns}
    out = df[list(keep)].rename(columns=keep)
    out["code"] = out["code"].astype(str).str.zfill(6)
    if is_days:
        # 多日: 资金流入净额带「万/亿」后缀；阶段涨跌幅/连续换手率为 %
        out["net_days"] = out["net_days"].map(lambda s: tonum(s, "亿"))
        for c in ("chg_days", "turnover_days"):
            if c in out.columns:
                out[c] = out[c].map(_to_pct)
    else:
        # 即时表净额无后缀=元；多日表=亿
        for c in ("in", "out", "net", "amount"):
            if c in out.columns:
                out[c] = out[c].map(lambda s: tonum(s, "元"))
        for c in ("chg", "turnover"):
            if c in out.columns:
                out[c] = out[c].map(_to_pct)
    return out


def fetch_stock(period: str = "now") -> pd.DataFrame:
    """全市场个股资金流排行（5000+ 只）。"""
    import akshare as ak

    raw = ak.stock_fund_flow_individual(symbol=PERIODS[period])
    out = _standardize_stock(raw, period != "now")
    out["period"] = period
    return out
