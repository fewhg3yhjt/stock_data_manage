from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence
import math
import requests

from ..contracts import EndpointContract, FailureClass, ProviderContractError
from .realtime import EastMoneyRequestsTransport


@dataclass(frozen=True, slots=True)
class FinancialMainRecord:
    symbol: str
    report_date: str
    notice_date: str | None
    report_type: str | None
    currency: str | None
    eps: float | None
    bps: float | None
    operating_revenue: float | None
    parent_net_profit: float | None
    roe: float | None
    operating_cash_flow: float | None


@dataclass(frozen=True, slots=True)
class FinancialMainFetchResult:
    records: tuple[FinancialMainRecord, ...]
    requested_symbols: tuple[str, ...]
    response_status: int


@dataclass(slots=True)
class EastMoneyFinancialMainProvider:
    transport: Any | None = None
    endpoint: str = "financial_main"
    name: str = "eastmoney"
    capability_version: str = "eastmoney-financial-main-v1"
    timeout_seconds: float = 20.0
    page_size: int = 8
    url: str = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    _event_session: Any = field(default=None, init=False, repr=False)
    _event_last_call: float = field(default=0.0, init=False, repr=False)
    _event_total: int | None = field(default=None, init=False, repr=False)
    _event_pages: int | None = field(default=None, init=False, repr=False)

    def close_event_session(self):
        if self._event_session is not None:
            self._event_session.close()
            self._event_session = None

    def _event_get(self, url, *, params, timeout):
        # Preserve the successful runnable V3.9 loader's Session, proxy and retry policy.
        import random
        import time
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        if self._event_session is None:
            self._event_session = requests.Session()
            adapter = HTTPAdapter(max_retries=Retry(total=3, connect=3, backoff_factor=0.6,
                status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]))
            self._event_session.mount("https://", adapter)
            self._event_session.mount("http://", adapter)
        wait = 1.0 - (time.time() - self._event_last_call)
        if wait > 0:
            time.sleep(wait + random.uniform(0.1, 0.5))
        try:
            return self._event_session.get(url, params=params, headers=None, timeout=timeout)
        finally:
            self._event_last_call = time.time()

    def fetch_event_list(self, *, code=None, report_date=None, start=None, end=None, detail=False, limit=100):
        """Bounded source events, retaining original fields; YAML owns canonical mapping."""
        import json
        import re
        from ..contracts import InputFetchResult
        from .realtime import history_stock_identity
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 5000:
            raise ValueError("limit must be an integer in 1..5000")
        if self.endpoint not in {"earnings_forecast", "institution_survey"}:
            raise ValueError("event input endpoint is not implemented")
        if detail is not False:
            raise ValueError("only the evidence-backed survey summary is supported")
        bare = history_stock_identity(code)[0] if code is not None else None
        equal = {"SECURITY_CODE": bare} if bare else {}
        dates = {}
        if self.endpoint == "earnings_forecast":
            if start is not None or end is not None:
                raise ValueError("forecast does not support notice window")
            period = _event_day(report_date) if report_date else None
            extra = f"(REPORT_DATE='{period}')" if period else ""
            filter_str = extra + (f'(SECURITY_CODE="{bare}")' if bare else "")
            if period:
                dates["REPORT_DATE"] = (period, period)
            report = "RPT_PUBLIC_OP_NEWPREDICT"
            sort, order = "NOTICE_DATE,SECURITY_CODE,REPORT_DATE,PREDICT_FINANCE_CODE", "-1,1,-1,1"
            narrowed = bool(bare or period)
        else:
            if report_date is not None:
                raise ValueError("survey does not support report period")
            lo, hi = _event_day(start) if start else None, _event_day(end) if end else None
            if lo and hi and lo > hi:
                raise ValueError("start must not follow end")
            filter_str = '(IS_SOURCE="1")(NUMBERNEW="1")' + (f'(SECURITY_CODE="{bare}")' if bare else "")
            if lo:
                filter_str += f"(NOTICE_DATE>='{lo}')"
            if hi:
                filter_str += f"(NOTICE_DATE<='{hi}')"
            equal.update(IS_SOURCE="1", NUMBERNEW="1")
            if lo or hi:
                dates["NOTICE_DATE"] = (lo, hi)
            report = "RPT_ORG_SURVEYNEW"
            sort, order = "NOTICE_DATE,SECURITY_CODE,RECEIVE_START_DATE", "-1,1,-1"
            narrowed = bool(bare or lo or hi)
        self._event_total = self._event_pages = None
        rows = self._event_rows(report, filter_str, sort, order, page_size=min(limit, 500), max_rows=limit)
        if not rows and not narrowed:
            raise RuntimeError("unfiltered event list is empty; source availability is unverified")
        if len({json.dumps(row, sort_keys=True, ensure_ascii=False) for row in rows}) != len(rows):
            raise RuntimeError("event pages returned duplicate source rows")
        for row in rows:
            if not re.fullmatch(r"[0-9]{6}", str(row.get("SECURITY_CODE", ""))):
                raise RuntimeError("event source security code changed")
            for name, value in equal.items():
                if row.get(name) != value:
                    raise RuntimeError("event response ignored requested equality filter")
            for name, (lo, hi) in dates.items():
                day = _event_day(row.get(name))
                if day is None or (lo and day < lo) or (hi and day > hi):
                    raise RuntimeError("event response ignored requested date filter")
            # Preserve the original parser's validity checks even when units remain pending.
            numeric = ("PREDICT_AMT_LOWER", "PREDICT_AMT_UPPER", "ADD_AMP_LOWER", "ADD_AMP_UPPER", "PREYEAR_SAME_PERIOD") if self.endpoint == "earnings_forecast" else ("SUM",)
            for name in numeric:
                _event_num(row.get(name))
            date_fields = ("NOTICE_DATE", "REPORT_DATE") if self.endpoint == "earnings_forecast" else ("NOTICE_DATE", "RECEIVE_START_DATE", "RECEIVE_END_DATE")
            for name in date_fields:
                _event_day(row.get(name))
        return InputFetchResult(tuple(rows), source_url=self.url + "?reportName=" + report,
            empty_is_valid=not rows and narrowed, mapping_context={"source_total_count": self._event_total,
                "source_page_count": self._event_pages, "requested_limit": limit, "source_report": report})

    def fetch_lpr_history(self):
        """Preserve the source script's four-page history and baseline-row exclusion."""
        from ..contracts import InputFetchResult
        self._event_total = self._event_pages = None
        source = self._event_rows("RPTA_WEB_RATE", sort_columns="TRADE_DATE", sort_types="1",
                                  columns="TRADE_DATE,LPR1Y,LPR5Y")
        selected = [row for row in source if row.get("LPR1Y") is not None]
        excluded = [row for row in source if row.get("LPR1Y") is None]
        if not selected:
            raise RuntimeError("LPR report contains no LPR data")
        for row in selected:
            if _event_num(row["LPR1Y"]) is None or _event_day(row.get("TRADE_DATE")) is None:
                raise RuntimeError("required LPR date or one-year value is missing")
            _event_num(row.get("LPR5Y"))
        return InputFetchResult(tuple(selected), source_url=self.url+"?reportName=RPTA_WEB_RATE",
            source_rows=tuple(source), excluded_rows=tuple(excluded), mapping_context={
                "source_total_count":self._event_total,"source_page_count":self._event_pages,
                "requested_limit":5000,"source_report":"RPTA_WEB_RATE"})

    def fetch_convertible_bonds(self, *, include_delisted=False, replay_reference_date=None):
        """Original CB query/status contract; replay clock never becomes a source query."""
        from datetime import datetime, timedelta, timezone
        from ..contracts import InputFetchResult
        if type(include_delisted) is not bool:
            raise ValueError("include_delisted must be boolean")
        quotes = ("f2~01~CONVERT_STOCK_CODE~CONVERT_STOCK_PRICE,f235~10~SECURITY_CODE~TRANSFER_PRICE,"
                  "f236~10~SECURITY_CODE~TRANSFER_VALUE,f2~10~SECURITY_CODE~CURRENT_BOND_PRICE,"
                  "f237~10~SECURITY_CODE~TRANSFER_PREMIUM_RATIO")
        self._event_total = self._event_pages = None
        source = self._event_rows("RPT_BOND_CB_LIST", "", "PUBLIC_START_DATE,SECURITY_CODE", "-1,1",
            page_size=500, max_rows=20000, extra={"quoteColumns": quotes, "quoteType": "0"})
        if not source:
            raise RuntimeError("CB source list is empty")
        reference_day = replay_reference_date() if callable(replay_reference_date) else replay_reference_date
        today = (reference_day or datetime.now(timezone(timedelta(hours=8))).date()).isoformat()
        out, excluded = [], []
        for row in source:
            listing, delist = _event_day(row.get("LISTING_DATE")), _event_day(row.get("DELIST_DATE"))
            market = row.get("TRADE_MARKET")
            if delist and delist <= today or market == "STAS00":
                status = "delisted"
            elif market not in ("CNSESH", "CNSESZ"):
                status = "unknown"
            elif listing and listing <= today:
                status = "listed"
            elif listing or row.get("PUBLIC_START_DATE"):
                status = "upcoming"
            else:
                status = "unknown"
            if status == "delisted" and not include_delisted:
                excluded.append(row)
                continue
            if "SECURITY_CODE" not in row:
                raise RuntimeError("CB source security code is missing")
            item = {"code":row["SECURITY_CODE"], "name":row.get("SECURITY_NAME_ABBR"), "status":status,
                "stock_code":row.get("CONVERT_STOCK_CODE"), "stock_name":row.get("SECURITY_SHORT_NAME"),
                "rating":row.get("RATING"), "apply_code":row.get("CORRECODE"), "listing_date":listing, "delist_date":delist}
            for name, source_name in {"apply_date":"PUBLIC_START_DATE", "expire_date":"EXPIRE_DATE", "convert_start":"TRANSFER_START_DATE"}.items():
                item[name] = _event_day(row.get(source_name))
            for name, source_name in {"issue_size_100m":"ACTUAL_ISSUE_SCALE", "initial_convert_price":"INITIAL_TRANSFER_PRICE",
                "convert_price":"TRANSFER_PRICE", "bond_price":"CURRENT_BOND_PRICE", "stock_price":"CONVERT_STOCK_PRICE",
                "convert_value":"TRANSFER_VALUE", "premium_pct":"TRANSFER_PREMIUM_RATIO"}.items():
                item[name] = _event_num(row.get(source_name))
            out.append(item)
        if len({item["code"] for item in out}) != len(out):
            raise RuntimeError("CB source contains duplicate selected codes")
        return InputFetchResult(tuple(out), source_rows=tuple(source), excluded_rows=tuple(excluded), empty_is_valid=not out,
            source_url=self.url+"?reportName=RPT_BOND_CB_LIST", mapping_context={"reference_date":today,
                "source_total_count":self._event_total,"source_page_count":self._event_pages,"requested_limit":20000})

    def fetch_action_list(self, *, code=None, start=None, end=None, direction=None, progress=None, date=None, limit=100):
        """Source financial actions, preserving the successful V3.9 query contracts."""
        import json
        import re
        from ..contracts import InputFetchResult
        from .realtime import history_stock_identity
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 5000:
            raise ValueError("limit must be an integer in 1..5000")
        bare, market = history_stock_identity(code)[:2] if code is not None else (None, None)
        equal = {}
        dates = {}
        code_field = "DIM_SCODE" if self.endpoint == "buyback" else "SECURITY_CODE"
        if bare:
            equal[code_field] = bare
        self._event_total = self._event_pages = None
        if self.endpoint == "holder_trades":
            if progress is not None or date is not None:
                raise ValueError("holder trades do not support progress or statistic date")
            if direction not in {None, "增持", "减持"}:
                raise ValueError("direction must be 增持 or 减持")
            if direction:
                equal["DIRECTION"] = direction
            lo, hi = _event_day(start) if start else None, _event_day(end) if end else None
            if lo and hi and lo > hi:
                raise ValueError("start must not follow end")
            filter_str = (f'(DIRECTION="{direction}")' if direction else "") + (f'(SECURITY_CODE="{bare}")' if bare else "")
            if lo:
                filter_str += f"(NOTICE_DATE>='{lo}')"
            if hi:
                filter_str += f"(NOTICE_DATE<='{hi}')"
            if lo or hi:
                dates["NOTICE_DATE"] = (lo, hi)
            report, sort, order = "RPT_SHARE_HOLDER_INCREASE", "NOTICE_DATE,SECURITY_CODE,HOLDER_NAME,START_DATE,END_DATE", "-1,1,1,1,1"
            narrowed = bool(bare or lo or hi)
            numeric = ("CHANGE_NUM_SYMBOL", "AFTER_CHANGE_RATE", "CHANGE_FREE_RATIO", "AFTER_HOLDER_NUM", "HOLD_RATIO", "FREE_SHARES", "FREE_SHARES_RATIO", "TRADE_AVERAGE_PRICE")
            date_fields = ("NOTICE_DATE", "START_DATE", "END_DATE")
        elif self.endpoint == "buyback":
            if start is not None or end is not None or direction is not None or date is not None:
                raise ValueError("buyback does not support a date window or direction")
            progress_codes = {"董事会预案":"001", "股东大会通过":"002", "股东大会否决":"003", "实施中":"004", "停止实施":"005", "完成实施":"006"}
            if progress is not None:
                if progress not in progress_codes:
                    raise ValueError("unsupported buyback progress")
                equal["REPURPROGRESS"] = progress_codes[progress]
            filter_str = "".join(f'({field}="{value}")' for field, value in equal.items())
            report, sort, order = "RPTA_WEB_GETHGLIST_NEW", "UPD,DIM_SCODE,REPURCODE", "-1,1,1"
            narrowed = bool(equal)
            numeric = ("REPURPRICECAP", "REPURNUMLOWER", "REPURNUMCAP", "REPURAMOUNTLOWER", "REPURAMOUNTLIMIT", "ZSZXX", "ZSZSX", "REPURNUM", "REPURAMOUNT", "REPURPRICELOWER1", "REPURPRICECAP1")
            date_fields = ("REPURSTARTDATE", "REPURENDDATE", "UPDATEDATE")
        elif self.endpoint == "pledge":
            if start is not None or end is not None or direction is not None or progress is not None:
                raise ValueError("pledge accepts only a stock or a statistic date")
            if code is not None and date is not None:
                raise ValueError("pledge code and date cannot be supplied together")
            if market == "bj":
                raise ValueError("pledge source covers only SH/SZ stocks")
            report = "RPT_CSDC_LIST"
            if bare:
                filter_str = f'(SECURITY_CODE="{bare}")'
                sort, order = "TRADE_DATE", "-1"
            else:
                if date is None:
                    latest = self._event_rows(report, "", "TRADE_DATE", "-1", page_size=1, max_rows=1)
                    if not latest:
                        raise RuntimeError("pledge latest statistic date is missing")
                    date = latest[0].get("TRADE_DATE")
                    if not date:
                        raise RuntimeError("pledge source statistic date field changed")
                day = _event_day(date)
                if day is None:
                    raise ValueError("pledge statistic date is required")
                filter_str = f"(TRADE_DATE='{day}')"
                sort, order = "PLEDGE_RATIO,SECURITY_CODE", "-1,1"
                dates["TRADE_DATE"] = (day, day)
            narrowed = True
            numeric = ("PLEDGE_RATIO", "REPURCHASE_BALANCE", "PLEDGE_MARKET_CAP", "PLEDGE_DEAL_NUM", "REPURCHASE_UNLIMITED_BALANCE", "REPURCHASE_LIMITED_BALANCE")
            date_fields = ("TRADE_DATE",)
        elif self.endpoint == "ipo_calendar":
            if any(value is not None for value in (code, start, end, direction, progress, date)):
                raise ValueError("IPO calendar accepts only limit")
            report, filter_str, sort, order = "RPTA_APP_IPOAPPLY", "", "APPLY_DATE,SECURITY_CODE", "-1,-1"
            narrowed = False
            numeric = ("ISSUE_PRICE", "AFTER_ISSUE_PE", "INDUSTRY_PE", "ISSUE_NUM", "ONLINE_ISSUE_NUM", "ONLINE_APPLY_UPPER", "TOP_APPLY_MARKETCAP", "ONLINE_ISSUE_LWR", "CLOSE_PRICE", "LD_CLOSE_CHANGE")
            date_fields = ("APPLY_DATE", "BALLOT_NUM_DATE", "BALLOT_PAY_DATE", "LISTING_DATE")
        else:
            raise ValueError("financial action endpoint is not implemented")
        rows = self._event_rows(report, filter_str, sort, order, page_size=min(limit, 500), max_rows=limit)
        if not rows and self.endpoint == "pledge" and bare is None:
            raise ValueError("requested date is not a published pledge statistic date")
        if not rows and not narrowed:
            raise RuntimeError("unfiltered source action list is empty")
        if len({json.dumps(row, sort_keys=True, ensure_ascii=False) for row in rows}) != len(rows):
            raise RuntimeError("action pages returned duplicate source rows")
        for row in rows:
            if not re.fullmatch(r"[0-9]{6}", str(row.get(code_field, ""))):
                raise RuntimeError("action source security code changed")
            for name, value in equal.items():
                if row.get(name) != value:
                    raise RuntimeError("action response ignored requested equality filter")
            for name, (lo, hi) in dates.items():
                day = _event_day(row.get(name))
                if day is None or lo and day < lo or hi and day > hi:
                    raise RuntimeError("action response ignored requested date filter")
            for name in numeric:
                _event_num(row.get(name))
            for name in date_fields:
                _event_day(row.get(name))
            if self.endpoint == "holder_trades":
                signed = _event_num(row.get("CHANGE_NUM_SYMBOL"))
                if signed is not None and row.get("DIRECTION") in {"增持", "减持"} and (signed < 0) != (row["DIRECTION"] == "减持"):
                    raise RuntimeError("holder direction and signed shares disagree")
            if self.endpoint == "pledge":
                total, free, locked = (_event_num(row.get(name)) for name in ("REPURCHASE_BALANCE", "REPURCHASE_UNLIMITED_BALANCE", "REPURCHASE_LIMITED_BALANCE"))
                if None not in (total, free, locked) and abs(free + locked - total) > max(1.0, total * 0.001):
                    raise RuntimeError("pledge free and restricted shares do not sum to total")
        return InputFetchResult(tuple(rows), source_url=self.url + "?reportName=" + report,
            empty_is_valid=not rows and narrowed, mapping_context={"source_total_count":self._event_total,
                "source_page_count":self._event_pages, "requested_limit":limit, "source_report":report,
                "returned_date_field":{"holder_trades":"NOTICE_DATE","buyback":"UPDATEDATE","pledge":"TRADE_DATE","ipo_calendar":"APPLY_DATE"}[self.endpoint]})

    def _event_rows(self, report_name, filter_str="", sort_columns="", sort_types="",
                              page_size=500, max_rows=5000, columns="ALL", extra=None):
        """东财 datacenter 严格版：code=0 取数据；第 1 页就 9201(返回数据为空) → []；其他错误码直接抛。

        与旧 eastmoney_datacenter() 的区别：后者把任何失败都变成 []，调用方分不清
        「这只票确实没有」和「参数写错/被风控」。sortTypes 个数必须与 sortColumns 一致，
        否则东财返回 9501「排序字段和顺序数量不一致」。
        翻页中途失败（第 2 页起 9201、空页、非末页不满页、缺 pages / count、总页数或总条数变了、
        最终条数与 count 不符）抛 RuntimeError，不把部分结果当完整结果返回。
        payload / result 不是对象、data 不是由对象组成的列表，同样抛 RuntimeError。
        只有「第 1 页、pages=1、data 为空」才算确实没有数据。
        max_rows 只在来源自报总数 count > max_rows 时提前截断；count 不超过上限的，一律走完分页并核对总数。
        """
        n_cols = len([c for c in sort_columns.split(",") if c]) if sort_columns else 0
        n_types = len([t for t in sort_types.split(",") if t]) if sort_types else 0
        if n_cols != n_types:
            raise ValueError(f"sortColumns({n_cols}) 与 sortTypes({n_types}) 个数不一致")
        rows, page, first = [], 1, None
        while True:
            params = {"reportName": report_name, "columns": columns, "filter": filter_str,
                      "pageNumber": str(page), "pageSize": str(page_size),
                      "sortColumns": sort_columns, "sortTypes": sort_types,
                      "source": "WEB", "client": "WEB"}
            params.update(extra or {})
            try:
                response = self._event_get(self.url, params=params, timeout=20)
                response.raise_for_status()
            except requests.RequestException as exc:
                raise RuntimeError(f"东财 {report_name} 请求失败: {type(exc).__name__}: {exc}") from exc
            payload = _event_json(response)
            if not isinstance(payload, dict):
                raise RuntimeError(f"东财 {report_name} 返回的不是 JSON 对象: {str(payload)[:100]}")
            if payload.get("code") == 9201:
                if page == 1:
                    return []
                raise RuntimeError(f"东财 {report_name} 第 {page} 页返回「数据为空」，"
                                   f"前面已取 {len(rows)} 条，结果不完整")
            if payload.get("code") != 0 or not payload.get("result"):
                raise RuntimeError(f"东财 {report_name} 返回错误: "
                                   f"{payload.get('code')} {payload.get('message')}")
            result = payload["result"]
            if not isinstance(result, dict):
                raise RuntimeError(f"东财 {report_name} 的 result 不是对象: {str(result)[:100]}")
            pages, count, data = result.get("pages"), result.get("count"), result.get("data")
            if data is None:
                data = []
            if not isinstance(data, list) or not all(isinstance(r, dict) for r in data):
                raise RuntimeError(f"东财 {report_name} 第 {page} 页的 data 不是由对象组成的列表，格式可能已变")
            if (any(isinstance(v, bool) or not isinstance(v, int) for v in (pages, count))
                    or pages < 1 or count < 0):
                raise RuntimeError(f"东财 {report_name} 缺少分页信息（pages={pages!r}, count={count!r}）")
            if first is None:
                first = (pages, count)
                self._event_pages, self._event_total = pages, count
            elif (pages, count) != first:
                raise RuntimeError(f"东财 {report_name} 翻页时总页数 / 总条数从 {first} 变成 {(pages, count)}，"
                                   "结果可能错位，请重试")
            if not data and (page > 1 or pages > 1):
                raise RuntimeError(f"东财 {report_name} 第 {page}/{pages} 页是空的，结果不完整")
            if page < pages and len(data) != int(page_size):
                raise RuntimeError(f"东财 {report_name} 第 {page}/{pages} 页只有 {len(data)} 条"
                                   f"（非末页应为 {page_size} 条），结果不完整")
            rows.extend(data)
            # 只有来源自报的总数确实超过上限才提前截断；否则（count <= max_rows 却已拿到更多行）
            # 必须走完分页并核对总数，不然「data 比 count 还多」这种格式异常会被当成正常截断放过
            if len(rows) >= max_rows and count > max_rows:
                return rows[:max_rows]
            if page >= pages:
                if len(rows) != count:
                    raise RuntimeError(f"东财 {report_name} 翻页后 {len(rows)} 条，与总数 {count} 不符")
                return rows
            page += 1

    def __post_init__(self) -> None:
        if self.transport is None:
            self.transport = EastMoneyRequestsTransport()

    def fetch(self, symbols: Sequence[str]) -> FinancialMainFetchResult:
        requested = tuple(symbols)
        records: list[FinancialMainRecord] = []
        status = 200
        for symbol in requested:
            secucode = _secucode(symbol)
            response = self.transport.get(
                self.url,
                params={
                    "reportName": "RPT_F10_FINANCE_MAINFINADATA",
                    "columns": "ALL",
                    "filter": f'(SECUCODE="{secucode}")',
                    "pageNumber": "1",
                    "pageSize": str(self.page_size),
                    "sortColumns": "REPORT_DATE",
                    "sortTypes": "-1",
                    "source": "HSF10",
                    "client": "PC",
                },
                timeout_seconds=self.timeout_seconds,
            )
            status = response.status_code
            payload = EndpointContract(frozenset()).parse_json(response)
            rows = _rows(payload)
            for row in rows:
                records.append(FinancialMainRecord(
                    symbol=symbol,
                    report_date=_text(row, "REPORT_DATE") or "",
                    notice_date=_text(row, "NOTICE_DATE"),
                    report_type=_text(row, "REPORT_TYPE"),
                    currency=_text(row, "CURRENCY"),
                    eps=_number(row, "EPSJB"), bps=_number(row, "BPS"),
                    operating_revenue=_number(row, "TOTALOPERATEREVE"),
                    parent_net_profit=_number(row, "PARENTNETPROFIT"),
                    roe=_number(row, "ROEJQ"), operating_cash_flow=_number(row, "MGJYXJJE"),
                ))
        return FinancialMainFetchResult(tuple(records), requested, status)


def _rows(payload: object) -> list[Mapping[str, Any]]:
    if not isinstance(payload, dict):
        raise ProviderContractError("EastMoney financial response envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
    result = payload.get("result")
    rows = result.get("data") if isinstance(result, dict) else None
    if not isinstance(rows, list):
        raise ProviderContractError("EastMoney financial rows changed", FailureClass.SCHEMA_CHANGED, retryable=False)
    return [row for row in rows if isinstance(row, dict)]


def _secucode(symbol: str) -> str:
    text = str(symbol).lower()
    if len(text) < 8 or text[:2] not in {"sh", "sz", "bj"}:
        raise ValueError(f"EastMoney requires exchange-prefixed symbol: {symbol}")
    return f"{text[2:]}.{text[:2].upper()}"


def _text(row: Mapping[str, Any], name: str) -> str | None:
    value = row.get(name)
    return None if value in (None, "") else str(value)


def _number(row: Mapping[str, Any], name: str) -> float | None:
    value = row.get(name)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _event_num(value):
    """'1,234.50' → 1234.5；空串 / '-' / '--' / None → None；其他非数字抛 RuntimeError
    （来源给了认不出的值是「源的格式变了」，不能和参数错误的 ValueError 混在一起）。
    JSON 布尔值同样抛错：float(True)=1.0 会把格式错误静默写成价格 / 成交量。"""
    if value is None:
        return None
    if isinstance(value, bool):
        raise RuntimeError(f"来源在数值字段给了布尔值 {value!r}")
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return float(value)
    text = str(value).replace(",", "").strip()
    if text in ("", "-", "--", "None", "null"):
        return None
    try:
        number = float(text)
    except ValueError as exc:
        raise RuntimeError(f"来源返回了无法识别的数值 {value!r}") from exc
    return number if math.isfinite(number) else None

def _event_json(response):
    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError("event source returned non-JSON data") from exc


def _event_day(value):
    if value is None or value == "":
        return None
    from datetime import datetime
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise RuntimeError("event source date format changed") from exc
