"""Official security catalogs using the retained probes' request contracts."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import json
import os
import re
from time import sleep
from typing import Any

import requests

from ..contracts import FailureClass, InputFetchResult, ProviderContractError


@dataclass(slots=True)
class BseSecurityListProvider:
    client: Any = None
    name: str = "bse"
    endpoint: str = "security_snapshot"
    capability_version: str = "bse-security-catalog-v1"
    input_hosts = ("https://www.bse.cn",)
    page_url = "https://www.bse.cn/nq/quotation.html"
    api_url = "https://www.bse.cn/nqhqController/nqhq_en.do"

    def fetch_listing_history(self, *, trade_date, source_responses):
        return fetch_script_listing(self, kind='bj_current', trade_date=trade_date,
                                    source_responses=source_responses)

    def fetch_snapshot(self) -> InputFetchResult:
        session = self.client or requests.Session()
        owned = self.client is None
        if owned:
            session.trust_env = False
            proxy = os.environ.get("STOCK_DATA_HTTP_PROXY", "http://127.0.0.1:20171")
            session.proxies.update({"http": proxy, "https": proxy})
            session.headers.update({"User-Agent": "Mozilla/5.0"})
        session.headers.update({"Referer": self.page_url,
            "Accept": "application/json, text/javascript, */*; q=0.01"})
        records = []
        expected_total = None
        total_pages = None
        try:
            session.get(self.page_url, timeout=20, allow_redirects=False)
            for page in range(100):
                form = {"page": page, "type_en": '["B"]', "sortfield": "hqzqdm",
                        "sorttype": "asc", "xxfcbj_en": "[2]", "zqdm": ""}
                response = session.post(self.api_url, data=form, timeout=30, allow_redirects=False)
                if 300 <= response.status_code < 400:
                    session.get(self.page_url, timeout=20, allow_redirects=False)
                    response = session.post(self.api_url, data=form, timeout=30, allow_redirects=False)
                response.raise_for_status()
                body = response.text.strip()
                match = re.fullmatch(r"[A-Za-z_$][\w$]*\((.*)\);?", body, re.S)
                payload = json.loads(match.group(1) if match else body)
                if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
                    self._invalid("BSE official response envelope changed")
                block = payload[0]
                batch = block.get("content")
                total = block.get("totalElements")
                pages = block.get("totalPages")
                if (not isinstance(batch, list) or not batch or type(total) is not int or total <= 0
                        or type(pages) is not int or pages <= 0 or type(block.get("number")) is not int
                        or block["number"] != page or type(block.get("lastPage")) is not bool
                        or type(block.get("numberOfElements")) is not int or block["numberOfElements"] != len(batch)):
                    self._invalid("BSE page fields, number, or row count changed")
                if expected_total is None:
                    expected_total, total_pages = total, pages
                elif (total, pages) != (expected_total, total_pages):
                    self._invalid("BSE declared total changed during pagination")
                records.extend(batch)
                if len(records) >= expected_total:
                    if len(records) != expected_total or not block["lastPage"] or page + 1 != total_pages:
                        self._invalid("BSE final page contradicts declared total")
                    break
                if block["lastPage"] or page + 1 >= total_pages:
                    self._invalid("BSE pagination ended before declared total")
                sleep(0.25)
            else:
                self._invalid("BSE pagination exceeded the original 100-page limit")
        finally:
            if owned:
                session.close()
        normalized = []
        quote_dates = set()
        for record in records:
            if not isinstance(record, dict):
                self._invalid("BSE security row changed")
            code = str(record.get("hqzqdm", ""))
            name = record.get("hqzqjc")
            if (len(code) != 6 or not code.isdigit() or not code.startswith(("4", "8", "92"))
                    or not isinstance(name, str) or not name.strip()):
                self._invalid("BSE security identity or name changed")
            if record.get("hqjsrq"):
                quote_dates.add(datetime.strptime(str(record["hqjsrq"]), "%Y%m%d").date().isoformat())
            normalized.append({**record, "exchange": "BSE", "asset_type": "stock", "status": "unknown"})
        if len({r["hqzqdm"] for r in normalized}) != len(normalized):
            self._invalid("BSE catalog has duplicate securities")
        return InputFetchResult(tuple(normalized), source_rows=tuple(normalized), source_url=self.api_url,
            mapping_context={"source_total_count": expected_total, "source_page_count": total_pages,
                "pagination_completeness_verified": True, "quote_dates": sorted(quote_dates),
                "status_meaning": "official catalog does not provide verified trading status",
                "snapshot_date_meaning": "response capture day; quotation dates do not date the security catalog",
                "independent_universe_denominator": None})

    @staticmethod
    def _invalid(message):
        raise ProviderContractError(message, FailureClass.SCHEMA_CHANGED, retryable=False)


SCRIPT_LISTING_CALLS = {
    'sh_main_current': ('stock_info_sh_name_code', {'symbol': '主板A股'}, 'XSHG'),
    'sh_star_current': ('stock_info_sh_name_code', {'symbol': '科创板'}, 'XSHG'),
    'sz_current': ('stock_info_sz_name_code', {'symbol': 'A股列表'}, 'XSHE'),
    'bj_current': ('stock_info_bj_name_code', {}, 'BSE'),
    'sh_delisted': ('stock_info_sh_delist', {'symbol': '全部'}, 'XSHG'),
    'sz_delisted': ('stock_info_sz_delist', {'symbol': '终止上市公司'}, 'XSHE'),
}


def script_date(value):
    """Keep missing/invalid dates as uncertainty, as in the supplied script."""
    import pandas as pd
    parsed = pd.to_datetime(str(value), errors='coerce') if value is not None else None
    return None if parsed is None or pd.isna(parsed) else parsed.date()


def script_code(value):
    text = str(value).strip()
    if text.endswith('.0'):
        text = text[:-2]
    return text.zfill(6) if re.fullmatch(r'\d{1,6}', text) else None


def script_stock_code(code, exchange):
    # Preserve the project's previously accepted 302 and 689 ranges.
    return bool(code and len(code) == 6 and code.isdigit() and (exchange == 'BSE' or
        exchange == 'XSHG' and code.startswith(('60', '68')) or
        exchange == 'XSHE' and code.startswith(('000', '001', '002', '003', '300', '301', '302'))))


def select_script_listing(source_rows, *, kind, trade_date):
    """Reconstruct candidates without treating suspension as a delisting date."""
    exchange = SCRIPT_LISTING_CALLS[kind][2]
    fields = ('公司代码', '公司简称') if kind == 'sh_delisted' else ('证券代码', '证券简称')
    listing_field = '上市日期'
    if kind == 'sz_current':
        fields = ('A股代码', 'A股简称')
        listing_field = 'A股上市日期'
    required = {*fields, listing_field}
    if kind == 'sh_delisted':
        required.add('暂停上市日期')
    if kind == 'sz_delisted':
        required.add('终止上市日期')
    if not source_rows or any(not required <= set(row) for row in source_rows):
        raise ValueError('script listing fields changed or source table is empty')
    selected, excluded = [], []
    for row in source_rows:
        code = script_code(row[fields[0]])
        listed = script_date(row[listing_field])
        stopped = script_date(row.get('暂停上市日期'))
        delisted = script_date(row.get('终止上市日期'))
        reason = None
        if not script_stock_code(code, exchange):
            reason = 'invalid or out-of-scope stock code'
        elif listed is None:
            reason = 'listing date missing or invalid'
        elif listed > trade_date:
            reason = 'listed after target date'
        elif kind == 'sh_delisted' and (stopped is None or trade_date >= stopped):
            reason = 'suspension is not a verified termination date; historical interval uncertain'
        elif kind == 'sz_delisted' and delisted is not None and trade_date >= delisted:
            reason = 'outside target termination interval'
        if reason:
            excluded.append({**row, 'reason': reason})
            continue
        if not str(row[fields[1]] or '').strip():
            raise ValueError('selected stock name is empty')
        selected.append({**row, 'stock_code': code, 'stock_name': str(row[fields[1]]),
            'exchange': exchange, 'asset_type': 'stock', 'status': 'unknown',
            'trade_date': trade_date, 'list_date': listed, 'delist_date': delisted,
            'date_basis': 'suspension_interval_candidate' if kind == 'sh_delisted' else
                ('termination_interval_candidate' if delisted else 'termination_unknown_candidate')
                if kind == 'sz_delisted' else 'current_listing_date_candidate',
            'script_sources': 'ak_' + kind})
    if len({r['stock_code'] for r in selected}) != len(selected):
        raise ValueError('duplicate stock identity in original listing table')
    return tuple(selected), tuple(excluded)


def fetch_script_listing(provider, *, kind, trade_date, source_responses):
    from ..akshare.session import load_client
    provider.client = provider.client or load_client()
    function_name, parameters, exchange = SCRIPT_LISTING_CALLS[kind]
    function = getattr(provider.client, function_name)
    # The BSE SDK memoizes parsed tables. Use its unchanged request/parser body
    # so every call is backed by captured bytes rather than a hidden memory cache.
    frame = getattr(function, '__wrapped__', function)(**parameters)
    if frame.empty:
        raise ValueError('original script listing source returned empty data')
    responses = list(source_responses())
    if not responses:
        raise ValueError('listing result has no retained HTTP response')
    source_rows = tuple(frame.astype(object).where(frame.notna(), None).to_dict(orient='records'))
    if kind == 'bj_current':
        blocks = []
        for response in responses:
            body = response['body'].decode(response['encoding'] or 'utf-8')
            blocks.append(json.loads(body[body.find('['):-1])[0])
        total_pages = blocks[0]['totalPages']
        batches = blocks[1:]
        if (type(total_pages) is not int or total_pages <= 0 or len(batches) != total_pages
                or len(source_rows) != sum(len(b['content']) for b in batches)
                or len(source_rows) != blocks[0]['totalElements']):
            raise ValueError('BSE SDK pagination differs from retained original pages')
        if any(b.get('totalPages') != total_pages or b.get('totalElements') != len(source_rows)
               or b.get('number') != page for page, b in enumerate(batches)):
            raise ValueError('BSE total pages changed while collecting')
    selected, excluded = select_script_listing(source_rows, kind=kind, trade_date=trade_date)
    return InputFetchResult(selected, source_rows=source_rows, source_url=responses[-1]['url'],
        excluded_rows=excluded, empty_is_valid=not selected,
        mapping_context={'script_source': 'ak_' + kind, 'source_total_count': len(source_rows),
            'date_semantics': 'target-date candidates inferred from current listings/events; not a direct historical snapshot',
            'original_request_parameters': parameters, 'historical_universe_verified_complete': False,
            'exchange': exchange, 'sdk_cache_policy': 'native request body used; no parsed-table memoization',
            'warnings': ['later delistings and historical code changes may be missing',
                         'suspension dates are not termination dates']})


@dataclass(slots=True)
class SseEtfListProvider:
    client: Any = None
    name: str = "sse"
    endpoint: str = "etf_security_snapshot"
    capability_version: str = "sse-etf-catalog-v1"
    input_hosts = ("https://query.sse.com.cn",)
    page_url = "https://etf.sse.com.cn/fundlist/"
    api_url = "https://query.sse.com.cn/commonQuery.do"

    def fetch_snapshot(self, trade_date: date) -> InputFetchResult:
        # END_DATE filters listing dates; it does not reconstruct a past catalog.
        trade_date = date.fromisoformat(trade_date) if isinstance(trade_date, str) else trade_date
        params = {"isPagination": "true", "sqlId": "COMMON_JJZWZ_JJLB_L", "pageHelp.cacheSize": 1,
            "pageHelp.pageSize": 10000, "pageHelp.pageNo": 1, "pageHelp.beginPage": 1, "pageHelp.endPage": 1,
            "FUND_CODE": "", "COMPANY_NAME": "", "INDEX_NAME": "", "START_DATE": "",
            "END_DATE": trade_date.strftime("%Y%m%d"), "CATEGORY": "F000", "CATEGORY_ASC": 1,
            "SUBCLASS": "", "SWING_TRADE": "", "type": "inParams"}
        session = self.client or requests.Session()
        owned = self.client is None
        if owned:
            session.trust_env = False
            proxy = os.environ.get("STOCK_DATA_HTTP_PROXY", "http://127.0.0.1:20171")
            session.proxies.update({"http": proxy, "https": proxy})
            session.headers.update({"User-Agent": "Mozilla/5.0"})
        try:
            response = session.request("GET", self.api_url, params=params, data=None,
                headers={"Referer": self.page_url}, timeout=30, allow_redirects=False)
            response.raise_for_status()
            if 300 <= response.status_code < 400:
                self._invalid("SSE fund catalog redirected; investigate the original request contract")
            payload = response.json()
        finally:
            if owned:
                session.close()
        rows = payload.get("result") if isinstance(payload, dict) else None
        total = payload.get("pageHelp", {}).get("total") if isinstance(payload, dict) else None
        if (not isinstance(rows, list) or not rows or isinstance(total, bool)
                or not isinstance(total, (int, str)) or not str(total).isdigit() or int(total) != len(rows)):
            self._invalid("SSE fund catalog is empty or differs from its declared total")
        selected, excluded, identities = [], [], set()
        # The archived official category tree puts F110..F150 under ETF (F100).
        # F200 LOF, F400 other money funds and F600 REITs are outside this input.
        etf_categories = {"F100", "F110", "F111", "F112", "F113", "F114", "F115", "F120",
                          "F121", "F122", "F123", "F130", "F131", "F140", "F141", "F150"}
        for row in rows:
            if not isinstance(row, dict):
                self._invalid("SSE fund row changed")
            code, name, category = row.get("FUND_CODE"), row.get("FUND_ABBR"), row.get("CATEGORY")
            if not isinstance(code, str) or len(code) != 6 or not code.isdigit() or code in identities:
                self._invalid("SSE fund identity is invalid or duplicated")
            identities.add(code)
            if not isinstance(category, str) or not re.fullmatch(r"F\d{3}", category):
                self._invalid("SSE fund category is missing or invalid")
            if category.startswith("F1") and category not in etf_categories:
                self._invalid("SSE ETF category changed; update against the official category tree")
            if category not in etf_categories:
                excluded.append({**row, "reason": "outside_etf_category"})
                continue
            if not isinstance(name, str) or not name.strip():
                self._invalid("SSE ETF name is missing")
            try:
                listed = date.fromisoformat(row["LISTING_DATE"])
            except (KeyError, TypeError, ValueError):
                self._invalid("SSE ETF listing date is invalid")
            if listed > trade_date:
                excluded.append({**row, "reason": "future_listing"})
                continue
            selected.append({**row, "exchange": "XSHG", "asset_type": "etf", "status": "unknown"})
        if not selected:
            self._invalid("SSE catalog has no eligible listed ETFs")
        return InputFetchResult(tuple(selected), source_rows=tuple(rows), excluded_rows=tuple(excluded),
            source_url=self.api_url, mapping_context={"source_total_count": int(total), "source_page_count": 1,
                "pagination_completeness_verified": True, "listing_date_to": trade_date.isoformat(),
                "status_meaning": "listed directory membership; trading and liquidation status are unknown",
                "snapshot_date_meaning": "response capture day; END_DATE only filters listing dates",
                "independent_universe_denominator": None})

    @staticmethod
    def _invalid(message):
        raise ProviderContractError(message, FailureClass.SCHEMA_CHANGED, retryable=False)
