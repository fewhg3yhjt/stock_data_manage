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
