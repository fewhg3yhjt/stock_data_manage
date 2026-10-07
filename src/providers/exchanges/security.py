"""BSE catalog using the retained official-source probe's Session and pagination."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
