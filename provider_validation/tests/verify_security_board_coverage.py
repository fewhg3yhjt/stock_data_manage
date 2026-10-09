"""Reproduce the 2026-09-30 A-share universe and board-coverage probe.

This is a read-only live probe. It writes JSON evidence under the provider
validation results directory and does not write to any production data directory.

Read the retained result without network access:
    python provider_validation/tests/verify_security_board_coverage.py

Fetch a new snapshot only when explicitly requested:
    python provider_validation/tests/verify_security_board_coverage.py --refresh

The local VPN proxy defaults to http://127.0.0.1:20171. Override it with
STOCK_DATA_HTTP_PROXY when needed. Baostock is used for the SH/SZ universe and
industry snapshot; the Beijing Stock Exchange site supplies the BSE universe;
EastMoney slist supplies per-security board affiliations for BSE.
"""

from __future__ import annotations

import collections
import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

from raw_response_archive import RawResponseArchive


TRADE_DATE = "2026-09-30"
PROXY = os.environ.get("STOCK_DATA_HTTP_PROXY", "http://127.0.0.1:20171")
ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "provider_validation/results/legacy/2026-10-01-security-board-coverage.json"
SH_CODES = ("60", "68")
SZ_CODES = ("000", "001", "002", "003", "300", "301")
MIN_EM_INTERVAL_SECONDS = 1.02


def _a_share_code(source_code: str) -> str | None:
    prefix, dot, code = source_code.lower().partition(".")
    if not dot or len(code) != 6 or not code.isdigit():
        return None
    if (prefix == "sh" and code.startswith(SH_CODES)) or (
        prefix == "sz" and code.startswith(SZ_CODES)
    ):
        return code
    return None


def fetch_sh_sz(archive: RawResponseArchive) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    import baostock as bs

    login = bs.login()
    if getattr(login, "error_code", None) != "0":
        raise RuntimeError(f"BaoStock login failed: {login.error_code} {login.error_msg}")
    try:
        listed_result = bs.query_all_stock(day=TRADE_DATE)
        if listed_result.error_code != "0":
            raise RuntimeError(f"query_all_stock failed: {listed_result.error_code} {listed_result.error_msg}")
        listed: dict[str, dict[str, Any]] = {}
        raw_listed_rows: list[dict[str, Any]] = []
        while listed_result.next():
            record = dict(zip(listed_result.fields, listed_result.get_row_data()))
            code = _a_share_code(str(record.get("code", "")))
            if code:
                raw_listed_rows.append(record)
                listed[code] = {
                    "code": code,
                    "name": record.get("code_name", ""),
                    "status": "active" if str(record.get("tradeStatus", "")) == "1" else "suspended",
                }

        industry_result = bs.query_stock_industry(date=TRADE_DATE)
        if industry_result.error_code != "0":
            raise RuntimeError(
                f"query_stock_industry failed: {industry_result.error_code} {industry_result.error_msg}"
            )
        industries: dict[str, dict[str, str]] = {}
        raw_industry_rows_list: list[dict[str, Any]] = []
        snapshot_dates: set[str] = set()
        raw_industry_rows = 0
        while industry_result.next():
            record = dict(zip(industry_result.fields, industry_result.get_row_data()))
            code = _a_share_code(str(record.get("code", "")))
            if not code:
                continue
            raw_industry_rows += 1
            raw_industry_rows_list.append(record)
            snapshot_dates.add(str(record.get("updateDate", "")))
            industries[code] = {
                "industry": str(record.get("industry", "")),
                "industry_classification": str(record.get("industryClassification", "")),
                "classification_update_date": str(record.get("updateDate", "")),
            }
        for endpoint, fields, rows in (
            ("query_all_stock", list(listed_result.fields), raw_listed_rows),
            ("query_stock_industry", list(industry_result.fields), raw_industry_rows_list),
        ):
            with archive.scope(capability=endpoint, trade_date=TRADE_DATE, exchange_scope="SH+SZ A shares"):
                payload = json.dumps(
                    {"fields": fields, "rows": rows}, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
                archive.store_source_payload(
                    payload,
                    provider="baostock",
                    endpoint=endpoint,
                    representation="SDK-decoded ResultSet fields and rows; raw TCP wire frame is not exposed by BaoStock",
                    metadata={"row_count": len(rows), "trade_date": TRADE_DATE},
                )
    finally:
        bs.logout()

    rows = []
    active = 0
    suspended = 0
    for code, security in sorted(listed.items()):
        industry = industries.get(code)
        is_active = security["status"] == "active"
        active += int(is_active)
        suspended += int(not is_active)
        rows.append({**security, **(industry or {}), "has_industry": bool(industry and industry["industry"])})
    active_matched = sum(row["status"] == "active" and row["has_industry"] for row in rows)
    labels = sorted({row["industry"] for row in rows if row.get("industry")})
    summary = {
        "security_count_including_suspended": len(rows),
        "active_security_count": active,
        "suspended_security_count": suspended,
        "classification_rows_for_a_shares": raw_industry_rows,
        "classification_snapshot_dates": sorted(date for date in snapshot_dates if date),
        "distinct_industry_labels": len(labels),
        "distinct_industry_names": labels,
        "active_with_industry": active_matched,
        "active_without_industry": [
            {"code": row["code"], "name": row["name"]}
            for row in rows
            if row["status"] == "active" and not row["has_industry"]
        ],
        "active_coverage_percent": round(active_matched * 100 / active, 4) if active else None,
    }
    return rows, summary


def _proxy_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.proxies.update({"http": PROXY, "https": PROXY})
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session


def fetch_bse_universe(
    session: requests.Session, archive: RawResponseArchive
) -> list[dict[str, Any]]:
    page_url = "https://www.bse.cn/nq/quotation.html"
    api_url = "https://www.bse.cn/nqhqController/nqhq_en.do"
    session.headers.update(
        {
            "Referer": page_url,
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }
    )
    # The official page may return a 302 for an anonymous session. Its API can
    # still establish a session; refresh the page only if the API redirects.
    with archive.scope(capability="bse_security_universe_page", trade_date=TRADE_DATE):
        session.get(page_url, timeout=20, allow_redirects=False)
    records: list[dict[str, Any]] = []
    expected_total: int | None = None
    for page in range(100):
        form = {
            "page": page,
            "type_en": '["B"]',
            "sortfield": "hqzqdm",
            "sorttype": "asc",
            "xxfcbj_en": "[2]",
            "zqdm": "",
        }
        with archive.scope(capability="bse_security_universe", trade_date=TRADE_DATE, page=str(page)):
            response = session.post(api_url, data=form, timeout=30, allow_redirects=False)
        if 300 <= response.status_code < 400:
            with archive.scope(capability="bse_security_universe_page", trade_date=TRADE_DATE, retry="1"):
                session.get(page_url, timeout=20, allow_redirects=False)
            with archive.scope(capability="bse_security_universe", trade_date=TRADE_DATE, page=str(page), retry="1"):
                response = session.post(api_url, data=form, timeout=30, allow_redirects=False)
        response.raise_for_status()
        body = response.text.strip()
        match = re.fullmatch(r"[A-Za-z_$][\w$]*\((.*)\);?", body, re.S)
        payload = json.loads(match.group(1) if match else body)
        if not isinstance(payload, list) or len(payload) != 1:
            raise RuntimeError("Unexpected BSE official response shape")
        block = payload[0]
        batch = block.get("content")
        total = int(block.get("totalElements", -1))
        if not isinstance(batch, list) or total < 0:
            raise RuntimeError("Unexpected BSE official page fields")
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise RuntimeError("BSE total changed during pagination")
        records.extend(batch)
        print(f"BSE official list: {len(records)}/{expected_total}", flush=True)
        if len(records) >= expected_total:
            break
        if not batch:
            raise RuntimeError("BSE pagination ended before the declared total")
        time.sleep(0.25)
    if expected_total is None or len(records) != expected_total:
        raise RuntimeError(f"Incomplete BSE official list: {len(records)}/{expected_total}")
    return [
        {
            "code": str(record.get("hqzqdm", "")).zfill(6),
            "name": str(record.get("hqzqjc", "")),
        }
        for record in records
    ]


def fetch_bse_affiliations(
    session: requests.Session,
    securities: list[dict[str, Any]],
    archive: RawResponseArchive,
) -> list[dict[str, Any]]:
    url = "https://push2.eastmoney.com/api/qt/slist/get"
    session.headers.update({"Referer": "https://quote.eastmoney.com/"})
    results: list[dict[str, Any]] = []
    last_request = 0.0
    for index, security in enumerate(securities, start=1):
        wait = MIN_EM_INTERVAL_SECONDS - (time.monotonic() - last_request)
        if last_request and wait > 0:
            time.sleep(wait)
        params = {
            "fltt": "2",
            "invt": "2",
            "secid": f"0.{security['code']}",
            "spt": "3",
            "pi": "0",
            "pz": "200",
            "po": "1",
            "fields": "f12,f14,f3,f128",
        }
        try:
            with archive.scope(
                capability="eastmoney_security_board_membership",
                security_code=security["code"],
                trade_date=TRADE_DATE,
            ):
                response = session.get(url, params=params, timeout=20)
            last_request = time.monotonic()
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data") or {}
            diff = data.get("diff") or []
            values = list(diff.values()) if isinstance(diff, dict) else list(diff)
            boards = [
                {
                    "code": str(board.get("f12", "")),
                    "name": str(board.get("f14", "")),
                    "change_pct": board.get("f3"),
                    "leader": str(board.get("f128", "")),
                }
                for board in values
                if board.get("f12")
            ]
            results.append({**security, "boards": boards, "error": None})
        except Exception as exc:  # Preserve per-security failures in evidence.
            last_request = time.monotonic()
            results.append(
                {
                    **security,
                    "boards": [],
                    "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                }
            )
        if index % 20 == 0 or index == len(securities):
            errors = sum(row["error"] is not None for row in results)
            empty = sum(not row["boards"] and row["error"] is None for row in results)
            print(f"BSE board affiliations: {index}/{len(securities)}; errors={errors}; empty={empty}", flush=True)
    return results


def probe_eastmoney_board_directories(
    session: requests.Session, archive: RawResponseArchive
) -> list[dict[str, Any]]:
    checks = []
    for host in ("push2.eastmoney.com", "push2delay.eastmoney.com"):
        url = f"https://{host}/api/qt/clist/get"
        try:
            with archive.scope(capability="eastmoney_concept_board_directory", host=host, page="1", page_size="1"):
                response = session.get(
                    url,
                    params={
                        "pn": "1",
                        "pz": "1",
                        "po": "1",
                        "np": "1",
                        "fid": "f3",
                        "fs": "m:90+t:3",
                        "fields": "f12,f14",
                    },
                    timeout=15,
                )
            response.raise_for_status()
            payload = response.json()
            checks.append(
                {
                    "host": host,
                    "status": "success" if payload.get("data") else "empty_or_changed",
                    "http_status": response.status_code,
                    "sample_rows": len(((payload.get("data") or {}).get("diff") or [])),
                }
            )
        except Exception as exc:
            checks.append(
                {
                    "host": host,
                    "status": "transport_error",
                    "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                }
            )
    return checks


def fetch_official_catalog_baselines(output: Path, trade_date: dt.date, only=None) -> dict[str, Any]:
    """Independent source probes only; preserve bytes before any JSON/XLSX parsing."""
    import hashlib
    import io
    import pandas as pd
    from stock_data_manage.storage.raw import RawObjectStore
    output = output.resolve()
    if not output.is_relative_to(ROOT / "provider_validation/results"):
        raise ValueError("catalog probes must remain in provider_validation/results")
    output.mkdir(parents=True, exist_ok=False)
    store = RawObjectStore(output / "_raw")
    code_bytes=Path(__file__).read_bytes()
    code_version = hashlib.sha256(code_bytes).hexdigest()
    (output/"verification-code.txt").write_bytes(code_bytes)
    session = _proxy_session()
    results, failures = {}, {}
    enabled=lambda key: only is None or key in only
    class NotSelected(Exception):
        pass

    def request(key, url, *, params=None, data=None, referer=None, permit_page_redirect=False):
        headers = {"Referer": referer} if referer else {}
        method = "POST" if data is not None else "GET"
        scope = {"check": key, "as_of_date": trade_date.isoformat(), "params": params, "form": data}
        try:
            response = session.request(method, url, params=params, data=data, headers=headers,
                timeout=30, allow_redirects=False)
        except requests.RequestException as exc:
            store.append_event({"event": "http_response", "provider": "official_catalog_baseline", "endpoint": key,
                "method": method, "url": url, "scope": scope, "outcome": "transport_error", "mode": "live",
                "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "code_version": code_version,
                "error_type": type(exc).__name__})
            raise
        store.record_response(response=response, url=response.request.url, method=method,
            request_headers=response.request.headers, scope=scope, provider="official_catalog_baseline",
            endpoint=key, code_version=code_version, mode="live",
            request_options={"timeout":30,"allow_redirects":False,"trust_env":session.trust_env})
        response.raise_for_status()
        if 300 <= response.status_code < 400 and not permit_page_redirect:
            raise ValueError("official baseline redirected; source contract requires investigation")
        return response

    def save(key, rows, code_column, name_column, date_column=None):
        selected, excluded = [], []
        for row in rows:
            code = str(row[code_column]).split(".")[0].zfill(6)
            if len(code) != 6 or not code.isdigit():
                raise ValueError("official baseline security code changed")
            if date_column:
                listed = pd.to_datetime(row[date_column], errors="raise").date()
                if listed > trade_date:
                    excluded.append({**row,"reason":"future_listing"})
                    continue
            selected.append({"code":code,"name":str(row[name_column]),"source_row":row})
        if not selected or len({r["code"] for r in selected}) != len(selected):
            raise ValueError("official baseline is empty or has duplicate securities")
        path = output / (key + ".json")
        path.write_text(json.dumps({"rows":selected,"excluded":excluded},ensure_ascii=False,default=str,indent=2),encoding="utf-8")
        results[key] = {"rows":len(selected),"excluded":len(excluded),"path":str(path),
                        "sha256":hashlib.sha256(path.read_bytes()).hexdigest()}

    try:
        for key, kind in (("sh-main-stock","1"),("sh-star-stock","8")):
            if not enabled(key): continue
            try:
                params={"STOCK_TYPE":kind,"REG_PROVINCE":"","CSRC_CODE":"","STOCK_CODE":"",
                    "sqlId":"COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L","COMPANY_STATUS":"2,4,5,7,8","type":"inParams",
                    "isPagination":"true","pageHelp.cacheSize":"1","pageHelp.beginPage":"1",
                    "pageHelp.pageSize":"10000","pageHelp.pageNo":"1","pageHelp.endPage":"1"}
                doc=request(key,"https://query.sse.com.cn/sseQuery/commonQuery.do",params=params,
                    referer="https://www.sse.com.cn/assortment/stock/list/share/").json()
                rows=doc["result"]
                if int(doc["pageHelp"]["total"]) != len(rows): raise ValueError("SSE stock page is incomplete")
                save(key,rows,"A_STOCK_CODE","SEC_NAME_CN","LIST_DATE")
            except Exception as exc: failures[key]=f"{type(exc).__name__}: {str(exc)[:500]}"
        for key,url,params,referer,code_col,name_col,date_col in (
            ("sz-stock","https://www.szse.cn/api/report/ShowReport",
             {"SHOWTYPE":"xlsx","CATALOGID":"1110","TABKEY":"tab1","random":"0.6935816432433362"},
             "https://www.szse.cn/market/product/stock/list/index.html","A股代码","A股简称","A股上市日期"),
            ("sz-fund","https://fund.szse.cn/api/report/ShowReport",
             {"SHOWTYPE":"xlsx","CATALOGID":"1000_lf","TABKEY":"tab1","random":"0.07610353191740105"},
             "https://fund.szse.cn/marketdata/fundslist/index.html","基金代码","基金简称","上市日期")):
            if not enabled(key): continue
            try:
                response=request(key,url,params=params,referer=referer)
                frame=pd.read_excel(io.BytesIO(response.content),dtype={code_col:str})
                save(key,frame.to_dict(orient="records"),code_col,name_col,date_col)
            except Exception as exc: failures[key]=f"{type(exc).__name__}: {str(exc)[:500]}"
        try:
            key="sh-etf-scale"
            if not enabled(key): raise NotSelected()
            params={"isPagination":"true","pageHelp.pageSize":"10000","pageHelp.pageNo":"1","pageHelp.beginPage":"1",
                "pageHelp.cacheSize":"1","pageHelp.endPage":"1","sqlId":"COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
                "STAT_DATE":trade_date.isoformat()}
            doc=request(key,"https://query.sse.com.cn/commonQuery.do",params=params,referer="https://www.sse.com.cn/").json()
            rows=doc["result"]
            if int(doc["pageHelp"]["total"]) != len(rows): raise ValueError("SSE ETF scale page is incomplete")
            if any(row["STAT_DATE"][:10] != trade_date.isoformat() for row in rows): raise ValueError("ETF scale date differs")
            save(key,rows,"SEC_CODE","SEC_NAME")
        except NotSelected: pass
        except Exception as exc: failures["sh-etf-scale"]=f"{type(exc).__name__}: {str(exc)[:500]}"
        try:
            key="bse-listed-stock"
            if not enabled(key): raise NotSelected()
            page_url="https://www.bse.cn/nq/listedcompany.html"
            request(key+"-page",page_url,permit_page_redirect=True)
            rows=[]; total=None
            for page in range(100):
                form={"page":page,"typejb":"T","xxfcbj[]":"2","xxzqdm":"","sortfield":"xxzqdm","sorttype":"asc"}
                response=request(key,"https://www.bse.cn/nqxxController/nqxxCnzq.do",data=form,referer=page_url,permit_page_redirect=True)
                if 300 <= response.status_code < 400:
                    request(key+"-page",page_url,permit_page_redirect=True)
                    response=request(key,"https://www.bse.cn/nqxxController/nqxxCnzq.do",data=form,referer=page_url)
                body=response.text.strip();match=re.fullmatch(r"[A-Za-z_$][\w$]*\((.*)\);?",body,re.S)
                block=json.loads(match.group(1) if match else body)[0]
                if total is None: total=int(block["totalElements"])
                if int(block["totalElements"]) != total or block["number"] != page or not block["content"]:
                    raise ValueError("BSE independent list pagination differs")
                rows.extend(block["content"])
                if len(rows)>=total:
                    if len(rows)!=total or not block["lastPage"]: raise ValueError("BSE independent list is incomplete")
                    break
                time.sleep(1)
            else: raise ValueError("BSE independent list exceeded 100 pages")
            save(key,rows,"xxzqdm","xxzqjc")
        except NotSelected: pass
        except Exception as exc: failures["bse-listed-stock"]=f"{type(exc).__name__}: {str(exc)[:500]}"
        if only and enabled("sh-fund-directory"):
            try:
                params={"isPagination":"true","sqlId":"COMMON_JJZWZ_JJLB_L","pageHelp.cacheSize":1,
                    "pageHelp.pageSize":10000,"pageHelp.pageNo":1,"pageHelp.beginPage":1,"pageHelp.endPage":1,
                    "FUND_CODE":"","COMPANY_NAME":"","INDEX_NAME":"","START_DATE":"","END_DATE":trade_date.strftime("%Y%m%d"),
                    "CATEGORY":"F000","CATEGORY_ASC":1,"SUBCLASS":"","SWING_TRADE":"","type":"inParams"}
                doc=request("sh-fund-directory","https://query.sse.com.cn/commonQuery.do",params=params,referer="https://etf.sse.com.cn/fundlist/").json()
                rows=doc["result"]
                if int(doc["pageHelp"]["total"]) != len(rows): raise ValueError("SSE fund list is incomplete")
                save("sh-fund-directory",rows,"FUND_CODE","FUND_ABBR","LISTING_DATE")
            except Exception as exc: failures["sh-fund-directory"]=f"{type(exc).__name__}: {str(exc)[:500]}"
        if only and enabled("sh-fund-types"):
            try:
                params={"sqlId":"COMMON_JJZWZ_JJLB_JJLX_C","CATEGORY_PARENT_CODE":""}
                doc=request("sh-fund-types","https://query.sse.com.cn/commonQuery.do",params=params,referer="https://etf.sse.com.cn/fundlist/").json()
                path=output/"sh-fund-types.json"
                path.write_text(json.dumps(doc,ensure_ascii=False,indent=2),encoding="utf-8")
                results["sh-fund-types"]={"rows":len(doc["result"]),"path":str(path),"sha256":hashlib.sha256(path.read_bytes()).hexdigest()}
            except Exception as exc: failures["sh-fund-types"]=f"{type(exc).__name__}: {str(exc)[:500]}"
        if only and enabled("sh-etf-list-page"):
            try:
                response=request("sh-etf-list-page","https://etf.sse.com.cn/fundlist/")
                scripts=re.findall(r'<script[^>]+src=[\"\x27]([^\"\x27]+)',response.text)
                (output/"sh-etf-page-scripts.json").write_text(json.dumps(scripts,ensure_ascii=False,indent=2),encoding="utf-8")
                results["sh-etf-list-page"]={"bytes":len(response.content),"scripts":scripts}
            except Exception as exc: failures["sh-etf-list-page"]=f"{type(exc).__name__}: {str(exc)[:500]}"
        resources={"sh-etf-list-data":"https://etf.sse.com.cn/fundlist/data.js",
            "sh-etf-list-api":"https://etf.sse.com.cn/xhtml/js/api.js?v=V202103-01",
            "sh-etf-list-view":"https://etf.sse.com.cn/xhtml/js/js.js?v=V202201-10",
            "sh-etf-list-driver":"https://etf.sse.com.cn/xhtml/js/fundlist.js?v=V3.1.0_20260304"}
        for key,url in resources.items():
            if not only or not enabled(key): continue
            try:
                response=request(key,url,referer="https://etf.sse.com.cn/fundlist/")
                path=output/(key+".txt")
                path.write_bytes(response.content)
                results[key]={"bytes":len(response.content),"path":str(path),"sha256":hashlib.sha256(response.content).hexdigest()}
            except Exception as exc: failures[key]=f"{type(exc).__name__}: {str(exc)[:500]}"
    finally:
        session.close()
    manifest = store.root / "manifest.ndjson"
    events = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()] if manifest.exists() else []
    for key, result in results.items():
        result["source_response_hashes"] = [event["body_sha256"] for event in events
            if event.get("endpoint") == key and event.get("body_sha256")]
        result["transformation_version"] = code_version
    summary={"as_of_date":trade_date.isoformat(),"code_version":code_version,"results":results,"failures":failures,
        "validated_at_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
        "raw_manifest_sha256":RawObjectStore.verify_manifest(manifest) if events else None,
        "date_meaning":"Current official catalogs with listing-date cutoff where available; not a historical reconstruction",
        "raw_manifest":str(store.root/"manifest.ndjson"),"routing_eligible":False,
        "note":"Independent source evidence only; ETF scope and exact identities must still be compared."}
    (output/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="make live requests and append raw responses; omitted means read the saved result only",
    )
    parser.add_argument("--official-baselines",type=Path,help="new evidence directory for independent official catalogs")
    parser.add_argument("--date",type=dt.date.fromisoformat,help="explicit baseline date")
    parser.add_argument("--only",action="append",choices=("sh-main-stock","sh-star-stock","sz-stock","sz-fund",
        "sh-etf-scale","bse-listed-stock","sh-etf-list-page","sh-etf-list-data","sh-etf-list-api","sh-etf-list-view","sh-etf-list-driver",
        "sh-fund-directory","sh-fund-types"),help="probe only missing independent evidence")
    args = parser.parse_args()
    if args.official_baselines:
        if args.date is None: parser.error("official baselines require --date")
        result=fetch_official_catalog_baselines(args.official_baselines,args.date,args.only)
        print(json.dumps(result,ensure_ascii=False,indent=2))
        return 2 if result["failures"] else 0
    if not args.refresh and OUTPUT.exists():
        saved = json.loads(OUTPUT.read_text(encoding="utf-8"))
        print("Saved evidence found; no network requests made.")
        summary = saved.get("summary", {})
        print(
            json.dumps(
                {
                    "sh_sz": {
                        key: value
                        for key, value in summary.get("sh_sz", {}).items()
                        if key != "distinct_industry_names"
                    },
                    "bse": summary.get("bse", {}),
                    "combined_active_sh_sz_plus_bse_listing": summary.get(
                        "combined_active_sh_sz_plus_bse_listing"
                    ),
                },
                ensure_ascii=True,
                indent=2,
            )
        )
        print(f"Evidence: {OUTPUT}")
        return 0
    print(f"Read-only coverage probe for {TRADE_DATE}; proxy={PROXY}", flush=True)
    session = _proxy_session()
    run_id = "2026-10-01-security-board-coverage-v2"
    try:
        with RawResponseArchive(run_id) as archive:
            sh_sz, sh_sz_summary = fetch_sh_sz(archive)
            with archive.scope(capability="bse_security_universe", trade_date=TRADE_DATE, page="all"):
                bse_universe = fetch_bse_universe(session, archive)
            bse_rows = fetch_bse_affiliations(session, bse_universe, archive)
            directory_checks = probe_eastmoney_board_directories(session, archive)
    finally:
        session.close()

    successful = [row for row in bse_rows if row["error"] is None]
    histogram = collections.Counter(len(row["boards"]) for row in successful)
    unique_boards = {
        (board["code"], board["name"])
        for row in successful
        for board in row["boards"]
    }
    bse_summary = {
        "official_universe_count": len(bse_universe),
        "membership_query_success": len(successful),
        "membership_query_failure": len(bse_rows) - len(successful),
        "successful_but_zero_boards": sum(not row["boards"] for row in successful),
        "total_membership_relations": sum(len(row["boards"]) for row in successful),
        "unique_board_codes_seen_in_bse_memberships": len(unique_boards),
        "affiliation_count_histogram": {str(key): histogram[key] for key in sorted(histogram)},
        "coverage_percent": round(len(successful) * 100 / len(bse_universe), 4) if bse_universe else None,
    }
    result = {
        "probe": "full_security_board_coverage",
        "trade_date": TRADE_DATE,
        "fetched_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "sources": {
            "sh_sz_universe_and_industry": "BaoStock query_all_stock + query_stock_industry",
            "bse_universe": "BSE official full quotation list (nqhq_en.do)",
            "bse_affiliations": "EastMoney push2 slist/get, per-security",
            "all_concept_board_directory": "EastMoney clist/get probe (see directory_checks)",
        },
        "raw_capture": {
            "status": "captured_before_parsing",
            "archive": f"provider_validation/results/raw/{run_id}",
            "manifest": "manifest.ndjson",
            "body_encoding": "gzip of requests decoded response bytes; SHA-256 is over the uncompressed body",
        },
        "scope_note": (
            "EastMoney slist returns mixed industry, concept, and region board affiliations. "
            "A successful per-security response does not by itself prove a complete categorized "
            "concept-board directory or complete SH/SZ concept memberships."
        ),
        "summary": {
            "sh_sz": sh_sz_summary,
            "bse": bse_summary,
            "combined_active_sh_sz_plus_bse_listing": sh_sz_summary["active_security_count"] + len(bse_universe),
        },
        "board_directory_checks": directory_checks,
        "securities": {
            "sh_sz": sh_sz,
            "bse": bse_rows,
        },
    }
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("Summary:")
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    print(f"Evidence saved: {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
