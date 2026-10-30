"""Source catalog contracts: stock/ETF selection, BSE paging and retained evidence."""
from datetime import date
import json
from pathlib import Path

import pytest
import requests

from stock_data_manage.pipeline.inputs import collect_input
from stock_data_manage.providers.baostock.industry import _a_share_code, _is_etf
from stock_data_manage.providers.contracts import ProviderContractError
from stock_data_manage.providers.exchanges.security import BseSecurityListProvider, SseEtfListProvider
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.service.instruments_update import prepare_security_publication


ROOT = Path(__file__).resolve().parents[1]
BAO = ROOT / "provider_validation/results/live-probes/baostock-industry-20260930-20261003/_raw/baostock-industry/manifest.ndjson"
BSE = ROOT / "provider_validation/results/raw/security-catalog-bse-20261007/manifest.ndjson"
SSE_FUNDS = ROOT / "provider_validation/results/security-activation-20261009/official-sh-funds/_raw/manifest.ndjson"


@pytest.mark.parametrize('kind,count', [('sse', 920), ('money', 24), ('szse', 750)])
def test_dated_etf_real_response_yaml_contract(tmp_path, kind, count):
    """Replay retained Oct 9 bytes, never call the network or production root."""
    root = ROOT / 'tmp/universe-history-probe-20261011'
    archive = root / ('money/raw' if kind == 'money' else kind + '/network/raw') / 'manifest.ndjson'
    if not archive.exists():
        pytest.skip('local dated ETF investigation evidence is not bundled')
    identifier = {'sse': 'SECURITY-SSE-ETF-HISTORY-001', 'money': 'SECURITY-SSE-MONEY-ETF-HISTORY-001',
                  'szse': 'SECURITY-SZSE-ETF-HISTORY-001'}[kind]
    result = collect_input(input_id=identifier, context={'request': {'trade_date': '2026-10-09'},
        'calendar': {'trading_dates': [date(2026, 10, 9)]}}, config_root=ROOT / 'config',
        output_root=tmp_path, mode='replay', replay_manifest=archive,
        pacer=RequestPacer(wait=lambda seconds: None))
    assert result['status'] == 'candidate_complete', result.get('error')
    assert result['live_http_calls'] == 0
    rows = output_rows(result)
    assert len(rows) == count
    assert {row['trade_date'] for row in rows} == {'2026-10-09'}
    assert {row['status'] for row in rows} == {'unknown'}
    assert all(row['list_date'] is None for row in rows)


@pytest.mark.parametrize('change', ['date', 'total', 'duplicate', 'code', 'shares'])
def test_sse_dated_etf_rejects_bad_source(change):
    from stock_data_manage.providers.sse.market import _dated_etf_rows
    rows = [{'STAT_DATE': '2026-10-09', 'SEC_CODE': '510300', 'SEC_NAME': 'ETF', 'TOT_VOL': '100'}]
    payload = {'result': rows, 'pageHelp': {'total': 1}}
    if change == 'date':
        rows[0]['STAT_DATE'] = '2026-10-08'
    elif change == 'total':
        payload['pageHelp']['total'] = 2
    elif change == 'duplicate':
        rows.append(dict(rows[0]))
        payload['pageHelp']['total'] = 2
    elif change == 'code':
        rows[0]['SEC_CODE'] = 'bad'
    else:
        rows[0]['TOT_VOL'] = '-1'
    with pytest.raises(ValueError):
        _dated_etf_rows(payload, date(2026, 10, 9))


@pytest.mark.parametrize('change', ['date', 'code', 'duplicate', 'discard', 'shares'])
def test_szse_dated_etf_rejects_sdk_hidden_source_damage(change):
    import io
    import pandas as pd
    from types import SimpleNamespace
    from stock_data_manage.providers.exchanges.daily import ExchangeDailyProvider
    row = {'日期': '2026-10-09', '基金代码': 159001, '基金简称': 'ETF', '基金规模(份)': '1,000'}
    if change == 'date':
        row['日期'] = '2026-10-08'
    elif change == 'code':
        row['基金代码'] = 159001.5
    elif change == 'shares':
        row['基金规模(份)'] = 'bad'
    raw = pd.DataFrame([row, dict(row)] if change in {'duplicate', 'discard'} else [row])
    sdk = pd.DataFrame([{'日期': date(2026, 10, 9), '基金代码': '159001', '基金简称': 'ETF', '基金份额': 1000}]
        * (2 if change == 'duplicate' else 1))
    body = io.BytesIO()
    raw.to_excel(body, index=False)
    client = SimpleNamespace(fund_scale_daily_szse=lambda **params: sdk)
    with pytest.raises(ValueError):
        ExchangeDailyProvider(client).fetch_etf_history(trade_date=date(2026, 10, 9),
            source_responses=lambda: [{'body': body.getvalue(), 'url': 'https://fixture.invalid/'}])



def sse_fund(code="512390", category="F112", listed="2018-07-13"):
    return {"FUND_CODE": code, "FUND_ABBR": "fixture fund", "CATEGORY": category, "LISTING_DATE": listed}


class SseCatalogSession:
    def __init__(self, rows, total=None, status=200):
        self.rows, self.total, self.status, self.calls = rows, len(rows) if total is None else total, status, []

    def request(self, method, url, **options):
        self.calls.append((method, url, options))
        return CatalogSession.response(self.status, json.dumps({"result": self.rows, "pageHelp": {"total": self.total}}))


def test_sse_catalog_preserves_listed_etfs_and_original_request_contract():
    rows = [sse_fund(), sse_fund("511600", "F150"), sse_fund("501001", "F211"),
            sse_fund("508001", "F600"), sse_fund("530001", "F111", "2026-10-12")]
    client = SseCatalogSession(rows)
    result = SseEtfListProvider(client=client).fetch_snapshot(date(2026, 10, 9))
    assert [r["FUND_CODE"] for r in result.rows] == ["512390", "511600"]
    assert all(r["status"] == "unknown" for r in result.rows)
    assert result.source_rows == tuple(rows) and len(result.excluded_rows) == 3
    method, url, options = client.calls[0]
    assert method == "GET" and url == SseEtfListProvider.api_url
    assert options["params"]["END_DATE"] == "20261009" and options["params"]["CATEGORY"] == "F000"
    assert options["headers"] == {"Referer": "https://etf.sse.com.cn/fundlist/"}
    assert options["timeout"] == 30 and options["allow_redirects"] is False and options["data"] is None


@pytest.mark.parametrize("problem", ["partial", "duplicate", "unknown_etf_category", "bad_date", "missing_code", "redirect", "empty"])
def test_sse_catalog_rejects_incomplete_or_changed_responses(problem):
    rows, total, status = [sse_fund()], None, 200
    if problem == "partial": total = 2
    if problem == "duplicate": rows.append(sse_fund())
    if problem == "unknown_etf_category": rows[0]["CATEGORY"] = "F199"
    if problem == "bad_date": rows[0]["LISTING_DATE"] = "bad"
    if problem == "missing_code": rows[0].pop("FUND_CODE")
    if problem == "redirect": status = 302
    if problem == "empty": rows = []
    with pytest.raises(ProviderContractError):
        SseEtfListProvider(client=SseCatalogSession(rows, total, status)).fetch_snapshot(date(2026, 10, 9))


def test_sse_real_archive_uses_yaml_mapping_original_date_and_preserves_liquidating_etf(tmp_path):
    from stock_data_manage.storage.raw import RawObjectStore
    report = collect_input(input_id="SECURITY-SSE-ETF-001", config_root=ROOT / "config", data_root=tmp_path,
        context={"request": {"trade_date": date(2026, 10, 9)}}, mode="replay", replay_manifest=SSE_FUNDS,
        pacer=RequestPacer(wait=lambda _: None))
    assert report["status"] == "candidate_complete", report.get("error")
    rows = output_rows(report)
    assert len(rows) == 944 and report["source_rows"]["row_count"] == 1125
    assert report["excluded_rows"]["row_count"] == 181
    assert {row["trade_date"] for row in rows} == {"2026-10-09"}
    preserved = next(row for row in rows if row["stock_code"] == "512390")
    assert preserved["list_date"] == "2018-07-13" and preserved["status"] == "unknown"
    manifest = (Path(report["run_directory"]) / report["raw_manifest"]["path"]).resolve()
    assert RawObjectStore.verify_manifest(manifest) == report["raw_manifest"]["sha256"]
    assert not report["eligible_for_production_routing"] and report["live_http_calls"] == 0


@pytest.fixture
def official_baseline_probe(monkeypatch, tmp_path):
    import importlib.util
    monkeypatch.syspath_prepend(str(ROOT / "provider_validation/tests"))
    spec = importlib.util.spec_from_file_location("catalog_baseline_probe", ROOT / "provider_validation/tests/verify_security_board_coverage.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    return module, tmp_path / "provider_validation/results/fixture"


class OfficialBaselineSession:
    trust_env = False

    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []
        self.closed = False

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        response = requests.Response()
        response.status_code, body = self.replies.pop(0)
        response._content = body.encode("utf-8")
        response.encoding = "utf-8"
        response.headers["Content-Type"] = "application/json"
        response.request = requests.Request(method, url, headers=kwargs["headers"],
            params=kwargs["params"], data=kwargs["data"]).prepare()
        return response

    def close(self):
        self.closed = True


def test_official_baseline_archives_malformed_response_before_parsing(official_baseline_probe, monkeypatch):
    from stock_data_manage.storage.raw import RawObjectStore
    module, output = official_baseline_probe
    session = OfficialBaselineSession([(200, "not-json")])
    monkeypatch.setattr(module, "_proxy_session", lambda: session)
    result = module.fetch_official_catalog_baselines(output, date(2026, 10, 9), ["sh-main-stock"])
    assert "sh-main-stock" in result["failures"] and not result["results"]
    manifest = Path(result["raw_manifest"])
    event = json.loads(manifest.read_text().splitlines()[0])
    assert RawObjectStore.read_response(manifest, event) == b"not-json"
    assert result["raw_manifest_sha256"] == RawObjectStore.verify_manifest(manifest)
    assert len(session.calls) == 1 and session.closed


def test_official_baseline_excludes_future_listings_with_evidence(official_baseline_probe, monkeypatch):
    module, output = official_baseline_probe
    rows = [{"A_STOCK_CODE": "600001", "SEC_NAME_CN": "fixture", "LIST_DATE": "2020-01-01"},
            {"A_STOCK_CODE": "600002", "SEC_NAME_CN": "future", "LIST_DATE": "2026-10-12"}]
    session = OfficialBaselineSession([(200, json.dumps({"result": rows, "pageHelp": {"total": 2}}))])
    monkeypatch.setattr(module, "_proxy_session", lambda: session)
    result = module.fetch_official_catalog_baselines(output, date(2026, 10, 9), ["sh-main-stock"])
    derived = json.loads(Path(result["results"]["sh-main-stock"]["path"]).read_text())
    assert not result["failures"] and [r["code"] for r in derived["rows"]] == ["600001"]
    assert derived["excluded"][0]["reason"] == "future_listing"
    assert len(result["results"]["sh-main-stock"]["source_response_hashes"]) == 1


def test_official_bse_refreshes_redirect_and_does_not_infer_listing_date(official_baseline_probe, monkeypatch):
    module, output = official_baseline_probe
    block = {"totalElements": 1, "number": 0, "lastPage": True,
        "content": [{"xxzqdm": "920001", "xxzqjc": "fixture", "fxssrq": "20180101"}]}
    session = OfficialBaselineSession([(302, "page"), (307, "refresh"), (302, "page"), (200, json.dumps([block]))])
    monkeypatch.setattr(module, "_proxy_session", lambda: session)
    result = module.fetch_official_catalog_baselines(output, date(2026, 10, 9), ["bse-listed-stock"])
    assert not result["failures"] and result["results"]["bse-listed-stock"]["rows"] == 1
    assert [c[0] for c in session.calls] == ["GET", "POST", "GET", "POST"]
    assert session.calls[1] == session.calls[3]
    derived = json.loads(Path(result["results"]["bse-listed-stock"]["path"]).read_text())
    assert derived["rows"][0]["source_row"]["fxssrq"] == "20180101"
    assert "listing_date" not in derived["rows"][0]


def test_official_baseline_rejects_incomplete_declared_page(official_baseline_probe, monkeypatch):
    module, output = official_baseline_probe
    session = OfficialBaselineSession([(200, json.dumps({"result": [], "pageHelp": {"total": 2}}))])
    monkeypatch.setattr(module, "_proxy_session", lambda: session)
    result = module.fetch_official_catalog_baselines(output, date(2026, 10, 9), ["sh-fund-directory"])
    assert "incomplete" in result["failures"]["sh-fund-directory"]
    assert len(session.calls) == 1 and not result["results"]


@pytest.mark.parametrize("code,name,expected", [
    ("sh.510300", "沪深300ETF", True), ("sh.530001", "示例ETF", True),
    ("sz.158001", "示例ETF", True), ("sz.159001", "示例ETF", True),
    ("sz.399999", "ETF指数", False), ("sh.000001", "ETF指数", False),
    ("sz.160001", "ETF联接LOF", False), ("sh.501001", "普通LOF", False),
    ("sh.501002", "ETF联接LOF", False), ("sh.508002", "示例ETF", False),
    ("sz.150001", "ETF联接", False),
    ("sh.511600", "华安日日鑫货币H", True), ("sz.159003", "招商快线", True),
    ("sh.510000", "", False), ("bj.920001", "示例ETF", False),
    ("sh.51000", "ETF", False),
])
def test_etf_classification_does_not_include_indexes_or_unconfirmed_funds(code, name, expected):
    assert _is_etf(code, name) is expected


@pytest.mark.parametrize("code", ["sh.600001", "sh.688001", "sz.000001", "sz.001001", "sz.003001", "sz.300001", "sz.301001", "sz.302132"])
def test_stock_selection_keeps_main_growth_and_star_boards(code):
    assert _a_share_code(code, include_transferred=True) == code[3:]


def page(number, codes, total=3, last=False):
    return {"content": [{"hqzqdm": code, "hqzqjc": "证券" + code, "hqjsrq": "20260930"} for code in codes],
            "totalElements": total, "totalPages": 2, "number": number,
            "numberOfElements": len(codes), "lastPage": last}


class CatalogSession:
    def __init__(self, pages, redirect=False):
        self.pages = list(pages)
        self.redirect = redirect
        self.headers = {}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self.response(302, "redirect")

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        if self.redirect:
            self.redirect = False
            return self.response(302, "redirect")
        return self.response(200, "null(" + json.dumps([self.pages.pop(0)]) + ");")

    @staticmethod
    def response(status, body):
        response = requests.Response()
        response.status_code = status
        response._content = body.encode("utf-8")
        response.encoding = "utf-8"
        return response


def test_bse_full_pages_preserve_requests_and_refresh_session_on_redirect(monkeypatch):
    monkeypatch.setattr("stock_data_manage.providers.exchanges.security.sleep", lambda _: None)
    session = CatalogSession([page(0, ["920001", "920002"]), page(1, ["920003"], last=True)], redirect=True)
    result = BseSecurityListProvider(client=session).fetch_snapshot()
    assert len(result.rows) == result.mapping_context["source_total_count"] == 3
    assert all(row["status"] == "unknown" and row["exchange"] == "BSE" for row in result.rows)
    assert result.mapping_context["quote_dates"] == ["2026-09-30"]
    assert result.mapping_context["source_page_count"] == 2
    assert [call[0] for call in session.calls] == ["GET", "POST", "GET", "POST", "POST"]
    posts = [call for call in session.calls if call[0] == "POST"]
    assert posts[0] == posts[1]
    assert posts[-1][2] == {"data": {"page": 1, "type_en": '["B"]', "sortfield": "hqzqdm",
                                   "sorttype": "asc", "xxfcbj_en": "[2]", "zqdm": ""},
                             "timeout": 30, "allow_redirects": False}
    assert session.headers["Referer"] == BseSecurityListProvider.page_url


@pytest.mark.parametrize("problem", ["total_changed", "duplicate", "early_end", "wrong_page", "empty",
                                   "count_changed", "wrong_identity", "boolean_total", "wrong_last", "bad_date"])
def test_bse_incomplete_or_invalid_pages_are_rejected(monkeypatch, problem):
    monkeypatch.setattr("stock_data_manage.providers.exchanges.security.sleep", lambda _: None)
    pages = [page(0, ["920001", "920002"]), page(1, ["920003"], last=True)]
    if problem == "total_changed": pages[1]["totalElements"] = 4
    if problem == "duplicate": pages[1]["content"][0]["hqzqdm"] = "920001"
    if problem == "early_end": pages[0]["lastPage"] = True
    if problem == "wrong_page": pages[0]["number"] = 1
    if problem == "empty": pages[0]["content"] = []
    if problem == "count_changed": pages[0]["numberOfElements"] = 9
    if problem == "wrong_identity": pages[0]["content"][0]["hqzqdm"] = "600001"
    if problem == "boolean_total": pages[0]["totalElements"] = True
    if problem == "wrong_last": pages[1]["lastPage"] = False
    if problem == "bad_date": pages[0]["content"][0]["hqjsrq"] = "20261399"
    with pytest.raises((ProviderContractError, ValueError)):
        BseSecurityListProvider(client=CatalogSession(pages)).fetch_snapshot()


def output_rows(report):
    return json.loads((Path(report["run_directory"]) / report["output"]["path"]).read_text(encoding="utf-8"))


def test_bao_real_archived_snapshot_adds_etfs_without_reclassifying_indexes(tmp_path):
    day = date(2026, 9, 30)
    report = collect_input(input_id="SDA-BOARD-005", config_root=ROOT / "config", data_root=tmp_path,
        context={"request": {"trade_date": day}, "calendar": {"trading_dates": [day]}, "config": {"include_etf": True}},
        mode="replay", replay_manifest=BAO, pacer=RequestPacer(wait=lambda _: None))
    assert report["status"] == "candidate_complete", report.get("error")
    rows = output_rows(report)
    # This snapshot reports stock/ETF counts. Check index exclusion directly
    # against every candidate instead of inventing a third report field.
    assert report["asset_type_counts"] == {"stock": 5224, "etf": 1692}
    assert {row["asset_type"] for row in rows} == {"stock", "etf"}
    assert len(rows) == 6916 and report["excluded_rows"]["row_count"] == 507
    assert any(row["stock_code"] == "302132" and row["asset_type"] == "stock" for row in rows)
    assert any(row["stock_code"] == "511600" and row["asset_type"] == "etf" for row in rows)
    assert any(row["stock_code"] == "159003" and row["asset_type"] == "etf" for row in rows)
    assert len({(r["exchange"], r["stock_code"]) for r in rows}) == len(rows)
    assert not any(row["exchange"] == "XSHE" and row["stock_code"].startswith("399") for row in rows)
    records = prepare_security_publication({"baostock": rows}).records
    assert sum(record.asset_type.value == "etf" for record in records) == 1692
    manifest = json.loads(next((Path(report["run_directory"]) / "sources").rglob("*.manifest.json")).read_text(encoding="utf-8"))
    assert manifest["publication_permitted"] is False


def test_bse_real_archived_catalog_uses_source_capture_day_and_yaml_mapping(tmp_path, monkeypatch):
    monkeypatch.setattr("stock_data_manage.providers.exchanges.security.sleep", lambda _: None)
    report = collect_input(input_id="SECURITY-BSE-001", config_root=ROOT / "config", data_root=tmp_path,
        context={}, mode="replay", replay_manifest=BSE, pacer=RequestPacer(wait=lambda _: None))
    assert report["status"] == "candidate_complete", report.get("error")
    rows = output_rows(report)
    assert len(rows) == report["source_total_count"] == 348
    assert report["source_page_count"] == 18 and report["pagination_completeness_verified"] is True
    assert {row["trade_date"] for row in rows} == {"2026-10-07"}
    assert report["source_metadata"]["quote_dates"] == ["2026-09-30"]
    assert all(row["asset_type"] == "stock" and row["status"] == "unknown" for row in rows)
    manifest = json.loads(next((Path(report["run_directory"]) / "sources").rglob("*.manifest.json")).read_text(encoding="utf-8"))
    assert manifest["publication_permitted"] is False and report["live_http_calls"] == 0
    previous = json.loads((ROOT / "provider_validation/results/security-catalog-20261007/bse-original-result.json").read_text(encoding="utf-8"))
    assert [(row["stock_code"], row["stock_name"]) for row in rows] == [(row["code"], row["name"]) for row in previous["rows"]]
    records = prepare_security_publication({"bse": rows}).records
    assert len(records) == 348
    assert all(record.is_expected_on(date(2026, 10, 7)) for record in records)
    original_manifest = [json.loads(line) for line in BSE.read_text(encoding="utf-8").splitlines()]
    assert len(report["responses"]) == len(original_manifest) == 19
    for original, migrated in zip(original_manifest, report["responses"]):
        assert {k: original[k] for k in ("url", "method", "status_code", "body_sha256")} == {
            k: migrated[k] for k in ("url", "method", "status_code", "body_sha256")}
        assert migrated["request_options"]["request_body_sha256"] == original["request_body_sha256"]
        # The raw archive redacts Cookie; offline replay cannot reconstruct that secret.
        assert {k: v for k, v in migrated["request_headers"].items() if k.lower() != "cookie"} == {
            k: v for k, v in original["request_headers"].items() if k.lower() != "cookie"}


def test_bse_historical_and_symbol_queries_are_rejected_before_request(tmp_path):
    for request in [{"trade_date": date(2026, 9, 30)}, {"symbol": "bj920001"}, {"symbols": ["bj920001"]}]:
        with pytest.raises(ValueError, match="does not support"):
            collect_input(input_id="SECURITY-BSE-001", config_root=ROOT / "config", data_root=tmp_path,
                          context={"request": request}, mode="replay", replay_manifest=BSE)


def test_post_page_cache_and_replay_match_request_body_hash(tmp_path, monkeypatch):
    from urllib.parse import parse_qs
    from stock_data_manage.providers.transport import captured_requests
    from stock_data_manage.storage.raw import RawObjectStore
    network_calls = []

    def fixture_send(session, request, **kwargs):
        number = parse_qs(request.body)["page"][0]
        network_calls.append(number)
        response = CatalogSession.response(200, json.dumps({"page": number}))
        response.headers["Content-Type"] = "application/json"
        response.url = request.url
        return response

    monkeypatch.setattr(requests.Session, "send", fixture_send)
    original = RawObjectStore(tmp_path / "first")
    for store in (original, RawObjectStore(tmp_path / "second")):
        with captured_requests(store, provider="bse", endpoint="catalog", scope={}, code_version="fixture",
                pacer=RequestPacer(wait=lambda _: None), evidence_roots=(tmp_path,), max_age_seconds=3600,
                match_request_body=True) as events:
            with requests.Session() as session:
                assert [session.post("https://fixture.invalid/catalog", data={"page": number}).json()["page"]
                        for number in (0, 1)] == ["0", "1"]
        assert len(events) == 2
    assert network_calls == ["0", "1"]
    # Reverse the archive order: replay still selects the requested page, not the next URL match.
    manifest = original.root / "manifest.ndjson"
    lines = manifest.read_text(encoding="utf-8").splitlines()
    reversed_manifest = original.root / "reversed.ndjson"
    reversed_manifest.write_text("\n".join(reversed(lines)) + "\n", encoding="utf-8")
    with captured_requests(RawObjectStore(tmp_path / "third"), provider="bse", endpoint="catalog", scope={},
            code_version="fixture", pacer=RequestPacer(wait=lambda _: None), replay_manifest=reversed_manifest,
            match_request_body=True) as events:
        with requests.Session() as session:
            assert [session.post("https://fixture.invalid/catalog", data={"page": number}).json()["page"]
                    for number in (0, 1)] == ["0", "1"]
    assert network_calls == ["0", "1"]
    assert [event["source_ref"]["line"] for event in events] == [2, 1]


def test_bse_native_session_preserves_page_cookie_proxy_and_timeouts(monkeypatch):
    from email.message import Message
    from types import SimpleNamespace
    calls = []

    def adapter_send(adapter, request, **kwargs):
        calls.append((request, kwargs))
        if request.method == "GET":
            response = CatalogSession.response(302, "redirect")
            response.headers.update({"Set-Cookie": "fixture_session=fixture; Path=/", "Location": request.url})
        else:
            block = page(0, ["920001"], total=1, last=True)
            block["totalPages"] = 1
            response = CatalogSession.response(200, json.dumps([block]))
        headers = Message()
        if request.method == "GET":
            headers["Set-Cookie"] = response.headers["Set-Cookie"]
        response.raw = SimpleNamespace(_original_response=SimpleNamespace(msg=headers))
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", adapter_send)
    monkeypatch.setenv("STOCK_DATA_HTTP_PROXY", "http://127.0.0.1:20171")
    result = BseSecurityListProvider().fetch_snapshot()
    assert len(result.rows) == 1 and len(calls) == 2
    assert calls[1][0].headers["Cookie"] == "fixture_session=fixture"
    assert calls[0][1]["timeout"] == 20 and calls[1][1]["timeout"] == 30
    assert calls[1][1]["proxies"] == {"http": "http://127.0.0.1:20171", "https": "http://127.0.0.1:20171"}


@pytest.mark.parametrize("data_crosses_midnight", [False, True])
def test_catalog_date_uses_data_pages_and_rejects_pages_across_midnight(tmp_path, monkeypatch, data_crosses_midnight):
    from types import SimpleNamespace
    from stock_data_manage.storage.raw import RawObjectStore
    monkeypatch.setattr("stock_data_manage.providers.exchanges.security.sleep", lambda _: None)
    fixture = RawObjectStore(tmp_path / "time-fixture")
    events = []
    originals = [json.loads(line) for line in BSE.read_text(encoding="utf-8").splitlines()]
    for index, original in enumerate(originals):
        event = fixture.record_response(response=SimpleNamespace(status_code=original["status_code"],
            content=RawObjectStore.read_response(BSE, original), headers=original["response_headers"],
            encoding=original.get("response_encoding")), url=original["url"], method=original["method"],
            request_headers=original["request_headers"], provider="bse", endpoint="fixture-time",
            scope={"synthetic_time_override": True}, code_version="fixture-time", mode="fixture",
            request_options={"request_body_sha256": original["request_body_sha256"]})
        # Page bootstrap is yesterday; the data pages all belong to today unless the last crosses midnight.
        event["fetched_at_utc"] = "2026-10-06T15:59:59+00:00" if original["method"] == "GET" else "2026-10-07T12:00:00+00:00"
        if data_crosses_midnight and index == len(originals) - 1:
            event["fetched_at_utc"] = "2026-10-07T16:00:00+00:00"
        events.append(event)
    manifest = fixture.root / "manifest.ndjson"
    manifest.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
    report = collect_input(input_id="SECURITY-BSE-001", config_root=ROOT / "config", data_root=tmp_path / "candidate",
        context={}, mode="replay", replay_manifest=manifest, pacer=RequestPacer(wait=lambda _: None))
    if data_crosses_midnight:
        assert report["status"] == "failed" and "crossed capture days" in report["error"]
    else:
        assert report["status"] == "candidate_complete", report.get("error")
        assert {row["trade_date"] for row in output_rows(report)} == {"2026-10-07"}


@pytest.mark.parametrize('status', [302, 303, 307, 308])
@pytest.mark.parametrize('cross_host', [False, True])
def test_capture_redirects_keep_native_semantics_and_replay(tmp_path, status, cross_host):
    import requests
    from requests.adapters import HTTPAdapter
    from stock_data_manage.providers.transport import captured_requests, RequestPacer
    from stock_data_manage.storage.raw import RawObjectStore

    calls = []
    target = 'https://other.test/final' if cross_host else 'https://example.test/final'

    class FixtureAdapter(HTTPAdapter):
        def send(self, request, **kwargs):
            calls.append((request.url, request.method, request.body, request.headers.get('Cookie'),
                          request.headers.get('Authorization'), kwargs.get('proxies')))
            response = requests.Response()
            response.request, response.url = request, request.url
            response.status_code = status if request.url.endswith('/first') else 200
            response._content = b'redirect' if response.status_code != 200 else b'final'
            response._content_consumed = True
            if response.status_code != 200:
                response.headers['Location'] = target
                from email.message import Message
                from types import SimpleNamespace
                headers = Message()
                headers['Set-Cookie'] = 'redirect_cookie=value; Path=/'
                response.headers['Set-Cookie'] = headers['Set-Cookie']
                response.raw = SimpleNamespace(_original_response=SimpleNamespace(msg=headers))
            return response

    def run():
        with requests.Session() as session:
            session.mount('https://', FixtureAdapter())
            session.cookies.set('fixture', 'value', domain='example.test', path='/')
            return session.post('https://example.test/first', data='payload',
                                headers={'Authorization': 'fixture-secret'}, proxies={'https': 'http://proxy.test'},
                                timeout=7)

    expected = run()
    original_calls = calls[:]
    calls.clear()
    store = RawObjectStore(tmp_path/'live')
    options = dict(provider='fixture', endpoint='redirect', scope={}, code_version='fixture',
                   pacer=RequestPacer(wait=lambda _: None))
    with captured_requests(store, **options) as events:
        actual = run()
    assert calls == original_calls
    if not cross_host:
        assert 'redirect_cookie=value' in calls[-1][3]
    assert actual.content == expected.content == b'final'
    assert [r.status_code for r in actual.history] == [status]
    assert [e['status_code'] for e in events] == [status, 200]
    assert [RawObjectStore.read_response(store.manifest, e) for e in events] == [b'redirect', b'final']
    calls.clear()
    with captured_requests(RawObjectStore(tmp_path/'replay'), replay_manifest=store.manifest, **options) as replay_events:
        replay = run()
    assert not calls
    assert replay.content == actual.content
    assert [r.status_code for r in replay.history] == [status]
    assert [e['status_code'] for e in replay_events] == [status, 200]


@pytest.mark.parametrize('scenario', ['disabled', 'loop', 'transport_failure'])
def test_capture_redirect_errors_retain_first_hop(tmp_path, scenario):
    import requests
    from requests.adapters import HTTPAdapter
    from stock_data_manage.providers.transport import captured_requests, RequestPacer
    from stock_data_manage.storage.raw import RawObjectStore

    class FixtureAdapter(HTTPAdapter):
        def send(self, request, **kwargs):
            if scenario == 'transport_failure' and request.url.endswith('/final'):
                raise requests.ConnectionError('fixture failure')
            response = requests.Response()
            response.request, response.url = request, request.url
            response.status_code, response._content, response._content_consumed = 302, b'redirect', True
            response.headers['Location'] = '/first' if scenario == 'loop' else '/final'
            return response

    store = RawObjectStore(tmp_path)
    with captured_requests(store, provider='fixture', endpoint='redirect', scope={}, code_version='fixture',
                           pacer=RequestPacer(wait=lambda _: None)) as events:
        with requests.Session() as session:
            session.mount('https://', FixtureAdapter())
            session.max_redirects = 2
            if scenario == 'disabled':
                response = session.get('https://example.test/first', allow_redirects=False)
                assert response.status_code == 302 and not response.history
            else:
                with pytest.raises(requests.TooManyRedirects if scenario == 'loop' else requests.ConnectionError):
                    session.get('https://example.test/first')
    assert events[0]['status_code'] == 302
    assert RawObjectStore.read_response(store.manifest, events[0]) == b'redirect'
    if scenario == 'transport_failure':
        import json
        records = [json.loads(line) for line in store.manifest.read_text(encoding='utf-8').splitlines()]
        assert records[-1]['outcome'] == 'transport_error'
