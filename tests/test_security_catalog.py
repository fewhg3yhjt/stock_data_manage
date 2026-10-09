"""Source catalog contracts: stock/ETF selection, BSE paging and retained evidence."""
from datetime import date
import json
from pathlib import Path

import pytest
import requests

from stock_data_manage.pipeline.inputs import collect_input
from stock_data_manage.providers.baostock.industry import _a_share_code, _is_etf
from stock_data_manage.providers.contracts import ProviderContractError
from stock_data_manage.providers.exchanges.security import BseSecurityListProvider
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.service.instruments_update import prepare_security_publication


ROOT = Path(__file__).resolve().parents[1]
BAO = ROOT / "provider_validation/results/live-probes/baostock-industry-20260930-20261003/_raw/baostock-industry/manifest.ndjson"
BSE = ROOT / "provider_validation/results/raw/security-catalog-bse-20261007/manifest.ndjson"


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
    assert report["asset_type_counts"] == {"stock": 5224, "etf": 1692}
    assert len(rows) == 6916 and report["excluded_rows"]["row_count"] == 507
    assert any(row["stock_code"] == "302132" and row["asset_type"] == "stock" for row in rows)
    assert any(row["stock_code"] == "511600" and row["asset_type"] == "etf" for row in rows)
    assert any(row["stock_code"] == "159003" and row["asset_type"] == "etf" for row in rows)
    assert len({(r["exchange"], r["stock_code"]) for r in rows}) == len(rows)
    assert not any(row["exchange"] == "XSHE" and row["stock_code"].startswith("399") for row in rows)
    records = prepare_security_publication({"baostock": rows}).records
    assert sum(record.asset_type.value == "etf" for record in records) == 1692
    manifest = json.loads((Path(report["run_directory"]) / "manifest.json").read_text(encoding="utf-8"))
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
    manifest = json.loads((Path(report["run_directory"]) / "manifest.json").read_text(encoding="utf-8"))
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
