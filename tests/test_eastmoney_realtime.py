import json

from stock_data_manage.providers.eastmoney.realtime import EastMoneyRealtimeQuoteProvider, EastMoneyRequestsTransport
from stock_data_manage.providers.contracts import HttpResponse


class FakeTransport:
    def __init__(self, payloads):
        self.payloads = list(payloads)

    def get(self, url, *, params, timeout_seconds):
        return HttpResponse(200, {"Content-Type": "application/json"}, json.dumps(self.payloads.pop(0)).encode())


def test_single_quote_maps_eastmoney_fields():
    provider = EastMoneyRealtimeQuoteProvider(FakeTransport([{"data": {
        "f57": "600519", "f58": "贵州茅台", "f43": 1236.22, "f44": 1245.87,
        "f45": 1230.88, "f46": 1244.6, "f47": 23963, "f48": 2963178177,
        "f60": 1243.88, "f116": 1545375876788.22, "f117": 1545375876788.22,
        "f162": 17.36, "f167": 6.15, "f168": 0.19, "f169": -7.66, "f170": -0.62,
    }}]), endpoint="single_quote")
    result = provider.fetch_quotes(["sh600519"])
    assert result.records[0].symbol == "sh600519"
    assert result.records[0].price == 1236.22
    assert result.records[0].amount == 2963178177


def test_batch_quote_maps_diff_rows():
    provider = EastMoneyRealtimeQuoteProvider(FakeTransport([{"data": {"diff": [
        {"f12": "600519", "f13": 1, "f14": "贵州茅台", "f2": 1236.22, "f3": -0.62, "f4": -7.66},
        {"f12": "000001", "f13": 0, "f14": "平安银行", "f2": 10, "f3": 0.1, "f4": 0.01},
    ]}}]), endpoint="batch_quote")
    result = provider.fetch_quotes(["sh600519", "sz000001"])
    assert {record.symbol for record in result.records} == {"sh600519", "sz000001"}


def test_intraday_trend_maps_csv_rows():
    provider = EastMoneyRealtimeQuoteProvider(FakeTransport([{"data": {"trends": [
        "2026-09-29 14:47,1236.42,1236.54,1236.50,1236.22,22,2720021.00,0"
    ]}}]), endpoint="intraday_trend")
    result = provider.fetch_trends(["sh600519"])
    assert len(result.records) == 1
    assert result.records[0].price == 1236.42


def test_eastmoney_requests_transport_matches_probe_session(monkeypatch):
    captured = {}

    class FakeSession:
        def __init__(self):
            self.trust_env = True
            self.headers = {}

        def mount(self, scheme, adapter):
            captured.setdefault("mounts", []).append((scheme, adapter))

        def get(self, url, *, params, timeout):
            captured["url"] = url
            captured["params"] = params
            captured["timeout"] = timeout
            return type("Response", (), {"status_code": 200, "headers": {"Content-Type": "application/json"}, "content": b'{"data": {}}'})()

    monkeypatch.setattr("stock_data_manage.providers.eastmoney.realtime.requests.Session", FakeSession)
    transport = EastMoneyRequestsTransport()
    transport.get("https://push2.eastmoney.com/api/qt/stock/get", params={"secid": "1.600519"}, timeout_seconds=12)
    assert transport.session.trust_env is False
    assert "Mozilla/5.0" in transport.session.headers["User-Agent"]
    assert transport.session.headers["Referer"] == "https://quote.eastmoney.com/"
    assert captured["timeout"] == (5, 12)


def test_realtime_provider_falls_back_to_next_host_on_empty_business_data():
    class CandidateTransport:
        def __init__(self):
            self.urls = []

        def get(self, url, *, params, timeout_seconds):
            self.urls.append(url)
            body = b'{"data": {}}' if "push2.eastmoney.com" in url else b'{"data": {"f57": "600519", "f13": 1, "f43": 10}}'
            return type("Response", (), {"status_code": 200, "headers": {}, "body": body, "text": body.decode()})()

    transport = CandidateTransport()
    result = EastMoneyRealtimeQuoteProvider(transport, endpoint="single_quote").fetch_quotes(["sh600519"])
    assert len(result.records) == 1
    assert transport.urls == [
        "https://push2.eastmoney.com/api/qt/stock/get",
        "https://push2delay.eastmoney.com/api/qt/stock/get",
    ]
