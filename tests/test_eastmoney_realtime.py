import json

from stock_data_manage.providers.eastmoney.realtime import EastMoneyRealtimeQuoteProvider
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
