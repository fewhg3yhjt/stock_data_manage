import json

from stock_data_manage.providers.eastmoney import EastMoneyFinancialMainProvider, EastMoneyShareholderCountProvider
from stock_data_manage.providers.contracts import HttpResponse


class FakeTransport:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, *, params, timeout_seconds):
        self.calls.append(params)
        return HttpResponse(200, {"Content-Type": "application/json"}, json.dumps(self.payload).encode())


def test_financial_main_maps_script_fields():
    provider = EastMoneyFinancialMainProvider(FakeTransport({"result": {"data": [{
        "REPORT_DATE": "2026-06-30 00:00:00", "NOTICE_DATE": "2026-08-15 00:00:00",
        "REPORT_TYPE": "中报", "CURRENCY": "CNY", "EPSJB": 35.57, "BPS": 200.98,
        "TOTALOPERATEREVE": 92278072083.21, "PARENTNETPROFIT": 44516880421.86,
        "ROEJQ": 16.75, "MGJYXJJE": 56.54,
    }]}}))
    result = provider.fetch(["sh600519"])
    assert len(result.records) == 1
    assert result.records[0].report_date.startswith("2026-06-30")
    assert result.records[0].parent_net_profit == 44516880421.86


def test_shareholder_count_maps_script_fields():
    provider = EastMoneyShareholderCountProvider(FakeTransport({"result": {"data": [{
        "END_DATE": "2026-06-30 00:00:00", "PRE_END_DATE": "2026-03-31 00:00:00",
        "HOLDER_NUM": 296404, "PRE_HOLDER_NUM": 243159, "HOLDER_NUM_CHANGE": 53245,
        "HOLDER_NUM_RATIO": 21.897, "HOLD_NOTICE_DATE": "2026-08-15 00:00:00",
        "AVG_MARKET_CAP": 4999794.99, "AVG_HOLD_NUM": 4217.49,
        "TOTAL_MARKET_CAP": 1481959237169.49, "CHANGE_REASON": "资产重组",
    }]}}))
    result = provider.fetch(["sh600519"])
    assert result.records[0].holder_count == 296404
    assert result.records[0].change_reason == "资产重组"
