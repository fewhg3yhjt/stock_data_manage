import json
from typing import Mapping

from stock_data_manage.providers.eastmoney import EastMoneySecurityListProvider
from stock_data_manage.providers.contracts import HttpResponse


class FakeTransport:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, *, params, timeout_seconds):
        self.calls.append(params)
        payload = self.payloads.pop(0) if self.payloads else {"data": {"diff": [], "total": 0}}
        return HttpResponse(200, {"Content-Type": "application/json"}, json.dumps(payload).encode())


def test_eastmoney_security_list_pages_and_maps_market_asset_type():
    transport = FakeTransport([
        {"data": {"total": 2, "diff": [
            {"f12": "600519", "f13": "1", "f14": "贵州茅台"},
            {"f12": "510300", "f13": "1", "f14": "沪深300ETF"},
        ]}},
        {"data": {"total": 2, "diff": []}},
    ])
    provider = EastMoneySecurityListProvider(transport, filters=("test",), page_size=2)
    rows = provider.fetch_security_list()
    assert {row["symbol"] for row in rows} == {"sh600519", "sh510300"}
    assert next(row for row in rows if row["symbol"] == "sh510300")["asset_type"] == "etf"
    assert transport.calls[0]["fs"] == "test"


def test_eastmoney_security_list_deduplicates_same_exchange_symbol():
    row = {"f12": "600519", "f13": "1", "f14": "贵州茅台"}
    provider = EastMoneySecurityListProvider(
        FakeTransport([{"data": {"total": 1, "diff": [row]}}, {"data": {"total": 1, "diff": [row]}}]),
        filters=("a", "b"),
    )
    assert len(provider.fetch_security_list()) == 1
