import json

from stock_data_manage.providers.eastmoney import EastMoneyStockFundFlowProvider
from stock_data_manage.providers.contracts import HttpResponse


class FakeTransport:
    def get(self, url, *, params, timeout_seconds):
        payload = {"data": {"klines": [
            "2026-09-28,-198158880.0,-343470.0,198502352.0,-83004848.0,-115154032.0,-5.68,-0.01,5.69,-2.38,-3.30,1243.88,0.56"
        ]}}
        return HttpResponse(200, {"Content-Type": "application/json"}, json.dumps(payload).encode())


def test_eastmoney_stock_fund_flow_maps_csv_values():
    result = EastMoneyStockFundFlowProvider(FakeTransport()).fetch_daily(["sh600519"])
    assert len(result.records) == 1
    assert result.records[0].trade_date == "2026-09-28"
    assert result.records[0].main_net_inflow == -198158880.0
    assert result.records[0].close == 1243.88
