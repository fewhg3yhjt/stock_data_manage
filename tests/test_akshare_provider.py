from datetime import date

import pytest

from stock_data_manage.domain import Adjustment, AssetType
from stock_data_manage.providers.akshare.daily import AkShareDailyProvider
from stock_data_manage.providers.contracts import ProviderContractError


class FakeAkShare:
    def __init__(self):
        self.calls = []

    def stock_zh_a_hist(self, **kwargs):
        self.calls.append(("stock_zh_a_hist", kwargs))
        return [{"日期": "2026-09-11", "开盘": "10", "最高": "11", "最低": "9", "收盘": "10.5", "成交量": "100", "成交额": "1000"}]

    def fund_etf_hist_em(self, **kwargs):
        self.calls.append(("fund_etf_hist_em", kwargs))
        return [{"日期": "2026-09-11", "开盘": "1", "最高": "1.1", "最低": "0.9", "收盘": "1.0", "成交量": "100", "成交额": "1000"}]

    def fund_lof_hist_em(self, **kwargs):
        self.calls.append(("fund_lof_hist_em", kwargs))
        return [{"日期": "2026-09-11", "开盘": "1", "最高": "1.1", "最低": "0.9", "收盘": "1.0", "成交量": "100", "成交额": "1000"}]

    def stock_zh_index_daily_em(self, **kwargs):
        self.calls.append(("stock_zh_index_daily_em", kwargs))
        return [{"date": "2026-09-11", "open": "3000", "high": "3010", "low": "2990", "close": "3005", "volume": "100", "amount": "1000"}]


@pytest.mark.parametrize(
    ("asset_type", "symbol", "method"),
    [
        (AssetType.STOCK, "sh600519", "stock_zh_a_hist"),
        (AssetType.ETF, "sh510300", "fund_etf_hist_em"),
        (AssetType.LOF, "sh501018", "fund_lof_hist_em"),
        (AssetType.INDEX, "sh000001", "stock_zh_index_daily_em"),
    ],
)
def test_akshare_daily_provider_maps_function_and_rows(asset_type, symbol, method):
    client = FakeAkShare()
    provider = AkShareDailyProvider(asset_type=asset_type, client=client)
    result = provider.fetch_daily([symbol], date(2026, 9, 11))
    assert len(result.rows) == 1
    assert client.calls[0][0] == method


def test_akshare_index_rejects_adjustment():
    with pytest.raises(ProviderContractError):
        AkShareDailyProvider(asset_type=AssetType.INDEX, adjustment=Adjustment.FORWARD, client=FakeAkShare()).fetch_daily(
            ["sh000001"], date(2026, 9, 11)
        )
