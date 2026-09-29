from contextlib import contextmanager
from datetime import date, datetime, timezone

from stock_data_manage.domain import Adjustment, AssetType
from stock_data_manage.providers.baostock.daily import BaoStockDailyProvider
from stock_data_manage.providers.baostock.minute import BaoStockMinuteProvider
from stock_data_manage.providers.baostock import daily as daily_module
from stock_data_manage.providers.baostock import minute as minute_module


class FakeResult:
    error_code = "0"
    error_msg = ""
    fields = [
        "date", "code", "open", "high", "low", "close", "preclose",
        "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg",
    ]

    def __init__(self, rows):
        self._rows = iter(rows)

    def next(self):
        try:
            self._row = next(self._rows)
            return True
        except StopIteration:
            return False

    def get_row_data(self):
        return self._row


class FakeMinuteResult(FakeResult):
    fields = ["date", "time", "code", "open", "high", "low", "close", "volume", "amount", "adjustflag"]


class FakeBaoStock:
    def __init__(self):
        self.calls = []

    def query_history_k_data_plus(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if kwargs["frequency"] == "d":
            return FakeResult([
                ["2026-09-11", "sh.600519", "10", "11", "9", "10.5", "9.8", "100", "1000", "3", "1", "1", "2"]
            ])
        return FakeMinuteResult([
            ["2026-09-11", "093100", "sh.600519", "10", "11", "9", "10.5", "100", "1000", "3"]
        ])


@contextmanager
def fake_session(client):
    yield client


def test_baostock_daily_normalizes_rows_and_adjustflag(monkeypatch):
    client = FakeBaoStock()
    monkeypatch.setattr(daily_module, "logged_in_session", lambda: fake_session(client))
    provider = BaoStockDailyProvider(adjustment=Adjustment.FORWARD)
    result = provider.fetch_daily(["sh600519"], date(2026, 9, 11))
    assert result.rows[0]["close"] == "10.5"
    assert result.adjustment is Adjustment.FORWARD
    assert client.calls[0][1]["adjustflag"] == "2"


def test_baostock_5m_normalizes_rows_and_closes_to_response_shape(monkeypatch):
    client = FakeBaoStock()
    monkeypatch.setattr(minute_module, "logged_in_session", lambda: fake_session(client))
    provider = BaoStockMinuteProvider()
    result = provider.fetch_realtime_minute(["sh600519"], datetime(2026, 9, 11, 10, tzinfo=timezone.utc))
    assert result.rows[0]["bar_time"] == "2026-09-11 093100"
    assert result.units == ("volume:share", "amount:cny")
    assert client.calls[0][1]["frequency"] == "5"
