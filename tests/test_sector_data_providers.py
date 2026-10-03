from contextlib import contextmanager
from datetime import date, datetime, timezone

import json
import pytest

from stock_data_manage.providers.akshare.boards import AkShareBoardProvider
from stock_data_manage.providers.baostock.industry import BaoStockIndustryMembershipProvider
from stock_data_manage.providers.baostock import industry as industry_module
from stock_data_manage.providers.contracts import FailureClass, ProviderContractError


class Frame:
    def __init__(self, columns, rows):
        self.columns = columns
        self._rows = rows

    def itertuples(self, *, index=False, name=None):
        return iter(self._rows)


class FakeAkShare:
    def __init__(self):
        self.calls = []

    def stock_board_industry_name_ths(self):
        self.calls.append(("stock_board_industry_name_ths", {}))
        return Frame(["name", "code"], [("半导体", "881121")])

    def stock_board_industry_index_ths(self, **kwargs):
        self.calls.append(("stock_board_industry_index_ths", kwargs))
        return Frame(
            ["日期", "开盘价", "最高价", "最低价", "收盘价", "成交量", "成交额"],
            [("2026-09-30", 10, 11, 9, 10.5, 1000, 2000)],
        )

    def stock_fund_flow_industry(self, **kwargs):
        self.calls.append(("stock_fund_flow_industry", kwargs))
        return self._flow_frame()

    def stock_fund_flow_concept(self, **kwargs):
        self.calls.append(("stock_fund_flow_concept", kwargs))
        return self._flow_frame()

    @staticmethod
    def _flow_frame():
        return Frame(
            ["序号", "行业", "行业指数", "行业-涨跌幅", "流入资金", "流出资金", "净额", "公司家数", "领涨股", "领涨股-涨跌幅", "当前价"],
            [(1, "半导体", 100, 1.2, 20, 15, 5, 100, "样本股", 5.1, 10.0)],
        )


def test_akshare_ths_industry_directory_and_history_preserve_request_contract():
    client = FakeAkShare()
    provider = AkShareBoardProvider(client=client)

    directory = provider.fetch_industry_list()
    result = provider.fetch_industry_daily(
        "半导体", date(2026, 9, 1), date(2026, 10, 2), board_code="881121"
    )

    assert directory.rows == (
        {"board_type": "industry", "board_name": "半导体", "board_code": "881121", "source": "ths"},
    )
    assert result.rows[0]["trade_date"] == "2026-09-30"
    assert result.rows[0]["board_code"] == "881121"
    assert client.calls[1] == (
        "stock_board_industry_index_ths",
        {"symbol": "半导体", "start_date": "20260901", "end_date": "20261002"},
    )


def test_akshare_board_fund_flow_is_timestamped_and_keeps_source_units():
    client = FakeAkShare()
    provider = AkShareBoardProvider(client=client)
    snapshot_at = datetime(2026, 10, 2, 15, tzinfo=timezone.utc)

    result = provider.fetch_fund_flow("industry", period="即时", snapshot_at=snapshot_at)

    assert result.rows[0]["net_inflow"] == 5
    assert result.rows[0]["snapshot_at"] == snapshot_at.isoformat()
    assert result.units == ("money:source_display_unit_unconfirmed", "percentage:percent")
    assert client.calls == [("stock_fund_flow_industry", {"symbol": "即时"})]


def test_existing_board_method_resolves_directory_and_keeps_period_return_contract():
    client = FakeAkShare()
    provider = AkShareBoardProvider(client=client)
    daily = provider.fetch_industry_daily("半导体", date(2026, 9, 1), date(2026, 10, 2))
    assert daily.rows[0]["board_code"] == "881121"
    assert "日期" in daily.source_rows[0]
    def period_frame(**kwargs):
        assert kwargs == {"symbol": "5日排行"}
        return Frame(["序号", "行业", "公司家数", "行业指数", "阶段涨跌幅", "流入资金", "流出资金", "净额"],
                     [(1, "半导体", 100, 120, 1.5, 20, 15, 5)])
    client.stock_fund_flow_industry = period_frame
    stamp = datetime(2026, 10, 2, tzinfo=timezone.utc)
    result = provider.fetch_fund_flow("industry", period="5日排行", snapshot_at=stamp)
    assert result.rows == ({"board_type": "industry", "period": "5日排行", "rank": 1, "board_name": "半导体",
                            "company_count": 100, "index_value": 120, "change_pct": 1.5, "money_inflow": 20,
                            "money_outflow": 15, "net_inflow": 5, "snapshot_at": stamp.isoformat(), "source": "ths"},)


class FakeResult:
    error_code = "0"
    error_msg = ""

    def __init__(self, fields, rows):
        self.fields = fields
        self._rows = iter(rows)

    def next(self):
        try:
            self._current = next(self._rows)
            return True
        except StopIteration:
            return False

    def get_row_data(self):
        return self._current


class FakeBaoStock:
    def __init__(self):
        self.calls = []

    def query_all_stock(self, **kwargs):
        self.calls.append(("query_all_stock", kwargs))
        return FakeResult(
            ["code", "code_name", "tradeStatus"],
            [["sh.600519", "贵州茅台", "1"]],
        )

    def query_stock_industry(self, **kwargs):
        self.calls.append(("query_stock_industry", kwargs))
        return FakeResult(
            ["code", "code_name", "industry", "industryClassification", "updateDate"],
            [["sh.600519", "贵州茅台", "酒、饮料和精制茶制造业", "证监会行业分类", "2026-09-28"]],
        )


class CaptureArchive:
    def __init__(self):
        self.payloads = []
        self.scopes = []

    @contextmanager
    def scope(self, **metadata):
        self.scopes.append(metadata)
        yield

    def store_source_payload(self, payload, **kwargs):
        self.payloads.append((payload, kwargs))


def test_baostock_csrc_membership_archives_sdk_rows_and_normalizes(monkeypatch):
    fake = FakeBaoStock()

    @contextmanager
    def fake_session():
        yield fake

    monkeypatch.setattr(industry_module, "logged_in_session", fake_session)
    archive = CaptureArchive()
    result = BaoStockIndustryMembershipProvider().fetch_snapshot(
        date(2026, 9, 30), ["sh600519"], raw_archive=archive
    )

    assert result.rows[0] == {
        "trade_date": "2026-09-30",
        "stock_code": "600519",
        "stock_name": "贵州茅台",
        "exchange": "XSHG",
        "status": "active",
        "industry_name": "酒、饮料和精制茶制造业",
        "classification": "证监会行业分类",
        "classification_update_date": "2026-09-28",
        "source": "baostock",
    }
    assert archive.payloads
    assert len(archive.payloads) == 2
    assert any(b"industryClassification" in item[0] for item in archive.payloads)
    assert all(item[1]["representation"].startswith("SDK-decoded") for item in archive.payloads)
    assert result.coverage_denominator == 1
    assert result.missing_symbols == ()
    assert fake.calls == [
        ("query_all_stock", {"day": "2026-09-30"}),
        ("query_stock_industry", {"date": "2026-09-30"}),
    ]


def test_baostock_provider_archives_sdk_failure_before_raising(monkeypatch):
    class FailedResult:
        error_code = "10001001"
        error_msg = "connection failed"
        fields = []

    class FailedBaoStock:
        def query_all_stock(self, **kwargs):
            return FailedResult()

    @contextmanager
    def failed_session():
        yield FailedBaoStock()

    monkeypatch.setattr(industry_module, "logged_in_session", failed_session)
    archive = CaptureArchive()

    with pytest.raises(ProviderContractError) as error:
        BaoStockIndustryMembershipProvider().fetch_snapshot(
            date(2026, 9, 30), raw_archive=archive
        )

    assert error.value.failure_class is FailureClass.CONNECTION
    assert len(archive.payloads) == 1
    saved = json.loads(archive.payloads[0][0])
    assert saved["result"]["error_code"] == "10001001"
