from dataclasses import dataclass
from datetime import date

from stock_data_manage.domain import AssetType, Exchange
from stock_data_manage.service.instruments import SecurityMasterStore, stable_instrument_id
from stock_data_manage.service.instruments_update import SecurityMasterUpdater
from stock_data_manage.service.calendar import CalendarDay, TradingCalendarStore
from stock_data_manage.service.calendar_update import TradingCalendarUpdater


@dataclass
class SecurityFixture:
    name: str
    rows: list[dict]
    error: Exception | None = None

    def fetch_security_list(self):
        if self.error:
            raise self.error
        return self.rows


@dataclass
class CalendarFixture:
    name: str
    priority: int
    days: list[CalendarDay]
    error: Exception | None = None

    def fetch_calendar(self, start, end):
        if self.error:
            raise self.error
        return self.days


def test_security_master_update_does_not_delist_when_one_source_is_empty(tmp_path) -> None:
    instrument_id = stable_instrument_id(Exchange.XSHG, "sh600519")
    with SecurityMasterStore(str(tmp_path / "security.duckdb")) as store:
        updater = SecurityMasterUpdater(
            store,
            [
                SecurityFixture(
                    "sina",
                    [{"symbol": "sh600519", "exchange": "XSHG", "asset_type": "stock", "name": "贵州茅台"}],
                ),
                SecurityFixture("eastmoney", []),
            ],
        )
        first = updater.update(effective_date=date(2026, 9, 13))
        assert first.source_errors == {}
        assert store.all()[0].instrument_id == instrument_id
        second = SecurityMasterUpdater(
            store,
            [SecurityFixture("sina", [], RuntimeError("temporarily missing"))],
        ).update(effective_date=date(2026, 9, 14))
        assert second.source_errors == {"sina": "temporarily missing"}
        assert store.all()[0].delist_date is None


def test_calendar_update_keeps_previous_day_when_source_fails(tmp_path) -> None:
    trade_day = date(2026, 9, 11)
    with TradingCalendarStore(str(tmp_path / "calendar.duckdb")) as store:
        updater = TradingCalendarUpdater(
            store,
            [CalendarFixture("exchange", 100, [CalendarDay(trade_day, True, "exchange", 100)])],
        )
        updater.update(start=trade_day, end=trade_day)
        failed = TradingCalendarUpdater(
            store,
            [CalendarFixture("sina", 10, [], RuntimeError("network"))],
        ).update(start=trade_day, end=trade_day)
        assert failed.source_errors == {"sina": "network"}
        assert store.is_trading_day(trade_day) is True


def test_calendar_update_does_not_create_holiday_from_single_empty_source(tmp_path) -> None:
    trade_day = date(2026, 9, 12)
    with TradingCalendarStore(str(tmp_path / "calendar.duckdb")) as store:
        result = TradingCalendarUpdater(
            store, [CalendarFixture("sina", 10, [])]
        ).update(start=trade_day, end=trade_day)
        assert result.merge.days == ()
        assert store.is_trading_day(trade_day) is None
