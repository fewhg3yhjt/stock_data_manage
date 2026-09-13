from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_data_manage.domain import QualityStatus
from stock_data_manage.hot_store import HotMinuteStore
from stock_data_manage.resample import resample_1m_to_5m


SHANGHAI = ZoneInfo("Asia/Shanghai")


def test_resample_five_complete_minutes(minute_bar) -> None:
    records = []
    for index in range(5):
        records.append(
            replace(
                minute_bar,
                bar_time=datetime(2026, 9, 11, 9, 31 + index, tzinfo=SHANGHAI),
                open=Decimal(100 + index),
                high=Decimal(101 + index),
                low=Decimal(99 + index),
                close=Decimal("100.5") + index,
                volume=Decimal(10 + index),
                amount=Decimal(100 + index),
                quality_status=QualityStatus.FINAL,
            )
        )
    result = resample_1m_to_5m(records)
    assert len(result) == 1
    bar = result[0]
    assert bar.bar_time.minute == 35
    assert (bar.open, bar.high, bar.low, bar.close) == (
        Decimal("100"),
        Decimal("105"),
        Decimal("99"),
        Decimal("104.5"),
    )
    assert bar.volume == Decimal("60")
    assert bar.amount == Decimal("510")


def test_resample_does_not_bridge_missing_minute(minute_bar) -> None:
    records = [
        replace(minute_bar, bar_time=datetime(2026, 9, 11, 9, minute, tzinfo=SHANGHAI))
        for minute in (31, 32, 34, 35, 36)
    ]
    assert resample_1m_to_5m(records) == []


def test_resample_requires_exchange_aligned_five_minute_window(minute_bar) -> None:
    records = [
        replace(minute_bar, bar_time=datetime(2026, 9, 11, 9, minute, tzinfo=SHANGHAI))
        for minute in (32, 33, 34, 35, 36)
    ]
    assert resample_1m_to_5m(records) == []


def test_hot_store_upserts_higher_quality_and_queries_immediately(
    tmp_path, minute_bar, final_minute_bar
) -> None:
    path = tmp_path / "minute_hot.db"
    with HotMinuteStore(path) as store:
        assert store.upsert(final_minute_bar)
        assert not store.upsert(minute_bar)
        rows = store.query(
            minute_bar.instrument_id,
            datetime(2026, 9, 11, 9, 30, tzinfo=SHANGHAI),
            datetime(2026, 9, 11, 9, 32, tzinfo=SHANGHAI),
        )
        assert len(rows) == 1
        assert rows[0].quality_status is QualityStatus.FINAL
        assert rows[0].source_provider == "tdx"
    with HotMinuteStore(path) as reopened:
        assert len(
            reopened.query(
                minute_bar.instrument_id,
                datetime(2026, 9, 11, 9, 30, tzinfo=SHANGHAI),
                datetime(2026, 9, 11, 9, 32, tzinfo=SHANGHAI),
            )
        ) == 1
