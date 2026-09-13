from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from stock_data_manage.domain import Adjustment, BarRecord, Dataset, QualityStatus


SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.fixture
def minute_bar() -> BarRecord:
    return BarRecord(
        dataset=Dataset.MINUTE_BAR_1M,
        instrument_id="XSHG:600519",
        source_symbol="sh600519",
        trade_date=date(2026, 9, 11),
        bar_time=datetime(2026, 9, 11, 9, 31, tzinfo=SHANGHAI),
        interval_minutes=1,
        adjustment=Adjustment.NONE,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1000"),
        amount=Decimal("100500"),
        source_provider="tencent",
        endpoint="native_1m",
        source_method="realtime_minute",
        quality_status=QualityStatus.PROVISIONAL,
        field_level="full_bar",
        capability_priority=100,
        capability_version="2026-09-13",
        raw_object_path="raw/tencent/a.json",
        fetch_time=datetime(2026, 9, 11, 9, 32, 2, tzinfo=SHANGHAI),
        freshness_class="realtime",
        source_delay_seconds=2,
    )


@pytest.fixture
def final_minute_bar(minute_bar: BarRecord) -> BarRecord:
    return replace(
        minute_bar,
        source_provider="tdx",
        endpoint="delayed_1m",
        source_method="minute_history",
        quality_status=QualityStatus.FINAL,
        capability_priority=80,
        fetch_time=datetime(2026, 9, 11, 16, 0, tzinfo=SHANGHAI),
        freshness_class="delayed",
        source_delay_seconds=900,
    )

