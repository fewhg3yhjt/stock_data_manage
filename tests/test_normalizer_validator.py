from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange, QualityStatus
from stock_data_manage.quality.normalization import NormalizationRule, Normalizer
from stock_data_manage.quality.validation import validate_bar


def test_normalizer_converts_units_and_start_time_semantics() -> None:
    normalizer = Normalizer(
        [
            NormalizationRule(
                provider="fixture",
                endpoint="minute",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=1,
                volume_multiplier=Decimal("100"),
                amount_multiplier=Decimal("10000"),
                version="rules-v1",
                source_bar_time_semantics="start_time",
                volume_semantics="shares",
            )
        ]
    )
    record = normalizer.normalize_bar(
        {
            "symbol": "sh600519",
            "trade_date": "2026-09-11",
            "bar_time": "2026-09-11T09:30:00",
            "open": "10",
            "high": "10.2",
            "low": "9.9",
            "close": "10.1",
            "volume": "12",
            "amount": "3.5",
        },
        dataset=Dataset.MINUTE_BAR_1M,
        instrument_id="XSHG:600519",
        exchange=Exchange.XSHG,
        asset_type=AssetType.STOCK,
        provider="fixture",
        endpoint="minute",
        adjustment=Adjustment.NONE,
        capability_priority=100,
        capability_version="cap-v1",
        raw_object_path="raw/fixture.json",
        fetch_time=datetime(2026, 9, 11, 9, 32),
        quality_status=QualityStatus.FINAL,
        frequency=1,
    )
    assert record.bar_time == datetime(2026, 9, 11, 9, 31, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert record.volume == Decimal("1200")
    assert record.amount == Decimal("35000.0")
    assert validate_bar(record).valid


def test_validator_rejects_impossible_ohlc(minute_bar) -> None:
    from dataclasses import replace

    invalid = replace(minute_bar, high=Decimal("98"))
    result = validate_bar(invalid)
    assert not result.valid
    assert "high_below_ohlc" in result.errors
