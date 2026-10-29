from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange, QualityStatus
from stock_data_manage.quality.normalization import NormalizationRule, Normalizer
from stock_data_manage.quality.validation import validate_bar


def test_verified_units_are_limited_by_security_api_adjustment_and_date():
    from pathlib import Path
    import yaml
    root=Path(__file__).resolve().parents[1]/'config'
    rule=yaml.safe_load((root/'normalization/daily_bar.yaml').read_text(encoding='utf-8'))['rules'][0]
    fields=yaml.safe_load((root/'datasets/daily_bar.yaml').read_text(encoding='utf-8'))['candidate']['fields']
    raw={'0':'2026-10-09','1':'10','2':'11','3':'12','4':'9','5':'100','8':'20'}
    context={'instrument_id':'XSHG:600519','source_api':'newfqkline','adjustment':'forward'}
    row=Normalizer.normalize_fields(raw,rule=rule,fields=fields,context=context,allow_pending=True)
    assert row['volume']==Decimal('10000') and row['amount']==Decimal('200000')
    star=Normalizer.normalize_fields(raw,rule=rule,fields=fields,
        context={**context,'instrument_id':'XSHG:688981'},allow_pending=True)
    assert star['volume']==Decimal('100')
    for other_context,other_raw in [({**context,'instrument_id':'BSE:920001'},raw),
        ({**context,'adjustment':'backward'},raw),
        ({**context,'source_api':'fqkline'},raw),(context,{**raw,'0':'2026-10-12'})]:
        other=Normalizer.normalize_fields(other_raw,rule=rule,fields=fields,context=other_context,allow_pending=True)
        assert other['volume'] is None and other['amount'] is None


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
