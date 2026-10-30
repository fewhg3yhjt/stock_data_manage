from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
import pytest

from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange, QualityStatus
from stock_data_manage.quality.normalization import NormalizationRule, Normalizer
from stock_data_manage.quality.validation import validate_bar


def daily_unit_fixture():
    from pathlib import Path
    import yaml
    root=Path(__file__).resolve().parents[1]/'config'
    rule=yaml.safe_load((root/'normalization/daily_bar.yaml').read_text(encoding='utf-8'))['rules'][0]
    fields=yaml.safe_load((root/'datasets/daily_bar.yaml').read_text(encoding='utf-8'))['candidate']['fields']
    raw={'0':'2026-10-30','1':'10','2':'11','3':'12','4':'9','5':'100','8':'20'}
    context={'provider':'tencent','endpoint':'kline_daily','exchange':'XSHG','asset_type':'stock',
        'instrument_id':'XSHG:600519','source_api':'newfqkline','source_bar_key':'qfqday','adjustment':'forward'}
    return rule,fields,raw,context


@pytest.mark.parametrize('identity,asset,series,adjustment,multiplier', [
    ('XSHG:601318','stock','day','none',100),
    ('XSHE:001001','stock','qfqday','forward',100),
    ('XSHE:300001','stock','hfqday','backward',100),
    ('XSHG:688001','stock','day','none',1),
    ('XSHG:510500','etf','qfqday','forward',100),
    ('XSHG:511880','etf','qfqday','forward',100),
    ('XSHE:159900','etf','qfqday','forward',100),
    ('BSE:920001','stock','day','none',None),
    ('XSHG:688001','stock','qfqday','forward',None),
    ('XSHG:688001','stock','hfqday','backward',None),
    ('XSHG:510500','etf','day','none',None),
    ('XSHG:510500','etf','hfqday','backward',None),
    ('XSHG:560001','etf','qfqday','forward',None),
    ('XSHG:600519','etf','qfqday','forward',None),
    ('XSHG:600519','index','qfqday','forward',None),
    ('XSHG:600519','lof','qfqday','forward',None),
    ('XSHG:689001','stock','day','none',None),
])
def test_unit_contracts_follow_asset_market_segment_and_actual_series(identity,asset,series,adjustment,multiplier):
    rule,fields,raw,context=daily_unit_fixture()
    context.update(instrument_id=identity,exchange=identity.split(':')[0],asset_type=asset,
        source_bar_key=series,adjustment=adjustment)
    row=Normalizer.normalize_fields(raw,rule=rule,fields=fields,context=context,allow_pending=True)
    if multiplier is None:
        assert row['volume'] is None and row['amount'] is None
    else:
        assert row['volume']==Decimal('100')*multiplier and row['amount']==Decimal('200000')


def test_unit_contract_reuses_dates_without_mutating_source_values():
    rule,fields,raw,context=daily_unit_fixture()
    for day in ('2026-10-09','2026-10-28','2026-10-30'):
        row=Normalizer.normalize_fields({**raw,'0':day},rule=rule,fields=fields,context=context,allow_pending=True)
        assert row['volume']==10000 and row['amount']==200000
    assert raw['5']=='100' and raw['8']=='20'


@pytest.mark.parametrize('units', [ {'volume':'CNY','amount':'ten_thousand_CNY'},
    {'volume':[],'amount':'ten_thousand_CNY'}, {'volume':'share','amount':'ten_thousand_CNY'} ])
def test_source_unit_description_cannot_change_or_mislabel_conversion(units):
    from stock_data_manage.quality.normalization import NormalizationError
    rule,fields,raw,context=daily_unit_fixture()
    rule['unit_contracts'][0]['source_units']=units
    with pytest.raises(NormalizationError):
        Normalizer.normalize_fields(raw,rule=rule,fields=fields,context=context,allow_pending=True)


@pytest.mark.parametrize('change', [
    {'provider':'other'}, {'endpoint':'other'}, {'source_api':'fqkline'}, {'asset_type':None},
    {'exchange':'XSHE'}, {'source_bar_key':'day'}, {'adjustment':'backward'},
])
def test_unit_contract_does_not_guess_missing_or_conflicting_source_context(change):
    rule,fields,raw,context=daily_unit_fixture()
    row=Normalizer.normalize_fields(raw,rule=rule,fields=fields,context={**context,**change},allow_pending=True)
    assert row['volume'] is None and row['amount'] is None


@pytest.mark.parametrize('damage', ['zero','negative','nan','infinite','wrong_series','missing_scope','bad_prefix','wrong_flags','overlap'])
def test_invalid_or_ambiguous_unit_contracts_fail_explicitly(damage):
    from copy import deepcopy
    from stock_data_manage.quality.normalization import NormalizationError
    rule,fields,raw,context=daily_unit_fixture()
    c=rule['unit_contracts'][0]
    if damage in {'zero','negative','nan','infinite'}:
        c['multipliers']['volume']={'zero':'0','negative':'-1','nan':'NaN','infinite':'Infinity'}[damage]
    elif damage=='wrong_series': c['series_adjustments']={'unknown':None}
    elif damage=='missing_scope': del c['asset_type']
    elif damage=='bad_prefix': c['instrument_prefixes']=['XSHG:600519']
    elif damage=='wrong_flags': c['precision_flags']['open']='wrong'
    else:
        duplicate=deepcopy(c);duplicate['id']='overlapping';rule['unit_contracts'].append(duplicate)
    with pytest.raises(NormalizationError):
        Normalizer.normalize_fields(raw,rule=rule,fields=fields,context=context,allow_pending=True)


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
