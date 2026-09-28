from datetime import date, datetime, timezone
from decimal import Decimal

from stock_data_manage.domain import AssetType, Exchange
from stock_data_manage.quality.normalization import NormalizationRule, Normalizer
from stock_data_manage.pipeline.snapshot_daily import SnapshotDailyBuilder


def test_snapshot_daily_builder_creates_provisional_bar_with_rule_units() -> None:
    normalizer = Normalizer(
        [
            NormalizationRule(
                provider="tencent",
                endpoint="bulk_snapshot",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=None,
                volume_multiplier=Decimal("100"),
                amount_multiplier=Decimal("10000"),
                version="tencent-snapshot-v1",
                volume_semantics="snapshot_cumulative",
            )
        ]
    )
    builder = SnapshotDailyBuilder(
        normalizer,
        lambda symbol: ("600519", Exchange.XSHG, AssetType.STOCK)
        if symbol == "sh600519"
        else None,
    )
    result = builder.build(
        [
            {
                "symbol": "sh600519",
                "trade_date": "2026-09-11",
                "open": "1285.15",
                "high": "1286.15",
                "low": "1263.01",
                "close": "1275.16",
                "volume": "34801",
                "amount": "445001.2",
            }
        ],
        trade_date=date(2026, 9, 11),
        provider="tencent",
        endpoint="bulk_snapshot",
        capability_priority=100,
        capability_version="tencent-snapshot-v1",
        raw_object_path="snapshot/2026-09-11.json",
        fetch_time=datetime(2026, 9, 11, 15, 16, tzinfo=timezone.utc),
    )
    assert len(result.records) == 1
    assert result.records[0].quality_status.value == "provisional"
    assert result.records[0].volume == Decimal("3480100")
    assert result.records[0].amount == Decimal("4450012000")


def test_snapshot_daily_builder_rejects_wrong_date_and_missing_identity() -> None:
    builder = SnapshotDailyBuilder(Normalizer([]), lambda _symbol: None)
    result = builder.build(
        [
            {"symbol": "sh600519", "trade_date": "2026-09-10"},
            {"symbol": "sz000001", "trade_date": "2026-09-11"},
        ],
        trade_date=date(2026, 9, 11),
        provider="sina",
        endpoint="snapshot",
        capability_priority=90,
        capability_version="sina-snapshot-v1",
        raw_object_path="snapshot/2026-09-11.json",
        fetch_time=datetime(2026, 9, 11, 15, 16, tzinfo=timezone.utc),
    )
    assert result.records == ()
    assert result.rejected_symbols == ("sh600519", "sz000001")
