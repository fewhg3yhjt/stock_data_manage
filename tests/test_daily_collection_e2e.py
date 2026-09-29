from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from stock_data_manage.storage.parquet import CanonicalPartitionStore
from stock_data_manage.pipeline.daily import (
    DailyCollectionService,
    Instrument,
    PublicationThresholdExceeded,
)
from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange, ItemStatus
from stock_data_manage.routing.capabilities import ProviderCapability
from stock_data_manage.providers.tencent import TencentSnapshotProvider
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.quality.normalization import NormalizationRule, Normalizer
from stock_data_manage.providers.base import FixtureDailyProvider
from stock_data_manage.providers.contracts import FailureClass, ProviderContractError, HttpResponse
from stock_data_manage.storage.raw import RawObjectStore
from stock_data_manage.quality.publication import load_publication_policy


SHANGHAI = ZoneInfo("Asia/Shanghai")
DATASETS_CONFIG = Path(__file__).resolve().parents[1] / "config" / "datasets.yaml"
DAILY_PUBLICATION_POLICY = load_publication_policy(DATASETS_CONFIG, "daily_bar")


def row(symbol: str, close: str) -> dict[str, str]:
    return {
        "symbol": symbol,
        "trade_date": "2026-09-11",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": "10",
        "amount": "100",
    }


def test_partial_primary_falls_back_only_for_missing_and_rerun_is_noop(tmp_path) -> None:
    primary = FixtureDailyProvider(
        "primary",
        "history",
        {"sh600001": row("sh600001", "10"), "sh600002": row("sh600002", "20")},
        capability_priority=100,
    )
    fallback = FixtureDailyProvider(
        "fallback",
        "history",
        {"sh600003": row("sh600003", "30")},
        capability_priority=80,
    )
    rules = [
        NormalizationRule(
            provider=provider,
            endpoint="history",
            exchange=Exchange.XSHG,
            asset_type=AssetType.STOCK,
            frequency=None,
            volume_multiplier=Decimal("100"),
            amount_multiplier=Decimal("1"),
            version="fixture-rules-v1",
        )
        for provider in ("primary", "fallback")
    ]
    instruments = [
        Instrument(f"XSHG:60000{index}", f"sh60000{index}", Exchange.XSHG, AssetType.STOCK)
        for index in (1, 2, 3)
    ]
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    fixed_now = datetime(2026, 9, 11, 16, 30, tzinfo=SHANGHAI)
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[primary, fallback],
            normalizer=Normalizer(rules),
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=canonical,
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
            clock=lambda: fixed_now,
        )
        first = service.collect(
            instruments=instruments, trade_date=date(2026, 9, 11), run_id="run-1"
        )
        second = service.collect(
            instruments=instruments, trade_date=date(2026, 9, 11), run_id="run-2"
        )
        partition = metadata.partition("daily_bar", "stock", "2026-09-11")

    assert primary.requests == [("sh600001", "sh600002", "sh600003")]
    assert fallback.requests == [("sh600003",)]
    assert first.requested_by_provider == {
        "primary": ("sh600001", "sh600002", "sh600003"),
        "fallback": ("sh600003",),
    }
    assert second.requested_by_provider == {}
    assert second.publish.no_op
    assert set(first.item_statuses.values()) == {ItemStatus.SUCCESS}
    assert partition is not None and partition["status"] == "complete_clean"
    records = canonical.read(Dataset.DAILY_BAR, "stock", "2026-09-11")
    assert len(records) == 3
    assert len({record.key for record in records}) == 3
    assert {record.volume for record in records} == {Decimal("1000.00000000")}


@pytest.mark.parametrize(
    ("asset_type", "symbol", "instrument_id"),
    [
        (AssetType.STOCK, "sh600519", "XSHG:600519"),
        (AssetType.ETF, "sh510300", "XSHG:510300"),
        (AssetType.LOF, "sh501018", "XSHG:501018"),
    ],
)
def test_forward_adjusted_daily_collection_publishes_stock_etf_and_lof(
    tmp_path, asset_type: AssetType, symbol: str, instrument_id: str
) -> None:
    provider = FixtureDailyProvider(
        "tencent",
        "forward_history",
        {symbol: row(symbol, "10")},
        capability_version="tencent-qfq-kline-v1",
        adjustment=Adjustment.FORWARD,
    )
    normalizer = Normalizer(
        [
            NormalizationRule(
                provider="tencent",
                endpoint="forward_history",
                exchange=Exchange.XSHG,
                asset_type=asset_type,
                frequency=None,
                volume_multiplier=Decimal("1"),
                amount_multiplier=Decimal("1"),
                version="tencent-qfq-v1",
                adjustment=Adjustment.FORWARD,
            )
        ]
    )
    instrument = Instrument(instrument_id, symbol, Exchange.XSHG, asset_type)
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        result = DailyCollectionService(
            providers=[provider],
            normalizer=normalizer,
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=canonical,
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
        ).collect(
            instruments=[instrument],
            trade_date=date(2026, 9, 11),
            run_id=f"qfq-{asset_type.value}",
            adjustment=Adjustment.FORWARD,
        )

    assert result.item_statuses[instrument_id] is ItemStatus.SUCCESS
    record = canonical.read(Dataset.DAILY_BAR, asset_type.value, "2026-09-11")[0]
    assert record.adjustment is Adjustment.FORWARD
    assert record.source_provider == "tencent"


def test_forward_adjusted_collection_does_not_use_unadjusted_provider(tmp_path) -> None:
    provider = FixtureDailyProvider(
        "sina",
        "full_history",
        {"sh600519": row("sh600519", "10")},
        adjustment=Adjustment.NONE,
    )
    instrument = Instrument("XSHG:600519", "sh600519", Exchange.XSHG, AssetType.STOCK)
    normalizer = Normalizer(
        [
            NormalizationRule(
                provider="sina",
                endpoint="full_history",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=None,
                volume_multiplier=Decimal("1"),
                amount_multiplier=Decimal("1"),
                version="sina-v1",
            )
        ]
    )
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[provider],
            normalizer=normalizer,
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=CanonicalPartitionStore(tmp_path / "canonical"),
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
        )
        with pytest.raises(PublicationThresholdExceeded):
            service.collect(
                instruments=[instrument],
                trade_date=date(2026, 9, 11),
                run_id="qfq-reject-none",
                adjustment=Adjustment.FORWARD,
            )
    assert provider.requests == []


def test_forward_adjusted_collection_skips_provider_without_asset_qualification(tmp_path) -> None:
    provider = FixtureDailyProvider(
        "tencent",
        "forward_history",
        {"sh501018": row("sh501018", "10")},
        adjustment=Adjustment.FORWARD,
        supported_asset_types=frozenset({AssetType.STOCK, AssetType.ETF}),
    )
    instrument = Instrument("XSHG:501018", "sh501018", Exchange.XSHG, AssetType.LOF)
    normalizer = Normalizer(
        [
            NormalizationRule(
                provider="tencent",
                endpoint="forward_history",
                exchange=Exchange.XSHG,
                asset_type=AssetType.LOF,
                frequency=None,
                volume_multiplier=Decimal("1"),
                amount_multiplier=Decimal("1"),
                version="tencent-qfq-v1",
                adjustment=Adjustment.FORWARD,
            )
        ]
    )
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[provider],
            normalizer=normalizer,
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=CanonicalPartitionStore(tmp_path / "canonical"),
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
        )
        with pytest.raises(PublicationThresholdExceeded):
            service.collect(
                instruments=[instrument],
                trade_date=date(2026, 9, 11),
                run_id="qfq-skip-lof",
                adjustment=Adjustment.FORWARD,
            )
    assert provider.requests == []


def test_forward_adjusted_collection_rejects_mismatched_provider_response(tmp_path) -> None:
    provider = FixtureDailyProvider(
        "tencent",
        "forward_history",
        {"sh600519": row("sh600519", "10")},
        adjustment=Adjustment.NONE,
    )
    instrument = Instrument("XSHG:600519", "sh600519", Exchange.XSHG, AssetType.STOCK)
    normalizer = Normalizer(
        [
            NormalizationRule(
                provider="tencent",
                endpoint="forward_history",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=None,
                volume_multiplier=Decimal("1"),
                amount_multiplier=Decimal("1"),
                version="tencent-qfq-v1",
            )
        ]
    )
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[provider],
            normalizer=normalizer,
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=CanonicalPartitionStore(tmp_path / "canonical"),
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
        )
        with pytest.raises(PublicationThresholdExceeded):
            service.collect(
                instruments=[instrument],
                trade_date=date(2026, 9, 11),
                run_id="qfq-reject-response",
                adjustment=Adjustment.FORWARD,
            )
    assert provider.requests == []


def test_forward_collection_does_not_reuse_unadjusted_metadata_or_break_partition_count(tmp_path) -> None:
    instrument = Instrument("XSHG:600519", "sh600519", Exchange.XSHG, AssetType.STOCK)
    rules = [
        NormalizationRule(
            provider="fixture",
            endpoint="history",
            exchange=Exchange.XSHG,
            asset_type=AssetType.STOCK,
            frequency=None,
            volume_multiplier=Decimal("1"),
            amount_multiplier=Decimal("1"),
            version="fixture-v1",
        ),
        NormalizationRule(
            provider="fixture-qfq",
            endpoint="forward_history",
            exchange=Exchange.XSHG,
            asset_type=AssetType.STOCK,
            frequency=None,
            volume_multiplier=Decimal("1"),
            amount_multiplier=Decimal("1"),
            version="fixture-qfq-v1",
            adjustment=Adjustment.FORWARD,
        ),
    ]
    normalizer = Normalizer(rules)
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[
                FixtureDailyProvider("fixture", "history", {"sh600519": row("sh600519", "1")}),
                FixtureDailyProvider(
                    "fixture-qfq",
                    "forward_history",
                    {"sh600519": row("sh600519", "10")},
                    adjustment=Adjustment.FORWARD,
                ),
            ],
            normalizer=normalizer,
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=canonical,
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
        )
        service.collect(
            instruments=[instrument],
            trade_date=date(2026, 9, 11),
            run_id="none-first",
            adjustment=Adjustment.NONE,
        )
        qfq = service.collect(
            instruments=[instrument],
            trade_date=date(2026, 9, 11),
            run_id="qfq-second",
            adjustment=Adjustment.FORWARD,
        )
        records = canonical.read(Dataset.DAILY_BAR, "stock", "2026-09-11")
        manifest = metadata.partition("daily_bar", "stock", "2026-09-11")

    assert qfq.requested_by_provider == {"fixture-qfq": ("sh600519",)}
    assert {record.adjustment for record in records} == {Adjustment.NONE, Adjustment.FORWARD}
    assert manifest is not None and manifest["actual_count"] == 2 and manifest["expected_count"] == 2


def test_forward_collection_rejects_index_unconditionally(tmp_path) -> None:
    instrument = Instrument("XSHG:000001", "sh000001", Exchange.XSHG, AssetType.INDEX)
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[],
            normalizer=Normalizer([]),
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=CanonicalPartitionStore(tmp_path / "canonical"),
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
        )
        with pytest.raises(ValueError, match="index daily bars"):
            service.collect(
                instruments=[instrument],
                trade_date=date(2026, 9, 11),
                run_id="qfq-index-reject",
                adjustment=Adjustment.FORWARD,
            )


def test_rate_limited_primary_is_cooled_and_fallback_completes_partition(tmp_path) -> None:
    primary = FixtureDailyProvider(
        "primary",
        "history",
        {"sh600001": row("sh600001", "10")},
        failures=[
            ProviderContractError(
                "HTTP 429",
                FailureClass.RATE_LIMITED,
                retryable=False,
                http_status=429,
            )
        ],
    )
    fallback = FixtureDailyProvider(
        "fallback", "history", {"sh600001": row("sh600001", "10")}, capability_priority=80
    )
    rules = [
        NormalizationRule(
            provider=provider,
            endpoint="history",
            exchange=Exchange.XSHG,
            asset_type=AssetType.STOCK,
            frequency=None,
            volume_multiplier=Decimal("1"),
            amount_multiplier=Decimal("1"),
            version="v1",
        )
        for provider in ("primary", "fallback")
    ]
    instrument = Instrument("XSHG:600001", "sh600001", Exchange.XSHG, AssetType.STOCK)
    now = datetime(2026, 9, 11, 16, 30, tzinfo=SHANGHAI)
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[primary, fallback],
            normalizer=Normalizer(rules),
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=CanonicalPartitionStore(tmp_path / "canonical"),
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
            clock=lambda: now,
        )
        result = service.collect(
            instruments=[instrument], trade_date=date(2026, 9, 11), run_id="rate-limit"
        )
        health = metadata.provider_health(
            provider="primary",
            endpoint="history",
            market="XSHG",
            asset_type="stock",
            dataset="daily_bar",
            capability_version="fixture-v1",
        )
    assert primary.requests == [("sh600001",)]
    assert fallback.requests == [("sh600001",)]
    assert result.item_statuses[instrument.instrument_id] is ItemStatus.SUCCESS
    assert health is not None and health.http_429_count == 1
    assert health.open_until is not None


def test_snapshot_provider_publishes_provisional_and_final_provider_can_replace_it(tmp_path) -> None:
    parts = ["-"] * 38
    parts[1] = "贵州茅台"
    parts[3:7] = ["1275.16", "1285.15", "1285.15", "34801"]
    parts[30] = "20260911150003"
    parts[33] = "1286.15"
    parts[34] = "1263.01"
    parts[37] = "445001.2"
    body = f'v_sh600519="{"~".join(parts)}";\n'.encode("gbk")

    class SnapshotTransport:
        def get(self, url, *, params, timeout_seconds):
            return HttpResponse(200, {"content-type": "text/plain"}, body)

    capability = ProviderCapability(
        provider="tencent",
        endpoint="bulk_snapshot",
        version="snapshot-v1",
        datasets=frozenset({Dataset.SNAPSHOT, Dataset.DAILY_BAR}),
        exchanges=frozenset({Exchange.XSHG}),
        asset_types=frozenset({AssetType.STOCK}),
        frequencies=frozenset(),
        adjustments=frozenset(),
        priority=50,
        validated_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        validation_expires_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    snapshot = TencentSnapshotProvider(SnapshotTransport(), capability)
    final_row = row("sh600519", "1275.16")
    final_row.update({"open": "1285.15", "high": "1286.15", "low": "1263.01"})
    final_row["volume"] = "3480100"
    final_row["amount"] = "4450012000"
    final = FixtureDailyProvider("history", "history", {"sh600519": final_row})
    rules = Normalizer(
        [
            NormalizationRule(
                provider="tencent",
                endpoint="bulk_snapshot",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=None,
                volume_multiplier=Decimal("100"),
                amount_multiplier=Decimal("10000"),
                version="snapshot-v1",
            ),
            NormalizationRule(
                provider="history",
                endpoint="history",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=None,
                volume_multiplier=Decimal("1"),
                amount_multiplier=Decimal("1"),
                version="history-v1",
            ),
        ]
    )
    instrument = Instrument("XSHG:600519", "sh600519", Exchange.XSHG, AssetType.STOCK)
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[snapshot],
            normalizer=rules,
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=CanonicalPartitionStore(tmp_path / "canonical"),
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
            clock=lambda: datetime(2026, 9, 11, 16, 30, tzinfo=SHANGHAI),
        )
        provisional = service.collect(
            instruments=[instrument], trade_date=date(2026, 9, 11), run_id="snapshot"
        )
        assert provisional.item_statuses[instrument.instrument_id] is ItemStatus.TEMPORARY_EMPTY
        service.providers = (snapshot, final)
        completed = service.collect(
            instruments=[instrument], trade_date=date(2026, 9, 11), run_id="history"
        )
    assert completed.item_statuses[instrument.instrument_id] is ItemStatus.SUCCESS
    record = CanonicalPartitionStore(tmp_path / "canonical").read(
        Dataset.DAILY_BAR, "stock", "2026-09-11"
    )[0]
    assert record.quality_status.value == "final"
    assert record.source_provider == "history"


def test_daily_collection_allows_publication_at_configured_missing_limit(tmp_path) -> None:
    rows = {
        f"sh600{index:03d}": row(f"sh600{index:03d}", "10")
        for index in range(1, 100)
    }
    provider = FixtureDailyProvider("primary", "history", rows)
    normalizer = Normalizer(
        [
            NormalizationRule(
                provider="primary",
                endpoint="history",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=None,
                volume_multiplier=Decimal("1"),
                amount_multiplier=Decimal("1"),
                version="v1",
            )
        ]
    )
    instruments = [
        Instrument(
            f"XSHG:600{index:03d}",
            f"sh600{index:03d}",
            Exchange.XSHG,
            AssetType.STOCK,
        )
        for index in range(1, 101)
    ]
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[provider],
            normalizer=normalizer,
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=canonical,
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
        )

        service.collect(
            instruments=instruments,
            trade_date=date(2026, 9, 11),
            run_id="at-missing-limit",
        )

    assert len(canonical.read(Dataset.DAILY_BAR, "stock", "2026-09-11")) == 99


def test_daily_collection_rejects_publication_above_configured_missing_limit(tmp_path) -> None:
    rows = {
        f"sh600{index:03d}": row(f"sh600{index:03d}", "10")
        for index in range(1, 99)
    }
    provider = FixtureDailyProvider("primary", "history", rows)
    normalizer = Normalizer(
        [
            NormalizationRule(
                provider="primary",
                endpoint="history",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=None,
                volume_multiplier=Decimal("1"),
                amount_multiplier=Decimal("1"),
                version="v1",
            )
        ]
    )
    instruments = [
        Instrument(
            f"XSHG:600{index:03d}",
            f"sh600{index:03d}",
            Exchange.XSHG,
            AssetType.STOCK,
        )
        for index in range(1, 101)
    ]
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        service = DailyCollectionService(
            providers=[provider],
            normalizer=normalizer,
            raw_store=RawObjectStore(tmp_path / "raw"),
            canonical_store=canonical,
            metadata=metadata,
            publication_policy=DAILY_PUBLICATION_POLICY,
        )

        with pytest.raises(PublicationThresholdExceeded):
            service.collect(
                instruments=instruments,
                trade_date=date(2026, 9, 11),
                run_id="too-many-missing",
            )

    assert canonical.read(Dataset.DAILY_BAR, "stock", "2026-09-11") == []
