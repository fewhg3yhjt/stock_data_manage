from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_data_manage.canonical_storage import CanonicalPartitionStore
from stock_data_manage.collection import DailyCollectionService, Instrument
from stock_data_manage.domain import AssetType, Dataset, Exchange, ItemStatus
from stock_data_manage.capability import ProviderCapability
from stock_data_manage.http_providers import TencentSnapshotProvider
from stock_data_manage.metadata import MetadataStore
from stock_data_manage.normalizer import NormalizationRule, Normalizer
from stock_data_manage.providers import FixtureDailyProvider
from stock_data_manage.provider_contract import FailureClass, ProviderContractError
from stock_data_manage.raw_storage import RawObjectStore
from stock_data_manage.provider_contract import HttpResponse


SHANGHAI = ZoneInfo("Asia/Shanghai")


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
