from dataclasses import replace
from datetime import datetime, time
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from stock_data_manage.routing.capabilities import ProviderCapability
from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange, QualityStatus
from stock_data_manage.storage.hot import HotMinuteStore
from stock_data_manage.service.market_data import MinuteQuery
from stock_data_manage.quality.normalization import NormalizationRule, Normalizer
from stock_data_manage.pipeline.minute import FixtureRealtimeMinuteProvider, RealtimeMinuteCollector
from stock_data_manage.domain.sessions import MarketSchedule, MarketSession, missing_bar_times
from stock_data_manage.pipeline.snapshot_aggregator import SnapshotMinuteAggregator
from stock_data_manage.storage.parquet import CanonicalPartitionStore
from stock_data_manage.storage.metadata import MetadataStore


SHANGHAI = ZoneInfo("Asia/Shanghai")


def capability() -> ProviderCapability:
    return ProviderCapability(
        provider="fixture",
        endpoint="realtime",
        version="v1",
        datasets=frozenset({Dataset.MINUTE_BAR_1M}),
        exchanges=frozenset({Exchange.XSHG}),
        asset_types=frozenset({AssetType.STOCK}),
        frequencies=frozenset({1}),
        adjustments=frozenset({Adjustment.NONE}),
        priority=100,
        validated_at=datetime(2026, 9, 1, tzinfo=SHANGHAI),
        validation_expires_at=datetime(2026, 10, 1, tzinfo=SHANGHAI),
        max_symbols_per_request=100,
        request_interval_seconds=1,
    )


def normalizer() -> Normalizer:
    return Normalizer(
        [
            NormalizationRule(
                provider="fixture",
                endpoint="realtime",
                exchange=Exchange.XSHG,
                asset_type=AssetType.STOCK,
                frequency=1,
                volume_multiplier=Decimal("1"),
                amount_multiplier=Decimal("1"),
                version="v1",
            )
        ]
    )


def test_session_expected_bars_and_boundaries() -> None:
    schedule = MarketSchedule()
    expected = schedule.expected_bar_times(datetime(2026, 9, 11).date())
    assert len(expected) == 240
    assert expected[0].time() == time(9, 31)
    assert expected[119].time() == time(11, 30)
    assert expected[120].time() == time(13, 1)
    assert expected[-1].time() == time(15, 0)
    assert schedule.is_expected_bar_time(expected[0])
    assert not schedule.is_expected_bar_time(datetime(2026, 9, 11, 11, 31, tzinfo=SHANGHAI))
    assert missing_bar_times(expected[:2], [expected[0]]) == (expected[1],)


def test_realtime_collector_writes_closed_bar_to_hot_store(tmp_path) -> None:
    symbol = "sh600519"
    provider = FixtureRealtimeMinuteProvider(
        "fixture", "realtime", capability(),
        {
            symbol: {
                "symbol": symbol,
                "trade_date": "2026-09-11",
                "bar_time": "2026-09-11T09:31:00+08:00",
                "open": "10",
                "high": "11",
                "low": "9",
                "close": "10.5",
                "volume": "100",
                "amount": "1000",
            }
        },
        [],
    )
    with HotMinuteStore(tmp_path / "hot.db") as hot:
        collector = RealtimeMinuteCollector(
            provider=provider,
            capability=capability(),
            normalizer=normalizer(),
            hot_store=hot,
            clock=lambda: datetime(2026, 9, 11, 9, 32, 2, tzinfo=SHANGHAI),
        )
        result = collector.collect_cycle(
            universe_type="watchlist",
            symbols=[symbol],
            instrument_id_by_symbol={symbol: "XSHG:600519"},
            exchange=Exchange.XSHG,
            asset_type=AssetType.STOCK,
        )
        assert len(result.accepted) == 1
        assert result.missing_symbols == ()
        assert hot.query(
            "XSHG:600519",
            datetime(2026, 9, 11, 9, 31, tzinfo=SHANGHAI),
            datetime(2026, 9, 11, 9, 31, tzinfo=SHANGHAI),
        )


def test_realtime_collector_skips_outside_session_and_rejects_full_market(tmp_path) -> None:
    provider = FixtureRealtimeMinuteProvider("fixture", "realtime", capability(), {}, [])
    with HotMinuteStore(tmp_path / "hot.db") as hot:
        collector = RealtimeMinuteCollector(
            provider=provider, capability=capability(), normalizer=normalizer(), hot_store=hot
        )
        skipped = collector.collect_cycle(
            universe_type="watchlist", symbols=["sh600519"], instrument_id_by_symbol={},
            exchange=Exchange.XSHG, asset_type=AssetType.STOCK,
            now=datetime(2026, 9, 11, 12, 0, tzinfo=SHANGHAI),
        )
        assert skipped.skipped and provider.requests == []
        with pytest.raises(ValueError, match="does not allow"):
            collector.collect_cycle(
                universe_type="all_stock", symbols=["sh600519"], instrument_id_by_symbol={},
                exchange=Exchange.XSHG, asset_type=AssetType.STOCK,
                now=datetime(2026, 9, 11, 10, 0, tzinfo=SHANGHAI),
            )


def test_snapshot_aggregation_handles_cumulative_volume_and_sampling_flag() -> None:
    aggregator = SnapshotMinuteAggregator(min_samples=2)
    snapshots = [
        {"symbol": "sh600519", "timestamp": "2026-09-11T09:30:58+08:00", "price": "10", "cum_volume": "90", "cum_amount": "900"},
        {"symbol": "sh600519", "timestamp": "2026-09-11T09:31:03+08:00", "price": "10.1", "cum_volume": "100", "cum_amount": "1000"},
        {"symbol": "sh600519", "timestamp": "2026-09-11T09:31:40+08:00", "price": "10.3", "cum_volume": "120", "cum_amount": "1200"},
        {"symbol": "sh600519", "timestamp": "2026-09-11T09:32:10+08:00", "price": "10.2", "cum_volume": "130", "cum_amount": "1300"},
    ]
    result = aggregator.aggregate(
        snapshots,
        instrument_id_by_symbol={"sh600519": "XSHG:600519"},
        exchange=Exchange.XSHG, asset_type=AssetType.STOCK,
        trade_date=datetime(2026, 9, 11, tzinfo=SHANGHAI).date(),
        provider="fixture", endpoint="snapshot",
        fetch_time=datetime(2026, 9, 11, 9, 33, tzinfo=SHANGHAI),
    )
    bar = [bar for bar in result.bars if bar.bar_time and bar.bar_time.minute == 31][0]
    assert bar.open == Decimal("10.1") and bar.high == Decimal("10.3") and bar.close == Decimal("10.3")
    assert bar.volume == Decimal("30") and bar.amount == Decimal("300")
    assert "incomplete_sampling" not in bar.flags
    assert result.rejected_snapshots == 0


def test_unified_query_prefers_final_canonical_over_hot(tmp_path, final_minute_bar) -> None:
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    canonical.publish(
        dataset=Dataset.MINUTE_BAR_1M, asset_type="stock", partition_key="2026-09-11",
        new_records=[final_minute_bar], expected_count=1, run_id="final",
    )
    with HotMinuteStore(tmp_path / "hot.db") as hot:
        hot.upsert(replace(final_minute_bar, quality_status=QualityStatus.PROVISIONAL, source_provider="tencent"))
        result = MinuteQuery(canonical=canonical, hot=hot).query(
            instrument_id=final_minute_bar.instrument_id, asset_type="stock",
            start=final_minute_bar.bar_time, end=final_minute_bar.bar_time,
        )
    assert len(result.records) == 1
    assert result.records[0].quality_status is QualityStatus.FINAL


def test_minute_completeness_metadata(tmp_path) -> None:
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        metadata.record_minute_completeness(
            instrument_id="XSHG:600519", trade_date=datetime(2026, 9, 11).date(),
            interval_minutes=1, adjustment="none", expected_rows=240, actual_rows=239,
            first_timestamp=datetime(2026, 9, 11, 9, 31, tzinfo=SHANGHAI),
            last_timestamp=datetime(2026, 9, 11, 15, 0, tzinfo=SHANGHAI),
        )
        row = metadata.minute_completeness("XSHG:600519", datetime(2026, 9, 11).date(), 1, "none")
    assert row is not None and row["status"] == "partial" and row["coverage_ratio"] == 239 / 240
