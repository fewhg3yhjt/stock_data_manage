from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_data_manage.canonical_storage import CanonicalPartitionStore
from stock_data_manage.domain import Dataset, QualityStatus
from stock_data_manage.hot_store import HotMinuteStore
from stock_data_manage.metadata import MetadataStore
from stock_data_manage.minute_reconciliation import MinuteReconciliation


SHANGHAI = ZoneInfo("Asia/Shanghai")


def test_reconciliation_promotes_history_over_hot(tmp_path, minute_bar, final_minute_bar) -> None:
    with HotMinuteStore(tmp_path / "hot.db") as hot, MetadataStore(
        tmp_path / "metadata.duckdb"
    ) as metadata:
        hot.upsert(minute_bar)
        result = MinuteReconciliation(
            canonical=CanonicalPartitionStore(tmp_path / "canonical"),
            hot=hot,
            metadata=metadata,
        ).reconcile(
            instrument_id=minute_bar.instrument_id,
            asset_type="stock",
            trade_date=minute_bar.trade_date,
            history_records=[final_minute_bar],
            expected_bar_times=[minute_bar.bar_time],
        )
        completeness = metadata.minute_completeness(
            minute_bar.instrument_id, minute_bar.trade_date, 1, "none"
        )
    assert result.completeness_status == "complete"
    assert result.selected_count == 1
    assert result.publish.records[0].quality_status is QualityStatus.FINAL
    assert completeness is not None and completeness["status"] == "complete"


def test_reconciliation_isolates_hard_conflict(tmp_path, minute_bar, final_minute_bar) -> None:
    conflicting = replace(final_minute_bar, close=Decimal("120"), high=Decimal("121"))
    with HotMinuteStore(tmp_path / "hot.db") as hot, MetadataStore(
        tmp_path / "metadata.duckdb"
    ) as metadata:
        hot.upsert(minute_bar)
        result = MinuteReconciliation(
            canonical=CanonicalPartitionStore(tmp_path / "canonical"),
            hot=hot,
            metadata=metadata,
        ).reconcile(
            instrument_id=minute_bar.instrument_id,
            asset_type="stock",
            trade_date=minute_bar.trade_date,
            history_records=[conflicting],
            expected_bar_times=[minute_bar.bar_time],
        )
        assert metadata.conflict_count("minute_bar_1m", "2026-09-11") > 0
    assert result.hard_conflict_keys
    assert result.completeness_status == "partial"
