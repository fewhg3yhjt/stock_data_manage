from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stock_data_manage.storage.parquet import CanonicalPartitionStore
from stock_data_manage.pipeline.daily_reconciliation import DailyReconciliation
from stock_data_manage.domain import (
    Adjustment,
    AssetType,
    BarRecord,
    Dataset,
    ItemStatus,
    QualityStatus,
)
from stock_data_manage.storage.metadata import MetadataStore


SHANGHAI = ZoneInfo("Asia/Shanghai")


def bar(instrument_id: str, quality: QualityStatus, close: str = "10") -> BarRecord:
    return BarRecord(
        dataset=Dataset.DAILY_BAR,
        instrument_id=instrument_id,
        source_symbol="sh" + instrument_id[-6:],
        trade_date=date(2026, 9, 11),
        bar_time=None,
        interval_minutes=None,
        adjustment=Adjustment.NONE,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal("100"),
        amount=Decimal("1000"),
        source_provider="snapshot" if quality is QualityStatus.PROVISIONAL else "history",
        endpoint="snapshot" if quality is QualityStatus.PROVISIONAL else "history",
        source_method="snapshot" if quality is QualityStatus.PROVISIONAL else "daily_history",
        quality_status=quality,
        field_level="full_bar",
        capability_priority=100,
        capability_version="v1",
        raw_object_path="raw/test.json",
        fetch_time=datetime(2026, 9, 11, 16, tzinfo=SHANGHAI),
    )


def test_daily_reconciliation_promotes_provisional_and_records_clean_partition(tmp_path) -> None:
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        canonical.publish(
            dataset=Dataset.DAILY_BAR,
            asset_type=AssetType.STOCK.value,
            partition_key="2026-09-11",
            new_records=[bar("XSHG:600001", QualityStatus.PROVISIONAL)],
            expected_count=1,
            run_id="snapshot",
            item_statuses={"XSHG:600001": ItemStatus.TEMPORARY_EMPTY},
        )
        result = DailyReconciliation(canonical=canonical, metadata=metadata).reconcile(
            asset_type=AssetType.STOCK.value,
            trade_date=date(2026, 9, 11),
            expected_instrument_ids=["XSHG:600001"],
            history_records=[bar("XSHG:600001", QualityStatus.FINAL)],
            run_id="history",
        )
        partition = metadata.partition("daily_bar", "stock", "2026-09-11")
    assert result.completeness_status == "complete"
    assert result.selected_count == 1
    assert partition is not None and partition["status"] == "complete_clean"
    assert canonical.read(Dataset.DAILY_BAR, "stock", "2026-09-11")[0].quality_status is QualityStatus.FINAL


def test_daily_reconciliation_marks_unseen_expected_items_missing(tmp_path) -> None:
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        result = DailyReconciliation(canonical=canonical, metadata=metadata).reconcile(
            asset_type="stock",
            trade_date=date(2026, 9, 11),
            expected_instrument_ids=["XSHG:600001", "XSHE:000001"],
            history_records=[bar("XSHG:600001", QualityStatus.FINAL)],
        )
    assert result.completeness_status == "partial"
    assert result.selected_count == 1
