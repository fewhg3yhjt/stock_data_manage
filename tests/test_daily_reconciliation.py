from datetime import date
import pytest
from stock_data_manage.pipeline.daily_reconciliation import DailyReconciliation
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.storage.parquet import CanonicalPartitionStore
from stock_data_manage.worker.acceptance import _acceptance_bar


def test_daily_reconciliation_uses_complete_aggregate_and_shared_store(tmp_path):
    day = date(2026, 9, 11)
    record = _acceptance_bar(day, "XSHG:600001")
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        result = DailyReconciliation(canonical=canonical, metadata=metadata).reconcile(
            asset_type="stock", trade_date=day, expected_instrument_ids=[record.instrument_id],
            history_records=[record])
        assert metadata.partition("daily_bar", "stock", day.isoformat(), adjustment="aggregate")["actual_count"] == 1
    assert result.completeness_status == "complete"
    assert canonical.read_daily("stock", day.isoformat()) == [record]


def test_daily_reconciliation_rejects_incomplete_scope_without_publication(tmp_path):
    day = date(2026, 9, 11)
    canonical = CanonicalPartitionStore(tmp_path / "canonical")
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        with pytest.raises(ValueError, match="target scope"):
            DailyReconciliation(canonical=canonical, metadata=metadata).reconcile(
                asset_type="stock", trade_date=day, expected_instrument_ids=["XSHG:600001", "XSHE:000001"],
                history_records=[_acceptance_bar(day, "XSHG:600001")])
    assert canonical.read_daily("stock", day.isoformat()) == []
