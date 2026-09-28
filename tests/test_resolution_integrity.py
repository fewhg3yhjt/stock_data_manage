from dataclasses import replace
from decimal import Decimal

from stock_data_manage.storage.integrity import Manifest, row_hash
from stock_data_manage.quality.resolution import resolve_records


def test_final_history_replaces_provisional_without_field_splicing(
    minute_bar, final_minute_bar
) -> None:
    result = resolve_records([minute_bar, final_minute_bar])
    assert not result.quarantined
    assert result.selected is final_minute_bar
    assert result.selected.amount == final_minute_bar.amount


def test_hard_conflict_quarantines_only_the_record(minute_bar, final_minute_bar) -> None:
    conflicting = replace(final_minute_bar, close=Decimal("120"), high=Decimal("121"))
    result = resolve_records([minute_bar, conflicting])
    assert result.quarantined
    assert result.selected is None
    assert any(item.field == "close" and item.severity == "hard" for item in result.conflicts)
    assert all(item.canonical_key == minute_bar.key for item in result.conflicts)


def test_row_hash_is_order_stable() -> None:
    assert row_hash({"b": 2, "a": 1}) == row_hash({"a": 1, "b": 2})


def test_manifest_detects_file_tampering(tmp_path) -> None:
    data = tmp_path / "data.parquet"
    data.write_bytes(b"complete partition")
    manifest = Manifest.from_file(
        data,
        dataset="daily_bar",
        row_count=1,
        first_key="a",
        last_key="a",
        raw_refs=("raw/a",),
    )
    manifest_path = tmp_path / "manifest.json"
    manifest.write_atomic(manifest_path)
    assert manifest.verify(data)
    data.write_bytes(b"truncated")
    assert not manifest.verify(data)
