from pathlib import Path

from stock_data_manage.config.loader import (
    load_dataset_normalization_rules,
    load_normalization_document,
    load_normalization_rules_for_datasets,
)
from stock_data_manage.domain import Adjustment


ROOT = Path(__file__).resolve().parents[1]


def test_daily_bar_uses_dataset_specific_normalization_file() -> None:
    rules = load_dataset_normalization_rules(ROOT / "config" / "normalization", "daily_bar")
    assert any(rule.provider == "tencent" and rule.adjustment is Adjustment.FORWARD for rule in rules)


def test_dividend_event_uses_dataset_specific_normalization_file() -> None:
    document = load_normalization_document(ROOT / "config" / "normalization", "dividend_event")
    assert document["dataset"] == "dividend_event"
    assert document["rules"][0]["provider"] == "eastmoney"
    assert document["rules"][0]["endpoint"] == "dividend_event"


def test_dataset_rules_can_be_loaded_together_without_old_aggregate_file() -> None:
    rules = load_normalization_rules_for_datasets(ROOT / "config" / "normalization", ("daily_bar",))
    document = load_normalization_document(ROOT / "config" / "normalization", "dividend_event")
    assert any(rule.endpoint == "forward_history" for rule in rules)
    assert document["dataset"] == "dividend_event"
