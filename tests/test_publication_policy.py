from decimal import Decimal

from stock_data_manage.quality.publication import PublicationPolicy
from stock_data_manage.quality.publication import load_publication_policy
import pytest
import yaml


def test_publication_policy_allows_values_on_both_boundaries() -> None:
    policy = PublicationPolicy(Decimal("0.01"), 50)

    assert policy.allows(expected_count=5_000, actual_count=4_950)


def test_publication_policy_rejects_when_either_boundary_is_exceeded() -> None:
    policy = PublicationPolicy(Decimal("0.01"), 50)

    assert not policy.allows(expected_count=100, actual_count=98)
    assert not policy.allows(expected_count=10_000, actual_count=9_949)


def test_policy_loader_reads_the_dataset_contract_and_rejects_wrong_identity(tmp_path):
    contract = tmp_path / "daily.yaml"
    contract.write_text(yaml.safe_dump({"dataset": {"name": "daily_bar", "publication": {
        "max_missing_ratio": "0.02", "max_missing_count": 3}}}), encoding="utf-8")
    policy = load_publication_policy(contract, "daily_bar")
    assert policy.max_missing_ratio == Decimal("0.02") and policy.max_missing_count == 3
    with pytest.raises(ValueError, match="does not match"):
        load_publication_policy(contract, "security_master")
