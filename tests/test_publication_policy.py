from decimal import Decimal

from stock_data_manage.quality.publication import PublicationPolicy


def test_publication_policy_allows_values_on_both_boundaries() -> None:
    policy = PublicationPolicy(Decimal("0.01"), 50)

    assert policy.allows(expected_count=5_000, actual_count=4_950)


def test_publication_policy_rejects_when_either_boundary_is_exceeded() -> None:
    policy = PublicationPolicy(Decimal("0.01"), 50)

    assert not policy.allows(expected_count=100, actual_count=98)
    assert not policy.allows(expected_count=10_000, actual_count=9_949)
