from datetime import datetime, timedelta, timezone

import pytest

from stock_data_manage.capability import CapabilityRegistry, ProviderCapability
from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange, ItemStatus
from stock_data_manage.planner import calculate_missing_set, plan_realtime_collection


def capability(**overrides: object) -> ProviderCapability:
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    values = dict(
        provider="tencent",
        endpoint="native_1m",
        version="v1",
        datasets=frozenset({Dataset.MINUTE_BAR_1M}),
        exchanges=frozenset({Exchange.XSHG, Exchange.XSHE}),
        asset_types=frozenset({AssetType.STOCK}),
        frequencies=frozenset({1}),
        adjustments=frozenset({Adjustment.NONE}),
        priority=100,
        validated_at=now - timedelta(days=1),
        validation_expires_at=now + timedelta(days=1),
        max_symbols_per_request=100,
        request_interval_seconds=1,
        effective_concurrency=1,
    )
    values.update(overrides)
    return ProviderCapability(**values)


def test_missing_set_only_excludes_evidence_backed_completion() -> None:
    statuses = {
        "a": ItemStatus.SUCCESS,
        "b": ItemStatus.NO_TRADE,
        "c": ItemStatus.CONFIRMED_NO_DATA,
        "d": ItemStatus.TEMPORARY_EMPTY,
        "e": ItemStatus.CONFLICT,
    }
    assert calculate_missing_set("abcdef", statuses) == frozenset({"d", "e", "f"})


def test_registry_rejects_expired_and_over_budget_capability() -> None:
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    registry = CapabilityRegistry(
        [
            capability(),
            capability(provider="expired", validation_expires_at=now),
            capability(provider="slow", request_interval_seconds=60),
        ]
    )
    selected = registry.select(
        now=now,
        dataset=Dataset.MINUTE_BAR_1M,
        exchange=Exchange.XSHG,
        asset_type=AssetType.STOCK,
        symbol="600519",
        frequency=1,
        adjustment=Adjustment.NONE,
        symbol_count=200,
        cycle_deadline_seconds=50,
    )
    assert [item.provider for item in selected] == ["tencent"]


def test_disabled_capability_is_never_selected() -> None:
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    registry = CapabilityRegistry([capability(role="disabled")])
    assert registry.select(
        now=now,
        dataset=Dataset.MINUTE_BAR_1M,
        exchange=Exchange.XSHG,
        asset_type=AssetType.STOCK,
        symbol="600519",
        frequency=1,
        adjustment=Adjustment.NONE,
        include_validation_only=True,
    ) == []


def test_watchlist_capacity_and_universe_guard() -> None:
    symbols = [f"{index:06d}" for index in range(200)]
    plan = plan_realtime_collection(
        universe_type="watchlist",
        symbols=symbols,
        capabilities=[capability()],
        max_watchlist_symbols=200,
        cycle_deadline_seconds=50,
    )
    assert plan.estimated_cycle_seconds == 2
    with pytest.raises(ValueError, match="maximum"):
        plan_realtime_collection(
            universe_type="watchlist",
            symbols=[*symbols, "999999"],
            capabilities=[capability()],
            max_watchlist_symbols=200,
            cycle_deadline_seconds=50,
        )
    with pytest.raises(ValueError, match="does not allow"):
        plan_realtime_collection(
            universe_type="all_stock",
            symbols=symbols,
            capabilities=[capability()],
            max_watchlist_symbols=200,
            cycle_deadline_seconds=50,
        )
