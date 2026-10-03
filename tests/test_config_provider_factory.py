from pathlib import Path
from datetime import datetime, timezone
from datetime import timedelta

import pytest
import yaml

from stock_data_manage.config.loader import load_capability_routes, load_dataset_normalization_rules, load_provider_configs
from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange
from stock_data_manage.routing.factory import load_provider_registry
from stock_data_manage.providers.tencent import TencentDailyProvider
from stock_data_manage.providers.baostock import BaoStockDailyProvider, BaoStockIndustryMembershipProvider
from stock_data_manage.providers.eastmoney import EastMoneyDividendProvider
from stock_data_manage.providers.eastmoney import EastMoneySecurityListProvider
from stock_data_manage.providers.eastmoney import EastMoneyRealtimeQuoteProvider
from stock_data_manage.providers.eastmoney import EastMoneyStockFundFlowProvider
from stock_data_manage.providers.ths import ThsBoardProvider
from stock_data_manage.providers.eastmoney import EastMoneyFinancialMainProvider, EastMoneyShareholderCountProvider
from stock_data_manage.providers.akshare import AkShareBoardProvider, AkShareDailyProvider
from stock_data_manage.providers.probes import ProbeEvidence
from stock_data_manage.storage.metadata import MetadataStore


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 3, tzinfo=timezone.utc)


def routing_evidence(**overrides):
    values = dict(
        provider="tencent", endpoint="forward_history", capability_version="tencent-qfq-kline-v1",
        validated_at=NOW - timedelta(hours=1), validation_expires_at=NOW + timedelta(hours=1),
        status="complete", eligible_for_selection=True, row_count=1,
        first_key="2026-09-30", last_key="2026-09-30", evidence_hash="offline-fixture-hash",
        request_scope=("sh600519",), response_status=200, returned_window="2026-09-30",
        field_semantics=("trade_date", "close"), units=("price:cny", "volume:lot"), adjustment="forward",
    )
    values.update(overrides)
    return ProbeEvidence(**values)


def save_routing_evidence(metadata, evidence, **overrides):
    dimensions = dict(dataset="daily_bar", market="XSHG", asset_type="stock", frequency="daily", adjustment="forward")
    dimensions.update(overrides)
    metadata.save_probe_evidence(evidence, **dimensions)


def select_qfq(registry, **overrides):
    request = dict(now=NOW, dataset=Dataset.DAILY_BAR, exchange=Exchange.XSHG,
                   asset_type=AssetType.STOCK, symbol="sh600519", adjustment=Adjustment.FORWARD)
    request.update(overrides)
    return registry.select(**request)


def test_default_registry_does_not_fabricate_evidence():
    registry, providers = load_provider_registry(ROOT / "config/providers.yaml", now=NOW)
    assert providers == ()
    assert select_qfq(registry) == []


def test_factory_preserves_evidence_scope_expiry_and_shared_pacing_budget(tmp_path):
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        evidence = routing_evidence()
        save_routing_evidence(metadata, evidence)
        registry, providers = load_provider_registry(ROOT / "config/providers.yaml", now=NOW, metadata=metadata)
        assert len(providers) == 1
        selected = select_qfq(registry)
        assert len(selected) == 1
        assert selected[0].validated_at == evidence.validated_at
        assert selected[0].validation_expires_at == evidence.validation_expires_at
        assert selected[0].request_interval_seconds == 1  # Tencent kline group, stricter than 0.5.
        assert select_qfq(registry, symbol="sh600000") == []
        assert select_qfq(registry, now=evidence.validation_expires_at) == []
        assert select_qfq(registry, exchange=Exchange.XSHE) == []


@pytest.mark.parametrize("changes,dimensions", [
    ({"eligible_for_selection": False}, {}),
    ({"status": "temporary_empty", "row_count": 0}, {}),
    ({"validation_expires_at": NOW}, {}),
    ({"validated_at": NOW + timedelta(minutes=1)}, {}),
    ({"capability_version": "wrong-version"}, {}),
    ({"units": ("volume:unverified",)}, {}),
    ({"units": ("",)}, {}),
    ({"field_semantics": ()}, {}),
    ({"request_scope": ()}, {}),
    ({"response_status": 429}, {}),
    ({}, {"frequency": "5m"}),
    ({}, {"dataset": "minute_bar_5m"}),
])
def test_factory_rejects_unusable_evidence(tmp_path, changes, dimensions):
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        save_routing_evidence(metadata, routing_evidence(**changes), **dimensions)
        registry, providers = load_provider_registry(ROOT / "config/providers.yaml", now=NOW, metadata=metadata)
        assert providers == ()
        assert select_qfq(registry) == []


def test_selection_rechecks_cooldown_and_revoked_evidence(tmp_path):
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        save_routing_evidence(metadata, routing_evidence())
        registry, _ = load_provider_registry(ROOT / "config/providers.yaml", now=NOW, metadata=metadata)
        assert len(select_qfq(registry)) == 1
        key = dict(provider="tencent", endpoint="forward_history", capability_version="tencent-qfq-kline-v1",
                   market="XSHG", asset_type="stock", dataset="daily_bar")
        metadata.record_provider_failure(**key, failure_class="rate_limited", error="offline 429",
                                         now=NOW, cooldown_seconds=30, http_status=429)
        assert select_qfq(registry) == []
        assert select_qfq(registry, now=NOW + timedelta(seconds=31)) == []
        metadata.record_provider_success(**key, now=NOW, coverage=1, probe_ttl=timedelta(hours=1))
        assert len(select_qfq(registry)) == 1
        save_routing_evidence(metadata, routing_evidence(status="temporary_empty", row_count=0))
        assert select_qfq(registry) == []


def test_unimplemented_or_unpaced_declaration_cannot_enter_formal_routing(tmp_path):
    payload = yaml.safe_load((ROOT / "config/providers.yaml").read_text("utf-8"))
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        save_routing_evidence(metadata, routing_evidence())
        for field, value in [("implementation_status", "unimplemented"), ("request_limit_enforcement", "call_boundary_only")]:
            modified = yaml.safe_load(yaml.safe_dump(payload))
            modified["providers"]["tencent"]["endpoints"]["forward_history"][field] = value
            path = tmp_path / "providers.yaml"
            path.write_text(yaml.safe_dump(modified, allow_unicode=True), encoding="utf-8")
            registry, providers = load_provider_registry(path, now=NOW, metadata=metadata)
            assert providers == ()
            assert select_qfq(registry) == []


def test_provider_yaml_declares_tencent_qfq_and_routes() -> None:
    configs = load_provider_configs(ROOT / "config" / "providers.yaml")
    qfq = next(item for item in configs if item.provider == "tencent" and item.endpoint == "forward_history")
    assert qfq.adjustments == frozenset({Adjustment.FORWARD})
    assert qfq.datasets == frozenset({Dataset.DAILY_BAR})
    assert qfq.capability_version == "tencent-qfq-kline-v1"

    routes = load_capability_routes(ROOT / "config" / "capabilities.yaml")
    assert routes[Dataset.DAILY_BAR][0]["endpoint"] == "forward_history"


def test_normalization_yaml_declares_tencent_qfq_rules() -> None:
    rules = load_dataset_normalization_rules(ROOT / "config" / "normalization", "daily_bar")
    assert any(
        rule.provider == "tencent"
        and rule.endpoint == "forward_history"
        and rule.adjustment is Adjustment.FORWARD
        for rule in rules
    )


def test_provider_factory_builds_configured_qfq_provider() -> None:
    registry, providers = load_provider_registry(
        ROOT / "config" / "providers.yaml",
        ROOT / "config" / "capabilities.yaml",
        include_unverified=True,
    )
    qfq = next(provider for provider in providers if isinstance(provider, TencentDailyProvider) and provider.adjustment is Adjustment.FORWARD)
    assert qfq.endpoint == "forward_history"
    assert qfq.capability_version == "tencent-qfq-kline-v1"
    assert registry.select(
        now=qfq.capability_version and __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        dataset=Dataset.DAILY_BAR,
        exchange=__import__("stock_data_manage.domain", fromlist=["Exchange"]).Exchange.XSHG,
        asset_type=__import__("stock_data_manage.domain", fromlist=["AssetType"]).AssetType.STOCK,
        symbol="sh600519",
        adjustment=Adjustment.FORWARD,
    ) == []  # Building a probe adapter must not manufacture routing evidence.


def test_provider_factory_registers_baostock_validation_provider() -> None:
    _, providers = load_provider_registry(
        ROOT / "config" / "providers.yaml",
        ROOT / "config" / "capabilities.yaml",
        include_unverified=True,
    )
    provider = next(item for item in providers if isinstance(item, BaoStockDailyProvider))
    assert provider.endpoint == "daily_history"
    assert provider.capability_version == "baostock-daily-v1"
    assert provider.adjustment is Adjustment.NONE


def test_provider_factory_does_not_build_disabled_eastmoney_dividend_provider() -> None:
    _, providers = load_provider_registry(
        ROOT / "config" / "providers.yaml",
        ROOT / "config" / "capabilities.yaml",
        include_unverified=True,
    )
    assert not any(isinstance(item, EastMoneyDividendProvider) for item in providers)
    assert not any(isinstance(item, EastMoneySecurityListProvider) for item in providers)
    assert not any(isinstance(item, EastMoneyRealtimeQuoteProvider) for item in providers)
    assert not any(isinstance(item, EastMoneyStockFundFlowProvider) for item in providers)
    assert not any(isinstance(item, EastMoneyFinancialMainProvider) for item in providers)
    assert not any(isinstance(item, EastMoneyShareholderCountProvider) for item in providers)
    assert sum(isinstance(item, ThsBoardProvider) for item in providers) == 2
    assert all(item.request_interval_seconds == 3 for item in providers if isinstance(item, ThsBoardProvider))


def test_provider_factory_registers_optional_akshare_daily_providers() -> None:
    _, providers = load_provider_registry(
        ROOT / "config/providers.yaml", ROOT / "config/capabilities.yaml", include_unverified=True,
    )
    assert sum(isinstance(item, AkShareDailyProvider) for item in providers) == 4


def test_provider_factory_registers_sector_providers_as_validation_only() -> None:
    registry, providers = load_provider_registry(
        ROOT / "config/providers.yaml", ROOT / "config/capabilities.yaml", include_unverified=True,
    )
    assert any(isinstance(item, BaoStockIndustryMembershipProvider) for item in providers)
    board_providers = [item for item in providers if isinstance(item, AkShareBoardProvider)]
    assert {item.endpoint for item in board_providers} == {
        "industry_index_daily", "industry_fund_flow", "concept_fund_flow"
    }
    now = datetime.now(timezone.utc)
    selected = registry.select(
        now=now,
        dataset=Dataset.BOARD_FUND_FLOW,
        exchange=Exchange.XSHG,
        asset_type=AssetType.STOCK,
        symbol="sh600519",
        adjustment=Adjustment.NONE,
        include_validation_only=True,
    )
    assert selected == []
