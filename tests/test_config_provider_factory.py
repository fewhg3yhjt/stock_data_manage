from pathlib import Path

from stock_data_manage.config.loader import load_capability_routes, load_normalization_rules, load_provider_configs
from stock_data_manage.domain import Adjustment, Dataset
from stock_data_manage.routing.factory import load_provider_registry
from stock_data_manage.providers.tencent import TencentDailyProvider
from stock_data_manage.providers.baostock import BaoStockDailyProvider
from stock_data_manage.providers.eastmoney import EastMoneyCorporateActionProvider
from stock_data_manage.providers.akshare import AkShareDailyProvider


ROOT = Path(__file__).resolve().parents[1]


def test_provider_yaml_declares_tencent_qfq_and_routes() -> None:
    configs = load_provider_configs(ROOT / "config" / "providers.yaml")
    qfq = next(item for item in configs if item.provider == "tencent" and item.endpoint == "forward_history")
    assert qfq.adjustments == frozenset({Adjustment.FORWARD})
    assert qfq.datasets == frozenset({Dataset.DAILY_BAR})
    assert qfq.capability_version == "tencent-qfq-kline-v1"

    routes = load_capability_routes(ROOT / "config" / "capabilities.yaml")
    assert routes[Dataset.DAILY_BAR][0]["endpoint"] == "forward_history"


def test_normalization_yaml_declares_tencent_qfq_rules() -> None:
    rules = load_normalization_rules(ROOT / "config" / "normalization.yaml")
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
    )[0].endpoint == "forward_history"


def test_provider_factory_registers_baostock_validation_provider() -> None:
    _, providers = load_provider_registry(
        ROOT / "config" / "providers.yaml",
        ROOT / "config" / "capabilities.yaml",
    )
    provider = next(item for item in providers if isinstance(item, BaoStockDailyProvider))
    assert provider.endpoint == "daily_history"
    assert provider.capability_version == "baostock-daily-v1"
    assert provider.adjustment is Adjustment.NONE


def test_provider_factory_can_build_disabled_eastmoney_action_provider() -> None:
    _, providers = load_provider_registry(
        ROOT / "config" / "providers.yaml",
        ROOT / "config" / "capabilities.yaml",
    )
    assert not any(isinstance(item, EastMoneyCorporateActionProvider) for item in providers)


def test_provider_factory_registers_optional_akshare_daily_providers() -> None:
    _, providers = load_provider_registry(
        ROOT / "config/providers.yaml", ROOT / "config/capabilities.yaml"
    )
    assert sum(isinstance(item, AkShareDailyProvider) for item in providers) == 4
