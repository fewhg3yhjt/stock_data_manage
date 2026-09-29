from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import replace

from ..config.loader import ProviderConfig, load_capability_routes, load_provider_configs
from ..domain import Adjustment
from ..providers.sina import SinaDailyProvider, SinaMinuteProvider, SinaSnapshotProvider
from ..providers.tencent import TencentDailyProvider, TencentMinuteProvider, TencentSnapshotProvider
from ..providers.transport import HttpTransport, UrlLibTransport
from ..providers.tdx import TdxMinuteProvider
from ..providers.baostock import BaoStockDailyProvider, BaoStockMinuteProvider
from ..providers.eastmoney import (
    EastMoneyDividendProvider,
    EastMoneyRealtimeQuoteProvider,
    EastMoneySecurityListProvider,
    EastMoneyStockFundFlowProvider,
)
from ..providers.eastmoney.realtime import EastMoneyRequestsTransport
from ..providers.akshare import AkShareDailyProvider
from .capabilities import CapabilityRegistry


def build_provider(config: ProviderConfig, transport: HttpTransport | None = None, *, capability=None):
    transport = transport or UrlLibTransport()
    if config.provider == "tencent" and config.endpoint in {"daily_history", "recent_history", "forward_history"}:
        adjustment = Adjustment.FORWARD if Adjustment.FORWARD in config.adjustments else Adjustment.NONE
        provider = TencentDailyProvider(transport, adjustment=adjustment)
        provider.endpoint = config.endpoint
        provider.capability_version = config.capability_version
        return provider
    if config.provider == "tencent" and config.endpoint == "bulk_snapshot":
        return TencentSnapshotProvider(transport, capability or config.capability(now()))
    if config.provider == "tencent" and config.endpoint == "native_1m":
        return TencentMinuteProvider(transport, capability or config.capability(now()))
    if config.provider == "sina" and config.endpoint in {"daily_history", "full_history"}:
        provider = SinaDailyProvider(transport)
        provider.endpoint = config.endpoint
        provider.capability_version = config.capability_version
        return provider
    if config.provider == "sina" and config.endpoint == "snapshot":
        return SinaSnapshotProvider(transport, capability or config.capability(now()))
    if config.provider == "sina" and config.endpoint == "native_5m":
        return SinaMinuteProvider(transport, capability or config.capability(now()))
    if config.provider == "baostock" and config.endpoint == "daily_history":
        return BaoStockDailyProvider(
            capability_priority=config.priority,
            capability_version=config.capability_version,
            adjustment=next(
                (adjustment for adjustment in (Adjustment.NONE, Adjustment.FORWARD, Adjustment.BACKWARD)
                 if adjustment in config.adjustments),
                Adjustment.NONE,
            ),
            supported_asset_types=config.asset_types,
        )
    if config.provider == "baostock" and config.endpoint == "minute_5m":
        return BaoStockMinuteProvider(
            capability_version=config.capability_version,
            adjustment=next(iter(config.adjustments), Adjustment.NONE),
        )
    if config.provider == "eastmoney" and config.endpoint == "dividend_event":
        return EastMoneyDividendProvider(
            transport,
            capability_version=config.capability_version,
            page_size=config.max_symbols_per_request,
        )
    if config.provider == "eastmoney" and config.endpoint == "security_list":
        return EastMoneySecurityListProvider(
            transport,
            capability_version=config.capability_version,
            page_size=config.max_symbols_per_request,
        )
    if config.provider == "eastmoney" and config.endpoint in {"single_quote", "batch_quote", "intraday_trend"}:
        return EastMoneyRealtimeQuoteProvider(
            EastMoneyRequestsTransport(),
            endpoint=config.endpoint,
            capability_version=config.capability_version,
        )
    if config.provider == "eastmoney" and config.endpoint == "stock_fund_flow":
        return EastMoneyStockFundFlowProvider(capability_version=config.capability_version)
    if config.provider == "akshare" and config.endpoint in {
        "stock_daily", "etf_daily", "lof_daily", "index_daily"
    }:
        asset_type = {
            "stock_daily": __import__("stock_data_manage.domain", fromlist=["AssetType"]).AssetType.STOCK,
            "etf_daily": __import__("stock_data_manage.domain", fromlist=["AssetType"]).AssetType.ETF,
            "lof_daily": __import__("stock_data_manage.domain", fromlist=["AssetType"]).AssetType.LOF,
            "index_daily": __import__("stock_data_manage.domain", fromlist=["AssetType"]).AssetType.INDEX,
        }[config.endpoint]
        return AkShareDailyProvider(
            asset_type=asset_type,
            adjustment=next(iter(config.adjustments), Adjustment.NONE),
            capability_version=config.capability_version,
        )
    raise ValueError(f"no provider factory for {config.provider}.{config.endpoint}")


def load_provider_registry(
    path: str | Path,
    capabilities_path: str | Path | None = None,
    *,
    now: datetime | None = None,
):
    configs = load_provider_configs(path)
    routes = load_capability_routes(capabilities_path) if capabilities_path else {}
    now = now or datetime.now(timezone.utc)
    registry = CapabilityRegistry()
    providers = []
    for config in configs:
        if not config.enabled:
            continue
        priority = config.priority
        for route in routes.get(next(iter(config.datasets), None), ()):
            if route.get("provider") == config.provider and route.get("endpoint") == config.endpoint:
                priority = int(route.get("priority", priority))
        capability = replace(
            config.capability(validated_at=now - timedelta(days=1), validation_expires_at=now + timedelta(days=1)),
            priority=priority,
        )
        registry.register(capability)
        if config.endpoint in {
            "daily_history", "recent_history", "forward_history",
            "stock_daily", "etf_daily", "lof_daily", "index_daily",
        }:
            try:
                providers.append(build_provider(config))
            except ValueError:
                # Configured but unimplemented providers remain explicitly unavailable.
                continue
        elif config.endpoint in {"dividend_event", "security_list", "single_quote", "batch_quote", "intraday_trend", "stock_fund_flow"}:
            try:
                providers.append(build_provider(config))
            except ValueError:
                continue
    return registry, tuple(providers)
