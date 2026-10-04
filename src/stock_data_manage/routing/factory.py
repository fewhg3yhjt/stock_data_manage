from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from dataclasses import replace
import json

from ..config.loader import ProviderConfig, load_capability_routes, load_provider_configs
from ..domain import Adjustment, Dataset, Exchange, AssetType
from ..providers.sina import SinaDailyProvider, SinaMinuteProvider, SinaSnapshotProvider
from ..providers.tencent import TencentDailyProvider, TencentMinuteProvider, TencentSnapshotProvider
from ..providers.transport import HttpTransport, UrlLibTransport, RequestPacer, PacedTransport, RequestsTransport
from ..providers.tdx import TdxMinuteProvider
from ..providers.baostock import BaoStockDailyProvider, BaoStockIndustryMembershipProvider, BaoStockMinuteProvider
from ..providers.eastmoney import (
    EastMoneyDividendProvider,
    EastMoneyRealtimeQuoteProvider,
    EastMoneySecurityListProvider,
    EastMoneyStockFundFlowProvider,
    EastMoneyFinancialMainProvider,
    EastMoneyShareholderCountProvider,
)
from ..providers.eastmoney.realtime import EastMoneyRequestsTransport
from ..providers.akshare import AkShareBoardProvider, AkShareDailyProvider
from ..providers.ths import ThsBoardProvider
from .capabilities import CapabilityRegistry


def build_input_provider(contract, *, providers_path, client=None):
    """Bind explicit validation inputs to existing adapters, outside production routing."""
    if contract.implementation_status != "implemented_validation_only":
        raise ValueError("input adapter is not implemented for validation")
    expected_methods = {"ASTOCK-001": "fetch_snapshot", "ASTOCK-002-daily": "fetch_window", "ASTOCK-002-5m": "fetch_recent",
                        "ASTOCK-045": "fetch", "ASTOCK-070": "fetch",
                        "ASTOCK-026": "fetch_history", "ASTOCK-027": "fetch_history", "ASTOCK-028": "fetch_history",
                        "ASTOCK-046": "fetch", "ASTOCK-047": "fetch", "ASTOCK-048": "fetch", "ASTOCK-050": "fetch",
                        "ASTOCK-078": "fetch_event_list", "ASTOCK-079": "fetch_event_list",
                        "ASTOCK-080": "fetch_action_list", "ASTOCK-081": "fetch_action_list", "ASTOCK-082": "fetch_action_list", "ASTOCK-083": "fetch_action_list",
                        "ASTOCK-065": "fetch_lpr_history",
                        "ASTOCK-032": "fetch", "ASTOCK-033": "fetch", "ASTOCK-034": "fetch", "ASTOCK-035": "fetch",
                        "ASTOCK-064": "fetch", "ASTOCK-084": "fetch_convertible_bonds",
                        "ASTOCK-061": "fetch", "ASTOCK-062": "fetch_pmi",
                        "ASTOCK-074": "fetch_futures", "ASTOCK-075": "fetch_futures_kline", "ASTOCK-076": "fetch_a50",
                        "SDA-BOARD-001": "fetch_industry_list", "SDA-BOARD-002": "fetch_industry_daily",
                        "SDA-BOARD-003": "fetch_fund_flow", "SDA-BOARD-004": "fetch_fund_flow",
                        "SDA-BOARD-005": "fetch_snapshot", "SDA-BOARD-006": "fetch_snapshot"}
    if contract.input_id not in expected_methods or contract.runtime_method != expected_methods[contract.input_id]:
        raise ValueError("input runtime method does not match its verified adapter")
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
               "Referer": "https://gu.qq.com/"}
    if contract.input_id == "ASTOCK-001":
        config = next(c for c in load_provider_configs(providers_path) if c.provider == "tencent" and c.endpoint == "bulk_snapshot")
        quote_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36",
                         "Accept": "*/*", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"}
        capability = replace(config.capability(), endpoint=contract.endpoint, version="tencent-snapshot-input-v2")
        return TencentSnapshotProvider(RequestsTransport(quote_headers, use_session=True), capability,
                                       endpoint=contract.endpoint, timeout_seconds=20,
                                       max_symbols_per_request=config.max_symbols_per_request)
    if contract.input_id == "ASTOCK-002-daily":
        provider = TencentDailyProvider(RequestsTransport(headers), adjustment=Adjustment.FORWARD)
        provider.endpoint, provider.capability_version = contract.endpoint, "tencent-window-input-v1"
        return provider
    if contract.input_id == "ASTOCK-002-5m":
        config = next(c for c in load_provider_configs(providers_path) if c.provider == "tencent" and c.endpoint == "native_1m")
        capability = replace(config.capability(), endpoint=contract.endpoint, version="tencent-recent-5m-input-v1",
                             datasets=frozenset({Dataset.MINUTE_BAR_5M}), frequencies=frozenset({5}))
        return TencentMinuteProvider(RequestsTransport(headers), capability, endpoint=contract.endpoint)
    if contract.input_id in {"ASTOCK-045", "ASTOCK-046", "ASTOCK-047", "ASTOCK-048", "ASTOCK-050"}:
        from ..providers.eastmoney.limit_pool import EastMoneyLimitUpProvider
        return EastMoneyLimitUpProvider(client=client, endpoint=contract.endpoint,
            capability_version="eastmoney-limit-up-input-v1" if contract.input_id == "ASTOCK-045" else "eastmoney-stock-pool-input-v1")
    if contract.input_id == "ASTOCK-070":
        from ..providers.sina.calendar import SinaTradingCalendarProvider
        return SinaTradingCalendarProvider(client=client)
    if contract.input_id == "ASTOCK-034":
        from ..providers.wallstreetcn.news import WallStreetCNNewsProvider
        return WallStreetCNNewsProvider()
    if contract.input_id == "ASTOCK-032":
        from ..providers.cls.news import CLSTelegraphProvider
        return CLSTelegraphProvider(client=client)
    if contract.input_id == "ASTOCK-033":
        from ..providers.sina.news import SinaGlobalNewsProvider
        return SinaGlobalNewsProvider(client=client)
    if contract.input_id == "ASTOCK-035":
        from ..providers.cctv.news import CCTVNewsProvider
        return CCTVNewsProvider()
    if contract.input_id == "ASTOCK-064":
        from ..providers.chinamoney.rates import ChinamoneyRepoRateProvider
        return ChinamoneyRepoRateProvider()
    if contract.input_id == "ASTOCK-061":
        from ..providers.mofcom.social_financing import MofcomSocialFinancingProvider
        return MofcomSocialFinancingProvider(client=client)
    if contract.input_id == "ASTOCK-062":
        return EastMoneyFinancialMainProvider(endpoint=contract.endpoint,client=client,capability_version="eastmoney-pmi-input-v1")
    if contract.input_id == "ASTOCK-084":
        return EastMoneyFinancialMainProvider(endpoint=contract.endpoint, capability_version="eastmoney-cb-input-v1")
    if contract.input_id == "ASTOCK-075":
        provider = SinaDailyProvider(RequestsTransport({}))
        provider.endpoint, provider.capability_version = contract.endpoint, "sina-futures-kline-input-v1"
        return provider
    if contract.input_id in {"ASTOCK-074", "ASTOCK-076"}:
        config = next(c for c in load_provider_configs(providers_path) if c.provider == "sina" and c.endpoint == "full_history")
        capability = replace(config.capability(),endpoint=contract.endpoint,version="sina-futures-quote-input-v1")
        return SinaSnapshotProvider(RequestsTransport({}),capability,endpoint=contract.endpoint)
    if contract.input_id in {"ASTOCK-065", "ASTOCK-078", "ASTOCK-079", "ASTOCK-080", "ASTOCK-081", "ASTOCK-082", "ASTOCK-083"}:
        return EastMoneyFinancialMainProvider(endpoint=contract.endpoint,
            capability_version="eastmoney-lpr-input-v1" if contract.input_id == "ASTOCK-065" else "eastmoney-events-input-v1")
    if contract.input_id in {"ASTOCK-026", "ASTOCK-027", "ASTOCK-028"}:
        from ..providers.eastmoney.shareholder import EastMoneyShareholderCountProvider
        from ..providers.eastmoney.dividend import EastMoneyDividendProvider
        from ..providers.eastmoney.fund_flow import EastMoneyStockFundFlowProvider
        adapter = {"ASTOCK-026": EastMoneyShareholderCountProvider, "ASTOCK-027": EastMoneyDividendProvider,
                   "ASTOCK-028": EastMoneyStockFundFlowProvider}[contract.input_id]
        # fetch_history uses the original SDK; this legacy transport is never invoked by that method.
        return adapter(transport=RequestsTransport({}), client=client, endpoint=contract.endpoint,
                       capability_version="eastmoney-history-input-v1")
    if contract.input_id in {"SDA-BOARD-005", "SDA-BOARD-006"}:
        return BaoStockIndustryMembershipProvider(client=client, endpoint=contract.endpoint,
                                                  normalization_root=Path(providers_path).parent / "normalization")
    if contract.input_id.startswith("SDA-BOARD-"):
        return AkShareBoardProvider(client=client, endpoint=contract.endpoint,
                                   normalization_root=Path(providers_path).parent / "normalization")
    raise ValueError("no executable input adapter for this input ID")


def build_provider(config: ProviderConfig, transport: HttpTransport | None = None, *, capability=None, pacer=None):
    if config.implementation_status != "implemented":
        raise ValueError(f"provider is unimplemented: {config.provider}.{config.endpoint}")
    pacer = pacer or RequestPacer()
    group = config.request_group or config.provider
    pacer.configure(group, config.request_interval_seconds, config.effective_concurrency)
    # Keep the source-specific transport; pacing only surrounds its existing call.
    base_transport = transport or (
        EastMoneyRequestsTransport()
        if config.provider == "eastmoney" and config.endpoint in {
            "single_quote", "batch_quote", "intraday_trend", "stock_fund_flow", "financial_main", "shareholder_count"
        } else UrlLibTransport()
    )
    transport = PacedTransport(base_transport, pacer, group)
    if config.provider == "tencent" and config.endpoint in {"daily_history", "recent_history", "forward_history"}:
        adjustment = Adjustment.FORWARD if Adjustment.FORWARD in config.adjustments else Adjustment.NONE
        provider = TencentDailyProvider(transport, adjustment=adjustment)
        provider.endpoint = config.endpoint
        provider.capability_version = config.capability_version
        return provider
    if config.provider == "tencent" and config.endpoint == "bulk_snapshot":
        return TencentSnapshotProvider(transport, capability or config.capability(), max_symbols_per_request=config.max_symbols_per_request)
    if config.provider == "tencent" and config.endpoint == "native_1m":
        return TencentMinuteProvider(transport, capability or config.capability())
    if config.provider == "sina" and config.endpoint in {"daily_history", "full_history"}:
        provider = SinaDailyProvider(transport)
        provider.endpoint = config.endpoint
        provider.capability_version = config.capability_version
        return provider
    if config.provider == "sina" and config.endpoint == "snapshot":
        return SinaSnapshotProvider(transport, capability or config.capability())
    if config.provider == "sina" and config.endpoint == "native_5m":
        return SinaMinuteProvider(transport, capability or config.capability())
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
    if config.provider == "baostock" and config.endpoint == "industry_membership":
        return BaoStockIndustryMembershipProvider(capability_version=config.capability_version)
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
            transport,
            endpoint=config.endpoint,
            capability_version=config.capability_version,
        )
    if config.provider == "eastmoney" and config.endpoint == "stock_fund_flow":
        return EastMoneyStockFundFlowProvider(transport=transport, capability_version=config.capability_version)
    if config.provider == "eastmoney" and config.endpoint == "financial_main":
        return EastMoneyFinancialMainProvider(transport=transport, capability_version=config.capability_version)
    if config.provider == "eastmoney" and config.endpoint == "shareholder_count":
        return EastMoneyShareholderCountProvider(transport=transport, capability_version=config.capability_version)
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
    if config.provider == "akshare" and config.endpoint in {
        "industry_index_daily", "industry_fund_flow", "concept_fund_flow"
    }:
        return AkShareBoardProvider(
            endpoint=config.endpoint,
            capability_version=config.capability_version,
        )
    if config.provider == "ths" and config.endpoint in {"industry_board", "concept_board"}:
        return ThsBoardProvider(
            capability_version=config.capability_version,
            request_interval_seconds=config.request_interval_seconds,
        )
    raise ValueError(f"no provider factory for {config.provider}.{config.endpoint}")


def _evidence_capability(config, evidence, *, now, priority, role, metadata):
    try:
        dataset, exchange, asset_type = Dataset(evidence["dataset"]), Exchange(evidence["market"]), AssetType(evidence["asset_type"])
        adjustment = Adjustment(evidence["adjustment"])
        if dataset not in config.datasets or exchange not in config.exchanges or asset_type not in config.asset_types or adjustment not in config.adjustments:
            return None
        if role in {"primary", "fallback", "discovery"} and config.request_limit_enforcement != "physical_request":
            return None
        if evidence["status"] not in {"complete", "truncated"} or not evidence["eligible_for_selection"] or not evidence["evidence_hash"] or evidence["row_count"] <= 0:
            return None
        validated, expires = evidence["validated_at"], evidence["validation_expires_at"]
        if validated.tzinfo is None or expires.tzinfo is None or not validated <= now < expires:
            return None
        scope = json.loads(evidence["request_scope_json"])
        units = json.loads(evidence["units_json"])
        fields = json.loads(evidence["field_semantics_json"])
        if not isinstance(scope, list) or not scope or not all(isinstance(s, str) and s.strip() for s in scope):
            return None
        if not isinstance(fields, list) or not fields or not all(isinstance(f, str) and f.strip() for f in fields):
            return None
        if not isinstance(units, list) or not units:
            return None
        if any(not isinstance(u, str) or not u.strip() or any(word in u.lower() for word in ("unverified", "unconfirmed", "unknown")) for u in units):
            return None
        if evidence.get("response_status") is not None and evidence["response_status"] >= 400:
            return None
        frequency = str(evidence["frequency"])
        if dataset == Dataset.DAILY_BAR and frequency != "daily":
            return None
        frequencies = frozenset() if frequency in {"daily", "snapshot"} else frozenset({int(frequency.removesuffix("m"))})
        if config.frequencies and not frequencies <= config.frequencies:
            return None
        if dataset in {Dataset.MINUTE_BAR_1M, Dataset.MINUTE_BAR_5M} and not frequencies:
            return None
        if (dataset == Dataset.MINUTE_BAR_1M and frequencies != frozenset({1})) or (dataset == Dataset.MINUTE_BAR_5M and frequencies != frozenset({5})):
            return None
        health = metadata.provider_health(
            provider=config.provider, endpoint=config.endpoint, capability_version=config.capability_version,
            market=exchange.value, asset_type=asset_type.value, dataset=dataset.value,
        )
        if not metadata.provider_available(health, now):
            return None
        return replace(
            config.capability(validated_at=validated, validation_expires_at=expires),
            datasets=frozenset({dataset}), exchanges=frozenset({exchange}), asset_types=frozenset({asset_type}),
            adjustments=frozenset({adjustment}), frequencies=frequencies,
            validated_symbols=frozenset(scope), evidence_hash=str(evidence["evidence_hash"]),
            priority=priority, role=role,
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        # Malformed/unknown evidence cannot widen the declared scope.
        return None


def load_provider_registry(
    path: str | Path,
    capabilities_path: str | Path | None = None,
    *,
    now: datetime | None = None,
    metadata=None,
    include_unverified: bool = False,
):
    configs = load_provider_configs(path)
    # Capacity estimates must use the same strictest shared policy as the pacer.
    shared_policies = {}
    for config in configs:
        if config.enabled and config.implementation_status == "implemented":
            group = config.request_group or config.provider
            previous = shared_policies.get(group, (0.0, config.effective_concurrency))
            shared_policies[group] = (
                max(previous[0], config.request_interval_seconds),
                min(previous[1], config.effective_concurrency),
            )
    configs = tuple(
        replace(config, request_interval_seconds=shared_policies[config.request_group or config.provider][0],
                effective_concurrency=shared_policies[config.request_group or config.provider][1])
        if config.enabled and config.implementation_status == "implemented" else config
        for config in configs
    )
    routes = load_capability_routes(capabilities_path) if capabilities_path else {}
    now = now or datetime.now(timezone.utc)
    def available(capability, selection_time):
        if metadata is None:
            return True
        config = next(c for c in configs if c.provider == capability.provider and c.endpoint == capability.endpoint)
        for evidence in metadata.capability_evidence(provider=capability.provider, endpoint=capability.endpoint, capability_version=capability.version):
            if evidence.get("evidence_hash") != capability.evidence_hash:
                continue
            current = _evidence_capability(config, evidence, now=selection_time, priority=capability.priority,
                                           role=capability.role, metadata=metadata)
            if current is not None and (current.datasets, current.exchanges, current.asset_types, current.adjustments, current.frequencies, current.validated_symbols) == (
                capability.datasets, capability.exchanges, capability.asset_types, capability.adjustments, capability.frequencies, capability.validated_symbols
            ):
                return True
        return False

    registry = CapabilityRegistry(availability=available)
    providers = []
    pacer = RequestPacer()
    for config in configs:
        if config.enabled and config.implementation_status == "implemented":
            pacer.configure(config.request_group or config.provider, config.request_interval_seconds, config.effective_concurrency)
    for config in configs:
        if not config.enabled or config.implementation_status != "implemented":
            continue
        priority = config.priority
        eligible = []
        if metadata is not None:
            for evidence in metadata.capability_evidence(provider=config.provider, endpoint=config.endpoint, capability_version=config.capability_version):
                try:
                    dataset = Dataset(evidence["dataset"])
                except (ValueError, KeyError):
                    continue
                route = next((r for r in routes.get(dataset, ()) if r.get("provider") == config.provider and r.get("endpoint") == config.endpoint), {})
                route_config = replace(config, adjustments=config.adjustments & frozenset(Adjustment(v) for v in route.get("adjustments", config.adjustments)))
                capability = _evidence_capability(route_config, evidence, now=now, priority=int(route.get("priority", priority)), role=str(route.get("role", config.role)), metadata=metadata)
                if capability is not None:
                    eligible.append(capability)
        if not eligible and not include_unverified:
            continue
        for capability in eligible or [config.capability()]:
            try:
                provider_config = replace(config, adjustments=capability.adjustments)
                provider = build_provider(provider_config, capability=capability, pacer=pacer)
            except ValueError:
                # Configured but unimplemented providers remain explicitly unavailable.
                continue
            registry.register(capability)
            providers.append(provider)
    return registry, tuple(providers)
