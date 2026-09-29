from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from datetime import datetime, timezone, timedelta

import yaml

from ..domain import Adjustment, AssetType, Dataset, Exchange
from ..routing.capabilities import ProviderCapability
from ..quality.normalization import NormalizationRule


@dataclass(frozen=True, slots=True)
class ProviderConfig:
    provider: str
    endpoint: str
    enabled: bool
    role: str
    datasets: frozenset[Dataset]
    adjustments: frozenset[Adjustment]
    asset_types: frozenset[AssetType]
    exchanges: frozenset[Exchange]
    capability_version: str
    priority: int
    max_symbols_per_request: int
    max_rows_per_request: int | None
    request_interval_seconds: float
    effective_concurrency: int
    freshness_delay_seconds: int

    def now(self) -> ProviderCapability:
        now = datetime.now(timezone.utc)
        return self.capability(
            validated_at=now - timedelta(days=1),
            validation_expires_at=now + timedelta(days=1),
        )

    def capability(self, *, validated_at, validation_expires_at) -> ProviderCapability:
        return ProviderCapability(
            provider=self.provider,
            endpoint=self.endpoint,
            version=self.capability_version,
            datasets=self.datasets,
            exchanges=self.exchanges,
            asset_types=self.asset_types,
            frequencies=frozenset(),
            adjustments=self.adjustments,
            priority=self.priority,
            validated_at=validated_at,
            validation_expires_at=validation_expires_at,
            enabled=self.enabled,
            role=self.role,
            max_symbols_per_request=self.max_symbols_per_request,
            max_rows_per_request=self.max_rows_per_request,
            request_interval_seconds=self.request_interval_seconds,
            effective_concurrency=self.effective_concurrency,
            freshness_delay_seconds=self.freshness_delay_seconds,
        )


def load_provider_configs(path: str | Path) -> tuple[ProviderConfig, ...]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    result: list[ProviderConfig] = []
    for provider, provider_payload in payload.get("providers", {}).items():
        if not isinstance(provider_payload, dict):
            continue
        for endpoint, endpoint_payload in (provider_payload.get("endpoints") or {}).items():
            if not isinstance(endpoint_payload, dict):
                continue
            result.append(
                ProviderConfig(
                    provider=provider,
                    endpoint=endpoint,
                    enabled=bool(endpoint_payload.get("enabled", provider_payload.get("enabled", True))),
                    role=str(endpoint_payload.get("role", provider_payload.get("default_role", "primary"))),
                    datasets=frozenset(Dataset(value) for value in endpoint_payload.get("datasets", [])),
                    adjustments=frozenset(
                        Adjustment(value) for value in endpoint_payload.get("adjustments", ["none"])
                    ),
                    asset_types=frozenset(
                        AssetType(value) for value in endpoint_payload.get("asset_types", [item.value for item in AssetType])
                    ),
                    exchanges=frozenset(
                        Exchange(value) for value in endpoint_payload.get("exchanges", [item.value for item in Exchange])
                    ),
                    capability_version=str(endpoint_payload.get("capability_version", f"{provider}-{endpoint}-v1")),
                    priority=int(endpoint_payload.get("priority", 100)),
                    max_symbols_per_request=int(endpoint_payload.get("max_symbols_per_request", 1)),
                    max_rows_per_request=(
                        None
                        if endpoint_payload.get("max_rows_per_request") is None
                        else int(endpoint_payload["max_rows_per_request"])
                    ),
                    request_interval_seconds=float(endpoint_payload.get("request_interval_seconds", 1)),
                    effective_concurrency=int(endpoint_payload.get("effective_concurrency", 1)),
                    freshness_delay_seconds=int(endpoint_payload.get("freshness_delay_seconds", 0)),
                )
            )
    return tuple(result)


def load_capability_routes(path: str | Path) -> dict[Dataset, tuple[dict[str, object], ...]]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    result: dict[Dataset, tuple[dict[str, object], ...]] = {}
    for dataset_name, capability in (payload.get("capabilities") or {}).items():
        try:
            dataset = Dataset(dataset_name)
        except ValueError:
            continue
        result[dataset] = tuple(capability.get("routes") or [])
    return result


def load_normalization_rules(path: str | Path) -> tuple[NormalizationRule, ...]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    rules = []
    for item in payload.get("rules", []):
        rules.append(
            NormalizationRule(
                provider=str(item["provider"]),
                endpoint=str(item["endpoint"]),
                exchange=Exchange(str(item["exchange"])),
                asset_type=AssetType(str(item["asset_type"])),
                frequency=item.get("frequency"),
                volume_multiplier=__import__("decimal").Decimal(str(item["volume_multiplier"])),
                amount_multiplier=__import__("decimal").Decimal(str(item["amount_multiplier"])),
                version=str(item["version"]),
                source_bar_time_semantics=str(item.get("source_bar_time_semantics", "end_time")),
                volume_semantics=item.get("volume_semantics"),
                adjustment=Adjustment(str(item.get("adjustment", "none"))),
            )
        )
    return tuple(rules)


def load_dataset_normalization_rules(root: str | Path, dataset: str) -> tuple[NormalizationRule, ...]:
    """Load one dataset's rule file; the dataset file is the preferred source."""
    path = Path(root) / f"{dataset}.yaml"
    return load_normalization_rules(path)


def load_normalization_document(root: str | Path, dataset: str) -> dict[str, object]:
    path = Path(root) / f"{dataset}.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_normalization_rules_for_datasets(root: str | Path, datasets: tuple[str, ...]) -> tuple[NormalizationRule, ...]:
    rules: list[NormalizationRule] = []
    for dataset in datasets:
        rules.extend(load_dataset_normalization_rules(root, dataset))
    return tuple(rules)
