from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import date, datetime, timezone
from typing import Any, Mapping
import math

import yaml

from ..domain import Adjustment, AssetType, Dataset, Exchange
from ..routing.capabilities import ProviderCapability
from ..quality.normalization import NormalizationRule


@dataclass(frozen=True, slots=True)
class ParameterBinding:
    name: str
    source: str
    value_type: str
    required: bool = False
    default: Any = None
    choices: tuple[Any, ...] = ()
    minimum: float | None = None
    maximum: float | None = None
    applied_at: str = "request"

    def __post_init__(self) -> None:
        namespace, separator, key = self.source.partition(".")
        if not separator or not key or "." in key or namespace not in {
            "request", "config", "calendar", "metadata", "dependency", "credential_ref"
        }:
            raise ValueError(f"unsupported parameter source: {self.source}")
        if self.value_type not in {"string", "integer", "number", "boolean", "date", "datetime", "strings", "records"}:
            raise ValueError(f"unsupported parameter type: {self.value_type}")
        if namespace == "credential_ref" and self.value_type != "string":
            raise ValueError("credential_ref must contain a reference name, not a credential value")
        if self.applied_at not in {"request", "local_filter"}:
            raise ValueError(f"unsupported parameter application: {self.applied_at}")
        if (self.minimum is not None or self.maximum is not None) and self.value_type not in {"integer", "number"}:
            raise ValueError("numeric bounds require a numeric parameter")

    def resolve(self, context: Mapping[str, Mapping[str, Any]]) -> Any:
        namespace, key = self.source.split(".")
        value = context.get(namespace, {}).get(key, self.default)
        if value is None:
            if self.required:
                raise ValueError(f"missing required parameter: {self.name} ({self.source})")
            return None
        if self.value_type == "date":
            if isinstance(value, str):
                value = date.fromisoformat(value)
            valid = isinstance(value, date) and not isinstance(value, datetime)
        elif self.value_type == "datetime":
            if isinstance(value, str):
                value = datetime.fromisoformat(value)
            valid = isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None
        elif self.value_type == "strings":
            valid = isinstance(value, (list, tuple)) and bool(value) and all(isinstance(v, str) and v.strip() for v in value)
            if valid:
                value = tuple(value)
        elif self.value_type == "records":
            valid = isinstance(value, (list, tuple)) and all(isinstance(v, Mapping) for v in value)
        elif self.value_type == "number":
            valid = type(value) in {int, float} and math.isfinite(value)
        else:
            valid = {"string": lambda: isinstance(value, str) and bool(value.strip()),
                     "integer": lambda: type(value) is int,
                     "boolean": lambda: type(value) is bool}[self.value_type]()
        if not valid:
            raise ValueError(f"invalid {self.value_type} parameter: {self.name}")
        if self.choices and value not in self.choices:
            raise ValueError(f"unsupported value for parameter: {self.name}")
        if (self.minimum is not None and value < self.minimum) or (self.maximum is not None and value > self.maximum):
            raise ValueError(f"parameter outside allowed range: {self.name}")
        return value


def _parameter_bindings(payload: Mapping[str, Any]) -> tuple[ParameterBinding, ...]:
    return tuple(ParameterBinding(
        name=name, source=str(item["source"]), value_type=str(item["type"]),
        required=bool(item.get("required", False)), default=item.get("default"),
        choices=tuple(item.get("choices", ())), minimum=item.get("minimum"), maximum=item.get("maximum"),
        applied_at=str(item.get("applied_at", "request")),
    ) for name, item in payload.items())


@dataclass(frozen=True, slots=True)
class InputCapabilityConfig:
    """A source contract in providers.yaml; it never grants runtime routing eligibility."""
    input_id: str
    provider: str
    endpoint: str
    display_name: str
    data_kind: str
    data_frequency: str
    request_shape: str
    collection_profile: str
    implementation_status: str
    evidence_refs: tuple[str, ...]
    parameters: tuple[ParameterBinding, ...]
    limitations: tuple[str, ...]
    canonical_input: str | None = None
    runtime_endpoint: str | None = None
    forbidden_sources: tuple[str, ...] = ()
    trading_date_parameter: str | None = None
    request_interval_seconds: float = 3.0
    effective_concurrency: int = 1
    request_limit_enforcement: str = "unimplemented"
    base_requests_per_fetch: int = 1
    dataset: str | None = None
    runtime_method: str | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.request_interval_seconds) or self.request_interval_seconds < 0 or self.effective_concurrency < 1 or self.base_requests_per_fetch < 1:
            raise ValueError("invalid input request policy")
        if self.request_limit_enforcement not in {"unimplemented", "call_boundary_only", "physical_request"}:
            raise ValueError("invalid input request enforcement")
        if self.request_shape not in {"single_symbol", "symbol_batch", "date_snapshot", "full_snapshot", "file_package", "paged_list"}:
            raise ValueError("invalid input request shape")
        if self.data_frequency not in {"daily", "5m", "snapshot", "event", "report_period", "monthly", "tick"}:
            raise ValueError("invalid input data frequency")
        if not self.evidence_refs:
            raise ValueError("input capability requires evidence references")

    def bind_parameters(self, context: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
        for source in self.forbidden_sources:
            namespace, key = source.split(".")
            if context.get(namespace, {}).get(key) is not None:
                raise ValueError(f"{self.input_id} does not support {source}")
        values = {binding.name: binding.resolve(context) for binding in self.parameters}
        values = {name: value for name, value in values.items() if value is not None}
        if "start" in values and "end" in values and values["end"] < values["start"]:
            raise ValueError("end must not precede start")
        if self.trading_date_parameter and self.trading_date_parameter in values:
            known_dates = context.get("calendar", {}).get("trading_dates")
            if known_dates is None:
                raise ValueError("explicit trading calendar coverage is required")
            if values[self.trading_date_parameter] not in known_dates:
                raise ValueError("requested date is not a known trading day")
        return values


def load_input_capabilities(path: str | Path) -> tuple[InputCapabilityConfig, ...]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    result = []
    for input_id, item in payload.get("input_capabilities", {}).items():
        status = str(item["implementation_status"])
        if status not in {"unimplemented", "migration_pending", "implemented_validation_only", "alias", "blocked"}:
            raise ValueError(f"unsupported input implementation status: {status}")
        result.append(InputCapabilityConfig(
            input_id=input_id, provider=str(item["provider"]), endpoint=str(item["endpoint"]),
            display_name=str(item["display_name"]), data_kind=str(item["data_kind"]),
            data_frequency=str(item["data_frequency"]), request_shape=str(item["request_shape"]),
            collection_profile=str(item["collection_profile"]), implementation_status=status,
            evidence_refs=tuple(item["evidence_refs"]), parameters=_parameter_bindings(item.get("parameters", {})),
            limitations=tuple(item.get("limitations", ())), canonical_input=item.get("canonical_input"),
            runtime_endpoint=item.get("runtime_endpoint"), forbidden_sources=tuple(item.get("forbidden_sources", ())),
            trading_date_parameter=item.get("trading_date_parameter"),
            request_interval_seconds=float(item.get("request_interval_seconds", 3)),
            effective_concurrency=int(item.get("effective_concurrency", 1)),
            request_limit_enforcement=str(item.get("request_limit_enforcement", "unimplemented")),
            base_requests_per_fetch=int(item.get("base_requests_per_fetch", 1)),
            dataset=item.get("dataset"),
            runtime_method=item.get("runtime_method"),
        ))
    ids = {item.input_id for item in result}
    if any(item.canonical_input and item.canonical_input not in ids for item in result):
        raise ValueError("input alias points to an unknown input")
    by_id = {item.input_id: item for item in result}
    for item in result:
        visited = {item.input_id}
        target = item.canonical_input
        while target is not None:
            if target in visited:
                raise ValueError("cyclic input aliases")
            visited.add(target)
            target = by_id[target].canonical_input
    return tuple(result)


@dataclass(frozen=True, slots=True)
class CollectionProfile:
    name: str
    mode: str
    refresh_interval_seconds: float | None
    business_day: str
    scheduling_enabled: bool = False


def load_collection_profiles(path: str | Path) -> tuple[CollectionProfile, ...]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    profiles = []
    for name, item in payload.get("collection_profiles", {}).items():
        if item.get("scheduling_enabled", False):
            raise ValueError("input collection profiles describe intent; scheduling is not implemented")
        mode = str(item["mode"])
        if mode not in {"intraday", "after_close", "daily", "periodic", "manual"}:
            raise ValueError(f"unsupported collection mode: {mode}")
        interval = item.get("refresh_interval_seconds")
        if interval is not None and (type(interval) not in {int, float} or not math.isfinite(interval) or interval <= 0):
            raise ValueError("refresh interval must be finite and positive")
        business_day = str(item["business_day"])
        if business_day not in {"trading_day", "calendar_day", "source_calendar"}:
            raise ValueError(f"unsupported business day: {business_day}")
        profiles.append(CollectionProfile(name, mode, interval, business_day))
    return tuple(profiles)


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
    frequencies: frozenset[int] = frozenset()
    max_days_per_request: int | None = None
    supports_pagination: bool = False
    max_pages_per_request: int | None = None
    request_shape: str = "symbol_batch"
    base_requests_per_fetch: int = 1
    request_group: str = ""
    parameters: tuple[ParameterBinding, ...] = ()
    implementation_status: str = "implemented"
    request_limit_enforcement: str = "unimplemented"

    def __post_init__(self) -> None:
        if self.max_symbols_per_request < 1 or self.effective_concurrency < 1 or self.base_requests_per_fetch < 1:
            raise ValueError("request sizes and concurrency must be positive")
        if not math.isfinite(self.request_interval_seconds) or self.request_interval_seconds < 0:
            raise ValueError("request interval must be finite and non-negative")
        if self.request_shape not in {"single_symbol", "symbol_batch", "date_snapshot", "full_snapshot", "file_package", "paged_list"}:
            raise ValueError(f"unsupported request shape: {self.request_shape}")
        if self.implementation_status not in {"implemented", "unimplemented"}:
            raise ValueError(f"unsupported provider implementation status: {self.implementation_status}")
        if self.role not in {"primary", "fallback", "validation_only", "discovery", "disabled"}:
            raise ValueError(f"unsupported provider role: {self.role}")
        if any(frequency < 1 for frequency in self.frequencies):
            raise ValueError("bar frequencies must be positive")
        if self.request_limit_enforcement not in {"unimplemented", "call_boundary_only", "physical_request"}:
            raise ValueError("invalid provider request enforcement")
        for limit in (self.max_rows_per_request, self.max_days_per_request, self.max_pages_per_request):
            if limit is not None and limit < 1:
                raise ValueError("request limits must be positive")

    def capability(self, *, validated_at=None, validation_expires_at=None) -> ProviderCapability:
        # An adapter may be built for a probe, but declaration is never fresh evidence.
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return ProviderCapability(
            provider=self.provider,
            endpoint=self.endpoint,
            version=self.capability_version,
            datasets=self.datasets,
            exchanges=self.exchanges,
            asset_types=self.asset_types,
            frequencies=self.frequencies,
            adjustments=self.adjustments,
            priority=self.priority,
            validated_at=validated_at or epoch,
            validation_expires_at=validation_expires_at or epoch,
            enabled=self.enabled,
            role=self.role,
            max_symbols_per_request=self.max_symbols_per_request,
            max_rows_per_request=self.max_rows_per_request,
            request_interval_seconds=self.request_interval_seconds,
            effective_concurrency=self.effective_concurrency,
            freshness_delay_seconds=self.freshness_delay_seconds,
            max_days_per_request=self.max_days_per_request,
            supports_pagination=self.supports_pagination,
            max_pages_per_request=self.max_pages_per_request,
            request_shape=self.request_shape,
            base_requests_per_fetch=self.base_requests_per_fetch,
        )

    def bind_parameters(self, context: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
        values = {binding.name: binding.resolve(context) for binding in self.parameters}
        values = {name: value for name, value in values.items() if value is not None}
        if "start_date" in values and "end_date" in values and values["end_date"] < values["start_date"]:
            raise ValueError("end_date must not precede start_date")
        return values


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
                    enabled=bool(provider_payload.get("enabled", True)) and bool(endpoint_payload.get("enabled", True)),
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
                    frequencies=frozenset(int(v) for v in endpoint_payload.get("frequencies", [])),
                    max_days_per_request=endpoint_payload.get("max_days_per_request"),
                    supports_pagination=bool(endpoint_payload.get("supports_pagination", False)),
                    max_pages_per_request=endpoint_payload.get("max_pages_per_request"),
                    request_shape=str(endpoint_payload.get("request_shape", "symbol_batch")),
                    base_requests_per_fetch=int(endpoint_payload.get("base_requests_per_fetch", 1)),
                    request_group=str(endpoint_payload.get("request_group", provider)),
                    parameters=_parameter_bindings(endpoint_payload.get("parameters", {})),
                    implementation_status=str(endpoint_payload.get("implementation_status", "implemented")),
                    request_limit_enforcement=str(endpoint_payload.get("request_limit_enforcement", "unimplemented")),
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
                field_mapping=dict(item.get("field_mapping", {})),
                code_prefixes=tuple(item.get("code_prefixes", ())),
                status=str(item.get("status", "pending_validation")),
                null_values=tuple(item.get("null_values", (None, ""))),
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


def load_input_field_contract(root: str | Path, capability: InputCapabilityConfig):
    """Use existing dataset/normalization files; input ID disambiguates source-specific rules."""
    if not capability.dataset or not capability.dataset.replace("_", "").isalnum():
        raise ValueError("input has no valid dataset field contract")
    root = Path(root)
    dataset = yaml.safe_load((root / "datasets" / f"{capability.dataset}.yaml").read_text(encoding="utf-8"))
    document = load_normalization_document(root / "normalization", capability.dataset)
    matches = [rule for rule in document.get("rules", ()) if rule.get("input_id") == capability.input_id
               and rule.get("provider") == capability.provider and rule.get("endpoint") == capability.endpoint]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one input mapping rule, found {len(matches)}")
    rule = matches[0]
    fields = dataset.get("fields", {})
    if not fields or not dataset.get("dataset", {}).get("primary_key"):
        raise ValueError("dataset must define fields and primary key")
    if set(dataset["dataset"]["primary_key"]) - set(fields):
        raise ValueError("dataset primary key contains undefined fields")
    if set(rule.get("field_mapping", {})) - set(fields):
        raise ValueError("mapping targets contain undefined fields")
    if rule.get("status") not in {"validated", "pending_validation", "disabled", "expired"}:
        raise ValueError("invalid input rule status")
    return dataset, rule


def load_normalization_rules_for_datasets(root: str | Path, datasets: tuple[str, ...]) -> tuple[NormalizationRule, ...]:
    rules: list[NormalizationRule] = []
    for dataset in datasets:
        rules.extend(load_dataset_normalization_rules(root, dataset))
    return tuple(rules)
