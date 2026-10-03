from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ..domain import Adjustment, AssetType, BarRecord, Dataset, Exchange, QualityStatus


SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class NormalizationRule:
    provider: str
    endpoint: str
    exchange: Exchange
    asset_type: AssetType
    frequency: int | None
    volume_multiplier: Decimal
    amount_multiplier: Decimal
    version: str
    code_prefixes: tuple[str, ...] = ()
    source_bar_time_semantics: str = "end_time"
    volume_semantics: str | None = None
    adjustment: Adjustment = Adjustment.NONE
    field_mapping: Mapping[str, str] = field(default_factory=dict)
    status: str = "validated"
    null_values: tuple[Any, ...] = (None, "")

    def __post_init__(self) -> None:
        if self.status not in {"validated", "pending_validation", "disabled", "expired"}:
            raise NormalizationError(f"invalid normalization status: {self.status}")
        if any(not isinstance(k, str) or not isinstance(v, str) or not v for k, v in self.field_mapping.items()):
            raise NormalizationError("field mapping must map target names to source names")
        if not self.volume_multiplier.is_finite() or not self.amount_multiplier.is_finite():
            raise NormalizationError("unit multipliers must be finite")

    def matches(
        self,
        *,
        provider: str,
        endpoint: str,
        exchange: Exchange,
        asset_type: AssetType,
        source_symbol: str,
        frequency: int | None,
        adjustment: Adjustment,
    ) -> bool:
        return (
            self.provider == provider
            and self.endpoint == endpoint
            and self.exchange == exchange
            and self.asset_type == asset_type
            and self.frequency == frequency
            and self.adjustment == adjustment
            and (not self.code_prefixes or source_symbol.startswith(self.code_prefixes))
        )


class NormalizationError(ValueError):
    pass


class Normalizer:
    def __init__(self, rules: list[NormalizationRule], *, allow_pending: bool = False) -> None:
        self._rules = list(rules)
        self.allow_pending = allow_pending

    def _find_rule(self, **criteria: Any) -> NormalizationRule:
        matches = [rule for rule in self._rules if rule.matches(**criteria)]
        if len(matches) != 1:
            raise NormalizationError(
                f"expected exactly one normalization rule, found {len(matches)}"
            )
        rule = matches[0]
        if rule.status != "validated" and not (self.allow_pending and rule.status == "pending_validation"):
            raise NormalizationError(f"normalization rule is not validated: {rule.version}")
        return rule

    @staticmethod
    def map_fields(raw: Mapping[str, Any], mapping: Mapping[str, str], *, null_values=(None, "")) -> dict[str, Any]:
        """Only names are mapped here; no expressions or implicit unit conversions."""
        values = dict(raw) if not mapping else {target: raw.get(source) for target, source in mapping.items()}
        return {name: None if value in null_values else value for name, value in values.items()}

    @staticmethod
    def normalize_fields(raw, *, rule, fields, context=None, allow_pending=False):
        """Execute a dataset's existing YAML field/transform contract, including non-bar rows."""
        status = rule.get("status", "pending_validation")
        if status != "validated" and not (allow_pending and status == "pending_validation"):
            raise NormalizationError("field rule is not validated")
        mapping = rule.get("field_mapping", {})
        if not isinstance(mapping, Mapping) or any(not isinstance(v, str) for v in mapping.values()):
            raise NormalizationError("invalid field mapping")
        unknown = set(mapping) - set(fields)
        unknown |= set(rule.get("transforms", {})) - set(fields)
        unknown |= set(rule.get("unverified_fields", ())) - set(fields)
        if unknown:
            raise NormalizationError(f"mapping targets absent from dataset: {sorted(unknown)}")
        # A source unit can be unverified while its numeric validity is still mandatory.
        for source in rule.get("source_required_numeric", ()):
            try:
                value = raw[source]
                if isinstance(value, bool) or not Decimal(str(value).replace(",", "").strip()).is_finite():
                    raise ValueError("finite source number required")
            except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
                raise NormalizationError(f"invalid required source number: {source}") from exc
        values = Normalizer.map_fields(raw, mapping, null_values=tuple(rule.get("null_values", (None, ""))))
        # Explicit bindings supply request dates/identity; never inferred from wall-clock time.
        for name, key in rule.get("context_fields", {}).items():
            if name not in fields or context is None or key not in context:
                raise NormalizationError(f"missing field context: {name}")
            values[name] = context[key]
        result = {}
        for name, definition in fields.items():
            value = values.get(name)
            if name in rule.get("unverified_fields", ()):
                if definition.get("required", False):
                    raise NormalizationError(f"required field semantics are unverified: {name}")
                # Retained in source rows; do not label an unknown unit as the dataset's canonical unit.
                result[name] = None
                continue
            transform = rule.get("transforms", {}).get(name, {})
            if set(transform) - {"format", "timezone", "multiplier", "remove_commas", "nonfinite_is_null", "fallback_source", "value_mapping", "zero_is_null"}:
                raise NormalizationError(f"unsupported transform for {name}")
            if value is None and "fallback_source" in transform:
                if not isinstance(transform["fallback_source"], str):
                    raise NormalizationError(f"invalid fallback source for {name}")
                value = raw.get(transform["fallback_source"])
            if "value_mapping" in transform:
                if not isinstance(transform["value_mapping"], Mapping):
                    raise NormalizationError(f"invalid value mapping for {name}")
                if value is not None:
                    value = transform["value_mapping"].get(str(value))
            if transform.get("zero_is_null") and value is not None:
                try:
                    if not isinstance(value, bool) and Decimal(str(value)) == 0:
                        value = None
                except InvalidOperation:
                    pass
            if value is None:
                if definition.get("required", False):
                    raise NormalizationError(f"missing required field: {name}")
                result[name] = None
                continue
            try:
                kind = definition["type"]
                if kind in {"decimal", "integer"}:
                    if isinstance(value, bool):
                        raise ValueError("boolean is not numeric")
                    text = str(value).replace(",", "") if transform.get("remove_commas", False) else str(value)
                    value = Decimal(text)
                    if not value.is_finite():
                        if transform.get("nonfinite_is_null") and not definition.get("required", False):
                            result[name] = None
                            continue
                        raise ValueError("non-finite number")
                    if kind == "integer":
                        if value != value.to_integral_value():
                            raise ValueError("fractional integer")
                        value = int(value)
                elif kind == "date":
                    value = datetime.strptime(str(value), transform["format"]).date() if "format" in transform else _as_date(value)
                elif kind == "datetime":
                    value = datetime.strptime(str(value), transform["format"]) if "format" in transform else (
                        value if isinstance(value, datetime) else datetime.fromisoformat(str(value)))
                    value = value.replace(tzinfo=ZoneInfo(transform.get("timezone", "Asia/Shanghai"))) if value.tzinfo is None else value
                elif kind in {"string", "enum"}:
                    value = str(value)
                elif kind == "boolean":
                    if type(value) is not bool:
                        raise ValueError("boolean required")
                else:
                    raise ValueError(f"unsupported field type: {kind}")
                if "multiplier" in transform:
                    if kind not in {"decimal", "integer"}:
                        raise ValueError("multiplier requires a numeric field")
                    multiplier = Decimal(str(transform["multiplier"]))
                    if not multiplier.is_finite():
                        raise ValueError("non-finite multiplier")
                    value *= multiplier
                    if kind == "integer":
                        if value != value.to_integral_value():
                            raise ValueError("multiplier produced a fractional integer")
                        value = int(value)
                if definition.get("values") and value not in definition["values"]:
                    raise ValueError("enum value not allowed")
            except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
                raise NormalizationError(f"invalid field {name}: {exc}") from exc
            result[name] = value
        for name in rule.get("positive_fields", ()):
            if name not in result or result[name] is None or result[name] <= 0:
                raise NormalizationError(f"non-positive field: {name}")
        return result

    def normalize_bar(
        self,
        raw: Mapping[str, Any],
        *,
        dataset: Dataset,
        instrument_id: str,
        exchange: Exchange,
        asset_type: AssetType,
        provider: str,
        endpoint: str,
        adjustment: Adjustment,
        capability_priority: int,
        capability_version: str,
        raw_object_path: str,
        fetch_time: datetime,
        quality_status: QualityStatus,
        field_level: str = "full_bar",
        source_method: str | None = None,
        frequency: int | None = None,
    ) -> BarRecord:
        symbol_values = {str(raw.get(rule.field_mapping.get("symbol", "symbol"), "")) for rule in self._rules
                         if rule.provider == provider and rule.endpoint == endpoint and rule.exchange == exchange
                         and rule.asset_type == asset_type and rule.frequency == frequency and rule.adjustment == adjustment}
        if len(symbol_values) != 1 or not next(iter(symbol_values), ""):
            raise NormalizationError("missing or ambiguous source symbol mapping")
        source_symbol = next(iter(symbol_values))
        rule = self._find_rule(
            provider=provider,
            endpoint=endpoint,
            exchange=exchange,
            asset_type=asset_type,
            source_symbol=source_symbol,
            frequency=frequency,
            adjustment=adjustment,
        )
        if quality_status is QualityStatus.FINAL and rule.volume_semantics in {"unverified", "unknown", "source_unit_unconfirmed"}:
            raise NormalizationError("unit semantics are unverified")
        mapped = self.map_fields(raw, rule.field_mapping, null_values=rule.null_values)
        try:
            trade_date = _as_date(mapped["trade_date"])
            bar_time = _as_datetime(mapped["bar_time"]) if frequency else None
            if bar_time is not None and rule.source_bar_time_semantics == "start_time":
                bar_time += timedelta(minutes=frequency or 0)
            prices = {name: _optional_decimal(mapped[name], Decimal(1)) for name in ("open", "high", "low", "close")}
            if any(value is None for value in prices.values()):
                raise ValueError("missing required price")
            volume = _optional_decimal(mapped.get("volume"), rule.volume_multiplier)
            amount = _optional_decimal(mapped.get("amount"), rule.amount_multiplier)
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            raise NormalizationError(f"invalid raw bar: {exc}") from exc

        return BarRecord(
            dataset=dataset,
            instrument_id=instrument_id,
            trade_date=trade_date,
            bar_time=bar_time,
            interval_minutes=frequency,
            adjustment=adjustment,
            open=prices["open"],
            high=prices["high"],
            low=prices["low"],
            close=prices["close"],
            volume=volume,
            amount=amount,
            source_provider=provider,
            endpoint=endpoint,
            source_method=source_method or endpoint,
            quality_status=quality_status,
            field_level=field_level,
            capability_priority=capability_priority,
            capability_version=capability_version,
            raw_object_path=raw_object_path,
            fetch_time=_aware_shanghai(fetch_time),
            source_symbol=source_symbol,
            volume_semantics=rule.volume_semantics,
            freshness_class=mapped.get("freshness_class", raw.get("freshness_class")),
            source_delay_seconds=mapped.get("source_delay_seconds", raw.get("source_delay_seconds")),
            as_of=_as_datetime(mapped.get("as_of", raw.get("as_of"))) if mapped.get("as_of", raw.get("as_of")) is not None else None,
            normalizer_version=rule.version,
        )


def _optional_decimal(value: Any, multiplier: Decimal) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("boolean is not numeric")
    number = Decimal(str(value)) * multiplier
    if not number.is_finite():
        raise ValueError("non-finite number")
    return number


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value))
    return _aware_shanghai(result)


def _aware_shanghai(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)
