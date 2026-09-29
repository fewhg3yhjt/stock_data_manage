from __future__ import annotations

from dataclasses import dataclass
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
    def __init__(self, rules: list[NormalizationRule]) -> None:
        self._rules = list(rules)

    def _find_rule(self, **criteria: Any) -> NormalizationRule:
        matches = [rule for rule in self._rules if rule.matches(**criteria)]
        if len(matches) != 1:
            raise NormalizationError(
                f"expected exactly one normalization rule, found {len(matches)}"
            )
        return matches[0]

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
        source_symbol = str(raw["symbol"])
        rule = self._find_rule(
            provider=provider,
            endpoint=endpoint,
            exchange=exchange,
            asset_type=asset_type,
            source_symbol=source_symbol,
            frequency=frequency,
            adjustment=adjustment,
        )
        try:
            trade_date = _as_date(raw["trade_date"])
            bar_time = _as_datetime(raw["bar_time"]) if frequency else None
            if bar_time is not None and rule.source_bar_time_semantics == "start_time":
                bar_time += timedelta(minutes=frequency or 0)
            prices = {name: Decimal(str(raw[name])) for name in ("open", "high", "low", "close")}
            volume = _optional_decimal(raw.get("volume"), rule.volume_multiplier)
            amount = _optional_decimal(raw.get("amount"), rule.amount_multiplier)
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
            freshness_class=raw.get("freshness_class"),
            source_delay_seconds=raw.get("source_delay_seconds"),
            as_of=_as_datetime(raw["as_of"]) if raw.get("as_of") is not None else None,
            normalizer_version=rule.version,
        )


def _optional_decimal(value: Any, multiplier: Decimal) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value)) * multiplier


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
