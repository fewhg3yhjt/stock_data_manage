from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from itertools import combinations

from ..domain import BarRecord, QualityStatus


FIELD_LEVEL_RANK = {"ohlc": 1, "ohlcv": 2, "full_bar": 3, "extended_bar": 4}
METHOD_RANK = {"snapshot": 1, "realtime_quote": 1, "realtime_minute": 2, "minute_history": 3, "daily_history": 3}
QUALITY_RANK = {
    QualityStatus.PENDING_VALIDATION: 0,
    QualityStatus.PROVISIONAL: 1,
    QualityStatus.FINAL: 2,
}


@dataclass(frozen=True, slots=True)
class ConflictThresholds:
    price_relative_diff: Decimal = Decimal("0.001")
    volume_relative_diff: Decimal = Decimal("0.01")
    amount_relative_diff: Decimal = Decimal("0.01")
    hard_multiplier: Decimal = Decimal("5")


@dataclass(frozen=True, slots=True)
class Conflict:
    canonical_key: tuple[object, ...]
    field: str
    provider_a: str
    provider_b: str
    value_a: Decimal
    value_b: Decimal
    diff_ratio: Decimal
    severity: str


@dataclass(frozen=True, slots=True)
class ResolutionResult:
    selected: BarRecord | None
    conflicts: tuple[Conflict, ...]
    quarantined: bool
    reason: str


def resolve_records(
    records: list[BarRecord],
    thresholds: ConflictThresholds | None = None,
) -> ResolutionResult:
    if not records:
        raise ValueError("at least one record is required")
    if len({record.key for record in records}) != 1:
        raise ValueError("records with different canonical keys cannot be resolved together")
    thresholds = thresholds or ConflictThresholds()
    conflicts = tuple(_find_conflicts(records, thresholds))
    if any(conflict.severity == "hard" for conflict in conflicts):
        return ResolutionResult(None, conflicts, True, "hard_conflict")
    selected = max(records, key=_rank)
    reason = "duplicate" if not conflicts and len(records) > 1 else "resolution_policy_v1"
    return ResolutionResult(selected, conflicts, False, reason)


def _rank(record: BarRecord) -> tuple[object, ...]:
    return (
        QUALITY_RANK[record.quality_status],
        FIELD_LEVEL_RANK.get(record.field_level, 0),
        METHOD_RANK.get(record.source_method, 0),
        record.capability_priority,
        record.source_provider,
        record.endpoint,
        record.fetch_time.timestamp(),
    )


def _find_conflicts(
    records: list[BarRecord], thresholds: ConflictThresholds
) -> list[Conflict]:
    result: list[Conflict] = []
    field_thresholds = {
        "open": thresholds.price_relative_diff,
        "high": thresholds.price_relative_diff,
        "low": thresholds.price_relative_diff,
        "close": thresholds.price_relative_diff,
        "volume": thresholds.volume_relative_diff,
        "amount": thresholds.amount_relative_diff,
    }
    for left, right in combinations(records, 2):
        for field, threshold in field_thresholds.items():
            a = getattr(left, field)
            b = getattr(right, field)
            if a is None or b is None or a == b:
                continue
            diff = _relative_diff(a, b)
            if diff <= threshold:
                continue
            severity = "hard" if diff > threshold * thresholds.hard_multiplier else "soft"
            result.append(
                Conflict(
                    left.key,
                    field,
                    left.source_provider,
                    right.source_provider,
                    a,
                    b,
                    diff,
                    severity,
                )
            )
    return result


def _relative_diff(left: Decimal, right: Decimal) -> Decimal:
    denominator = max(abs(left), abs(right))
    if denominator == 0:
        return Decimal(0)
    return abs(left - right) / denominator
