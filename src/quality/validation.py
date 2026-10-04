from __future__ import annotations

from dataclasses import dataclass

from ..domain import BarRecord, Dataset, QualityStatus


@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid: bool
    errors: tuple[str, ...]


def validate_bar(record: BarRecord) -> ValidationResult:
    errors: list[str] = []
    if min(record.open, record.high, record.low, record.close) < 0:
        errors.append("price_negative")
    if record.high < max(record.open, record.close, record.low):
        errors.append("high_below_ohlc")
    if record.low > min(record.open, record.close, record.high):
        errors.append("low_above_ohlc")
    if record.volume is not None and record.volume < 0:
        errors.append("volume_negative")
    if record.amount is not None and record.amount < 0:
        errors.append("amount_negative")
    if record.quality_status is QualityStatus.PENDING_VALIDATION:
        errors.append("normalization_pending_validation")
    if record.dataset in {Dataset.MINUTE_BAR_1M, Dataset.MINUTE_BAR_5M}:
        if record.bar_time is None or record.interval_minutes is None:
            errors.append("minute_key_incomplete")
        elif record.bar_time.tzinfo is None:
            errors.append("bar_time_timezone_missing")
        else:
            if record.bar_time.date() != record.trade_date:
                errors.append("bar_time_trade_date_mismatch")
            if record.bar_time.second or record.bar_time.microsecond:
                errors.append("bar_time_not_minute_boundary")
    return ValidationResult(not errors, tuple(errors))
