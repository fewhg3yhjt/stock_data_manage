from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class Dataset(StrEnum):
    SECURITY_MASTER = "security_master"
    DAILY_BAR = "daily_bar"
    MINUTE_BAR_1M = "minute_bar_1m"
    MINUTE_BAR_5M = "minute_bar_5m"
    SNAPSHOT = "snapshot"
    DIVIDEND_EVENT = "dividend_event"
    REALTIME_QUOTE = "realtime_quote"
    INTRADAY_TREND = "intraday_trend"


class Exchange(StrEnum):
    XSHG = "XSHG"
    XSHE = "XSHE"
    BSE = "BSE"


class AssetType(StrEnum):
    STOCK = "stock"
    ETF = "etf"
    LOF = "lof"
    INDEX = "index"


class Adjustment(StrEnum):
    NONE = "none"
    FORWARD = "forward"
    BACKWARD = "backward"


class QualityStatus(StrEnum):
    FINAL = "final"
    PROVISIONAL = "provisional"
    PENDING_VALIDATION = "pending_validation"


class ItemStatus(StrEnum):
    SUCCESS = "success"
    NO_TRADE = "no_trade"
    CONFIRMED_NO_DATA = "confirmed_no_data"
    MISSING = "missing"
    INVALID = "invalid"
    CONFLICT = "conflict"
    QUARANTINED = "quarantined"
    TEMPORARY_EMPTY = "temporary_empty"


class AttemptStatus(StrEnum):
    PENDING = "pending"
    LEASED = "leased"
    FETCHING = "fetching"
    RAW_COMMITTED = "raw_committed"
    NORMALIZED = "normalized"
    VALIDATED = "validated"
    PUBLISHED = "published"
    RETRYABLE_FAILED = "retryable_failed"
    TERMINAL_FAILED = "terminal_failed"
    TEMPORARY_EMPTY = "temporary_empty"
    CONFIRMED_NO_DATA = "confirmed_no_data"
    CONFLICT = "conflict"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class DividendEvent:
    source_security_code: str
    ex_dividend_date: date
    record_date: date | None
    pretax_bonus_rmb: Decimal | None
    bonus_ratio: Decimal | None
    transfer_ratio: Decimal | None
    assignment_progress: str | None
    notice_date: date | None
    source_provider: str
    endpoint: str
    capability_version: str
    raw_object_path: str
    fetch_time: datetime

    @property
    def key(self) -> tuple[str, date]:
        return (self.source_security_code, self.ex_dividend_date)


@dataclass(frozen=True, slots=True)
class BarRecord:
    """One indivisible provider record in canonical units.

    A record is selected as a whole.  Fields from different providers are never
    spliced together during resolution.
    """

    dataset: Dataset
    instrument_id: str
    trade_date: date
    adjustment: Adjustment
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    amount: Decimal | None
    source_provider: str
    endpoint: str
    source_method: str
    quality_status: QualityStatus
    field_level: str
    capability_priority: int
    capability_version: str
    raw_object_path: str
    fetch_time: datetime
    bar_time: datetime | None = None
    interval_minutes: int | None = None
    source_symbol: str | None = None
    volume_semantics: str | None = None
    freshness_class: str | None = None
    source_delay_seconds: int | None = None
    as_of: datetime | None = None
    normalizer_version: str = "v1"
    resolution_policy_version: str = "v1"
    flags: tuple[str, ...] = ()

    @property
    def key(self) -> tuple[Any, ...]:
        if self.dataset in {Dataset.MINUTE_BAR_1M, Dataset.MINUTE_BAR_5M}:
            return (
                self.instrument_id,
                self.bar_time,
                self.interval_minutes,
                self.adjustment,
            )
        return (self.instrument_id, self.trade_date, self.adjustment)

    def with_flags(self, *flags: str) -> "BarRecord":
        return replace(self, flags=tuple(dict.fromkeys((*self.flags, *flags))))
