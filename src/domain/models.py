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
    STOCK_FUND_FLOW = "stock_fund_flow"
    FINANCIAL_MAIN = "financial_main"
    SHAREHOLDER_COUNT = "shareholder_count"
    INDUSTRY_BOARD = "industry_board"
    CONCEPT_BOARD = "concept_board"
    INDUSTRY_MEMBERSHIP = "industry_membership"
    INDUSTRY_INDEX_DAILY = "industry_index_daily"
    BOARD_FUND_FLOW = "board_fund_flow"


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


class QuoteStatus(StrEnum):
    AVAILABLE = "available"
    NOT_APPLICABLE = "not_applicable"
    MISSING = "missing"
    UNVERIFIED = "unverified"


@dataclass(frozen=True, slots=True)
class QuoteGroup:
    """One source's indivisible price/point group, never a locally adjusted price."""
    status: QuoteStatus
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    volume: Decimal | None = None
    amount: Decimal | None = None
    source_provider: str = ""
    endpoint: str = ""
    raw_object_path: str = ""
    fetch_time: datetime | None = None
    normalizer_version: str = ""
    capability_version: str = ""
    reason: str = ""
    evidence_ref: str = ""
    flags: tuple[str, ...] = ()

    def __post_init__(self):
        if not isinstance(self.status, QuoteStatus):
            raise ValueError("quote status must be explicit")
        values = (self.open, self.high, self.low, self.close)
        if self.status is QuoteStatus.AVAILABLE:
            if not all(isinstance(v, Decimal) and v.is_finite() and v > 0 for v in values):
                raise ValueError("available quote requires finite positive OHLC")
            if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
                raise ValueError("invalid OHLC ordering")
            if not self.source_provider or not self.endpoint or not self.raw_object_path or self.fetch_time is None or self.fetch_time.utcoffset() is None:
                raise ValueError("available quote requires source evidence and aware fetch time")
            if any(v is not None and (not isinstance(v, Decimal) or not v.is_finite() or v < 0) for v in (self.volume, self.amount)):
                raise ValueError("invalid quote volume or amount")
        elif any(v is not None for v in (*values, self.volume, self.amount)):
            raise ValueError("unavailable quote cannot contain copied prices")
        elif not self.reason:
            raise ValueError("unavailable quote requires a reason")
        if self.status is QuoteStatus.NOT_APPLICABLE and not self.evidence_ref:
            raise ValueError("not-applicable quote requires applicability evidence")


@dataclass(frozen=True, slots=True)
class DailyRecord:
    instrument_id: str
    trade_date: date
    asset_type: AssetType
    none: QuoteGroup
    forward: QuoteGroup
    backward: QuoteGroup

    def __post_init__(self):
        import re
        if not isinstance(self.asset_type, AssetType) or not re.fullmatch(r"(?:XSHG|XSHE|BSE):[0-9]{6}", self.instrument_id):
            raise ValueError("daily record requires typed asset and market-qualified identity")
        if type(self.trade_date) is not date or not all(isinstance(q, QuoteGroup) for q in (self.none, self.forward, self.backward)):
            raise ValueError("daily record requires a date and all three explicit quote slots")

    @property
    def key(self):
        return self.instrument_id, self.trade_date

    @property
    def quotes(self):
        return {"none": self.none, "forward": self.forward, "backward": self.backward}

    @property
    def value_kind(self):
        return "index_points" if self.asset_type is AssetType.INDEX else "price"

    @property
    def quote_unit(self):
        return "point" if self.asset_type is AssetType.INDEX else "CNY"

    @property
    def complete(self):
        if self.none.status is not QuoteStatus.AVAILABLE:
            return False
        if self.asset_type is AssetType.STOCK:
            return all(q.status is QuoteStatus.AVAILABLE for q in self.quotes.values())
        allowed = {QuoteStatus.AVAILABLE, QuoteStatus.NOT_APPLICABLE}
        if self.asset_type in {AssetType.ETF, AssetType.INDEX}:
            # Their required default quote is publishable while optional
            # adjustment slots explicitly retain unverified applicability.
            allowed.add(QuoteStatus.UNVERIFIED)
        return all(q.status in allowed for q in self.quotes.values())
