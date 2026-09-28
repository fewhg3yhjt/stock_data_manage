from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol, Sequence

from ..storage.integrity import row_hash
from .base import DailyProvider
from .contracts import FailureClass, ProviderContractError, WindowStatus


class MinuteProvider(Protocol):
    name: str
    endpoint: str
    capability_version: str

    def fetch_realtime_minute(self, symbols: Sequence[str], as_of: datetime): ...


@dataclass(frozen=True, slots=True)
class ProbeEvidence:
    provider: str
    endpoint: str
    capability_version: str
    validated_at: datetime
    validation_expires_at: datetime
    status: str
    eligible_for_selection: bool
    row_count: int
    first_key: str | None
    last_key: str | None
    evidence_hash: str
    failure_class: str | None = None
    message: str | None = None


def probe_daily_capability(
    provider: DailyProvider,
    *,
    symbol: str,
    trade_date: date,
    now: datetime,
    ttl: timedelta,
) -> ProbeEvidence:
    try:
        result = provider.fetch_daily([symbol], trade_date)
        keys = sorted(str(row["trade_date"]) for row in result.rows)
        status = getattr(result, "window_status", None) or (
            WindowStatus.COMPLETE if keys else WindowStatus.TEMPORARY_EMPTY
        )
        eligible = bool(keys) and status in {WindowStatus.COMPLETE, WindowStatus.TRUNCATED}
        failure = None if eligible else FailureClass.TEMPORARY_EMPTY.value
        message = None
    except ProviderContractError as exc:
        keys = []
        status = "failed"
        eligible = False
        failure = exc.failure_class.value
        message = str(exc)
    except Exception as exc:
        keys = []
        status = "failed"
        eligible = False
        failure = FailureClass.CONNECTION.value
        message = str(exc)
    payload = {
        "provider": provider.name,
        "endpoint": provider.endpoint,
        "version": provider.capability_version,
        "validated_at": now,
        "status": str(status),
        "keys": keys,
        "failure_class": failure,
    }
    return ProbeEvidence(
        provider=provider.name,
        endpoint=provider.endpoint,
        capability_version=provider.capability_version,
        validated_at=now,
        validation_expires_at=now + ttl,
        status=str(status),
        eligible_for_selection=eligible,
        row_count=len(keys),
        first_key=keys[0] if keys else None,
        last_key=keys[-1] if keys else None,
        evidence_hash=row_hash(payload),
        failure_class=failure,
        message=message,
    )


def probe_minute_capability(
    provider: MinuteProvider,
    *,
    symbol: str,
    as_of: datetime,
    ttl: timedelta,
) -> ProbeEvidence:
    """Probe one minute endpoint and persist only evidence-backed eligibility."""
    try:
        result = provider.fetch_realtime_minute([symbol], as_of)
        keys = sorted(
            str(row.get("bar_time"))
            for row in result.rows
            if isinstance(row, dict) and row.get("bar_time") not in (None, "")
        )
        status = WindowStatus.COMPLETE if keys else WindowStatus.TEMPORARY_EMPTY
        eligible = bool(keys)
        failure = None if eligible else FailureClass.TEMPORARY_EMPTY.value
        message = None
    except ProviderContractError as exc:
        keys = []
        status = "failed"
        eligible = False
        failure = exc.failure_class.value
        message = str(exc)
    except Exception as exc:
        keys = []
        status = "failed"
        eligible = False
        failure = FailureClass.CONNECTION.value
        message = str(exc)
    payload = {
        "provider": provider.name,
        "endpoint": provider.endpoint,
        "version": provider.capability_version,
        "validated_at": as_of,
        "status": str(status),
        "keys": keys,
        "failure_class": failure,
    }
    return ProbeEvidence(
        provider=provider.name,
        endpoint=provider.endpoint,
        capability_version=provider.capability_version,
        validated_at=as_of,
        validation_expires_at=as_of + ttl,
        status=str(status),
        eligible_for_selection=eligible,
        row_count=len(keys),
        first_key=keys[0] if keys else None,
        last_key=keys[-1] if keys else None,
        evidence_hash=row_hash(payload),
        failure_class=failure,
        message=message,
    )
