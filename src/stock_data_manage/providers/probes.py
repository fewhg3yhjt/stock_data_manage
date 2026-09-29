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
    request_scope: tuple[str, ...] = ()
    response_status: int | None = None
    returned_window: str | None = None
    field_semantics: tuple[str, ...] = ()
    units: tuple[str, ...] = ()
    adjustment: str = "none"
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
        window_count = getattr(result, "returned_row_count", None) or len(keys)
        first_key = getattr(result, "returned_first_key", None) or (keys[0] if keys else None)
        last_key = getattr(result, "returned_last_key", None) or (keys[-1] if keys else None)
        status = getattr(result, "window_status", None) or (
            WindowStatus.COMPLETE if keys else WindowStatus.TEMPORARY_EMPTY
        )
        eligible = bool(keys) and status in {WindowStatus.COMPLETE, WindowStatus.TRUNCATED}
        failure = None if eligible else FailureClass.TEMPORARY_EMPTY.value
        message = None
        response_status = _single_status(getattr(result, "response_statuses", ()))
        field_semantics = tuple(getattr(result, "field_semantics", ())) or _row_fields(result.rows)
        units = tuple(getattr(result, "units", ())) or _row_units(result.rows)
        adjustment = getattr(getattr(result, "adjustment", None), "value", "none")
    except ProviderContractError as exc:
        keys = []
        status = "failed"
        eligible = False
        failure = exc.failure_class.value
        message = str(exc)
        response_status = None
        field_semantics = ()
        units = ()
        adjustment = "none"
    except Exception as exc:
        keys = []
        status = "failed"
        eligible = False
        failure = FailureClass.CONNECTION.value
        message = str(exc)
        response_status = None
        field_semantics = ()
        units = ()
        adjustment = "none"
    payload = {
        "provider": provider.name,
        "endpoint": provider.endpoint,
        "version": provider.capability_version,
        "validated_at": now,
        "status": str(status),
        "keys": keys,
        "window_count": window_count,
        "window_first_key": first_key,
        "window_last_key": last_key,
        "failure_class": failure,
        "request_scope": [symbol],
        "response_status": response_status,
        "returned_window": str(status),
        "field_semantics": field_semantics,
        "units": units,
        "adjustment": adjustment,
    }
    return ProbeEvidence(
        provider=provider.name,
        endpoint=provider.endpoint,
        capability_version=provider.capability_version,
        validated_at=now,
        validation_expires_at=now + ttl,
        status=str(status),
        eligible_for_selection=eligible,
        row_count=window_count,
        first_key=first_key,
        last_key=last_key,
        evidence_hash=row_hash(payload),
        request_scope=(symbol,),
        response_status=response_status,
        returned_window=str(status),
        field_semantics=field_semantics,
        units=units,
        adjustment=adjustment,
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
        window_count = getattr(result, "returned_row_count", None) or len(keys)
        first_key = getattr(result, "returned_first_key", None) or (keys[0] if keys else None)
        last_key = getattr(result, "returned_last_key", None) or (keys[-1] if keys else None)
        if last_key is not None and _parse_datetime(last_key) > as_of.replace(tzinfo=None):
            raise ProviderContractError(
                "provider returned bars newer than probe as-of time",
                FailureClass.SCHEMA_CHANGED,
                retryable=False,
            )
        status = WindowStatus.COMPLETE if keys else WindowStatus.TEMPORARY_EMPTY
        eligible = bool(keys)
        failure = None if eligible else FailureClass.TEMPORARY_EMPTY.value
        message = None
        response_status = _single_status(getattr(result, "response_statuses", ()))
        field_semantics = tuple(getattr(result, "field_semantics", ())) or _row_fields(result.rows)
        units = tuple(getattr(result, "units", ())) or _row_units(result.rows)
    except ProviderContractError as exc:
        keys = []
        status = "failed"
        eligible = False
        failure = exc.failure_class.value
        message = str(exc)
        response_status = None
        field_semantics = ()
        units = ()
    except Exception as exc:
        keys = []
        status = "failed"
        eligible = False
        failure = FailureClass.CONNECTION.value
        message = str(exc)
        response_status = None
        field_semantics = ()
        units = ()
    payload = {
        "provider": provider.name,
        "endpoint": provider.endpoint,
        "version": provider.capability_version,
        "validated_at": as_of,
        "status": str(status),
        "keys": keys,
        "window_count": window_count,
        "window_first_key": first_key,
        "window_last_key": last_key,
        "failure_class": failure,
        "request_scope": [symbol],
        "response_status": response_status,
        "returned_window": str(status),
        "field_semantics": field_semantics,
        "units": units,
    }
    return ProbeEvidence(
        provider=provider.name,
        endpoint=provider.endpoint,
        capability_version=provider.capability_version,
        validated_at=as_of,
        validation_expires_at=as_of + ttl,
        status=str(status),
        eligible_for_selection=eligible,
        row_count=window_count,
        first_key=first_key,
        last_key=last_key,
        evidence_hash=row_hash(payload),
        request_scope=(symbol,),
        response_status=response_status,
        returned_window=str(status),
        field_semantics=field_semantics,
        units=units,
        failure_class=failure,
        message=message,
    )


def _single_status(statuses: object) -> int | None:
    values = tuple(int(value) for value in statuses) if statuses else ()
    return values[0] if values and len(set(values)) == 1 else None


def _row_fields(rows: Sequence[object]) -> tuple[str, ...]:
    fields: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            fields.update(str(key) for key in row)
    return tuple(sorted(fields))


def _row_units(rows: Sequence[object]) -> tuple[str, ...]:
    units: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            for key, value in row.items():
                if str(key).endswith("_unit") and value not in (None, ""):
                    units.add(f"{key}={value}")
    return tuple(sorted(units)) or ("unverified",)


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
