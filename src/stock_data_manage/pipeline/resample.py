from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timedelta
from decimal import Decimal

from ..domain import BarRecord, Dataset, QualityStatus


CN_A_SESSION_ENDS = (
    (time(9, 31), time(11, 30)),
    (time(13, 1), time(15, 0)),
)


def resample_1m_to_5m(records: list[BarRecord]) -> list[BarRecord]:
    if not records:
        return []
    ordered = sorted(records, key=lambda record: record.bar_time or record.fetch_time)
    identity = {(record.instrument_id, record.trade_date, record.adjustment) for record in ordered}
    if len(identity) != 1:
        raise ValueError("records must belong to one instrument, date and adjustment")
    if any(record.dataset is not Dataset.MINUTE_BAR_1M or record.bar_time is None for record in ordered):
        raise ValueError("only complete 1-minute records can be resampled")

    by_time = {record.bar_time: record for record in ordered if record.bar_time is not None}
    first_time = ordered[0].bar_time
    assert first_time is not None
    timezone = first_time.tzinfo
    result: list[BarRecord] = []
    for session_start, session_end in CN_A_SESSION_ENDS:
        group_start = datetime.combine(ordered[0].trade_date, session_start, tzinfo=timezone)
        session_last = datetime.combine(ordered[0].trade_date, session_end, tzinfo=timezone)
        while group_start <= session_last:
            expected_times = [group_start + timedelta(minutes=offset) for offset in range(5)]
            group = [by_time.get(expected_time) for expected_time in expected_times]
            group_start += timedelta(minutes=5)
            if any(record is None for record in group):
                continue
            complete_group = [record for record in group if record is not None]
            first, last = complete_group[0], complete_group[-1]
            result.append(
                replace(
                    first,
                    dataset=Dataset.MINUTE_BAR_5M,
                    bar_time=last.bar_time,
                    interval_minutes=5,
                    open=first.open,
                    high=max(item.high for item in complete_group),
                    low=min(item.low for item in complete_group),
                    close=last.close,
                    volume=_sum_optional([item.volume for item in complete_group]),
                    amount=_sum_optional([item.amount for item in complete_group]),
                    source_provider="canonical_1m",
                    endpoint="resample_5m",
                    source_method="derived_1m",
                    quality_status=QualityStatus.FINAL,
                    field_level=min(
                        complete_group, key=lambda item: _field_rank(item.field_level)
                    ).field_level,
                    raw_object_path="|".join(
                        dict.fromkeys(item.raw_object_path for item in complete_group)
                    ),
                    flags=tuple(
                        dict.fromkeys(flag for item in complete_group for flag in item.flags)
                    ),
                )
            )
    return result


def _sum_optional(values: list[Decimal | None]) -> Decimal | None:
    present = [value for value in values if value is not None]
    return sum(present, start=Decimal(0)) if len(present) == len(values) else None


def _field_rank(field_level: str) -> int:
    return {"ohlc": 1, "ohlcv": 2, "full_bar": 3, "extended_bar": 4}.get(field_level, 0)
