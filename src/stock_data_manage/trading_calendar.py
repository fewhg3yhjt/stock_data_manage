from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable, Mapping

import duckdb


@dataclass(frozen=True, slots=True)
class CalendarDay:
    trade_date: date
    is_trading_day: bool
    source: str
    priority: int
    evidence: str | None = None


@dataclass(frozen=True, slots=True)
class CalendarMergeResult:
    days: tuple[CalendarDay, ...]
    conflicts: tuple[tuple[date, str, str], ...]


def merge_calendar_sources(
    sources: Mapping[str, Iterable[CalendarDay]],
    *,
    previous: Iterable[CalendarDay] = (),
) -> CalendarMergeResult:
    candidates: dict[date, list[CalendarDay]] = {}
    for day in previous:
        candidates.setdefault(day.trade_date, []).append(day)
    for source, days in sources.items():
        for day in days:
            candidates.setdefault(day.trade_date, []).append(
                CalendarDay(day.trade_date, day.is_trading_day, source, day.priority, day.evidence)
            )
    result: list[CalendarDay] = []
    conflicts: list[tuple[date, str, str]] = []
    for trade_date, options in candidates.items():
        selected = max(options, key=lambda day: (day.priority, day.source))
        values = {day.is_trading_day for day in options}
        if len(values) > 1:
            conflicts.append((trade_date, str(selected.is_trading_day), ",".join(sorted({day.source for day in options}))))
        result.append(selected)
    return CalendarMergeResult(tuple(sorted(result, key=lambda day: day.trade_date)), tuple(conflicts))


class TradingCalendarStore(AbstractContextManager["TradingCalendarStore"]):
    def __init__(self, path: str = ":memory:") -> None:
        self.connection = duckdb.connect(path)
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trading_calendar (
                trade_date DATE PRIMARY KEY, is_trading_day BOOLEAN NOT NULL,
                source VARCHAR NOT NULL, priority INTEGER NOT NULL,
                evidence VARCHAR, updated_at TIMESTAMPTZ NOT NULL
            )
            """
        )

    def upsert(self, days: Iterable[CalendarDay]) -> None:
        now = datetime.now(timezone.utc)
        for day in days:
            self.connection.execute(
                """
                INSERT INTO trading_calendar VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (trade_date) DO UPDATE SET
                    is_trading_day=excluded.is_trading_day, source=excluded.source,
                    priority=excluded.priority, evidence=excluded.evidence,
                    updated_at=excluded.updated_at
                """,
                [day.trade_date, day.is_trading_day, day.source, day.priority, day.evidence, now],
            )

    def is_trading_day(self, value: date) -> bool | None:
        row = self.connection.execute(
            "SELECT is_trading_day FROM trading_calendar WHERE trade_date=?", [value]
        ).fetchone()
        return bool(row[0]) if row is not None else None

    def trading_dates(self, start: date, end: date) -> tuple[date, ...]:
        rows = self.connection.execute(
            """SELECT trade_date FROM trading_calendar
               WHERE trade_date BETWEEN ? AND ? AND is_trading_day ORDER BY trade_date""",
            [start, end],
        ).fetchall()
        return tuple(row[0] for row in rows)

    def all(self) -> tuple[CalendarDay, ...]:
        rows = self.connection.execute(
            "SELECT trade_date, is_trading_day, source, priority, evidence FROM trading_calendar ORDER BY trade_date"
        ).fetchall()
        return tuple(CalendarDay(row[0], row[1], row[2], row[3], row[4]) for row in rows)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "TradingCalendarStore":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
