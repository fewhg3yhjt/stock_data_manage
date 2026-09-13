from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class MarketSession:
    start: time
    end: time

    def contains(self, value: time) -> bool:
        return self.start < value <= self.end

    def expected_bar_times(self, trade_date: date) -> tuple[datetime, ...]:
        current = datetime.combine(trade_date, self.start, tzinfo=SHANGHAI) + timedelta(minutes=1)
        last = datetime.combine(trade_date, self.end, tzinfo=SHANGHAI)
        values: list[datetime] = []
        while current <= last:
            values.append(current)
            current += timedelta(minutes=1)
        return tuple(values)


@dataclass(frozen=True, slots=True)
class MarketSchedule:
    sessions: tuple[MarketSession, ...] = (
        MarketSession(time(9, 30), time(11, 30)),
        MarketSession(time(13, 0), time(15, 0)),
    )
    timezone: ZoneInfo = SHANGHAI
    realtime_start: time = time(9, 25)
    realtime_stop: time = time(15, 5)

    def in_realtime_window(self, value: datetime) -> bool:
        local = value.astimezone(self.timezone)
        return self.realtime_start <= local.time() <= self.realtime_stop

    def session_for(self, value: datetime) -> MarketSession | None:
        local = value.astimezone(self.timezone)
        return next((session for session in self.sessions if session.contains(local.time())), None)

    def expected_bar_times(self, trade_date: date) -> tuple[datetime, ...]:
        return tuple(time for session in self.sessions for time in session.expected_bar_times(trade_date))

    def expected_count(self) -> int:
        return sum(len(session.expected_bar_times(date(2026, 1, 2))) for session in self.sessions)

    def is_expected_bar_time(self, value: datetime) -> bool:
        local = value.astimezone(self.timezone)
        return any(
            session.start < local.time() <= session.end
            and local.second == 0
            and local.microsecond == 0
            for session in self.sessions
        )


def missing_bar_times(
    expected: Iterable[datetime], received: Iterable[datetime]
) -> tuple[datetime, ...]:
    return tuple(sorted(set(expected) - set(received)))

