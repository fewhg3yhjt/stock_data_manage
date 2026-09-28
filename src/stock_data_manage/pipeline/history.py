from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable


@dataclass(frozen=True, slots=True)
class HistoryWindow:
    symbol: str
    start_date: date
    end_date: date
    expected_rows: int


def split_history_windows(
    *,
    symbols: Iterable[str],
    trading_dates: Iterable[date],
    max_days_per_request: int | None,
    max_rows_per_request: int | None,
    expected_rows_per_day: int = 1,
) -> tuple[HistoryWindow, ...]:
    if expected_rows_per_day <= 0:
        raise ValueError("expected_rows_per_day must be positive")
    dates = tuple(sorted(set(trading_dates)))
    if not dates:
        return ()
    day_limits = [len(dates)]
    if max_days_per_request is not None:
        if max_days_per_request <= 0:
            raise ValueError("max_days_per_request must be positive")
        day_limits.append(max_days_per_request)
    if max_rows_per_request is not None:
        if max_rows_per_request < expected_rows_per_day:
            raise ValueError("endpoint row limit cannot hold one expected trading day")
        day_limits.append(max_rows_per_request // expected_rows_per_day)
    days_per_window = min(day_limits)
    windows: list[HistoryWindow] = []
    for symbol in dict.fromkeys(symbols):
        for offset in range(0, len(dates), days_per_window):
            chunk = dates[offset : offset + days_per_window]
            windows.append(
                HistoryWindow(
                    symbol=symbol,
                    start_date=chunk[0],
                    end_date=chunk[-1],
                    expected_rows=len(chunk) * expected_rows_per_day,
                )
            )
    return tuple(windows)
