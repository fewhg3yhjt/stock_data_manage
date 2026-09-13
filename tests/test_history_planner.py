from datetime import date, timedelta

import pytest

from stock_data_manage.history_planner import split_history_windows
from stock_data_manage.provider_contract import WindowStatus, assess_returned_window


def test_history_plan_respects_day_and_row_limits_per_symbol() -> None:
    start = date(2026, 9, 1)
    trading_dates = [start + timedelta(days=index) for index in range(12)]
    windows = split_history_windows(
        symbols=["a", "b"],
        trading_dates=trading_dates,
        max_days_per_request=5,
        max_rows_per_request=4,
    )
    assert len(windows) == 6
    assert [window.expected_rows for window in windows[:3]] == [4, 4, 4]
    assert windows[0].symbol == "a" and windows[3].symbol == "b"


def test_minute_endpoint_that_cannot_hold_one_day_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot hold one"):
        split_history_windows(
            symbols=["a"],
            trading_dates=[date(2026, 9, 11)],
            max_days_per_request=5,
            max_rows_per_request=120,
            expected_rows_per_day=240,
        )


def test_returned_window_distinguishes_partial_and_truncated() -> None:
    assert assess_returned_window(
        requested_first="2026-01-01",
        requested_last="2026-12-31",
        returned_keys=["2026-09-01", "2026-09-02"],
        max_rows_per_request=2,
        supports_pagination=False,
    ) is WindowStatus.TRUNCATED
    assert assess_returned_window(
        requested_first="2026-01-01",
        requested_last="2026-12-31",
        returned_keys=["2026-09-01"],
        max_rows_per_request=2,
        supports_pagination=False,
    ) is WindowStatus.PARTIAL

