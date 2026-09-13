from datetime import date, timedelta

import pytest

from stock_data_manage.domain import AssetType, Exchange
from stock_data_manage.provider_contract import FailureClass, ProviderContractError
from stock_data_manage.retry import RetryPolicy, execute_with_fallback, execute_with_retry
from stock_data_manage.security_master import (
    SecurityRecord,
    SecurityMasterStore,
    SymbolHistory,
    expected_security_set,
    merge_security_sources,
    stable_instrument_id,
)
from stock_data_manage.trading_calendar import CalendarDay, TradingCalendarStore, merge_calendar_sources


def test_retry_retries_timeout_but_not_rate_limit() -> None:
    calls = []
    waits = []

    def operation() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise ProviderContractError("timeout", FailureClass.TIMEOUT, retryable=True)
        return "ok"

    result = execute_with_retry(
        operation,
        policy=RetryPolicy(max_attempts=3, retry_wait_seconds=2),
        sleep=waits.append,
    )
    assert result.value == "ok"
    assert len(result.attempts) == 2
    assert waits == [2, 2]

    with pytest.raises(ProviderContractError):
        execute_with_retry(
            lambda: (_ for _ in ()).throw(
                ProviderContractError("429", FailureClass.RATE_LIMITED, retryable=False)
            ),
            policy=RetryPolicy(max_attempts=3),
            sleep=waits.append,
        )
    assert len(waits) == 2


def test_fallback_moves_to_next_provider_after_retry_budget() -> None:
    result = execute_with_fallback(
        [
            ("primary", lambda: (_ for _ in ()).throw(TimeoutError("down"))),
            ("fallback", lambda: "ok"),
        ],
        retry_policy=RetryPolicy(max_attempts=2, retry_wait_seconds=0),
        sleep=lambda _: None,
    )
    assert result.provider == "fallback"
    assert result.failed_providers == ("primary",)


def test_security_merge_preserves_previous_on_missing_and_records_classification_conflict() -> None:
    instrument_id = stable_instrument_id(Exchange.XSHG, "sh600519")
    previous = SecurityRecord(
        instrument_id, "600519", Exchange.XSHG, AssetType.STOCK,
        list_date=date(2010, 1, 1), evidence_sources=("previous",),
    )
    candidate = SecurityRecord(
        instrument_id, "600519", Exchange.XSHG, AssetType.ETF,
        list_date=date(2010, 1, 1), evidence_sources=(),
    )
    merged = merge_security_sources(previous=[previous], sources={"sina": [candidate], "eastmoney": []})
    assert merged.records[0].asset_type is AssetType.STOCK
    assert merged.records[0].classification_conflict
    assert merged.missing_sources == ("eastmoney",)
    assert len(merged.classification_conflicts) == 1


def test_security_expected_set_uses_historical_listing_window() -> None:
    records = [
        SecurityRecord("a", "a", Exchange.XSHG, AssetType.STOCK, list_date=date(2026, 1, 1)),
        SecurityRecord("b", "b", Exchange.XSHG, AssetType.STOCK, list_date=date(2026, 9, 12)),
        SecurityRecord("c", "c", Exchange.XSHG, AssetType.STOCK, list_date=date(2020, 1, 1), delist_date=date(2026, 9, 1)),
    ]
    assert [record.instrument_id for record in expected_security_set(records, date(2026, 9, 1))] == ["a", "c"]


def test_security_master_store_round_trips_records_and_aliases(tmp_path) -> None:
    record = SecurityRecord(
        stable_instrument_id(Exchange.BSE, "bj920000"),
        "920000",
        Exchange.BSE,
        AssetType.STOCK,
        list_date=date(2026, 1, 1),
        evidence_sources=("exchange",),
    )
    alias = SymbolHistory(record.instrument_id, "exchange", "bj430000", date(2020, 1, 1), date(2025, 12, 31), "code_migration", "exchange")
    with SecurityMasterStore(str(tmp_path / "security.duckdb")) as store:
        store.upsert([record], [alias])
        assert store.all() == (record,)


def test_calendar_source_priority_and_unknown_day_semantics(tmp_path) -> None:
    trade_day = date(2026, 9, 11)
    result = merge_calendar_sources(
        {
            "exchange": [CalendarDay(trade_day, True, "exchange", 100)],
            "sina": [CalendarDay(trade_day, False, "sina", 10)],
        }
    )
    assert result.days[0].is_trading_day
    assert result.conflicts == ((trade_day, "True", "exchange,sina"),)
    with TradingCalendarStore(str(tmp_path / "calendar.duckdb")) as store:
        store.upsert(result.days)
        assert store.is_trading_day(trade_day) is True
        assert store.is_trading_day(date(2026, 9, 12)) is None
