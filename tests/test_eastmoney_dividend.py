import json
from datetime import date, datetime, timezone
from decimal import Decimal

from stock_data_manage.domain import DividendEvent
from stock_data_manage.providers.contracts import HttpResponse
from stock_data_manage.providers.eastmoney import EastMoneyDividendProvider
from stock_data_manage.storage.metadata import MetadataStore


class FakeTransport:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, *, params, timeout_seconds):
        self.calls.append((url, params, timeout_seconds))
        return HttpResponse(200, {"Content-Type": "application/json"}, json.dumps(self.payloads.pop(0)).encode())


def payload(rows, count=None):
    return {"result": {"data": rows, "count": count if count is not None else len(rows)}}


def test_eastmoney_dividend_normalizes_and_deduplicates() -> None:
    transport = FakeTransport([
        payload([
            {
                "SECURITY_CODE": "600519",
                "EX_DIVIDEND_DATE": "2026-09-11",
                "EQUITY_RECORD_DATE": "2026-09-10",
                "PRETAX_BONUS_RMB": "10.00",
                "BONUS_RATIO": "0",
                "IT_RATIO": "0",
                "ASSIGN_PROGRESS": "实施",
                "NOTICE_DATE": "2026-08-01",
            },
            {
                "SECURITY_CODE": "600519",
                "EX_DIVIDEND_DATE": "2026-09-11",
                "PRETAX_BONUS_RMB": "10.00",
            },
        ])
    ])
    result = EastMoneyDividendProvider(transport).fetch_events(
        date(2026, 9, 1), date(2026, 9, 15), fetch_time=datetime.now(timezone.utc)
    )
    assert len(result.events) == 1
    assert result.events[0].key == ("600519", date(2026, 9, 11))
    assert result.events[0].pretax_bonus_rmb == Decimal("10.00")
    assert "EX_DIVIDEND_DATE" in transport.calls[0][1]["filter"]


def test_dividend_events_are_persisted_by_business_key(tmp_path) -> None:
    event = DividendEvent(
        source_security_code="600519",
        ex_dividend_date=date(2026, 9, 11),
        record_date=date(2026, 9, 10),
        pretax_bonus_rmb=Decimal("10"),
        bonus_ratio=None,
        transfer_ratio=None,
        assignment_progress="实施",
        notice_date=date(2026, 8, 1),
        source_provider="eastmoney",
        endpoint="dividend_event",
        capability_version="eastmoney-dividend-event-v1",
        raw_object_path="raw/eastmoney.json",
        fetch_time=datetime.now(timezone.utc),
    )
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        assert metadata.save_dividend_events([event]) == 1
        assert metadata.save_dividend_events([event]) == 1
        rows = metadata.dividend_events()
    assert len(rows) == 1
    assert rows[0]["source_security_code"] == "600519"
