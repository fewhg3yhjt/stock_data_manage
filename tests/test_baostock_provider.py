from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
from types import SimpleNamespace

import pytest

from stock_data_manage.domain import Adjustment, AssetType
from stock_data_manage.providers.baostock.daily import BaoStockDailyProvider
from stock_data_manage.providers.baostock.minute import BaoStockMinuteProvider
from stock_data_manage.providers.baostock import daily as daily_module
from stock_data_manage.providers.baostock import minute as minute_module
from stock_data_manage.providers.baostock.session import captured_sdk_queries
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.storage.raw import RawObjectStore


class FakeResult:
    error_code = "0"
    error_msg = ""
    fields = [
        "date", "code", "open", "high", "low", "close", "preclose",
        "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg",
        "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "isST",
    ]

    def __init__(self, rows):
        self._rows = iter(rows)

    def next(self):
        try:
            self._row = next(self._rows)
            return True
        except StopIteration:
            return False

    def get_row_data(self):
        return self._row


class FakeMinuteResult(FakeResult):
    fields = ["date", "time", "code", "open", "high", "low", "close", "volume", "amount", "adjustflag"]


class FakeBaoStock:
    def __init__(self):
        self.calls = []

    def login(self):
        return SimpleNamespace(error_code="0", error_msg="")

    def logout(self):
        return SimpleNamespace(error_code="0", error_msg="")

    def query_history_k_data_plus(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if kwargs["frequency"] == "d":
            import baostock.common.context as sdk_context
            sdk_context.default_socket.recv(4096)
            code = kwargs["code"]
            return FakeResult([
                ["2026-09-11", code, "10", "11", "9", "10.5", "9.8", "100", "1000", kwargs["adjustflag"], "1", "1", "2", "12", "3", "4", "5", "0"]
            ])
        return FakeMinuteResult([
            ["2026-09-11", "093100", "sh.600519", "10", "11", "9", "10.5", "100", "1000", "3"]
        ])


@contextmanager
def fake_session(client):
    yield client


class MemorySocket:
    def __init__(self, responses):
        self.responses = list(responses)

    def recv(self, size, *args):
        return self.responses.pop(0) if self.responses else b""

    def send(self, data, *args):
        return len(data)


def daily_request(symbol, trade_date, *, adjustment=Adjustment.FORWARD):
    return {
        "code": f"{symbol[:2]}.{symbol[2:]}",
        "fields": ",".join(daily_module.BAOSTOCK_DAILY_SOURCE_FIELDS),
        "start_date": trade_date.isoformat(),
        "end_date": trade_date.isoformat(),
        "frequency": "d",
        "adjustflag": {Adjustment.NONE: "3", Adjustment.BACKWARD: "1", Adjustment.FORWARD: "2"}[adjustment],
    }


def test_baostock_daily_captures_wire_and_full_sdk_rows_then_replays_without_network(monkeypatch, tmp_path):
    import baostock.common.context as sdk_context

    trade_date = date(2026, 9, 11)
    scope = {"symbols": ["sh600519"], "trade_date": trade_date.isoformat(), "adjustment": "forward"}
    request = daily_request("sh600519", trade_date)
    client = FakeBaoStock()
    monkeypatch.setattr(sdk_context, "default_socket", MemorySocket([b"wire-response-1"]), raising=False)
    source = RawObjectStore(tmp_path / "source")
    with captured_sdk_queries(
        source, mode="live", replay_manifest=None, evidence_roots=(), scope=scope,
        code_version="baostock-daily-test-v1", trade_date=trade_date,
        pacer=RequestPacer(wait=lambda _: None), interval_seconds=3, max_age_seconds=0,
        client=client, query_requests={"query_history_k_data_plus": [request]},
    ) as (sdk_client, archive, events):
        provider = BaoStockDailyProvider(adjustment=Adjustment.FORWARD, start_date=trade_date, client=sdk_client)
        result = provider.fetch_daily(["sh600519"], trade_date, raw_archive=archive)

    assert result.rows[0]["close"] == "10.5"
    assert result.adjustment is Adjustment.FORWARD
    assert client.calls[0][1]["adjustflag"] == "2"
    assert client.calls[0][1]["fields"] == ",".join(daily_module.BAOSTOCK_DAILY_SOURCE_FIELDS)
    assert result.source_rows[0]["peTTM"] == "12"
    assert len(result.source_rows[0]) == 19  # 18 source columns plus the local symbol key
    entries = [json.loads(line) for line in source.manifest.read_text(encoding="utf-8").splitlines()]
    wire = next(item for item in entries if item["event"] == "tcp_response")
    decoded = next(item for item in entries if item["event"] == "source_payload")
    assert RawObjectStore.read_response(source.manifest, wire) == b"wire-response-1"
    assert decoded["original_transport_bytes_available"] is True
    assert decoded["tcp_response_sha256"] == wire["body_sha256"]
    saved_result = json.loads(RawObjectStore.read_response(source.manifest, decoded))
    assert saved_result["fields"] == list(daily_module.BAOSTOCK_DAILY_SOURCE_FIELDS)
    assert len(saved_result["rows"]) == 1

    replay_client = FakeBaoStock()
    replay = RawObjectStore(tmp_path / "replay")
    with captured_sdk_queries(
        replay, mode="replay", replay_manifest=source.manifest, evidence_roots=(), scope=scope,
        code_version="baostock-daily-test-v2", trade_date=trade_date,
        pacer=RequestPacer(wait=lambda _: None), interval_seconds=3, max_age_seconds=0,
        client=replay_client, query_requests={"query_history_k_data_plus": [request]},
    ) as (sdk_client, archive, replay_events):
        replayed = BaoStockDailyProvider(adjustment=Adjustment.FORWARD, start_date=trade_date, client=sdk_client).fetch_daily(
            ["sh600519"], trade_date, raw_archive=archive,
        )
    assert replayed.rows == result.rows
    assert replayed.source_rows == result.source_rows
    assert replay_client.calls == []
    assert all(not event.get("actual_session_called", False)
               for event in replay_events if event["event"] == "sdk_session")

    missing_date = date(2026, 9, 12)
    missing_scope = {**scope, "trade_date": missing_date.isoformat()}
    missing_request = daily_request("sh600519", missing_date)
    with captured_sdk_queries(
        RawObjectStore(tmp_path / "missing"), mode="replay", replay_manifest=source.manifest, evidence_roots=(),
        scope=missing_scope, code_version="baostock-daily-test-v2", trade_date=missing_date,
        pacer=RequestPacer(wait=lambda _: None), interval_seconds=3, max_age_seconds=0,
        client=replay_client, query_requests={"query_history_k_data_plus": [missing_request]},
    ) as (sdk_client, archive, _):
        with pytest.raises(ValueError, match="never falls back to network"):
            BaoStockDailyProvider(adjustment=Adjustment.FORWARD, start_date=missing_date, client=sdk_client).fetch_daily(
                ["sh600519"], missing_date, raw_archive=archive,
            )
    assert replay_client.calls == []


def test_baostock_daily_requires_capture_context():
    with pytest.raises(ValueError, match="captured_sdk_queries"):
        BaoStockDailyProvider().fetch_daily(["sh600519"], date(2026, 9, 11))


def test_baostock_5m_normalizes_rows_and_closes_to_response_shape(monkeypatch):
    client = FakeBaoStock()
    monkeypatch.setattr(minute_module, "logged_in_session", lambda: fake_session(client))
    provider = BaoStockMinuteProvider()
    result = provider.fetch_realtime_minute(["sh600519"], datetime(2026, 9, 11, 10, tzinfo=timezone.utc))
    assert result.rows[0]["bar_time"] == "2026-09-11 093100"
    assert result.units == ("volume:share", "amount:cny")
    assert client.calls[0][1]["frequency"] == "5"
