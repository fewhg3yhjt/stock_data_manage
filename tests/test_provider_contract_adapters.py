import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Mapping

import pytest

from stock_data_manage.providers.probes import probe_daily_capability, probe_minute_capability
from stock_data_manage.providers.sina import SinaDailyProvider, SinaMinuteProvider, SinaSnapshotProvider
from stock_data_manage.providers.tdx import TdxMinuteProvider
from stock_data_manage.providers.tencent import TencentDailyProvider, TencentMinuteProvider, TencentSnapshotProvider
from stock_data_manage.providers.transport import UrlLibTransport, RequestPacer, PacedTransport
from stock_data_manage.config.loader import load_provider_configs
from stock_data_manage.routing.factory import build_provider
from pathlib import Path
from stock_data_manage.routing.capabilities import ProviderCapability
from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.providers.contracts import (
    EndpointContract,
    FailureClass,
    HttpResponse,
    ProviderContractError,
    WindowStatus,
)


@dataclass
class FakeTransport:
    responses: list[HttpResponse]
    calls: list[tuple[str, Mapping[str, str], float]] = field(default_factory=list)

    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse:
        self.calls.append((url, params, timeout_seconds))
        return self.responses.pop(0)


def test_shared_pacer_preserves_request_response_and_releases_after_failure():
    time = [0.0]
    waits = []
    def advance(seconds):
        waits.append(seconds)
        time[0] += seconds
    pacer = RequestPacer(clock=lambda: time[0], wait=advance)
    pacer.configure("shared", 0.5, 2)
    pacer.configure("shared", 3, 1)
    response = HttpResponse(200, {"x-source": "unchanged"}, b"exact response bytes")
    first = FakeTransport([response])
    second = FakeTransport([response])
    params = {"param": "sh600519,m1,,120"}
    assert PacedTransport(first, pacer, "shared").get("https://source/path", params=params, timeout_seconds=7) is response
    assert first.calls == [("https://source/path", params, 7)]
    assert first.calls[0][1] is params
    with pytest.raises(RuntimeError):
        with pacer.request("shared"):
            raise RuntimeError("offline transport failure")
    assert PacedTransport(second, pacer, "shared").get("https://source/path", params=params, timeout_seconds=7) is response
    assert waits == [3, 3]
    with pytest.raises(ValueError, match="before issuing"):
        pacer.configure("shared", 4)


def test_factory_paces_each_tencent_single_symbol_minute_request():
    configs = load_provider_configs(Path(__file__).resolve().parents[1] / "config/providers.yaml")
    config = next(c for c in configs if c.provider == "tencent" and c.endpoint == "native_1m")
    clock = [0.0]
    starts = []
    class Source:
        def get(self, url, *, params, timeout_seconds):
            starts.append((clock[0], params["param"]))
            symbol = params["param"].split(",")[0]
            payload = {"code": 0, "data": {symbol: {"m1": [["202609301000", "1", "1", "1", "1", "10"]]}}}
            return HttpResponse(200, {}, json.dumps(payload).encode())
    def advance(seconds):
        clock[0] += seconds
    pacer = RequestPacer(clock=lambda: clock[0], wait=advance)
    source = Source()
    provider = build_provider(config, source, pacer=pacer)
    result = provider.fetch_realtime_minute(["sh600519", "sz000001"], datetime(2026, 9, 30, 10, tzinfo=timezone.utc))
    assert len(result.rows) == 2
    assert starts == [(0, "sh600519,m1,,120"), (1, "sz000001,m1,,120")]
    assert provider.transport.transport is source
    assert config.max_symbols_per_request == 1


def test_factory_keeps_eastmoney_source_transport(monkeypatch):
    from stock_data_manage.routing import factory
    configs = load_provider_configs(Path(__file__).resolve().parents[1] / "config/providers.yaml")
    config = next(c for c in configs if c.provider == "eastmoney" and c.endpoint == "financial_main")
    source = FakeTransport([])
    monkeypatch.setattr(factory, "EastMoneyRequestsTransport", lambda: source)
    provider = build_provider(config)
    assert provider.transport.transport is source
    assert config.request_limit_enforcement == "call_boundary_only"


def response(payload: object, status: int = 200, content_type: str = "application/json") -> HttpResponse:
    return HttpResponse(status, {"Content-Type": content_type}, json.dumps(payload).encode())


def test_contract_rejects_html_200_and_rate_limit() -> None:
    contract = EndpointContract(frozenset({"symbol"}))
    with pytest.raises(ProviderContractError) as html:
        contract.parse_json(HttpResponse(200, {"content-type": "text/html"}, b"<html>blocked"))
    assert html.value.failure_class is FailureClass.HTML_RESPONSE
    with pytest.raises(ProviderContractError) as limited:
        contract.parse_json(HttpResponse(429, {}, b"{}"))
    assert limited.value.failure_class is FailureClass.RATE_LIMITED
    assert not limited.value.retryable
    assert contract.parse_json(
        HttpResponse(200, {"content-type": "text/html"}, b'[{"symbol":"sh600519"}]')
    ) == [{"symbol": "sh600519"}]


def test_contract_detects_schema_change_and_silent_row_limit() -> None:
    contract = EndpointContract(
        frozenset({"symbol", "close"}), max_rows_per_request=2, supports_pagination=False
    )
    with pytest.raises(ProviderContractError) as changed:
        contract.validate_rows([{"symbol": "sh600519"}])
    assert changed.value.failure_class is FailureClass.SCHEMA_CHANGED
    assert contract.validate_rows(
        [{"symbol": "a", "close": 1}, {"symbol": "b", "close": 2}]
    ) is WindowStatus.TRUNCATED


@pytest.mark.parametrize(
    ("status", "failure", "retryable"),
    [
        (500, FailureClass.HTTP_5XX, True),
        (404, FailureClass.HTTP_ERROR, False),
        (403, FailureClass.RATE_LIMITED, False),
        (429, FailureClass.RATE_LIMITED, False),
    ],
)
def test_contract_classifies_http_failures(status: int, failure: FailureClass, retryable: bool) -> None:
    with pytest.raises(ProviderContractError) as error:
        EndpointContract(frozenset()).parse_json(HttpResponse(status, {}, b"{}"))
    assert error.value.failure_class is failure
    assert error.value.retryable is retryable


def test_contract_classifies_invalid_json() -> None:
    with pytest.raises(ProviderContractError) as error:
        EndpointContract(frozenset()).parse_json(HttpResponse(200, {}, b"not-json"))
    assert error.value.failure_class is FailureClass.INVALID_JSON


def test_tencent_adapter_parses_observed_daily_envelope() -> None:
    transport = FakeTransport(
        [
            response(
                {
                    "code": 0,
                    "msg": "",
                    "data": {
                        "sh600519": {
                            "day": [
                                ["2026-09-10", "1291.000", "1285.130", "1294.990", "1282.000", "18900.000"],
                                ["2026-09-11", "1285.150", "1275.160", "1286.150", "1263.010", "34801.000"],
                            ]
                        }
                    },
                }
            )
        ]
    )
    provider = TencentDailyProvider(transport)
    result = provider.fetch_daily(["sh600519"], date(2026, 9, 11))
    assert len(result.rows) == 1
    assert result.rows[0]["close"] == "1275.160"
    assert transport.calls[0][1]["param"] == "sh600519,day,,,1024"


def test_tencent_forward_daily_adapter_requires_qfq_envelope() -> None:
    transport = FakeTransport(
        [
            response(
                {
                    "code": 0,
                    "data": {
                        "sh600519": {
                            "qfqday": [["2026-09-11", "10", "10", "10", "10", "100"]],
                            "day": [["2026-09-11", "1", "1", "1", "1", "100"]],
                        }
                    },
                }
            )
        ]
    )
    provider = TencentDailyProvider(transport, adjustment=Adjustment.FORWARD)
    result = provider.fetch_daily(["sh600519"], date(2026, 9, 11))
    assert result.adjustment is Adjustment.FORWARD
    assert result.rows[0]["close"] == "10"
    assert transport.calls[0][1]["param"] == "sh600519,day,,,640,qfq"


def test_tencent_forward_daily_does_not_fallback_to_unadjusted_day() -> None:
    provider = TencentDailyProvider(
        FakeTransport([response({"code": 0, "data": {"sh600519": {"day": [["2026-09-11", "1", "1", "1", "1", "100"]]}}})]),
        adjustment=Adjustment.FORWARD,
    )
    result = provider.fetch_daily(["sh600519"], date(2026, 9, 11))
    assert result.rows == ()
    assert result.adjustment is Adjustment.FORWARD


def test_tencent_forward_daily_is_qualified_only_for_stock_and_etf() -> None:
    provider = TencentDailyProvider(FakeTransport([]), adjustment=Adjustment.FORWARD)
    assert provider.supported_asset_types == frozenset({AssetType.STOCK, AssetType.ETF})


def test_daily_adapter_reports_the_full_returned_window() -> None:
    transport = FakeTransport(
        [
            response(
                {
                    "code": 0,
                    "data": {
                        "sh600519": {
                            "day": [
                                ["2026-09-10", "1", "1", "1", "1", "1"],
                                ["2026-09-11", "2", "2", "2", "2", "2"],
                            ]
                        }
                    },
                }
            )
        ]
    )
    result = TencentDailyProvider(transport).fetch_daily(["sh600519"], date(2026, 9, 11))
    assert result.returned_row_count == 2
    assert result.returned_first_key == "2026-09-10"
    assert result.returned_last_key == "2026-09-11"


def test_sina_adapter_parses_observed_daily_envelope_and_probe_evidence() -> None:
    payload = [
        {
            "day": "2026-09-11",
            "open": "1285.150",
            "high": "1286.150",
            "low": "1263.010",
            "close": "1275.160",
            "volume": "3480142",
        }
    ]
    transport = FakeTransport([response(payload)])
    provider = SinaDailyProvider(transport)
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    evidence = probe_daily_capability(
        provider,
        symbol="sh600519",
        trade_date=date(2026, 9, 11),
        now=now,
        ttl=timedelta(days=7),
    )
    assert evidence.eligible_for_selection
    assert evidence.row_count == 1
    assert evidence.first_key == "2026-09-11"
    assert evidence.validation_expires_at == now + timedelta(days=7)


def test_empty_probe_is_not_promoted_to_success() -> None:
    provider = SinaDailyProvider(FakeTransport([response(None)]))
    evidence = probe_daily_capability(
        provider,
        symbol="sh600519",
        trade_date=date(2026, 9, 11),
        now=datetime(2026, 9, 13, tzinfo=timezone.utc),
        ttl=timedelta(days=1),
    )
    assert not evidence.eligible_for_selection
    assert evidence.failure_class == FailureClass.TEMPORARY_EMPTY.value


def test_probe_evidence_is_persisted_in_capability_registry(tmp_path) -> None:
    payload = [
        {
            "day": "2026-09-11",
            "open": "10",
            "high": "11",
            "low": "9",
            "close": "10.5",
            "volume": "100",
        }
    ]
    provider = SinaDailyProvider(FakeTransport([response(payload)]))
    evidence = probe_daily_capability(
        provider,
        symbol="sh600519",
        trade_date=date(2026, 9, 11),
        now=datetime(2026, 9, 13, tzinfo=timezone.utc),
        ttl=timedelta(days=7),
    )
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        metadata.save_probe_evidence(
            evidence, dataset="daily_bar", market="XSHG", asset_type="stock", code_prefix="600"
        )
        saved = metadata.latest_probe(
            provider="sina",
            endpoint="full_history",
            capability_version="sina-cn-marketdata-v1",
            dataset="daily_bar",
            market="XSHG",
            asset_type="stock",
            code_prefix="600",
        )
    assert saved is not None
    assert saved["eligible_for_selection"] is True
    assert saved["evidence_hash"] == evidence.evidence_hash
    assert saved["request_scope_json"] == '["sh600519"]'
    assert saved["response_status"] == 200
    assert saved["returned_window"] == "complete"
    assert "trade_date" in saved["field_semantics_json"]
    assert saved["adjustment"] == "none"


def test_forward_probe_persists_forward_adjustment(tmp_path) -> None:
    provider = TencentDailyProvider(
        FakeTransport(
            [
                response(
                    {
                        "code": 0,
                        "data": {
                            "sh600519": {
                                "qfqday": [["2026-09-11", "10", "10", "10", "10", "100"]]
                            }
                        },
                    }
                )
            ]
        ),
        adjustment=Adjustment.FORWARD,
    )
    evidence = probe_daily_capability(
        provider,
        symbol="sh600519",
        trade_date=date(2026, 9, 11),
        now=datetime(2026, 9, 13, tzinfo=timezone.utc),
        ttl=timedelta(days=7),
    )
    assert evidence.adjustment == "forward"
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        metadata.save_probe_evidence(
            evidence,
            dataset="daily_bar",
            market="XSHG",
            asset_type="stock",
            adjustment="forward",
        )
        saved = metadata.latest_probe(
            provider="tencent",
            endpoint="forward_history",
            capability_version="tencent-qfq-kline-v1",
            dataset="daily_bar",
            market="XSHG",
            asset_type="stock",
            adjustment="forward",
        )
    assert saved is not None and saved["adjustment"] == "forward"


def minute_capability(provider: str, endpoint: str, frequency: int) -> ProviderCapability:
    return ProviderCapability(
        provider=provider,
        endpoint=endpoint,
        version="v1",
        datasets=frozenset({Dataset.MINUTE_BAR_1M if frequency == 1 else Dataset.MINUTE_BAR_5M}),
        exchanges=frozenset({Exchange.XSHG}),
        asset_types=frozenset({AssetType.STOCK}),
        frequencies=frozenset({frequency}),
        adjustments=frozenset({Adjustment.NONE}),
        priority=100,
        validated_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        validation_expires_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
        max_symbols_per_request=100,
    )


def test_tencent_minute_adapter_parses_compact_timestamp() -> None:
    transport = FakeTransport(
        [
            response(
                {
                    "code": 0,
                    "data": {
                        "sh600519": {
                            "m1": [["202609111301", "1271.63", "1273.07", "1277.0", "1271.63", "546"]]
                        }
                    },
                }
            )
        ]
    )
    provider = TencentMinuteProvider(transport, minute_capability("tencent", "native_1m", 1))
    result = provider.fetch_realtime_minute(["sh600519"], datetime(2026, 9, 11, 13, 2, tzinfo=timezone.utc))
    assert result.rows[0]["bar_time"] == "2026-09-11T13:01:00"
    assert result.units == ("volume:lot",)


def test_minute_adapter_filters_rows_newer_than_as_of() -> None:
    transport = FakeTransport(
        [
            response(
                {
                    "code": 0,
                    "data": {
                        "sh600519": {
                            "m1": [
                                ["202609111301", "1", "1", "1", "1", "10"],
                                ["202609111302", "2", "2", "2", "2", "20"],
                            ]
                        }
                    },
                }
            )
        ]
    )
    result = TencentMinuteProvider(
        transport, minute_capability("tencent", "native_1m", 1)
    ).fetch_realtime_minute(["sh600519"], datetime(2026, 9, 11, 13, 1, tzinfo=timezone.utc))
    assert [row["bar_time"] for row in result.rows] == ["2026-09-11T13:01:00"]
    assert transport.calls[0][1]["param"] == "sh600519,m1,,120"


def test_minute_probe_records_time_range_and_expiry() -> None:
    as_of = datetime(2026, 9, 11, 13, 2, tzinfo=timezone.utc)
    provider = TencentMinuteProvider(
        FakeTransport(
            [
                response(
                    {
                        "code": 0,
                        "data": {"sh600519": {"m1": [["202609111301", "1", "1", "1", "1", "10"]]}},
                    }
                )
            ]
        ),
        minute_capability("tencent", "native_1m", 1),
    )
    evidence = probe_minute_capability(
        provider, symbol="sh600519", as_of=as_of, ttl=timedelta(days=7)
    )
    assert evidence.eligible_for_selection
    assert evidence.row_count == 1
    assert evidence.first_key == "2026-09-11T13:01:00"
    assert evidence.validation_expires_at == as_of + timedelta(days=7)


def test_sina_minute_adapter_parses_day_timestamp() -> None:
    transport = FakeTransport(
        [
            response(
                [
                    {
                        "day": "2026-09-11 15:00:00",
                        "open": "1276.47",
                        "high": "1277.58",
                        "low": "1275.16",
                        "close": "1275.16",
                        "volume": "120232",
                    }
                ]
            )
        ]
    )
    provider = SinaMinuteProvider(transport, minute_capability("sina", "native_5m", 5))
    result = provider.fetch_realtime_minute(["sh600519"], datetime(2026, 9, 11, 15, 1, tzinfo=timezone.utc))
    assert result.rows[0]["trade_date"] == "2026-09-11"
    assert result.rows[0]["bar_time"] == "2026-09-11T15:00:00"
    assert result.units == ("volume:share",)


def test_tdx_minute_adapter_normalizes_client_rows_and_closes_client() -> None:
    class FakeTdxClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.closed = False

        def bars(self, **kwargs: object) -> list[dict[str, object]]:
            self.calls.append(kwargs)
            return [
                {
                    "datetime": "2026-09-11 13:01:00",
                    "open": 10,
                    "close": 10.2,
                    "high": 10.3,
                    "low": 9.9,
                    "vol": 123,
                    "amount": 4567,
                }
            ]

        def close(self) -> None:
            self.closed = True

    client = FakeTdxClient()
    provider = TdxMinuteProvider(
        lambda: client,
        minute_capability("tdx", "delayed_1m", 1),
    )
    result = provider.fetch_realtime_minute(
        ["sh600519"], datetime(2026, 9, 11, 13, 2, tzinfo=timezone.utc)
    )
    assert result.rows[0]["symbol"] == "sh600519"
    assert result.rows[0]["volume"] == 123
    assert client.calls == [{"symbol": "600519", "frequency": 8, "offset": 240}]
    assert client.closed


def test_tdx_adapter_marks_missing_bars_as_empty_without_inventing_rows() -> None:
    class EmptyClient:
        def bars(self, **kwargs: object) -> list[object]:
            return []

    result = TdxMinuteProvider(
        EmptyClient,
        minute_capability("tdx", "delayed_1m", 1),
    ).fetch_realtime_minute(["bj920000"], datetime(2026, 9, 11, tzinfo=timezone.utc))
    assert result.rows == ()
    assert result.requested_symbols == ("bj920000",)


def test_tdx_adapter_rejects_rows_with_missing_ohlcv_fields() -> None:
    class BrokenClient:
        def bars(self, **kwargs: object) -> list[dict[str, object]]:
            return [{"datetime": "2026-09-11 13:01:00", "close": 10.2}]

    with pytest.raises(ProviderContractError) as error:
        TdxMinuteProvider(
            BrokenClient,
            minute_capability("tdx", "delayed_1m", 1),
        ).fetch_realtime_minute(["sh600519"], datetime(2026, 9, 11, tzinfo=timezone.utc))
    assert error.value.failure_class is FailureClass.SCHEMA_CHANGED


def test_tencent_snapshot_adapter_parses_observed_quote_line() -> None:
    parts = ["-"] * 38
    parts[1] = "贵州茅台"
    parts[2] = "600519"
    parts[3:7] = ["1275.16", "1285.15", "1285.15", "34801"]
    parts[30] = "20260911150003"
    parts[33] = "1286.15"
    parts[34] = "1263.01"
    parts[37] = "445001.2"
    body = f'v_sh600519="{"~".join(parts)}";\n'
    provider = TencentSnapshotProvider(
        FakeTransport([HttpResponse(200, {"content-type": "text/plain"}, body.encode("gbk"))]),
        minute_capability("tencent", "bulk_snapshot", 1),
    )
    result = provider.fetch_snapshot(["sh600519"], datetime(2026, 9, 11, tzinfo=timezone.utc))
    assert result.returned_symbols == {"sh600519"}
    assert result.rows[0]["name"] == "贵州茅台"
    assert result.rows[0]["close"] == "1275.16"
    assert result.rows[0]["volume_unit"] == "lot"


def test_sina_snapshot_adapter_parses_observed_quote_line() -> None:
    fields = ["贵州茅台", "1285.15", "1275.16", "1275.16", "1286.15", "1263.01"]
    fields.extend(["0", "0", "3480142", "445001200"])
    fields.extend(["0"] * (30 - len(fields)))
    fields.extend(["2026-09-11", "15:00:03"])
    body = f'var hq_str_sh600519="{",".join(fields)}";\n'
    provider = SinaSnapshotProvider(
        FakeTransport([HttpResponse(200, {"content-type": "text/plain"}, body.encode())]),
        minute_capability("sina", "snapshot", 5),
    )
    result = provider.fetch_snapshot(["sh600519"], datetime(2026, 9, 11, tzinfo=timezone.utc))
    assert result.returned_symbols == {"sh600519"}
    assert result.rows[0]["trade_date"] == "2026-09-11"
    assert result.rows[0]["volume_unit"] == "share"



def test_tencent_snapshot_batches_entire_requested_scope_without_dropping_remainder():
    symbols = tuple(f"sh{600000+i}" for i in range(203))
    class BatchTransport:
        def __init__(self):
            self.requests = []
        def get(self, url, *, params, timeout_seconds):
            batch = url.split("q=", 1)[1].split(",")
            self.requests.append(tuple(batch))
            parts = ["-"] * 38
            parts[1] = "测试股票"
            parts[3:7] = ["10", "9", "9", "100"]
            parts[30], parts[33], parts[34], parts[37] = "20260930150000", "11", "8", "1000"
            body = "\n".join(f'v_{s}="{"~".join(parts[:2] + [s[2:]] + parts[3:])}";' for s in batch).encode("gbk")
            return HttpResponse(200, {"content-type": "text/html; charset=GBK"}, body)
    transport = BatchTransport()
    provider = TencentSnapshotProvider(transport, minute_capability("tencent", "bulk_snapshot", 1))
    result = provider.fetch_snapshot(symbols, datetime(2026, 9, 30, tzinfo=timezone.utc))
    assert [len(b) for b in transport.requests] == [100, 100, 3]
    assert result.requested_symbols == symbols and result.returned_symbols == set(symbols)
    assert all(row["name"] == "测试股票" for row in result.rows)
    for invalid in ([], [symbols[0], symbols[0]]):
        with pytest.raises(ValueError):
            provider.fetch_snapshot(invalid, datetime(2026, 9, 30, tzinfo=timezone.utc))
    provider.max_symbols_per_request = 0
    with pytest.raises(ValueError, match="batch size"):
        provider.fetch_snapshot(symbols, datetime(2026, 9, 30, tzinfo=timezone.utc))
