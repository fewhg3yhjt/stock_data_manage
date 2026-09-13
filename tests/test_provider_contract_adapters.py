import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Mapping

import pytest

from stock_data_manage.capability_probe import probe_daily_capability, probe_minute_capability
from stock_data_manage.http_providers import (
    SinaDailyProvider,
    SinaMinuteProvider,
    SinaSnapshotProvider,
    TdxMinuteProvider,
    TencentDailyProvider,
    TencentMinuteProvider,
    TencentSnapshotProvider,
)
from stock_data_manage.capability import ProviderCapability
from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange
from stock_data_manage.metadata import MetadataStore
from stock_data_manage.provider_contract import (
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
