"""YAML execution and migration checks against independently archived source responses."""
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil

import pytest
import requests
import yaml

from stock_data_manage.config.loader import load_dataset_normalization_rules
from stock_data_manage.domain import Adjustment, AssetType, Dataset, Exchange, QualityStatus
from stock_data_manage.pipeline.inputs import collect_input
from stock_data_manage.providers.transport import captured_requests, RequestPacer
from stock_data_manage.quality.normalization import Normalizer, NormalizationError
from stock_data_manage.storage.raw import RawObjectStore

ROOT = Path(__file__).resolve().parents[1]
TENCENT_ARCHIVE = ROOT / "provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson"
SDK_ARCHIVE = ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson"
CASES = [
    ("ASTOCK-002-daily", {"request": {"symbol": "600519", "start_date": date(2026, 9, 1), "end_date": date(2026, 9, 18)}}, TENCENT_ARCHIVE, 14),
    ("ASTOCK-002-5m", {"request": {"symbol": "300750"}, "config": {"count": 96}}, TENCENT_ARCHIVE, 96),
    ("ASTOCK-045", {"request": {"trade_date": date(2026, 9, 30)}, "calendar": {"trading_dates": [date(2026, 9, 30)]}}, SDK_ARCHIVE, 52),
    ("ASTOCK-070", {}, SDK_ARCHIVE, 8797),
]


def read_artifact(report, key):
    path = Path(report["run_directory"]) / report[key]["path"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == report[key]["sha256"]
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def no_network(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("offline validation must never issue a network request")
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", fail)


@pytest.mark.parametrize("input_id,context,manifest,count", CASES)
def test_archived_inputs_execute_yaml_and_preserve_evidence(tmp_path, no_network, input_id, context, manifest, count):
    if input_id in {"ASTOCK-045", "ASTOCK-070"}:
        pytest.importorskip("akshare")
    original_send, original_init = requests.Session.send, requests.Session.__init__
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config",
                           output_root=tmp_path, replay_manifest=manifest)
    assert report["status"] == "candidate_complete", report
    assert report["row_count"] == report["coverage_denominator"] == count
    assert not report["eligible_for_production_routing"] and report["production_writes"] == report["live_http_calls"] == 0
    assert requests.Session.send is original_send and requests.Session.__init__ is original_init
    assert report["normalization_status"] == "pending_validation"
    source, mapped = read_artifact(report, "source_rows"), read_artifact(report, "output")
    raw_manifest = Path(report["run_directory"]) / report["raw_manifest"]["path"]
    for event in report["responses"]:
        assert event["mode"] == "replay"
        if event["outcome"] == "response":
            body = RawObjectStore.read_response(raw_manifest, event)
            assert len(body) == event["body_bytes"]
            assert event["source_ref"]["manifest"] == str(manifest.resolve())
    if input_id.startswith("ASTOCK-002"):
        assert all(row["volume"] is None for row in mapped)
        assert all(row["5"] is not None for row in source)
        assert all(Decimal(row["close"]) == Decimal(raw["2"]) for row, raw in zip(mapped, source))
        assert all(row["amount"] is None for row in mapped)
    elif input_id == "ASTOCK-045":
        pandas = pytest.importorskip("pandas")
        expected = pandas.read_csv(ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003/43_东财涨停池/data.csv", dtype=str)
        assert set(source[0]) == set(expected.columns)
        for actual, golden in zip(source, expected.to_dict("records")):
            for key, value in golden.items():
                if key in {"序号", "涨跌幅", "最新价", "成交额", "流通市值", "总市值", "换手率", "封板资金", "炸板次数", "连板数"}:
                    assert float(actual[key]) == pytest.approx(float(value), rel=1e-12)
                else:
                    assert str(actual[key]) == value
        assert mapped[0]["source_security_code"] == source[0]["代码"]
        assert all(row["amount"] is None for row in mapped)
    else:
        expected = (ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003/68_交易日历/data.csv").read_text(encoding="utf-8-sig").splitlines()[1:]
        assert [row["trade_date"] for row in source] == expected
        assert [row["trade_date"] for row in mapped] == expected


@pytest.mark.parametrize("input_id,context,manifest,count", CASES[:2])
def test_tencent_matches_original_shipped_script(tmp_path, no_network, input_id, context, manifest, count):
    path = ROOT / "provider_validation/tests/source_snapshots/a-stock-data/tests/test_v39_sources.py"
    spec = importlib.util.spec_from_file_location("original_v39_input_comparison", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    namespace = module.load_shipped_code()
    store = RawObjectStore(tmp_path / "original")
    with captured_requests(store, provider="tencent", endpoint=input_id, scope=context, code_version="source-snapshot",
                           pacer=RequestPacer(), replay_manifest=manifest) as events:
        if input_id.endswith("daily"):
            frame = namespace["tencent_kline"]("600519", period="day", adjust="qfq", start="2026-09-01", end="2026-09-18")
        else:
            frame = namespace["tencent_kline"]("300750", period="m5", count=96)
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "adapter", replay_manifest=manifest)
    assert report["status"] == "candidate_complete"
    mapped, source = read_artifact(report, "output"), read_artifact(report, "source_rows")
    assert len(frame) == len(mapped) == count
    for golden, actual, raw in zip(frame.to_dict("records"), mapped, source):
        for name in ("open", "high", "low", "close"):
            assert Decimal(actual[name]) == Decimal(str(golden[name]))
        assert Decimal(raw["5"]) == Decimal(str(golden["volume"]))
        assert actual["trade_date"] == (golden["date"] if input_id.endswith("daily") else golden["datetime"][:10])
        if input_id.endswith("5m"):
            assert actual["bar_time"].replace("T", " ")[:16] == golden["datetime"]
            assert float(actual["turnover_rate_pct"]) == pytest.approx(golden["turnover_rate_pct"], rel=1e-12)
            assert Decimal(actual["turnover_rate_pct"]) == Decimal(raw["7"]) / 100
    assert report["source_url"] == frame["source_url"].iloc[0]
    # Compare original and migrated requests, statuses, failures and returned body hashes.
    original_events = [json.loads(line) for line in (store.root / "manifest.ndjson").read_text(encoding="utf-8").splitlines()]
    for left, right in zip(original_events, report["responses"]):
        for key in ("url", "method", "request_headers", "outcome", "status_code", "body_sha256", "error_type"):
            assert left.get(key) == right.get(key), key
    assert len(original_events) == len(report["responses"])


def test_yaml_alias_mapping_status_and_code_scope_are_executed(tmp_path):
    document = {"dataset": "daily_bar", "rules": [{"provider": "fixture", "endpoint": "daily", "exchange": "XSHG", "asset_type": "stock", "frequency": None,
        "adjustment": "none", "volume_multiplier": "100", "amount_multiplier": "10000", "version": "alias-test",
        "status": "pending_validation", "code_prefixes": ["sh"], "null_values": [None, "--"],
        "field_mapping": {"symbol": "ticker", "trade_date": "date", "open": "O", "high": "H", "low": "L", "close": "C", "volume": "V", "amount": "A"}}]}
    path = tmp_path / "daily_bar.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    rules = load_dataset_normalization_rules(tmp_path, "daily_bar")
    raw = {"ticker": "sh600519", "date": "2026-09-18", "O": "10", "H": "11", "L": "9", "C": "10.5", "V": "2", "A": "--"}
    kwargs = dict(dataset=Dataset.DAILY_BAR, instrument_id="XSHG:600519", exchange=Exchange.XSHG, asset_type=AssetType.STOCK,
                  provider="fixture", endpoint="daily", adjustment=Adjustment.NONE, capability_priority=1, capability_version="fixture", raw_object_path="fixture.json",
                  fetch_time=datetime.now(timezone.utc), quality_status=QualityStatus.PROVISIONAL)
    with pytest.raises(NormalizationError, match="not validated"):
        Normalizer(rules).normalize_bar(raw, **kwargs)
    record = Normalizer(rules, allow_pending=True).normalize_bar(raw, **kwargs)
    assert record.close == Decimal("10.5") and record.volume == 200 and record.amount is None
    with pytest.raises(NormalizationError, match="found 0"):
        Normalizer(rules, allow_pending=True).normalize_bar({**raw, "ticker": "sz000001"}, **kwargs)


def test_yaml_types_units_dates_timezone_and_nullable_values():
    fields = {"date": {"type": "date", "required": True}, "time": {"type": "datetime", "required": True},
              "value": {"type": "decimal"}, "count": {"type": "integer"}, "nullable": {"type": "decimal"}}
    rule = {"status": "validated", "field_mapping": {"date": "D", "time": "T", "value": "V", "count": "N", "nullable": "X"},
            "null_values": [None, "--"], "transforms": {"date": {"format": "%Y%m%d"}, "time": {"format": "%Y%m%d%H%M", "timezone": "UTC"},
                                                      "value": {"remove_commas": True, "multiplier": "100"}}}
    row = Normalizer.normalize_fields({"D": "20260918", "T": "202609180930", "V": "1,234.50", "N": "2", "X": "--"}, rule=rule, fields=fields)
    assert row["date"] == date(2026, 9, 18) and row["time"].utcoffset().total_seconds() == 0
    assert row["value"] == Decimal("123450") and row["count"] == 2 and row["nullable"] is None


@pytest.mark.parametrize("value", [True, "nan", "inf", None, "garbage"])
def test_unverified_units_still_require_valid_source_numbers(value):
    rule = {"status": "pending_validation", "field_mapping": {"volume": "5"}, "unverified_fields": ["volume"], "source_required_numeric": ["5"]}
    with pytest.raises(NormalizationError, match="source number"):
        Normalizer.normalize_fields({"5": value}, rule=rule, fields={"volume": {"type": "decimal"}}, allow_pending=True)


@pytest.mark.parametrize("status", ["pending_validation", "disabled", "expired"])
def test_unvalidated_generic_rules_cannot_publish(status):
    with pytest.raises(NormalizationError, match="not validated"):
        Normalizer.normalize_fields({}, rule={"status": status}, fields={})


def test_field_selection_uses_yaml_and_requires_mandatory_fields(tmp_path, no_network):
    config = tmp_path / "config"
    shutil.copytree(ROOT / "config", config)
    fields = yaml.safe_load((config / "datasets/daily_bar.yaml").read_text(encoding="utf-8"))["fields"]
    required = [name for name, definition in fields.items() if definition.get("required")]
    input_id, context, manifest, _ = CASES[0]
    report = collect_input(input_id=input_id, context=context, config_root=config, output_root=tmp_path / "out", replay_manifest=manifest, fields=required)
    assert report["status"] == "candidate_complete"
    assert set(read_artifact(report, "output")[0]) == set(required)
    with pytest.raises(ValueError, match="required fields"):
        collect_input(input_id=input_id, context=context, config_root=config, output_root=tmp_path / "out", replay_manifest=manifest, fields=["close"])
    # Changing only the YAML mapping changes the actual collected field.
    path = config / "normalization/daily_bar.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["rules"][0]["field_mapping"]["open"] = "2"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    changed = collect_input(input_id=input_id, context=context, config_root=config, output_root=tmp_path / "out", replay_manifest=manifest)
    assert changed["status"] == "candidate_complete"
    assert all(row["open"] == row["close"] for row in read_artifact(changed, "output"))


def test_replay_missing_request_is_failure_with_no_network_fallback(tmp_path, no_network):
    input_id, context, manifest, _ = CASES[0]
    original = requests.Session.send
    report = collect_input(input_id=input_id, context={"request": {**context["request"], "symbol": "000001"}},
                           config_root=ROOT / "config", output_root=tmp_path, replay_manifest=manifest)
    assert report["status"] == "failed" and report["failure_class"] == "ValueError"
    assert "never falls back" in report["error"]
    assert requests.Session.send is original
    assert Path(report["report_path"]).is_file()


def test_date_snapshot_requires_explicit_calendar_and_blocks_production_output(tmp_path):
    with pytest.raises(ValueError, match="calendar coverage"):
        collect_input(input_id="ASTOCK-045", context={"request": {"trade_date": date(2026, 9, 30)}}, config_root=ROOT / "config", output_root=tmp_path, replay_manifest=SDK_ARCHIVE)
    with pytest.raises(ValueError, match="production"):
        collect_input(input_id="ASTOCK-070", context={}, config_root=ROOT / "config", output_root=ROOT / "data/raw", replay_manifest=SDK_ARCHIVE)


def test_exact_response_is_saved_before_json_parser_and_secrets_are_redacted(tmp_path, no_network):
    archive = RawObjectStore(tmp_path / "archive")
    response = requests.Response()
    response.status_code, response._content, response.encoding = 200, b"not-json\x00\xff", "utf-8"
    request = requests.Request("GET", "https://example.test/input?token=hidden", headers={"Authorization": "hidden"}).prepare()
    original_event = archive.record_response(response=response, url=request.url, method="GET", request_headers=request.headers,
        scope={"token": "hidden"}, provider="fixture", endpoint="input", code_version="fixture")
    destination = RawObjectStore(tmp_path / "destination")
    with captured_requests(destination, provider="fixture", endpoint="input", scope={}, code_version="fixture", pacer=RequestPacer(), replay_manifest=archive.root / "manifest.ndjson"):
        result = requests.get(request.url)
        assert RawObjectStore.read_response(destination.root / "manifest.ndjson", original_event) == response.content
        with pytest.raises(requests.exceptions.JSONDecodeError):
            result.json()
    assert "hidden" not in (archive.root / "manifest.ndjson").read_text(encoding="utf-8")
    response._content = b'{"access_token":"sensitive"}'
    with pytest.raises(ValueError, match="retention suppressed"):
        destination.record_response(response=response, url=request.url, method="GET", request_headers={}, scope={}, provider="fixture", endpoint="input", code_version="fixture")
    assert not (destination.root / "bodies" / (hashlib.sha256(response.content).hexdigest() + ".bin")).exists()


@pytest.mark.parametrize("mutation", ["duplicate", "outside_window", "empty_adjusted", "malformed_volume"])
def test_daily_source_contract_rejects_invalid_rows_and_keeps_original_bytes(tmp_path, no_network, mutation):
    original = next(json.loads(line) for line in TENCENT_ARCHIVE.read_text(encoding="utf-8").splitlines() if "fqkline" in line)
    body = json.loads(RawObjectStore.read_response(TENCENT_ARCHIVE, original))
    bars = body["data"]["sh600519"]["qfqday"]
    if mutation == "duplicate":
        bars.append(bars[0])
    elif mutation == "outside_window":
        bars[0][0] = "2026-08-31"
    elif mutation == "empty_adjusted":
        body["data"]["sh600519"]["day"] = bars
        body["data"]["sh600519"]["qfqday"] = []
    else:
        bars[0][5] = True
    response = requests.Response()
    response.status_code, response._content, response.encoding = 200, json.dumps(body).encode(), "utf-8"
    archive = RawObjectStore(tmp_path / "source")
    event = archive.record_response(response=response, url=original["url"], method="GET", request_headers={}, scope={}, provider="tencent", endpoint="kline_daily", code_version="fixture")
    input_id, context, _, _ = CASES[0]
    report = collect_input(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "candidate", replay_manifest=archive.root / "manifest.ndjson")
    assert report["status"] == "failed"
    captured = report["responses"][0]
    assert captured["body_sha256"] == event["body_sha256"]
    assert RawObjectStore.read_response(Path(report["run_directory"]) / report["raw_manifest"]["path"], captured) == response.content
    assert "output" not in report


def test_live_path_reuses_matching_fresh_response_before_repeating_request(tmp_path, monkeypatch):
    original = next(json.loads(line) for line in TENCENT_ARCHIVE.read_text(encoding="utf-8").splitlines() if "fqkline" in line)
    body = RawObjectStore.read_response(TENCENT_ARCHIVE, original)
    calls = []
    def simulated_send(session, request, **kwargs):
        calls.append((request, kwargs))
        response = requests.Response()
        response.status_code, response._content, response.encoding = 200, body, "utf-8"
        response.url, response.request = request.url, request
        return response
    monkeypatch.setattr(requests.Session, "send", simulated_send)
    input_id, context, _, _ = CASES[0]
    kwargs = dict(input_id=input_id, context=context, config_root=ROOT / "config", output_root=tmp_path / "out", evidence_root=tmp_path, mode="live")
    first = collect_input(**kwargs)
    second = collect_input(**kwargs)
    assert first["status"] == second["status"] == "candidate_complete"
    assert len(calls) == 1 and first["live_http_calls"] == 1 and second["live_http_calls"] == 0
    assert calls[0][1]["timeout"] == (8, 20)
    assert second["responses"][0]["mode"] == "cached"
    assert second["responses"][0]["source_ref"]["sha256"] == first["responses"][0]["body_sha256"]


def test_sdk_session_retry_policy_and_restoration(tmp_path):
    original_init = requests.Session.__init__
    with captured_requests(RawObjectStore(tmp_path), provider="fixture", endpoint="sdk", scope={}, code_version="fixture", pacer=RequestPacer(), sdk_retry_policy=True):
        with requests.Session() as session:
            policy = session.adapters["https://"].max_retries
            assert policy.total == 2 and policy.respect_retry_after_header
            assert not policy.is_retry("GET", 403, True) and not policy.is_retry("GET", 429, True)
            assert policy.is_retry("GET", 503, False)
            assert policy.increment(method="GET", error=ConnectionError()).get_backoff_time() >= 5
    assert requests.Session.__init__ is original_init
