from pathlib import Path
from datetime import date, datetime, timezone
import csv
import json

import yaml
import pytest

from stock_data_manage.quality.publication import load_publication_policy
from stock_data_manage.config.loader import (
    ParameterBinding, load_input_capabilities, load_collection_profiles, load_provider_configs,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_yaml_configuration_is_parseable_and_keeps_minute_scope_bounded() -> None:
    datasets = yaml.safe_load((PROJECT_ROOT / "config" / "datasets.yaml").read_text("utf-8"))
    collection = yaml.safe_load((PROJECT_ROOT / "config" / "collection.yaml").read_text("utf-8"))
    providers = yaml.safe_load((PROJECT_ROOT / "config" / "providers.yaml").read_text("utf-8"))
    yaml.safe_load((PROJECT_ROOT / "config" / "normalization" / "daily_bar.yaml").read_text("utf-8"))

    allowed = set(datasets["datasets"]["minute_bar_1m"]["allowed_universes"])
    daily_policy = load_publication_policy(
        PROJECT_ROOT / "config" / "datasets.yaml", "daily_bar"
    )
    assert allowed == {"watchlist", "strategy_candidate", "static"}
    assert collection["realtime_minute"]["reject_full_market_universe"] is True
    assert collection["realtime_minute"]["max_watchlist_symbols"] == 200
    assert collection["storage"]["metadata_path"].endswith(".duckdb")
    assert providers["providers"]["eastmoney"]["enabled"] is False
    assert providers["providers"]["tdx"]["enabled"] is False
    assert providers["providers"]["baostock"]["default_role"] == "validation_only"
    assert providers["providers"]["capability_probe"]["cooldown_probe_required"] is True
    assert str(daily_policy.max_missing_ratio) == "0.01"
    assert daily_policy.max_missing_count == 50


def test_input_catalog_covers_successful_rows_without_granting_production_routes():
    inputs = load_input_capabilities(PROJECT_ROOT / "config/providers.yaml")
    profiles = {p.name: p for p in load_collection_profiles(PROJECT_ROOT / "config/collection.yaml")}
    with (PROJECT_ROOT / "provider_validation/coverage/interface-coverage.csv").open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    successes = {r["接口ID"] for r in rows if r["接口取数结果"] in {"通过", "部分通过"}}
    # Preserve the original 87-interface investigation as a historical inventory.
    original_inputs = [c for c in inputs if c.input_id != "SECURITY-BSE-001"]
    assert {ref for c in original_inputs for ref in c.evidence_refs} == successes
    assert len(original_inputs) == 77 and len(inputs) == 78
    assert len(successes) == 73
    assert all(c.collection_profile in profiles for c in inputs)
    assert all(not p.scheduling_enabled for p in profiles.values())
    by_id = {c.input_id: c for c in inputs}
    assert by_id["ASTOCK-049"].canonical_input == "ASTOCK-045"
    assert by_id["ASTOCK-050"].endpoint == "strong_stock_pool"
    assert by_id["ASTOCK-061"].provider == "mofcom"
    assert by_id["ASTOCK-070"].provider == "sina"
    assert by_id["ASTOCK-086"].implementation_status == "blocked"
    assert by_id["ASTOCK-052"].data_kind == "derived"
    assert by_id["ASTOCK-002-daily"].evidence_refs == by_id["ASTOCK-002-5m"].evidence_refs == ("ASTOCK-002",)
    catalog = json.loads((PROJECT_ROOT / "provider_validation/coverage/successful-input-capabilities.json").read_text(encoding="utf-8"))
    assert {row["contract"]["input_id"] for row in catalog["catalog"]} == {c.input_id for c in original_inputs}
    bse = by_id["SECURITY-BSE-001"]
    assert bse.implementation_status == "implemented_validation_only"
    assert bse.evidence_refs == ("provider_validation/results/raw/security-catalog-bse-20261007/manifest.ndjson",)
    assert all((PROJECT_ROOT / ref).is_file() for ref in bse.evidence_refs)
    assert all(not row["eligible_for_production_routing"] for row in catalog["catalog"])


def test_parameter_binding_uses_explicit_contexts_and_preserves_types():
    cases = [
        (ParameterBinding("date", "calendar.latest_completed_trade_date", "date", True), {"calendar": {"latest_completed_trade_date": "2026-09-30"}}, date(2026, 9, 30)),
        (ParameterBinding("symbols", "request.symbols", "strings", True), {"request": {"symbols": ["sh600519"]}}, ("sh600519",)),
        (ParameterBinding("cursor", "metadata.cursor", "string"), {"metadata": {"cursor": "next-page"}}, "next-page"),
        (ParameterBinding("board", "dependency.board_code", "string", True), {"dependency": {"board_code": "881121"}}, "881121"),
        (ParameterBinding("period", "config.period", "string", default="即时"), {}, "即时"),
        (ParameterBinding("auth", "credential_ref.api_key", "string"), {"credential_ref": {"api_key": "env:PROVIDER_KEY"}}, "env:PROVIDER_KEY"),
    ]
    for binding, context, expected in cases:
        assert binding.resolve(context) == expected
    stamp = datetime(2026, 9, 30, 8, tzinfo=timezone.utc)
    assert ParameterBinding("as_of", "request.as_of", "datetime", True).resolve({"request": {"as_of": stamp.isoformat()}}) == stamp
    with pytest.raises(ValueError, match="source"):
        ParameterBinding("x", "eval.run()", "string")
    with pytest.raises(ValueError, match="missing required"):
        ParameterBinding("code", "request.symbol", "string", True).resolve({})
    with pytest.raises(ValueError, match="datetime"):
        ParameterBinding("as_of", "request.as_of", "datetime", True).resolve({"request": {"as_of": "2026-09-30T08:00:00"}})


def test_recent_minute_contract_rejects_history_and_invalid_count():
    contract = next(c for c in load_input_capabilities(PROJECT_ROOT / "config/providers.yaml") if c.input_id == "ASTOCK-002-5m")
    assert contract.bind_parameters({"request": {"symbol": "sz300750"}}) == {"code": "sz300750", "count": 96}
    with pytest.raises(ValueError, match="does not support"):
        contract.bind_parameters({"request": {"symbol": "sz300750", "start_date": "2026-09-01"}})
    for count in (0, 321, True, 1.5):
        with pytest.raises(ValueError):
            contract.bind_parameters({"request": {"symbol": "sz300750"}, "config": {"count": count}})


def test_snapshot_requires_known_trading_date_and_separates_local_filter():
    contract = next(c for c in load_input_capabilities(PROJECT_ROOT / "config/providers.yaml") if c.input_id == "SDA-BOARD-005")
    context = {"request": {"trade_date": "2026-09-30"}, "calendar": {"trading_dates": [date(2026, 9, 30)]}}
    assert contract.bind_parameters(context) == {"trade_date": date(2026, 9, 30), "include_etf": False}
    with pytest.raises(ValueError, match="calendar coverage"):
        contract.bind_parameters({"request": {"trade_date": "2026-09-30"}})
    with pytest.raises(ValueError, match="known trading day"):
        contract.bind_parameters({**context, "request": {"trade_date": "2026-10-02"}})
    config = next(c for c in load_provider_configs(PROJECT_ROOT / "config/providers.yaml") if c.provider == "baostock" and c.endpoint == "industry_membership")
    assert next(p for p in config.parameters if p.name == "symbols").applied_at == "local_filter"
    assert config.request_shape == "full_snapshot"
    assert config.base_requests_per_fetch == 2


def test_window_validation_and_unsupported_scheduling_are_explicit(tmp_path):
    contract = next(c for c in load_input_capabilities(PROJECT_ROOT / "config/providers.yaml") if c.input_id == "ASTOCK-002-daily")
    with pytest.raises(ValueError, match="end must not precede"):
        contract.bind_parameters({"request": {"symbol": "sh600519", "start_date": "2026-09-30", "end_date": "2026-09-01"}})
    profile_path = tmp_path / "collection.yaml"
    profile_path.write_text("collection_profiles:\n  news:\n    mode: intraday\n    business_day: calendar_day\n    scheduling_enabled: true\n", encoding="utf-8")
    with pytest.raises(ValueError, match="not implemented"):
        load_collection_profiles(profile_path)


def test_disabled_provider_cannot_be_enabled_by_an_endpoint_override(tmp_path):
    config_path = tmp_path / "providers.yaml"
    config_path.write_text("providers:\n  example:\n    enabled: false\n    endpoints:\n      sample:\n        enabled: true\n", encoding="utf-8")
    assert not load_provider_configs(config_path)[0].enabled


@pytest.mark.parametrize("key,value", [("raw_root", "../outside"),
    ("workspace_root", "data/raw/nested"), ("archive_root", "data/task_workspace"),
    ("metadata_path", "data/metadata.duckdb")])
def test_storage_paths_reject_escape_or_overlapping_layers(tmp_path, key, value):
    from stock_data_manage.config.loader import load_storage_paths
    config = tmp_path / "config"
    config.mkdir()
    document = yaml.safe_load((PROJECT_ROOT / "config/collection.yaml").read_text(encoding="utf-8"))
    document["storage"][key] = value
    (config / "collection.yaml").write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError):
        load_storage_paths(config)


def test_storage_paths_override_whole_data_root_without_creating_directories(tmp_path):
    from stock_data_manage.config.loader import load_storage_paths
    paths = load_storage_paths(PROJECT_ROOT / "config", data_root=tmp_path / "isolated")
    for name, relative in {"raw_root":"raw", "workspace_root":"task_workspace",
                          "archive_root":"task_archive", "canonical_root":"canonical",
                          "metadata_path":"metadata/metadata.duckdb", "hot_path":"hot/minute_hot.db"}.items():
        assert paths[name] == tmp_path / "isolated" / relative
    assert not paths["data_root"].exists()
