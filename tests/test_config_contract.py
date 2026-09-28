from pathlib import Path

import yaml

from stock_data_manage.publication_policy import load_publication_policy


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_yaml_configuration_is_parseable_and_keeps_minute_scope_bounded() -> None:
    datasets = yaml.safe_load((PROJECT_ROOT / "config" / "datasets.yaml").read_text("utf-8"))
    collection = yaml.safe_load((PROJECT_ROOT / "config" / "collection.yaml").read_text("utf-8"))
    providers = yaml.safe_load((PROJECT_ROOT / "config" / "providers.yaml").read_text("utf-8"))
    yaml.safe_load((PROJECT_ROOT / "config" / "normalization.yaml").read_text("utf-8"))

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
