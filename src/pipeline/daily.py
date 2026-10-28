"""Daily result contracts, source-unit expansion and aggregation; no network or task loop."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import yaml

from ..domain import Adjustment, AssetType, DailyRecord, QuoteGroup, QuoteStatus, ItemStatus
from ..quality.publication import PublicationPolicy
from ..storage.integrity import file_hash, row_hash
from ..storage.parquet import CanonicalPartitionStore, DAILY_SCHEMA_VERSION
from ..config.loader import load_input_capabilities


class PublicationThresholdExceeded(ValueError):
    pass


def resolve_daily_definition(definition, config_root):
    definition = deepcopy(definition)
    tasks = yaml.safe_load((config_root / "collection.yaml").read_text(encoding="utf-8")).get("tasks", {})
    if "task" in definition:
        target = tasks[definition["task"]]
        if target["dataset"] != "daily_bar":
            raise ValueError("configured task is not a daily result task")
        if any(k in definition and definition[k] != v for k, v in target.items()):
            raise ValueError("task file conflicts with configured task target")
        definition = {**target, **definition}
    if "adjustment" in definition:
        raise ValueError("daily tasks now produce aggregate records; single-adjustment task definitions require a new explicit task")
    asset = {"all_stock": "stock", "all_etf": "etf", "all_index": "index"}.get(definition.get("universe"), definition.get("asset_type", "stock"))
    if definition.get("asset_type", asset) != asset:
        raise ValueError("daily asset_type conflicts with universe")
    AssetType(asset)
    document = yaml.safe_load((config_root / "datasets/daily_bar.yaml").read_text(encoding="utf-8"))
    contract = deepcopy(document["applicability"][asset])
    for adjustment in Adjustment:
        spec = contract[adjustment.value]
        if spec not in {"required", "unverified"}:
            raise ValueError("non-applicable scope requires independently verified applicability implementation; not inferred here")
    if asset == "stock" and any(v != "required" for v in contract.values()):
        raise ValueError("stock daily result requires all three source adjustments")
    policy_name = definition.get("acquisition_policy", "daily_fixed")
    policies = yaml.safe_load((config_root / "capabilities.yaml").read_text(encoding="utf-8"))["acquisition_policies"]
    policy = deepcopy(policies[policy_name])
    if policy["mode"] not in {"fixed", "failover"} or len(policy["inputs"]) != 1:
        raise ValueError("first offline daily integration permits one explicitly configured formal input")
    source = policy["inputs"][0]
    if source["input_id"] != "ASTOCK-002-daily":
        raise ValueError("daily formal input is not implemented")
    if definition.get("input_id", source["input_id"]) != source["input_id"]:
        raise ValueError("forced source is outside the configured acquisition policy")
    return {**definition, "asset_type": asset, "input_id": source["input_id"],
            "acquisition_policy": policy_name, "source_policy": policy,
            "quote_contract": contract, "schema_version": DAILY_SCHEMA_VERSION,
            "contract_versions": {name: file_hash(config_root / name) for name in
                ("datasets/daily_bar.yaml", "normalization/daily_bar.yaml")},
            "source_contract_version": row_hash(asdict(next(c for c in load_input_capabilities(config_root / "providers.yaml")
                if c.input_id == source["input_id"]))),
            "publication_policy": deepcopy(document["dataset"]["publication"])}


def expand_daily_units(units, definition):
    requests = definition["source_policy"]["inputs"][0]["adjustment_requests"]
    expanded = []
    target_limit = definition["source_policy"].get("max_symbols")
    if target_limit is not None and len({u.get("symbol") for u in units}) > int(target_limit):
        raise ValueError(f"daily task exceeds configured symbol limit {target_limit}")
    for unit in units:
        if "adjustment" in unit:
            expanded.append(unit)
            continue
        if set(unit.get("context", {}).get("config", {})) & {"api", "adjust"}:
            raise ValueError("daily adjustment request parameters belong to the acquisition policy")
        if unit.get("replay_manifest"):
            raise ValueError("aggregate replay requires explicit per-adjustment replay_manifests")
        for name, state in definition["quote_contract"].items():
            if state != "required":
                continue
            item = deepcopy(unit)
            item.update(key=unit["symbol"] + "__" + name, adjustment=name, asset_type=definition["asset_type"])
            item["context"].setdefault("config", {}).update(requests[name])
            manifests = item.pop("replay_manifests", {})
            if name in manifests:
                item["replay_manifest"] = manifests[name]
            expanded.append(item)
    for unit in expanded:
        if (unit["input_id"] != definition["input_id"] or definition["quote_contract"].get(unit["adjustment"]) != "required"
                or unit["context"].get("config", {}) != requests[unit["adjustment"]]):
            raise ValueError("daily source unit conflicts with frozen acquisition policy")
    return expanded


def _source_capture_time(raw_manifest):
    """Read acquisition time from persisted response evidence, not the parser report."""
    import json
    stamps = []
    for line in Path(raw_manifest).read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        if event.get("outcome") != "response":
            continue
        stamp = event.get("fetched_at_utc")
        if event.get("mode") in {"replay", "cached"}:
            stamp = (event.get("source_ref") or {}).get("fetched_at_utc") or stamp
        if stamp:
            stamps.append(datetime.fromisoformat(stamp))
    return max(stamps) if stamps else None


def prepare_daily_task_publication(state, units, source_rows, paths, config_root, trade_date):
    definition = state["definition"]
    asset = AssetType(definition["asset_type"])
    expected = {u["symbol"]: {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[u["symbol"][:2]] + ":" + u["symbol"][2:] for u in units}
    selected = set(state.get("selected_symbols", ()))
    target_ids = {identity for symbol, identity in expected.items() if not selected or symbol in selected}
    groups, issues = {}, []
    for unit in units:
        symbol, adjustment = unit["symbol"], unit["adjustment"]
        if selected and symbol not in selected:
            continue
        source = source_rows.get(unit["key"])
        if source is None:
            entry = state["units"].get(unit["key"], {})
            report = entry.get("report", {})
            issues.append({"stage": entry.get("report", {}).get("failure_stage", "processing"), "symbol": symbol,
                "trade_date": trade_date.isoformat(), "adjustment": adjustment, "input_id": unit["input_id"],
                "code": report.get("failure_class") or entry.get("failure_class") or "missing",
                "reason": report.get("error") or entry.get("error") or "required source quote is missing",
                "raw_ref": entry.get("raw_manifest"), "provider": report.get("provider"),
                "endpoint": report.get("endpoint"), "record_locator": report.get("record_locator"),
                "field": report.get("error_field"), "impact": "incomplete_daily_record"})
            continue
        report = source["entry"]["report"]
        rows = source["rows"]
        if len(rows) != 1:
            raise ValueError("one daily source unit must provide exactly one target-date row")
        row = rows[0]
        if row["instrument_id"] != expected[symbol] or row["trade_date"] != trade_date or row["adjustment"] != adjustment:
            raise ValueError("daily candidate identity, date or adjustment differs from its unit")
        key = (symbol, adjustment)
        if key in groups:
            raise ValueError("duplicate daily source quote; no implicit source arbitration")
        groups[key] = QuoteGroup(QuoteStatus.AVAILABLE, **{k: row.get(k) for k in ("open", "high", "low", "close", "volume", "amount")},
            source_provider=source["contract"].provider, endpoint=source["contract"].endpoint,
            raw_object_path=source["raw_ref"], fetch_time=_source_capture_time(source["raw_ref"]),
            normalizer_version=report["normalization_version"], capability_version=report["adapter_version"],
            flags=tuple(dict.fromkeys(flag for scope in report.get("verified_unit_scopes", ())
                if row["instrument_id"] in scope["instrument_ids"] and str(row["trade_date"]) in scope["dates"]
                for flag in scope.get("precision_flags", ()))))
    records = []
    for symbol, identity in expected.items():
        if selected and symbol not in selected:
            continue
        quotes = {}
        for name, requirement in definition["quote_contract"].items():
            quotes[name] = groups.get((symbol, name)) or QuoteGroup(
                QuoteStatus.MISSING if requirement == "required" else QuoteStatus.UNVERIFIED,
                reason="required source quote missing" if requirement == "required" else "applicability has not been verified")
            if requirement == "unverified":
                issues.append({"stage": "quality", "symbol": symbol, "adjustment": name, "code": "unverified_applicability",
                               "reason": "asset/source applicability is unverified", "impact": "incomplete_daily_record"})
        record = DailyRecord(identity, trade_date, asset, **quotes)
        if record.complete:
            records.append(record)
    store = CanonicalPartitionStore(paths["canonical_root"])
    existing = store.read_daily(asset.value, trade_date.isoformat())
    available = {r.instrument_id for r in records}
    if state["redo"] != "full":
        available.update(r.instrument_id for r in existing if r.instrument_id not in target_ids)
    statuses = {identity: ItemStatus.SUCCESS.value if identity in available else ItemStatus.MISSING.value for identity in expected.values()}
    state["daily_check"] = {"issues": issues, "item_statuses": statuses,
        "expected_count": len(expected), "actual_count": len(set(expected.values()) & available)}
    settings = definition["publication_policy"]
    policy = PublicationPolicy(Decimal(str(settings["max_missing_ratio"])), settings["max_missing_count"])
    if not policy.allows(expected_count=len(expected), actual_count=state["daily_check"]["actual_count"]):
        raise PublicationThresholdExceeded("daily aggregate coverage rejected; see per-adjustment issues")
    return {"records": records, "partition_directory": str(store.daily_directory(asset.value, trade_date.isoformat())),
        "asset_type": asset.value, "partition_key": trade_date.isoformat(), "expected_count": len(expected),
        "item_statuses": statuses, "replace_ids": sorted(target_ids), "adjustment": "aggregate",
        "publication_metadata": {"schema_version": DAILY_SCHEMA_VERSION, "issues": issues}}
