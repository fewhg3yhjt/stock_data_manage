"""Daily batch contracts and the independent capture, interpretation and build operations."""
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


def save_batch_json(path, value):
    """Atomic durable artifact replacement; persistent names describe their contents."""
    import json
    import os
    from .inputs import _json_value
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as stream:
        stream.write(json.dumps(_json_value(value), ensure_ascii=False, sort_keys=True).encode("utf-8"))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def daily_unit_paths(work, unit):
    base = Path(work) / "sources" / "tencent" / "kline_daily" / unit["key"]
    return {name: base.with_name(base.name + suffix) for name, suffix in
            {"manifest": ".manifest.json", "report": ".report.json", "candidate": ".normalized.parquet",
             "source": ".source_rows.json"}.items()}


def capture_daily_unit(*, unit, contract, provider, paths, task_id, trade_date, pacer, replay_manifest=None,
                       cache_lookup=None, max_age_seconds=0, attempt=1):
    """Save exact responses only. This operation never interprets prices or source lists."""
    from datetime import timezone
    from ..providers.transport import captured_requests
    from ..storage.raw import RawObjectStore
    parameters = contract.bind_parameters(unit["context"])
    directory = RawObjectStore.task_unit_path(paths["raw_root"], provider=contract.provider,
        endpoint=contract.endpoint, data_date=trade_date)
    temporary = directory / RawObjectStore.request_manifest_name(parameters, input_id=contract.input_id)
    audit = paths["archive_root"] / "_raw_evidence" / task_id / unit["key"] / f"attempt-{attempt}" / "manifest.ndjson"
    import json
    request_key = temporary.name.removeprefix('manifest.').removesuffix('.ndjson')
    owner_path = directory / ('_managed_task.' + request_key + '.json')
    owner = {'task_id': task_id, 'unit_key': unit['key'], 'attempt': attempt, 'data_date': trade_date}
    if temporary.exists() and (not owner_path.exists() or json.loads(owner_path.read_text(encoding='utf-8')) != owner):
        raise ValueError('temporary daily response belongs to another task/attempt; use a new isolated root')
    # A response can have reached disk just before the process stopped, before its
    # unit index was written. Recover it without repeating the request.
    if audit.exists():
        RawObjectStore.verify_manifest(audit)
    else:
        if not temporary.exists():
            save_batch_json(owner_path, owner)
            store = RawObjectStore(directory, manifest_name=temporary.name)
            with captured_requests(store, provider=contract.provider, endpoint=contract.endpoint,
                    scope={"input_id": contract.input_id, "parameters": parameters},
                    code_version=provider.capability_version, pacer=pacer,
                    replay_manifest=replay_manifest, evidence_roots=(), interval_seconds=contract.request_interval_seconds,
                    cache_lookup=cache_lookup, max_age_seconds=max_age_seconds):
                provider.capture_window(**parameters, asset_type=unit["asset_type"])
        RawObjectStore.copy_request(temporary, audit)
    events = [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()]
    responses = [e for e in events if e.get("outcome") == "response"]
    if not responses or responses[-1].get('status_code', 500) >= 400:
        from ..providers.contracts import ProviderContractError, FailureClass
        raise ProviderContractError('daily capture did not finish with a successful transport response',
            FailureClass.HTTP_ERROR, retryable=False)
    return {"task_id": task_id, "unit_key": unit["key"], "attempt": attempt,
        "attempt_id": task_id + ":" + unit["key"] + f":attempt-{attempt}",
        "key": unit["key"], "symbol": unit["symbol"], "asset_type": unit["asset_type"],
        "input_id": contract.input_id, "provider": contract.provider, "endpoint": contract.endpoint,
        "capture_status": "captured", "process_status": "pending", "status": "captured",
        "raw_manifest": str(audit), "raw_hash": RawObjectStore.verify_manifest(audit),
        "temporary_raw": str(temporary), "source_response_hashes": [responses[-1]["body_sha256"]],
        "live_http_calls": sum(e.get('event') == 'http_response' and e.get("mode") == "live" for e in events),
        "captured_at_utc": datetime.now(timezone.utc).isoformat()}


def process_daily_unit(*, unit, entry, provider, contract, field_contract, rule, work):
    """Interpret retained bytes, normalize and check one candidate; no transport is used."""
    import json
    from datetime import timezone
    from urllib.parse import urlsplit
    from ..providers.contracts import HttpResponse
    from ..quality.normalization import Normalizer, NormalizationError
    from ..storage.raw import RawObjectStore
    from ..storage.parquet import write_normalized_rows
    parameters = contract.bind_parameters(unit["context"])
    manifest = Path(entry["raw_manifest"])
    if RawObjectStore.verify_manifest(manifest) != entry["raw_hash"]:
        raise ValueError("daily response evidence changed before processing")
    events = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
    responses = [e for e in events if e.get("outcome") == "response"]
    if not responses:
        raise ValueError('daily processing has no completed transport response')
    # The last retained response is the one returned by the verified transport
    # after native redirects/allowed host fallback. Earlier failures stay raw.
    event = responses[-1]
    url = urlsplit(event["url"])
    captured = {"symbol": unit["symbol"], "start": parameters["start"], "end": parameters["end"],
        "adjust": parameters["adjust"], "api": parameters["api"], "path": url.path,
        "segments": ((parameters["start"], parameters["end"], HttpResponse(event["status_code"],
            event.get("response_headers", {}), RawObjectStore.read_response(manifest, event)), url.netloc),)}
    fetched = provider.parse_window(captured)
    exchange = _daily_market(unit["symbol"])
    context = {"provider": contract.provider, "endpoint": contract.endpoint, **fetched.mapping_context,
        "instrument_id": exchange + ":" + unit["symbol"][2:], "exchange": exchange,
        "asset_type": unit["asset_type"], "adjustment": unit["adjustment"]}
    if fetched.mapping_context.get("adjustment") != unit["adjustment"]:
        raise NormalizationError("requested adjustment differs from returned source series")
    rows = [Normalizer.normalize_fields(row, rule=rule, fields=field_contract["fields"],
        context=context, allow_pending=True) for row in fetched.rows]
    if len(rows) != 1 or rows[0]["trade_date"] != parameters["start"]:
        raise NormalizationError("one daily unit must yield exactly its frozen target date")
    row = rows[0]
    if not (row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"]):
        raise NormalizationError("invalid OHLC ordering")
    unit_contract = Normalizer.match_unit_contract(rule, context)
    artifact_paths = daily_unit_paths(work, unit)
    if artifact_paths["candidate"].exists():
        import pyarrow.parquet as pq
        if pq.read_table(artifact_paths["candidate"]).to_pylist() != rows:
            raise ValueError("retained daily candidate differs from reprocessed source bytes")
        descriptor = {"sha256": file_hash(artifact_paths["candidate"])}
    else:
        descriptor = write_normalized_rows(artifact_paths["candidate"], rows, field_contract["fields"])
    save_batch_json(artifact_paths["source"], fetched.source_rows)
    report = {"dataset": "daily_bar", "data_date": parameters['start'].isoformat(), "asset_type": unit['asset_type'],
        "input_id": contract.input_id, "provider": contract.provider, "endpoint": contract.endpoint,
        "status": "candidate_complete", "row_count": len(rows), "parameters": parameters,
        "adapter_version": provider.capability_version, "normalization_version": rule["version"],
        "source_response_hashes": entry["source_response_hashes"], "raw_manifest": str(manifest),
        "source_rows": {"path": str(artifact_paths['source'].relative_to(work)),
            "sha256": file_hash(artifact_paths['source']), "row_count": len(fetched.source_rows)},
        "applied_unit_contract": unit_contract,
        "unit_precision_flags": [flag for name, flag in (unit_contract["precision_flags"].items()
            if unit_contract else ()) if row.get(name) is not None],
        "unverified_fields": [name for name in rule.get("unverified_fields", ()) if row.get(name) is None],
        "original_row_count": len(fetched.source_rows), "selected_row_count": len(rows),
        "coverage_denominator": 1, "validated_at_utc": datetime.now(timezone.utc), "processing_http_calls": 0}
    save_batch_json(artifact_paths["report"], report)
    return {**entry, "process_status": "candidate_complete", "status": "candidate_complete",
        "artifact": str(artifact_paths["candidate"]), "artifact_hash": descriptor["sha256"],
        "report_path": str(artifact_paths["report"]), "report_hash": file_hash(artifact_paths["report"]), "row_count": 1}


class PublicationThresholdExceeded(ValueError):
    pass


_DAILY_MARKET_BY_PREFIX = {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}
_DAILY_ADJUSTMENT_TOKEN = {"none": "none", "forward": "qfq", "backward": "hfq"}


def _daily_market(symbol):
    try:
        return _DAILY_MARKET_BY_PREFIX[str(symbol)[:2].lower()]
    except KeyError as exc:
        raise ValueError(f"daily source policy cannot identify the market for {symbol!r}") from exc


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
    if "asset_types" in definition:
        assets = definition["asset_types"]
        if (not isinstance(assets, list) or not assets or len(set(assets)) != len(assets)
                or set(assets) - {"stock", "etf", "index"} or "asset_type" in definition
                or definition.get("universe") not in {"static", "all"}):
            raise ValueError("daily batch requires explicit unique asset_types and universe static/all")
        resolved = {asset: resolve_daily_definition({**{k: v for k, v in definition.items()
                    if k not in {"asset_types", "task"}}, "asset_type": asset, "universe": "static"}, config_root)
                    for asset in assets}
        first = next(iter(resolved.values()))
        return {**{k: v for k, v in first.items() if k not in {"asset_type", "quote_contract"}},
                **definition, "quote_contracts": {a: r["quote_contract"] for a, r in resolved.items()}}
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
    market_requests = source.get("market_adjustment_requests")
    if not isinstance(market_requests, dict) or set(market_requests) != set(_DAILY_MARKET_BY_PREFIX.values()):
        raise ValueError("daily source policy must explicitly cover XSHG, XSHE and BSE")
    for market, requests in market_requests.items():
        if not isinstance(requests, dict) or set(requests) != set(_DAILY_ADJUSTMENT_TOKEN):
            raise ValueError(f"daily source policy for {market} must declare all three adjustment requests")
        for adjustment, request in requests.items():
            if (not isinstance(request, dict) or set(request) != {"api", "adjust"}
                    or request["adjust"] != _DAILY_ADJUSTMENT_TOKEN[adjustment]):
                raise ValueError(f"daily source policy has an invalid {market}/{adjustment} request")
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
    if "asset_types" in definition:
        expanded = []
        for asset in definition["asset_types"]:
            group = [u for u in units if u.get("asset_type") == asset]
            if not group:
                raise ValueError(f"daily batch has no securities for {asset}")
            typed = {k: v for k, v in definition.items() if k != "asset_types"}
            typed.update(asset_type=asset, quote_contract=definition["quote_contracts"][asset])
            expanded.extend(expand_daily_units(group, typed))
        if len({u["key"] for u in expanded}) != len(expanded):
            raise ValueError("daily batch source units must be unique")
        return expanded
    requests_by_market = definition["source_policy"]["inputs"][0]["market_adjustment_requests"]
    expanded = []
    target_limit = definition["source_policy"].get("max_symbols")
    if target_limit is not None and len({u.get("symbol") for u in units}) > int(target_limit):
        raise ValueError(f"daily task exceeds configured symbol limit {target_limit}")
    for unit in units:
        market = _daily_market(unit.get("symbol", ""))
        requests = requests_by_market.get(market)
        if requests is None:
            raise ValueError(f"daily source policy has no route for market {market}")
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
        market_requests = requests_by_market.get(_daily_market(unit.get("symbol", "")))
        expected_request = market_requests.get(unit["adjustment"]) if market_requests else None
        if (unit["input_id"] != definition["input_id"] or definition["quote_contract"].get(unit["adjustment"]) != "required"
                or expected_request is None or unit["context"].get("config", {}) != expected_request):
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
            flags=tuple(report.get("unit_precision_flags", ())) + tuple(
                "unit_unverified:" + field for field in report.get("unverified_fields", ())
                if field in {"volume", "amount"}))
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
                               "reason": "asset/source applicability is unverified", "impact": "unverified_quote_group"})
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
