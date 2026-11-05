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
    base = Path(work) / "sources" / unit.get("provider", "tencent") / unit.get("endpoint", "kline_daily") / unit["key"]
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
        "request_symbol": parameters.get("code", unit["symbol"]),
        "input_id": contract.input_id, "provider": contract.provider, "endpoint": contract.endpoint,
        "capture_status": "captured", "process_status": "pending", "status": "captured",
        "raw_manifest": str(audit), "raw_hash": RawObjectStore.verify_manifest(audit),
        "temporary_raw": str(temporary), "source_response_hashes": [responses[-1]["body_sha256"]],
        "live_http_calls": sum(e.get('event') == 'http_response' and e.get("mode") == "live" for e in events),
        "captured_at_utc": datetime.now(timezone.utc).isoformat()}


def process_daily_unit(*, unit, entry, provider, contract, field_contract, rule, work, config_root):
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
    captured = {"symbol": parameters.get("code", unit["symbol"]), "start": parameters["start"], "end": parameters["end"],
        **{name: parameters[name] for name in ("adjust", "api", "frequency") if name in parameters}, "path": url.path,
        "segments": ((parameters["start"], parameters["end"], HttpResponse(event["status_code"],
            event.get("response_headers", {}), RawObjectStore.read_response(manifest, event)), url.netloc),)}
    fetched = provider.parse_window(captured)
    exchange = _daily_market(unit["symbol"])
    value_contract = unit.get("value_contract", "full_ohlc")
    context = {"provider": contract.provider, "endpoint": contract.endpoint, **fetched.mapping_context,
        "instrument_id": unit.get("instrument_id") or exchange + ":" + unit["symbol"][2:], "exchange": exchange,
        "asset_type": unit["asset_type"], "adjustment": unit["adjustment"], "value_contract": value_contract}
    if fetched.mapping_context.get("adjustment") != unit["adjustment"]:
        raise NormalizationError("requested adjustment differs from returned source series")
    if fetched.mapping_context.get("optional_series_status") == "unavailable":
        if unit["adjustment"] == "none":
            raise NormalizationError("the required unadjusted daily series cannot be unavailable")
        reason = fetched.mapping_context["optional_series_reason"]
        artifact_paths = daily_unit_paths(work, unit)
        report = {"dataset": "daily_bar", "data_date": parameters["start"].isoformat(),
            "asset_type": unit["asset_type"], "input_id": contract.input_id,
            "provider": contract.provider, "endpoint": contract.endpoint,
            "status": "unverified", "row_count": 0, "parameters": parameters,
            "value_contract": value_contract, "adapter_version": provider.capability_version,
            "normalization_version": rule["version"], "source_response_hashes": entry["source_response_hashes"],
            "raw_manifest": str(manifest), "reason": reason,
            "coverage_denominator": 0, "validated_at_utc": datetime.now(timezone.utc),
            "processing_http_calls": 0}
        save_batch_json(artifact_paths["report"], report)
        return {**entry, "process_status": "unverified", "status": "unverified", "error": reason,
            "report_path": str(artifact_paths["report"]), "report_hash": file_hash(artifact_paths["report"]),
            "row_count": 0}
    rows = [Normalizer.normalize_fields(row, rule=rule, fields=field_contract["fields"],
        context=context, allow_pending=True) for row in fetched.rows]
    if len(rows) != 1 or rows[0]["trade_date"] != parameters["start"]:
        raise NormalizationError("one daily unit must yield exactly its frozen target date")
    row = rows[0]
    if row.get("value_contract") != value_contract:
        raise NormalizationError("normalized value contract differs from the frozen source unit")
    if value_contract == "full_ohlc":
        if not all(isinstance(row.get(name), Decimal) and row[name] > 0 for name in ("open", "high", "low", "close")):
            raise NormalizationError("full OHLC unit is missing a positive price field")
        if not (row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"]):
            raise NormalizationError("invalid OHLC ordering")
    elif (value_contract != "close_only" or unit["asset_type"] != "index" or unit["adjustment"] != "none"
            or any(row.get(name) is not None for name in ("open", "high", "low"))
            or row.get("volume") is not None or row.get("amount") is not None
            or not isinstance(row.get("close"), Decimal) or row["close"] <= 0):
        raise NormalizationError("close-only data is permitted only for a positive index close with empty OHLC and volume/amount")
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
        "value_contract": value_contract,
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
    from ..storage.read_index import append_source_report
    from ..storage.parquet import PartitionLockedError
    try:
        append_source_report(artifact_paths["report"], run_root=work, config_root=config_root,
                             field_mapping=rule.get("field_mapping", {}))
    except (OSError, ValueError, TypeError, KeyError, IndexError, UnicodeError,
            PartitionLockedError) as exc:
        import warnings
        warnings.warn(f"source query index refresh failed for {artifact_paths['report'].name}: {exc}", RuntimeWarning)
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


def _normalize_symbol_parallel_codes(groups):
    """Expand the explicitly documented cross-market code pairs from security_master.yaml."""
    import re
    market_prefix = {"XSHG": "sh", "XSHE": "sz", "BSE": "bj"}
    result, seen = {}, set()
    for group in groups:
        if not isinstance(group, dict):
            raise ValueError("parallel-code group must be a mapping")
        logical_market = str(group.get("logical_market", "")).upper()
        request_market = str(group.get("request_market", "")).upper()
        asset_type = str(group.get("asset_type", "")).lower()
        if logical_market not in market_prefix or request_market not in market_prefix or asset_type != "index":
            raise ValueError("parallel-code group requires supported markets and index asset type")
        pairs, evidence = group.get("pairs"), group.get("evidence_refs")
        if not isinstance(pairs, dict) or not pairs or not isinstance(evidence, (list, tuple)) or not evidence:
            raise ValueError("parallel-code group requires explicit pairs and evidence references")
        if any(not isinstance(ref, str) or not ref.strip() for ref in evidence):
            raise ValueError("parallel-code evidence reference is empty")
        for logical_code, request_symbol in pairs.items():
            logical_code, request_symbol = str(logical_code), str(request_symbol).lower()
            key = (logical_market, asset_type, logical_code)
            if (not re.fullmatch(r"[0-9]{6}", logical_code)
                    or not re.fullmatch(market_prefix[request_market] + r"[0-9]{6}", request_symbol)
                    or key in seen):
                raise ValueError("parallel code must be unique and use the declared request market")
            seen.add(key)
            result[key] = {"logical_market": logical_market, "asset_type": asset_type,
                "logical_symbol": logical_code, "request_market": request_market,
                "request_symbol": request_symbol, "evidence_refs": list(evidence)}
    return result


def resolve_daily_definition(definition, config_root):
    definition = deepcopy(definition)
    # Suspension exclusions are derived only from a fresh, non-fallback master
    # publication; a task file cannot lower its own coverage denominator.
    definition.pop("suspended_securities", None)
    definition.pop("lifecycle_exclusions", None)
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
    if contract.get("none") != "required":
        raise ValueError("daily result requires the unadjusted quote")
    policy_name = definition.get("acquisition_policy", "daily_fixed")
    policies = yaml.safe_load((config_root / "capabilities.yaml").read_text(encoding="utf-8"))["acquisition_policies"]
    policy = deepcopy(policies[policy_name])
    from ..service.instruments import normalize_symbol_successions
    security_master = yaml.safe_load((config_root / "datasets/security_master.yaml").read_text(encoding="utf-8"))
    symbol_successions = normalize_symbol_successions(security_master.get("symbol_successions", []))
    symbol_parallel_codes = _normalize_symbol_parallel_codes(security_master.get("symbol_parallel_codes", []))
    if policy["mode"] not in {"fixed", "failover"} or not isinstance(policy.get("inputs"), list) or not policy["inputs"]:
        raise ValueError("daily source policy requires an explicit nonempty input list")
    contracts = {item.input_id: item for item in load_input_capabilities(config_root / "providers.yaml")}
    defaults = [item for item in policy["inputs"] if item.get("default") is True]
    if len(defaults) != 1 or defaults[0]["input_id"] != "ASTOCK-002-daily":
        raise ValueError("daily source policy requires exactly one Tencent default route")
    seen_symbols = set()
    for source in policy["inputs"]:
        input_id = source.get("input_id")
        capability = contracts.get(input_id)
        if capability is None or capability.dataset != "daily_bar" or capability.implementation_status != "implemented_validation_only":
            raise ValueError(f"daily route has no implemented validation-only input: {input_id}")
        if input_id == "ASTOCK-002-daily":
            market_requests = source.get("market_adjustment_requests")
            if not isinstance(market_requests, dict) or set(market_requests) != set(_DAILY_MARKET_BY_PREFIX.values()):
                raise ValueError("daily Tencent route must explicitly cover XSHG, XSHE and BSE")
            for market, requests in market_requests.items():
                if not isinstance(requests, dict) or set(requests) != set(_DAILY_ADJUSTMENT_TOKEN):
                    raise ValueError(f"daily source policy for {market} must declare all three adjustment requests")
                for adjustment, request in requests.items():
                    if (not isinstance(request, dict) or set(request) != {"api", "adjust"}
                            or request["adjust"] != _DAILY_ADJUSTMENT_TOKEN[adjustment]):
                        raise ValueError(f"daily source policy has an invalid {market}/{adjustment} request")
        elif input_id in {"CSI-INDEX-DAILY-001", "CSI-INDEX-DAILY-OHLC-001",
                          "CSI-INDEX-DAILY-CLOSE-PAIR-001", "CNI-INDEX-DAILY-001"}:
            symbols = source.get("symbols")
            expected_contract = ("close_only" if input_id in {
                "CSI-INDEX-DAILY-001", "CSI-INDEX-DAILY-CLOSE-PAIR-001"} else "full_ohlc")
            csi_input = input_id != "CNI-INDEX-DAILY-001"
            expected_market = "XSHG" if csi_input else "XSHE"
            expected_prefix = "sh" if input_id == "CSI-INDEX-DAILY-001" else "sz"
            if (not isinstance(symbols, list) or not symbols or source.get("asset_type") != "index"
                    or source.get("value_contract") != expected_contract):
                raise ValueError("non-default daily routes require explicit code scope and value contract")
            if (set(source.get("market_adjustment_requests", {})) != {expected_market}
                    or set(source["market_adjustment_requests"][expected_market]) != {"none"}):
                raise ValueError("external index route must explicitly cover one market and unadjusted daily values")
            for symbol in symbols:
                if (not isinstance(symbol, str) or not symbol.startswith(expected_prefix)
                        or not symbol[2:].isdigit() or len(symbol) != 8 or symbol in seen_symbols):
                    raise ValueError("external index route symbols must be unique six-digit codes for the configured market")
                if input_id in {"CSI-INDEX-DAILY-OHLC-001", "CSI-INDEX-DAILY-CLOSE-PAIR-001"}:
                    relation = symbol_parallel_codes.get(("XSHE", "index", symbol[2:]))
                    if relation is None or relation["request_market"] != "XSHG":
                        raise ValueError(f"CSI paired-code route lacks an evidenced parallel code for {symbol}")
                seen_symbols.add(symbol)
        source["provider"], source["endpoint"] = capability.provider, capability.endpoint
    default = defaults[0]
    if definition.get("input_id", default["input_id"]) != default["input_id"]:
        raise ValueError("forced source is outside the configured default acquisition policy")
    return {**definition, "asset_type": asset, "input_id": default["input_id"],
            "acquisition_policy": policy_name, "source_policy": policy,
            "symbol_successions": list(symbol_successions),
            "symbol_parallel_codes": deepcopy(security_master.get("symbol_parallel_codes", [])),
            "quote_contract": contract, "schema_version": DAILY_SCHEMA_VERSION,
            "contract_versions": {name: file_hash(config_root / name) for name in
                ("datasets/daily_bar.yaml", "normalization/daily_bar.yaml")},
            "source_contract_version": row_hash(asdict(contracts[default["input_id"]])),
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
    units = [deepcopy(unit) for unit in units]
    sources = definition["source_policy"]["inputs"]
    default_source = next(source for source in sources if source.get("default") is True)
    from datetime import date
    from ..service.instruments import symbol_succession_for
    parallel_codes = _normalize_symbol_parallel_codes(definition.get("symbol_parallel_codes", ()))
    for unit in units:
        symbol = str(unit.get("symbol", ""))
        market = _daily_market(symbol)
        request = unit.get("context", {}).get("request", {})
        target = definition.get("trade_date") or request.get("end_date") or request.get("trade_date")
        if not target:
            continue
        target_date = target if isinstance(target, date) else date.fromisoformat(str(target)[:10])
        relation = symbol_succession_for(definition.get("symbol_successions", ()), market=market,
            asset_type=unit.get("asset_type", definition["asset_type"]), symbol=symbol, as_of_date=target_date)
        if relation is None:
            continue
        prefix = {"XSHG": "sh", "XSHE": "sz", "BSE": "bj"}[market]
        new_symbol = prefix + str(relation["new_symbol"])
        unit["symbol"] = new_symbol
        unit["instrument_id"] = f"{market}:{relation['old_symbol']}"
        if unit.get("key") == symbol:
            unit["key"] = new_symbol
        elif str(unit.get("key", "")).startswith(symbol + "__"):
            unit["key"] = new_symbol + str(unit["key"])[len(symbol):]
        unit.setdefault("context", {}).setdefault("request", {})["symbol"] = new_symbol
        unit["symbol_relation"] = relation
    override_by_symbol = {symbol: source for source in sources if source is not default_source
                          for symbol in source["symbols"]}
    expanded = []
    target_limit = definition["source_policy"].get("max_symbols")
    if target_limit is not None and len({u.get("symbol") for u in units}) > int(target_limit):
        raise ValueError(f"daily task exceeds configured symbol limit {target_limit}")
    for unit in units:
        source = override_by_symbol.get(unit.get("symbol"), default_source)
        if source is not default_source and unit.get("asset_type", definition["asset_type"]) != source.get("asset_type"):
            raise ValueError(f"daily source route for {unit.get('symbol')} is restricted to {source.get('asset_type')}")
        requests_by_market = source["market_adjustment_requests"]
        logical_market = _daily_market(unit.get("symbol", ""))
        source_symbol = unit.get("symbol")
        market = logical_market
        if source["input_id"] in {"CSI-INDEX-DAILY-OHLC-001", "CSI-INDEX-DAILY-CLOSE-PAIR-001"}:
            relation = parallel_codes.get((logical_market, "index", str(unit["symbol"])[2:]))
            if relation is None:
                raise ValueError(f"CSI route has no evidenced paired request code for {unit.get('symbol')}")
            source_symbol, market = relation["request_symbol"], relation["request_market"]
            unit["source_symbol"] = source_symbol
            unit["parallel_code_evidence"] = relation["evidence_refs"]
            unit.setdefault("context", {}).setdefault("request", {})["source_symbol"] = source_symbol
        else:
            unit.pop("source_symbol", None)
            unit.pop("parallel_code_evidence", None)
            unit.setdefault("context", {}).setdefault("request", {}).pop("source_symbol", None)
        requests = requests_by_market.get(market)
        if requests is None:
            raise ValueError(f"daily source policy has no route for market {market}")
        value_contract = source.get("value_contract", "full_ohlc")
        if "adjustment" in unit:
            item = deepcopy(unit)
            item.update(input_id=source["input_id"], provider=source["provider"], endpoint=source["endpoint"],
                        value_contract=value_contract)
            expanded.append(item)
            continue
        if set(unit.get("context", {}).get("config", {})) & {"api", "adjust"}:
            raise ValueError("daily adjustment request parameters belong to the acquisition policy")
        if unit.get("replay_manifest"):
            raise ValueError("aggregate replay requires explicit per-adjustment replay_manifests")
        for name, state in definition["quote_contract"].items():
            # Keep the existing stock collection behavior for all three
            # Tencent series. Publication requires only `none`; adjusted
            # stock series remain best-effort. ETF/index optional scopes are
            # not expanded into new source requests here.
            request_optional_stock_adjustment = (
                definition["asset_type"] == "stock" and name in {"forward", "backward"}
            )
            if state != "required" and not request_optional_stock_adjustment:
                continue
            item = deepcopy(unit)
            item.update(key=unit["symbol"] + "__" + name, adjustment=name, asset_type=definition["asset_type"],
                        input_id=source["input_id"], provider=source["provider"], endpoint=source["endpoint"],
                        value_contract=value_contract)
            item["context"].setdefault("config", {}).update(requests[name])
            manifests = item.pop("replay_manifests", {})
            if name in manifests:
                item["replay_manifest"] = manifests[name]
            expanded.append(item)
    for unit in expanded:
        source = next((item for item in sources if item["input_id"] == unit["input_id"]), None)
        request_market = _daily_market(unit.get("source_symbol") or unit.get("symbol", ""))
        market_requests = source["market_adjustment_requests"].get(request_market) if source else None
        expected_request = market_requests.get(unit["adjustment"]) if market_requests else None
        optional_stock_adjustment = (
            unit.get("asset_type", definition["asset_type"]) == "stock"
            and unit["adjustment"] in {"forward", "backward"}
            and definition["quote_contract"].get(unit["adjustment"]) == "unverified"
        )
        if ((definition["quote_contract"].get(unit["adjustment"]) != "required" and not optional_stock_adjustment)
                or expected_request is None or unit["context"].get("config", {}) != expected_request
                or unit.get("provider") != source["provider"] or unit.get("endpoint") != source["endpoint"]):
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
    expected = {u["symbol"]: u.get("instrument_id") or
        {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[u["symbol"][:2]] + ":" + u["symbol"][2:] for u in units}
    selected = set(state.get("selected_symbols", ()))
    suspended = [] if selected else list(definition.get("suspended_securities", ()))
    lifecycle_exclusions = [] if selected else list(definition.get("lifecycle_exclusions", ()))
    target_ids = {identity for symbol, identity in expected.items() if not selected or symbol in selected}
    target_ids.update(item["instrument_id"] for item in suspended)
    groups, issues, unavailable_reasons = {}, [], {}
    for unit in units:
        symbol, adjustment = unit["symbol"], unit["adjustment"]
        if selected and symbol not in selected:
            continue
        source = source_rows.get(unit["key"])
        if source is None:
            entry = state["units"].get(unit["key"], {})
            report = entry.get("report", {})
            optional = definition["quote_contract"][adjustment] != "required"
            reason = report.get("error") or entry.get("error") or (
                "optional adjusted quote was not provided by the source" if optional else "required source quote is missing")
            if optional:
                unavailable_reasons[(symbol, adjustment)] = reason
            issues.append({"stage": entry.get("report", {}).get("failure_stage", "processing"), "symbol": symbol,
                "trade_date": trade_date.isoformat(), "adjustment": adjustment, "input_id": unit["input_id"],
                "code": ("optional_adjustment_unavailable" if optional else
                    report.get("failure_class") or entry.get("failure_class") or "missing"),
                "reason": reason,
                "raw_ref": entry.get("raw_manifest"), "provider": report.get("provider"),
                "endpoint": report.get("endpoint"), "record_locator": report.get("record_locator"),
                "field": report.get("error_field"),
                "impact": "unverified_quote_group" if optional else "incomplete_daily_record"})
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
            value_contract=row.get("value_contract", unit.get("value_contract", "full_ohlc")),
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
                reason=unavailable_reasons.get((symbol, name)) or
                    ("required source quote missing" if requirement == "required" else "applicability has not been verified"))
            if requirement == "unverified" and (symbol, name) not in unavailable_reasons:
                issues.append({"stage": "quality", "symbol": symbol, "adjustment": name, "code": "unverified_applicability",
                               "reason": "asset/source applicability is unverified", "impact": "unverified_quote_group"})
        record = DailyRecord(identity, trade_date, asset, **quotes)
        if record.complete:
            records.append(record)
    for item in suspended:
        basis = item.get("status_evidence", {}).get("basis")
        reason = ("manual lifecycle override marks the target date suspended"
            if basis == "manual_security_lifecycle_override"
            else "target-date BaoStock tradeStatus=0; no daily bar is required")
        issues.append({"stage": "qualification", "symbol": item["symbol"], "instrument_id": item["instrument_id"],
            "asset_type": item["asset_type"], "code": "confirmed_suspended", "status": "suspended",
            "reason": reason,
            "status_evidence": item["status_evidence"], "impact": "excluded_from_daily_denominator"})
    for item in lifecycle_exclusions:
        issues.append({"stage": "qualification", "symbol": item["symbol"],
            "instrument_id": item.get("instrument_id"), "asset_type": item["asset_type"],
            "code": "manual_lifecycle_exclusion", "status": item["status"],
            "reason": "manual lifecycle override excludes the security from the target-date universe",
            "status_evidence": item["status_evidence"], "impact": item["impact"]})
    store = CanonicalPartitionStore(paths["canonical_root"])
    existing = store.read_daily(asset.value, trade_date.isoformat())
    available = {r.instrument_id for r in records}
    if state["redo"] != "full":
        available.update(r.instrument_id for r in existing if r.instrument_id not in target_ids)
    statuses = {identity: ItemStatus.SUCCESS.value if identity in available else ItemStatus.MISSING.value for identity in expected.values()}
    statuses.update({item["instrument_id"]: ItemStatus.NO_TRADE.value for item in suspended})
    state["daily_check"] = {"issues": issues, "item_statuses": statuses,
        "expected_count": len(expected), "actual_count": len(set(expected.values()) & available),
        "suspended_count": len(suspended), "suspended_securities": suspended,
        "lifecycle_exclusions": lifecycle_exclusions}
    settings = definition["publication_policy"]
    policy = PublicationPolicy(Decimal(str(settings["max_missing_ratio"])), settings["max_missing_count"])
    if not policy.allows(expected_count=len(expected), actual_count=state["daily_check"]["actual_count"]):
        raise PublicationThresholdExceeded("daily aggregate coverage rejected; see per-adjustment issues")
    return {"records": records, "partition_directory": str(store.daily_directory(asset.value, trade_date.isoformat())),
        "asset_type": asset.value, "partition_key": trade_date.isoformat(), "expected_count": len(expected),
        "item_statuses": statuses, "replace_ids": sorted(target_ids), "adjustment": "aggregate",
        "publication_metadata": {"schema_version": DAILY_SCHEMA_VERSION, "issues": issues}}
