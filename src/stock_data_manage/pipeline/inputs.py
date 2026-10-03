from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from ..config.loader import load_input_capabilities, load_input_field_contract, load_collection_profiles
from ..providers.transport import captured_requests, RequestPacer
from ..quality.normalization import Normalizer, NormalizationError
from ..routing.factory import build_input_provider
from ..storage.raw import RawObjectStore, sanitized_metadata


def _json_value(value):
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value is None or type(value) in {str, int, float, bool}:
        return value
    # Decimals retain precision instead of being coerced through binary floating point.
    return str(value)


def _validate_candidate_root(config_root, output_root):
    # Avoid accidental writes through the manual input path into configured production stores.
    import yaml
    collection_document = yaml.safe_load((config_root / "collection.yaml").read_text(encoding="utf-8"))
    project_root = config_root.resolve().parent
    for key in ("raw_root", "canonical_root", "metadata_path"):
        protected = (project_root / collection_document["storage"][key]).resolve()
        if output_root.resolve().is_relative_to(protected):
            raise ValueError("manual input outputs must be outside production paths")
    hot_path = (project_root / collection_document.get("realtime_minute", {}).get("hot_store", {}).get("path", "data/hot")).resolve()
    if output_root.resolve().is_relative_to(hot_path.parent):
        raise ValueError("manual input outputs must be outside production hot store")
    return project_root


def collect_input(*, input_id, context, config_root, output_root, mode="replay", replay_manifest=None,
                  evidence_root=None, fields=None, client=None, pacer=None):
    """One explicit input, candidate output only. Existing Bar publication flows are unchanged."""
    config_root, output_root = Path(config_root), Path(output_root)
    if mode not in {"live", "replay"} or (mode == "replay" and replay_manifest is None):
        raise ValueError("replay requires an explicit manifest; only live/replay modes are supported")
    project_root = _validate_candidate_root(config_root, output_root)
    contracts = {c.input_id: c for c in load_input_capabilities(config_root / "providers.yaml")}
    if input_id not in contracts:
        raise ValueError("unknown input ID")
    contract = contracts[input_id]
    parameters = contract.bind_parameters(context)
    dataset, rule = load_input_field_contract(config_root, contract)
    schema_fields = dataset["fields"]
    selected = set(schema_fields) if fields is None else set(fields)
    required = {name for name, definition in schema_fields.items() if definition.get("required", False)}
    if selected - set(schema_fields) or not required <= selected:
        raise ValueError("selected fields must be declared and include all required fields")
    profiles = {p.name: p for p in load_collection_profiles(config_root / "collection.yaml")}
    profile = profiles[contract.collection_profile]
    provider = build_input_provider(contract, providers_path=config_root / "providers.yaml", client=client)
    code_files = [Path(__file__), Path(__file__).parents[1] / "cli.py", Path(__file__).parents[1] / "config/loader.py",
                  Path(__file__).parents[1] / "providers/contracts.py",
                  Path(__file__).parents[1] / "quality/normalization.py",
                  Path(__file__).parents[1] / "providers/transport.py",
                  Path(__file__).parents[1] / "storage/raw.py",
                  Path(__file__).parents[1] / "routing/factory.py",
                  Path(__file__).parents[1] / "providers/tencent/daily.py",
                  Path(__file__).parents[1] / "providers/tencent/minute.py",
                  Path(__file__).parents[1] / "providers/tencent/snapshot.py",
                  Path(__file__).parents[1] / "providers/eastmoney/limit_pool.py",
                  Path(__file__).parents[1] / "providers/sina/calendar.py",
                  Path(__file__).parents[1] / "providers/akshare/boards.py",
                  Path(__file__).parents[1] / "providers/baostock/industry.py",
                  Path(__file__).parents[1] / "providers/baostock/session.py"]
    code_version = hashlib.sha256(b"".join(path.read_bytes() for path in code_files)).hexdigest()
    # Response filenames contain a full SHA-256 plus a temporary suffix. Keep the
    # run component short for Windows paths; UTC times remain in every evidence record.
    run_id = uuid4().hex[:12]
    directory = output_root / f"{input_id}-{run_id}"
    directory.mkdir(parents=True, exist_ok=False)
    raw_store, result_store = RawObjectStore(directory / "_raw"), RawObjectStore(directory)
    normalized_context = _json_value(parameters)
    report = {"input_id": input_id, "dataset": contract.dataset, "mode": mode,
        "parameters": sanitized_metadata(normalized_context), "provider": contract.provider, "endpoint": contract.endpoint,
        "collection_profile": _json_value(asdict(profile)),
        "request_interval_seconds": max(3, contract.request_interval_seconds),
        "effective_concurrency": 1, "request_limit_enforcement": "call_boundary_only",
        "adapter_version": provider.capability_version, "code_version": code_version,
        "code_files": [{"path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in code_files],
        "config_files": [{"path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in (config_root / "providers.yaml", config_root / "collection.yaml",
                         config_root / "datasets" / f"{contract.dataset}.yaml",
                         config_root / "normalization" / f"{contract.dataset}.yaml")],
        "normalization_version": rule["version"], "normalization_status": rule["status"],
        "field_units": {name: definition.get("unit") for name, definition in schema_fields.items() if name in selected},
        "unverified_fields": rule.get("unverified_fields", []), "selected_fields": sorted(selected),
        "eligible_for_production_routing": False, "production_writes": 0, "live_http_calls": 0,
        "validation_time_utc": datetime.now(timezone.utc).isoformat(), "status": "started"}
    report["response_freshness_seconds"] = profile.refresh_interval_seconds or 86400
    is_baostock = input_id in {"SDA-BOARD-005", "SDA-BOARD-006"}
    is_tencent_snapshot = input_id == "ASTOCK-001"
    if is_tencent_snapshot:
        report.update(coverage_denominator=len(parameters["symbols"]), requested_symbols=list(parameters["symbols"]),
                      universe_completeness_verified=False, online_batch_validation=False,
                      quote_time_semantics="source quote timestamp; as_of is a local date check, not a historical query")
    if input_id == "SDA-BOARD-002":
        report["config_files"].extend({"path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in (config_root / "datasets/industry_directory.yaml", config_root / "normalization/industry_directory.yaml"))
    if context.get("calendar", {}).get("trading_dates") is not None:
        import json
        days = sorted(day.isoformat() for day in context["calendar"]["trading_dates"])
        report["calendar_context"] = {"positive_date_count": len(days),
                                     "sha256": hashlib.sha256(json.dumps(days).encode()).hexdigest()}
    calendar_path = context.get("metadata", {}).get("calendar_path")
    if calendar_path:
        report["calendar_dependency"] = {"path": calendar_path, "sha256": hashlib.sha256(Path(calendar_path).read_bytes()).hexdigest()}
    context_path = context.get("metadata", {}).get("context_path")
    if context_path:
        report["context_dependency"] = {"path": context_path, "sha256": hashlib.sha256(Path(context_path).read_bytes()).hexdigest()}
    report["field_units"] = {name: ("unverified; source value retained separately" if name in rule.get("unverified_fields", ()) else unit)
                              for name, unit in report["field_units"].items()}
    pacer = pacer or RequestPacer()
    # Host intervals configured before execution; tuple timeout and SDK retry behavior are retained.
    from urllib.parse import urlsplit
    for host in getattr(provider, "input_hosts", ()):
        pacer.configure(urlsplit(host).hostname, max(3, contract.request_interval_seconds), 1)
    evidence_root = Path(evidence_root) if evidence_root else project_root / "provider_validation/results"
    try:
        if is_baostock:
            import inspect
            import importlib.metadata
            try:
                import baostock as sdk_module
            except ImportError:
                sdk_module = None
            sdk = client or sdk_module
            report["sdk_dependency"] = {"version": (getattr(client, "__version__", "injected_fixture") if client is not None else
                                                     importlib.metadata.version("baostock") if sdk_module else "unavailable_offline"),
                                        "client_injected": client is not None, "dependencies": []}
            for name in ("query_all_stock", "query_stock_industry", "login", "logout"):
                function = getattr(sdk, name, None)
                if function is None:
                    continue
                try:
                    source = inspect.getsource(function).encode("utf-8")
                except (TypeError, OSError):
                    source = repr(type(sdk)).encode("utf-8")
                digest = hashlib.sha256(source).hexdigest()
                import re
                snapshot = re.sub(rb'''(?i)((?:password|user_id|token|api_key)\s*=\s*["'])[^"']*(["'])''', rb'\1<redacted>\2', source)
                sdk_ref = raw_store.write_bytes(snapshot, dataset="source_code", provider=contract.provider, endpoint=contract.endpoint,
                    fetched_at=datetime.now(timezone.utc), attempt_id="sdk-function", content_addressed=True)
                report["sdk_dependency"]["dependencies"].append({"name": name, "path": sdk_ref.path.relative_to(directory).as_posix(),
                    "sha256": sdk_ref.content_hash, "original_source_sha256": digest, "redacted": snapshot != source})
                code_version = hashlib.sha256((code_version + name + digest).encode()).hexdigest()
            code_version = hashlib.sha256((code_version + report["sdk_dependency"]["version"]).encode()).hexdigest()
            report["code_version"] = code_version
            report["evidence_representation"] = "SDK-decoded ResultSet fields and rows; BaoStock TCP wire bytes are not exposed"
            report["request_limit_enforcement"] = "sdk_query_boundary_only"
        sdk_functions = {"ASTOCK-045": "stock_zt_pool_em", "ASTOCK-070": "tool_trade_date_hist_sina",
                         "SDA-BOARD-001": "stock_board_industry_name_ths", "SDA-BOARD-002": "stock_board_industry_index_ths",
                         "SDA-BOARD-003": "stock_fund_flow_industry", "SDA-BOARD-004": "stock_fund_flow_concept"}
        if input_id in sdk_functions:
            import inspect
            from ..providers.akshare.session import load_client
            provider.client = provider.client or load_client()
            function = getattr(provider.client, sdk_functions[input_id])
            try:
                sdk_source = inspect.getsource(function).encode("utf-8")
            except (TypeError, OSError):
                sdk_source = repr(type(provider.client)).encode("utf-8")
            sdk_source_hash = hashlib.sha256(sdk_source).hexdigest()
            # The SDK embeds a public client selector; keep its hash, redact token-like literals in the snapshot.
            import re
            sdk_snapshot = re.sub(rb'''(?i)(["'](?:ut|token|access_token|api_key|password|secret)["']\s*:\s*["'])[^"']*(["'])''',
                                  rb'\1<redacted>\2', sdk_source)
            sdk_ref = raw_store.write_bytes(sdk_snapshot, dataset="source_code", provider=contract.provider,
                endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="sdk-function", content_addressed=True)
            report["sdk_dependency"] = {"version": getattr(provider.client, "__version__", "injected_fixture"),
                "function": function.__name__, "source_path": sdk_ref.path.relative_to(directory).as_posix(), "sha256": sdk_ref.content_hash,
                "original_source_sha256": sdk_source_hash, "redacted": sdk_snapshot != sdk_source}
            code_version = hashlib.sha256((code_version + sdk_source_hash + report["sdk_dependency"]["version"]).encode()).hexdigest()
            if input_id.startswith("SDA-BOARD-"):
                namespace = getattr(function, "__globals__", {})
                dependencies = []
                for name in ("_get_stock_board_industry_name_ths", "_get_file_content_ths", "get_ths_js"):
                    dependency = namespace.get(name)
                    if dependency is not None:
                        dependencies.append((name, inspect.getsource(dependency).encode("utf-8")))
                if namespace.get("get_ths_js"):
                    dependencies.append(("ths.js", Path(namespace["get_ths_js"]("ths.js")).read_bytes()))
                report["sdk_dependency"]["dependencies"] = []
                for name, body in dependencies:
                    digest = hashlib.sha256(body).hexdigest()
                    snapshot = re.sub(rb'''(?i)(["'](?:ut|token|access_token|api_key|password|secret)["']\s*:\s*["'])[^"']*(["'])''',
                                      rb'\1<redacted>\2', body)
                    ref = raw_store.write_bytes(snapshot, dataset="source_code", provider=contract.provider, endpoint=contract.endpoint,
                                               fetched_at=datetime.now(timezone.utc), attempt_id="sdk-dependency", content_addressed=True)
                    report["sdk_dependency"]["dependencies"].append({"name": name, "path": ref.path.relative_to(directory).as_posix(),
                        "sha256": ref.content_hash, "original_source_sha256": digest, "redacted": snapshot != body})
                    code_version = hashlib.sha256((code_version + name + digest).encode()).hexdigest()
            report["code_version"] = code_version
        from contextlib import ExitStack
        actual_code = None
        with ExitStack() as stack:
            runtime_parameters = dict(parameters)
            if is_tencent_snapshot:
                stack.callback(provider.transport.close)
                # The input is a stock scope, while the legacy Provider also supports funds/indexes.
                import re
                if any(not re.fullmatch(r"(?:sh(?:60\d{4}|68\d{4})|sz(?:00\d{4}|30\d{4})|bj(?:[48]\d{5}|92\d{4}))", symbol)
                       for symbol in parameters["symbols"]):
                    raise ValueError("snapshot input accepts explicit stock codes only")
            if is_baostock:
                from ..providers.baostock.session import captured_sdk_queries
                sdk_client, sdk_archive, response_events = stack.enter_context(captured_sdk_queries(raw_store, mode=mode,
                    replay_manifest=replay_manifest, evidence_roots=(evidence_root, output_root),
                    scope={"input_id": input_id, "parameters": normalized_context}, code_version=code_version,
                    trade_date=parameters["trade_date"], pacer=pacer, interval_seconds=max(3, contract.request_interval_seconds),
                    max_age_seconds=profile.refresh_interval_seconds or 86400, client=client))
                provider.client = sdk_client
                runtime_parameters["raw_archive"] = sdk_archive
            else:
                response_events = stack.enter_context(captured_requests(raw_store, provider=contract.provider, endpoint=contract.endpoint,
                    scope={"input_id": input_id, "parameters": normalized_context}, code_version=code_version, pacer=pacer,
                    replay_manifest=replay_manifest if mode == "replay" else None,
                    evidence_roots=(evidence_root, output_root), max_age_seconds=profile.refresh_interval_seconds or 86400,
                    sdk_retry_policy=input_id in {"ASTOCK-001", "ASTOCK-045", "ASTOCK-070"}))
            # Isolate only replay's directory cache; live SDK Session/cache policy stays intact.
            if mode == "replay" and input_id in {"SDA-BOARD-001", "SDA-BOARD-002"}:
                from functools import lru_cache
                from unittest.mock import patch
                namespace = getattr(function, "__globals__", {})
                helper = namespace.get("_get_stock_board_industry_name_ths")
                if helper is not None and hasattr(helper, "__wrapped__"):
                    stack.enter_context(patch.dict(namespace, {"_get_stock_board_industry_name_ths": lru_cache()(helper.__wrapped__)}))
                    report["replay_cache_isolated"] = True
            fetched = getattr(provider, contract.runtime_method)(**runtime_parameters)
            if input_id == "SDA-BOARD-002":
                helper = getattr(function, "__globals__", {}).get("_get_stock_board_industry_name_ths")
                if helper is not None:
                    actual_code = helper()[parameters["board_name"]]
        report["responses"] = response_events
        report["live_http_calls"] = sum(event["mode"] == "live" and event.get("event") == "http_response" for event in response_events)
        source_rows = getattr(fetched, "source_rows", None) or fetched.rows
        source_ref = result_store.write_json(_json_value(source_rows), dataset="source_rows", provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="source-rows")
        report["source_rows"] = {"path": source_ref.path.relative_to(directory).as_posix(),
                                 "sha256": source_ref.content_hash, "row_count": len(source_rows)}
        if actual_code is not None and fetched.mapping_context["board_code"] != actual_code:
            raise NormalizationError("board code dependency disagrees with source directory")
        report["source_url"] = fetched.source_url
        report["source_urls"] = list(dict.fromkeys(event["url"] for event in response_events if event.get("url")))
        if is_baostock:
            report.update(coverage_denominator=fetched.coverage_denominator, missing_symbols=list(fetched.missing_symbols),
                missing_industry_symbols=list(fetched.missing_industry_symbols),
                coverage_complete=len(source_rows) == fetched.coverage_denominator,
                live_sdk_calls=sum(event["mode"] == "live" for event in response_events),
                sdk_query_count=len(response_events), source_sdk_row_counts={event["endpoint"]: event.get("metadata", {}).get("row_count")
                                                                        for event in response_events})
            report["coverage_scope"] = "SH/SZ A-share codes filtered from this SDK response; not an independent market census"
        if is_tencent_snapshot:
            from zoneinfo import ZoneInfo
            quote_dates = [datetime.strptime(row["datetime"], "%Y%m%d%H%M%S").date() for row in source_rows]
            report.update(missing_symbols=list(fetched.missing_symbols), coverage_complete=not fetched.missing_symbols,
                          returned_window={"first": min(row["datetime"] for row in source_rows),
                                           "last": max(row["datetime"] for row in source_rows)},
                          coverage_scope="explicit requested stock list; no independent whole-market completeness proof")
            if any(day != parameters["as_of"].astimezone(ZoneInfo("Asia/Shanghai")).date() for day in quote_dates):
                raise NormalizationError("source quote date differs from requested as_of date; snapshot cannot query history")
        if not source_rows:
            raise NormalizationError("temporary empty input; not certified as a valid empty dataset")
        mapping_context = {"provider": contract.provider}
        mapping_context.update(getattr(fetched, "mapping_context", {}))
        if input_id.startswith("SDA-BOARD-") or is_tencent_snapshot:
            successful = [event for event in response_events if event.get("outcome") == "response"]
            if not successful:
                raise NormalizationError("source response evidence is required; an SDK memory cache alone is insufficient")
            source_times = []
            for event in successful:
                stamp = ((event.get("source_ref") or {}).get("fetched_at_utc")
                         if event["mode"] in {"replay", "cached"} else event["fetched_at_utc"])
                if not stamp:
                    raise NormalizationError("original source capture time is missing")
                source_times.append(datetime.fromisoformat(stamp))
            if any(stamp.tzinfo is None for stamp in source_times):
                raise NormalizationError("source capture time must be timezone-aware")
            mapping_context["source_snapshot_at"] = max(source_times)
            report["source_capture_window"] = {"first": min(source_times).isoformat(), "last": max(source_times).isoformat(),
                                               "meaning": "source response capture times; not row-level market timestamps"}
            if not is_tencent_snapshot:
                report["returned_window"] = {"first": fetched.returned_first_key, "last": fetched.returned_last_key}
                report["source_units"] = list(fetched.units)
                report["coverage_basis"] = "returned SDK rows; not an independently verified market universe"
        if "date" in parameters:
            mapping_context["trade_date"] = parameters["date"]
        if input_id.startswith("ASTOCK-002"):
            from ..providers.tencent.daily import tencent_symbol
            symbol = tencent_symbol(parameters["code"])
            mapping_context.update(instrument_id=f"{'XSHG' if symbol.startswith('sh') else 'XSHE'}:{symbol[2:]}",
                                   adjustment="forward" if input_id.endswith("daily") else "none")
        rows = [Normalizer.normalize_fields(row, rule=rule, fields=schema_fields,
                                            context=mapping_context, allow_pending=True) for row in source_rows]
        keys = [tuple(row[name] for name in dataset["dataset"]["primary_key"]) for row in rows]
        if len(set(keys)) != len(keys):
            raise NormalizationError("duplicate dataset primary key")
        if contract.dataset in {"daily_bar", "minute_bar_5m", "industry_index_daily"}:
            if any(not (row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"]) for row in rows):
                raise NormalizationError("invalid OHLC ordering")
        projected = [{name: value for name, value in row.items() if name in selected} for row in rows]
        normalized_ref = result_store.write_json(_json_value(projected), dataset=contract.dataset, provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="mapped-rows")
        report["output"] = {"path": normalized_ref.path.relative_to(directory).as_posix(),
                            "sha256": normalized_ref.content_hash, "row_count": len(projected),
                            "source_response_hashes": [event["body_sha256"] for event in response_events]}
        report.update(status="candidate_complete", row_count=len(projected),
                      coverage_denominator=fetched.coverage_denominator if is_baostock or is_tencent_snapshot else len(source_rows),
                      first_key=_json_value(keys[0]), last_key=_json_value(keys[-1]))
    except Exception as exc:
        report.update(status="failed", failure_class=getattr(getattr(exc, "failure_class", None), "value", type(exc).__name__),
                      error=str(exc) if isinstance(exc, (ValueError, NormalizationError)) else type(exc).__name__)
    raw_manifest = raw_store.root / "manifest.ndjson"
    if raw_manifest.exists():
        import json
        all_events = [json.loads(line) for line in raw_manifest.read_text(encoding="utf-8").splitlines()]
        report["responses"] = [event for event in all_events if event.get("event") in {"http_response", "source_payload", "sdk_query_failure"}]
        if is_baostock:
            report["sdk_session"] = [event for event in all_events if event.get("event") == "sdk_session"]
            report["sdk_derived"] = [event for event in all_events if event.get("event") == "sdk_derived"]
            report["live_sdk_calls"] = sum(event.get("mode") == "live" for event in report["responses"])
            report["sdk_query_count"] = len(report["responses"])
            report["sdk_dependency"]["live_sdk_executed"] = any(event.get("actual_session_called") for event in report["sdk_session"])
        report["raw_manifest"] = {"path": raw_manifest.relative_to(directory).as_posix(),
                                  "sha256": hashlib.sha256(raw_manifest.read_bytes()).hexdigest()}
        report["live_http_calls"] = sum(event.get("mode") == "live" and event.get("event") == "http_response" for event in all_events)
    report_ref = result_store.write_json(report, dataset="input_report", provider=contract.provider, endpoint=contract.endpoint,
                                        fetched_at=datetime.now(timezone.utc), attempt_id="report")
    return {**report, "run_directory": str(directory.resolve()), "report_path": str(report_ref.path.resolve())}


def collect_due_inputs(*, now, config_root, output_root, trading_dates, securities=(), symbols=(),
                       execute=False, mode="replay", replay_manifest=None, evidence_root=None,
                       dependency_paths=(), collector=None):
    """One scheduler tick, with durable attempts in candidate storage, never production publication."""
    from ..domain import AttemptStatus
    from ..storage.metadata import MetadataStore
    from ..worker.attempts import CollectionAttempt
    from ..worker.scheduler import plan_input_collection
    from time import monotonic
    import json

    root, output = Path(config_root), Path(output_root)
    if now.tzinfo is None:
        raise ValueError("schedule time must be timezone-aware")
    if mode not in {"live", "replay"}:
        raise ValueError("only live/replay modes are supported")
    if execute and mode == "live" and abs((datetime.now(timezone.utc) - now).total_seconds()) > 120:
        raise ValueError("live scheduling requires current time; use replay to inspect historical slots")
    _validate_candidate_root(root, output)
    trading_dates, securities, symbols = tuple(trading_dates), tuple(securities), tuple(symbols)
    jobs = plan_input_collection(root, now=now, trading_dates=trading_dates, securities=securities, symbols=symbols)
    profiles = {p.name: p for p in load_collection_profiles(root / "collection.yaml")}
    contracts = {c.input_id: c for c in load_input_capabilities(root / "providers.yaml")}
    if execute and mode == "replay" and replay_manifest is None:
        raise ValueError("execution in replay mode requires an explicit manifest")
    report = {"now": now.isoformat(), "execute": execute, "mode": mode,
        "eligible_for_production_routing": False, "production_writes": 0,
        "profiles": [_json_value(asdict(p)) for p in profiles.values() if p.frequency_unit],
        "dependencies": [{"path": str(Path(p).resolve()), "sha256": hashlib.sha256(Path(p).read_bytes()).hexdigest()}
                         for p in dependency_paths],
        "config_files": [{"path": str((root / name).resolve()), "sha256": hashlib.sha256((root / name).read_bytes()).hexdigest()}
                         for name in ("collection.yaml", "providers.yaml")],
        "code_files": [{"path": str(p.resolve()), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                       for p in (Path(__file__), Path(__file__).parents[1] / "worker/scheduler.py",
                                 Path(__file__).parents[1] / "config/loader.py")],
        "scope_context": _json_value({"trading_dates": trading_dates, "securities": [asdict(s) for s in securities],
                                      "symbols": symbols}), "jobs": []}
    output.mkdir(parents=True, exist_ok=True)
    pacer = RequestPacer()
    # All enabled Tencent Kline inputs share one host pacing policy before any call starts.
    interval = max([3] + [contracts[j.input_id].request_interval_seconds for j in jobs if j.status == "ready"])
    for host in ("web.ifzq.gtimg.cn", "proxy.finance.qq.com", "ifzq.gtimg.cn", "qt.gtimg.cn"):
        pacer.configure(host, interval, 1)
    collector = collector or collect_input
    metadata = MetadataStore(output / "schedule-attempts.duckdb") if execute else None
    try:
        for job in jobs:
            entry = {**_json_value(asdict(job)), "coverage_denominator": len(job.symbols), "results": [],
                     "validation_time_utc": datetime.now(timezone.utc).isoformat(),
                     "config_files": report["config_files"], "code_files": report["code_files"],
                     "universe_completeness_verified": False}
            report["jobs"].append(entry)
            if not execute or job.status != "ready":
                continue
            profile = profiles[job.profile]
            slot_key = job.slot.date().isoformat() if profile.frequency_unit == "day" else job.slot.isoformat()
            attempt_id = f"input-slot:{job.input_id}:{slot_key}"
            attempt = CollectionAttempt(attempt_id).lease(owner="input-scheduler", acquired_at=now,
                expires_at=now + timedelta(days=1)).transition(AttemptStatus.FETCHING)
            # Persist the reservation before calling any source; a restart cannot repeat the same slot.
            if not metadata.claim_attempt(attempt, updated_at=now):
                previous = metadata.load_attempt(attempt_id)
                entry.update(status="already_attempted", reason=previous.status.value,
                             previous_report_path=previous.raw_object_path)
                continue
            started = monotonic()
            budget = (profile.refresh_interval_seconds - (now - job.slot).total_seconds()
                      if profile.frequency_unit == "minute" else None)
            try:
                for symbol in job.symbols:
                    if budget is not None and monotonic() - started >= budget:
                        entry.update(status="failed", reason="collection interval budget exhausted")
                        break
                    request = {"symbol": symbol}
                    if job.input_id.endswith("daily"):
                        request.update(start_date=job.slot.date(), end_date=job.slot.date())
                    result = collector(input_id=job.input_id, context={"request": request,
                        "calendar": {"trading_dates": trading_dates}}, config_root=root, output_root=output,
                        mode=mode, replay_manifest=replay_manifest, evidence_root=evidence_root, pacer=pacer)
                    result_path = Path(result["report_path"])
                    entry["results"].append({"symbol": symbol, "status": result["status"],
                        "report_path": str(result_path), "report_sha256": hashlib.sha256(result_path.read_bytes()).hexdigest()
                        if result_path.is_file() else None, "row_count": result.get("row_count", 0),
                        "code_version": result.get("code_version"),
                        "source_response_hashes": result.get("output", {}).get("source_response_hashes", [])})
                complete = (len(entry["results"]) == len(job.symbols)
                            and all(r["status"] == "candidate_complete" for r in entry["results"]))
                entry["status"] = "candidate_complete" if complete else "failed"
                entry["successful_symbol_count"] = sum(r["status"] == "candidate_complete" for r in entry["results"])
                # This is collection coverage, not a claim of complete/current bars or production readiness.
                entry["coverage_semantics"] = "candidate results per requested security; bar freshness/completeness not certified"
            except Exception as exc:
                entry.update(status="failed", reason=type(exc).__name__)
            entry_bytes = json.dumps(entry, ensure_ascii=False, indent=2).encode("utf-8")
            saved = RawObjectStore(output).write_bytes(entry_bytes, dataset="schedule_attempt", provider="scheduler",
                endpoint=job.input_id, fetched_at=datetime.now(timezone.utc), attempt_id=uuid4().hex)
            attempt = replace(attempt, raw_object_path=str(saved.path.resolve()), raw_content_hash=saved.content_hash)
            entry["attempt_record"] = {"path": str(saved.path.resolve()), "sha256": saved.content_hash}
            if entry["status"] == "candidate_complete":
                attempt = attempt.transition(AttemptStatus.RAW_COMMITTED).transition(AttemptStatus.NORMALIZED).transition(AttemptStatus.VALIDATED)
            else:
                attempt = attempt.transition(AttemptStatus.TERMINAL_FAILED)
            metadata.save_attempt(attempt, updated_at=datetime.now(timezone.utc))
    finally:
        if metadata is not None:
            metadata.close()
    saved = RawObjectStore(output).write_json(report, dataset="schedule_tick", provider="scheduler", endpoint="inputs",
        fetched_at=datetime.now(timezone.utc), attempt_id=uuid4().hex)
    return {**report, "report_path": str(saved.path.resolve())}
