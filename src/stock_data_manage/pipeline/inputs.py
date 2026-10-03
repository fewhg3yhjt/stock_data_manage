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
                  Path(__file__).parents[1] / "providers/eastmoney/limit_pool.py",
                  Path(__file__).parents[1] / "providers/sina/calendar.py"]
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
        "eligible_for_production_routing": False, "production_writes": 0,
        "validation_time_utc": datetime.now(timezone.utc).isoformat(), "status": "started"}
    report["response_freshness_seconds"] = profile.refresh_interval_seconds or 86400
    if context.get("calendar", {}).get("trading_dates") is not None:
        import json
        days = sorted(day.isoformat() for day in context["calendar"]["trading_dates"])
        report["calendar_context"] = {"positive_date_count": len(days),
                                     "sha256": hashlib.sha256(json.dumps(days).encode()).hexdigest()}
    calendar_path = context.get("metadata", {}).get("calendar_path")
    if calendar_path:
        report["calendar_dependency"] = {"path": calendar_path, "sha256": hashlib.sha256(Path(calendar_path).read_bytes()).hexdigest()}
    report["field_units"] = {name: ("unverified; source value retained separately" if name in rule.get("unverified_fields", ()) else unit)
                              for name, unit in report["field_units"].items()}
    pacer = pacer or RequestPacer()
    # Host intervals configured before execution; tuple timeout and SDK retry behavior are retained.
    from urllib.parse import urlsplit
    for host in getattr(provider, "input_hosts", ()):
        pacer.configure(urlsplit(host).hostname, max(3, contract.request_interval_seconds), 1)
    evidence_root = Path(evidence_root) if evidence_root else project_root / "provider_validation/results"
    try:
        if input_id in {"ASTOCK-045", "ASTOCK-070"}:
            import inspect
            from ..providers.akshare.session import load_client
            provider.client = provider.client or load_client()
            function = getattr(provider.client, "stock_zt_pool_em" if input_id == "ASTOCK-045" else "tool_trade_date_hist_sina")
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
            report["code_version"] = code_version
        with captured_requests(raw_store, provider=contract.provider, endpoint=contract.endpoint,
             scope={"input_id": input_id, "parameters": normalized_context}, code_version=code_version, pacer=pacer,
             replay_manifest=replay_manifest if mode == "replay" else None,
             evidence_roots=(evidence_root, output_root), max_age_seconds=profile.refresh_interval_seconds or 86400,
             sdk_retry_policy=input_id in {"ASTOCK-045", "ASTOCK-070"}) as response_events:
            fetched = getattr(provider, contract.runtime_method)(**parameters)
        report["responses"] = response_events
        report["live_http_calls"] = sum(event["mode"] == "live" for event in response_events)
        source_ref = result_store.write_json(_json_value(fetched.rows), dataset="source_rows", provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="source-rows")
        report["source_rows"] = {"path": source_ref.path.relative_to(directory).as_posix(),
                                 "sha256": source_ref.content_hash, "row_count": len(fetched.rows)}
        report["source_url"] = fetched.source_url
        if not fetched.rows:
            raise NormalizationError("temporary empty input; not certified as a valid empty dataset")
        mapping_context = {"provider": contract.provider}
        if "date" in parameters:
            mapping_context["trade_date"] = parameters["date"]
        if input_id.startswith("ASTOCK-002"):
            from ..providers.tencent.daily import tencent_symbol
            symbol = tencent_symbol(parameters["code"])
            mapping_context.update(instrument_id=f"{'XSHG' if symbol.startswith('sh') else 'XSHE'}:{symbol[2:]}",
                                   adjustment="forward" if input_id.endswith("daily") else "none")
        rows = [Normalizer.normalize_fields(row, rule=rule, fields=schema_fields,
                                            context=mapping_context, allow_pending=True) for row in fetched.rows]
        keys = [tuple(row[name] for name in dataset["dataset"]["primary_key"]) for row in rows]
        if len(set(keys)) != len(keys):
            raise NormalizationError("duplicate dataset primary key")
        if contract.dataset in {"daily_bar", "minute_bar_5m"}:
            if any(not (row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"]) for row in rows):
                raise NormalizationError("invalid OHLC ordering")
        projected = [{name: value for name, value in row.items() if name in selected} for row in rows]
        normalized_ref = result_store.write_json(_json_value(projected), dataset=contract.dataset, provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="mapped-rows")
        report["output"] = {"path": normalized_ref.path.relative_to(directory).as_posix(),
                            "sha256": normalized_ref.content_hash, "row_count": len(projected),
                            "source_response_hashes": [event["body_sha256"] for event in response_events]}
        report.update(status="candidate_complete", row_count=len(projected), coverage_denominator=len(fetched.rows),
                      first_key=_json_value(keys[0]), last_key=_json_value(keys[-1]))
    except Exception as exc:
        report.update(status="failed", failure_class=getattr(getattr(exc, "failure_class", None), "value", type(exc).__name__),
                      error=str(exc) if isinstance(exc, (ValueError, NormalizationError)) else type(exc).__name__)
    raw_manifest = raw_store.root / "manifest.ndjson"
    if raw_manifest.exists():
        import json
        all_events = [json.loads(line) for line in raw_manifest.read_text(encoding="utf-8").splitlines()]
        report["responses"] = all_events
        report["raw_manifest"] = {"path": raw_manifest.relative_to(directory).as_posix(),
                                  "sha256": hashlib.sha256(raw_manifest.read_bytes()).hexdigest()}
        report["live_http_calls"] = sum(event.get("mode") == "live" for event in all_events)
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
