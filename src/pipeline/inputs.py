from __future__ import annotations

import hashlib
import os
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from ..config.loader import load_input_capabilities, load_input_field_contract, load_collection_profiles, load_storage_paths
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
    data_root = load_storage_paths(config_root)["data_root"]
    if output_root.resolve().is_relative_to(data_root):
        raise ValueError("manual input outputs must be outside production paths")
    for key in ("raw_root", "canonical_root", "metadata_path"):
        protected = (project_root / collection_document["storage"][key]).resolve()
        if output_root.resolve().is_relative_to(protected):
            raise ValueError("manual input outputs must be outside production paths")
    hot_path = (project_root / collection_document.get("realtime_minute", {}).get("hot_store", {}).get("path", "data/hot")).resolve()
    if output_root.resolve().is_relative_to(hot_path.parent):
        raise ValueError("manual input outputs must be outside production hot store")
    return project_root


def collect_input(*, input_id, context, config_root, output_root=None, data_root=None, mode="replay", replay_manifest=None,
                  evidence_root=None, fields=None, client=None, pacer=None, task_unit=None, force_fetch=False):
    """One explicit input, candidate output only. Existing Bar publication flows are unchanged."""
    import json
    import re
    config_root = Path(config_root)
    if output_root is not None and data_root is not None:
        raise ValueError("output_root and data_root are mutually exclusive")
    runtime = output_root is None
    if task_unit is not None and not runtime:
        raise ValueError("task collection requires runtime storage layout")
    paths = load_storage_paths(config_root, data_root=data_root) if runtime else None
    output_root = paths["raw_root"] if runtime else Path(output_root)
    if mode not in {"live", "replay"} or (mode == "replay" and replay_manifest is None):
        raise ValueError("replay requires an explicit manifest; only live/replay modes are supported")
    project_root = config_root.resolve().parent if runtime else _validate_candidate_root(config_root, output_root)
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
                  Path(__file__).parents[1] / "storage/parquet.py",
                  Path(__file__).parents[1] / "storage/metadata.py",
                  Path(__file__).parents[1] / "worker/attempts.py",
                  Path(__file__).parents[1] / "routing/factory.py",
                  Path(__file__).parents[1] / "providers/tencent/daily.py",
                  Path(__file__).parents[1] / "providers/tencent/minute.py",
                  Path(__file__).parents[1] / "providers/tencent/snapshot.py",
                  Path(__file__).parents[1] / "providers/eastmoney/limit_pool.py",
                  Path(__file__).parents[1] / "providers/eastmoney/shareholder.py",
                  Path(__file__).parents[1] / "providers/eastmoney/financial.py",
                  Path(__file__).parents[1] / "providers/eastmoney/news.py",
                  Path(__file__).parents[1] / "providers/wallstreetcn/news.py",
                  Path(__file__).parents[1] / "providers/cctv/news.py",
                  Path(__file__).parents[1] / "providers/cls/news.py",
                  Path(__file__).parents[1] / "providers/sina/news.py",
                  Path(__file__).parents[1] / "providers/sina/financial.py",
                  Path(__file__).parents[1] / "providers/sse/market.py",
                  Path(__file__).parents[1] / "providers/csindex/indices.py",
                  Path(__file__).parents[1] / "providers/tdx/minute.py",
                  Path(__file__).parents[1] / "providers/chinabond/yield_curve.py",
                  Path(__file__).parents[1] / "providers/exchanges/daily.py",
                  Path(__file__).parents[1] / "providers/exchanges/security.py",
                  Path(__file__).parents[1] / "providers/sge/spot.py",
                  Path(__file__).parents[1] / "providers/mofcom/social_financing.py",
                  Path(__file__).parents[1] / "providers/chinamoney/rates.py",
                  Path(__file__).parents[1] / "providers/eastmoney/dividend.py",
                  Path(__file__).parents[1] / "providers/eastmoney/fund_flow.py",
                  Path(__file__).parents[1] / "providers/eastmoney/realtime.py",
                  Path(__file__).parents[1] / "providers/sina/calendar.py",
                  Path(__file__).parents[1] / "providers/sina/snapshot.py",
                  Path(__file__).parents[1] / "providers/sina/daily.py",
                  Path(__file__).parents[1] / "providers/akshare/boards.py",
                  Path(__file__).parents[1] / "providers/baostock/industry.py",
                  Path(__file__).parents[1] / "providers/cninfo/profile.py",
                  Path(__file__).parents[1] / "providers/baostock/session.py"]
    code_version = hashlib.sha256(b"".join(path.read_bytes() for path in code_files)).hexdigest()
    # Response filenames contain a full SHA-256 plus a temporary suffix. Keep the
    # run component short for Windows paths; UTC times remain in every evidence record.
    run_id = uuid4().hex[:12]
    # The input identity already lives in the source directory and manifest.
    # Avoid repeating it in runtime paths near the Windows filename limit.
    task_id = run_id if runtime else f"{input_id}-{run_id}"
    normalized_context = _json_value(parameters)
    scope_key = "scope-" + hashlib.sha256(json.dumps(sanitized_metadata(normalized_context),
        ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]
    if runtime:
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in
               (contract.dataset, contract.provider, contract.endpoint, input_id)):
            raise ValueError("unsafe input storage path component")
        directory = paths["workspace_root"] / contract.dataset / scope_key / task_id
        raw_directory = paths["raw_root"] / contract.provider / contract.endpoint / datetime.now(timezone.utc).date().isoformat() / run_id
        if task_unit is not None:
            raw_directory = RawObjectStore.task_unit_path(paths["raw_root"], *task_unit)
            if raw_directory.exists():
                raise ValueError("task unit raw directory must be prepared before collection")
        source_directory = Path("sources") / contract.provider / input_id
    else:
        directory = output_root / task_id
        raw_directory = directory / "_raw"
    directory.mkdir(parents=True, exist_ok=False)
    raw_store, result_store = RawObjectStore(raw_directory), RawObjectStore(directory)
    if task_unit is not None:
        if not runtime:
            raise ValueError("task collection requires runtime storage layout")
        raw_store.write_json({"task_id": task_unit[0], "unit_key": task_unit[1]}, dataset="task",
            provider=contract.provider, endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc),
            attempt_id="owner", relative_path="_managed_task.json")

    def relative_file(path):
        return Path(os.path.relpath(path, directory)).as_posix()

    def write_result(value, **kwargs):
        if runtime:
            name = kwargs["attempt_id"]
            relative = Path("report.json") if name == "report" else (
                source_directory / "normalized.json" if name == "mapped-rows" else
                source_directory / "parsed" / f"{name}.json")
            kwargs["relative_path"] = relative
        return result_store.write_json(value, **kwargs)

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
    if runtime:
        from ..domain import AttemptStatus
        from ..storage.metadata import MetadataStore
        from ..worker.attempts import CollectionAttempt
        now = datetime.now(timezone.utc)
        attempt = CollectionAttempt(task_id).lease(owner="input-collection", acquired_at=now,
            expires_at=now + timedelta(days=1)).transition(AttemptStatus.FETCHING)
        metadata = MetadataStore(paths["metadata_path"])
        try:
            if not metadata.claim_attempt(attempt, updated_at=now):
                raise ValueError("input task ID already reserved")
        finally:
            metadata.close()
        result_store.write_json({"task_id": task_id, "status": "fetching", "scope_key": scope_key,
            "scope": sanitized_metadata(normalized_context), "dataset": contract.dataset,
            "provider": contract.provider, "input_id": input_id, "created_at": now.isoformat()},
            dataset="task_start", provider=contract.provider, endpoint=contract.endpoint,
            fetched_at=now, attempt_id="started", relative_path="started.json")
    report["response_freshness_seconds"] = profile.refresh_interval_seconds or 86400
    is_baostock = input_id in {"SDA-BOARD-005", "SDA-BOARD-006", "ASTOCK-044"}
    is_bse_catalog = input_id == "SECURITY-BSE-001"
    if is_bse_catalog:
        import requests
        import urllib3
        report["transport_dependency"] = {"requests": requests.__version__, "urllib3": urllib3.__version__}
        code_version = hashlib.sha256((code_version + requests.__version__ + urllib3.__version__).encode()).hexdigest()
        report["code_version"] = code_version
        report["catalog_transport_policy"] = {"source_contract": "retained BSE official catalog probe",
            "session": "reuse page session and cookies", "trust_env": False,
            "proxy_reference": "STOCK_DATA_HTTP_PROXY; original local proxy default",
            "headers": "original Mozilla/5.0, quotation-page Referer and JSON Accept",
            "page_timeout_seconds": 20, "api_timeout_seconds": 30, "allow_redirects": False,
            "http_adapter_retries": 0, "redirect_retry": "refresh page, then repeat same POST once",
            "page_pause_seconds": 0.25, "request_body_matching": True, "source_fallback": False}
    is_actual_data = input_id in {'ASTOCK-014', 'ASTOCK-037-profile', 'ASTOCK-037-events', 'ASTOCK-087'}
    is_tencent_snapshot = input_id == "ASTOCK-001"
    is_em_history = input_id in {"ASTOCK-026", "ASTOCK-027", "ASTOCK-028"}
    is_em_events = input_id in {"ASTOCK-078", "ASTOCK-079", "ASTOCK-080", "ASTOCK-081", "ASTOCK-082", "ASTOCK-083"}
    is_lpr = input_id == "ASTOCK-065"
    is_news = input_id in {"ASTOCK-034", "ASTOCK-035"}
    is_reports_calendar = input_id in {"ASTOCK-013", "ASTOCK-066"}
    is_sdk_news = input_id in {"ASTOCK-032", "ASTOCK-033"}
    is_sdk_macro = input_id in {"ASTOCK-061", "ASTOCK-062"}
    is_factor = input_id == "ASTOCK-006"
    is_market_events = input_id in {"ASTOCK-020", "ASTOCK-021"}
    is_reportapi = input_id == "ASTOCK-008"
    is_seats = input_id == "ASTOCK-019"
    is_reports_seats = is_reportapi or is_seats
    is_source_sdk = is_actual_data or input_id in {"ASTOCK-031","ASTOCK-039","ASTOCK-051","ASTOCK-055","ASTOCK-067","ASTOCK-068","ASTOCK-069","ASTOCK-085"}
    is_original_source = input_id in {"ASTOCK-003","ASTOCK-004","ASTOCK-030","ASTOCK-063","ASTOCK-071","ASTOCK-072","ASTOCK-073","ASTOCK-077"}
    is_source_extension = is_source_sdk or is_original_source
    is_repo_rate = input_id == "ASTOCK-064"
    is_cb = input_id == "ASTOCK-084"
    is_sina_futures = input_id in {"ASTOCK-074", "ASTOCK-075", "ASTOCK-076"}
    if is_news or is_reports_calendar or is_repo_rate or is_sina_futures:
        import requests
        import urllib3
        report["transport_dependency"] = {"requests": requests.__version__, "urllib3": urllib3.__version__}
        code_version = hashlib.sha256((code_version + requests.__version__ + urllib3.__version__).encode()).hexdigest()
        report["code_version"] = code_version
        policy_key = "sina_futures_transport_policy" if is_sina_futures else "fixing_transport_policy" if is_repo_rate else "news_transport_policy"
        report[policy_key] = {"source_contract": "successful runnable original source",
            "headers": "original Chrome/126 User-Agent and finance.sina.com.cn Referer" if is_sina_futures else "original Chrome/126 User-Agent and Chinamoney bkfrr Referer" if is_repo_rate else "original Chrome/126 User-Agent; requests defaults otherwise", "trust_env": True,
            "timeout_seconds": [10, 40], "allow_redirects": True, "retry_total": 0,
            "outer_capture_interval_seconds": 3}
        if input_id=="ASTOCK-013":
            report[policy_key].update(headers="original Chrome/126 User-Agent and finance.sina.com.cn Referer",
                original_report_min_interval_seconds=6,empty_page_attempts=2,encoding="GBK")
    if is_em_events or is_lpr or is_cb:
        import requests
        import urllib3
        report["transport_dependency"] = {"requests": requests.__version__, "urllib3": urllib3.__version__}
        code_version = hashlib.sha256((code_version + requests.__version__ + urllib3.__version__).encode()).hexdigest()
        report["code_version"] = code_version
        report["event_transport_policy"] = {"source_contract": "successful runnable V3.9 test loader",
            "headers": "requests Session defaults", "trust_env": True, "timeout_seconds": 20,
            "retry_total": 3, "retry_connect": 3, "backoff_factor": 0.6,
            "status_forcelist": [429, 500, 502, 503, 504], "minimum_interval_seconds": 1,
            "conditional_wait_jitter_seconds": [0.1, 0.5], "outer_capture_interval_seconds": 3,
            "physical_retry_count_observable": False}
    if is_reportapi:
        import requests
        import urllib3
        report["transport_dependency"] = {"requests":requests.__version__, "urllib3":urllib3.__version__}
        code_version = hashlib.sha256((code_version+requests.__version__+urllib3.__version__).encode()).hexdigest()
        report["code_version"] = code_version
        report["reportapi_transport_policy"] = {"source_contract":"successful original rate-limited source Session",
            "headers":"original Chrome/154 User-Agent, Accept/Accept-Language and data.eastmoney.com Referer", "timeout_seconds":30,
            "trust_env":True, "allow_redirects":True, "retry_total":2, "minimum_backoff_seconds":5,
            "host_pause":"403/429 immediately; two consecutive transport or server errors", "outer_capture_interval_seconds":3,
            "page_size":100, "page_pause_seconds":0.25, "cache_ignored_query_parameters":["_"], "source_fallback":False,
            "physical_retry_count_observable":False}
    is_stock_pool = input_id in {"ASTOCK-045", "ASTOCK-046", "ASTOCK-047", "ASTOCK-048", "ASTOCK-050"}
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
    evidence_root = Path(evidence_root) if evidence_root else (paths["raw_root"] if runtime else project_root / "provider_validation/results")
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
            for name in (("query_stock_basic", "login", "logout") if input_id == 'ASTOCK-044' else
                         ("query_all_stock", "query_stock_industry", "login", "logout")):
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
                report["sdk_dependency"]["dependencies"].append({"name": name, "path": relative_file(sdk_ref.path),
                    "sha256": sdk_ref.content_hash, "original_source_sha256": digest, "redacted": snapshot != source})
                code_version = hashlib.sha256((code_version + name + digest).encode()).hexdigest()
            code_version = hashlib.sha256((code_version + report["sdk_dependency"]["version"]).encode()).hexdigest()
            report["code_version"] = code_version
            report["evidence_representation"] = "SDK-decoded ResultSet fields and rows; BaoStock TCP wire bytes are not exposed"
            report["request_limit_enforcement"] = "sdk_query_boundary_only"
        sdk_functions = {"ASTOCK-087": "stock_notice_report", "ASTOCK-014": "stock_board_concept_name_ths", "ASTOCK-037-profile": "stock_profile_cninfo",
                         "ASTOCK-037-events": "stock_gsrl_gsdt_em",
                         "ASTOCK-031":"stock_news_em", "ASTOCK-069":"stock_zh_index_value_csindex", "ASTOCK-039":"stock_financial_report_sina", "ASTOCK-051":"stock_changes_em", "ASTOCK-055":"option_risk_indicator_sse",
                         "ASTOCK-067":"index_stock_cons_csindex", "ASTOCK-068":"index_stock_cons_weight_csindex", "ASTOCK-085":"stock_lhb_detail_daily_sina",
                         "ASTOCK-019": "stock_lhb_stock_detail_em", "ASTOCK-020": "stock_lhb_detail_em", "ASTOCK-021": "stock_restricted_release_detail_em",
                         "ASTOCK-006": "stock_zh_a_daily", "ASTOCK-045": "stock_zt_pool_em", "ASTOCK-070": "tool_trade_date_hist_sina",
                         "ASTOCK-032": "stock_info_global_cls", "ASTOCK-033": "stock_info_global_sina",
                         "ASTOCK-061": "macro_china_shrzgm", "ASTOCK-062": "macro_china_pmi",
                         "ASTOCK-046": "stock_zt_pool_zbgc_em", "ASTOCK-047": "stock_zt_pool_dtgc_em",
                         "ASTOCK-048": "stock_zt_pool_previous_em", "ASTOCK-050": "stock_zt_pool_strong_em",
                         "ASTOCK-026": "stock_zh_a_gdhs_detail_em", "ASTOCK-027": "stock_fhps_detail_em",
                         "ASTOCK-028": "stock_individual_fund_flow",
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
            sdk_snapshot = re.sub(rb'''(?i)(["'](?:ut|token|access_token|api_key|password|secret|cookie)["']\s*:\s*["'])[^"']*(["'])''',
                                  rb'\1<redacted>\2', sdk_source)
            sdk_ref = raw_store.write_bytes(sdk_snapshot, dataset="source_code", provider=contract.provider,
                endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="sdk-function", content_addressed=True)
            report["sdk_dependency"] = {"version": getattr(provider.client, "__version__", "injected_fixture"),
                "function": function.__name__, "source_path": relative_file(sdk_ref.path), "sha256": sdk_ref.content_hash,
                "original_source_sha256": sdk_source_hash, "redacted": sdk_snapshot != sdk_source}
            code_version = hashlib.sha256((code_version + sdk_source_hash + report["sdk_dependency"]["version"]).encode()).hexdigest()
            if is_sdk_news or is_sdk_macro or is_factor or is_market_events or is_seats or is_source_sdk:
                import requests
                import urllib3
                report["transport_dependency"] = {"requests": requests.__version__, "urllib3": urllib3.__version__}
                if input_id in {'ASTOCK-031','ASTOCK-069'}:
                    import pandas
                    report['transport_dependency']['pandas']=pandas.__version__
                    if input_id=='ASTOCK-031':
                        import curl_cffi
                        report['transport_dependency']['curl_cffi']=curl_cffi.__version__
                    import json
                    code_version=hashlib.sha256((code_version+json.dumps(report['transport_dependency'],sort_keys=True)).encode()).hexdigest()
                code_version = hashlib.sha256((code_version + requests.__version__ + urllib3.__version__).encode()).hexdigest()
                namespace = getattr(function, "__globals__", {})
                report["sdk_dependency"]["dependencies"] = []
                if is_factor:
                    constants = {name: namespace[name] for name in ("zh_sina_a_stock_qfq_url", "zh_sina_a_stock_hfq_url") if name in namespace}
                    import json
                    digest = hashlib.sha256(json.dumps(constants, sort_keys=True).encode()).hexdigest()
                    report["sdk_dependency"].update(url_constants=constants, url_constants_sha256=digest)
                    code_version = hashlib.sha256((code_version + digest).encode()).hexdigest()
                if input_id == "ASTOCK-032":
                    sdk_headers = sanitized_metadata(namespace.get("headers", {}))
                    import json
                    header_hash = hashlib.sha256(json.dumps(sdk_headers,sort_keys=True).encode()).hexdigest()
                    report["sdk_dependency"]["headers"] = sdk_headers
                    report["sdk_dependency"]["headers_sha256"] = header_hash
                    code_version = hashlib.sha256((code_version + header_hash).encode()).hexdigest()
                    helper = namespace.get("make_request_with_retry_json")
                    if helper is not None:
                        source = inspect.getsource(helper).encode("utf-8")
                        ref = raw_store.write_bytes(source, dataset="source_code", provider=contract.provider,
                            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="sdk-retry-helper", content_addressed=True)
                        report["sdk_dependency"]["dependencies"].append({"name": helper.__name__,
                            "path": relative_file(ref.path), "sha256": ref.content_hash})
                        code_version = hashlib.sha256((code_version + ref.content_hash).encode()).hexdigest()
                report["sdk_news_transport_policy"] = {"source_contract": "successful rate-limited original SDK probe",
                    "timeout_seconds": None, "trust_env": True, "allow_redirects": True,
                    "headers": "original Chrome/114 User-Agent" if input_id == "ASTOCK-032" else "requests defaults",
                    "adapter_retry_total": 2, "adapter_backoff_min_seconds": 5,
                    "status_forcelist": [500, 502, 503, 504], "outer_capture_interval_seconds": 3,
                    "sdk_outer_attempts": 10 if input_id == "ASTOCK-032" else 1,
                    "sdk_outer_delays_seconds": [1,2,4,8,16,32,64,128,256] if input_id == "ASTOCK-032" else [],
                    "host_pause": "403/429 immediately; two consecutive transport or server errors",
                    "physical_retry_count_observable": False}
                if is_market_events:
                    report["market_event_transport_policy"] = report.pop("sdk_news_transport_policy")
                    report["market_event_transport_policy"].update(response_guard="after exact retention, before SDK positional parsing", source_fallback=False)
                if is_seats:
                    report["seats_sdk_transport_policy"] = report.pop("sdk_news_transport_policy")
                    report["seats_sdk_transport_policy"].update(response_guard="after exact retention, before SDK positional parsing", source_fallback=False, implicit_date_selection=False)
                if is_source_sdk:
                    report["source_sdk_transport_policy"] = report.pop("sdk_news_transport_policy")
                    if input_id in {'ASTOCK-031','ASTOCK-069'}:
                        report['source_sdk_transport_policy']={'source_contract':'original native SDK transport',
                            'transport':'curl_cffi' if input_id=='ASTOCK-031' else 'pandas urllib',
                            'capture_boundary':'SDK application response before parsing; native transport delegated unchanged',
                            'requests_retry_policy_applies':False,'source_fallback_enabled':False,'implicit_date_selection':False}
                    report["source_sdk_transport_policy"].update(source_fallback=False,implicit_date_selection=False,
                        source_scope="explicit source SDK and parameters; exact original response retained before parsing")
                if is_sdk_macro:
                    del report["sdk_news_transport_policy"]
                    report["macro_sdk_transport_policy"] = {"source_contract":"successful rate-limited original SDK probe",
                        "timeout_seconds":None,"trust_env":True,"allow_redirects":True,"headers":"requests defaults",
                        "method":"POST" if input_id=="ASTOCK-061" else "GET",
                        "https_adapter":"original SDK TLSAdapter, retry_total=0" if input_id=="ASTOCK-061" else "original probe conservative retry_total=2, minimum backoff=5",
                        "outer_capture_interval_seconds":3,"sdk_outer_attempts":1,
                        "host_pause":"403/429 immediately; two consecutive transport or server errors",
                        "physical_retry_count_observable":False}
                    dependency=namespace.get("TLSAdapter") if input_id=="ASTOCK-061" else None
                    if dependency is not None:
                        body=inspect.getsource(dependency).encode("utf-8")
                        ref=raw_store.write_bytes(body,dataset="source_code",provider=contract.provider,endpoint=contract.endpoint,
                            fetched_at=datetime.now(timezone.utc),attempt_id="sdk-tls-adapter",content_addressed=True)
                        report["sdk_dependency"]["dependencies"].append({"name":"TLSAdapter","path":relative_file(ref.path),"sha256":ref.content_hash})
                        code_version=hashlib.sha256((code_version+ref.content_hash).encode()).hexdigest()
                if is_factor:
                    report["factor_sdk_transport_policy"] = report.pop("sdk_news_transport_policy")
                    report["factor_sdk_transport_policy"].update(method="GET", literal_gate="after exact raw retention, before original SDK eval")
            if input_id.startswith("SDA-BOARD-") or input_id in {'ASTOCK-014', 'ASTOCK-037-profile', 'ASTOCK-087'}:
                namespace = getattr(function, "__globals__", {})
                dependencies = []
                for name in ("_get_stock_board_industry_name_ths", "_get_stock_board_concept_name_ths", "_get_file_content_ths", "get_ths_js"):
                    dependency = namespace.get(name)
                    if dependency is not None:
                        dependencies.append((name, inspect.getsource(dependency).encode("utf-8")))
                if namespace.get("get_ths_js"):
                    dependencies.append(("ths.js", Path(namespace["get_ths_js"]("ths.js")).read_bytes()))
                if input_id == 'ASTOCK-014':
                    helper = namespace['_get_stock_board_concept_name_ths']
                    child = helper.__wrapped__.__globals__['__stock_board_concept_summary_ths']
                    dependencies.append(('__stock_board_concept_summary_ths', inspect.getsource(child).encode('utf-8')))
                    dependencies.append(('ths.js', helper.__wrapped__.__globals__['_get_file_content_ths']('ths.js').encode('utf-8')))
                if input_id == 'ASTOCK-037-profile':
                    dependencies.append(('cninfo.js', namespace['_get_file_content_ths']('cninfo.js').encode('utf-8')))
                if input_id == 'ASTOCK-087':
                    dependencies.append(('_stock_notice_report', inspect.getsource(namespace['_stock_notice_report']).encode('utf-8')))
                report["sdk_dependency"]["dependencies"] = []
                for name, body in dependencies:
                    digest = hashlib.sha256(body).hexdigest()
                    snapshot = re.sub(rb'''(?i)(["'](?:ut|token|access_token|api_key|password|secret)["']\s*:\s*["'])[^"']*(["'])''',
                                      rb'\1<redacted>\2', body)
                    ref = raw_store.write_bytes(snapshot, dataset="source_code", provider=contract.provider, endpoint=contract.endpoint,
                                               fetched_at=datetime.now(timezone.utc), attempt_id="sdk-dependency", content_addressed=True)
                    report["sdk_dependency"]["dependencies"].append({"name": name, "path": relative_file(ref.path),
                        "sha256": ref.content_hash, "original_source_sha256": digest, "redacted": snapshot != body})
                    code_version = hashlib.sha256((code_version + name + digest).encode()).hexdigest()
            report["code_version"] = code_version
        from contextlib import ExitStack
        actual_code = None
        with ExitStack() as stack:
            runtime_parameters = dict(parameters)
            if is_em_events or is_lpr or is_cb:
                stack.callback(provider.close_event_session)
            if is_reportapi:
                stack.callback(provider.close_report_session)
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
                    trade_date=parameters.get("trade_date"), pacer=pacer, interval_seconds=max(3, contract.request_interval_seconds),
                    max_age_seconds=0 if force_fetch else profile.refresh_interval_seconds or 86400, client=client,
                    query_requests={'query_stock_basic': {'code_name': 'ST'}} if input_id == 'ASTOCK-044' else None))
                provider.client = sdk_client
                runtime_parameters["raw_archive"] = sdk_archive
            else:
                response_validator = None
                if is_factor:
                    from ..providers.sina.daily import validate_adjustment_response
                    response_validator = validate_adjustment_response
                if is_market_events:
                    from ..providers.eastmoney.financial import validate_market_event_response
                    def response_validator(response):
                        validate_market_event_response(response.content, response.url, response.status_code,
                            endpoint=contract.endpoint, start=parameters.get("start", parameters.get("date")),
                            end=parameters.get("end", parameters.get("date")))
                if is_seats:
                    from ..providers.eastmoney.financial import validate_seat_response
                    from ..providers.eastmoney.realtime import history_stock_identity
                    def response_validator(response):
                        validate_seat_response(response.content, response.url, response.status_code,
                            code=history_stock_identity(parameters["code"])[0], date=parameters["date"])
                response_events = stack.enter_context(captured_requests(raw_store, provider=contract.provider, endpoint=contract.endpoint,
                    scope={"input_id": input_id, "parameters": normalized_context}, code_version=code_version, pacer=pacer,
                    replay_manifest=replay_manifest if mode == "replay" else None,
                    evidence_roots=(evidence_root, output_root), max_age_seconds=0 if force_fetch else profile.refresh_interval_seconds or 86400,
                    sdk_retry_policy=is_stock_pool or is_sdk_news or is_sdk_macro or is_factor or is_market_events or is_reports_seats or is_source_sdk or input_id in {"ASTOCK-001", "ASTOCK-045", "ASTOCK-070", "ASTOCK-026", "ASTOCK-027", "ASTOCK-028"},
                    probe_host_pause=is_sdk_news or is_sdk_macro or is_factor or is_market_events or is_reports_seats or is_source_sdk,require_empty_post_body=input_id=="ASTOCK-061",response_validator=response_validator,
                    cache_ignored_query_parameters=("_",) if is_reportapi else (),
                     native_transport='curl_cffi' if input_id=='ASTOCK-031' else 'pandas_urllib' if input_id=='ASTOCK-069' else None,
                     match_request_body=is_bse_catalog))
            if is_source_sdk:
                def retained_responses():
                    for event in response_events:
                        if event.get("outcome")=="response" and event["status_code"]==200:
                            yield {"url":event["url"],"body":RawObjectStore.read_response(raw_store.root/"manifest.ndjson",event),
                                   "encoding":event.get("encoding")}
                runtime_parameters["source_responses"]=retained_responses
            if is_reportapi and mode == "replay":
                from ..providers.eastmoney.financial import reportapi_replay_clock
                from ..providers.eastmoney.realtime import history_stock_identity
                clock = stack.enter_context(reportapi_replay_clock(provider.fetch_report_list.__func__, replay_manifest,
                    code=history_stock_identity(parameters["code"])[0], start=parameters["start"], end=parameters["end"]))
                report["replay_cache_busters"] = clock
                report["replay_clock_semantics"] = "matched original per-page cache-busters only; source dates and process clock unchanged"
            if is_factor:
                from ..providers.sina.daily import parse_adjustment_payload
                def factor_payloads():
                    for event in response_events:
                        body = RawObjectStore.read_response(raw_store.root / "manifest.ndjson", event)
                        yield parse_adjustment_payload(body, event["url"], event["status_code"])
                runtime_parameters["source_payloads"] = factor_payloads
            if is_cb and mode == "replay":
                from zoneinfo import ZoneInfo
                def original_classification_day():
                    successful = [event for event in response_events if event.get("outcome") == "response"]
                    if not successful:
                        raise ValueError("CB replay requires original capture-time evidence")
                    capture_time = max(datetime.fromisoformat(event["source_ref"]["fetched_at_utc"]) for event in successful)
                    return capture_time.astimezone(ZoneInfo("Asia/Shanghai")).date()
                runtime_parameters["replay_reference_date"] = original_classification_day
                report["replay_clock_semantics"] = "actual matched response capture day in China timezone for classification; not a historical quote query"
            if is_em_history or is_stock_pool or is_sdk_news or is_sdk_macro or is_market_events or is_seats:
                from ..providers.contracts import EndpointContract, HttpResponse
                def retained_payloads():
                    for event in response_events:
                        if event.get("outcome") == "response" and (not (is_sdk_news or is_sdk_macro or is_market_events or is_seats) or event["status_code"] == 200):
                            body = RawObjectStore.read_response(raw_store.root / "manifest.ndjson", event)
                            yield EndpointContract(frozenset()).parse_json(HttpResponse(
                                event["status_code"], event.get("response_headers", {}), body))
                runtime_parameters["source_payloads"] = retained_payloads
                if mode == "replay" and input_id == "ASTOCK-032":
                    from ..providers.cls.news import telegraph_replay_clock
                    cutoff = stack.enter_context(telegraph_replay_clock(function, replay_manifest))
                    report["replay_query_cutoff"] = cutoff
                    report["replay_clock_semantics"] = "original archived SDK query cutoff only; live and process clocks unchanged"
                if mode == "replay" and is_stock_pool:
                    from ..providers.eastmoney.limit_pool import pool_replay_clock
                    endpoint = {"ASTOCK-045": "getTopicZTPool", "ASTOCK-046": "getTopicZBPool", "ASTOCK-047": "getTopicDTPool",
                                "ASTOCK-048": "getYesterdayZTPool", "ASTOCK-050": "getTopicQSPool"}[input_id]
                    stack.enter_context(pool_replay_clock(function, replay_manifest, endpoint))
                    report["replay_clock_semantics"] = "original capture clock for SDK recency guard only; live clock unchanged"
                if mode == "replay" and input_id == "ASTOCK-028":
                    from ..providers.eastmoney.realtime import history_replay_clock
                    stack.enter_context(history_replay_clock(function, replay_manifest, parameters["code"]))
                    report["replay_clock_semantics"] = "SDK cache-buster clock from matching archived request; live clock unchanged"
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
        if runtime:
            raw_manifest = raw_store.root / "manifest.ndjson"
            attempt = replace(attempt, raw_object_path=str(raw_manifest.resolve()),
                raw_content_hash=hashlib.sha256(raw_manifest.read_bytes()).hexdigest()).transition(AttemptStatus.RAW_COMMITTED)
            metadata = MetadataStore(paths["metadata_path"])
            try:
                metadata.save_attempt(attempt, updated_at=datetime.now(timezone.utc))
            finally:
                metadata.close()
        report["live_http_calls"] = sum(event["mode"] == "live" and event.get("event") == "http_response" for event in response_events)
        source_rows = getattr(fetched, "source_rows", None) or fetched.rows
        source_ref = write_result(_json_value(source_rows), dataset="source_rows", provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="source-rows")
        report["source_rows"] = {"path": relative_file(source_ref.path),
                                 "sha256": source_ref.content_hash, "row_count": len(source_rows)}
        if is_stock_pool:
            report.update(source_quote_date=fetched.mapping_context["source_quote_date"].isoformat(),
                source_total_count=fetched.mapping_context["source_total_count"],
                returned_window={"first": parameters["date"].isoformat(), "last": parameters["date"].isoformat()},
                coverage_basis="source tc and returned SDK rows for requested qdate; not independent whole-market proof",
                universe_completeness_verified=False)
        mapping_rows = fetched.rows if input_id == 'ASTOCK-044' or is_em_history or is_lpr or is_news or is_reports_calendar or is_sdk_news or is_sdk_macro or is_factor or is_market_events or is_reports_seats or is_source_extension or is_repo_rate or is_cb or is_sina_futures else source_rows
        if input_id == 'ASTOCK-044' or is_news or is_reports_calendar or is_sdk_news or is_sdk_macro or is_factor or is_market_events or is_reports_seats or is_source_extension or is_repo_rate or is_cb or is_sina_futures:
            parsed_ref = write_result(_json_value(mapping_rows), dataset="parsed_rows", provider=contract.provider,
                endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="parsed-rows")
            report["parsed_rows"] = {"path":relative_file(parsed_ref.path),
                "sha256":parsed_ref.content_hash,"row_count":len(mapping_rows),"code_version":code_version,
                "source_response_hashes":[event["body_sha256"] for event in response_events]}
        if input_id == "SDA-BOARD-005":
            excluded_ref = write_result(_json_value(fetched.excluded_rows), dataset="excluded_rows", provider=contract.provider,
                endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="excluded-rows")
            report["excluded_rows"] = {"path": relative_file(excluded_ref.path), "sha256": excluded_ref.content_hash,
                "row_count": len(fetched.excluded_rows), "code_version": code_version,
                "source_response_hashes": [event["body_sha256"] for event in response_events]}
            report.update(source_metadata=_json_value(fetched.mapping_context), universe_completeness_verified=False,
                asset_type_counts={kind: sum(row["asset_type"] == kind for row in source_rows) for kind in ("stock", "etf")})
        if is_factor:
            excluded_ref = write_result(_json_value(fetched.excluded_rows), dataset="excluded_rows", provider=contract.provider,
                endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="excluded-rows")
            report["excluded_rows"] = {"path":relative_file(excluded_ref.path), "sha256":excluded_ref.content_hash,"row_count":len(fetched.excluded_rows)}
            report.update(original_row_count=len(source_rows), selected_row_count=len(mapping_rows), source_symbol=fetched.mapping_context["source_symbol"],
                factor_kind=parameters["kind"], selection_policy="original requests retain both histories; config.kind selects one candidate series",
                returned_window={"first":min(row["factor_date"] for row in mapping_rows),"last":max(row["factor_date"] for row in mapping_rows),"meaning":"source factor date labels; 1900 baseline is not a trading date"},
                coverage_basis="source-reported factor rows, not independently certified economic events or pricing semantics",universe_completeness_verified=False)
        if input_id == "ASTOCK-075":
            excluded_ref = write_result(_json_value(fetched.excluded_rows), dataset="excluded_rows",
                provider=contract.provider, endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="excluded-rows")
            report["excluded_rows"] = {"path":relative_file(excluded_ref.path),"sha256":excluded_ref.content_hash,"row_count":len(fetched.excluded_rows)}
            report.update(original_row_count=len(source_rows),selected_row_count=len(mapping_rows),
                selection_policy="original local date-window filter; complete returned source rows retained",
                series_identity=fetched.mapping_context["series_identity"],source_contract=fetched.mapping_context["source_contract"])
        if input_id=="ASTOCK-066":
            excluded_ref=write_result(_json_value(fetched.excluded_rows),dataset="excluded_rows",provider=contract.provider,
                endpoint=contract.endpoint,fetched_at=datetime.now(timezone.utc),attempt_id="excluded-rows")
            report["excluded_rows"]={"path":relative_file(excluded_ref.path),"sha256":excluded_ref.content_hash,"row_count":len(fetched.excluded_rows)}
        if is_cb:
            excluded_ref = write_result(_json_value(fetched.excluded_rows), dataset="excluded_rows",
                provider=contract.provider, endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="excluded-rows")
            report["excluded_rows"] = {"path": relative_file(excluded_ref.path),
                "sha256": excluded_ref.content_hash, "row_count": len(fetched.excluded_rows)}
            report.update(original_row_count=len(source_rows), selected_row_count=len(mapping_rows),
                selection_policy="original CB delisted exclusion unless include_delisted is true",
                classification_reference_date=fetched.mapping_context["reference_date"],
                source_total_count=fetched.mapping_context["source_total_count"],source_page_count=fetched.mapping_context["source_page_count"],
                requested_limit=20000,result_limited=len(source_rows)<fetched.mapping_context["source_total_count"],
                returned_window={"first":fetched.mapping_context["reference_date"],"last":fetched.mapping_context["reference_date"],
                    "meaning":"local status classification day; quotes remain the source response snapshot"},
                coverage_basis="selected source-reported CB list; not independent complete market coverage",
                universe_completeness_verified=False)
        if is_em_history or is_lpr:
            excluded_ref = write_result(_json_value(fetched.excluded_rows), dataset="excluded_rows",
                provider=contract.provider, endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="excluded-rows")
            report["excluded_rows"] = {"path": relative_file(excluded_ref.path),
                "sha256": excluded_ref.content_hash, "row_count": len(fetched.excluded_rows)}
            source_date = {"ASTOCK-026": "股东户数统计截止日", "ASTOCK-027": "报告期", "ASTOCK-028": "日期", "ASTOCK-065":"TRADE_DATE"}[input_id]
            dates = [str(row[source_date])[:10] if is_lpr else str(row[source_date]) for row in (mapping_rows if is_lpr else source_rows)]
            report.update(returned_window={"first": min(dates) if dates else None, "last": max(dates) if dates else None},
                original_row_count=len(source_rows), selected_row_count=len(mapping_rows),
                coverage_basis="selected returned SDK rows; not independently complete history or market coverage",
                universe_completeness_verified=False,
                selection_policy="original script LPR1Y non-null selection; baseline rows retained separately" if is_lpr else "implemented dividend events only; other plans retained in source evidence" if input_id == "ASTOCK-027" else "all returned rows")
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
            report["coverage_scope"] = "SH/SZ selected stocks and optional ETFs from the SDK response; not an independent market census"
            if input_id == 'ASTOCK-044':
                report.update(original_row_count=len(source_rows), selected_row_count=len(mapping_rows),
                    coverage_complete=True, coverage_basis='all rows selected by original active stock ST-name rule from returned SDK search',
                    source_fallback_enabled=False, implicit_date_selection=False, universe_completeness_verified=False,
                    source_metadata=_json_value(fetched.mapping_context))
        if is_tencent_snapshot:
            from zoneinfo import ZoneInfo
            quote_dates = [datetime.strptime(row["datetime"], "%Y%m%d%H%M%S").date() for row in source_rows]
            report.update(missing_symbols=list(fetched.missing_symbols), coverage_complete=not fetched.missing_symbols,
                          returned_window={"first": min(row["datetime"] for row in source_rows),
                                           "last": max(row["datetime"] for row in source_rows)},
                          coverage_scope="explicit requested stock list; no independent whole-market completeness proof")
            if any(day != parameters["as_of"].astimezone(ZoneInfo("Asia/Shanghai")).date() for day in quote_dates):
                raise NormalizationError("source quote date differs from requested as_of date; snapshot cannot query history")
        valid_empty = (is_em_events or is_cb or input_id=="ASTOCK-013") and fetched.empty_is_valid
        if (not source_rows or not mapping_rows) and not valid_empty:
            raise NormalizationError("temporary empty input; not certified as a valid empty dataset")
        mapping_context = {"provider": contract.provider}
        mapping_context.update(getattr(fetched, "mapping_context", {}))
        if is_bse_catalog or is_baostock or input_id.startswith("SDA-BOARD-") or is_tencent_snapshot or is_em_history or is_stock_pool or is_em_events or is_lpr or is_news or is_reports_calendar or is_sdk_news or is_sdk_macro or is_factor or is_market_events or is_reports_seats or is_source_extension or is_repo_rate or is_cb or is_sina_futures:
            successful = [event for event in response_events if event.get("outcome") == "response" and (not (is_sdk_news or is_sdk_macro or is_market_events or is_reports_seats) or event["status_code"] == 200)]
            if is_bse_catalog:
                successful = [event for event in successful if event.get("method") == "POST" and event.get("status_code") == 200]
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
            if is_bse_catalog:
                from zoneinfo import ZoneInfo
                capture_days = {stamp.astimezone(ZoneInfo("Asia/Shanghai")).date() for stamp in source_times}
                if len(capture_days) != 1:
                    raise NormalizationError("BSE catalog pages crossed capture days; retry a complete snapshot")
                mapping_context["source_capture_date"] = capture_days.pop()
                report.update(source_metadata=_json_value(fetched.mapping_context),
                    source_total_count=fetched.mapping_context["source_total_count"],
                    source_page_count=fetched.mapping_context["source_page_count"],
                    pagination_completeness_verified=True, universe_completeness_verified=False,
                    returned_window={"first": mapping_context["source_capture_date"].isoformat(),
                                     "last": mapping_context["source_capture_date"].isoformat(),
                                     "meaning": "catalog capture day; not a historical security query"})
            report["source_capture_window"] = {"first": min(source_times).isoformat(), "last": max(source_times).isoformat(),
                                               "meaning": "source response capture times; not row-level market timestamps"}
            if not is_bse_catalog and not is_tencent_snapshot and not is_em_history and not is_stock_pool and not is_em_events and not is_lpr and not is_news and not is_reports_calendar and not is_sdk_news and not is_sdk_macro and not is_factor and not is_market_events and not is_reports_seats and not is_source_extension and not is_repo_rate and not is_cb and not is_sina_futures:
                report["returned_window"] = {"first": fetched.returned_first_key, "last": fetched.returned_last_key}
                report["source_units"] = list(fetched.units)
                report["coverage_basis"] = "returned SDK rows; not an independently verified market universe"
        if is_em_events:
            date_field = fetched.mapping_context.get("returned_date_field", "NOTICE_DATE")
            notice_days = [str(row[date_field])[:10] for row in source_rows if row.get(date_field)]
            report.update(source_total_count=fetched.mapping_context["source_total_count"],
                source_page_count=fetched.mapping_context["source_page_count"],
                requested_limit=fetched.mapping_context["requested_limit"], valid_empty_dataset=valid_empty,
                result_limited=fetched.mapping_context["source_total_count"] is not None and len(source_rows) < fetched.mapping_context["source_total_count"],
                coverage_basis="bounded latest source events; denominator is returned rows, not whole-market coverage",
                universe_completeness_verified=False,
                returned_window={"first": min(notice_days) if notice_days else None,
                                 "last": max(notice_days) if notice_days else None, "field": date_field})
        if is_lpr:
            report.update(source_total_count=fetched.mapping_context["source_total_count"],
                source_page_count=fetched.mapping_context["source_page_count"],requested_limit=5000,
                result_limited=len(source_rows)<fetched.mapping_context["source_total_count"],
                coverage_basis="source-reported history within 5000-row original script limit; not independent completeness")
        if is_news:
            dates = [row["time"] for row in mapping_rows] if input_id == "ASTOCK-034" else [row["date"] for row in mapping_rows]
            report.update(returned_window={"first": min(dates), "last": max(dates)},
                coverage_basis="returned news items for requested channel/page or broadcast date; not independent complete history",
                universe_completeness_verified=False)
            if input_id == "ASTOCK-034":
                report.update(next_cursor=fetched.mapping_context["next_cursor"], requested_limit=parameters["limit"],
                    channel=parameters["channel"], pagination_completeness_verified=False)
            else:
                report.update(broadcast_date=fetched.mapping_context["broadcast_date"], article_content_verified=False,
                    content_requests=0, request_date_semantics="calendar broadcast date; not an exchange trading date")
        if is_sdk_news:
            dates = [row["published_at"] if input_id == "ASTOCK-032" else row["时间"] for row in mapping_rows]
            report.update(returned_window={"first":min(dates),"last":max(dates)}, requested_limit=20,
                coverage_basis="latest returned 20 source news items; not complete news history or market coverage",
                universe_completeness_verified=False, pagination_completeness_verified=False)
        if is_sdk_macro:
            periods=[row["statistical_month"] for row in mapping_rows]
            report.update(returned_window={"first":min(periods),"last":max(periods),"meaning":"statistical months, not publication dates"},
                coverage_basis="returned source monthly history; no independent current or complete history certification",
                universe_completeness_verified=False,publication_time_available=False)
        if is_market_events:
            dates = [row[fetched.mapping_context["date_field"]].isoformat() for row in mapping_rows]
            report.update(returned_window={"first": min(dates), "last": max(dates)},
                requested_window={"first": parameters.get("start", parameters.get("date")).isoformat(),
                    "last": parameters.get("end", parameters.get("date")).isoformat()},
                source_total_count=fetched.mapping_context["source_total_count"], source_page_count=fetched.mapping_context["source_page_count"],
                source_report=fetched.mapping_context["source_report"], coverage_complete=True,
                coverage_basis="all pages of the source-reported market event window; no independent market census",
                universe_completeness_verified=False, source_fallback_enabled=False, implicit_date_selection=False)
        if is_reports_seats:
            dates = [str(row["publishDate"])[:10] if is_reportapi else row["trade_date"].isoformat() for row in mapping_rows]
            report.update(returned_window={"first":min(dates), "last":max(dates)}, source_fallback_enabled=False,
                implicit_date_selection=False, universe_completeness_verified=False)
            if is_reportapi:
                report.update(source_total_count=fetched.mapping_context["source_total_count"], source_page_count=fetched.mapping_context["source_page_count"],
                    retrieved_pages=fetched.mapping_context["retrieved_pages"], result_limited=fetched.mapping_context["result_limited"],
                    coverage_complete=not fetched.mapping_context["result_limited"], requested_page_limit=parameters["pages"],
                    coverage_basis="bounded report pages for explicit stock/window; source reported hits only, not independently complete research coverage",
                    requested_window={"first":parameters["start"].isoformat(), "last":parameters["end"].isoformat()},
                    pdf_requests=0, pdf_download_verified=False, publication_time_precision_verified=False)
            else:
                report.update(side_counts=fetched.mapping_context["side_counts"], requested_window={"first":parameters["date"].isoformat(), "last":parameters["date"].isoformat()},
                    source_symbol=fetched.mapping_context["source_symbol"], coverage_complete=True, date_discovery_requests=0,
                    source_row_number_semantics="original SDK row order after type sort; not monetary rank or permanent department identity",
                    coverage_basis="two source-reported nonempty single pages for explicit stock/day; no independent seat census")
        if is_actual_data:
            dates = [str(row['交易日' if input_id == 'ASTOCK-037-events' else '公告日期']) for row in mapping_rows] if input_id in {'ASTOCK-037-events','ASTOCK-087'} else []
            report.update(returned_window={'first': min(dates) if dates else None, 'last': max(dates) if dates else None,
                'meaning': 'source event date labels' if dates else 'no source effective date; capture time retained separately'},
                source_fallback_enabled=False, implicit_date_selection=False, universe_completeness_verified=False,
                original_row_count=len(source_rows), selected_row_count=len(mapping_rows),
                source_metadata=_json_value(fetched.mapping_context),
                coverage_basis='returned actual source records; no independent universe proof')
        elif is_source_extension:
            key = '发布时间' if input_id=='ASTOCK-031' else '报告日' if input_id=='ASTOCK-039' else '时间' if input_id=='ASTOCK-051' else 'TRADE_DATE' if input_id=='ASTOCK-055' else '日期' if input_id in {'ASTOCK-067','ASTOCK-068','ASTOCK-069'} else 'trade_date' if input_id=='ASTOCK-085' else 'date'
            days=[str(row[key]) for row in mapping_rows]
            window_meaning = ('source intraday clock labels; source date unavailable' if input_id=='ASTOCK-051'
                else 'financial report periods, not publication dates' if input_id=='ASTOCK-039'
                else 'source date labels within explicit request scope')
            report.update(returned_window={'first':min(days),'last':max(days),'meaning':window_meaning},source_fallback_enabled=False,
                implicit_date_selection=False,universe_completeness_verified=False,
                coverage_basis='returned original source records within explicit source scope; no independent universe proof',
                source_rows_representation='retained source fields or original parser rows; exact complete HTTP bytes are separate',
                source_metadata=_json_value(fetched.mapping_context))
        if is_reports_calendar:
            times=[row["date"] if input_id=="ASTOCK-013" else row["time"] for row in mapping_rows]
            report.update(returned_window={"first":min(times) if times else None,"last":max(times) if times else None},
                coverage_basis="returned latest report page or original seven-day calendar slices; not independent complete history",
                universe_completeness_verified=False)
            if input_id=="ASTOCK-013":
                report.update(page=parameters["page"],empty_page_attempts=fetched.mapping_context["empty_page_attempts"],valid_empty_dataset=valid_empty,
                    empty_validation_basis="original two-attempt explicit empty-page rule; source absence not independently certified",pagination_completeness_verified=False)
            else:
                report.update(requested_window={"first":parameters["start"].isoformat(),"last":parameters["end"].isoformat()},
                    original_row_count=len(source_rows),selected_row_count=len(mapping_rows),country=parameters.get("country"),
                    min_importance=parameters["min_importance"],calendar_time_precision="minute, original source parser")
        if is_repo_rate:
            dates = [row["date"] for row in mapping_rows]
            report.update(rate_kind=parameters["kind"], returned_window={"first":min(dates),"last":max(dates)},
                coverage_basis="returned fixing CSV dates; not independently complete history",
                universe_completeness_verified=False)
        if is_sina_futures:
            dates = [row["date"] if input_id == "ASTOCK-075" else row["datetime"] for row in mapping_rows]
            report.update(returned_window={"first":min(dates),"last":max(dates)},
                coverage_basis="returned source contracts and local date window; not independent exchange or market coverage",
                universe_completeness_verified=False,quote_freshness_verified=False)
            if input_id == "ASTOCK-074":
                report["requested_contracts"] = fetched.mapping_context["requested_contracts"]
            elif input_id == "ASTOCK-076":
                report["source_contract"] = fetched.mapping_context["source_contract"]
        if "date" in parameters:
            mapping_context["trade_date"] = parameters["date"]
        if input_id.startswith("ASTOCK-002"):
            from ..providers.tencent.daily import tencent_symbol
            symbol = tencent_symbol(parameters["code"])
            mapping_context.update(instrument_id=f"{'XSHG' if symbol.startswith('sh') else 'XSHE'}:{symbol[2:]}",
                                   adjustment="forward" if input_id.endswith("daily") else "none")
        rows = [Normalizer.normalize_fields(row, rule=rule, fields=schema_fields,
                                            context=mapping_context, allow_pending=True) for row in mapping_rows]
        keys = [tuple(row[name] for name in dataset["dataset"]["primary_key"]) for row in rows]
        if len(set(keys)) != len(keys):
            raise NormalizationError("duplicate dataset primary key")
        if contract.dataset in {"daily_bar", "minute_bar_5m", "industry_index_daily"}:
            if any(not (row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"]) for row in rows):
                raise NormalizationError("invalid OHLC ordering")
        projected = [{name: value for name, value in row.items() if name in selected} for row in rows]
        normalized_ref = write_result(_json_value(projected), dataset=contract.dataset, provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="mapped-rows")
        report["output"] = {"path": relative_file(normalized_ref.path),
                            "sha256": normalized_ref.content_hash, "row_count": len(projected),
                            "source_response_hashes": [event["body_sha256"] for event in response_events]}
        if runtime:
            from ..storage.parquet import write_normalized_rows
            parquet_path = directory / source_directory / "normalized.parquet"
            descriptor = write_normalized_rows(parquet_path, projected,
                {name: definition for name, definition in schema_fields.items() if name in selected})
            report["normalized_parquet"] = {"path": relative_file(parquet_path), **descriptor,
                "source_response_hashes": report["output"]["source_response_hashes"],
                "code_version": code_version, "normalization_version": rule["version"]}
            attempt = attempt.transition(AttemptStatus.NORMALIZED)
        report.update(status="candidate_complete", row_count=len(projected),
                      coverage_denominator=fetched.coverage_denominator if is_baostock or is_tencent_snapshot else len(mapping_rows),
                      first_key=_json_value(keys[0]) if keys else None, last_key=_json_value(keys[-1]) if keys else None)
    except Exception as exc:
        report.update(status="failed", failure_class=getattr(getattr(exc, "failure_class", None), "value", type(exc).__name__),
                      error=str(exc) if isinstance(exc, (ValueError, NormalizationError)) else type(exc).__name__)
    raw_manifest = raw_store.root / "manifest.ndjson"
    if raw_manifest.exists():
        import json
        all_events = [json.loads(line) for line in raw_manifest.read_text(encoding="utf-8").splitlines()]
        report["responses"] = [event for event in all_events if event.get("event") in {"http_response", "source_payload", "sdk_query_failure"}]
        if is_sdk_news or is_sdk_macro or is_factor or is_market_events or is_reports_seats or is_source_sdk:
            report["host_pause_events"] = [event for event in all_events if event.get("event") == "host_paused"]
        if is_baostock:
            report["sdk_session"] = [event for event in all_events if event.get("event") == "sdk_session"]
            report["sdk_derived"] = [event for event in all_events if event.get("event") == "sdk_derived"]
            report["live_sdk_calls"] = sum(event.get("mode") == "live" for event in report["responses"])
            report["sdk_query_count"] = len(report["responses"])
            report["sdk_dependency"]["live_sdk_executed"] = any(event.get("actual_session_called") for event in report["sdk_session"])
        report["raw_manifest"] = {"path": relative_file(raw_manifest),
                                  "sha256": hashlib.sha256(raw_manifest.read_bytes()).hexdigest()}
        report["live_http_calls"] = sum(event.get("mode") == "live" and event.get("event") == "http_response" for event in all_events)
    report_ref = write_result(report, dataset="input_report", provider=contract.provider, endpoint=contract.endpoint,
                                        fetched_at=datetime.now(timezone.utc), attempt_id="report")
    if runtime:
        from ..domain import AttemptStatus
        from ..storage.metadata import MetadataStore
        from ..worker.attempts import CollectionAttempt
        source_refs = {"raw_manifest": report.get("raw_manifest"),
            "source_response_hashes": [event.get("body_sha256") for event in report.get("responses", [])
                                       if event.get("body_sha256")],
            "sdk_source_snapshots": report.get("sdk_dependency", {}),
            "code_version": code_version}
        result_store.write_json(source_refs, dataset="raw_refs", provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="raw-refs",
            relative_path=source_directory / "raw_refs.json")
        quality = {name: report.get(name) for name in ("status", "failure_class", "error", "row_count",
            "coverage_denominator", "first_key", "last_key", "unverified_fields", "validation_time_utc")}
        quality.update(publication_permitted=False, eligible_for_production_routing=False,
            source_response_hashes=source_refs["source_response_hashes"], code_version=code_version,
            report={"path": "report.json", "sha256": report_ref.content_hash})
        quality_ref = result_store.write_json(quality, dataset="quality_report", provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="quality",
            relative_path="quality_report.json")
        manifest = {"version": 1, "task_id": task_id, "dataset": contract.dataset,
            "provider": contract.provider, "input_id": input_id, "status": report["status"],
            "scope_key": scope_key, "scope": sanitized_metadata(normalized_context),
            "partition_semantics": "canonical request scope; not a business-date partition",
            "publication_permitted": False, "canonical_refs": [],
            "raw_manifest": report.get("raw_manifest"), "output": report.get("normalized_parquet"),
            "report": {"path": "report.json", "sha256": report_ref.content_hash},
            "quality_report": {"path": "quality_report.json", "sha256": quality_ref.content_hash},
            "source_response_hashes": source_refs["source_response_hashes"],
            "code_version": code_version, "config_files": report["config_files"],
            "created_at": report["validation_time_utc"], "archive_condition": "published canonical refs and published metadata attempt"}
        result_store.write_json(manifest, dataset="task_manifest", provider=contract.provider,
            endpoint=contract.endpoint, fetched_at=datetime.now(timezone.utc), attempt_id="task",
            relative_path="manifest.json")
        now = datetime.now(timezone.utc)
        raw_ref = report.get("raw_manifest")
        if raw_ref:
            attempt = replace(attempt, raw_object_path=str(raw_manifest.resolve()), raw_content_hash=raw_ref["sha256"])
        if report["status"] == "candidate_complete":
            attempt = attempt.transition(AttemptStatus.VALIDATED)
        else:
            attempt = attempt.transition(AttemptStatus.TERMINAL_FAILED if attempt.status == AttemptStatus.FETCHING else AttemptStatus.QUARANTINED)
        metadata = MetadataStore(paths["metadata_path"])
        try:
            metadata.save_attempt(attempt, updated_at=now)
        finally:
            metadata.close()
    return {**report, "run_directory": str(directory.resolve()), "report_path": str(report_ref.path.resolve())}


def collect_task(*, definition, config_root, data_root=None, redo="resume", symbols=(),
                 mode="live", collector=None, collector_options=None, failure_hook=None):
    """One durable source-to-publication task, using the existing input executor."""
    import json
    import re
    from ..storage.integrity import file_hash, row_hash
    from ..storage.metadata import MetadataStore
    from ..storage.parquet import PartitionLock

    config_root = Path(config_root).resolve()
    paths = load_storage_paths(config_root, data_root=data_root)
    if redo not in {"full", "resume", "selected"} or mode not in {"replay", "live"}:
        raise ValueError("unsupported task execution mode")
    validation_root = (Path(__file__).resolve().parents[2] / "provider_validation").resolve()
    if paths["data_root"].is_relative_to(validation_root):
        raise ValueError("business task data must be outside provider_validation; use configured data storage")
    production_root = load_storage_paths(config_root)["data_root"]
    if mode == "replay" and (paths["data_root"].is_relative_to(production_root) or production_root.is_relative_to(paths["data_root"])):
        raise ValueError("task replay publication requires an isolated --data-root")
    task_id = definition["task_id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", task_id):
        raise ValueError("invalid task ID")
    dataset = definition["dataset"]
    if dataset not in {"security_master", "daily_bar"}:
        raise ValueError("only security_master and daily_bar tasks are supported")
    day = datetime.strptime(definition["trade_date"], "%Y-%m-%d").date()
    contracts = {item.input_id: item for item in load_input_capabilities(config_root / "providers.yaml")}
    units = _task_units(definition, paths)
    if not units or len({unit["key"] for unit in units}) != len(units):
        raise ValueError("task requires nonempty, unique source units")
    for unit in units:
        calendar = unit["context"].get("calendar", {})
        if "trading_dates" in calendar:
            calendar["trading_dates"] = [datetime.strptime(value, "%Y-%m-%d").date() if isinstance(value, str) else value
                                         for value in calendar["trading_dates"]]
        contract = contracts[unit["input_id"]]
        if contract.dataset != ("security_snapshot" if dataset == "security_master" else "daily_bar"):
            raise ValueError("input dataset does not match business task")
        contract.bind_parameters(unit["context"])
        if dataset == "daily_bar":
            if contract.request_shape != "single_symbol" or not unit.get("symbol"):
                raise ValueError("daily tasks require one explicit security per unit")
            request = unit["context"]["request"]
            if not (str(request.get("start_date")) <= day.isoformat() <= str(request.get("end_date"))):
                raise ValueError("daily source window must contain the task trading date")
        if mode == "live":
            # Catalog routes must pass evidence checks before each source call.
            # Other datasets remain blocked pending their own qualification.
            if dataset != "security_master":
                raise ValueError("live task publication is blocked: inputs have no formal routing qualification")
            if unit.get("replay_manifest"):
                raise ValueError("live security tasks must not supply an external replay manifest")
    if redo == "selected":
        if dataset != "daily_bar" or not symbols:
            raise ValueError("selected redo requires a daily task and explicit symbols")
        if set(symbols) - {unit["symbol"] for unit in units}:
            raise ValueError("selected securities are outside the frozen task scope")
    elif symbols:
        raise ValueError("symbols are only accepted for selected redo")
    immutable = {**definition, "units": [{k: v for k, v in unit.items() if k != "replay_manifest"} for unit in units]}
    definition_hash = row_hash(immutable)
    lock_scope = row_hash([dataset, "current" if dataset == "security_master" else day.isoformat(),
                          "security" if dataset == "security_master" else definition.get("asset_type", "stock")])[:16]
    work = paths["workspace_root"] / "_tasks" / task_id
    with PartitionLock(paths["workspace_root"] / "_locks" / (lock_scope + ".lock"), recover_stale=True):
        with MetadataStore(paths["metadata_path"]) as metadata:
            state = metadata.load_collection_task(task_id)
            if state and state["definition_hash"] != definition_hash:
                raise ValueError("task scope changed; create a new task ID")
            if state and state["status"] == "committing":
                if redo != "resume":
                    raise ValueError("recover the pending commit before restarting or selecting securities")
                return _finish_task_commit(state, paths, metadata, failure_hook)
            if state and state["status"] == "published" and redo == "resume":
                published = Path(state["published_manifest"])
                if (published.parent / "task-commit.json").exists():
                    return _finish_task_commit(state, paths, metadata, failure_hook)
                if file_hash(published) != state["published_hash"]:
                    raise ValueError("task publication has changed; use a new task or explicit redo")
                from ..storage.integrity import Manifest
                if not Manifest.load(published).verify(published.parent / "data.parquet"):
                    raise ValueError("published task data failed integrity verification")
                if dataset != "security_master" or state.get("complete_today"):
                    return {**state, "no_op": True}
            if state is None:
                state = {"task_id": task_id, "dataset": dataset, "definition_hash": definition_hash,
                         "definition": _json_value(immutable), "units": {}, "status": "collecting"}
            if redo == "full":
                RawObjectStore.preserve_directory(paths["raw_root"] / "_tmp" / task_id,
                    permitted_root=paths["raw_root"] / "_tmp", archive_root=paths["archive_root"])
                RawObjectStore.preserve_directory(work, permitted_root=paths["workspace_root"] / "_tasks",
                    archive_root=paths["archive_root"])
                state["units"] = {}
            if redo == "resume" and state.get("status") == "failed":
                if dataset == "security_master" and (work / "coverage.json").exists():
                    # Failed checks are immutable evidence too. Archive the prior
                    # generated workspace before recording the next conclusion.
                    RawObjectStore.preserve_directory(work, permitted_root=paths["workspace_root"] / "_tasks",
                        archive_root=paths["archive_root"])
                effective_redo = state.get("redo", "resume")
                effective_symbols = state.get("selected_symbols", [])
            else:
                effective_redo, effective_symbols = redo, list(symbols)
            state.update(status="collecting", redo=effective_redo, selected_symbols=effective_symbols, error=None,
                         complete_today=False, verification_mode=mode)
            metadata.save_collection_task(state)
            work.mkdir(parents=True, exist_ok=True)
            executor = collector or collect_input
            for unit in units:
                key = unit["key"]
                old = state["units"].get(key)
                force = redo == "full" or (redo == "selected" and unit["symbol"] in symbols) or bool(old and old.get("coverage_rejected"))
                if state["redo"] == "selected" and unit["symbol"] not in state["selected_symbols"]:
                    continue
                if not force and old and _task_unit_valid(old):
                    continue
                temporary = RawObjectStore.task_unit_path(paths["raw_root"], task_id, key)
                replay = unit.get("replay_manifest")
                # A transformation failure can continue from exact saved bytes.
                if not force and old and (old.get("failure_class") == "NormalizationError" or old.get("status") == "candidate_complete"):
                    try:
                        RawObjectStore.verify_manifest(old["raw_manifest"])
                        replay = old["raw_manifest"]
                    except (ValueError, OSError, KeyError):
                        pass
                preserved = RawObjectStore.preserve_directory(temporary,
                    permitted_root=paths["raw_root"] / "_tmp" / task_id, archive_root=paths["archive_root"])
                if preserved and replay == str(temporary / "manifest.ndjson"):
                    replay = str(preserved / "manifest.ndjson")
                options = dict(collector_options or {})
                options.update(input_id=unit["input_id"], context=unit["context"], config_root=config_root,
                    data_root=paths["data_root"], mode=mode, replay_manifest=replay,
                    task_unit=(task_id, key), force_fetch=force)
                try:
                    if mode == "live" and dataset == "security_master":
                        from ..routing.factory import check_security_input_qualification
                        qualification = check_security_input_qualification(contracts[unit["input_id"]], unit["context"],
                            config_root=config_root, metadata=metadata, now=datetime.now(timezone.utc))
                        if not qualification["eligible"]:
                            raise ValueError("formal routing qualification blocked: " + json.dumps(qualification["reasons"], ensure_ascii=False))
                    report = executor(**options)
                    if mode == "live" and dataset == "security_master":
                        # Only newly observed HTTP throttling/forbidden responses
                        # affect health; qualification failures and replay do not.
                        bad_http = next((event["status_code"] for event in report.get("responses", [])
                            if event.get("mode") == "live" and event.get("status_code") in {403, 429}), None)
                        if bad_http is not None:
                            for group in qualification["verified_groups"]:
                                exchange, asset = group.split("/")
                                metadata.record_provider_failure(provider=contracts[unit["input_id"]].provider,
                                    endpoint=contracts[unit["input_id"]].endpoint, capability_version=qualification["capability_version"],
                                    dataset="security_master", market=exchange, asset_type=asset,
                                    failure_class="rate_limited" if bad_http == 429 else "forbidden", error=f"HTTP {bad_http}",
                                    now=datetime.now(timezone.utc), http_status=bad_http, cooldown_seconds=30)
                    result_dir = Path(report["run_directory"]).resolve()
                    if not result_dir.is_relative_to(paths["workspace_root"]):
                        raise ValueError("source result escaped task workspace")
                    raw_manifest = temporary / "manifest.ndjson"
                    entry = {"input_id": unit["input_id"], "symbol": unit.get("symbol"),
                             "report": report, "raw_manifest": str(raw_manifest),
                             "failure_class": report.get("failure_class"), "status": report["status"]}
                    if report["status"] == "candidate_complete":
                        entry["raw_hash"] = RawObjectStore.verify_manifest(raw_manifest)
                        artifact = result_dir / report["normalized_parquet"]["path"]
                        if not artifact.resolve().is_relative_to(result_dir):
                            raise ValueError("normalized artifact escaped source workspace")
                        if file_hash(artifact) != report["normalized_parquet"]["sha256"]:
                            raise ValueError("normalized artifact does not match the source report")
                        _validate_task_candidate(artifact, unit, contracts[unit["input_id"]], dataset, day,
                                                 definition.get("adjustment", "forward"))
                        entry.update(artifact=str(artifact), artifact_hash=file_hash(artifact))
                    state["units"][key] = entry
                except Exception as exc:
                    temporary.mkdir(parents=True, exist_ok=True)
                    RawObjectStore(temporary).append_event({"event": "task_unit_failure", "outcome": "failure",
                        "error_class": type(exc).__name__, "mode": mode, "unit_key": key,
                        "fetched_at_utc": datetime.now(timezone.utc).isoformat()})
                    state["units"][key] = {"status": "failed", "input_id": unit["input_id"],
                        "symbol": unit.get("symbol"), "raw_manifest": str(temporary / "manifest.ndjson"),
                        "failure_class": type(exc).__name__, "error": str(exc),
                        "coverage_rejected": dataset == "security_master" and isinstance(exc, ValueError)}
                metadata.save_collection_task(state)
            state["status"] = "checking"
            state.pop("security_check", None)
            metadata.save_collection_task(state)
            try:
                _prepare_task_commit(state, units, contracts, paths, config_root, work, day)
            except Exception as exc:
                state.update(status="failed", error=str(exc))
                if state.get("security_check"):
                    RawObjectStore(work).write_json(state["security_check"], dataset="security_master",
                        provider="pipeline", endpoint="coverage", fetched_at=datetime.now(timezone.utc),
                        attempt_id="coverage", relative_path="coverage.json")
                metadata.save_collection_task(state)
                return state
            state["status"] = "committing"
            metadata.save_collection_task(state)
            return _finish_task_commit(state, paths, metadata, failure_hook)


def _task_units(definition, paths):
    """Resolve all_stock/all_etf against published master once, then freeze in the task."""
    import json
    from datetime import date
    from ..storage.metadata import MetadataStore
    units = definition.get("units")
    if units is not None:
        return json.loads(json.dumps(_json_value(units)))
    if definition["dataset"] != "daily_bar":
        raise ValueError("security master requires explicit source-interface units")
    with MetadataStore(paths["metadata_path"]) as metadata:
        old = metadata.load_collection_task(definition["task_id"])
        if old:
            return old["definition"]["units"]
    import pyarrow.parquet as pq
    manifest_path = paths["canonical_root"] / "security_master" / "current" / "manifest.json"
    from ..storage.integrity import Manifest
    manifest = Manifest.load(manifest_path)
    source = manifest_path.parent / "data.parquet"
    if (manifest_path.parent / "task-commit.json").exists() or not manifest.verify(source):
        raise ValueError("published security master is unavailable or pending")
    day = date.fromisoformat(definition["trade_date"])
    selected = []
    wanted = "etf" if definition.get("universe") == "all_etf" else "stock"
    if definition.get("universe") not in {"all_stock", "all_etf"}:
        raise ValueError("supported dynamic universes are all_stock and all_etf")
    for row in pq.read_table(source).to_pylist():
        if row["asset_type"] != wanted or row["status"] == "prelisted":
            continue
        if row.get("list_date") and row["list_date"] > day or row.get("delist_date") and row["delist_date"] < day:
            continue
        prefix = {"XSHG": "sh", "XSHE": "sz", "BSE": "bj"}[row["exchange"]]
        symbol = prefix + row["symbol"]
        selected.append({"key": symbol, "symbol": symbol, "input_id": definition["input_id"],
            "context": {"request": {"symbol": symbol, "start_date": day.isoformat(), "end_date": day.isoformat()}}})
    return selected


def _task_unit_valid(entry):
    from ..storage.integrity import file_hash
    try:
        report = entry.get("report", {})
        config_ok = all(file_hash(Path(item["path"])) == item["sha256"]
                        for item in (*report.get("config_files", ()), *report.get("code_files", ())))
        return (config_ok and not entry.get("coverage_rejected") and entry["status"] == "candidate_complete"
                and RawObjectStore.verify_manifest(entry["raw_manifest"]) == entry["raw_hash"]
                and file_hash(Path(entry["artifact"])) == entry["artifact_hash"])
    except (OSError, ValueError, KeyError):
        return False


def _validate_task_candidate(artifact, unit, contract, dataset, day, adjustment):
    import pyarrow.parquet as pq
    rows = pq.read_table(artifact).to_pylist()
    if not rows:
        raise ValueError("empty source candidate is not a successful collection unit")
    if dataset == "security_master":
        keys = [(row["exchange"], row["stock_code"]) for row in rows]
        if any(row["trade_date"] != day for row in rows):
            raise ValueError("security snapshot date differs from task")
    else:
        symbol = unit["symbol"]
        prefix = {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}.get(symbol[:2])
        identity = f"{prefix}:{symbol[2:]}"
        request = unit["context"]["request"]
        if any(row["instrument_id"] != identity or row["adjustment"] != adjustment
               or not (str(request["start_date"]) <= row["trade_date"].isoformat() <= str(request["end_date"])) for row in rows):
            raise ValueError("source candidate is outside the source request window")
        rows = [row for row in rows if row["trade_date"] == day]
        if not rows:
            raise ValueError("source candidate has no row for the task trading date")
        keys = [(row["instrument_id"], row["trade_date"], row["adjustment"]) for row in rows]
        if any(row["instrument_id"] != identity or row["trade_date"] != day or row["adjustment"] != adjustment for row in rows):
            raise ValueError("source candidate is outside requested security/date/adjustment")
        if any(row["low"] <= 0 or row["high"] < max(row["open"], row["close"], row["low"])
               or row["low"] > min(row["open"], row["close"]) for row in rows):
            raise ValueError("source candidate OHLC values are invalid")
    if len(set(keys)) != len(keys):
        raise ValueError("source candidate has duplicate business keys")


def _prepare_task_commit(state, units, contracts, paths, config_root, work, day):
    import json
    import shutil
    import pyarrow.parquet as pq
    from .daily import prepare_daily_task_publication
    from ..service.instruments_update import prepare_security_publication, parse_security_row
    from ..storage.integrity import file_hash, row_hash, Manifest
    from ..storage.parquet import write_normalized_rows, _records_to_table
    import yaml

    valid = {key: entry for key, entry in state["units"].items() if _task_unit_valid(entry)}
    if not valid and state["dataset"] == "daily_bar":
        raise ValueError("no valid source units; raw stays in _tmp")
    promotions, source_rows = [], {}
    for unit in units:
        key = unit["key"]
        if key not in valid:
            continue
        entry, contract = valid[key], contracts[unit["input_id"]]
        if state["redo"] == "selected" and unit["symbol"] not in state["selected_symbols"]:
            continue
        parameters = contract.bind_parameters(unit["context"])
        if state["dataset"] == "security_master":
            parameters = {k: v for k, v in parameters.items() if k != "trade_date"}
        current = paths["raw_root"] / contract.provider / contract.endpoint / ("scope-" + row_hash(parameters)[:16])
        temporary = Path(entry["raw_manifest"]).parent
        # Stable immutable evidence references survive replacement of current raw.
        # Retain the full digest in metadata; short containers avoid Windows
        # MAX_PATH failures when the response body itself has a SHA-256 name.
        audit = paths["archive_root"] / "_raw_evidence" / entry["raw_hash"][:12]
        if not audit.exists():
            audit.parent.mkdir(parents=True, exist_ok=True)
            pending_audit = audit.parent / ("pending-" + uuid4().hex[:12])
            shutil.copytree(temporary, pending_audit, copy_function=os.link)
            try:
                os.rename(pending_audit, audit)
            except FileExistsError:
                RawObjectStore.preserve_directory(pending_audit, permitted_root=audit.parent,
                    archive_root=paths["archive_root"])
        if RawObjectStore.verify_manifest(audit / "manifest.ndjson") != entry["raw_hash"]:
            raise ValueError("audit evidence does not match source")
        if not entry.get("promoted"):
            promotions.append({"key": key, "temporary": str(temporary), "current": str(current),
                           "audit_manifest": str(audit / "manifest.ndjson"), "raw_hash": entry["raw_hash"]})
        rows = pq.read_table(entry["artifact"]).to_pylist()
        if state["dataset"] == "daily_bar":
            rows = [row for row in rows if row["trade_date"] == day]
        source_rows[key] = {"rows": rows, "entry": entry, "contract": contract,
                            "raw_ref": str(audit / "manifest.ndjson"), "symbol": unit.get("symbol")}
    if state["dataset"] == "daily_bar":
        prepared = prepare_daily_task_publication(state, units, source_rows, paths, config_root, day)
        candidate = work / "prepared.parquet"
        pq.write_table(_records_to_table(prepared.pop("records")), candidate, compression="zstd")
    else:
        from datetime import date
        from ..quality.publication import check_security_coverage
        current_dir = paths["canonical_root"] / "security_master" / "current"
        previous = []
        prior = None
        previous_path = current_dir / "data.parquet"
        policy = yaml.safe_load((config_root / "datasets/security_master.yaml").read_text(encoding="utf-8"))
        allowed_groups = {tuple(pair) for pair in policy["dataset"]["publication"]["required_groups"]}
        definition = state["definition"]
        if "required_groups" in definition:
            required_groups = {tuple(pair) for pair in definition["required_groups"]}
        else:
            exchanges = set(definition.get("required_exchanges", ["XSHG", "XSHE", "BSE"]))
            assets = set(definition.get("required_asset_types", ["stock", "etf"]))
            if not exchanges <= {"XSHG", "XSHE", "BSE"} or not assets <= {"stock", "etf"}:
                raise ValueError("unsupported security coverage scope")
            required_groups = {pair for pair in allowed_groups if pair[0] in exchanges and pair[1] in assets}
        if not required_groups or not required_groups <= allowed_groups:
            raise ValueError("unsupported security coverage groups")
        source_scope = [{"key": unit["key"], "input_id": unit["input_id"],
                         "parameters": {key: value for key, value in contracts[unit["input_id"]].bind_parameters(unit["context"]).items()
                                        if key != "trade_date"}} for unit in units]
        scope_hash = row_hash({"groups": sorted(required_groups), "sources": source_scope})
        if (current_dir / "manifest.json").exists():
            manifest = Manifest.load(current_dir / "manifest.json")
            if (current_dir / "task-commit.json").exists() or not manifest.verify(previous_path):
                raise ValueError("existing security master failed hash verification")
            previous = [parse_security_row(row, source="previous") for row in pq.read_table(previous_path).to_pylist()]
            prior = manifest.publication_metadata
            if prior.get("as_of_date") and date.fromisoformat(prior["as_of_date"]) > day:
                raise ValueError("cannot replace a newer security master with an older task")
        missing = [unit["key"] for unit in units if unit["key"] not in valid]
        # Inspect today's sources alone. Previous rows must not conceal omissions.
        merged = prepare_security_publication({key: value["rows"] for key, value in source_rows.items()})
        records = merged.records
        comparable = previous if prior and prior.get("scope_hash") == scope_hash else ()
        check = check_security_coverage(records, required_groups=required_groups, previous=comparable)
        check.update(missing_units=missing, requested_date=day.isoformat(),
                     classification_conflicts=[item.instrument_id for item in merged.classification_conflicts],
                     source_failures={key: {"failure_class": state["units"][key].get("failure_class"),
                                            "error": state["units"][key].get("error") or state["units"][key].get("report", {}).get("error")}
                                      for key in missing})
        if comparable:
            old_types = {item.instrument_id: item.asset_type for item in comparable}
            check["classification_conflicts"].extend(item.instrument_id for item in records
                if item.instrument_id in old_types and item.asset_type != old_types[item.instrument_id])
        check["passed"] = check["passed"] and not missing and not check["classification_conflicts"]
        check["recollect_units"] = []
        for key, value in source_rows.items():
            source_records = prepare_security_publication({key: value["rows"]}).records
            declared_groups = {tuple(pair) for pair in policy["dataset"]["publication"]["source_groups"].get(value["entry"]["input_id"], ())} & required_groups
            source_previous = [item for item in comparable if (item.exchange.value, item.asset_type.value) in declared_groups]
            unit_check = check_security_coverage(source_records, required_groups=declared_groups, previous=source_previous)
            conflicted_ids = set(check["classification_conflicts"])
            if not unit_check["passed"] or any(item.instrument_id in conflicted_ids for item in source_records):
                value["entry"]["coverage_rejected"] = True
                check["recollect_units"].append(key)
                check["passed"] = False
        check.update(validated_at_utc=datetime.now(timezone.utc).isoformat(),
                     policy_sha256=file_hash(config_root / "datasets/security_master.yaml"),
                     code_sha256=file_hash(Path(__file__).parents[1] / "quality/publication.py"),
                     source_response_hashes={key: value["entry"]["raw_hash"] for key, value in source_rows.items()})
        state["security_check"] = check
        max_age = policy["dataset"]["publication"]["max_previous_age_days"]
        if type(max_age) is not int or max_age < 0:
            raise ValueError("max_previous_age_days must be a nonnegative calendar-day count")
        fallback = not check["passed"]
        if fallback:
            eligible = (state["redo"] != "full" and definition.get("allow_previous", True) and previous and prior
                        and prior.get("scope_hash") == scope_hash and prior.get("coverage_passed")
                        and prior.get("as_of_date"))
            age = (day - date.fromisoformat(prior["as_of_date"])).days if eligible else None
            if not eligible or not 0 <= age <= max_age:
                check["fallback_rejection"] = "no matching dated valid publication within configured age, or fallback disabled"
                raise ValueError("security source units or market/asset coverage are incomplete; no eligible previous publication")
            if not check_security_coverage(previous, required_groups=required_groups)["passed"]:
                raise ValueError("previous security master failed coverage verification")
            if {source["raw_ref"] for source in prior["sources"]} != set(manifest.raw_refs):
                raise ValueError("previous source metadata does not match raw evidence references")
            for source in prior["sources"]:
                raw_manifest = Path(source["raw_ref"]).resolve()
                if not raw_manifest.is_relative_to(paths["archive_root"]) or RawObjectStore.verify_manifest(raw_manifest) != source["raw_hash"]:
                    raise ValueError("previous source evidence failed integrity verification")
            records = tuple(previous)
            promotions = []
            publication_metadata = {**prior, "requested_date": day.isoformat(), "fallback": True,
                                    "previous_age_days": age, "fallback_reason": check,
                                    "previous_manifest_hash": file_hash(current_dir / "manifest.json")}
        else:
            # Retain trusted descriptive fields, but never retain omitted identities.
            records = prepare_security_publication({key: value["rows"] for key, value in source_rows.items()}, comparable).records
            publication_metadata = {"as_of_date": day.isoformat(), "requested_date": day.isoformat(),
                "scope_hash": scope_hash, "coverage_passed": True, "fallback": False,
                "required_groups": sorted(required_groups), "coverage": check,
                "sources": [{"unit_key": key, "input_id": value["entry"]["input_id"],
                             "source_date": day.isoformat(), "raw_ref": value["raw_ref"],
                             "raw_hash": value["entry"]["raw_hash"],
                             "artifact_hash": value["entry"]["artifact_hash"],
                             "adapter_version": value["entry"]["report"].get("adapter_version"),
                             "normalization_version": value["entry"]["report"].get("normalization_version")}
                            for key, value in source_rows.items()]}
        rows = [{name: _json_value(getattr(item, name)) if name not in {"list_date", "delist_date"} else getattr(item, name)
                 for name in ("instrument_id", "symbol", "exchange", "asset_type", "name", "list_date", "delist_date", "status")}
                for item in records]
        fields = yaml.safe_load((config_root / "datasets/security_master.yaml").read_text(encoding="utf-8"))["fields"]
        candidate = work / "prepared.parquet"
        if candidate.exists():
            RawObjectStore.preserve_directory(work, permitted_root=paths["workspace_root"] / "_tasks", archive_root=paths["archive_root"])
            work.mkdir(parents=True, exist_ok=True)
        if fallback:
            shutil.copyfile(previous_path, candidate)
        else:
            write_normalized_rows(candidate, rows, fields)
        RawObjectStore(work).write_json(check, dataset="security_master", provider="pipeline", endpoint="coverage",
            fetched_at=datetime.now(timezone.utc), attempt_id="coverage", relative_path="coverage.json")
        prepared = {"partition_directory": str(current_dir), "expected_count": len(records),
                    "row_count": len(records), "asset_type": "security", "partition_key": "current",
                    "fallback_units": sorted(set(missing + check["recollect_units"])), "publication_metadata": publication_metadata,
                    "item_statuses": {item.instrument_id: "success" for item in records}}
        if fallback:
            prepared["prior_raw_refs"] = list(manifest.raw_refs)
    prepared.update(candidate=str(candidate), candidate_hash=file_hash(candidate), promotions=promotions,
                    trade_date=day.isoformat(), raw_refs=list(dict.fromkeys(
                        prepared["prior_raw_refs"] if "prior_raw_refs" in prepared else
                        [source["raw_ref"] for source in source_rows.values()])))
    state["commit"] = prepared
    state["complete_today"] = len(valid) == len(units) and not prepared.get("publication_metadata", {}).get("fallback", False)
    if state["dataset"] == "security_master":
        state["data_date"] = prepared["publication_metadata"]["as_of_date"]
        state["fallback_used"] = prepared["publication_metadata"]["fallback"]


def _finish_task_commit(state, paths, metadata, failure_hook=None):
    import json
    import pyarrow.parquet as pq
    from ..domain import Dataset, Adjustment, ItemStatus
    from ..storage.integrity import Manifest, file_hash
    from ..storage.parquet import CanonicalPartitionStore, _mapping_to_bar, _fsync_file
    commit = state["commit"]
    candidate = Path(commit["candidate"])
    if not candidate.resolve().is_relative_to(paths["workspace_root"]) or file_hash(candidate) != commit["candidate_hash"]:
        raise ValueError("prepared publication is missing, escaped, or corrupt")
    partition = Path(commit["partition_directory"])
    if not partition.resolve().is_relative_to(paths["canonical_root"]):
        raise ValueError("commit partition escaped canonical root")
    partition.mkdir(parents=True, exist_ok=True)
    marker = partition / "task-commit.json"
    if marker.exists() and json.loads(marker.read_text(encoding="utf-8"))["task_id"] != state["task_id"]:
        raise ValueError("another task has a pending commit for this partition")
    RawObjectStore(partition).write_json({"task_id": state["task_id"], "definition_hash": state["definition_hash"]},
        dataset=state["dataset"], provider="pipeline", endpoint="commit", fetched_at=datetime.now(timezone.utc),
        attempt_id="commit", relative_path="task-commit.json")
    if failure_hook:
        failure_hook("before_publish")
    if state["dataset"] == "daily_bar":
        # A crash may leave the new data paired with the previous manifest.
        # Complete this task's verified pair before reading/merging the partition.
        pending_manifest = partition / ("manifest." + state["task_id"] + ".tmp.json")
        if pending_manifest.exists():
            pending = Manifest.load(pending_manifest)
            pending_data = partition / ("data." + state["task_id"] + ".tmp.parquet")
            if pending_data.exists() and pending.verify(pending_data):
                os.replace(pending_data, partition / "data.parquet")
            elif not pending.verify(partition / "data.parquet"):
                raise ValueError("interrupted task publication failed integrity verification")
            os.replace(pending_manifest, partition / "manifest.json")
        result = CanonicalPartitionStore(paths["canonical_root"]).publish(dataset=Dataset.DAILY_BAR,
            asset_type=commit["asset_type"], partition_key=commit["partition_key"],
            new_records=[_mapping_to_bar(row) for row in pq.read_table(candidate).to_pylist()],
            expected_count=commit["expected_count"], run_id=state["task_id"],
            item_statuses={key: ItemStatus(value) for key, value in commit["item_statuses"].items()},
            replace_instrument_ids=frozenset(commit["replace_ids"]), replace_adjustment=Adjustment(commit["adjustment"]),
            failure_hook=failure_hook)
        manifest_path, manifest = result.manifest_path, result.manifest
        if result.quarantined_keys:
            raise ValueError("publication contains quarantined source conflicts")
    else:
        target = partition / ("data." + state["task_id"] + ".tmp.parquet")
        import shutil
        shutil.copyfile(candidate, target)
        _fsync_file(target)
        manifest = Manifest.from_file(target, dataset="security_master", asset_type="security", partition_key="current",
            row_count=commit["row_count"], expected_count=commit["expected_count"], first_key=None, last_key=None,
            raw_refs=tuple(commit["raw_refs"]), item_statuses=commit["item_statuses"],
            publication_metadata=commit.get("publication_metadata", {}))
        temporary_manifest = partition / ("manifest." + state["task_id"] + ".tmp.json")
        manifest.write_atomic(temporary_manifest)
        os.replace(target, partition / "data.parquet")
        if failure_hook:
            failure_hook("after_data_replace")
        manifest_path = partition / "manifest.json"
        os.replace(temporary_manifest, manifest_path)
    if failure_hook:
        failure_hook("after_publish")
    for promotion in commit["promotions"]:
        RawObjectStore.promote_unit(temporary=promotion["temporary"], current=promotion["current"],
            raw_root=paths["raw_root"], archive_root=paths["archive_root"], expected_hash=promotion["raw_hash"])
        if failure_hook:
            failure_hook("after_raw_promote")
        entry = state["units"][promotion["key"]]
        entry.update(raw_manifest=promotion["audit_manifest"], current_raw=promotion["current"], promoted=True)
    if not manifest.verify(partition / "data.parquet"):
        raise ValueError("completed task publication is corrupt")
    metadata.record_partition_publish(manifest=manifest, manifest_path=manifest_path)
    state.update(status="published", published_manifest=str(manifest_path),
                 published_hash=file_hash(manifest_path), row_count=manifest.row_count, error=None)
    metadata.save_collection_task(state)
    marker.unlink()
    temporary_task = paths["raw_root"] / "_tmp" / state["task_id"]
    if temporary_task.exists() and not any(temporary_task.iterdir()):
        temporary_task.rmdir()
    return state


def collect_due_inputs(*, now, config_root, output_root=None, data_root=None, trading_dates=(), securities=(), symbols=(),
                       execute=False, mode="replay", replay_manifest=None, evidence_root=None,
                       dependency_paths=(), collector=None, dataset="inputs", calendar_days=None):
    """One durable tick: source candidates or the qualified security catalog task."""
    if dataset == "security_master":
        if output_root is not None or replay_manifest is not None or securities or symbols:
            raise ValueError("security master scheduling uses configured storage and full source scope")
        return _collect_due_security_master(now=now, config_root=config_root, data_root=data_root, execute=execute,
            mode=mode, calendar_days=calendar_days, dependency_paths=dependency_paths, collector=collector)
    if dataset != "inputs":
        raise ValueError("unsupported scheduled dataset")
    from ..domain import AttemptStatus
    from ..storage.metadata import MetadataStore
    from ..worker.attempts import CollectionAttempt
    from ..worker.scheduler import plan_input_collection
    from time import monotonic
    import json

    root = Path(config_root)
    if output_root is not None and data_root is not None:
        raise ValueError("output_root and data_root are mutually exclusive")
    runtime = output_root is None
    paths = load_storage_paths(root, data_root=data_root) if runtime else None
    output = paths["workspace_root"] / "_scheduler" if runtime else Path(output_root)
    if now.tzinfo is None:
        raise ValueError("schedule time must be timezone-aware")
    if mode not in {"live", "replay"}:
        raise ValueError("only live/replay modes are supported")
    if execute and mode == "live" and abs((datetime.now(timezone.utc) - now).total_seconds()) > 120:
        raise ValueError("live scheduling requires current time; use replay to inspect historical slots")
    if not runtime:
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
    metadata = MetadataStore(paths["metadata_path"] if runtime else output / "schedule-attempts.duckdb") if execute else None
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
                    storage_args = {"data_root": paths["data_root"]} if runtime else {"output_root": output}
                    result = collector(input_id=job.input_id, context={"request": request,
                        "calendar": {"trading_dates": trading_dates}}, config_root=root, **storage_args,
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


def _collect_due_security_master(*, now, config_root, data_root, execute, mode, calendar_days, dependency_paths, collector):
    """Existing tick entry drives one durable business task, without starting a daemon."""
    import json
    from datetime import date
    from ..service.calendar import TradingCalendarStore
    from ..storage.metadata import MetadataStore
    from ..worker.scheduler import plan_security_master_collection, SHANGHAI
    from ..routing.factory import check_security_input_qualification
    from ..storage.integrity import file_hash
    if now.tzinfo is None:
        raise ValueError("schedule time must be timezone-aware")
    root = Path(config_root).resolve()
    paths = load_storage_paths(root, data_root=data_root)
    if paths["data_root"].is_relative_to(Path(__file__).parents[2] / "provider_validation"):
        raise ValueError("business task data must be outside provider_validation")
    if execute and mode != "live":
        raise ValueError("scheduled production tasks require live mode; use collect-task for isolated replay")
    day = now.astimezone(SHANGHAI).date()
    if calendar_days is None:
        paths["metadata_path"].parent.mkdir(parents=True, exist_ok=True)
        with TradingCalendarStore(str(paths["metadata_path"])) as calendar:
            is_open = calendar.is_trading_day(day)
    else:
        days = {}
        for row in calendar_days:
            if type(row.get("is_trading_day")) is not bool:
                raise ValueError("security schedule calendar rows require explicit is_trading_day")
            row_day = date.fromisoformat(str(row["trade_date"]))
            if row_day in days:
                raise ValueError("duplicate security calendar date")
            days[row_day] = row["is_trading_day"]
        is_open = days.get(day)
    job = plan_security_master_collection(root, now=now, is_trading_day=is_open)
    contracts = {c.input_id: c for c in load_input_capabilities(root / "providers.yaml")}
    if job["status"] == "ready":
        for unit in job["definition"]["units"]:
            contract = contracts[unit["input_id"]]
            if contract.trading_date_parameter:
                unit["context"]["request"] = {contract.trading_date_parameter: day.isoformat()}
                unit["context"]["calendar"] = {"trading_dates": [day.isoformat()]}
        with MetadataStore(paths["metadata_path"]) as metadata:
            qualifications = [check_security_input_qualification(contracts[u["input_id"]], u["context"],
                config_root=root, metadata=metadata, now=now) for u in job["definition"]["units"]]
            job["source_qualifications"] = qualifications
            previous = metadata.load_collection_task(job["task_id"])
        # A previously published degraded task must retry; a complete task no-ops.
        if not execute and any(not q["eligible"] for q in qualifications):
            job.update(status="blocked", reason="formal routing qualification is missing")
        elif execute:
            if previous and previous["status"] == "committing":
                # collect_task owns recovery and the same partition lock.
                job["reason"] = "resume interrupted publication"
            result = collect_task(definition=job["definition"], config_root=root, data_root=paths["data_root"],
                                  mode="live", collector=collector)
            job.update(status=result["status"], complete_today=result["complete_today"],
                       data_date=result.get("data_date"), fallback_used=result.get("fallback_used", False),
                       no_op=result.get("no_op", False), error=result.get("error"))
    report = {"dataset": "security_master", "now": now.isoformat(), "execute": execute, "mode": mode,
              "production_writes": int(job["status"] == "published" and not job.get("no_op")),
              "eligible_for_production_routing": bool(job.get("source_qualifications")) and all(q["eligible"] for q in job.get("source_qualifications", [])),
              "jobs": [job], "dependencies": [{"path": str(Path(p).resolve()), "sha256": file_hash(Path(p))} for p in dependency_paths],
              "config_files": [{"path": str(root / p), "sha256": file_hash(root / p)}
                               for p in ("collection.yaml", "schedules.yaml", "providers.yaml", "datasets/security_master.yaml")]}
    output = paths["workspace_root"] / "_scheduler/security_master"
    saved = RawObjectStore(output).write_json(report, dataset="schedule_tick", provider="scheduler", endpoint="security_master",
        fetched_at=datetime.now(timezone.utc), attempt_id=uuid4().hex)
    return {**report, "report_path": str(saved.path.resolve())}
