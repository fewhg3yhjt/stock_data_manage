"""Acceptance driver for the approved three-stock flow; business work uses existing APIs.

This is an explicitly invoked acceptance script, never a scheduler or production
entry point. It writes only tmp/daily-flow-20261029 and preserves source evidence.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import gzip
from importlib import metadata
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "tmp/daily-flow-20261029"
CONFIG = ROOT / "config"
DAY = "2026-10-29"
SYMBOLS = {"sh600519": "XSHG", "sz000001": "XSHE", "bj920001": "BSE"}
TOKENS = {"none": "none", "forward": "qfq", "backward": "hfq"}


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def save(name, value):
    path = BASE / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def environment(phase):
    if sys.version_info < (3, 11):
        raise RuntimeError("The project requires Python >=3.11")
    value = {"executable": sys.executable, "python_version": sys.version,
             "checked_at_utc": datetime.now(timezone.utc).isoformat(), "phase": phase,
             "packages": {name: metadata.version(name) for name in
                 ("pytest", "fastapi", "httpx", "duckdb", "pyarrow", "PyYAML", "requests", "uvicorn")}}
    save(f"checks/{phase}-environment.json", value)
    print(json.dumps(value, ensure_ascii=False), flush=True)


def context(symbol, adjustment):
    return {"request": {"symbol": symbol, "start_date": DAY, "end_date": DAY},
            "config": {"api": "newfqkline", "adjust": TOKENS[adjustment]}}


def raw_for(report, path=None):
    root = Path(report["run_directory"]) if "run_directory" in report else Path(path).parents[4]
    return (root / report["raw_manifest"]["path"]).resolve()


def excluded_archive(path):
    relative = path.relative_to(ROOT).as_posix()
    return path.is_relative_to(BASE) or any(part.startswith(("rmfp-", "erm-", "pytest-", "test_"))
        for part in path.relative_to(ROOT).parts) or "synthetic" in relative or "fixtures" in relative


def preflight():
    from stock_data_manage.config.loader import load_collection_profiles
    from stock_data_manage.storage.raw import RawObjectStore, sanitized_url
    from stock_data_manage.storage.integrity import file_hash
    profile = next(item for item in load_collection_profiles(CONFIG / "collection.yaml") if item.name == "tencent_daily")
    ttl = profile.refresh_interval_seconds or 86400
    search = subprocess.run(["rg", "--files", "--hidden", "--no-ignore", "-g", "manifest*.ndjson",
        "-g", "report.json", "-g", "*.report.json", "data", "tmp", "provider_validation/results"],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    if search.returncode not in (0, 2):
        raise RuntimeError(search.stderr)
    matches, reports, errors = [], [], []
    now = datetime.now(timezone.utc)
    for name in search.stdout.splitlines():
        path = ROOT / name
        if excluded_archive(path):
            continue
        try:
            if path.name.endswith(".ndjson"):
                for line in path.read_text(encoding="utf-8-sig").splitlines():
                    if DAY not in line or "newfqkline" not in line:
                        continue
                    event = json.loads(line)
                    param = parse_qs(urlsplit(event.get("url", "")).query).get("param", [""])[0].split(",")
                    if (event.get("provider"), event.get("endpoint"), event.get("mode"), event.get("status_code")) != ("tencent", "kline_daily", "live", 200):
                        continue
                    if len(param) != 6 or param[0] not in SYMBOLS or param[1:5] != ["day", DAY, DAY, "640"] or param[5] not in TOKENS.values():
                        continue
                    stamp = datetime.fromisoformat(event["fetched_at_utc"])
                    age = (now - stamp).total_seconds()
                    RawObjectStore.read_response(path, event)
                    matches.append({"manifest": str(path), "symbol": param[0], "token": param[5],
                        "fetched_at": stamp.isoformat(), "age_seconds": age, "fresh": 0 <= age <= ttl,
                        "response_sha256": event["body_sha256"], "url": event["url"]})
            else:
                report = read(path)
                params = report.get("parameters", {})
                if (report.get("input_id") == "ASTOCK-002-daily" and report.get("mode") == "live"
                        and report.get("live_http_calls", 0) > 0 and params.get("code") in SYMBOLS
                        and params.get("api") == "newfqkline" and params.get("start") == params.get("end") == DAY):
                    raw = raw_for(report, path)
                    RawObjectStore.verify_manifest(raw)
                    reports.append({"path": str(path), "symbol": params["code"], "token": params["adjust"], "raw": str(raw)})
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors.append({"path": str(path), "error": type(error).__name__ + ": " + str(error)})
    proxy = {}
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
        value = os.environ.get(name) or os.environ.get(name.lower())
        if value:
            proxy[name] = sanitized_url(value) if name != "NO_PROXY" else value
    listeners = []
    for name, address in proxy.items():
        parsed = urlsplit(address)
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} or not parsed.port:
            continue
        try:
            with socket.create_connection((parsed.hostname, parsed.port), timeout=1):
                reachable = True
        except OSError:
            reachable = False
        listeners.append({"variable": name, "address": address, "reachable_in_this_process": reachable})
    windows_proxy = {}
    if os.name == "nt":
        import winreg
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Internet Settings") as key:
                for name in ("ProxyEnable", "ProxyServer"):
                    try:
                        value = winreg.QueryValueEx(key, name)[0]
                        windows_proxy[name] = sanitized_url(value) if isinstance(value, str) else value
                    except OSError:
                        pass
        except OSError as error:
            windows_proxy["read_error"] = type(error).__name__
    save("archive-search.json", {"scope": {"date": DAY, "symbols": list(SYMBOLS), "tokens": TOKENS},
        "roots": ["data", "tmp", "provider_validation/results"], "excluded": "current acceptance and known synthetic/pytest fixture paths",
        "ttl_seconds": ttl, "responses": matches, "live_reports": reports, "errors": errors,
        "scan_errors": search.stderr.splitlines(), "scan_returncode": search.returncode,
        "proxy": proxy, "local_listeners": listeners, "windows_proxy": windows_proxy})
    snapshot = BASE / "original-production.json"
    if not snapshot.exists():
        names = ["data/metadata/metadata.duckdb", "data/canonical/2026-10-09/security_master/data.parquet",
                 "data/canonical/2026-10-09/security_master/manifest.json", "data/canonical/2026-10-28/daily_bar/stock/data.parquet",
                 "data/canonical/2026-10-28/daily_bar/stock/manifest.json"]
        save("original-production.json", {name: file_hash(ROOT / name) for name in names})
    print(json.dumps({"matching_raw_responses": len(matches), "fresh": sum(item["fresh"] for item in matches),
                      "live_reports": len(reports), "scan_errors": len(errors), "listeners": listeners}, ensure_ascii=False))


def collect(limit=None):
    from stock_data_manage.pipeline.inputs import collect_input
    from stock_data_manage.providers.transport import RequestPacer
    from stock_data_manage.storage.raw import RawObjectStore
    archive = read(BASE / "archive-search.json")
    index = read(BASE / "source-index.json") if (BASE / "source-index.json").exists() else {}
    pacer = RequestPacer()
    count = 0
    for symbol in SYMBOLS:
        for adjustment, token in TOKENS.items():
            scope = symbol + "-" + adjustment
            if scope in index and index[scope].get("status") == "candidate_complete":
                RawObjectStore.verify_manifest(Path(index[scope]["raw_manifest"]))
                continue
            fresh = [item for item in archive["responses"] if item["symbol"] == symbol and item["token"] == token and item["fresh"]]
            original = next((item for item in archive["live_reports"] if item["symbol"] == symbol and item["token"] == token
                and any(item["raw"] == raw["manifest"] for raw in fresh)), None)
            if original:
                report = read(original["path"])
                origin = "reused_matching_live_evidence"
            else:
                if fresh:
                    raise RuntimeError(f"Matching source bytes exist without an original live report: {scope}; review archive-search.json before another request")
                report = collect_input(input_id="ASTOCK-002-daily", context=context(symbol, adjustment),
                    config_root=CONFIG, output_root=BASE / "inputs", mode="live", security_asset_type="stock",
                    evidence_root=BASE / "inputs", pacer=pacer)
                origin = "current_live_input"
            raw = raw_for(report)
            index[scope] = {"symbol": symbol, "adjustment": adjustment, "context": context(symbol, adjustment),
                "status": report["status"], "live_report": report["report_path"], "raw_manifest": str(raw),
                "origin": origin, "live_http_calls": report.get("live_http_calls"), "error": report.get("error"),
                "responses": report.get("responses", [])}
            save("source-index.json", index)
            print(json.dumps({"scope": scope, "status": report["status"], "http_calls": report.get("live_http_calls"),
                              "error": report.get("error"), "report": report["report_path"]}, ensure_ascii=False), flush=True)
            if report["status"] != "candidate_complete":
                raise RuntimeError(f"Input failed; retained evidence: {report['report_path']}")
            RawObjectStore.verify_manifest(raw)
            count += 1
            if limit is not None and count >= limit:
                return


def module_tests(name, nodes):
    import pytest
    result = pytest.main([*nodes, "-q", "-p", "no:cacheprovider", "--basetemp=tmp/df29-" + name,
                          "--junitxml=" + str(BASE / "checks" / (name + ".xml"))])
    if result:
        raise SystemExit(result)


def process():
    from unittest.mock import patch
    from stock_data_manage.pipeline.inputs import collect_input
    from stock_data_manage.providers.transport import RequestPacer
    from stock_data_manage.storage.integrity import file_hash
    from stock_data_manage.storage.raw import RawObjectStore
    index = read(BASE / "source-index.json")
    results = []
    for scope, leg in index.items():
        symbol, adjustment = leg["symbol"], leg["adjustment"]
        raw = Path(leg["raw_manifest"])
        RawObjectStore.verify_manifest(raw)
        events = [json.loads(line) for line in raw.read_text(encoding="utf-8").splitlines()]
        responses = [e for e in events if e.get("event") == "http_response" and e.get("outcome") == "response"]
        assert len(responses) == 1
        event = responses[0]
        assert event["status_code"] == 200 and event["mode"] == "live"
        assert parse_qs(urlsplit(event["url"]).query)["param"] == [f"{symbol},day,{DAY},{DAY},640,{TOKENS[adjustment]}"]
        body = RawObjectStore.read_response(raw, event)
        payload = json.loads((gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body).decode("utf-8-sig"))
        assert payload["code"] in (0, "0")
        rows = payload["data"][symbol][{"none": "day", "forward": "qfqday", "backward": "hfqday"}[adjustment]]
        dates = [date.fromisoformat(str(row[0])) for row in rows]
        assert dates == sorted(set(dates)) and len(rows) <= 640
        targets = [row for row in rows if str(row[0]) == DAY]
        assert len(targets) == 1
        target = targets[0]
        prices = dict(zip(("open", "close", "high", "low"), (Decimal(str(v)) for v in target[1:5])))
        assert all(v.is_finite() and v > 0 for v in prices.values())
        assert prices["low"] <= min(prices["open"], prices["close"]) <= max(prices["open"], prices["close"]) <= prices["high"]
        values = {**prices, "volume": None if symbol.startswith("bj") else Decimal(str(target[5])) * 100,
                  "amount": None if symbol.startswith("bj") else Decimal(str(target[8])) * 10000}
        with patch("requests.adapters.HTTPAdapter.send", side_effect=AssertionError("Replay attempted HTTP")):
            replay = collect_input(input_id="ASTOCK-002-daily", context=leg["context"], config_root=CONFIG,
                output_root=BASE / "revalidated", mode="replay", replay_manifest=raw, security_asset_type="stock",
                pacer=RequestPacer(wait=lambda _: None))
        assert replay["status"] == "candidate_complete" and replay["live_http_calls"] == 0
        candidates = []
        for path in (Path(leg["live_report"]), Path(replay["report_path"])):
            report = read(path)
            output = path.parents[4] / report["output"]["path"]
            assert file_hash(output) == report["output"]["sha256"]
            normalized = read(output)
            assert report["row_count"] == len(normalized) == 1
            n = normalized[0]
            assert n["instrument_id"] == SYMBOLS[symbol] + ":" + symbol[2:]
            assert n["trade_date"] == DAY and n["adjustment"] == adjustment
            assert all(n[k] is None if v is None else Decimal(n[k]) == v for k, v in values.items())
            assert report["output"]["source_response_hashes"] == [event["body_sha256"]]
            candidates.append(n)
        assert candidates[0] == candidates[1]
        check = {"passed": True, "instrument_id": candidates[0]["instrument_id"], "asset_type": "stock",
            "parameters": read(leg["live_report"])["parameters"], "source_response_hashes": [event["body_sha256"]],
            "target_prices": prices, "expected_values": values, "source_target_row": target,
            "source_row_count": len(rows), "first_date": str(rows[0][0]), "last_date": str(rows[-1][0]),
            "coverage_denominator": 1, "target_row_count": 1, "live_and_replayed_equal": True,
            "live_http_calls_during_replay": 0, "validated_at_utc": datetime.now(timezone.utc).isoformat(),
            "method": "Independent retained JSON array positions; exact URL/date; OHLC and configured unit conversions; BSE null."}
        leg["revalidation_report"] = replay["report_path"]
        leg["semantic_check"] = str(save(f"checks/{scope}-semantic.json", check))
        results.append({"scope": scope, **check})
        save("source-index.json", index)
        print(json.dumps({"scope": scope, "values": values, "source_rows": len(rows), "passed": True}, default=str), flush=True)
    save("processing-results.json", {"passed": True, "groups": len(results), "price_comparisons": 36,
        "volume_amount_comparisons": 18, "results": results})


def task_definition(task_id, replay=True):
    index = read(BASE / "source-index.json")
    units = []
    for symbol in SYMBOLS:
        unit = {"key": symbol, "symbol": symbol, "input_id": "ASTOCK-002-daily",
                "context": {"request": context(symbol, "none")["request"]}}
        if replay:
            unit["replay_manifests"] = {a: index[symbol + "-" + a]["raw_manifest"] for a in TOKENS}
        units.append(unit)
    return {"task_id": task_id, "task": "stock_daily", "trade_date": DAY, "units": units}


def verify_partition(root):
    import pyarrow.parquet as pq
    from stock_data_manage.storage.integrity import Manifest, row_hash
    from stock_data_manage.storage.parquet import CanonicalPartitionStore
    from stock_data_manage.storage.raw import RawObjectStore
    path = root / f"canonical/{DAY}/daily_bar/stock/manifest.json"
    manifest = Manifest.load(path)
    assert manifest.verify(path.parent / "data.parquet")
    assert manifest.row_count == manifest.expected_count == 3
    assert manifest.schema_version == "daily-aggregate-v1" and manifest.adjustment == "aggregate"
    assert not (path.parent / "task-commit.json").exists()
    records = CanonicalPartitionStore(root / "canonical").read_daily("stock", DAY)
    assert len(records) == len({record.key for record in records}) == 3
    index = read(BASE / "source-index.json")
    for record in records:
        market, code = record.instrument_id.split(":")
        symbol = {"XSHG": "sh", "XSHE": "sz", "BSE": "bj"}[market] + code
        assert record.complete and record.quote_unit == "CNY" and len(record.quotes) == 3
        for adjustment, quote in record.quotes.items():
            check = read(index[symbol + "-" + adjustment]["semantic_check"])
            for field, value in check["expected_values"].items():
                assert getattr(quote, field) is None if value is None else getattr(quote, field) == Decimal(value)
            assert quote.source_provider == "tencent" and quote.endpoint == "kline_daily"
            RawObjectStore.verify_manifest(Path(quote.raw_object_path))
    rows = pq.read_table(path.parent / "data.parquet").to_pylist()
    for row in rows:
        for quote in row["quotes"].values():
            events = [json.loads(line) for line in Path(quote["raw_object_path"]).read_text(encoding="utf-8").splitlines()]
            quote["raw_object_path"] = sorted(e["body_sha256"] for e in events if e.get("body_sha256"))
    return {"passed": True, "manifest": str(path), "rows": 3, "quote_groups": 9,
            "raw_business_values_match": True, "business_signature": row_hash(sorted(rows, key=lambda r: r["instrument_id"]))}


def publish_replay(root_name="proof"):
    from unittest.mock import patch
    from stock_data_manage.pipeline.inputs import collect_task
    from stock_data_manage.providers.transport import RequestPacer
    root = BASE / root_name
    definition = task_definition("three-stock-20261029-" + root_name)
    save(f"{root_name}-task.json", definition)
    with patch("requests.adapters.HTTPAdapter.send", side_effect=AssertionError("Replay attempted HTTP")):
        state = collect_task(definition=definition, config_root=CONFIG, data_root=root, mode="replay",
                             collector_options={"pacer": RequestPacer(wait=lambda _: None)})
        save(f"{root_name}-state.json", state)
        assert state["status"] == "published", state.get("error")
        result = verify_partition(root)
        resumed = collect_task(definition=definition, config_root=CONFIG, data_root=root, mode="replay")
        assert resumed["no_op"]
    save(f"{root_name}-validation.json", {**result, "resume_no_op": True, "http_calls": 0})
    print(json.dumps({"root": root_name, **result, "resume_no_op": True}, ensure_ascii=False), flush=True)


def qualify():
    from stock_data_manage.config.loader import load_input_capabilities
    from stock_data_manage.providers.probes import ProbeEvidence
    from stock_data_manage.routing.factory import check_daily_input_qualification
    from stock_data_manage.storage.integrity import file_hash, row_hash
    from stock_data_manage.storage.metadata import MetadataStore
    assert read(BASE / "proof-validation.json")["passed"]
    index = read(BASE / "source-index.json")
    contract = next(c for c in load_input_capabilities(CONFIG / "providers.yaml") if c.input_id == "ASTOCK-002-daily")
    now = datetime.now(timezone.utc)
    gates = []
    with MetadataStore(BASE / "run/metadata/metadata.duckdb") as database:
        for scope, leg in index.items():
            artifacts = {name: Path(leg[name]) for name in ("live_report", "revalidation_report", "raw_manifest", "semantic_check")}
            artifacts.update(contract_tests=BASE / "checks/acquisition.xml",
                             publication_manifest=Path(read(BASE / "proof-validation.json")["manifest"]))
            proof = {"input_id": contract.input_id, "capability_version": "tencent-window-capture-v7",
                "asset_type": "stock", "allowed_modes": ["live", "retained"],
                "scope": "Isolated database; exact three stocks/date/API/adjustment; no market grant.",
                **{name: {"path": str(path), "sha256": file_hash(path)} for name, path in artifacts.items()}}
            path = save(f"checks/{scope}-qualification.json", proof)
            live = read(leg["live_report"])
            validated = datetime.fromisoformat(live["validation_time_utc"])
            expiry = validated + timedelta(days=7)
            assert now < expiry
            semantic = read(leg["semantic_check"])
            evidence = ProbeEvidence(provider="tencent", endpoint="kline_daily", capability_version=proof["capability_version"],
                validated_at=now, validation_expires_at=expiry, status="complete", eligible_for_selection=True,
                row_count=1, first_key=DAY, last_key=DAY, evidence_hash=file_hash(path), adjustment=leg["adjustment"],
                request_scope=("input:" + contract.input_id, "parameters:" + row_hash(contract.bind_parameters(leg["context"])),
                               "qualification:" + str(path)), response_status=200,
                returned_window=semantic["first_date"] + ".." + semantic["last_date"],
                field_semantics=("trade_date", "open", "high", "low", "close", "adjustment"), units=("price:CNY",))
            database.save_probe_evidence(evidence, dataset="daily_bar", market=SYMBOLS[leg["symbol"]], asset_type="stock",
                                         code_prefix=leg["symbol"], adjustment=leg["adjustment"])
            for mode in ("live", "retained"):
                gate = check_daily_input_qualification(contract, leg["context"], asset_type="stock", config_root=CONFIG,
                                                       metadata=database, now=now, execution_mode=mode)
                gates.append({"scope": scope, "mode": mode, **gate})
        save("qualification-results.json", gates)
        assert all(g["eligible"] for g in gates), gates
        negatives = []
        for changed in (context("sh601318", "none"), {**context("sh600519", "none"),
                "request": {"symbol": "sh600519", "start_date": "2026-10-28", "end_date": "2026-10-28"}}):
            gate = check_daily_input_qualification(contract, changed, asset_type="stock", config_root=CONFIG,
                                                   metadata=database, now=now, execution_mode="live")
            negatives.append({"context": changed, **gate})
        save("qualification-boundaries.json", negatives)
        assert all(not g["eligible"] for g in negatives)
    print("Isolated exact-scope qualification checks: 18 eligible; 2 out-of-scope blocked", flush=True)


def task_run():
    from stock_data_manage.pipeline.inputs import collect_task
    from stock_data_manage.providers.transport import RequestPacer
    definition = task_definition("three-stock-20261029-live", replay=False)
    save("live-task.json", definition)
    state = collect_task(definition=definition, config_root=CONFIG, data_root=BASE / "run", mode="live",
        collector_options={"evidence_root": BASE / "inputs", "pacer": RequestPacer()})
    save("live-state.json", state)
    assert state["status"] == "published", state.get("error")
    result = verify_partition(BASE / "run")
    assert result["business_signature"] == read(BASE / "proof-validation.json")["business_signature"]
    resume = collect_task(definition=definition, config_root=CONFIG, data_root=BASE / "run", mode="live")
    assert resume["no_op"]
    reports = [read(path) for path in (BASE / "run").rglob("*report.json")]
    events = [event for report in reports for event in report.get("responses", [])]
    save("live-validation.json", {**result, "resume_no_op": True,
        "task_http_calls": sum(report.get("live_http_calls", 0) for report in reports),
        "cached_responses": sum(event.get("mode") == "cached" for event in events)})
    print(json.dumps(result, ensure_ascii=False), flush=True)


def web_checks():
    from fastapi.testclient import TestClient
    from stock_data_manage.service.web import create_app
    index = read(BASE / "source-index.json")
    results = []
    with TestClient(create_app(config_root=CONFIG, data_root=BASE / "run")) as client:
        for adjustment in TOKENS:
            response = client.get("/api/daily", params={"start_date": DAY, "end_date": DAY,
                                  "asset_type": "stock", "adjustment": adjustment})
            assert response.status_code == 200
            value = response.json()
            assert value["total"] == 3 and len(value["items"]) == 3
            for row in value["items"]:
                check = read(index[row["symbol"] + "-" + adjustment]["semantic_check"])
                assert row["status"] == "available" and row["trade_date"] == DAY
                for field, expected in check["expected_values"].items():
                    assert row["values"][field] is None if expected is None else Decimal(str(row["values"][field])) == Decimal(expected)
            save(f"checks/api-{adjustment}.json", value)
            results.append({"adjustment": adjustment, "rows": value["total"], "all_values_match_raw": True})
        paths = ("/", "/static/app.js", "/static/styles.css", "/api/tasks", "/api/publications", "/api/raw?data_date=" + DAY)
        statuses = {path: client.get(path).status_code for path in paths}
        assert all(status == 200 for status in statuses.values())
        tasks = client.get("/api/tasks").json()
        publications = client.get("/api/publications").json()
        raw = client.get("/api/raw", params={"data_date": DAY}).json()
        save("checks/api-tasks.json", tasks)
        save("checks/api-publications.json", publications)
        save("checks/api-raw.json", raw)
        assert tasks["total"] == 1 and publications["total"] == 1 and raw["total"] == 9
    save("web-api-validation.json", {"passed": True, "projections": results, "read_only_paths": statuses,
        "task_count": tasks["total"], "publication_count": publications["total"], "raw_count": raw["total"]})
    print(json.dumps({"projections": results, "read_only_paths": statuses}, ensure_ascii=False), flush=True)


def review():
    import xml.etree.ElementTree as ET
    from stock_data_manage.storage.integrity import file_hash
    from stock_data_manage.storage.raw import RawObjectStore
    original = read(BASE / "original-production.json")
    production = [{"path": name, "unchanged": file_hash(ROOT / name) == digest} for name, digest in original.items()]
    assert all(item["unchanged"] for item in production)
    suites, unique = [], set()
    for name in ("acquisition", "processing", "pipeline", "web"):
        root = ET.parse(BASE / f"checks/{name}.xml").getroot()
        cases = list(root.iter("testcase"))
        assert cases and not any(list(case) for case in cases)
        suites.append({"suite": name, "executions": len(cases), "passed": len(cases), "failures": 0})
        unique.update((case.get("classname"), case.get("name")) for case in cases)
    index = read(BASE / "source-index.json")
    assert len(index) == 9
    for leg in index.values():
        RawObjectStore.verify_manifest(Path(leg["raw_manifest"]))
    source_http_calls = sum(leg["live_http_calls"] for leg in index.values())
    reports = [read(path) for path in (BASE / "run").rglob("*report.json")]
    task_http_calls = sum(report.get("live_http_calls", 0) for report in reports)
    cached = sum(event.get("mode") == "cached" for report in reports for event in report.get("responses", []))
    assert source_http_calls == 9 and task_http_calls == 0 and cached == 9
    for name in ("proof", "run", "reproduction"):
        assert verify_partition(BASE / name)["business_signature"] == read(BASE / "proof-validation.json")["business_signature"]
    assert read(BASE / "web-api-validation.json")["passed"] and read(BASE / "browser/results.json")["passed"]
    save("delivery.json", {"passed": True, "scope": {"date": DAY, "symbols": list(SYMBOLS), "adjustments": list(TOKENS)},
        "tests": suites, "test_executions": sum(s["executions"] for s in suites), "distinct_tests": len(unique),
        "source_live_http_calls": source_http_calls, "task_live_http_calls": task_http_calls,
        "task_cached_responses": cached, "reproduction_http_calls": 0, "production_files": production,
        "browser": "browser/results.json", "reviewed_at_utc": datetime.now(timezone.utc).isoformat(),
        "not_verified": ["Other dates and stocks", "Full-market continuous production", "Independent provider cross-check",
                         "New-machine environment reproduction", "Deferred historical test failures"],
        "design_conformance": "Existing input/task/provider/normalization/publication/web contracts; no business module or schema changes."})
    print(json.dumps(read(BASE / "delivery.json")), flush=True)


def cli_smoke():
    command = [sys.executable, "-B", "-X", "utf8", "-m", "stock_data_manage.cli", "collect-task",
        "--task-file", str(BASE / "reproduction-task.json"), "--config-root", str(CONFIG),
        "--data-root", str(BASE / "reproduction"), "--mode", "replay", "--no-progress"]
    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    result = {"command": command, "returncode": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr}
    save("checks/cli-smoke.json", result)
    assert completed.returncode == 0, result
    state = json.loads(completed.stdout)
    assert state["status"] == "published" and state["no_op"]
    print("Existing CLI replay resume: published, no_op", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("preflight", "module-tests", "collect", "process", "processing-tests",
        "pipeline-tests", "publish-proof", "qualify", "task-run", "reproduce", "web-tests", "web-checks", "review", "cli-smoke"))
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    environment(args.phase)
    if args.phase == "preflight":
        preflight()
    elif args.phase == "collect":
        collect(args.limit)
    elif args.phase == "process":
        process()
    elif args.phase == "processing-tests":
        module_tests("processing", ["tests/test_normalizer_validator.py"])
    elif args.phase == "pipeline-tests":
        module_tests("pipeline", ["tests/test_daily_collection_e2e.py", "tests/test_retained_daily.py"])
    elif args.phase == "publish-proof":
        publish_replay()
    elif args.phase == "reproduce":
        publish_replay("reproduction")
        assert read(BASE / "reproduction-validation.json")["business_signature"] == read(BASE / "live-validation.json")["business_signature"]
    elif args.phase == "qualify":
        qualify()
    elif args.phase == "task-run":
        task_run()
    elif args.phase == "web-tests":
        module_tests("web", ["tests/test_web_service.py"])
    elif args.phase == "web-checks":
        web_checks()
    elif args.phase == "review":
        review()
    elif args.phase == "cli-smoke":
        cli_smoke()
    else:
        module_tests("acquisition", ["tests/test_raw_metadata.py",
            "tests/test_provider_contract_adapters.py::test_tencent_alternative_window_keeps_source_rows_and_selects_dates",
            "tests/test_provider_contract_adapters.py::test_tencent_unadjusted_window_uses_newfqkline_and_selects_requested_date",
            "tests/test_provider_contract_adapters.py::test_tencent_daily_api_and_adjustment_must_match",
            "tests/test_provider_contract_adapters.py::test_tencent_bad_params_is_reported_as_request_rejection",
            "tests/test_provider_contract_adapters.py::test_tencent_alternative_rejects_bad_or_empty_window",
            "tests/test_daily_collection_e2e.py::test_capture_does_not_parse_or_switch_on_bad_business_payload",
            "tests/test_daily_collection_e2e.py::test_failover_only_catches_acquisition_faults"])


if __name__ == "__main__":
    main()
