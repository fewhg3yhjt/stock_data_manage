"""Verify current source/evidence links without live calls or production writes."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from stock_data_manage.config.loader import load_input_capabilities, load_collection_profiles
from stock_data_manage.routing.factory import build_input_provider
from stock_data_manage.storage.raw import RawObjectStore, sanitized_headers, sanitized_url


def reference(path):
    path = Path(path).resolve()
    return {"path": path.relative_to(ROOT).as_posix(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def check_report(path):
    report = json.loads(path.read_text(encoding="utf-8"))
    assert report["status"] == "candidate_complete"
    assert report["production_writes"] == report["live_http_calls"] == 0
    assert not report["eligible_for_production_routing"]
    for ref in report["code_files"] + report["config_files"]:
        assert hashlib.sha256(Path(ref["path"]).read_bytes()).hexdigest() == ref["sha256"]
    for key in ("source_rows", "output", "raw_manifest"):
        ref = report[key]
        assert hashlib.sha256((path.parents[4] / ref["path"]).read_bytes()).hexdigest() == ref["sha256"]
    run = path.parents[4]
    sdk = report["sdk_dependency"]
    for ref in [{"path": sdk["source_path"], "sha256": sdk["sha256"]}, *sdk["dependencies"]]:
        assert hashlib.sha256((run / ref["path"]).read_bytes()).hexdigest() == ref["sha256"]
    comparisons = []
    manifest = run / report["raw_manifest"]["path"]
    for event in report["responses"]:
        body = RawObjectStore.read_response(manifest, event)
        assert hashlib.sha256(body).hexdigest() == event["body_sha256"]
        original_manifest = Path(event["source_ref"]["manifest"])
        original = json.loads(original_manifest.read_text(encoding="utf-8").splitlines()[event["source_ref"]["line"] - 1])
        assert RawObjectStore.read_response(original_manifest, original) == body
        assert event["method"] == original["method"] and event["url"] == sanitized_url(original["url"])
        assert event["status_code"] == original["status_code"]
        assert event["source_ref"]["fetched_at_utc"] == original["fetched_at_utc"]
        expected_headers = sanitized_headers(original["request_headers"])
        differences = {name: {"original": expected_headers.get(name), "provider": event["request_headers"].get(name)}
                       for name in set(expected_headers) | set(event["request_headers"])
                       if expected_headers.get(name) != event["request_headers"].get(name)}
        comparisons.append({"url": event["url"], "original_response_sha256": original["body_sha256"],
                            "status_body_time_equal": True, "header_differences_after_redaction": differences})
    return {**reference(path), "input_id": report["input_id"], "row_count": report["row_count"],
            "code_version": report["code_version"], "archive_comparisons": comparisons}


def main():
    comparison_path = Path(__file__).parent / "comparison.json"
    comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
    checked = [check_report(ROOT / item["report_path"]) for item in comparison["inputs"]]
    cli_path = next((ROOT / "provider_validation/results/ths-cli-20261004/out").rglob("report.json"))
    cli = check_report(cli_path)
    cli_result = json.loads((ROOT / "provider_validation/results/ths-cli-20261004/cli-report.json").read_text(encoding="utf-8-sig"))
    assert cli_result["status"] == "candidate_complete" and cli_result["row_count"] == 21
    test_path = ROOT / "provider_validation/results/ths-final-20261004-tests.xml"
    suite = ET.parse(test_path).getroot()[0]
    assert suite.get("tests") == "239"
    assert all(suite.get(key) == "0" for key in ("failures", "errors", "skipped"))
    contracts = load_input_capabilities(ROOT / "config/providers.yaml")
    profiles = {p.name: p for p in load_collection_profiles(ROOT / "config/collection.yaml")}
    executable = []
    for contract in contracts:
        try:
            provider = build_input_provider(contract, providers_path=ROOT / "config/providers.yaml")
        except ValueError:
            continue
        executable.append({"input_id": contract.input_id, "method": contract.runtime_method, "dataset": contract.dataset})
        if contract.input_id.startswith("SDA-BOARD-"):
            assert not profiles[contract.collection_profile].scheduling_enabled
    assert len(executable) == 8
    source_path = ROOT / "provider_validation/results/ths-original-20261004/boards.py.bin"
    snapshot_manifest = ROOT / "provider_validation/results/ths-original-20261004/manifest.json"
    assert reference(source_path)["sha256"] == json.loads(snapshot_manifest.read_text(encoding="utf-8"))["sha256"]
    artifacts = [reference(p) for base in (Path(__file__).parent, source_path.parent, ROOT / "provider_validation/results/ths-cli-20261004")
                 for p in sorted(base.rglob("*")) if p.is_file()]
    result = {"validation_time_utc": datetime.now(timezone.utc).isoformat(), "mode": "offline_replay",
              "network_requests": 0, "production_writes": 0, "eligible_for_production_routing": False,
              "tests": {**reference(test_path), "passed": 239}, "comparison": reference(comparison_path),
              "original_provider": reference(source_path), "inputs": checked, "cli_smoke": cli,
              "executable_inputs": executable, "artifacts": artifacts,
              "limitations": ["历史样本，不认证当前在线接受度或全市場容量。", "未核实单位仍置空；没有生产端到端发布。",
                              "旧证据保留原代码版本，当前代码与修改前源码另行关联。"]}
    target = ROOT / "provider_validation/results/ths-final-20261004-verification.json"
    with target.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"tests": 239, "row_counts": [r["row_count"] for r in checked], "artifacts": len(artifacts),
                      "archive_header_differences": sum(bool(c["header_differences_after_redaction"]) for r in checked for c in r["archive_comparisons"]),
                      "verification": str(target)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
