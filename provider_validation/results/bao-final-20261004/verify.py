"""Verify this SDK migration's source, payload, coverage and test evidence links."""
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
from stock_data_manage.storage.raw import RawObjectStore


def ref(path):
    path = Path(path).resolve()
    return {"path": path.relative_to(ROOT).as_posix(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def check_report(path):
    report = json.loads(path.read_text(encoding="utf-8"))
    run = path.parents[4]
    assert report["status"] == "candidate_complete"
    assert report["live_http_calls"] == report["live_sdk_calls"] == report["production_writes"] == 0
    assert not report["eligible_for_production_routing"]
    for item in report["code_files"] + report["config_files"]:
        assert hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest() == item["sha256"]
    for key in ("source_rows", "output", "raw_manifest"):
        item = report[key]
        assert hashlib.sha256((run / item["path"]).read_bytes()).hexdigest() == item["sha256"]
    for item in report["sdk_dependency"]["dependencies"]:
        assert hashlib.sha256((run / item["path"]).read_bytes()).hexdigest() == item["sha256"]
    comparisons = []
    for event in report["responses"]:
        assert event["event"] == "source_payload" and event["mode"] == "replay"
        body = RawObjectStore.read_response(run / report["raw_manifest"]["path"], event)
        original_manifest = Path(event["source_ref"]["manifest"])
        original = json.loads(original_manifest.read_text(encoding="utf-8").splitlines()[event["source_ref"]["line"] - 1])
        assert body == RawObjectStore.read_response(original_manifest, original)
        assert event["body_sha256"] == original["body_sha256"]
        assert event["source_ref"]["fetched_at_utc"] == original["fetched_at_utc"]
        assert not event["original_transport_bytes_available"]
        decoded = json.loads(body)
        assert len(decoded["rows"]) == event["metadata"]["row_count"]
        comparisons.append({"method": event["method"], "parameters": event["request_parameters"],
                            "source_response_sha256": event["body_sha256"], "decoded_rows": len(decoded["rows"]),
                            "sdk_status_code": event["sdk_status_code"], "body_time_equal": True, "tcp_bytes_visible": False})
    for event in report["sdk_derived"]:
        RawObjectStore.read_response(run / report["raw_manifest"]["path"], event)
        assert event["source_response_sha256"] in {entry["body_sha256"] for entry in report["responses"]}
    assert report["coverage_denominator"] == 5223
    assert report["missing_industry_symbols"] == ["sz001246", "sz301716"]
    assert report["coverage_complete"] == (report["input_id"] == "SDA-BOARD-005")
    return {**ref(path), "input_id": report["input_id"], "rows": report["row_count"],
            "denominator": report["coverage_denominator"], "coverage_complete": report["coverage_complete"],
            "missing_symbols": report["missing_symbols"], "code_version": report["code_version"], "sdk_comparison": comparisons}


def main():
    root = Path(__file__).parent
    comparison = json.loads((root / "comparison.json").read_text(encoding="utf-8"))
    checked = [check_report(ROOT / item["report_path"]) for item in comparison["inputs"]]
    cli_root = ROOT / "provider_validation/results/bao-cli-20261004"
    cli = check_report(next((cli_root / "out").rglob("report.json")))
    calendar = json.loads((cli_root / "calendar-evidence.json").read_text(encoding="utf-8"))
    assert ref(ROOT / calendar["source_csv"])["sha256"] == calendar["source_sha256"]
    assert ref(cli_root / calendar["derived_calendar"])["sha256"] == calendar["derived_sha256"]
    test_path = ROOT / "provider_validation/results/bao-final-20261004-tests.xml"
    suite = ET.parse(test_path).getroot()[0]
    assert suite.get("tests") == "255" and all(suite.get(k) == "0" for k in ("failures", "errors", "skipped"))
    snapshots = ROOT / "provider_validation/results/bao-original-20261004"
    original = json.loads((snapshots / "manifest.json").read_text(encoding="utf-8"))
    for item in original["sources"]:
        assert ref(snapshots / item["snapshot_path"])["sha256"] == item["sha256"]
    assert ref(ROOT / original["original_sdk_manifest"]["path"])["sha256"] == original["original_sdk_manifest"]["sha256"]
    profiles = {p.name: p for p in load_collection_profiles(ROOT / "config/collection.yaml")}
    executable = []
    for contract in load_input_capabilities(ROOT / "config/providers.yaml"):
        try:
            build_input_provider(contract, providers_path=ROOT / "config/providers.yaml")
        except ValueError:
            continue
        executable.append({"input_id": contract.input_id, "method": contract.runtime_method, "dataset": contract.dataset})
        if contract.input_id in {"SDA-BOARD-005", "SDA-BOARD-006"}:
            assert not profiles[contract.collection_profile].scheduling_enabled
    assert len(executable) == 10
    files = [ref(p) for base in (root, snapshots, cli_root, ROOT / "provider_validation/results/bao-dev-20261004")
             for p in sorted(base.rglob("*")) if p.is_file()]
    result = {"validation_time_utc": datetime.now(timezone.utc).isoformat(), "mode": "offline_replay",
              "external_network_requests": 0, "production_writes": 0, "eligible_for_production_routing": False,
              "tests": {**ref(test_path), "passed": 255}, "comparison": ref(root / "comparison.json"),
              "inputs": checked, "cli_smoke": cli, "calendar_evidence": ref(cli_root / "calendar-evidence.json"),
              "executable_inputs": executable, "artifacts": files,
              "negative_checks": comparison["negative_checks"], "empty_scope": comparison["empty_scope"],
              "session_pacing_cache": comparison["session_pacing_cache"],
              "limitations": ["SDK解码表示可见，TCP帧和内部物理请求不可见。", "行业归属缺2只，分母仅为原SDK沪深A股筛选范围。",
                              "未新增实时来源探针，缓存/间隔在线分支仅用注入夹具；未完成生产端到端发布。"]}
    target = ROOT / "provider_validation/results/bao-final-20261004-verification.json"
    with target.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"tests": 255, "inputs": len(checked), "row_counts": [r["rows"] for r in checked],
                      "artifacts": len(files), "verification": str(target)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
