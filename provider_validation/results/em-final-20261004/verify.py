"""Independent saved-artifact audit; never fetch data or overwrite an existing audit index."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import sys
import subprocess
import xml.etree.ElementTree as ET

import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from stock_data_manage.storage.raw import RawObjectStore


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit_report(path, input_id, count, source_count):
    report = json.loads(path.read_text(encoding="utf-8"))
    # Locate the run root from the report's persisted relative references.
    run = next(parent for parent in path.parents if (parent / "_raw").is_dir())
    assert report["input_id"] == input_id and report["status"] == "candidate_complete"
    assert report["row_count"] == report["coverage_denominator"] == count
    assert report["original_row_count"] == source_count
    assert report["production_writes"] == report["live_http_calls"] == 0
    assert report["eligible_for_production_routing"] is False
    for ref in report["code_files"] + report["config_files"]:
        assert digest(Path(ref["path"])) == ref["sha256"], ref["path"]
    sdk = report["sdk_dependency"]
    assert digest(run / sdk["source_path"]) == sdk["sha256"]
    manifest = run / report["raw_manifest"]["path"]
    assert digest(manifest) == report["raw_manifest"]["sha256"]
    for event in report["responses"]:
        assert event["mode"] == "replay" and event["status_code"] == 200
        body = RawObjectStore.read_response(manifest, event)
        assert len(body) == event["body_bytes"]
        source_manifest = Path(event["source_ref"]["manifest"])
        original = json.loads(source_manifest.read_text(encoding="utf-8").splitlines()[event["source_ref"]["line"] - 1])
        assert RawObjectStore.read_response(source_manifest, original) == body
    artifacts = {}
    for key in ("output", "source_rows", "excluded_rows"):
        ref = report[key]
        saved = run / ref["path"]
        assert digest(saved) == ref["sha256"]
        artifacts[key] = json.loads(saved.read_text(encoding="utf-8"))
        assert len(artifacts[key]) == ref["row_count"]
    assert len(artifacts["output"]) == count and len(artifacts["source_rows"]) == source_count
    assert len(artifacts["excluded_rows"]) == source_count - count
    if input_id == "ASTOCK-027":
        excluded = artifacts["excluded_rows"][0]
        assert excluded["reason"] == "not_implemented_dividend_plan" and excluded["row"]["方案进度"] == "预披露"
        assert artifacts["source_rows"][excluded["source_row_index"]] == excluded["row"]
        selected = [r for r in artifacts["source_rows"] if r["方案进度"] == "实施分配"]
        assert [r["ex_dividend_date"] for r in artifacts["output"]] == [r["除权除息日"] for r in selected]
    assert all(row[field] is None for row in artifacts["output"] for field in report["unverified_fields"])
    return {"input_id": input_id, "source_count": source_count, "candidate_count": count, "result": "passed"}


def main():
    evidence = ROOT / "provider_validation/results"
    original = json.loads((evidence / "em-original-20261004/manifest.json").read_text(encoding="utf-8"))
    for ref in original["original_files"]:
        assert digest(ROOT / ref["snapshot"]) == ref["sha256"]
        assert (ROOT / ref["snapshot"]).stat().st_size == ref["bytes"]
    before = yaml.safe_load((evidence / "em-original-20261004/6-providers.yaml.bin").read_bytes())
    after = yaml.safe_load((ROOT / "config/providers.yaml").read_text(encoding="utf-8"))
    assert before["providers"] == after["providers"]
    for key, value in before["input_capabilities"].items():
        if key not in {"ASTOCK-026", "ASTOCK-027", "ASTOCK-028"}:
            assert value == after["input_capabilities"][key], key
    assert sum(c["implementation_status"] == "implemented_validation_only" for c in after["input_capabilities"].values()) == 14
    for name, number in (("shareholder_count", 7), ("stock_fund_flow", 8), ("dividend_event", 9)):
        assert yaml.safe_load((ROOT / f"config/datasets/{name}.yaml").read_text(encoding="utf-8")) == yaml.safe_load(
            (evidence / f"em-original-20261004/{number}-{name}.yaml.bin").read_bytes())
    tests = ET.parse(evidence / "em-final-20261004-tests.xml").getroot()[0]
    assert int(tests.attrib["tests"]) == 292 and all(int(tests.attrib[k]) == 0 for k in ("failures", "errors", "skipped"))
    comparison = json.loads((evidence / "em-final-20261004/comparison.json").read_text(encoding="utf-8"))
    results = []
    for item, count, source_count in zip(comparison["inputs"], (63, 27, 120), (63, 28, 120)):
        assert digest(ROOT / item["report_path"]) == item["report_sha256"]
        results.append(audit_report(ROOT / item["report_path"], item["input_id"], count, source_count))
    for item in comparison["original_vs_provider"]:
        for path_key, hash_key in (("original_csv", "original_csv_sha256"), ("original_probe", "original_probe_sha256"),
                                   ("original_manifest", "original_manifest_sha256"), ("provider_report", "provider_report_sha256")):
            assert digest(Path(item[path_key])) == item[hash_key]
        assert item["all_sdk_columns_and_rows_equal"] and item["all_csv_columns_and_rows_equal"] and item["requests_equal"]
    for item in comparison["negative_checks"]:
        assert digest(ROOT / item["report_path"]) == item["report_sha256"]
        failed = json.loads((ROOT / item["report_path"]).read_text(encoding="utf-8"))
        assert failed["status"] == "failed" and failed["failure_class"] == item["failure_class"]
        assert "output" not in failed
    cli = json.loads((evidence / "em-cli-20261004/verification.json").read_text(encoding="utf-8"))
    for item, count, source_count in zip(cli["results"], (63, 27, 120), (63, 28, 120)):
        assert item["exit_code"] == 0
        assert digest(Path(item["stdout"])) == item["stdout_sha256"] and digest(Path(item["stderr"])) == item["stderr_sha256"]
        results.append(audit_report(Path(item["report_path"]), item["input_id"], count, source_count))
    index_path = evidence / "em-final-20261004-verification.json"
    files = set()
    for directory in evidence.glob("em-*-20261004"):
        files.update(p for p in directory.rglob("*") if p.is_file())
    files.update(p for p in evidence.glob("em-*-20261004-*.xml") if p.is_file())
    files.update(ROOT / name for name in (".gitattributes", "CODE_STRUCTURE.md", "PROJECT_PROGRESS.md",
        "provider_validation/docs/2026-10-04-eastmoney-input-collection.md", "tests/test_input_collection.py",
        "provider_validation/tests/replay_input_capabilities.py"))
    for path in (ROOT / "config/providers.yaml", ROOT / "src/stock_data_manage/pipeline/inputs.py"):
        files.add(path)
    files.update(Path(ref["path"]) for ref in json.loads(Path(cli["results"][0]["report_path"]).read_text(encoding="utf-8"))["code_files"])
    files.update(ROOT / f"config/normalization/{name}.yaml" for name in ("shareholder_count", "dividend_event", "stock_fund_flow"))
    file_index = [{"path": p.relative_to(ROOT).as_posix(), "sha256": digest(p), "bytes": p.stat().st_size} for p in sorted(files)]
    if index_path.exists():
        previous = json.loads(index_path.read_text(encoding="utf-8"))
        assert previous["files"] == file_index
    else:
        record = dict(validation_time_utc=datetime.now(timezone.utc).isoformat(), result="passed", tests=292,
            network_requests=0, production_writes=0, eligible_for_production_routing=False,
            dividend_user_decision="27 implemented events only; one predisclosure preserved as source evidence",
            candidate_input_count=14, reports=results, files=file_index)
        index_path.write_text(json.dumps(record, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    if "--check-staged" in sys.argv:
        paths = subprocess.check_output(["git", "diff", "--cached", "--name-only", "-z"], cwd=ROOT).split(b"\0")
        for raw in paths:
            if not raw: continue
            path = raw.decode("utf-8")
            assert path not in {"AGENTS.md", "开发记录.md"} and "~$" not in path and ".env" not in path
            staged = subprocess.check_output(["git", "show", ":"+path], cwd=ROOT)
            assert staged == (ROOT / path).read_bytes(), path
    print(json.dumps(dict(result="passed", tests=292, audited_reports=len(results), indexed_files=len(file_index)), ensure_ascii=False))


if __name__ == "__main__":
    main()
