"""Audit this delivery's immutable evidence and optional staged bytes."""
from pathlib import Path
import ast
import collections
from datetime import datetime, timezone
import hashlib
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from stock_data_manage.storage.raw import RawObjectStore


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def audit(path):
    report = json.loads(path.read_text(encoding="utf-8"))
    if "raw_manifest" not in report:
        assert report["status"] == "failed" and report["failure_class"] == "ValueError"
        assert not report.get("responses") and "output" not in report
        assert report["production_writes"] == 0 and report["eligible_for_production_routing"] is False
        for ref in report["code_files"] + report["config_files"]:
            assert sha(ref["path"]) == ref["sha256"]
        return {"path":path.relative_to(ROOT).as_posix(), "sha256":sha(path), "status":"failed-before-request", "live_http_calls":0}
    run = Path(report["run_directory"]) if "run_directory" in report else path.parents[4]
    # Persisted reports omit returned absolute paths; locate the input run from its raw reference.
    while not (run / "_raw").exists():
        assert run != ROOT
        run = run.parent
    manifest = run / report["raw_manifest"]["path"]
    assert sha(manifest) == report["raw_manifest"]["sha256"]
    assert report["production_writes"] == 0 and report["eligible_for_production_routing"] is False
    for ref in report["code_files"] + report["config_files"]:
        assert sha(ref["path"]) == ref["sha256"], ref["path"]
    for event in report["responses"]:
        if event.get("outcome") == "response":
            body = RawObjectStore.read_response(manifest, event)
            assert hashlib.sha256(body).hexdigest() == event["body_sha256"]
            assert len(body) == event["body_bytes"]
            if (event.get("source_ref") or {}).get("manifest"):
                original = Path(event["source_ref"]["manifest"])
                records = [json.loads(line) for line in original.read_text(encoding="utf-8").splitlines()]
                ref = records[event["source_ref"]["line"]-1] if "line" in event["source_ref"] else next(
                    record for record in records if record.get("body_sha256") == event["source_ref"]["sha256"])
                assert RawObjectStore.read_response(original, ref) == body
    for name in ("source_rows", "output", "excluded_rows"):
        if name in report:
            ref = report[name]
            artifact = run / ref["path"]
            assert sha(artifact) == ref["sha256"]
            assert len(json.loads(artifact.read_text(encoding="utf-8"))) == ref["row_count"]
    if report["status"] == "failed":
        assert "output" not in report
    return {"path": path.relative_to(ROOT).as_posix(), "sha256": sha(path), "status": report["status"],
            "row_count": report.get("row_count"), "live_http_calls": report.get("live_http_calls")}


def main():
    results = ROOT / "provider_validation/results"
    original = json.loads((results / "lpr-original-20261004/manifest.json").read_text(encoding="utf-8"))
    for ref in original["original_files"]:
        assert sha(ROOT / ref["snapshot"]) == ref["sha256"]
    for source, field in (("source_code", "source_code_sha256"), ("raw_manifest", "raw_manifest_sha256")):
        assert sha(ROOT / original[source]) == original[field]
    before = yaml.safe_load((results / "lpr-original-20261004/4-providers.yaml.bin").read_text(encoding="utf-8"))
    after = yaml.safe_load((ROOT / "config/providers.yaml").read_text(encoding="utf-8"))
    assert before["providers"] == after["providers"]
    for key, contract in before["input_capabilities"].items():
        if key not in {"ASTOCK-065"}:
            assert contract == after["input_capabilities"][key], key
    counts = dict(collections.Counter(c["implementation_status"] for c in after["input_capabilities"].values()))
    assert counts == {"implemented_validation_only": 25, "unimplemented": 39, "blocked": 7, "alias": 3}
    tree = ast.parse((ROOT / "src/stock_data_manage/routing/factory.py").read_text(encoding="utf-8"))
    methods = next(ast.literal_eval(n.value) for n in ast.walk(tree) if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "expected_methods" for t in n.targets))
    assert set(methods) == {key for key, c in after["input_capabilities"].items() if c["implementation_status"] == "implemented_validation_only"}
    suite = ET.parse(results / "lpr-final-v2-20261004-tests.xml").getroot()[0]
    assert int(suite.attrib["tests"]) == 363 and all(int(suite.attrib[k]) == 0 for k in ("failures", "errors", "skipped"))
    summary = json.loads((results / "lpr-final-v2-20261004/comparison.json").read_text(encoding="utf-8"))
    assert sha(ROOT / summary["runner"]["path"]) == summary["runner"]["sha256"]
    assert sha(ROOT / summary["verification_code"]["path"]) == summary["verification_code"]["sha256"]
    assert len(summary["inputs"]) == 1 and summary["network_requests"] == 0
    for ref in summary["original_vs_provider"]:
        assert ref["all_business_fields_equal"] and ref["all_retained_source_fields_equal"] and ref["request_comparison_equal"]
        assert sha(ref["original_parsed_path"]) == ref["original_parsed_sha256"]
        assert sha(ref["report_path"]) == ref["report_sha256"]
    sample_report = json.loads(Path(summary["original_vs_provider"][0]["report_path"]).read_text(encoding="utf-8"))
    assert sample_report["original_row_count"] == 1576 and sample_report["row_count"] == 1538
    assert sample_report["excluded_rows"]["row_count"] == 38 and sample_report["source_total_count"] == 1576
    audited = [audit(p) for p in sorted((results / "lpr-final-v2-20261004").rglob("report.json"))]
    for ref in summary["negative_checks"]:
        assert sha(ROOT / ref["report_path"]) == ref["report_sha256"]
        report = json.loads((ROOT / ref["report_path"]).read_text(encoding="utf-8"))
        assert report["failure_class"] == ref["failure_class"] and report["status"] == "failed"
    cli = json.loads((results / "lpr-cli-20261004/verification.json").read_text(encoding="utf-8"))
    for ref in cli["results"]:
        assert ref["exit_code"] == 0 and ref["count"] == 1538
        assert sha(ROOT / ref["stdout"]) == ref["stdout_sha256"] and sha(ROOT / ref["stderr"]) == ref["stderr_sha256"]
        audited.append(audit(Path(ref["report_path"])))
    for ref in audited:
        if ref["live_http_calls"]:
            assert "session" in ref["path"] and ref["live_http_calls"] == 4
    files = {p for directory in results.glob("lpr-*-20261004") for p in directory.rglob("*") if p.is_file()}
    files.update(results.glob("lpr-*-20261004-*.xml"))
    files.update(ROOT / name for name in (".gitattributes", "CODE_STRUCTURE.md", "PROJECT_PROGRESS.md", "config/providers.yaml",
        "tests/test_input_collection.py", "provider_validation/tests/replay_input_capabilities.py",
        "provider_validation/docs/2026-10-04-lpr-input-collection.md"))
    sample = json.loads(Path(summary["original_vs_provider"][0]["report_path"]).read_text(encoding="utf-8"))
    files.update(Path(ref["path"]) for ref in sample["code_files"])
    for name in ("lpr_history",):
        files.update(ROOT / f"config/{directory}/{name}.yaml" for directory in ("datasets", "normalization"))
    index = [{"path": p.relative_to(ROOT).as_posix(), "sha256": sha(p), "bytes": p.stat().st_size} for p in sorted(files)]
    destination = results / "lpr-final-v2-20261004-verification.json"
    if destination.exists():
        assert json.loads(destination.read_text(encoding="utf-8"))["files"] == index
    else:
        destination.write_text(json.dumps(dict(validation_time_utc=datetime.now(timezone.utc).isoformat(), result="passed",
            tests=363, counts=counts, real_network_requests=0, production_writes=0, eligible_for_production_routing=False,
            audited_reports=audited, files=index), ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    if "--check-staged" in sys.argv:
        for raw in subprocess.check_output(["git", "diff", "--cached", "--name-only", "-z"], cwd=ROOT).split(b"\0"):
            if not raw: continue
            name = raw.decode("utf-8")
            assert name not in {"AGENTS.md", "开发记录.md"} and "~$" not in name and ".env" not in name
            assert subprocess.check_output(["git", "show", ":"+name], cwd=ROOT) == (ROOT / name).read_bytes(), name
    print(json.dumps(dict(result="passed", tests=363, audited_reports=len(audited), indexed_files=len(index))))


if __name__ == "__main__":
    main()
