"""Audit retained stock-pool responses, configs, derived rows, CLI and test evidence."""
from pathlib import Path
from datetime import datetime, timezone
import ast
import collections
import hashlib
import json
import sys
import subprocess
import xml.etree.ElementTree as ET
import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from stock_data_manage.storage.raw import RawObjectStore


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit(path, count, pool=True):
    report = json.loads(path.read_text(encoding="utf-8"))
    run = next(p for p in path.parents if (p / "_raw").is_dir())
    assert report["status"] == "candidate_complete" and report["row_count"] == report["coverage_denominator"] == count
    assert report["production_writes"] == report["live_http_calls"] == 0 and not report["eligible_for_production_routing"]
    for ref in report["code_files"] + report["config_files"]:
        assert sha(Path(ref["path"])) == ref["sha256"], ref["path"]
    dependency = report["sdk_dependency"]
    assert sha(run / dependency["source_path"]) == dependency["sha256"]
    manifest = run / report["raw_manifest"]["path"]
    assert sha(manifest) == report["raw_manifest"]["sha256"]
    for event in report["responses"]:
        assert event["mode"] == "replay" and event["status_code"] == 200
        body = RawObjectStore.read_response(manifest, event)
        assert len(body) == event["body_bytes"]
        source = Path(event["source_ref"]["manifest"])
        original = json.loads(source.read_text(encoding="utf-8").splitlines()[event["source_ref"]["line"] - 1])
        assert RawObjectStore.read_response(source, original) == body
        if pool:
            raw = json.loads(body)
            assert raw["data"]["qdate"] == 20260930 and raw["data"]["tc"] == len(raw["data"]["pool"]) == count
    artifacts = {}
    for key in ("source_rows", "output"):
        ref = report[key]; saved = run / ref["path"]
        assert sha(saved) == ref["sha256"]
        artifacts[key] = json.loads(saved.read_text(encoding="utf-8"))
        assert len(artifacts[key]) == ref["row_count"] == count
    assert all(r[f] is None for r in artifacts["output"] for f in report["unverified_fields"])
    if pool:
        assert report["source_quote_date"] == "2026-09-30" and report["source_total_count"] == count
        assert all(r["trade_date"] == "2026-09-30" and r["snapshot_at"] == report["source_capture_window"]["last"] for r in artifacts["output"])
        assert [r["source_security_code"] for r in artifacts["output"]] == [r["代码"] for r in artifacts["source_rows"]]
    return {"input_id": report["input_id"], "rows": count, "report": path.relative_to(ROOT).as_posix(), "result": "passed"}


def main():
    results = ROOT / "provider_validation/results"
    original = json.loads((results / "pools-original-20261004/manifest.json").read_text(encoding="utf-8"))
    for ref in original["original_files"]:
        assert sha(ROOT / ref["snapshot"]) == ref["sha256"]
    before = yaml.safe_load((results / "pools-original-20261004/4-providers.yaml.bin").read_bytes())
    after = yaml.safe_load((ROOT / "config/providers.yaml").read_text(encoding="utf-8"))
    assert before["providers"] == after["providers"]
    changed = {"ASTOCK-023", "ASTOCK-046", "ASTOCK-047", "ASTOCK-048", "ASTOCK-050"}
    for key, contract in before["input_capabilities"].items():
        if key not in changed:
            assert contract == after["input_capabilities"][key], key
    counts = dict(collections.Counter(c["implementation_status"] for c in after["input_capabilities"].values()))
    assert counts == {"implemented_validation_only": 18, "unimplemented": 46, "blocked": 7, "alias": 3}
    alias = after["input_capabilities"]["ASTOCK-023"]
    assert alias["canonical_input"] == "SDA-BOARD-003"
    tree = ast.parse((ROOT / "src/stock_data_manage/routing/factory.py").read_text(encoding="utf-8"))
    method_map = next(ast.literal_eval(n.value) for n in ast.walk(tree) if isinstance(n, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == "expected_methods" for t in n.targets))
    assert set(method_map) == {key for key, c in after["input_capabilities"].items() if c["implementation_status"] == "implemented_validation_only"}
    suite = ET.parse(results / "pools-final-20261004-tests.xml").getroot()[0]
    assert int(suite.attrib["tests"]) == 312 and all(int(suite.attrib[k]) == 0 for k in ("failures", "errors", "skipped"))
    summary = json.loads((results / "pools-final-20261004/comparison.json").read_text(encoding="utf-8"))
    audited = []
    for item, count in zip(summary["inputs"], (12, 9, 57, 199)):
        assert sha(ROOT / item["report_path"]) == item["report_sha256"]
        audited.append(audit(ROOT / item["report_path"], count))
    for item in summary["original_vs_provider"]:
        assert item["all_sdk_source_columns_equal"] and item["all_csv_columns_equal"] and item["returned_date_equal"]
        assert sha(Path(item["original_csv"])) == item["original_csv_sha256"]
        assert sha(Path(item["original_manifest"])) == item["original_manifest_sha256"]
        assert sha(Path(item["report_path"])) == item["report_sha256"]
        assert item["request_comparison"][0]["original"] == item["request_comparison"][0]["provider"]
    for item in summary["negative_checks"]:
        path = ROOT / item["report_path"]
        assert sha(path) == item["report_sha256"]
        failure = json.loads(path.read_text(encoding="utf-8"))
        assert failure["status"] == "failed" and failure["failure_class"] == item["failure_class"] and "output" not in failure
    cli = json.loads((results / "pools-cli-20261004/verification.json").read_text(encoding="utf-8"))
    for item in cli["results"]:
        assert item["exit_code"] == 0
        assert sha(ROOT / item["stdout"]) == item["stdout_sha256"] and sha(ROOT / item["stderr"]) == item["stderr_sha256"]
        audited.append(audit(Path(item["report_path"]), item["count"], item["input_id"] != "SDA-BOARD-003"))
    alias_result = json.loads((results / "pools-final-20261004/alias/alias-comparison.json").read_text(encoding="utf-8"))
    assert sha(Path(alias_result["source_csv"])) == alias_result["source_csv_sha256"] and alias_result["source_rows_compared"] == 90
    audited.append(audit(Path(alias_result["report_path"]), 90, False))
    files = set()
    for folder in results.glob("pools-*-20261004"):
        files.update(p for p in folder.rglob("*") if p.is_file())
    files.update(results.glob("pools-*-20261004-*.xml"))
    files.update(ROOT / name for name in (".gitattributes", "CODE_STRUCTURE.md", "PROJECT_PROGRESS.md", "config/providers.yaml",
        "tests/test_input_collection.py", "provider_validation/tests/replay_input_capabilities.py",
        "provider_validation/docs/2026-10-04-stock-pool-input-collection.md"))
    report = json.loads(Path(cli["results"][0]["report_path"]).read_text(encoding="utf-8"))
    files.update(Path(ref["path"]) for ref in report["code_files"])
    for name in ("broken_limit_pool", "limit_down_pool", "previous_limit_pool", "strong_stock_pool"):
        files.update(ROOT / f"config/{directory}/{name}.yaml" for directory in ("datasets", "normalization"))
    index = [{"path": p.relative_to(ROOT).as_posix(), "sha256": sha(p), "bytes": p.stat().st_size} for p in sorted(files)]
    destination = results / "pools-final-20261004-verification.json"
    if destination.exists():
        assert json.loads(destination.read_text(encoding="utf-8"))["files"] == index
    else:
        destination.write_text(json.dumps(dict(validation_time_utc=datetime.now(timezone.utc).isoformat(), result="passed", tests=312,
            counts=counts, network_requests=0, production_writes=0, eligible_for_production_routing=False,
            authorization="confirmed autonomous remaining-input conversion; routine batches need no new confirmation",
            audited_reports=audited, files=index), ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    if "--check-staged" in sys.argv:
        for raw in subprocess.check_output(["git", "diff", "--cached", "--name-only", "-z"], cwd=ROOT).split(b"\0"):
            if not raw: continue
            path = raw.decode("utf-8")
            assert path not in {"AGENTS.md", "开发记录.md"} and "~$" not in path and ".env" not in path
            assert subprocess.check_output(["git", "show", ":"+path], cwd=ROOT) == (ROOT / path).read_bytes(), path
    print(json.dumps(dict(result="passed", tests=312, audited_reports=len(audited), indexed_files=len(index))))


if __name__ == "__main__":
    main()
