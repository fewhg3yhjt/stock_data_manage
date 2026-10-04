"""Independent offline checks of saved runtime layers, original bytes and delivery."""
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import importlib.util
import json
import re
import xml.etree.ElementTree as ET

import duckdb
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).parent


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main():
    from stock_data_manage.storage.raw import RawObjectStore
    baseline = load(OUT / "baseline.json")
    for item in baseline["files"]:
        assert digest(ROOT / item["snapshot"]) == item["sha256"]
    for item in baseline["providers"]:
        assert digest(ROOT / item["path"]) == item["sha256"], item["path"]
    assert digest(ROOT / "AGENTS.md") == baseline["preexisting_agents_sha256"]
    config = yaml.safe_load((ROOT / "config/providers.yaml").read_text(encoding="utf-8"))["input_capabilities"]
    expected = {ident for ident, cfg in config.items() if cfg["implementation_status"] == "implemented_validation_only"}
    results = [load(p) for p in OUT.glob("ASTOCK-*-verification.json")]
    results += [load(p) for p in OUT.glob("SDA-*-verification.json")]
    assert {item["input_id"] for item in results} == expected and len(results) == 64
    data = OUT / "replay-data-v2"
    assert not (ROOT / "data").exists(), "offline acceptance must not create project runtime data"
    assert not (data / "canonical").exists() and not (data / "task_archive").exists()
    rows, responses, bodies, derived = 0, 0, set(), []
    with duckdb.connect(str(data / "metadata/metadata.duckdb"), read_only=True) as connection:
        attempts = {row[0]: row for row in connection.execute(
            "SELECT attempt_id,status,raw_object_path,raw_content_hash FROM collection_attempt").fetchall()}
    assert len(attempts) == 64
    for item in results:
        assert digest(item["source_manifest"]) == item["source_manifest_sha256"]
        assert digest(item["report_path"]) == item["report_sha256"]
        report = load(item["report_path"])
        task = Path(item["report_path"]).parent
        manifest = load(task / "manifest.json")
        assert manifest["task_id"] == task.name and manifest["input_id"] == item["input_id"]
        assert manifest["status"] == report["status"] == "candidate_complete"
        assert not manifest["publication_permitted"] and not manifest["canonical_refs"]
        assert not report["eligible_for_production_routing"]
        assert report["production_writes"] == report["live_http_calls"] == 0
        for source in report["code_files"] + report["config_files"]:
            assert digest(source["path"]) == source["sha256"], source["path"]
        raw = (task / report["raw_manifest"]["path"]).resolve()
        assert raw.is_relative_to(data / "raw" / report["provider"] / report["endpoint"])
        assert digest(raw) == report["raw_manifest"]["sha256"]
        attempt = attempts[task.name]
        assert attempt[1] == "validated" and Path(attempt[2]) == raw and attempt[3] == digest(raw)
        for event in report["responses"]:
            assert event.get("mode") == "replay", event
            responses += 1
            if event.get("body_storage"):
                body = RawObjectStore.read_response(raw, event)
                assert hashlib.sha256(body).hexdigest() == event["body_sha256"]
                source_ref = event.get("source_ref") or {}
                original_manifest = Path(source_ref["manifest"])
                original_events = [json.loads(line) for line in original_manifest.read_text(encoding="utf-8").splitlines()]
                assert any(original.get("body_sha256") == event["body_sha256"] for original in original_events)
                bodies.add(event["body_sha256"])
        for name in ("output", "report", "quality_report"):
            ref = manifest[name]
            assert digest(task / ref["path"]) == ref["sha256"]
        descriptor = report["normalized_parquet"]
        parquet = task / descriptor["path"]
        records = pq.ParquetFile(parquet).read().to_pylist()
        mapped = load(task / report["output"]["path"])
        assert len(records) == len(mapped) == item["row_count"] == descriptor["row_count"]
        for record, source in zip(records, mapped):
            assert set(record) == set(source)
            for name, value in record.items():
                if isinstance(value, Decimal):
                    assert value == Decimal(source[name])
                elif isinstance(value, datetime):
                    assert value == datetime.fromisoformat(source[name])
                elif hasattr(value, "isoformat"):
                    assert value.isoformat() == source[name]
                else:
                    assert value == source[name]
            for name in report["unverified_fields"]:
                if name in record:
                    assert record[name] is None
        for path in task.rglob("*"):
            if path.is_file():
                derived.append({"path":path.relative_to(ROOT).as_posix(), "sha256":digest(path),
                                "byte_length":path.stat().st_size})
        rows += len(records)
    suites = {}
    for name in ("release-tests.xml", "storage-final-tests.xml"):
        suite = ET.parse(OUT / name).getroot().find("testsuite")
        assert suite.attrib["failures"] == suite.attrib["errors"] == suite.attrib["skipped"] == "0"
        suites[name] = {key:int(suite.attrib[key]) for key in ("tests","failures","errors","skipped")}
    formal_dir = OUT / "formal"
    formal_dir.mkdir(exist_ok=True)
    spec = importlib.util.spec_from_file_location("formal_audit", ROOT / "provider_validation/results/formal-interface-spec-20261004/verify.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.OUT = formal_dir
    module.main()
    formal = load(ROOT / "docs/providers/源头采集接口说明.json")
    overview = formal["sheets"][0]
    assert len(overview["headers"]) == 35
    assert all(len(row) == 35 and "task_workspace/" in row[31] and "task_archive/" in row[34]
               for row in overview["rows"])
    links = []
    for path in (ROOT / "docs/storage/README.md", ROOT / "CODE_STRUCTURE.md"):
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", path.read_text(encoding="utf-8")):
            if "://" not in target:
                linked = path.parent / target.split("#")[0]
                assert linked.exists(), str(linked)
                links.append(target)
    result = {"result":"passed", "validated_at_utc":datetime.now(timezone.utc).isoformat(),
        "mode":"offline archived source replay and saved artifact comparison", "network_requests":0,
        "production_writes":0, "new_runtime_inputs":len(results), "unique_datasets":len({config[k]["dataset"] for k in expected}),
        "mapped_rows_checked":rows, "response_events_checked":responses, "unique_original_body_hashes":len(bodies),
        "provider_files_unchanged":len(baseline["providers"]), "tests":suites,
        "routing_and_schedules":"unchanged; new inputs not enabled", "canonical_and_archive_writes":0,
        "preexisting_agents_changes":"untouched", "markdown_links_checked":len(links),
        "task_artifacts":derived, "baseline_sha256":digest(OUT / "baseline.json"),
        "formal_verification_sha256":digest(formal_dir / "verification.json"),
        "remaining_work":["dataset-specific multi-source construction and publication semantics",
                          "connect successful production publication to archival",
                          "online freshness, scope and capacity qualification before routing"]}
    (OUT / "verification.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({key:value for key,value in result.items() if key != "task_artifacts"},ensure_ascii=False))


if __name__ == "__main__":
    main()
