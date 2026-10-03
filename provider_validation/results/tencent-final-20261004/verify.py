"""Independently check Tencent candidate evidence against immutable sources and executable bindings."""
from pathlib import Path
import hashlib
import json
import sys
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from stock_data_manage.config.loader import load_input_capabilities, load_collection_profiles
from stock_data_manage.routing.factory import build_input_provider
from stock_data_manage.storage.raw import RawObjectStore


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def ref(path):
    path = Path(path)
    return {"path": path.relative_to(ROOT).as_posix(), "sha256": digest(path)}


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


original = ROOT / "provider_validation/results/tencent-original-20261004"
manifest = read(original / "manifest.json")
for item in manifest["original_files"]:
    assert digest(ROOT / item["snapshot"]) == item["sha256"]
for key in ("source_manifest", "source_probe", "source_probe_runner"):
    assert digest(ROOT / manifest[key]) == manifest[key + "_sha256"]
before = yaml.safe_load((original / "6-providers.yaml.bin").read_bytes())
after = yaml.safe_load((ROOT / "config/providers.yaml").read_bytes())
assert before["providers"] == after["providers"]
assert before["input_capabilities"].keys() == after["input_capabilities"].keys()
assert all(value == after["input_capabilities"][key] for key, value in before["input_capabilities"].items() if key != "ASTOCK-001")
assert (original / "7-realtime_quote.yaml.bin").read_bytes() == (ROOT / "config/datasets/realtime_quote.yaml").read_bytes()
old_quote_rules = yaml.safe_load((original / "8-realtime_quote.yaml.bin").read_bytes())["rules"]
new_quote_rules = yaml.safe_load((ROOT / "config/normalization/realtime_quote.yaml").read_bytes())["rules"]
assert new_quote_rules[1:] == old_quote_rules

source_path = ROOT / manifest["source_manifest"]
source = next(read_line for read_line in map(json.loads, source_path.read_text(encoding="utf-8").splitlines())
              if read_line.get("url") == "https://qt.gtimg.cn/q=sh600519")
body = RawObjectStore.read_response(source_path, source)
assert len(body) == 550 and source["body_sha256"] == "6ed41c4189fef3f0c6daa5cccf77084b2920ba95e4df716d284aa74821231695"

final = ROOT / "provider_validation/results/tencent-final-20261004"
comparison = read(final / "comparison.json")
for key in ("verification_code", "runner"):
    assert digest(ROOT / comparison[key]["path"]) == comparison[key]["sha256"]
assert [item["row_count"] for item in comparison["inputs"]] == [1, 14, 96]
reports = []
for path in final.rglob("report.json"):
    report = read(path)
    directory = path.parents[4]
    assert directory.name.startswith(report["input_id"])
    assert not report["eligible_for_production_routing"] and report["production_writes"] == 0
    for item in report["code_files"] + report["config_files"]:
        assert digest(item["path"]) == item["sha256"], item["path"]
    for key in ("source_rows", "output", "raw_manifest"):
        if key in report:
            assert digest(directory / report[key]["path"]) == report[key]["sha256"]
    for event in report.get("responses", []):
        if event.get("body_storage"):
            RawObjectStore.read_response(directory / "_raw/manifest.ndjson", event)
    if report["input_id"] == "ASTOCK-001":
        assert not report["online_batch_validation"] and not report["universe_completeness_verified"]
    reports.append(report)
quote_report = read(ROOT / comparison["inputs"][0]["report_path"])
assert quote_report["coverage_denominator"] == quote_report["row_count"] == 1 and quote_report["coverage_complete"]
assert quote_report["responses"][0]["body_sha256"] == source["body_sha256"]
assert quote_report["responses"][0]["request_headers"] == source["request_headers"]
assert quote_report["source_capture_window"]["last"] == datetime.fromisoformat(source["fetched_at_utc"]).isoformat()
assert quote_report["returned_window"]["first"] == quote_report["returned_window"]["last"] == "20260930161458"
assert comparison["original_vs_provider"][0]["request_equal"] and comparison["original_vs_provider"][0]["legacy_rows_equal"]
assert len(comparison["negative_checks"]) == 11 and all(check["result"] == "passed" for check in comparison["negative_checks"])
assert comparison["batch_fixture"]["batch_sizes"] == [100, 100, 3]
assert comparison["batch_fixture"]["partial_denominator"] == 2

xml_path = ROOT / "provider_validation/results/tencent-final-20261004-tests.xml"
suites = ET.parse(xml_path).getroot().findall("testsuite")
assert sum(int(s.attrib["tests"]) for s in suites) == 270
assert all(int(s.attrib[key]) == 0 for s in suites for key in ("failures", "errors", "skipped"))
cli_path = ROOT / "provider_validation/results/tencent-cli-20261004/success/cli-validation.json"
cli = read(cli_path)
cli_report = read(cli["report"])
assert cli["exit_code"] == 0 and cli_report["row_count"] == 1 and cli_report["live_http_calls"] == 0
assert cli_report["context_dependency"]["sha256"] == cli["context_sha256"]

contracts = [c for c in load_input_capabilities(ROOT / "config/providers.yaml") if c.implementation_status == "implemented_validation_only"]
bindings = [{"input_id": c.input_id, "provider_class": type(build_input_provider(c, providers_path=ROOT / "config/providers.yaml")).__name__,
             "method": c.runtime_method} for c in contracts]
assert len(bindings) == 11
profile = next(p for p in load_collection_profiles(ROOT / "config/collection.yaml") if p.name == "tencent_market_snapshot")
assert not profile.scheduling_enabled and profile.universe == "all_stock"
assert profile.frequency_unit == "day" and profile.frequency_interval == 1 and profile.at_time.isoformat() == "15:10:00"

paths = []
for name in ("tencent-original-20261004", "tencent-dev-20261004", "tencent-final-20261004", "tencent-cli-20261004"):
    paths.extend(p for p in (ROOT / "provider_validation/results" / name).rglob("*") if p.is_file())
paths.extend((ROOT / "provider_validation/results").glob("tencent-*-20261004-*.xml"))
document = {"validation_time_utc": datetime.now(timezone.utc).isoformat(), "mode": "offline_replay",
    "external_network_requests": 0, "production_writes": 0, "eligible_for_production_routing": False,
    "original_inventory": ref(original / "manifest.json"), "comparison": ref(final / "comparison.json"),
    "tests": {**ref(xml_path), "passed": 270}, "cli": ref(cli_path),
    "real_source_tested_scope": ["sh600519"], "quote_source_sha256": source["body_sha256"],
    "row_counts": [1, 14, 96], "synthetic_batch": comparison["batch_fixture"],
    "negative_checks": comparison["negative_checks"], "executable_inputs": bindings,
    "other_input_contracts_unchanged": True, "dataset_template_unchanged": True, "eastmoney_rule_unchanged": True,
    "artifacts": [ref(p) for p in sorted(set(paths))],
    "limitations": ["实时来源证据仅单证券；203只分批为合成离线夹具。", "会话/间隔/缓存在线分支仅注入夹具，不证明实际网络重试或容量。",
                    "标准量额单位未独立核实，1分钟未迁移；自动快照仍关闭并有在线批量门禁。", "未进行生产发布、历史补采或管理台开发。"]}
target = ROOT / "provider_validation/results/tencent-final-20261004-verification.json"
with target.open("x", encoding="utf-8") as stream:
    json.dump(document, stream, ensure_ascii=False, indent=2)
    stream.write("\n")
print(json.dumps({"result": "passed", "artifacts": len(document["artifacts"]), "tests": 270, "executable_inputs": len(bindings)}))
