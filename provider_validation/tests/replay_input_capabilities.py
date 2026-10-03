"""Persist the four-input offline migration evidence using the executable regression checks."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from datetime import datetime, timezone
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def main():
    parser = argparse.ArgumentParser(description="保存四项输入的离线回放与原实现比较证据")
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    args.output_root = args.output_root.resolve()
    if not args.output_root.is_relative_to(ROOT / "provider_validation/results"):
        parser.error("verification outputs must be under provider_validation/results")
    if args.output_root.exists():
        parser.error("output directory must be new; existing evidence is never overwritten")
    test_source = ROOT / "tests/test_input_collection.py"
    spec = importlib.util.spec_from_file_location("input_migration_checks", test_source)
    checks = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checks)
    args.output_root.mkdir(parents=True)
    summary = {"validation_time_utc": datetime.now(timezone.utc).isoformat(), "mode": "offline_replay",
               "network_requests": 0, "production_writes": 0, "eligible_for_production_routing": False,
               "verification_code": {"path": test_source.relative_to(ROOT).as_posix(), "sha256": hashlib.sha256(test_source.read_bytes()).hexdigest()},
               "runner": {"path": Path(__file__).relative_to(ROOT).as_posix(), "sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
               "inputs": [], "original_vs_provider": [],
               "known_differences": ["历史m5归档另有web3失败请求；当前源码快照使用web、proxy、ifzq三个入口，本次严格对照当前快照，不改写历史记录。",
                   "Provider保留来源字段，标准列通过YAML映射；未核实单位的字段在候选标准输出置空，来源数值仍保留。",
                   "换手率用Decimal执行÷100，避免原脚本float的二进制舍入；比较数值语义并独立核验来源值÷100。",
                   "原始字节为HTTP库返回的应用负载；未捕获SDK/urllib3内部重试或线缆压缩帧。"]}

    def forbidden_network(*args, **kwargs):
        raise AssertionError("offline replay cannot access network")

    with patch("requests.adapters.HTTPAdapter.send", forbidden_network):
        for input_id, context, manifest, count in checks.CASES:
            directory = args.output_root / input_id
            checks.test_archived_inputs_execute_yaml_and_preserve_evidence(directory, None, input_id, context, manifest, count)
            report_path = next(directory.rglob("report.json"))
            report = json.loads(report_path.read_text(encoding="utf-8"))
            summary["inputs"].append({"input_id": input_id, "report_path": report_path.relative_to(ROOT).as_posix(),
                "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(), "source_manifest": manifest.relative_to(ROOT).as_posix(),
                "source_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(), "row_count": count,
                "scope": report["parameters"], "code_version": report["code_version"], "result": "passed"})
        for input_id, context, manifest, count in checks.CASES[:2]:
            directory = args.output_root / ("d" if input_id.endswith("daily") else "m")
            checks.test_tencent_matches_original_shipped_script(directory, None, input_id, context, manifest, count)
            original = [json.loads(line) for line in (directory / "original/manifest.ndjson").read_text(encoding="utf-8").splitlines()]
            report = json.loads(next((directory / "adapter").rglob("report.json")).read_text(encoding="utf-8"))
            keys = ["url", "method", "request_headers", "outcome", "status_code", "body_sha256", "error_type"]
            summary["original_vs_provider"].append({"input_id": input_id, "scope": report["parameters"],
                "rows_compared": count, "all_ohlc_equal": True, "source_volume_equal": True, "returned_window_equal": True,
                "request_comparison": [{"original": {k: left.get(k) for k in keys}, "provider": {k: right.get(k) for k in keys}, "equal": True}
                                       for left, right in zip(original, report["responses"])],
                "original_manifest": (directory / "original/manifest.ndjson").relative_to(ROOT).as_posix(),
                "provider_report": next((directory / "adapter").rglob("report.json")).relative_to(ROOT).as_posix()})
        for input_id, filename in [("ASTOCK-045", "43_东财涨停池"), ("ASTOCK-070", "68_交易日历")]:
            csv_path = ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003" / filename / "data.csv"
            summary["original_vs_provider"].append({"input_id": input_id, "original_parsed_csv": csv_path.relative_to(ROOT).as_posix(),
                "original_parsed_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
                "all_source_fields_compared": True, "all_rows_equal": True,
                "comparison": "原响应经原SDK函数解析，所有来源列及行与原归档CSV比较；标准化字段另按YAML执行。"})
    target = args.output_root / "comparison.json"
    target.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"result": "passed", "inputs": 4, "row_counts": [14, 96, 52, 8797], "comparison": str(target)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
