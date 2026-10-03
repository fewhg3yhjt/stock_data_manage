"""Persist selected input migration evidence using executable regression checks."""
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
    parser.add_argument("--verify-scheduling", action="store_true", help="also preserve scheduler and quote batch offline evidence")
    parser.add_argument("--verify-ths-inputs", action="store_true", help="verify only the four existing THS board inputs")
    parser.add_argument("--verify-bao-inputs", action="store_true", help="verify only the two existing BaoStock snapshot inputs")
    args = parser.parse_args()
    if sum((args.verify_ths_inputs, args.verify_bao_inputs, args.verify_scheduling)) > 1:
        parser.error("THS, BaoStock and scheduling verification are separate scopes")
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

    import socket
    original_connect = socket.socket.connect
    def guarded_connect(sock, address):
        if isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}:
            return original_connect(sock, address)
        forbidden_network()

    with patch("requests.adapters.HTTPAdapter.send", forbidden_network), patch("socket.socket.connect", guarded_connect):
        cases = checks.BAO_CASES if args.verify_bao_inputs else checks.THS_CASES if args.verify_ths_inputs else checks.CASES
        for index, (input_id, context, manifest, count) in enumerate(cases):
            directory = args.output_root / (str(index + 1) if args.verify_ths_inputs or args.verify_bao_inputs else input_id)
            validator = (checks.test_bao_archived_inputs_preserve_sdk_rows_and_coverage if args.verify_bao_inputs else
                         checks.test_ths_archived_inputs_execute_source_yaml if args.verify_ths_inputs else
                         checks.test_archived_inputs_execute_yaml_and_preserve_evidence)
            validator(directory, None, input_id, context, manifest, count)
            report_path = next(directory.rglob("report.json"))
            report = json.loads(report_path.read_text(encoding="utf-8"))
            summary["inputs"].append({"input_id": input_id, "report_path": report_path.relative_to(ROOT).as_posix(),
                "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(), "source_manifest": manifest.relative_to(ROOT).as_posix(),
                "source_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(), "row_count": count,
                "scope": report["parameters"], "code_version": report["code_version"], "result": "passed"})
        if args.verify_bao_inputs:
            summary["known_differences"] = ["SDK解码载荷已精确归档；TCP帧、URL、HTTP头和SDK内部物理请求不可见。",
                "证券快照保留原筛选的5223只证券；行业归属5221行、缺失2只，不能宣称全量分类。",
                "新字段合同从原SDK两查询结果提取两种输出，旧行业方法的默认返回保持一致。",
                "分类更新时间来自updateDate，不使用查询日期替代；既有行业主键定义保持。",
                "离线回放及缓存的ResultSet仅复现SDK外部接口；没有替换在线SDK传输。"]
            summary["original_vs_provider"].append(checks.compare_bao_original(args.output_root / "c"))
            summary["empty_scope"] = checks.bao_empty_scope_check(args.output_root / "e")
            summary["negative_checks"] = []
            for index, (mutation, expected) in enumerate((("sdk_error", "connection"), ("missing_query", "ValueError"),
                                                         ("schema", "schema_changed"), ("duplicate", "schema_changed"),
                                                         ("corrupt_hash", "ValueError"), ("secret_payload", "ValueError"))):
                directory = args.output_root / ("f" + str(index + 1))
                checks.test_bao_failed_contract_retains_source_evidence(directory, None, mutation, expected)
                failure_path = next(directory.rglob("report.json"))
                summary["negative_checks"].append({"mutation": mutation, "expected_failure_class": expected, "result": "passed",
                    "report_path": failure_path.relative_to(ROOT).as_posix(), "report_sha256": hashlib.sha256(failure_path.read_bytes()).hexdigest()})
            checks.test_bao_live_fixture_preserves_session_pacing_and_reuses_cache(args.output_root / "p", None)
            summary["session_pacing_cache"] = {"mode": "injected SDK fixture; not live provider verification",
                "post_payload_interval_seconds": 3, "cache_reused_before_login": True, "result": "passed"}
        if args.verify_ths_inputs:
            summary["known_differences"] = ["原Provider所有旧字段、行数和窗口均对照；候选标准字段按YAML映射。",
                "行业量额、资金流金额单位未确认，来源值保留、标准列置空。快照时间来自原响应采集批次。",
                "SDK动态Cookie/hexin-v继续由原SDK生成；离线对照不认证当前来源接受度。",
                "回放匹配或留证错误归为迁移失败，不作为来源不可用；HTTP连接失败仍使用原失败类别。",
                "仅回放隔离SDK目录内存缓存，在线SDK缓存/Session/代理/重试不变；没有捕获SDK内部物理重试。"]
            for index, (input_id, context, manifest, count) in enumerate(cases):
                directory = args.output_root / ("c" + str(index + 1))
                comparison = checks.compare_ths_original(directory, input_id, context, manifest, count)
                summary["original_vs_provider"].append({**comparison, "comparison_path": (directory / "comparison.json").relative_to(ROOT).as_posix(),
                    "comparison_sha256": hashlib.sha256((directory / "comparison.json").read_bytes()).hexdigest()})
            failure_directory = args.output_root / "f"
            failure_directory.mkdir()
            checks.test_ths_replay_miss_is_migration_failure_and_period_is_bounded(failure_directory, None)
            failure_path = next(failure_directory.rglob("report.json"))
            failure = json.loads(failure_path.read_text(encoding="utf-8"))
            summary["negative_checks"] = {"strict_replay_miss": "passed", "immediate_period_only": "passed",
                "report_path": failure_path.relative_to(ROOT).as_posix(), "report_sha256": hashlib.sha256(failure_path.read_bytes()).hexdigest(),
                "failure_class": failure["failure_class"], "production_writes": failure["production_writes"]}
        for input_id, context, manifest, count in ([] if args.verify_ths_inputs or args.verify_bao_inputs else checks.CASES[:2]):
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
        for input_id, filename in ([] if args.verify_ths_inputs or args.verify_bao_inputs else [("ASTOCK-045", "43_东财涨停池"), ("ASTOCK-070", "68_交易日历")]):
            csv_path = ROOT / "provider_validation/results/live-probes/rate-limited-all-20261003" / filename / "data.csv"
            summary["original_vs_provider"].append({"input_id": input_id, "original_parsed_csv": csv_path.relative_to(ROOT).as_posix(),
                "original_parsed_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
                "all_source_fields_compared": True, "all_rows_equal": True,
                "comparison": "原响应经原SDK函数解析，所有来源列及行与原归档CSV比较；标准化字段另按YAML执行。"})
    if args.verify_scheduling:
        summary["scheduling"] = verify_scheduling(args.output_root)
    target = args.output_root / "comparison.json"
    target.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"result": "passed", "inputs": len(cases), "row_counts": [case[3] for case in cases], "comparison": str(target)}, ensure_ascii=False))


def verify_scheduling(output_root):
    import csv
    import gzip
    import shutil
    from datetime import date
    from zoneinfo import ZoneInfo
    import yaml
    from stock_data_manage.config.loader import load_provider_configs, load_collection_profiles
    from stock_data_manage.pipeline.inputs import collect_due_inputs
    from stock_data_manage.providers.tencent import TencentSnapshotProvider
    from stock_data_manage.providers.contracts import HttpResponse
    from stock_data_manage.storage.raw import RawObjectStore
    from stock_data_manage.worker.scheduler import collection_slot
    from stock_data_manage.domain.sessions import MarketSchedule

    sink = output_root / "scheduling"
    sink.mkdir()
    source = ROOT / "provider_validation/results/live-probes/pilot-20261003-tencent-quote-network"
    manifest = source / "_raw/missing-capabilities-20261003T173649/manifest.ndjson"
    event = next(json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()
                 if json.loads(line).get("url") == "https://qt.gtimg.cn/q=sh600519")
    body = gzip.decompress((manifest.parent / event["body_storage"]).read_bytes())
    assert hashlib.sha256(body).hexdigest() == event["body_sha256"]
    config = next(c for c in load_provider_configs(ROOT / "config/providers.yaml")
                  if c.provider == "tencent" and c.endpoint == "bulk_snapshot")
    scope_time = datetime(2026, 9, 30, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
    store = RawObjectStore(sink / "fixtures")
    requests = []
    class FixtureTransport:
        def get(self, url, *, params, timeout_seconds):
            symbols = url.split("q=", 1)[1].split(",")
            payload = b"\n".join(body.strip().replace(b"v_sh600519=", ("v_"+symbol+"=").encode()) for symbol in symbols)
            saved = store.write_bytes(payload, dataset="offline_quote_fixture", provider="tencent", endpoint="bulk_snapshot",
                fetched_at=datetime.now(timezone.utc), attempt_id="batch-"+str(len(requests)))
            requests.append({"url": url, "params": params, "timeout_seconds": timeout_seconds,
                "source_response_sha256": event["body_sha256"], "synthetic": symbols != ["sh600519"],
                "body_path": str(saved.path.relative_to(ROOT)), "body_sha256": saved.content_hash,
                "requested_symbols": symbols, "mode": "offline_fixture"})
            return HttpResponse(200, {"content-type": "text/html; charset=GBK"}, payload)
    provider = TencentSnapshotProvider(FixtureTransport(), config.capability())
    single = provider.fetch_snapshot(["sh600519"], scope_time)
    golden_path = source / "01_腾讯财经/data.csv"
    with golden_path.open(encoding="utf-8-sig", newline="") as file:
        golden = next(csv.DictReader(file))
    columns = {"name": "name", "close": "price", "pre_close": "prev_close", "open": "open",
               "volume": "volume_lot", "quote_time": "datetime", "high": "high", "low": "low"}
    assert all(single.rows[0][left] == golden[right] for left, right in columns.items())
    # Synthetic fan-out exercises traversal; it is explicitly not live batch evidence.
    symbols = tuple(f"sh{600000+i}" for i in range(203))
    batch = provider.fetch_snapshot(symbols, scope_time)
    assert batch.returned_symbols == set(symbols) and len(batch.rows) == 203
    assert [len(r["requested_symbols"]) for r in requests[1:]] == [100, 100, 3]
    rows = store.write_json(list(batch.rows), dataset="parsed_offline_quotes", provider="tencent", endpoint="bulk_snapshot",
        fetched_at=datetime.now(timezone.utc), attempt_id="parsed")
    # Use a minimal saved testing config; checked-in schedules remain disabled.
    config_root = sink / "config"
    config_root.mkdir()
    for name in ("collection.yaml", "providers.yaml", "datasets/minute_bar_5m.yaml", "normalization/minute_bar_5m.yaml"):
        target = config_root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / "config" / name, target)
    path = config_root / "collection.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["collection_profiles"]["tencent_minute_5m"]["scheduling_enabled"] = True
    path.write_text(yaml.safe_dump(document, allow_unicode=True, sort_keys=False), encoding="utf-8")
    tick_time = datetime(2026, 9, 30, 13, 5, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    kwargs = dict(config_root=config_root, output_root=sink / "candidate-ticks", trading_dates=[date(2026, 9, 30)],
        symbols=["sz300750"], execute=True,
        replay_manifest=ROOT / "provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson")
    tick = collect_due_inputs(now=tick_time, **kwargs)
    assert tick["jobs"][0]["status"] == "candidate_complete", tick
    assert tick["jobs"][0]["results"][0]["row_count"] == 96
    candidate = json.loads(Path(tick["jobs"][0]["results"][0]["report_path"]).read_text(encoding="utf-8"))
    assert candidate["live_http_calls"] == 0 and all(e["mode"] == "replay" for e in candidate["responses"])
    repeated = collect_due_inputs(now=tick_time, **kwargs)
    assert repeated["jobs"][0]["status"] == "already_attempted"
    profile = next(p for p in load_collection_profiles(ROOT / "config/collection.yaml") if p.name == "tencent_minute_5m")
    assert collection_slot(profile, tick_time.replace(hour=12), schedule=MarketSchedule(), trading_dates=[date(2026, 9, 30)]) is None
    assert collection_slot(profile, tick_time, schedule=MarketSchedule(), trading_dates=[]) is None
    result = {"mode": "offline", "network_requests": 0, "production_writes": 0,
        "eligible_for_production_routing": False, "validation_time_utc": datetime.now(timezone.utc).isoformat(),
        "source_manifest": str(manifest.relative_to(ROOT)), "source_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "original_csv": str(golden_path.relative_to(ROOT)), "original_csv_sha256": hashlib.sha256(golden_path.read_bytes()).hexdigest(),
        "original_response_sha256": event["body_sha256"], "single_quote_fields_compared": columns,
        "comparison_scope": "GBK decoding and parsed field values only; transport/session equivalence remains pending",
        "requests": requests, "batch_sizes": [100, 100, 3], "row_count": 203,
        "coverage_denominator": 203, "parsed_rows": str(rows.path.relative_to(ROOT)), "parsed_rows_sha256": rows.content_hash,
        "scheduler_tick": str(Path(tick["report_path"]).relative_to(ROOT)),
        "scheduler_tick_sha256": hashlib.sha256(Path(tick["report_path"]).read_bytes()).hexdigest(),
        "repeat_tick": str(Path(repeated["report_path"]).relative_to(ROOT)), "repeat_status": "already_attempted",
        "limitations": ["203证券为归档响应合成，仅证明离线分批遍历，不证明全市场在线能力。",
                        "快照仍迁移待完成，传输会话、实时批量和全市场覆盖未验证。",
                        "5分钟候选仅复用原样本，不认证当前周期已完成或行情新鲜度；未启用生产路由。"]}
    destination = sink / "verification.json"
    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    return {"report": str(destination.relative_to(ROOT)), "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}


if __name__ == "__main__":
    main()
