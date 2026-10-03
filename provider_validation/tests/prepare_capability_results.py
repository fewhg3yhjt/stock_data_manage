"""Prepare readable capability rows and exact raw-response references."""

import csv
import json
import gzip
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[2]
CAPABILITY_DIR = ROOT / "provider_validation" / "coverage"
RESULTS_DIR = ROOT / "provider_validation" / "results"
INVENTORY = CAPABILITY_DIR / "a-stock-data-capability-inventory.csv"
OUTPUT = CAPABILITY_DIR / "capability-results-data.json"


def build_input_catalog():
    """Project reviewed contracts onto saved evidence; no provider calls or production writes."""
    sys.path.insert(0, str(ROOT / "src"))
    from stock_data_manage.config.loader import load_input_capabilities, load_collection_profiles
    from dataclasses import asdict

    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    config_path = ROOT / "config/providers.yaml"
    collection_path = ROOT / "config/collection.yaml"
    source_path = CAPABILITY_DIR / "interface-coverage.csv"
    contracts = load_input_capabilities(config_path)
    profiles = {profile.name: profile for profile in load_collection_profiles(collection_path)}
    with source_path.open(encoding="utf-8-sig", newline="") as stream:
        interfaces = {row["接口ID"]: row for row in csv.DictReader(stream)}
    successes = {ident for ident, row in interfaces.items() if row["接口取数结果"] in {"通过", "部分通过"}}
    covered = {ref for contract in contracts for ref in contract.evidence_refs}
    if covered != successes:
        raise ValueError(f"input coverage mismatch: missing={successes-covered}; extra={covered-successes}")
    checked = {}
    catalog = []
    csv_rows = []
    for contract in contracts:
        if contract.collection_profile not in profiles:
            raise ValueError(f"unknown collection profile: {contract.collection_profile}")
        records = []
        for ref in contract.evidence_refs:
            record_path = RESULTS_DIR / "interface-records" / f"{ref}.json"
            record = json.loads(record_path.read_text(encoding="utf-8"))
            if record["interface_id"] != ref or record["validation_result"] != interfaces[ref]["接口取数结果"]:
                raise ValueError(f"interface record mismatch: {ref}")
            for key in ("response_artifacts", "derived_artifacts", "source_code_artifacts"):
                for artifact in record.get(key, ()):
                    name = artifact["path"]
                    path = (ROOT / name).resolve()
                    if not path.is_relative_to(ROOT.resolve()):
                        raise ValueError("evidence must remain inside the validation workspace")
                    if name not in checked:
                        body = gzip.decompress(path.read_bytes()) if key == "response_artifacts" else path.read_bytes()
                        checked[name] = {"path": name, "sha256": hashlib.sha256(body).hexdigest(), "kind": key}
                    if checked[name]["sha256"] != artifact["sha256"]:
                        raise ValueError(f"evidence hash mismatch: {name}")
            records.append({
                "interface_id": ref, "path": record_path.relative_to(ROOT).as_posix(), "sha256": digest(record_path),
                "validation_result": record["validation_result"], "evidence_status": record["evidence_status"],
                "tested_scope": record["scope"], "coverage_denominator": record["coverage_denominator"],
                "original_note": record["finding"], "manifest_ref": record["manifest_ref"],
                "response_artifacts": record.get("response_artifacts", []),
                "derived_artifacts": record.get("derived_artifacts", []),
                "source_code_artifacts": record.get("source_code_artifacts", []),
                "validation_time_utc": record["validation_time_utc"],
                "original_transformation": record["transformation_code_version"],
            })
        catalog.append({"contract": asdict(contract), "collection_profile": asdict(profiles[contract.collection_profile]),
                        "evidence": records, "eligible_for_production_routing": False})
        csv_rows.append({
            "输入ID": contract.input_id, "验证接口ID": "; ".join(contract.evidence_refs),
            "正式能力名称": contract.display_name, "实际来源": contract.provider, "端点": contract.endpoint,
            "数据形态": contract.data_kind, "数据周期": contract.data_frequency, "请求形态": contract.request_shape,
            "参数来源": "; ".join(f"{p.name}<-{p.source} ({p.value_type}; {'必填' if p.required else '可选'})" for p in contract.parameters),
            "采集意图": contract.collection_profile, "刷新间隔秒": profiles[contract.collection_profile].refresh_interval_seconds,
            "请求间隔秒": contract.request_interval_seconds, "并发": contract.effective_concurrency,
            "限速实现状态": contract.request_limit_enforcement, "实现状态": contract.implementation_status,
            "对应现有适配器": contract.runtime_endpoint or "", "归并到": contract.canonical_input or "",
            "证据范围": "; ".join(r["tested_scope"] for r in records), "限制": "; ".join(contract.limitations),
            "证据记录": "; ".join(r["path"] for r in records), "生产路由资格": "未授予",
        })
    summary = {
        "record_type": "successful_input_capability_catalog", "validation_time_utc": datetime.now(timezone.utc).isoformat(),
        "input_count": len(contracts), "successful_interface_count": len(covered), "coverage_denominator": len(successes),
        "original_inventory_denominator": 87, "network_requests": 0, "production_writes": 0,
        "inputs": [{"path": p.relative_to(ROOT).as_posix(), "sha256": digest(p)} for p in (config_path, collection_path, source_path)],
        "transformation_code": {"path": Path(__file__).relative_to(ROOT).as_posix(), "sha256": digest(Path(__file__))},
        "hash_checked_files": list(checked.values()), "catalog": catalog,
    }
    # Only references/scopes are copied, never raw request headers or response samples.
    # Strip token-like values should an existing scope/note include them.
    import re
    def redact_reference(value):
        return re.sub(r'(?i)([?&](?:ut|token|access_token)=)[^&\s;"\\]+', r'\1[REDACTED]', value)

    summary["reference_redaction_policy"] = "Token-like query values in copied references are redacted; original archives are unchanged."
    serialized = json.dumps(summary, ensure_ascii=False, indent=2)
    serialized = redact_reference(serialized)
    json_output = CAPABILITY_DIR / "successful-input-capabilities.json"
    csv_output = CAPABILITY_DIR / "successful-input-capabilities.csv"
    json_output.write_text(serialized + "\n", encoding="utf-8")
    with csv_output.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(csv_rows[0]))
        writer.writeheader()
        writer.writerows({key: redact_reference(value) if isinstance(value, str) else value
                          for key, value in row.items()} for row in csv_rows)
    print(json.dumps({"input_count": len(contracts), "successful_interfaces": len(covered), "hash_checked_files": len(checked),
                      "network_requests": 0, "output": json_output.relative_to(ROOT).as_posix()}, ensure_ascii=False))


def clean_name(value):
    return value.replace("**", "").strip()


def map_capability(record):
    parts = urlsplit(record["url"])
    host, path = parts.hostname or "", parts.path
    query = parse_qs(parts.query)
    if "V3.10" in record.get("scope", {}).get("test_group", ""):
        return "期货日 K" if "futures/api/jsonp.php" in path else "腾讯逐笔成交"
    if host in {"web.ifzq.gtimg.cn", "web3.ifzq.gtimg.cn", "proxy.finance.qq.com", "ifzq.gtimg.cn"}:
        return "腾讯 K 线"
    if host == "www.tdx.com.cn":
        return "通达信盘后包"
    if "vReport_List" in path:
        return "新浪研报列表"
    if host == "query.sse.com.cn":
        return "ETF 份额"
    if host == "api-one-wscn.awtmt.com":
        return "全球宏观日历" if "macrodatas" in path else "华尔街见闻快讯"
    if host == "tv.cctv.com":
        return "新闻联播"
    if host in {"push2.eastmoney.com", "push2delay.eastmoney.com"}:
        return "ST 名单"
    if host == "sns.sseinfo.com":
        return "上证e互动"
    if host == "yield.chinabond.com.cn":
        return "中债收益率曲线"
    if host == "www.chinamoney.com.cn":
        return "回购定盘利率"
    if host == "datacenter-web.eastmoney.com":
        report = query.get("reportName", [""])[0]
        return {
            "RPTA_WEB_RATE": "LPR",
            "RPT_PUBLIC_OP_NEWPREDICT": "业绩预告",
            "RPT_ORG_SURVEYNEW": "机构调研",
            "RPT_SHARE_HOLDER_INCREASE": "股东增减持",
            "RPTA_WEB_GETHGLIST_NEW": "股票回购",
            "RPT_CSDC_LIST": "股权质押",
            "RPTA_APP_IPOAPPLY": "新股申购日历",
            "RPT_BOND_CB_LIST": "可转债",
        }.get(report, "未映射的数据中心请求: " + report)
    if "dailydata" in path and "/option/" in path:
        return "期权日行情"
    if "dailydata" in path and "/future/" in path:
        return "期货日行情"
    if "FutureDataDaily.txt" in path or "/fzjy/mrhq/" in path or path.endswith("/loadList"):
        return "期货日行情"
    if host == "www.cffex.com.cn" and "/sj/ccpm/" in path:
        return "会员持仓排名"
    if host == "hq.sinajs.cn" and "nf_" in path:
        return "实时期货"
    if host == "hq.sinajs.cn" and "CHA50" in path:
        return "A50 期指"
    if host == "www.sge.com.cn":
        return "上海金现货"
    return "未映射: " + host + path


def explanation(row):
    state = row["audit_status"]
    limitations = clean_name(row["documented_limitations"])
    if state == "verified_live_raw_saved":
        scope = {
            "腾讯 K 线": "本轮测了沪市日K与深市5分钟K；历史日期2026-09-18。",
            "通达信盘后包": "本轮测了2026-09-18全市场盘后包。",
            "ETF 份额": "本轮联网样例仅覆盖上交所日期数据，未测深交所当前快照。",
            "期货日行情": "本轮测了 SHFE、INE、CZCE、CFFEX、GFEX，日期2026-09-18。",
            "期权日行情": "本轮样例覆盖 SHFE 与 CFFEX，其他交易所未在本轮实测。",
            "会员持仓排名": "本轮样例覆盖 CFFEX；其他交易所未在本轮实测。",
            "腾讯逐笔成交": "本轮用深市000001样例，获取并核验成交序号、日期和成交额。",
            "期货日 K": "本轮实测 RB0、M0 两个合约。",
        }.get(clean_name(row.get("name") or row.get("能力项", "")), "对应联网测试返回非空数据并通过字段/来源检查；范围见原始请求清单。")
        return "通过。" + scope + (" 文档边界：" + limitations if limitations else "")
    if state == "partial_live_format_error_raw_saved":
        return "未通过。HTTP请求有响应，但页面含相对时间“今天 18:18”，解析器无法识别并报错；原始页面已保存，可离线复核。"
    if state == "historical_live_raw_missing":
        return "未通过（证据不足）。上游文档记载过历史实测，但没有保存原始返回；为避免重复请求，本轮未重拉。"
    if state == "documented_unavailable_or_partial":
        return "未通过。文档记载该行情通道自2026-09起返回空数据；财务/F10是单独能力。"
    if state == "key_required_not_live_verified":
        return "未通过（未验证）。此能力需要 iwencai API Key，本轮无凭据。"
    if state == "local_cache_unverified":
        return "未通过（未验证）。这是本地缓存功能，不是独立接口；本轮没有验证缓存累积、去重和读取。"
    if state == "shared_endpoint_variant_not_separately_live_verified":
        return "未通过（未验证）。与个股研报共用同一接口，仅查询参数不同；本轮没有单独请求这个参数变体。"
    return "未通过（未验证）。只有 README/SKILL 文档说明；未找到本轮可复查的真实返回，不能据此认定当前可用。"



def prepare_legacy_results():
    with INVENTORY.open(encoding="utf-8-sig", newline="") as f:
        source_rows = list(csv.DictReader(f))
    rows = []
    for i, source in enumerate(source_rows, 1):
        name = clean_name(source.get("name") or source.get("能力项", ""))
        audit_status = source.get("audit_status") or source.get("原审计状态", "")
        endpoint_flag = source.get("counts_as_published_endpoint")
        if endpoint_flag is None:
            endpoint_flag = "yes" if source.get("是否计入87项") == "是" else "no"
        result = "通过" if audit_status == "verified_live_raw_saved" else "未通过"
        rows.append({
            "序号": i,
            "是否计入87项": "是" if endpoint_flag == "yes" else "否（说明行）",
            "分类": source.get("category") or source.get("分类", ""),
            "能力项": name,
            "验证结果": result,
            "备注": explanation(source),
            "原始返回文件": "待映射",
            "清单原始行": source.get("readme_line", source.get("清单原始行", "")),
            "原审计状态": audit_status,
            "证据说明": source.get("evidence_ref") or source.get("证据说明", ""),
        })

    index_rows = []
    for run in ("2026-10-01-v39-live-escalated", "2026-10-01-v310-live"):
        manifest = RESULTS_DIR / "raw" / run / "manifest.ndjson"
        for line_no, line in enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1):
            record = json.loads(line)
            evidence_id = ("V39" if "v39" in run else "V310") + f"-{line_no:03d}"
            mapped = map_capability(record)
            index_rows.append({
                "证据编号": evidence_id,
                "能力项": mapped,
                "运行批次": run,
                "抓取时间UTC": record.get("fetched_at_utc", ""),
                "HTTP状态": record.get("status_code", ""),
                "结果": record.get("outcome", ""),
                "原始响应文件": (str((RESULTS_DIR / "raw" / run / record["body_storage"]).resolve())
                              if record.get("body_storage") else "无响应体；错误见同批次manifest.ndjson"),
                "SHA256": record.get("body_sha256", ""),
                "原始请求URL": record.get("url", ""),
            })

    ids_by_name = {}
    for record in index_rows:
        ids_by_name.setdefault(record["能力项"], []).append(record["证据编号"])
    events_by_name = {}
    for record in index_rows:
        events_by_name.setdefault(record["能力项"], []).append(record)

    for row in rows:
        events = events_by_name.get(row["能力项"], [])
        ids = ids_by_name.get(row["能力项"], [])
        files = [event["原始响应文件"] for event in events if event["原始响应文件"].startswith("D:")]
        errors = [event["证据编号"] for event in events if not event["原始响应文件"].startswith("D:")]
        if files:
            row["原始返回文件"] = "; ".join(files)
            if errors:
                row["原始返回文件"] += "; 传输失败记录见对应批次 manifest.ndjson：" + ", ".join(errors)
        elif events:
            row["原始返回文件"] = "无响应体；错误见对应批次 manifest.ndjson：" + ", ".join(errors)
        elif not ids:
            if row["验证结果"] == "通过":
                row["原始返回文件"] = "未找到逐项映射（需复核）"
            elif row["原审计状态"] == "historical_live_raw_missing":
                row["原始返回文件"] = "无：历史调用未留存原文；见备注"
            elif row["原审计状态"] == "historical_or_documented_only":
                row["原始返回文件"] = "无：本轮未请求；见备注"
            else:
                row["原始返回文件"] = "无：本轮没有该项的原始响应"

    # Put the decision-facing fields first so a CSV opened directly is readable.
    front = ["序号", "是否计入87项", "分类", "能力项", "验证结果", "备注", "原始返回文件"]
    metadata = ["claimed_data", "relation", "audit_status", "evidence_ref", "raw_evidence_ref",
                "readme_line", "documented_limitations", "preliminary_evidence_state"]
    tail = ["清单原始行", "原审计状态", "证据说明"] + metadata
    with INVENTORY.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=front + tail, extrasaction="ignore")
        writer.writeheader()
        for row, source in zip(rows, source_rows):
            combined = dict(row)
            combined.update({"清单原始行": source.get("readme_line", row["清单原始行"]),
                             "原审计状态": source.get("audit_status", row["原审计状态"]),
                             "证据说明": source.get("evidence_ref", row["证据说明"])})
            for key in metadata:
                combined[key] = source.get(key, "")
            writer.writerow(combined)

    OUTPUT.write_text(
        json.dumps({"capabilities": rows, "raw_events": index_rows}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"capability_rows": len(rows), "counted": sum(r["是否计入87项"] == "是" for r in rows),
                      "pass": sum(r["验证结果"] == "通过" and r["是否计入87项"] == "是" for r in rows),
                      "fail": sum(r["验证结果"] == "未通过" and r["是否计入87项"] == "是" for r in rows),
                      "raw_events": len(index_rows), "mapped": sum(not r["能力项"].startswith("未映射") for r in index_rows)},
                     ensure_ascii=False, indent=2))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Prepare saved provider evidence without network requests')
    parser.add_argument('--input-catalog', action='store_true')
    args = parser.parse_args()
    if args.input_catalog:
        build_input_catalog()
    else:
        prepare_legacy_results()
