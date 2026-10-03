"""Attach evidence-backed status to the published a-stock-data capability inventory."""

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CAPABILITY_DIR = ROOT / "provider_validation" / "coverage"
RESULTS_DIR = ROOT / "provider_validation" / "results"
INVENTORY = CAPABILITY_DIR / "a-stock-data-capability-inventory.csv"
V39_RAW = "provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson"
V310_RAW = "provider_validation/results/raw/2026-10-01-v310-live/manifest.ndjson"

V39_NAMES = {
    "腾讯 K 线", "通达信盘后包", "新浪研报列表", "ETF 份额", "华尔街见闻快讯", "新闻联播",
    "ST 名单", "上证e互动", "中债收益率曲线", "回购定盘利率", "LPR", "全球宏观日历",
    "期货日行情", "期权日行情", "会员持仓排名", "实时期货", "A50 期指", "上海金现货",
    "业绩预告", "机构调研", "股东增减持", "股票回购", "股权质押", "新股申购日历", "可转债",
}
V38_NAMES = {"指数成分", "指数权重", "指数估值", "官方交易日历", "官方两融备胎", "北交所行情备胎"}


def norm(value):
    return value.replace("**", "").strip()


def status_for(row):
    name = norm(row["name"])
    limitations = row["documented_limitations"].lower()
    if row["counts_as_published_endpoint"] == "no":
        if "local derived" in row["relation"].lower():
            return ("local_cache_unverified", "SKILL.md §3.2；需另验证本地缓存的累积、去重和读取行为。", "")
        return ("shared_endpoint_variant_not_separately_live_verified",
                "README 行业研报行说明与 reportapi 共用端点，仅 qType 不同；本轮未单独请求该参数变体。", "")
    if name == "腾讯逐笔成交":
        return ("verified_live_raw_saved", "SKILL.md §1.4；V3.10 联网测试 26 项全通过。", V310_RAW)
    if name == "期货日 K":
        return ("verified_live_raw_saved", "SKILL.md §13.7；V3.10 联网测试 26 项全通过。", V310_RAW)
    if name in V39_NAMES:
        if name == "上证e互动":
            return ("partial_live_format_error_raw_saved",
                    "V3.9 live suite：源返回 HTTP 200，但一条回复时间值为“18:18”，解析报 RuntimeError；原始响应已存。",
                    V39_RAW)
        return ("verified_live_raw_saved", "V3.9 live suite：对应新能力的真实返回检查通过；原始 HTTP 报文已存。", V39_RAW)
    if name in V38_NAMES:
        return ("historical_live_raw_missing",
                "docs/source-integration-v3.8.0.md 记录过实测；旧测试日期和结果可查，但未保存原始报文，本轮未重复请求。", "")
    if name == "mootdx 行情（留档）":
        return ("documented_unavailable_or_partial",
                "README FAQ #52 / V3.9 整合记录：行情 K 线、盘口、逐笔返回 0 行；财务/F10 是独立能力。", "")
    if name == "F10 文本":
        return ("historical_live_raw_missing",
                "V3.9 整合记录：财务/F10 仍可用，且说明测试过内置服务器；原始协议数据未留存。", "")
    if "requires an iwencai api key" in limitations:
        return ("key_required_not_live_verified", "README：该能力需用户 API Key；本轮没有凭据，不做在线调用。", "")
    return ("historical_or_documented_only", "参见 README / SKILL 对应能力说明；没有找到可复查的本轮真实响应原文。", "")


with INVENTORY.open(encoding="utf-8-sig", newline="") as stream:
    rows = list(csv.DictReader(stream))
fields = list(rows[0])
for field in ("audit_status", "evidence_ref", "raw_evidence_ref", "preliminary_evidence_state"):
    if field not in fields:
        fields.append(field)

counts = Counter()
for row in rows:
    status, evidence, raw = status_for(row)
    row["audit_status"] = status
    row["evidence_ref"] = evidence
    row["raw_evidence_ref"] = raw
    row["preliminary_evidence_state"] = status
    counts[status] += 1

with INVENTORY.open("w", encoding="utf-8-sig", newline="") as stream:
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

summary = {
    "record_type": "capability_audit_summary",
    "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "inventory": INVENTORY.name,
    "published_endpoint_rows": sum(row["counts_as_published_endpoint"] == "yes" for row in rows),
    "non_endpoint_rows": sum(row["counts_as_published_endpoint"] == "no" for row in rows),
    "status_counts": dict(sorted(counts.items())),
    "offline_tests": {
        "total": 168, "passed": 165, "skipped_opt_in_live": 3, "failures": 0, "errors": 0,
        "interpretation": "offline contract tests do not independently verify current provider availability",
    },
    "live_runs": {
        "v39": {
            "test_suite_tests": 115, "test_errors": 1, "test_failure": "sse_e_interaction returned a reply-time value '18:18' that the parser rejected",
            "http_events": 54, "http_responses_saved": 50, "transport_errors": 4,
            "raw_manifest": V39_RAW,
            "test_log": "2026-10-01-v39-live-escalated.log",
        },
        "v310": {
            "test_suite_tests": 26, "test_failures": 0, "test_errors": 0,
            "http_events": 69, "http_responses_saved": 69,
            "raw_manifest": V310_RAW,
            "test_log": "2026-10-01-v310-live.log",
        },
    },
    "important_limit": "Historical V3.8 and older live results do not retain raw responses; they were not re-requested solely to recreate missing archives.",
}
(CAPABILITY_DIR / "a-stock-data-capability-audit-summary.json").write_text(
    json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
)
print(json.dumps({"rows": len(rows), "counts": dict(counts), "summary": summary["live_runs"]}, ensure_ascii=False, indent=2))
