#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build the reproducible interface-level validation report from retained evidence."""

from __future__ import annotations

import ast
import csv
import gzip
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[2]
VALIDATION = ROOT / "provider_validation"
INVENTORY = VALIDATION / "coverage" / "a-stock-data-capability-inventory.csv"
SUMMARY = VALIDATION / "results" / "a-stock-data-missing-output" / "_summary.csv"
LIVE_PROBES_ROOT = VALIDATION / "results" / "live-probes"
PROBE_SOURCE = VALIDATION / "tests" / "source_snapshots" / "a-stock-data" / "a_stock_missing_capabilities.py"
SKILL = VALIDATION / "tests" / "source_snapshots" / "a-stock-data" / "SKILL.md"
OUT_CSV = VALIDATION / "coverage" / "interface-coverage.csv"
RECORDS_DIR = VALIDATION / "results" / "interface-records"

FIELDS = [
    "接口ID", "清单范围", "是否计入87项", "项目/来源", "类别", "接口/能力名称", "接口地址/协议",
    "接口/调用符号", "接口说明", "能力说明（文档声明）/返回字段", "调用方式/参数范围",
    "来源代码文件/行号", "代码SHA-256", "测试代码文件/行号", "项目Provider对照代码",
    "接口取数结果", "本轮补抓脚本结果", "证据完整度", "实测范围/日期/结果", "验证备注/数据核验结论",
    "原始响应Manifest", "原始响应文件/哈希", "解析结果/输出文件", "结果文件SHA-256/行数", "返回示例",
    "逐接口结果记录", "来源版本/提交", "证据状态代码", "证据状态说明"
]

EVIDENCE_STATUS_LABELS = {
    "live_raw": "实时原始响应已归档；仅代表本行注明的实测范围",
    "parsed_only": "仅保存解析结果；没有对应的原始响应",
    "historical_parsed_only": "仅有历史解析/合并结果；未做本轮实时验证",
    "local_cache_unverified": "存在本地缓存；未验证实时接口",
    "failed": "本轮探针或数据校验失败",
    "unavailable": "接口当前不可用或受到访问阻断",
    "credential_required": "需要凭据；本轮未能验证",
    "shared_endpoint_variant_not_separately_live_verified": "共用接口的该参数变体未单独实时验证",
    "undetermined": "证据状态未能判定",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def rel(path: Path | str | None) -> str:
    if not path:
        return ""
    p = Path(str(path))
    if not p.is_absolute():
        p = ROOT / p
    try:
        return p.resolve().relative_to(ROOT.resolve()).as_posix()
    except (ValueError, OSError):
        return str(path).replace("\\", "/")


def short(text: Any, n: int = 600) -> str:
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", str(text or ""))
    s = re.sub(r"\s+", " ", cleaned).strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def output_artifacts(path_text: str) -> list[dict[str, Any]]:
    artifacts = []
    for token in (path_text or "").split(";"):
        p = ROOT / token.strip()
        if not p.is_file():
            continue
        item: dict[str, Any] = {"path": rel(p), "sha256": sha256(p), "bytes": p.stat().st_size}
        try:
            if p.suffix.lower() == ".csv":
                with p.open(encoding="utf-8-sig", newline="") as f:
                    reader = csv.reader(f)
                    header = next(reader, [])
                    item["field_names"] = header
                    item["row_count"] = sum(1 for _ in reader)
            elif p.suffix.lower() == ".json":
                data = json.loads(p.read_text(encoding="utf-8"))
                item["row_count"] = len(data) if isinstance(data, list) else None
        except Exception as e:
            item["inspection_error"] = f"{type(e).__name__}: {e}"
        artifacts.append(item)
    return artifacts


def code_hashes(code_ref: str, test_ref: str, provider_ref: str) -> list[dict[str, str]]:
    refs = " ".join([code_ref or "", test_ref or "", provider_ref or ""])
    paths = re.findall(r"(?:provider_validation|src|tests)/[^\s;:()]+", refs)
    found = []
    for name in dict.fromkeys(paths):
        p = ROOT / name
        if p.is_file():
            found.append({"path": name, "sha256": sha256(p)})
    return found


def response_hashes(body_ref: str) -> list[dict[str, Any]]:
    found = []
    for token in (body_ref or "").split(";"):
        relpath = token.strip()
        p = ROOT / relpath
        if not p.is_file():
            continue
        digest = p.name.split(".")[0]
        try:
            with gzip.open(p, "rb") as f:
                h = hashlib.sha256()
                for block in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(block)
                actual = h.hexdigest()
            found.append({"path": relpath, "sha256": actual, "matches_filename": actual == digest})
        except OSError as e:
            found.append({"path": relpath, "sha256": "", "matches_filename": False, "error": str(e)})
    return found


def build_manifest_body_index() -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for manifest in (VALIDATION / "results" / "raw").glob("*/manifest.ndjson"):
        for line in manifest.read_text(encoding="utf-8").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            digest = event.get("body_sha256")
            if digest:
                index[digest] = {"manifest": rel(manifest), "url": event.get("url", ""), "method": event.get("method", ""), "status": event.get("status_code"), "scope": event.get("scope", {}), "fetched_at_utc": event.get("fetched_at_utc", "")}
    return index


def urls_from_function(path: Path, function: str) -> str:
    if not function or not re.fullmatch(r"[A-Za-z_]\w*", function):
        return ""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    node = next((n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == function), None)
    if node is None:
        return ""
    segment = ast.get_source_segment(path.read_text(encoding="utf-8"), node) or ""
    urls = re.findall(r"https?://[^\s'\"`<>]+", segment)
    return "; ".join(dict.fromkeys(x.rstrip(",)]") for x in urls[:5]))


def normalize_name(s: str) -> str:
    return re.sub(r"[\W_]+", "", s, flags=re.UNICODE).lower()


def code_index(path: Path) -> dict[str, int]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {n.name: n.lineno for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def upstream_capability_functions(path: Path) -> dict[int, tuple[str, str, int]]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    funcs = {n.name: n.lineno for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    mapping: dict[int, tuple[str, str, int]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(t, ast.Name) and t.id == "CAPABILITIES" for t in targets):
            continue
        if not isinstance(node.value, ast.Dict):
            continue
        for k, v in zip(node.value.keys, node.value.values):
            if not isinstance(k, ast.Constant) or not isinstance(k.value, int):
                continue
            if not isinstance(v, ast.Tuple) or len(v.elts) < 2:
                continue
            call = v.elts[1]
            if isinstance(call, ast.Name):
                mapping[k.value] = (call.id, str(PROBE_SOURCE.relative_to(ROOT).as_posix()), funcs.get(call.id, 1))
    return mapping


PROBE_NAME_BY_INVENTORY = {
    "mootdx 行情（留档）": "mootdx行情",
    "东财 reportapi": "东财reportapi",
    "东财 PDF 下载": "东财PDF下载",
    "iwencai NL 搜索": "iwencai自然语言搜索",
    "同花顺北向（实时）": "北向/沪深港通当前资金汇总",
    "东财资金流向": "东财个股资金流向",
    "个股资金流120日": "个股资金流近100~120日",
    "筹码分布 CYQ": "筹码分布CYQ",
    "F10 文本": "F10结构化资料",
    "同花顺涨停揭秘": "同花顺涨停揭秘/涨停原因替代",
    "东财重点监控池": "东财强势/重点监控替代池",
    "东财日内异动池（明细接口）": "东财日内异动池明细",
    "东财日内异动池（按标的统计接口）": "东财日内异动按标的统计",
    "T型报价": "期权T型报价",
    "希腊字母 + IV": "期权Greeks/风险指标",
    "同花顺热榜": "同花顺/问财热榜",
    "东财个股概念命中": "东财个股概念/关键词命中",
    "国家统计局 PMI": "PMI",
    "官方交易日历": "交易日历",
    "官方龙虎榜备胎": "龙虎榜备胎",
}

UPSTREAM_API_SYMBOLS = {
    "腾讯财经": "tencent_quote(codes)", "腾讯 K 线": "tencent_kline(code, period, adjust, start, end, count)",
    "通达信盘后包": "tdx_daily_package(date)", "腾讯逐笔成交": "tencent_ticks(code)", "百度K线": "baidu_kline_with_ma(code)",
    "新浪复权因子": "sina_adjust_factor(code, kind) / apply_adjust(bars, factors)", "mootdx 行情（留档）": "tdx_client() → .bars() / .quotes() / .transaction()",
    "东财 reportapi": "eastmoney_reports(code) / download_pdf(rec)", "东财 行业研报": "eastmoney_industry_reports(industry_code)",
    "东财 PDF 下载": "download_pdf(rec)", "同花顺一致预期": "ths_eps_forecast(code)", "iwencai NL 搜索": "iwencai_search(query) / iwencai_query(query)",
    "新浪研报列表": "sina_research_reports(code=None, page=1)", "同花顺热点": "ths_hot_reason()", "同花顺北向（实时）": "hsgt_realtime()",
    "同花顺北向（历史）": "本地北向缓存读取（非远程接口；需要独立缓存读写测试）", "东财板块归属": "eastmoney_concept_blocks(code)",
    "东财资金流向": "eastmoney_fund_flow_minute(code)", "龙虎榜席位": "dragon_tiger_board(code, date)",
    "全市场龙虎榜": "daily_dragon_tiger(date)", "限售解禁日历": "lockup_expiry(code, date)", "行业板块排名": "industry_comparison()",
    "板块资金流向": "board_fund_flow(board_type, period)", "融资融券明细": "margin_trading(code)", "大宗交易": "block_trade(code)",
    "股东户数变化": "holder_num_change(code)", "分红送转历史": "dividend_history(code)", "个股资金流120日": "stock_fund_flow_120d(code)",
    "筹码分布 CYQ": "chip_distribution(df)", "ETF 份额": "etf_shares(date, exchange)", "个股新闻": "eastmoney_stock_news(code)",
    "财联社电报": "cls_telegraph()", "全球资讯": "eastmoney_global_news()", "华尔街见闻快讯": "wallstreetcn_lives(channel, limit, cursor)",
    "新闻联播": "cctv_news(date, with_content=True)", "季报快照": "tdx_client(check='finance').finance(symbol)",
    "F10 文本": "tdx_client(check='finance').F10(symbol, name)", "东财个股信息": "eastmoney_stock_info(code)",
    "新浪财报三表": "sina_financial_report(code, type)", "巨潮公告": "cninfo_announcements(code)",
    "估值历史": "baostock_valuation_history(code, s, e)", "上市/退市日": "baostock_stock_basic(code)",
    "申万行业变迁史": "sw_industry_history() / sw_industry_as_of(df, code, d)", "ST 名单": "st_stock_list()",
    "东财涨停池": "em_zt_pool(date)", "东财炸板池": "em_zb_pool(date)", "东财跌停池": "em_dt_pool(date)",
    "东财昨日涨停池": "em_yzt_pool(date)", "同花顺涨停揭秘": "ths_limit_up_pool(date)", "东财重点监控池": "em_stock_monitor()",
    "东财日内异动池（明细接口）": "em_price_anomaly()", "东财日内异动池（按标的统计接口）": "em_price_anomaly_count()",
    "期权合约清单": "sina_option_codes(underlying, call, month)", "T型报价": "sina_option_tquote(underlying, call, month)",
    "希腊字母 + IV": "sina_option_greeks(underlying, call, month)", "互动易问答": "cninfo_irm(code)",
    "上证e互动": "sse_e_interaction(code=None, kind='answered')", "同花顺热榜": "ths_hot_list()",
    "东财人气榜": "em_hot_rank()", "东财个股概念命中": "em_hot_concept(code)",
    "人民银行社融": "pboc_social_financing(year)", "国家统计局 PMI": "nbs_pmi()", "中债收益率曲线": "chinabond_yield_curve(start, end, curve)",
    "回购定盘利率": "repo_fixing_rates(kind)", "LPR": "lpr_history()", "全球宏观日历": "macro_calendar(start, end, country, min_importance)",
    "指数成分": "index_constituents(index_code, provider)", "指数权重": "index_weights(index_code, provider)",
    "指数估值": "index_valuation(index_code)", "官方交易日历": "trading_calendar(year, month)",
    "期货日行情": "futures_daily(date, exchange)", "期权日行情": "options_daily(date, exchange)",
    "会员持仓排名": "futures_position_rank(date, exchange, symbol)", "实时期货": "futures_realtime(symbols)",
    "期货日 K": "futures_kline(symbol, start, end)", "A50 期指": "a50_futures()", "上海金现货": "sge_spot(instrument)",
    "业绩预告": "earnings_forecast(code, report_date, limit)", "机构调研": "institution_survey(code, start, end, detail, limit)",
    "股东增减持": "holder_trades(code, direction, start, end, limit)", "股票回购": "share_buyback(code, progress, limit)",
    "股权质押": "equity_pledge(code, date, limit)", "新股申购日历": "ipo_calendar(limit)",
    "可转债": "convertible_bonds(include_delisted=False)", "官方龙虎榜备胎": "dragon_tiger_backup()",
    "资金流备胎": "fund_flow_backup()", "公告备胎": "announcements_backup()",
    "官方两融备胎": "margin_trading_backup(date, exchange, code=None)", "北交所行情备胎": "bse_quote_backup(date, code=None)",
}


def api_doc_line(symbol: str, skill_lines: list[str]) -> int | None:
    if not symbol:
        return None
    names = re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(", symbol)
    route_lines = range(min(220, len(skill_lines)), min(310, len(skill_lines)))
    for name in names:
        for i in route_lines:
            if f"`{name}" in skill_lines[i] or f"{name}" in skill_lines[i]:
                return i + 1
    for name in names:
        for i, line in enumerate(skill_lines):
            if name in line:
                return i + 1
    return None


def examples_for_file(path_text: str) -> str:
    if not path_text:
        return ""
    candidates = [ROOT / p.strip() for p in path_text.split(";") if p.strip()]
    for p in candidates:
        if not p.exists() or p.is_dir():
            continue
        try:
            if p.suffix.lower() == ".csv":
                with p.open(encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    first = next(reader, None)
                    if first is None:
                        return f"CSV表头={','.join(reader.fieldnames or [])}; 0条数据"
                    return short(json.dumps({"columns": reader.fieldnames, "sample": first}, ensure_ascii=False), 900)
            if p.suffix.lower() == ".json":
                data = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    data = {"items": data[:1], "total": len(data)}
                return short(json.dumps(data, ensure_ascii=False), 900)
            return short(p.read_text(encoding="utf-8", errors="replace"), 700)
        except Exception as e:
            return f"样例读取失败：{type(e).__name__}: {e}"
    return ""


def example_from_raw(path_text: str) -> str:
    if not path_text:
        return ""
    import gzip
    for token in path_text.split(";"):
        p = ROOT / token.strip()
        if not p.exists() or not p.is_file():
            continue
        try:
            raw = gzip.open(p, "rb").read(8192) if p.suffix == ".gz" else p.read_bytes()[:8192]
            decoded = raw.decode("utf-8", errors="replace")
            return short(decoded, 700)
        except Exception as e:
            return f"原始响应片段读取失败：{type(e).__name__}: {e}"
    return ""


def kline_period_samples(path_text: str) -> str:
    """Summarize the retained daily and minute K-line responses independently."""
    samples = []
    for token in path_text.split(";"):
        p = ROOT / token.strip()
        if not p.is_file():
            continue
        try:
            raw = gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
            for symbol, item in payload.get("data", {}).items():
                if not isinstance(item, dict):
                    continue
                for period in ("qfqday", "day", "m5"):
                    bars = item.get(period)
                    if isinstance(bars, list) and bars:
                        label = {"qfqday": "日线（前复权）", "day": "日线", "m5": "5分钟"}[period]
                        samples.append(f"{label} {symbol}: {len(bars)}条；首条={json.dumps(bars[0], ensure_ascii=False)}")
        except Exception as e:
            samples.append(f"样例读取失败 {p.name}: {type(e).__name__}: {e}")
    return "；".join(samples) or "无可解析K线样例；查看原始响应文件/哈希"


def raw_refs(row: dict[str, str]) -> tuple[str, str]:
    manifest = []
    bodies = []
    for key in ("原始返回文件", "本轮原始返回文件", "raw_evidence_ref"):
        raw = row.get(key, "") or ""
        for token in re.split(r";\s*", raw):
            token = token.strip().strip("`")
            if not token or token.startswith(("无", "未", "不", "参见", "README", "SKILL", "历史")):
                continue
            token = token.replace("D:\\Project\\PythonProgram\\stock_data_manage\\", "")
            if "manifest.ndjson" in token:
                manifest.extend(x.strip() for x in token.split(";") if "manifest.ndjson" in x)
            elif "body" in token or token.endswith((".gz", ".jsonl", ".json")):
                bodies.append(token.replace("\\", "/"))
    def fix_ref(x: str) -> str:
        x = x.strip().replace("\\", "/")
        marker = "provider_validation/"
        return x[x.find(marker):] if marker in x else x
    return "; ".join(dict.fromkeys(fix_ref(x) for x in manifest)), "; ".join(dict.fromkeys(fix_ref(x) for x in bodies))


def load_live_probe_results() -> dict[int, dict[str, Any]]:
    """Load the newest completed, persisted low-frequency probe result per upstream capability ID."""
    latest: dict[int, dict[str, Any]] = {}
    if not LIVE_PROBES_ROOT.is_dir():
        return latest
    for run_dir in LIVE_PROBES_ROOT.iterdir():
        policy_path = run_dir / "probe-run-policy.json"
        summary_path = run_dir / "_summary.csv"
        if not policy_path.is_file() or not summary_path.is_file():
            continue
        try:
            policy = json.loads(policy_path.read_text(encoding="utf-8"))
            probe_rows = read_csv(summary_path)
            manifests = sorted((run_dir / "_raw").glob("*/manifest.ndjson"))
        except (OSError, json.JSONDecodeError):
            continue
        for probe_row in probe_rows:
            try:
                probe_id = int(probe_row["id"])
            except (KeyError, ValueError):
                continue
            events: list[dict[str, Any]] = []
            manifest_refs: list[str] = []
            body_refs: list[str] = []
            for manifest in manifests:
                matching = []
                for line in manifest.read_text(encoding="utf-8").splitlines():
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    cap = str(event.get("scope", {}).get("capability", ""))
                    match = re.match(r"^(\d+):", cap)
                    if not match or int(match.group(1)) != probe_id:
                        continue
                    matching.append(event)
                    body_storage = event.get("body_storage")
                    if body_storage:
                        body_path = (manifest.parent / str(body_storage)).resolve()
                        if body_path.is_file():
                            body_refs.append(rel(body_path))
                if matching:
                    events.extend(matching)
                    manifest_refs.append(rel(manifest))
            scope_parts = []
            capability_events = [e for e in events if e.get("scope", {}).get("capability", "").startswith(f"{probe_id:02d}:")]
            if capability_events:
                first_scope = capability_events[0].get("scope", {})
                params = [f"{key}={first_scope[key]}" for key in ("code", "date", "start", "end") if first_scope.get(key)]
                if params:
                    scope_parts.append("参数范围：" + ", ".join(params))
                outcomes = Counter(
                    (f"HTTP{e.get('status_code')}" if e.get("outcome") == "response" else str(e.get("outcome", "unknown")))
                    for e in capability_events
                )
                scope_parts.append("请求事件：" + ", ".join(f"{k}×{v}" for k, v in sorted(outcomes.items())))
                hosts = sorted({urlparse(str(e.get("url", ""))).hostname or "unknown" for e in capability_events if e.get("url")})
                if hosts:
                    scope_parts.append("主机：" + ", ".join(hosts))
            scope_parts.append(f"低频探测批次：{run_dir.name}；策略={policy.get('policy_version', 'unknown')}；每主机最短间隔{policy.get('min_interval_seconds_per_hostname', 'n/a')}秒")
            entry = {
                **probe_row,
                "probe_id": probe_id,
                "policy": policy,
                "run_dir": run_dir,
                "manifest_ref": "; ".join(dict.fromkeys(manifest_refs)),
                "body_ref": "; ".join(dict.fromkeys(body_refs)),
                "events": events,
                "scope_summary": "；".join(scope_parts),
                "created_at_utc": str(policy.get("created_at_utc", "")),
                "files": [x.strip() for x in str(probe_row.get("files", "")).split(";") if x.strip()],
            }
            current = latest.get(probe_id)
            if current is None or entry["created_at_utc"] > current["created_at_utc"]:
                latest[probe_id] = entry
    return latest


def main() -> None:
    inventory = read_csv(INVENTORY)
    summary_rows = read_csv(SUMMARY)
    summary_by_id = {int(r["id"]): r for r in summary_rows}
    live_probe_by_id = load_live_probe_results()
    cap_map = upstream_capability_functions(PROBE_SOURCE)
    manifest_body_index = build_manifest_body_index()
    skill_text = SKILL.read_text(encoding="utf-8")
    skill_lines = skill_text.splitlines()
    test_dir = VALIDATION / "tests" / "source_snapshots" / "a-stock-data" / "tests"
    rows: list[dict[str, str]] = []
    result_payloads: list[dict[str, Any]] = []
    matched_output_ids: set[int] = set()
    counted = 0

    summary_name_map = {normalize_name(r["name"]): int(r["id"]) for r in summary_rows}
    capability_name_map: dict[str, int] = {}
    cap_tree = ast.parse(PROBE_SOURCE.read_text(encoding="utf-8"))
    for node in ast.walk(cap_tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(t, ast.Name) and t.id == "CAPABILITIES" for t in targets) or not isinstance(node.value, ast.Dict):
            continue
        for key, value in zip(node.value.keys, node.value.values):
            if isinstance(key, ast.Constant) and isinstance(key.value, int) and isinstance(value, ast.Tuple) and value.elts and isinstance(value.elts[0], ast.Constant):
                capability_name_map[normalize_name(str(value.elts[0].value))] = key.value
    for inv in inventory:
        num = int(inv["序号"])
        name = inv["能力项"]
        included = inv["是否计入87项"] == "是"
        if included:
            counted += 1
        source_name = PROBE_NAME_BY_INVENTORY.get(name, name)
        probe_id = summary_name_map.get(normalize_name(source_name))
        if probe_id is None:
            probe_id = capability_name_map.get(normalize_name(source_name))
        result = live_probe_by_id.get(probe_id) if probe_id is not None and probe_id in live_probe_by_id else (summary_by_id.get(probe_id) if probe_id is not None else None)
        live_result = bool(result and result.get("policy", {}).get("policy_version") == "rate-limited-live-probe-v1")
        api_symbol = UPSTREAM_API_SYMBOLS.get(name, "")
        api_line = api_doc_line(api_symbol, skill_lines)
        if result:
            matched_output_ids.add(probe_id)
        function = ""
        code_ref = ""
        test_ref = ""
        if result and probe_id in cap_map:
            function, script_rel, line = cap_map[probe_id]
            code_ref = f"{script_rel}:{line} ({function})"
            test_ref = f"provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py:{line} (CLI --run {probe_id})"
        else:
            # The pinned upstream SKILL is the authoritative source snapshot for capabilities
            # without an executed source-script entry in the retained audit batch.
            needles = [name.split("（")[0].split("(")[0].strip(), source_name.split("（")[0].split("(")[0].strip()]
            idx = api_line or next((i for i, line in enumerate(skill_lines, 1) if any(n and n in line for n in needles)), 1)
            code_ref = f"provider_validation/tests/source_snapshots/a-stock-data/SKILL.md:{idx} (上游内嵌接口说明/实现快照)"
            suite = "test_v310_sources.py" if "V3.10" in (inv.get("证据说明", "") + inv.get("备注", "")) else "test_v39_sources.py"
            suite_path = test_dir / suite
            test_ref = f"{suite_path.relative_to(ROOT).as_posix()}:1 (该版本测试快照；未找到专属用例时以清单证据注明)"
            # Recover an exact published function symbol if one appears on the matching SKILL lines.
            for ln in skill_lines[max(0, idx - 4): min(len(skill_lines), idx + 8)]:
                m = re.search(r"`([A-Za-z_][A-Za-z0-9_]*\([^`]*\))`", ln)
                if m:
                    function = m.group(1)
                    break
        if result and api_line:
            code_ref += f"; provider_validation/tests/source_snapshots/a-stock-data/SKILL.md:{api_line} ({api_symbol})"

        original_status = inv.get("原审计状态", "") or inv.get("原始状态", "")
        probe_status = (result or {}).get("status", "")
        notes = inv.get("本轮数据核验结论", "") or inv.get("本轮备注", "") or inv.get("备注", "") or inv.get("证据说明", "")
        current_run_note = inv.get("本轮备注", "")
        manifest_ref, body_ref = raw_refs(inv)
        if live_result:
            manifest_ref = result.get("manifest_ref", "")
            body_ref = result.get("body_ref", "")
            probe_status = result.get("status", "")
            result_files = result.get("files", [])
            output_ref = "; ".join(rel(p) for p in result_files)
            status_msg = result.get("message", "")
            notes = short((notes + "；" if notes else "") + f"本轮低频真实探测 {probe_status}：{status_msg}。{result.get('scope_summary', '')}", 1200)
            if probe_status == "success":
                verdict = "部分通过"  # A live parseable sample is not sufficient for full semantic validation.
                evidence = "低频实时样本解析成功，原始响应和探测输出已留档；本轮仅单样本验证，未做全量语义核验。"
            elif probe_status == "partial":
                verdict = "部分通过"
                evidence = "低频实时探测有部分数据，原始响应/失败事件及输出已留档；结果不完整。"
            elif probe_status == "credential_required":
                verdict = "未验证"
                evidence = "实际调用受凭据限制；已留存失败/凭据状态，不能据此判源不可用。"
            else:
                verdict = "未通过"
                evidence = "低频实时探测未取得可用业务数据；失败事件已留档，不能将传输阻断判为源数据为空。"
            validation_scope = result.get("scope_summary", "")
            validation_finding = notes
            if probe_id in cap_map:
                function, script_rel, line = cap_map[probe_id]
                code_ref = f"{script_rel}:{line} ({function})"
                test_ref = f"provider_validation/tests/run_a_stock_rate_limited_probes.py (低频安全入口)；--ids {probe_id}"
            endpoint_parts = list(dict.fromkeys(f"{e.get('method', '')} {e.get('url', '')}".strip() for e in result.get("events", []) if e.get("url")))
        if result and not live_result:
            status_msg = result.get("message", "")
            if probe_status == "success":
                verdict = "部分通过"
                evidence = "解析后输出已保存；"
                evidence += "另有历史原始响应归档。" if (manifest_ref or body_ref) else "本批未保存精确原始HTTP响应。"
                notes = short((notes + "；" if notes else "") + f"脚本结果 success：{status_msg}。解析结果非空；未留精确原始响应。", 1200)
            elif probe_status == "partial":
                verdict = "部分通过"
                evidence = "仅解析后输出；返回中存在空项或错误项，原始HTTP响应未留存。"
                notes = short((notes + "；" if notes else "") + f"脚本结果 partial：{status_msg}。", 1200)
            elif probe_status in ("credential_required",):
                verdict = "未验证"
                evidence = "调用受凭据限制；不是源不可用结论。"
                notes = short((notes + "；" if notes else "") + status_msg, 1200)
            else:
                verdict = "未通过"
                evidence = "探针明确失败/不可用；保存了执行摘要或失败信息，无成功原始业务响应。"
                notes = short((notes + "；" if notes else "") + f"脚本结果 {probe_status}: {status_msg}", 1200)
        elif not live_result:
            status_map = {
                "verified_live_raw_saved": ("通过", "实时接口响应与原始响应均已归档。"),
                "partial_live_format_error_raw_saved": ("未通过", "源有返回且原始响应已保存，但解析/语义验证未通过。"),
                "historical_live_raw_missing": ("未验证", "只有历史结论，未保存原始返回；本轮未重复请求。"),
                "historical_or_documented_only": ("未验证", "仅有文档或历史说明，没有可复查的本轮接口响应。"),
                "documented_unavailable_or_partial": ("未通过", "文档/历史证据显示接口返回空或失效；当前未取得可用数据。"),
                "key_required_not_live_verified": ("未验证", "需要凭据，本轮未执行真实接口调用。"),
                "local_cache_unverified": ("未验证", "本地缓存能力尚无独立持久化读写验证。"),
                "shared_endpoint_variant_not_separately_live_verified": ("未验证", "与共享接口为不同参数变体，未单独发请求验证。"),
            }
            verdict, evidence = status_map.get(original_status, ("未验证", f"原状态={original_status or '缺失'}；没有本轮专属结果。"))
        if original_status == "verified_live_raw_saved" and not live_result:
            verdict = "通过"
            evidence = "此前实时验证已保存原始响应；本轮补抓脚本结果另列，测试范围不自动视为相同。"
        if not included and not live_result:
            verdict = "说明项"
            evidence = "不计入87项能力分母；行内记录未单独测试的接口变体或本地行为。"

        source_id = f"ASTOCK-{num:03d}"
        output_ref = output_ref if live_result else inv.get("本轮解析数据文件", "")
        if not output_ref or output_ref.startswith(("无", "未")):
            # Preserve older known derived-output reference if one was recorded.
            output_ref = inv.get("派生解析数据文件", "") or ""
        output_ref = "; ".join(rel(p) for p in output_ref.split(";") if p.strip()) if output_ref else ""
        sample = examples_for_file(output_ref.split(";")[0] if output_ref else "") or example_from_raw(body_ref)
        if name == "腾讯 K 线" and not live_result:
            sample = kline_period_samples(body_ref)
        if not sample and result:
            sample = short(result.get("message", ""), 500)
        if not function:
            function = "未在本轮脚本中独立映射；参见上游调用代码快照"
        declared_content = short(inv.get("claimed_data", "") or inv.get("数据内容", ""), 800)
        if not live_result:
            validation_scope = short((inv.get("本轮备注", "") + " " + inv.get("本轮文件行数结构检查", "")).strip() or inv.get("证据说明", ""), 500)
            validation_finding = notes
        if name == "腾讯 K 线":
            declared_content = "上游声明支持：沪深日/周/月K（前/后复权）及1/5/15/30/60分钟K；上游文档注明不含北交所。此处是能力声明，不代表各周期均已实测。"
            validation_scope = (
                "日线：sh600519，qfq，2026-09-01至2026-09-18，HTTP 200、code=0，返回14条日线。"
                "5分钟：sz300750，未传日期窗口、请求最近96条；直连web3.ifzq.gtimg.cn发生2次TLS EOF，"
                "使用备用域名proxy.finance.qq.com回退后HTTP 200、code=0，返回96条，样例日期为2026-09-29。"
                "未单独验证：周线、月线、1/15/30/60分钟线；不含北交所。"
            )
            validation_finding = (
                "实测样本的日线直连和5分钟备用域名回退均取得有效K线数据；直连5分钟请求失败已作为传输失败事件留档。"
                "5分钟请求未限制日期窗口，响应日期与清单requested_data_date不同，因此不能将其视为2026-09-18历史5分钟数据验证。"
                "通过结论仅覆盖上述日线及未限定日期的最近96条5分钟样本，不能代表其他周期或北交所。"
            )
        evidence_status = ("live_raw" if body_ref else ("failed" if probe_status not in ("success", "partial") else "parsed_only")) if live_result else ("live_raw" if original_status == "verified_live_raw_saved" else ("parsed_only" if result and probe_status in ("success", "partial") else (probe_status or original_status or "undetermined")))
        raw_digests = [Path(x.strip()).name.split(".")[0] for x in body_ref.split(";") if x.strip()]
        manifest_events = [manifest_body_index[d] for d in raw_digests if d in manifest_body_index]
        endpoint_parts = list(dict.fromkeys(f"{e['method']} {e['url']}".strip() for e in manifest_events if e.get("url")))
        if live_result:
            endpoint_parts = list(dict.fromkeys(endpoint_parts + [f"{e.get('method', '')} {e.get('url', '')}".strip() for e in result.get("events", []) if e.get("url")]))
        if not endpoint_parts and result and function.startswith(("fetch_", "unavailable_")):
            endpoint_parts = [urls_from_function(PROBE_SOURCE, function)]
        endpoint_info = "; ".join(x for x in endpoint_parts if x) or "未从现存探针清单提取；见来源函数/代码快照"
        artifacts = output_artifacts(output_ref)
        code_artifacts = code_hashes(code_ref, test_ref, "")
        raw_artifacts = response_hashes(body_ref)
        output_summary = "; ".join(f"{a['path']} (rows={a.get('row_count', 'n/a')}; sha256={a['sha256']})" for a in artifacts)
        if not output_summary:
            output_summary = "无解析输出文件" if not result else "执行失败，无解析输出"
        scope = short(inv.get("本轮备注", "") or inv.get("备注", "") or inv.get("证据说明", ""), 500)
        row = {
            "接口ID": source_id,
            "清单范围": "a-stock-data published endpoint" if included else "说明/变体（不计分母）",
            "是否计入87项": "是" if included else "否",
            "项目/来源": "a-stock-data（上游）",
            "类别": inv.get("分类", ""),
            "接口/能力名称": name,
            "接口地址/协议": endpoint_info,
            "接口/调用符号": api_symbol or function,
            "接口说明": short(name + "；上游能力声明，调用细节见代码快照和能力说明/返回字段列。", 500),
            "能力说明（文档声明）/返回字段": declared_content,
            "调用方式/参数范围": (f"探针入口：a_stock_missing_capabilities.py --run {probe_id} → {function}(cfg)；上游API：{UPSTREAM_API_SYMBOLS.get(name, function)}" if result else f"上游API：{UPSTREAM_API_SYMBOLS.get(name, function)}；本轮无独立执行入口或未单独探测。"),
            "来源代码文件/行号": code_ref,
            "代码SHA-256": "; ".join(f"{a['path']}={a['sha256']}" for a in code_artifacts) or "见来源快照路径/固定提交",
            "测试代码文件/行号": test_ref,
            "项目Provider对照代码": "",
            "接口取数结果": verdict,
            "本轮补抓脚本结果": ("未执行" if not result else {"success": "成功（解析输出非空）", "partial": "部分成功（有空/错项）", "failed": "失败", "unavailable": "不可用/受阻", "credential_required": "凭据缺失"}.get(probe_status, probe_status)),
            "证据完整度": evidence,
            "实测范围/日期/结果": validation_scope,
            "验证备注/数据核验结论": short(validation_finding, 1200),
            "原始响应Manifest": manifest_ref,
            "原始响应文件/哈希": body_ref or ("无原始响应；" + evidence if result else ("无；" + evidence if not manifest_ref else "见 Manifest 与归档 body 文件；SHA-256 文件名")),
            "解析结果/输出文件": output_ref or ("无解析数据文件" if not result else "本次失败，无解析数据"),
            "结果文件SHA-256/行数": output_summary,
            "返回示例": sample or "无样例响应；本轮没有成功可复查的接口数据。",
            "逐接口结果记录": f"provider_validation/results/interface-records/{source_id}.json",
            "来源版本/提交": "a-stock-data V3.10.0 / f814dcfe209dd7958f4858f9d878d591ee85fb56",
            "证据状态代码": evidence_status,
            "证据状态说明": EVIDENCE_STATUS_LABELS.get(evidence_status, f"未配置状态说明（{evidence_status}）"),
        }
        rows.append(row)
        result_payloads.append({"interface_id": source_id, "source": "a-stock-data", "validation_result": verdict, "current_missing_probe_status": probe_status or "not_run", "evidence_status": evidence_status, "evidence_status_description": row["证据状态说明"], "evidence_level": evidence, "source_code_ref": code_ref, "source_code_artifacts": code_artifacts, "test_code_ref": test_ref, "manifest_ref": manifest_ref, "response_artifacts": raw_artifacts, "parsed_output_ref": output_ref, "derived_artifacts": artifacts, "coverage_denominator": 87, "scope": row["实测范围/日期/结果"], "finding": row["验证备注/数据核验结论"], "sample": sample, "source_version": row["来源版本/提交"], "transformation_code_version": {"path": rel(Path(__file__)), "sha256": sha256(Path(__file__))}, "validation_time_utc": datetime.now(timezone.utc).isoformat()})

    # Six separately listed board interfaces discussed in the migration review.
    board_rows = [
        ("SDA-BOARD-001", "板块/行业", "AkShare.stock_board_industry_name_ths()", "同花顺行业目录（全行业代码与名称）", "通过", "2026-10-02；90行、90个唯一代码", "provider_validation/results/raw/2026-10-02-sector-capabilities-network-retry/manifest.ndjson", "provider_validation/results/2026-10-02-sector-derived/ths-industry-directory.csv", "上游 stock-data-analyse/warehouse/industry.py:189；本项目 src/stock_data_manage/providers/akshare/boards.py:31 fetch_industry_list", "行业目录不等于证券概念归属；单位不适用"),
        ("SDA-BOARD-002", "板块/行业", "AkShare.stock_board_industry_index_ths(symbol=..., start_date=..., end_date=...)", "行业指数日线", "部分通过", "半导体（881121），2026-09-01至2026-10-02请求；返回21行，实际末日09-30", "provider_validation/results/raw/2026-10-02-sector-capabilities-network-retry/manifest.ndjson", "provider_validation/results/2026-10-02-sector-derived/ths-semiconductor-index-daily.csv", "上游 warehouse/industry.py:201；本项目 src/stock_data_manage/providers/akshare/boards.py:66 fetch_industry_daily", "只测一个板块和窗口；成交量/成交额单位未确认"),
        ("SDA-BOARD-003", "板块/行业资金流", "AkShare.stock_fund_flow_industry(symbol='即时')", "行业即时资金流排行快照", "通过", "即时；90行、90个唯一行业名", "provider_validation/results/raw/2026-10-02-sector-capabilities-network-retry/manifest.ndjson", "provider_validation/results/2026-10-02-sector-derived/ths-industry-fund-flow-now.csv", "上游 fundflow/sources.py:96；本项目 src/stock_data_manage/providers/akshare/boards.py:144 fetch_fund_flow", "快照无逐行日期；金额单位未确认；不是历史资金流"),
        ("SDA-BOARD-004", "板块/概念资金流", "AkShare.stock_fund_flow_concept(symbol='即时')", "概念即时资金流排行快照", "通过", "即时；387行、387个唯一概念名", "provider_validation/results/raw/2026-10-02-sector-capabilities-network-retry/manifest.ndjson", "provider_validation/results/2026-10-02-sector-derived/ths-concept-fund-flow-now.csv", "上游 fundflow/sources.py:98；本项目 src/stock_data_manage/providers/akshare/boards.py:144 fetch_fund_flow", "不是证券-概念成员关系；无逐行日期；金额单位未确认"),
        ("SDA-BOARD-005", "证券清单/行业快照", "BaoStock.query_all_stock(day='2026-09-30')", "查询当日沪深上市证券清单", "部分通过", "旧全量探针日期2026-09-30；与行业查询合并统计5,212只在市证券", "", "provider_validation/results/legacy/2026-10-01-security-board-coverage.json", "上游 warehouse/industry.py:125；本项目 src/stock_data_manage/providers/baostock/industry.py:32 fetch_snapshot", "BaoStock只保存旧合并解析结果；无原始SDK行或TCP帧；不是当前Provider独立Live Probe"),
        ("SDA-BOARD-006", "证券-证监会行业关系", "BaoStock.query_stock_industry(date='2026-09-30')", "查询日期快照中的证券与证监会行业分类", "部分通过", "旧全量探针日期2026-09-30；5,212只中5,210只获得分类（99.9616%），缺2只", "", "provider_validation/results/legacy/2026-10-01-security-board-coverage.json", "上游 warehouse/industry.py:125；本项目 src/stock_data_manage/providers/baostock/industry.py:32 fetch_snapshot", "旧合并结果可离线重放，缺原始SDK行/TCP帧；不可标为原始返回已归档"),
    ]
    baostock_run = VALIDATION / "results" / "live-probes" / "baostock-industry-20260930-20261003"
    baostock_result_path = baostock_run / "result.json"
    baostock_manifest_path = baostock_run / "_raw" / "baostock-industry" / "manifest.ndjson"
    baostock_raw_by_endpoint: dict[str, str] = {}
    baostock_rows_by_endpoint: dict[str, int] = {}
    baostock_result: dict[str, Any] = {}
    if baostock_result_path.is_file() and baostock_manifest_path.is_file():
        try:
            baostock_result = json.loads(baostock_result_path.read_text(encoding="utf-8"))
            for line in baostock_manifest_path.read_text(encoding="utf-8").splitlines():
                event = json.loads(line)
                storage = event.get("body_storage")
                baostock_rows_by_endpoint[str(event.get("endpoint", ""))] = int(event.get("metadata", {}).get("row_count", 0))
                if storage:
                    body_path = (baostock_manifest_path.parent / storage).resolve()
                    baostock_raw_by_endpoint[str(event.get("endpoint", ""))] = rel(body_path)
        except (OSError, json.JSONDecodeError):
            baostock_result = {}
    if baostock_result.get("status") == "success":
        date_scope = str(baostock_result.get("date_scope", ""))
        listed_count = int(baostock_result.get("listed_symbol_count", 0))
        classified_count = int(baostock_result.get("classified_row_count", 0))
        missing_count = int(baostock_result.get("missing_symbol_count", 0))
        derived_path = str(baostock_result.get("parsed_output", ""))
        manifest_ref = rel(baostock_manifest_path)
        raw_listed_count = baostock_rows_by_endpoint.get("query_all_stock", 0)
        board_rows[4] = (
            "SDA-BOARD-005", "证券清单/行业快照", f"BaoStock.query_all_stock(day='{date_scope}')",
            "查询当日全市场证券清单；原始SDK行已归档", "通过",
            f"{date_scope}；原始SDK返回{raw_listed_count}条记录；Provider筛得沪深A股{listed_count}只",
            manifest_ref, derived_path,
            "本项目 src/stock_data_manage/providers/baostock/industry.py:32 fetch_snapshot；provider_validation/tests/run_baostock_industry_live_probe.py",
            "通过代码/交易所规则筛选得到沪深A股证券集合；BaoStock TCP原始帧不对SDK调用方开放。2026-10-02非交易日空返回已保存在 provider_validation/results/live-probes/baostock-industry-20261003/result.json。",
        )
        board_rows[5] = (
            "SDA-BOARD-006", "证券-证监会行业关系", f"BaoStock.query_stock_industry(date='{date_scope}')",
            "查询沪深证券与证监会行业分类关系", "部分通过",
            f"{date_scope}；沪深A股分母{listed_count}只，成功匹配{classified_count}只，缺失{missing_count}只：{','.join(baostock_result.get('missing_symbols', []))}",
            manifest_ref, derived_path,
            "本项目 src/stock_data_manage/providers/baostock/industry.py:32 fetch_snapshot；provider_validation/tests/run_baostock_industry_live_probe.py",
            f"已归档query_stock_industry SDK解码字段与原始行；缺失{missing_count}只，因此覆盖不完整。BaoStock TCP原始帧不对SDK调用方开放。2026-10-02非交易日空返回已单独留档。",
        )
    board_test_by_id = {
        "SDA-BOARD-001": "tests/test_sector_data_providers.py:53; provider_validation/tests/replay_sector_capability_archives.py:176",
        "SDA-BOARD-002": "tests/test_sector_data_providers.py:53; provider_validation/tests/replay_sector_capability_archives.py:178",
        "SDA-BOARD-003": "tests/test_sector_data_providers.py:73; provider_validation/tests/replay_sector_capability_archives.py:196",
        "SDA-BOARD-004": "tests/test_sector_data_providers.py:73; provider_validation/tests/replay_sector_capability_archives.py:196",
        "SDA-BOARD-005": "tests/test_sector_data_providers.py:138; provider_validation/tests/replay_sector_capability_archives.py:212; provider_validation/tests/run_baostock_industry_live_probe.py",
        "SDA-BOARD-006": "tests/test_sector_data_providers.py:138; provider_validation/tests/replay_sector_capability_archives.py:212; provider_validation/tests/run_baostock_industry_live_probe.py",
    }
    board_source = "provider_validation/tests/source_snapshots/stock-data-analyse/warehouse/industry.py; provider_validation/tests/source_snapshots/stock-data-analyse/fundflow/sources.py"
    for ident, category, symbol, content, verdict, scope, manifest, output, provider_ref, limitation in board_rows:
        endpoint_key = "query_all_stock" if ident == "SDA-BOARD-005" else "query_stock_industry" if ident == "SDA-BOARD-006" else ""
        board_body_ref = baostock_raw_by_endpoint.get(endpoint_key, "")
        if endpoint_key and board_body_ref:
            evidence = "BaoStock SDK解码字段与原始行已归档（非TCP线缆帧）；派生覆盖表已保存。"
        else:
            evidence = "原始HTTP响应已按 manifest/hash 归档" if manifest else "旧派生/合并结果可复核；BaoStock SDK原始行不可见，未保存原始返回帧。"
        code_ref = board_source + "; " + provider_ref
        sample = examples_for_file(output) or limitation
        row = {
            "接口ID": ident, "清单范围": "stock-data-analyse 板块接口补充，不计入87项", "是否计入87项": "否",
            "项目/来源": "stock-data-analyse → 本项目 Provider", "类别": category, "接口/能力名称": symbol,
            "接口地址/协议": "BaoStock SDK/TCP（SDK解码行见归档；底层TCP帧不可见）" if endpoint_key else ("HTTPS/HTTP，精确URL见Manifest" if manifest else "BaoStock SDK over TCP（底层原始帧不可见）"),
            "接口/调用符号": symbol, "接口说明": content, "能力说明（文档声明）/返回字段": content,
            "调用方式/参数范围": symbol, "来源代码文件/行号": code_ref, "测试代码文件/行号": board_test_by_id[ident],
            "项目Provider对照代码": provider_ref, "接口取数结果": verdict, "本轮补抓脚本结果": "不适用", "证据完整度": evidence,
            "实测范围/日期/结果": scope, "验证备注/数据核验结论": limitation,
            "原始响应Manifest": manifest, "原始响应文件/哈希": ("; ".join(f"{a['path']} sha256={a['sha256']}" for a in response_hashes(board_body_ref)) if board_body_ref else ("见对应Manifest下的body文件" if manifest else "未保存：SDK没有暴露原始TCP帧/原始SDK行")),
            "解析结果/输出文件": output, "返回示例": sample,
            "逐接口结果记录": f"provider_validation/results/interface-records/{ident}.json",
            "来源版本/提交": "stock-data-analyse c26cabcf89443ec8f1445d0e5af86bf3d6dacf3; 本项目代码以当前工作树为准",
            "证据状态代码": "live_raw" if manifest else "historical_parsed_only",
            "证据状态说明": EVIDENCE_STATUS_LABELS["live_raw" if manifest else "historical_parsed_only"],
        }
        board_artifacts = output_artifacts(output)
        board_response_artifacts = response_hashes(board_body_ref)
        board_code_artifacts = code_hashes(code_ref, board_test_by_id[ident], provider_ref)
        row["代码SHA-256"] = "; ".join(f"{a['path']}={a['sha256']}" for a in board_code_artifacts)
        row["结果文件SHA-256/行数"] = "; ".join(f"{a['path']} (rows={a.get('row_count', 'n/a')}; sha256={a['sha256']})" for a in board_artifacts)
        rows.append(row)
        result_payloads.append({"interface_id": ident, "source": "stock-data-analyse / local Provider", "validation_result": verdict, "evidence_status": row["证据状态代码"], "evidence_status_description": row["证据状态说明"], "evidence_level": evidence, "source_code_ref": code_ref, "source_code_artifacts": board_code_artifacts, "test_code_ref": board_test_by_id[ident], "manifest_ref": manifest, "response_artifacts": board_response_artifacts, "parsed_output_ref": output, "derived_artifacts": board_artifacts, "coverage_denominator": None, "scope": scope, "finding": limitation, "sample": sample, "source_version": row["来源版本/提交"], "transformation_code_version": {"path": rel(Path(__file__)), "sha256": sha256(Path(__file__))}, "validation_time_utc": datetime.now(timezone.utc).isoformat()})

    if counted != 87:
        raise SystemExit(f"Expected 87 counted capabilities; found {counted}")
    if len(rows) != 95:
        raise SystemExit(f"Expected 89 inventory rows + 6 board interfaces = 95; found {len(rows)}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    RECORDS_DIR.mkdir(parents=True, exist_ok=True)
    for payload in result_payloads:
        dest = RECORDS_DIR / f"{payload['interface_id']}.json"
        dest.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    snapshots_root = VALIDATION / "tests" / "source_snapshots"
    snapshots = []
    for path in sorted(p for p in snapshots_root.rglob("*") if p.is_file() and p.name != "manifest.json"):
        relative = rel(path)
        if "/a-stock-data/" in relative:
            upstream_path = relative.split("/a-stock-data/", 1)[1]
            source_url = f"https://github.com/simonlin1212/a-stock-data/blob/f814dcfe209dd7958f4858f9d878d591ee85fb56/{upstream_path}"
        elif "/stock-data-analyse/" in relative:
            upstream_path = relative.split("/stock-data-analyse/", 1)[1]
            source_url = f"https://github.com/fewhg3yhjt/stock-data-analyse/blob/c26cabcf89443ec8f1445d0e5af86bf3d6dacf3/{upstream_path}"
        else:
            upstream_path = ""
            source_url = ""
        snapshots.append({"path": relative, "upstream_path": upstream_path, "source_url": source_url, "sha256": sha256(path), "bytes": path.stat().st_size})
    snapshot_manifest = {
        "record_type": "source_snapshot_manifest",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": [
            {"project": "a-stock-data", "repository": "https://github.com/simonlin1212/a-stock-data", "commit": "f814dcfe209dd7958f4858f9d878d591ee85fb56"},
            {"project": "stock-data-analyse", "repository": "https://github.com/fewhg3yhjt/stock-data-analyse", "commit": "c26cabcf89443ec8f1445d0e5af86bf3d6dacf3"},
        ],
        "files": snapshots,
    }
    snapshot_manifest_path = VALIDATION / "tests" / "source_snapshots" / "manifest.json"
    snapshot_manifest_path.write_text(json.dumps(snapshot_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raw_hash_results = [r for payload in result_payloads for r in payload.get("response_artifacts", [])]
    raw_integrity_failures = [r for r in raw_hash_results if not r.get("matches_filename")]
    tally = Counter(r["接口取数结果"] for r in rows if r["是否计入87项"] == "是")
    live_probe_status_counts = Counter(str(r.get("status", "unknown")) for r in live_probe_by_id.values())
    live_probe_body_hashes = {
        str(event.get("body_sha256"))
        for result in live_probe_by_id.values()
        for event in result.get("events", [])
        if event.get("body_sha256")
    }
    summary = {
        "record_type": "interface_coverage_report_summary",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "report": OUT_CSV.relative_to(ROOT).as_posix(),
        "row_count": len(rows), "counted_a_stock_data_capabilities": counted,
        "non_counted_inventory_explanatory_rows": sum(1 for x in inventory if x["是否计入87项"] != "是"),
        "stock_data_analyse_board_interfaces": len(board_rows),
        "counted_result_counts": dict(tally),
        "latest_rate_limited_live_probe_capability_count": len(live_probe_by_id),
        "latest_rate_limited_live_probe_status_counts": dict(live_probe_status_counts),
        "latest_rate_limited_live_probe_unique_response_bodies": len(live_probe_body_hashes),
        "missing_output_summary_rows": len(summary_rows),
        "missing_output_summary_matched_inventory": len(matched_output_ids),
        "missing_output_summary_unmatched_ids": [r["id"] for r in summary_rows if int(r["id"]) not in matched_output_ids],
        "retained_raw_response_bodies_hash_checked": len(raw_hash_results),
        "retained_raw_response_hash_failures": len(raw_integrity_failures),
        "source_snapshot_manifest": rel(snapshot_manifest_path),
        "raw_response_integrity_scope": "Existing raw manifests/bodies reused; no network requests made by this builder.",
    }
    (VALIDATION / "results" / "interface-coverage-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
