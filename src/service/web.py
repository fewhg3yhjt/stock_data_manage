"""Read-only local web console for task, raw-evidence and publication views."""
from __future__ import annotations

import argparse
import asyncio
import base64
import codecs
import hashlib
import json
import logging
import os
import re
import sqlite3
import stat
from contextlib import asynccontextmanager, contextmanager
from threading import Event, Lock, Thread
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import yaml

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from ..config.loader import load_storage_paths
from ..storage.read_index import (SOURCE_QUERY_DATABASE, load_source_query_records,
                                  query_source_data_page, rebuild_source_query_indexes,
                                  source_query_has_missing_asset_context)


ACTIVE_TASK_STATES = frozenset({"collecting", "checking", "committing"})
RESPONSE_EVENTS = frozenset({"http_response", "source_payload", "sdk_query_failure"})
STALE_AFTER_SECONDS = 300
MAX_PREVIEW_BYTES = 256 * 1024
DEFAULT_PREVIEW_BYTES = 64 * 1024
SENSITIVE_KEY = re.compile(
    r"(?:authorization|cookie|token|secret|password|passwd|api[-_]?key|credential|proxy|proxies)",
    re.IGNORECASE,
)
ASSET_ROOT = Path(__file__).with_name("static")
DISPLAY_FIELDS = frozenset({"symbol", "security_name", "asset_type", "trade_date", "adjustment",
    "open", "high", "low", "close", "volume", "amount", "provider", "status"})
DISPLAY_NUMBERS = frozenset({"open", "high", "low", "close", "volume", "amount"})


def _presentation(config_root: Path, view: str) -> list[dict[str, Any]]:
    """Validate the small declared view contract; never evaluate expressions."""
    try:
        document = yaml.safe_load((config_root / "presentation.yaml").read_text(encoding="utf-8"))
        if not isinstance(document, dict) or set(document) != {"fields", "views"}:
            raise ValueError("必须定义fields和views")
        fields, views = document["fields"], document["views"]
        if not isinstance(fields, dict) or set(fields) - DISPLAY_FIELDS or not isinstance(views, dict) or set(views) != {"daily", "daily_source"}:
            raise ValueError("未知字段或视图")
        for key, field in fields.items():
            if not isinstance(field, dict) or set(field) - {"label", "format", "min_decimals", "trim_trailing_zeros"}:
                raise ValueError("非法字段说明: " + key)
            if not isinstance(field.get("label"), str) or not field["label"].strip() or not isinstance(field.get("format"), str) or field.get("format") not in {
                    "text", "date", "asset_type", "adjustment", "decimal", "quantity", "status"}:
                raise ValueError("非法标题或格式: " + key)
            if "min_decimals" in field and (type(field["min_decimals"]) is not int or not 0 <= field["min_decimals"] <= 8):
                raise ValueError("非法小数位: " + key)
            if "trim_trailing_zeros" in field and type(field["trim_trailing_zeros"]) is not bool:
                raise ValueError("非法尾零设置: " + key)
        for name, spec in views.items():
            allowed = {"columns", "values"} if name == "daily" else {"columns", "preserve_source_values", "technical_fields", "unknown_fields"}
            if not isinstance(spec, dict) or set(spec) - allowed:
                raise ValueError("非法视图设置: " + name)
            columns = spec.get("columns")
            if not isinstance(columns, list) or not columns or any(not isinstance(k, str) or k not in fields for k in columns) or len(set(columns)) != len(columns):
                raise ValueError("未知或重复列: " + name)
            paths = spec.get("values", {})
            if not isinstance(paths, dict) or any(k not in DISPLAY_NUMBERS or p != "values." + k for k, p in paths.items()):
                raise ValueError("非法数据路径: " + name)
            if name == "daily" and set(paths) != DISPLAY_NUMBERS:
                raise ValueError("业务数值列必须声明全部已知取值路径")
            if name == "daily_source" and (spec.get("preserve_source_values") is not True or
                    spec.get("technical_fields") != "details" or spec.get("unknown_fields") != "details"):
                raise ValueError("来源原值和详情边界不能修改")
        spec = views[view]
        return [{"key": key, **fields[key], "path": ("display_values." + key if view == "daily_source" else
                    spec.get("values", {}).get(key, "values." + key if key in DISPLAY_NUMBERS else key)),
                 "preserve_source": view == "daily_source"} for key in spec["columns"]]
    except (OSError, yaml.YAMLError, ValueError, KeyError) as exc:
        raise HTTPException(422, "展示配置错误: " + str(exc)) from exc


def _daily_field_metadata(row: dict[str, Any]) -> dict[str, Any]:
    flags = row["flags"]
    result = {}
    for field in DISPLAY_NUMBERS:
        unit = row["quote_unit"] if field in {"open", "high", "low", "close"} else (
            "CNY" if field == "amount" else {"stock": "share", "etf": "fund_unit", "lof": "fund_unit"}.get(row["asset_type"], ""))
        state = "unverified" if "unit_unverified:" + field in flags else ("missing" if row["values"][field] is None else "available")
        related = [flag for flag in flags if flag.startswith("source_" + field + "_rounded")]
        result[field] = {"unit": unit, "status": state, "reason": "单位未核验" if state == "unverified" else
            ("未提供标准值" if state == "missing" else ""), "precision_flags": related}
        if row.get("value_contract") == "close_only" and field in {"open", "high", "low"}:
            result[field].update(status="missing", reason="该指数按收盘序列合同交付，不包含此价格")
    return result


class SecurityHeadersMiddleware:
    """Add browser protections without buffering or changing response bodies."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        async def send_with_headers(message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(
                    [
                        (b"x-content-type-options", b"nosniff"),
                        (b"x-frame-options", b"DENY"),
                        (b"referrer-policy", b"no-referrer"),
                        (
                            b"content-security-policy",
                            b"default-src 'self'; style-src 'self'; script-src 'self'; "
                            b"img-src 'self' data:; connect-src 'self'; object-src 'none'; "
                            b"base-uri 'none'; frame-ancestors 'none'",
                        ),
                    ]
                )
                if scope.get("path", "").startswith("/api/"):
                    headers.append((b"cache-control", b"no-store"))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read managed JSON: {path.name}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"managed JSON must contain an object: {path.name}")
    return value


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _page(items: list[dict[str, Any]], page: int, page_size: int) -> dict[str, Any]:
    start = (page - 1) * page_size
    return {
        "items": items[start : start + page_size],
        "page": page,
        "page_size": page_size,
        "total": len(items),
        "pages": (len(items) + page_size - 1) // page_size,
    }


def _redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    pairs = [
        (key, "[已隐藏]" if SENSITIVE_KEY.search(key) else item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    return urlunsplit((parsed.scheme, parsed.netloc.rsplit("@", 1)[-1], parsed.path, urlencode(pairs), ""))


def _redact(value: Any, *, key: str = "") -> Any:
    if key and SENSITIVE_KEY.search(key):
        return "[已隐藏]"
    if isinstance(value, dict):
        return {str(name): _redact(item, key=str(name)) for name, item in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, tuple):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        value = re.sub(r"https?://[^\s\"<>]+", lambda match: _redact_url(match[0]), value)
        value = re.sub(r"(?i)(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+", "[已隐藏]", value)
        value = re.sub(r"(?i)(token|password|api[_-]?key|secret)=([^&\s]+)", r"\1=[已隐藏]", value)
        value = re.sub(r"(?<![A-Za-z])[A-Za-z]:[\\/][^\s\"<>]*", "[本地路径]", value)
    return value


def _within(path: Path, root: Path) -> bool:
    return path.resolve().is_relative_to(root.resolve())


def _component(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value) or value in {".", ".."}:
        raise HTTPException(status_code=404, detail="记录不存在")
    return value


def _token(path: Path, root: Path) -> str:
    relative = path.resolve().relative_to(root.resolve()).as_posix().encode("utf-8")
    return base64.urlsafe_b64encode(relative).decode("ascii").rstrip("=")


def _from_token(value: str, root: Path, *, manifest: bool = True) -> Path:
    try:
        padding = "=" * (-len(value) % 4)
        relative = base64.urlsafe_b64decode(value + padding).decode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise HTTPException(status_code=404, detail="记录不存在") from exc
    if "\x00" in relative:
        raise HTTPException(status_code=404, detail="记录不存在")
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root.resolve()) or not candidate.is_file():
        raise HTTPException(status_code=404, detail="记录不存在")
    if manifest and not (candidate.name.startswith("manifest") and candidate.suffix == ".ndjson"):
        raise HTTPException(status_code=404, detail="记录不存在")
    return candidate


def _iter_ndjson(path: Path) -> Iterable[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    yield value
    except (OSError, UnicodeError):
        return


class DashboardReader:
    """Build bounded read models from existing managed files without writing state."""

    def __init__(self, config_root: str | Path, data_root: str | Path | None = None) -> None:
        self.paths = load_storage_paths(config_root, data_root=data_root)
        self.config_root = Path(config_root).resolve()
        self._raw_query_index_cache: dict[Path, tuple[tuple[int, int], list[dict[str, Any]]]] = {}

    def _query_reports(self) -> list[tuple[Path, dict[str, Any]]]:
        """Read per-run selector indexes; source reports are opened only for selected rows."""
        indexed = load_source_query_records(
            query_db_path=self.paths["data_root"] / SOURCE_QUERY_DATABASE)
        result = []
        for report_path, report in indexed:
            if not isinstance(report, dict) or not isinstance(report.get("source_rows"), dict):
                continue
            report = dict(report)
            result.append((report_path, report))
        return result

    def data_options(self) -> dict[str, Any]:
        groups: dict[str, dict[str, Any]] = {}
        for _, report in self._query_reports():
            dataset = report.get("dataset")
            if not isinstance(dataset, str) or not re.fullmatch(r"[a-z0-9_]+", dataset):
                continue
            if dataset not in groups:
                label = dataset
                schema_path = self.config_root / "datasets" / f"{dataset}.yaml"
                if schema_path.is_file():
                    schema = yaml.safe_load(schema_path.read_text(encoding="utf-8")) or {}
                    label = schema.get("dataset", {}).get("display_name", dataset)
                groups[dataset] = {"id": dataset, "label": label, "providers": set(), "dates": set(),
                                   "requires_security": dataset == "daily_bar" or dataset.startswith("minute_bar")}
            group = groups[dataset]
            group["providers"].add(str(report.get("provider", "")))
            if report.get("data_date"):
                group["dates"].add(str(report["data_date"]))
        return {"datasets": [{**group, "providers": sorted(group["providers"]),
                              "dates": sorted(group["dates"])} for group in groups.values()]}

    @staticmethod
    def _security_code(value: Any) -> tuple[str, str]:
        text = str(value or "").strip().lower()
        match = re.fullmatch(r"(?:(xshg|xshe|bse|sh|sz|bj)[.:]?)?(\d{6})", text)
        if not match:
            return "", ""
        market = {"xshg": "sh", "xshe": "sz", "bse": "bj"}.get(match[1], match[1] or "")
        return market, match[2]

    def _security_catalog(self) -> dict[tuple[str, str], dict[str, str]]:
        import pyarrow.parquet as parquet
        from ..service.instruments import SecurityMasterStore, security_record_to_mapping
        from ..storage.integrity import Manifest

        catalog = {}
        root = self.paths["canonical_root"]
        current_dir = root / "security_master" / "current"
        current_manifest = current_dir / "manifest.json"
        current_data = current_dir / "security_master.duckdb"
        rows, source_date = None, None
        if (current_manifest.is_file() and current_data.is_file()
                and not (current_dir / "task-commit.json").exists()):
            try:
                manifest = Manifest.load(current_manifest)
                if manifest.verify(current_data) and manifest.dataset == "security_master":
                    with SecurityMasterStore(str(current_data)) as store:
                        rows = [security_record_to_mapping(record) for record in store.all()]
                    source_date = manifest.publication_metadata.get("as_of_date") or manifest.partition_key
            except (OSError, ValueError):
                rows = None
        if rows is None and not current_manifest.exists() and not current_data.exists():
            legacy = sorted(root.glob("*/security_master/manifest.json"), reverse=True)
            if legacy:
                manifest_path = legacy[0]
                data_path = manifest_path.parent / "data.parquet"
                try:
                    manifest = Manifest.load(manifest_path)
                    if manifest.verify(data_path) and manifest.dataset == "security_master":
                        rows = parquet.read_table(data_path).to_pylist()
                        source_date = manifest.publication_metadata.get("as_of_date") or manifest.partition_key
                except (OSError, ValueError):
                    rows = None
        for row in rows or ():
            code = self._security_code(row.get("instrument_id") or row.get("symbol"))
            if not code[0]:
                market = {"XSHG": "sh", "XSHE": "sz", "BSE": "bj"}.get(row.get("exchange"), "")
                code = (market, code[1])
            if code[1] and code not in catalog:
                catalog[code] = {"name": str(row.get("name") or ""),
                                 "asset_type": str(row.get("asset_type") or ""),
                                 "name_source_date": str(source_date or "")}
        return catalog

    def data_records(self, *, dataset: str, security: str = "", start_date: date | None = None,
                     end_date: date | None = None, provider: str = "", asset_type: str = "",
                     adjustment: str = "", page: int = 1, page_size: int = 50) -> dict[str, Any]:
        if start_date and end_date and start_date > end_date:
            raise HTTPException(status_code=422, detail="开始日期不能晚于结束日期")
        _component(dataset)
        schema_path = self.config_root / "normalization" / f"{dataset}.yaml"
        rules = (yaml.safe_load(schema_path.read_text(encoding="utf-8")) or {}).get("rules", []) if schema_path.is_file() else []
        catalog = self._security_catalog()
        presentation_columns = _presentation(self.config_root, "daily_source") if dataset == "daily_bar" else None
        target_market, target_code = self._security_code(security)
        references = {}
        needs_frozen_context = dataset != "daily_bar" or start_date is None or end_date is None
        if dataset == "daily_bar" and start_date is not None and end_date is not None:
            needs_frozen_context = source_query_has_missing_asset_context(
                self.paths["data_root"] / SOURCE_QUERY_DATABASE, dataset=dataset,
                start_date=start_date.isoformat(), end_date=end_date.isoformat(), provider=provider,
                adjustment=adjustment, target_market=target_market, target_code=target_code)
        if needs_frozen_context:
            for document, summary_path, _ in self._task_documents():
                definition_path = summary_path.with_name("task.json")
                definition = _read_json(definition_path) if _within(definition_path, summary_path.parent) and definition_path.is_file() else {}
                frozen_units = {unit.get("key"): unit for unit in definition.get("units", []) if isinstance(unit, dict)}
                for unit in document.get("source_units", []):
                    if isinstance(unit, dict) and unit.get("report_path"):
                        frozen_asset = frozen_units.get(unit.get("key"), {}).get("asset_type") or definition.get("asset_type")
                        references[str(Path(unit["report_path"]).resolve())] = {**unit, "frozen_asset_type": frozen_asset}

        query_result = query_source_data_page(
            self.paths["data_root"] / SOURCE_QUERY_DATABASE, dataset=dataset, security=security,
            target_market=target_market, target_code=target_code,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None, provider=provider,
            asset_type=asset_type, adjustment=adjustment, catalog=catalog,
            references=references, page=page, page_size=page_size)
        if query_result["conflict"]:
            raise HTTPException(409, query_result["conflict"])
        total = query_result["total"]
        unknown_dates = query_result["unknown_dates"]
        issues = [{"provider": item.get("provider"), "input_id": item.get("input_id"),
                   "reason": _redact(str(item.get("index_error") or "来源行查询索引不可用"))}
                  for item in query_result["issues"]]
        titles = {"trade_date": "日期", "bar_time": "时间", "stock_code": "证券代码",
                  "stock_name": "证券名称", "open": "开盘价", "close": "收盘价",
                  "high": "最高价", "low": "最低价", "volume": "成交量（来源单位）",
                  "amount": "成交额（来源单位）"}
        if presentation_columns is not None:
            titles.update({column["key"]: column["label"] for column in presentation_columns})

        row_cache: dict[Path, list[dict[str, Any]] | str] = {}
        items = []
        column_labels = {}
        for candidate in query_result["items"]:
            report_path = candidate["report_path"]
            run_root = candidate["run_root"]
            if not report_path.is_file() and run_root.is_relative_to(self.paths["workspace_root"]):
                archived_root = (self.paths["archive_root"] / run_root.relative_to(self.paths["workspace_root"])).resolve()
                archived_report = archived_root / report_path.relative_to(run_root)
                if archived_report.is_file():
                    run_root, report_path = archived_root, archived_report
            report = candidate["report"]
            descriptor = report["source_rows"]
            source = (run_root / str(candidate["source_rows_path"])).resolve()
            if source not in row_cache:
                try:
                    if not _within(source, run_root) or not source.is_file():
                        raise ValueError("来源行文件不存在")
                    payload = source.read_bytes()
                    if not descriptor.get("sha256") or hashlib.sha256(payload).hexdigest() != descriptor["sha256"]:
                        raise ValueError("来源行文件哈希不匹配")
                    rows = json.loads(payload)
                    if (not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows)
                            or (descriptor.get("row_count") is not None and len(rows) != descriptor["row_count"])):
                        raise ValueError("来源行数或格式与报告不一致")
                    row_cache[source] = rows
                except (OSError, ValueError) as exc:
                    row_cache[source] = str(exc)
                    issues.append({"provider": candidate["provider"], "input_id": report.get("input_id"),
                                   "reason": _redact(str(exc))})
                    continue
            loaded = row_cache[source]
            if isinstance(loaded, str):
                continue
            row_index = candidate["source_row_index"]
            if not isinstance(row_index, int) or row_index < 0 or row_index >= len(loaded):
                issues.append({"provider": candidate["provider"], "input_id": report.get("input_id"),
                               "reason": "来源行索引超出文件范围"})
                continue
            row = loaded[row_index]
            rule = next((item for item in rules if item.get("input_id") == report.get("input_id")
                         and item.get("version") == report.get("normalization_version")), {})
            mapping = rule.get("field_mapping", {})
            for canonical, original in mapping.items():
                if canonical in titles and isinstance(original, str):
                    column_labels[original] = f"{original} · {titles[canonical]}"

            def value(field_row, canonical, aliases):
                keys = [mapping.get(canonical), *aliases]
                return next((field_row[key] for key in keys if isinstance(key, str) and key in field_row
                             and field_row[key] not in (None, "")), None)

            evidence_path = None
            unit = references.get(str(report_path.resolve()), {})
            for reference in (unit.get("raw_manifest"), unit.get("current_raw")):
                if reference and (evidence_path := self._evidence_path(str(reference))):
                    break
            if not evidence_path:
                raw_reference = report.get("raw_manifest")
                reference = (raw_reference.get("path") if isinstance(raw_reference, dict)
                             else raw_reference if isinstance(raw_reference, str) else None)
                if reference:
                    reference_path = Path(reference)
                    evidence_path = self._evidence_path(str(reference_path if reference_path.is_absolute()
                                                             else run_root / reference_path))
            evidence = self._manifest_records(evidence_path) if evidence_path else []
            record_date = candidate["record_date"]
            row_market, row_code = candidate["row_market"], candidate["row_code"]
            name, known, row_asset = candidate["name"], candidate["known"], candidate["asset_type"]
            display_values = {field: value(row, field, []) for field in DISPLAY_NUMBERS}
            display_values.update(trade_date=record_date.isoformat() if record_date else None,
                symbol=candidate["symbol"], security_name=name, asset_type=row_asset,
                adjustment=candidate["adjustment"], provider=candidate["provider"])
            source_units = (report.get("applied_unit_contract") or {}).get("source_units", {})
            field_metadata = {field: {"unit": source_units.get(field, ""),
                "status": "available" if field in source_units else "unverified",
                "reason": "" if field in source_units else "单位未核验"} for field in ("volume", "amount")}
            field_metadata.update({field: {"unit": "point" if row_asset == "index" else "CNY", "status": "available"}
                                   for field in ("open", "high", "low", "close")})
            if presentation_columns is None:
                field_metadata = {}
            items.append({"record_date": record_date.isoformat() if record_date else None,
                "date_basis": candidate["date_basis"], "symbol": candidate["symbol"],
                "security_name": name, "asset_type": row_asset, "provider": candidate["provider"],
                "endpoint": candidate["endpoint"], "adjustment": candidate["adjustment"],
                "values": _redact(row), "name_source_date": known.get("name_source_date", ""),
                "display_values": _redact(display_values), "field_metadata": field_metadata,
                "source_field_labels": {key: next((titles[canonical] for canonical, original in mapping.items()
                    if original == key and canonical in titles),
                    {"source_api":"来源接口", "source_bar_key":"来源价格序列", "symbol":"证券代码"}.get(key,
                        "来源字段" + key + "（含义未核验）")) for key in row},
                "source_row_index": row_index, "source_rows_sha256": descriptor["sha256"],
                "code_version": report.get("code_version"),
                "normalization_version": report.get("normalization_version"), "evidence": evidence})

        columns = list(dict.fromkeys(key for row in items for key in row["values"]))
        result = {"items": items, "total": max(total, 0), "page": page, "page_size": page_size,
                  "pages": (max(total, 0) + page_size - 1) // page_size,
                  "columns": [{"key": key, "label": column_labels.get(key, key)} for key in columns],
                  "issues": issues[:50], "excluded_unknown_date_count": unknown_dates,
                  "dataset": dataset,
                  "total_basis": "匹配的索引候选行；当前页完整性失败通过issues报告，不重读其他页",
                  "representation":"来源行提取结果；保留来源字段和原值，不是完整原始响应字节"}
        if presentation_columns is not None:
            result["columns"] = presentation_columns
            result["presentation"] = "daily_source"
        return result

    def _task_documents(self) -> list[tuple[dict[str, Any], Path, str]]:
        candidates: list[tuple[dict[str, Any], Path, str]] = []
        roots = (
            (self.paths["workspace_root"], "workspace"),
            (self.paths["archive_root"], "archive"),
        )
        for root, location in roots:
            if not root.exists():
                continue
            # Task summaries live below date/task folders. Avoid recursively
            # walking private evidence and check directories (notably the
            # per-unit raw archive) that can contain many thousands of files.
            for scope in root.iterdir():
                if (not scope.is_dir()
                        or (scope.name.startswith("_")
                            and not (location == "archive" and scope.name == "_workspace_audit"))):
                    continue
                for path in scope.rglob("summary.json"):
                    if not _within(path, root):
                        continue
                    try:
                        document = _read_json(path)
                    except ValueError:
                        continue
                    if isinstance(document.get("task_id"), str):
                        candidates.append((document, path, location))
        latest: dict[str, tuple[dict[str, Any], Path, str]] = {}
        for document, path, location in candidates:
            task_id = document["task_id"]
            current = latest.get(task_id)
            updated = _parse_time(document.get("generated_at_utc")) or datetime.fromtimestamp(
                path.stat().st_mtime, timezone.utc
            )
            if current is None:
                latest[task_id] = (document, path, location)
                continue
            previous = _parse_time(current[0].get("generated_at_utc")) or datetime.fromtimestamp(
                current[1].stat().st_mtime, timezone.utc
            )
            if updated >= previous:
                latest[task_id] = (document, path, location)
        return list(latest.values())

    @staticmethod
    def _task_card(document: dict[str, Any], location: str, *,
                   persisted_updated_at: datetime | None = None,
                   now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        updated = persisted_updated_at or _parse_time(document.get("generated_at_utc"))
        age = None
        if updated is not None:
            age = max(0, int((now - updated).total_seconds()))
        status = str(document.get("status", "unknown"))
        stale = status in ACTIVE_TASK_STATES and (
            age is None or (now - updated).total_seconds() > STALE_AFTER_SECONDS)
        progress = document.get("progress") if isinstance(document.get("progress"), dict) else {}
        stages = progress.get("stages") if isinstance(progress.get("stages"), dict) else {}
        raw = stages.get("raw") if isinstance(stages.get("raw"), dict) else {}
        missing = document.get("missing_units")
        source_units = document.get("source_units")
        return {
            "task_id": document.get("task_id"),
            "dataset": document.get("dataset"),
            "status": status,
            "display_status": "stale" if stale else status,
            "stale": stale,
            "complete_today": bool(document.get("complete_today")),
            "data_date": document.get("data_date"),
            "requested_date": document.get("requested_date"),
            "generated_at_utc": document.get("generated_at_utc"),
            "updated_at_utc": persisted_updated_at.isoformat() if persisted_updated_at is not None else None,
            "age_seconds": age,
            "verification_mode": document.get("verification_mode"),
            "progress": progress,
            "completed_units": raw.get("completed"),
            "total_units": raw.get("total"),
            "missing_count": document.get("missing_count", len(missing) if isinstance(missing, list) else 0),
            "source_unit_count": len(source_units) if isinstance(source_units, list) else document.get('source_unit_count', 0),
            "phase": document.get('phase'),
            "partitions": _redact(document.get('partitions', {})),
            "error": _redact(document.get("error") or (document.get("final_check") or {}).get("error")),
            "location": location,
        }

    def tasks(
        self,
        *,
        status: str | None = None,
        dataset: str | None = None,
        query: str | None = None,
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        with self._runtime_read_snapshot() as metadata:
            result = metadata.query_collection_tasks(now=now, stale_after_seconds=STALE_AFTER_SECONDS,
                status=status, dataset=dataset, query=query, page=page, page_size=page_size)
        items = [self._task_row_card(row, now) for row in result["items"]]
        return {"items": items, "page": page, "page_size": page_size, "total": result["total"],
                "pages": (result["total"] + page_size - 1) // page_size}

    @contextmanager
    def _runtime_read_snapshot(self):
        from ..storage.metadata import CollectionTaskDataError, MetadataStore
        try:
            with MetadataStore(self.paths["metadata_path"], read_only=True) as metadata:
                metadata.connection.execute("BEGIN")
                try:
                    yield metadata
                    metadata.connection.execute("COMMIT")
                except Exception:
                    if metadata.connection.in_transaction:
                        metadata.connection.execute("ROLLBACK")
                    raise
        except FileNotFoundError as exc:
            raise HTTPException(503, "运行记录数据库尚未创建") from exc
        except CollectionTaskDataError as exc:
            logging.getLogger(__name__).error("runtime record JSON is corrupt", exc_info=True)
            raise HTTPException(409, "运行记录格式损坏") from exc
        except ValueError as exc:
            logging.getLogger(__name__).warning("runtime database schema is unavailable", exc_info=True)
            raise HTTPException(503, "运行记录数据库尚未就绪") from exc
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", None)
            if isinstance(code, int) and (code & 0xFF) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                raise HTTPException(503, "运行记录暂时繁忙，请稍后重试", headers={"Retry-After": "1"}) from exc
            message = str(exc).lower()
            if "no such table" in message or "no such column" in message:
                raise HTTPException(503, "运行记录数据库尚未就绪") from exc
            raise
        except sqlite3.DatabaseError as exc:
            code = getattr(exc, "sqlite_errorcode", None)
            if isinstance(code, int) and (code & 0xFF) in {sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB}:
                raise HTTPException(409, "运行记录数据库损坏") from exc
            raise

    def _task_row_card(self, row: dict[str, Any], now: datetime) -> dict[str, Any]:
        state = row["state"] if isinstance(row.get("state"), dict) else {}
        updated = row.get("updated_at")
        if isinstance(updated, datetime):
            updated = updated.replace(tzinfo=timezone.utc) if updated.tzinfo is None else updated.astimezone(timezone.utc)
        else:
            updated = _parse_time(str(updated))
        definition = state.get("definition") if isinstance(state.get("definition"), dict) else {}
        execution = state.get("execution_definition") if isinstance(state.get("execution_definition"), dict) else {}
        definition_units = definition.get("units") if isinstance(definition.get("units"), list) else execution.get("units")
        source_units = state.get("source_units")
        location = "unknown"
        definition_ref = state.get("definition_ref") if isinstance(state.get("definition_ref"), dict) else {}
        summary_ref = state.get("summary") if isinstance(state.get("summary"), dict) else {}
        candidate = definition_ref.get("path") or summary_ref.get("path")
        if isinstance(candidate, str):
            path = Path(os.path.abspath(candidate))
            if path.is_relative_to(self.paths["workspace_root"]):
                location = "workspace"
            elif path.is_relative_to(self.paths["archive_root"]):
                location = "archive"
        requested_date = state.get("requested_date")
        if requested_date is None:
            requested_date = definition.get("trade_date")
        source_unit_count = (len(definition_units) if isinstance(definition_units, list)
                             else state.get("source_unit_count", len(source_units) if isinstance(source_units, list) else 0))
        if state.get("source_unit_count") is None and isinstance(definition_units, list):
            state = {**state, "source_unit_count": len(definition_units)}
        document = {**state, "task_id": row["task_id"], "dataset": row["dataset"],
                    "status": row["status"], "requested_date": requested_date}
        card = self._task_card(document, location, persisted_updated_at=updated, now=now)
        if row["status"] == "waiting" and not document.get("progress"):
            card["completed_units"] = 0
            card["total_units"] = card["source_unit_count"]
        card["queue_order"] = row.get("queue_order")
        card["first_execution_order"] = row.get("first_execution_order")
        return card

    def _find_task(self, task_id: str) -> tuple[dict[str, Any], Path, str]:
        for document, path, location in self._task_documents():
            if document.get("task_id") == task_id:
                return document, path, location
        raise HTTPException(status_code=404, detail="任务不存在")

    def task(self, task_id: str) -> dict[str, Any]:
        document, path, location = self._find_task(task_id)
        card = self._task_card(document, location)
        final_check = document.get("final_check") if isinstance(document.get("final_check"), dict) else {}
        coverage = final_check.get("coverage") if isinstance(final_check.get("coverage"), dict) else {}
        missing = document.get("missing_units") if isinstance(document.get("missing_units"), list) else []
        try:
            definition_path = path.with_name("task.json")
            if not _within(definition_path, path.parent):
                raise ValueError("unmanaged task definition")
            saved_definition = _read_json(definition_path)
        except ValueError:
            saved_definition = {}
        if document.get('format_version') in {'daily-batch-v1', 'daily-batch-v2'}:
            saved_definition = saved_definition.get('definition', {})
        definition = {
            key: value
            for key, value in saved_definition.items()
            if key not in {"units"} and not SENSITIVE_KEY.search(str(key))
        }
        publication = document.get("publication") or {}
        # Full frozen symbol lists remain in task.json; limit the browser payload.
        for key, value in list(definition.items()):
            if isinstance(value, list) and len(value) > 200:
                definition[key] = {"count": len(value), "sample": value[:200]}
        for key, value in list(coverage.items()):
            if isinstance(value, list) and len(value) > 200:
                coverage[key] = {"count": len(value), "sample": value[:200]}
        final_check = {**final_check, "coverage": coverage}
        partition_results = []
        if document.get('format_version') in {'daily-batch-v1', 'daily-batch-v2'}:
            for asset, result in document.get('partitions', {}).items():
                coverage_path = path.with_name(f'coverage.{asset}.json')
                check = _read_json(coverage_path) if _within(coverage_path, path.parent) and coverage_path.is_file() else {}
                unverified_codes = {'unverified_applicability', 'optional_adjustment_unavailable'}
                excluded_codes = {'confirmed_suspended', 'manual_lifecycle_exclusion'}
                issue_codes = [item.get('code') for item in check.get('issues', [])
                    if isinstance(item, dict)
                    and (item.get('code') not in excluded_codes or item.get('asset_type') == asset)]
                partition_results.append({'asset_type': asset, 'status': result['status'],
                    'expected_count': check.get('expected_count'), 'actual_count': check.get('actual_count'),
                    'other_issue_count': sum(code not in unverified_codes | excluded_codes for code in issue_codes),
                    'unverified_count': sum(code in unverified_codes for code in issue_codes),
                    'excluded_count': sum(code in excluded_codes for code in issue_codes),
                    'error': result.get('error')})
        return {
            **card,
            "definition": _redact(definition),
            "definition_hash": document.get("definition_hash"),
            "final_check": _redact(final_check),
            "coverage": _redact(coverage),
            "missing_units": missing[:200],
            "missing_units_truncated": len(missing) > 200,
            "row_count": publication.get("row_count", document.get("row_count")),
            "fallback_used": document.get("fallback_used"),
            "fallback_reason": document.get("fallback_reason"),
            "published_hash": publication.get("manifest_sha256", document.get("published_hash")),
            "partition_results": partition_results,
        }

    def task_units(
        self,
        task_id: str,
        *,
        status: str | None = None,
        query: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        document, summary_path, _ = self._find_task(task_id)
        units = document.get("source_units")
        batch_total = None
        batch_offset = 0
        if document.get('format_version') in {'daily-batch-v1', 'daily-batch-v2'}:
            from ..pipeline.daily import daily_unit_paths
            frozen = _read_json(summary_path.with_name('task.json'))
            plan = frozen['units']
            if query:
                needle = query.casefold()
                plan = [u for u in plan if any(needle in str(u.get(k, '')).casefold() for k in ('key', 'symbol', 'input_id'))]
            if not status:
                batch_total = len(plan)
                batch_offset = (page - 1) * page_size
                plan = plan[batch_offset:batch_offset + page_size]
            units = []
            for unit in plan:
                path = daily_unit_paths(summary_path.parent, unit)['manifest']
                if not _within(path, summary_path.parent):
                    raise HTTPException(409, '任务明细路径越界')
                units.append(_read_json(path) if path.is_file() else {**unit, 'status': 'pending'})
        if not isinstance(units, list):
            units = []
        matching = [(index, item) for index, item in enumerate(units) if isinstance(item, dict)]
        if status:
            matching = [(index, item) for index, item in matching if item.get("status") == status]
        if query:
            needle = query.casefold()
            matching = [(index, item) for index, item in matching if any(
                needle in str(item.get(name, "")).casefold() for name in ("key", "symbol", "input_id", "error"))]
        total = batch_total if batch_total is not None else len(matching)
        matching = [(batch_offset + index, item) for index, item in matching] if batch_total is not None else matching[(page - 1) * page_size:page * page_size]
        safe: list[dict[str, Any]] = []
        allowed = {
            "key", "symbol", "input_id", "status", "provider", "endpoint", "row_count",
            "failure_class", "error", "coverage_rejected", "returned_window", "raw_hash",
            "source_response_hashes", "source_capture_window", "request_scope", "mode",
        }
        for index, item in matching:
            if not isinstance(item, dict):
                continue
            row = {name: item.get(name) for name in allowed if name in item}
            row["index"] = index
            row["evidence"] = []
            for name in ("raw_manifest", "current_raw"):
                reference = item.get(name)
                if reference:
                    manifest = self._evidence_path(str(reference))
                    if manifest:
                        row["evidence"] = self._manifest_records(manifest)
                        break
            safe.append(_redact(row))
        return {"items": safe, "total": total, "page": page, "page_size": page_size,
                "pages": (total + page_size - 1) // page_size}

    def _evidence_path(self, reference: str) -> Path | None:
        candidate = Path(reference).resolve()
        if any(_within(candidate, root) for root in self._evidence_roots()) and candidate.is_file():
            if candidate.name.startswith("manifest") and candidate.suffix == ".ndjson":
                return candidate
        return None

    def _evidence_roots(self) -> tuple[Path, ...]:
        return (self.paths["raw_root"], self.paths["archive_root"] / "_raw_evidence")

    def _manifest_records(self, path: Path) -> list[dict[str, Any]]:
        return [self._raw_record(path, self.paths["data_root"], event, index)
                for index, event in enumerate(_iter_ndjson(path))
                if event.get("event") in RESPONSE_EVENTS]

    def _raw_index_records(self, path: Path) -> list[dict[str, Any]]:
        try:
            stat = path.stat()
        except OSError:
            return []
        stamp = (stat.st_mtime_ns, stat.st_size)
        cached = self._raw_query_index_cache.get(path)
        if cached is not None and cached[0] == stamp:
            return cached[1]
        records = [row for row in _iter_ndjson(path) if row.get("event") == "request_manifest"]
        self._raw_query_index_cache[path] = (stamp, records)
        return records

    def raw_options(self) -> dict[str, Any]:
        root = self.paths["raw_root"]
        dates: list[str] = []
        if root.exists():
            with os.scandir(root) as entries:
                for entry in entries:
                    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", entry.name):
                        continue
                    reparse = entry.is_symlink()
                    if os.name == "nt" and not reparse:
                        reparse = bool(entry.stat(follow_symlinks=False).st_file_attributes
                                       & stat.FILE_ATTRIBUTE_REPARSE_POINT)
                    if reparse:
                        path = Path(entry.path)
                        if path.is_dir() and _within(path, root):
                            dates.append(entry.name)
                    elif entry.is_dir(follow_symlinks=False):
                        dates.append(entry.name)
            dates.sort(reverse=True)
        latest = dates[0] if dates else None
        return {"dates": dates, "latest_date": latest}

    def raw_sources(self, data_date: str) -> dict[str, Any]:
        _component(data_date)
        day = (self.paths["raw_root"] / data_date).resolve()
        root = self.paths["raw_root"].resolve()
        if not day.is_relative_to(root) or not day.is_dir() or data_date.startswith("_"):
            raise HTTPException(status_code=404, detail="原始数据日期不存在")
        sources = []
        for provider in sorted(path for path in day.iterdir() if path.is_dir() and _within(path, day)):
            endpoints = sorted(path.name for path in provider.iterdir() if path.is_dir() and _within(path, day))
            sources.append({"provider": provider.name, "endpoints": endpoints})
        return {"data_date": data_date, "sources": sources}

    @staticmethod
    def _raw_record(path: Path, root: Path, response: dict[str, Any], index: int,
                    *, check_body: bool = True, path_token: str | None = None) -> dict[str, Any]:
        try:
            relative = path.relative_to(root)
        except ValueError:
            relative = path.resolve().relative_to(root.resolve())
        parts = relative.parts
        body_storage = response.get("body_storage")
        body = (path.parent / str(body_storage)).resolve() if body_storage else None
        has_body = bool(body and _within(body, path.parent / "bodies")
                        and (body.is_file() if check_body else True))
        return {
            "id": f"{path_token or _token(path, root)}.{index}",
            "data_date": next((part for part in parts if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part)), None),
            "provider": response.get("provider"),
            "endpoint": response.get("endpoint"),
            "manifest_name": path.name,
            "method": response.get("method"),
            "url": _redact_url(str(response.get("url", ""))),
            "scope": _redact(response.get("scope", {})),
            "fetched_at_utc": response.get("fetched_at_utc"),
            "mode": response.get("mode"),
            "outcome": response.get("outcome"),
            "status_code": response.get("status_code"),
            "event_kind": response.get("event"),
            "sdk_status_code": response.get("sdk_status_code"),
            "representation": response.get("representation"),
            "content_type": response.get("content_type"),
            "encoding": response.get("encoding"),
            "body_bytes": response.get("body_bytes", response.get("payload_bytes")),
            "body_sha256": response.get("body_sha256"),
            "error": _redact(response.get("error")),
            "has_body": has_body,
        }

    def raw_records(
        self,
        *,
        data_date: str | None,
        provider: str | None,
        endpoint: str | None,
        query: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, Any]:
        options = self.raw_options()
        chosen_date = data_date or options["latest_date"]
        if not chosen_date:
            return _page([], page, page_size)
        _component(chosen_date)
        if provider:
            _component(provider)
        if endpoint:
            _component(endpoint)
        day = (self.paths["raw_root"] / chosen_date).resolve()
        root = self.paths["raw_root"].resolve()
        if not day.is_relative_to(root) or not day.is_dir() or chosen_date.startswith("_"):
            raise HTTPException(status_code=404, detail="原始数据日期不存在")
        if endpoint and not provider:
            raise HTTPException(status_code=400, detail="选择接口前必须先选择来源")
        provider_dirs = [((day / provider).resolve(), provider)] if provider else [
            (item.resolve(), item.name) for item in day.iterdir() if item.is_dir() and _within(item, day)]
        token_root = self.paths["data_root"].resolve()
        candidates = []
        for provider_dir, provider_name in provider_dirs:
            if not provider_dir.is_relative_to(day) or not provider_dir.is_dir():
                continue
            endpoint_dirs = [((provider_dir / endpoint).resolve(), endpoint)] if endpoint else [
                (item.resolve(), item.name) for item in provider_dir.iterdir()
                if item.is_dir() and _within(item, provider_dir)]
            for endpoint_dir, endpoint_name in endpoint_dirs:
                if not endpoint_dir.is_relative_to(provider_dir) or not endpoint_dir.is_dir():
                    continue
                index_path = endpoint_dir / "manifest.ndjson"
                if not index_path.is_file() or not _within(index_path, root):
                    continue
                for line in self._raw_index_records(index_path):
                    manifest_name = line.get("request_manifest")
                    if not isinstance(manifest_name, str) or Path(manifest_name).name != manifest_name:
                        continue
                    manifest = endpoint_dir / manifest_name
                    for summary in line.get("responses", []):
                        if not isinstance(summary, dict) or not isinstance(summary.get("event_index"), int):
                            continue
                        response = summary.get("response")
                        if not isinstance(response, dict):
                            continue
                        candidates.append((str(response.get("fetched_at_utc") or ""), manifest, response,
                                           summary["event_index"], provider_name, endpoint_name))
        if query:
            needle = query.casefold()
            records = []
            for _, manifest, response, event_index, provider_name, endpoint_name in candidates:
                record = self._raw_record(manifest, token_root, response, event_index)
                record["provider"] = record.get("provider") or provider_name
                record["endpoint"] = record.get("endpoint") or endpoint_name
                if needle in json.dumps(record, ensure_ascii=False).casefold():
                    records.append(record)
            records.sort(key=lambda item: str(item.get("fetched_at_utc") or ""), reverse=True)
            result = _page(records, page, page_size)
        else:
            # The default list only needs sort keys for every indexed response.
            # Build full DTOs and check body existence after selecting the page.
            candidates.sort(key=lambda item: item[0], reverse=True)
            start = (page - 1) * page_size
            selected = candidates[start:start + page_size]
            items = []
            for _, manifest, response, event_index, provider_name, endpoint_name in selected:
                relative_manifest = manifest.relative_to(token_root).as_posix().encode("utf-8")
                path_token = base64.urlsafe_b64encode(relative_manifest).decode("ascii").rstrip("=")
                record = self._raw_record(manifest, token_root, response, event_index,
                                          check_body=False, path_token=path_token)
                record["provider"] = record.get("provider") or provider_name
                record["endpoint"] = record.get("endpoint") or endpoint_name
                body_storage = response.get("body_storage")
                body = (manifest.parent / body_storage).resolve() if isinstance(body_storage, str) else None
                record["has_body"] = bool(body and _within(body, manifest.parent / "bodies") and body.is_file())
                items.append(record)
            total = len(candidates)
            result = {"items": items, "page": page, "page_size": page_size, "total": total,
                      "pages": (total + page_size - 1) // page_size}
        result["data_date"] = chosen_date
        return result

    def _raw_document(self, evidence_id: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        try:
            token, index_text = evidence_id.rsplit(".", 1)
            index = int(index_text)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="记录不存在") from exc
        root = self.paths["data_root"]
        path = _from_token(token, root)
        if not any(_within(path, allowed) for allowed in self._evidence_roots()):
            raise HTTPException(status_code=404, detail="记录不存在")
        for position, event in enumerate(_iter_ndjson(path)):
            if position == index and event.get("event") in RESPONSE_EVENTS:
                return path, self._raw_record(path, root, event, index), event
        raise HTTPException(status_code=404, detail="响应记录不存在")

    def raw_detail(self, evidence_id: str, *, preview_bytes: int) -> dict[str, Any]:
        path, record, response = self._raw_document(evidence_id)
        storage = response.get("body_storage")
        preview: dict[str, Any] = {"kind": "unavailable", "text": None, "truncated": False}
        if storage:
            body = (path.parent / str(storage)).resolve()
            if _within(body, path.parent / "bodies") and any(_within(body, root) for root in self._evidence_roots()) and body.is_file():
                with body.open("rb") as stream:
                    payload = stream.read(preview_bytes + 1)
                truncated = len(payload) > preview_bytes
                payload = payload[:preview_bytes]
                encoding = str(response.get("encoding") or "utf-8")
                text: str | None = None
                for candidate in (encoding, "utf-8", "gb18030"):
                    try:
                        decoder = codecs.getincrementaldecoder(candidate)(errors="strict")
                        text = decoder.decode(payload, final=not truncated)
                        break
                    except (UnicodeDecodeError, LookupError):
                        continue
                binary_signature = payload.startswith((b"PK\x03\x04", b"%PDF-", b"\x89PNG", b"\xff\xd8\xff", b"\xd0\xcf\x11\xe0"))
                if text is not None and not binary_signature and "\x00" not in text:
                    stripped = text.strip()
                    if stripped.startswith(("{", "[")):
                        try:
                            text = json.dumps(json.loads(text), ensure_ascii=False, indent=2)
                            kind = "json"
                        except json.JSONDecodeError:
                            kind = "text"
                    else:
                        kind = "text"
                    preview = {"kind": kind, "text": text, "truncated": truncated}
                else:
                    preview = {"kind": "binary", "text": None, "truncated": truncated}
        return {
            **record,
            "request": _redact({
                "method": response.get("method"),
                "url": response.get("url"),
                "headers": response.get("request_headers", {}),
                "scope": response.get("scope", {}),
                "parameters": response.get("request_parameters", {}),
                "options": response.get("request_options", {}),
            }),
            "response": _redact({
                "status_code": response.get("status_code"),
                "headers": response.get("response_headers", {}),
                "content_type": response.get("content_type"),
                "encoding": response.get("encoding"),
                "body_bytes": response.get("body_bytes"),
                "body_sha256": response.get("body_sha256"),
                "persistence_status": response.get("persistence_status"),
                "sdk_status_code": response.get("sdk_status_code"),
                "representation": response.get("representation"),
                "original_transport_bytes_available": response.get("original_transport_bytes_available"),
            }),
            "preview": preview,
        }

    def raw_body(self, evidence_id: str) -> tuple[Path, str | None, str | None]:
        path, record, response = self._raw_document(evidence_id)
        storage = response.get("body_storage")
        if not storage:
            raise HTTPException(status_code=404, detail="原始响应文件不存在")
        body = (path.parent / str(storage)).resolve()
        if (not _within(body, path.parent / "bodies")
                or not any(_within(body, root) for root in self._evidence_roots()) or not body.is_file()):
            raise HTTPException(status_code=404, detail="原始响应文件不存在")
        digest = hashlib.sha256()
        with body.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        if not record.get("body_sha256") or digest.hexdigest() != record["body_sha256"]:
            raise HTTPException(status_code=409, detail="原始响应哈希不匹配，下载已阻止")
        return body, record.get("content_type"), record.get("body_sha256")

    def publications(
        self,
        *,
        dataset: str | None = None,
        asset_type: str | None = None,
        partition_key: str | None = None,
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        with self._runtime_read_snapshot() as metadata:
            result = metadata.query_publications(dataset=dataset, asset_type=asset_type,
                partition_key=partition_key, page=page, page_size=page_size)
        items = [self._publication_row(row) for row in result["items"]]
        return {"scope": "registered_publications", "items": items, "page": page,
                "page_size": page_size, "total": result["total"],
                "pages": (result["total"] + page_size - 1) // page_size,
                "security_master_status": result["security_master_status"]}

    @staticmethod
    def _publication_row(row: dict[str, Any]) -> dict[str, Any]:
        expected, actual = row["expected_count"], row["actual_count"]
        coverage = (round(actual / expected * 100, 2)
                    if isinstance(actual, int) and isinstance(expected, int) and expected > 0 else None)
        updated = row["updated_at"]
        if isinstance(updated, datetime):
            updated = updated.replace(tzinfo=timezone.utc) if updated.tzinfo is None else updated.astimezone(timezone.utc)
            updated = updated.isoformat()
        return {"dataset": row["dataset"], "asset_type": row["asset_type"],
                "partition_key": row["partition_key"], "adjustment": row["adjustment"],
                "status": row["status"], "row_count": actual,
                "expected_count": expected, "coverage_percent": coverage,
                "coverage_reason": row["coverage_reason"], "no_trade_count": row["no_trade_count"],
                "quote_counts": {"none": {"available": row["none_available_count"],
                    "unverified": row["none_unverified_count"], "not_applicable": row["none_not_applicable_count"]},
                    "forward": {"available": row["forward_available_count"],
                    "unverified": row["forward_unverified_count"], "not_applicable": row["forward_not_applicable_count"]},
                    "backward": {"available": row["backward_available_count"],
                    "unverified": row["backward_unverified_count"], "not_applicable": row["backward_not_applicable_count"]}},
                "conflict_count": row["conflict_count"],
                "quarantined_count": row["quarantined_count"],
                "data_revision": row["data_revision"], "updated_at_utc": updated,
                "content_hash": row["content_hash"], "latest_task_id": row["latest_task_id"],
                "raw_date": row["partition_key"]}

    def overview(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        with self._runtime_read_snapshot() as metadata:
            task_snapshot = metadata.collection_task_dashboard(now=now, stale_after_seconds=STALE_AFTER_SECONDS,
                                                                limit=6)
            release_snapshot = metadata.query_publications(page=1, page_size=6)
        tasks = [self._task_row_card(row, now) for row in task_snapshot["items"]]
        releases = [self._publication_row(row) for row in release_snapshot["items"]]
        return {
            "generated_at_utc": now.isoformat(),
            "task_counts": task_snapshot["counts"],
            "recent_tasks": tasks,
            "recent_publications": releases,
            "publication_scope": "registered_publications",
            "security_master_status": release_snapshot["security_master_status"],
            "raw_latest_date": self.raw_options()["latest_date"],
        }


def create_app(
    *, config_root: str | Path = Path("config"), data_root: str | Path | None = None
) -> FastAPI:
    reader = DashboardReader(config_root, data_root)
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.collection_worker_stop_event = Event()
        app.state.collection_worker_thread = None
        try:
            if can_write_data_root() and collection_queue_available():
                start_collection_worker()
            yield
        finally:
            app.state.collection_worker_stop_event.set()
            worker = app.state.collection_worker_thread
            if worker is not None:
                await asyncio.to_thread(worker.join)
            app.state.collection_worker_thread = None

    app = FastAPI(
        title="证券数据运行台",
        description="查询任务、原始证据及发布结果；本机可启动已配置任务",
        version="1.0",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.reader = reader
    app.state.collection_worker_guard = Lock()
    app.state.collection_worker_stop_event = Event()
    app.state.collection_worker_thread = None
    app.add_middleware(SecurityHeadersMiddleware)
    app.mount("/static", StaticFiles(directory=ASSET_ROOT), name="static")

    def require_local_admin_request(request: Request) -> None:
        if (request.client is None or request.client.host not in {"127.0.0.1", "::1"}
                or request.url.hostname not in {"127.0.0.1", "localhost", "::1"}):
            raise HTTPException(403, "只能从本机管理任务")
        origin = request.headers.get("origin")
        if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
            raise HTTPException(403, "请求页面与当前运行台不一致")

    def can_write_data_root() -> bool:
        candidate = reader.paths["data_root"]
        while not candidate.exists() and candidate.parent != candidate:
            candidate = candidate.parent
        return os.access(candidate, os.W_OK | os.X_OK)

    def collection_queue_available() -> bool:
        metadata_path = reader.paths["metadata_path"]
        if not metadata_path.is_file():
            return False
        try:
            from ..storage.metadata import MetadataStore
            with MetadataStore(metadata_path, read_only=True) as metadata:
                metadata.connection.execute("SELECT 1 FROM collection_task LIMIT 0")
                metadata.connection.execute("SELECT 1 FROM collection_task_context LIMIT 0")
            return True
        except (OSError, ValueError, sqlite3.Error):
            logging.getLogger(__name__).warning("collection queue is unavailable; Web will not consume tasks",
                                                exc_info=True)
            return False

    def start_collection_worker() -> bool:
        if not can_write_data_root():
            return False
        with app.state.collection_worker_guard:
            stop_event = app.state.collection_worker_stop_event
            if stop_event.is_set():
                return False
            current = app.state.collection_worker_thread
            if current is not None and current.is_alive():
                return True
            from ..pipeline.inputs import run_collection_worker
            worker = Thread(
                target=run_collection_worker,
                kwargs={"config_root": reader.config_root, "data_root": reader.paths["data_root"],
                        "stop_event": stop_event, "poll_interval_seconds": 0.5},
                name="web-collection-worker",
                daemon=False,
            )
            try:
                worker.start()
            except RuntimeError:
                logging.getLogger(__name__).exception("Web collection worker could not start")
                return False
            app.state.collection_worker_thread = worker
            return True

    def task_acceptance_response(accepted: dict[str, Any], *, dataset: str, resumed: bool) -> JSONResponse:
        task_id = str(accepted.get("task_id", ""))
        status = str(accepted.get("status", "waiting"))
        if accepted.get("no_op"):
            return JSONResponse(status_code=200, content={
                "task_id": task_id,
                "dataset": dataset,
                "status": status,
                "resumed": resumed,
                "already_queued": False,
                "no_op": True,
                "queue_order": accepted.get("queue_order"),
                "message": accepted.get("reason", "任务已经完成，无需重复提交"),
            })
        worker_active = start_collection_worker()
        if worker_active:
            message = ("任务已持久受理并进入FIFO队列；可在运行记录中查看等待或执行状态")
        else:
            message = ("任务已持久受理并保留在FIFO队列；当前没有可用执行worker，后续可写运行台启动后继续")
        return JSONResponse(status_code=202, content={
            "task_id": task_id,
            "dataset": dataset,
            "status": status,
            "resumed": resumed,
            "already_queued": bool(accepted.get("already_queued")),
            "queue_order": accepted.get("queue_order"),
            "worker_active": worker_active,
            "message": message,
        })

    def runtime_task(task_id: str) -> dict[str, Any] | None:
        metadata_path = reader.paths["metadata_path"]
        if not metadata_path.is_file():
            return None
        from ..storage.metadata import MetadataStore
        try:
            with MetadataStore(metadata_path, read_only=True) as metadata:
                return metadata.load_collection_task(task_id)
        except (OSError, ValueError, sqlite3.Error) as exc:
            raise HTTPException(503, "任务运行库不可读取") from exc

    def frozen_task_for_resume(task_id: str, state: dict[str, Any]) -> tuple[dict[str, Any], str]:
        dataset = state.get("dataset")
        if dataset not in {"daily_bar", "security_master"}:
            raise HTTPException(409, "当前页面只支持续跑证券名单和日线任务")
        reference = state.get("definition_ref")
        task_path = Path(reference.get("path", "")) if isinstance(reference, dict) else Path()
        if (task_path.name != "task.json" or not _within(task_path, reader.paths["workspace_root"])
                or not task_path.is_file()):
            raise HTTPException(409, "归档任务或缺少冻结定义的任务不能从页面续跑")
        if not isinstance(reference.get("sha256"), str):
            raise HTTPException(409, "冻结任务定义缺少完整性记录，不能安全续跑")
        from ..storage.integrity import file_hash
        try:
            if file_hash(task_path) != reference["sha256"]:
                raise HTTPException(409, "冻结任务定义已变化，不能安全续跑")
            frozen = _read_json(task_path)
        except (OSError, ValueError) as exc:
            raise HTTPException(409, "冻结任务定义无法读取，不能安全续跑") from exc
        if dataset == "daily_bar":
            definition = frozen.get("request")
            mode = frozen.get("mode") or state.get("verification_mode")
            resolved_definition = frozen.get("definition")
            definition_matches_dataset = (
                isinstance(resolved_definition, dict)
                and resolved_definition.get("dataset") == dataset
                and isinstance(definition, dict)
                and definition.get("dataset") in {None, dataset}
            )
        else:
            definition = frozen.get("execution_definition") or state.get("execution_definition")
            mode = state.get("verification_mode")
            definition_matches_dataset = (isinstance(definition, dict)
                                          and definition.get("dataset") == dataset)
        if (not isinstance(definition, dict) or definition.get("task_id") != task_id
                or not definition_matches_dataset):
            raise HTTPException(409, "冻结任务编号或数据集不匹配，不能安全续跑")
        if mode not in {"live", "replay", "retained"}:
            raise HTTPException(409, "冻结任务缺少有效运行模式，不能安全续跑")
        return definition, mode

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(ASSET_ROOT / "index.html", media_type="text/html")

    @app.get("/api/overview")
    def overview():
        return reader.overview()

    @app.get("/api/data/options")
    def data_options():
        return reader.data_options()

    @app.get("/api/data")
    def data_records(dataset: str, security: str = "", start_date: date | None = None,
                     end_date: date | None = None, provider: str = "", asset_type: str = "",
                     adjustment: str = "", page: int = Query(1, ge=1),
                     page_size: int = Query(50, ge=1, le=100)):
        return reader.data_records(dataset=dataset, security=security, start_date=start_date,
                                   end_date=end_date, provider=provider, asset_type=asset_type,
                                   adjustment=adjustment, page=page, page_size=page_size)

    @app.get("/api/daily")
    def daily_records(security: str = "", start_date: date | None = None, end_date: date | None = None,
                      asset_type: str = "", adjustment: str = "none", page: int = Query(1, ge=1),
                      page_size: int = Query(50, ge=1, le=100)):
        from .market_data import (DailyQueryBusy, DailyQueryInputError, DailyQueryIntegrityError,
                                  DailyQueryNotReady, query_daily)
        from ..service.instruments import read_published_security_master
        from ..storage.parquet import (InvalidPartitionError, PartitionBusyError,
                                       PartitionLockedError)
        try:
            security_policy = yaml.safe_load((reader.config_root / "datasets/security_master.yaml").read_text(encoding="utf-8")) or {}
            result = query_daily(reader.paths["metadata_path"], security=security, start_date=start_date,
                end_date=end_date, asset_type=asset_type, adjustment=adjustment, page=page, page_size=page_size,
                symbol_successions=security_policy.get("symbol_successions", []))
            name_status, name_reason, name_date, catalog = "not_loaded", "当前页没有行情记录", "", {}
            if result["items"]:
                keys = []
                for row in result["items"]:
                    keys.append((row["instrument_id"], row["asset_type"]))
                current_dir = reader.paths["canonical_root"] / "security_master" / "current"
                try:
                    manifest, records = read_published_security_master(current_dir, identities=keys)
                    name_date = str(manifest.publication_metadata.get("as_of_date") or manifest.partition_key)
                    catalog = {(record.instrument_id, record.asset_type.value): record
                               for record in records}
                    name_status, name_reason = "available", ""
                except FileNotFoundError:
                    name_status, name_reason = "unavailable", "正式证券清单不存在"
                except (PartitionBusyError, PartitionLockedError):
                    name_status, name_reason = "updating", "证券清单正在更新，名称暂不可读"
                except (InvalidPartitionError, OSError):
                    name_status, name_reason = "unavailable", "正式证券清单未通过完整性检查"
            for row in result["items"]:
                key = (row["instrument_id"], row["asset_type"])
                known = catalog.get(key)
                row.update(security_name=(known.name or "") if known else "",
                           name_source_date=name_date if known else "",
                           security_name_status=("available" if known else
                               "missing" if name_status == "available" else name_status),
                           security_name_reason=("证券清单中没有该市场/资产类型/代码" if not known and name_status == "available"
                               else name_reason),
                           field_metadata=_daily_field_metadata(row))
            result["columns"] = _presentation(reader.config_root, "daily")
            return result
        except DailyQueryInputError as exc:
            raise HTTPException(400, str(exc)) from exc
        except DailyQueryNotReady as exc:
            logging.getLogger(__name__).warning("daily table is not ready", exc_info=True)
            raise HTTPException(503, "正式日K数据库未就绪") from exc
        except DailyQueryBusy as exc:
            logging.getLogger(__name__).warning("daily table read was busy", exc_info=True)
            raise HTTPException(503, "日K查询繁忙，请稍后重试", headers={"Retry-After": "1"}) from exc
        except DailyQueryIntegrityError as exc:
            logging.getLogger(__name__).exception("daily table integrity check failed")
            raise HTTPException(409, "正式日K数据完整性检查失败") from exc

    @app.get("/api/tasks")
    def tasks(
        status: str | None = None,
        dataset: str | None = None,
        q: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int = Query(25, ge=1, le=100),
    ):
        return reader.tasks(status=status, dataset=dataset, query=q, page=page, page_size=page_size)

    @app.get("/api/task-options")
    def task_options():
        try:
            from ..config.loader import load_input_capabilities
            collection = yaml.safe_load((reader.config_root / "collection.yaml").read_text(encoding="utf-8")) or {}
            schedules = yaml.safe_load((reader.config_root / "schedules.yaml").read_text(encoding="utf-8")) or {}
            capabilities = yaml.safe_load((reader.config_root / "capabilities.yaml").read_text(encoding="utf-8")) or {}
            jobs = {item.get("name"): item for item in schedules.get("jobs", []) if isinstance(item, dict)}
            profiles = collection.get("collection_profiles", {})
            input_labels = {item.input_id: f"{item.provider}/{item.endpoint} · {item.input_id}"
                            for item in load_input_capabilities(reader.config_root / "providers.yaml")}
            definitions = []
            for key, label in (("security_master", "证券名单更新"), ("daily_bar", "全市场日线")):
                configured = collection.get(key, {})
                profile_name = configured.get("collection_profile")
                profile = profiles.get(profile_name, {})
                schedule_name = "security_master_update" if key == "security_master" else "daily_bar_sync"
                if key == "security_master":
                    source_summary = {
                        "当日名单": [input_labels.get(item, item) for item in configured.get("source_inputs", [])],
                        "历史名单": [input_labels.get(item, item) for item in configured.get("historical", {}).get("source_inputs", [])],
                    }
                    scope_summary = "按目标日期选择当日或历史名单配置"
                else:
                    task_name = configured.get("task")
                    target = collection.get("tasks", {}).get(task_name, {})
                    policy_name = target.get("acquisition_policy")
                    policy = capabilities.get("acquisition_policies", {}).get(policy_name, {})
                    source_summary = {policy_name or "未配置来源策略": [
                        input_labels.get(item.get("input_id"), item.get("input_id"))
                        for item in policy.get("inputs", []) if isinstance(item, dict)]}
                    assets = target.get("asset_types", [target.get("asset_type", "stock")])
                    scope_summary = f"{target.get('universe', '未配置')}：" + "、".join(assets)
                definitions.append({
                    "dataset": key,
                    "label": label,
                    "configured": isinstance(configured, dict) and bool(configured),
                    "profile": profile_name,
                    "scope_summary": scope_summary,
                    "source_summary": source_summary,
                    "schedule_configured": schedule_name in jobs,
                    "schedule_enabled": (bool(profile.get("scheduling_enabled")) and schedule_name in jobs
                                         and jobs[schedule_name].get("enabled", True) is not False),
                    "schedule_time": (profile.get("frequency") or {}).get("at"),
                    "scheduler_active": False,
                })
            return {"items": definitions, "scheduler_active": False,
                    "storage_target": "当前运行台配置的数据根目录",
                    "manual_run_enabled": can_write_data_root()}
        except (OSError, yaml.YAMLError, AttributeError, TypeError) as exc:
            raise HTTPException(409, "任务配置不可读取") from exc

    @app.post("/api/tasks/run", status_code=202)
    async def run_configured_task(request: Request):
        require_local_admin_request(request)
        if not can_write_data_root():
            raise HTTPException(409, "当前运行台配置的数据目录不可写；只读部署不能启动采集任务")
        if request.headers.get("content-type", "").split(";", 1)[0].strip() != "application/json":
            raise HTTPException(415, "请使用JSON提交任务")
        try:
            body = await request.json()
            if not isinstance(body, dict) or set(body) != {"dataset", "trade_date"}:
                raise ValueError("必须选择已支持的数据集和目标日期")
            dataset = body["dataset"]
            if dataset not in {"daily_bar", "security_master"}:
                raise ValueError("当前只支持证券名单更新和全市场日线任务")
            target_day = date.fromisoformat(body["trade_date"])
            task_id = ("market-daily-" if dataset == "daily_bar" else "security-master-") + target_day.strftime("%Y%m%d")
            existing = runtime_task(task_id)
            if existing is not None:
                if existing.get("dataset") != dataset:
                    raise HTTPException(409, "同一任务编号已用于其他数据集，拒绝覆盖")
                reference = existing.get("definition_ref", {})
                path = Path(reference.get("path", "")) if isinstance(reference, dict) else Path()
                if not _within(path, reader.paths["workspace_root"]):
                    raise HTTPException(409, "该日期的任务已归档；页面不会覆盖或改写归档记录")
                if existing.get("status") in {"waiting", "collecting", "checking", "committing"}:
                    return task_acceptance_response({**existing, "already_queued": True},
                                                    dataset=dataset, resumed=False)
                definition, mode = frozen_task_for_resume(task_id, existing)
                if definition.get("trade_date") != target_day.isoformat():
                    raise HTTPException(409, "该任务编号的冻结目标日期不匹配，拒绝覆盖")
                resumed = True
            else:
                try:
                    reader._find_task(task_id)
                except HTTPException as exc:
                    if exc.status_code != 404:
                        raise
                else:
                    raise HTTPException(409, "已有任务文件未登记在当前运行库；页面不会覆盖该记录")
                metadata_path = reader.paths["metadata_path"]
                if not metadata_path.is_file():
                    raise HTTPException(409, "本地交易日历尚未初始化；请先完成交易日历导入")
                try:
                    from .calendar import TradingCalendarStore
                    with TradingCalendarStore(str(metadata_path), read_only=True) as calendar:
                        is_trading_day = calendar.is_trading_day(target_day)
                except (OSError, ValueError, sqlite3.Error) as exc:
                    raise HTTPException(409, "本地交易日历不可读取；任务未受理") from exc
                if is_trading_day is not True:
                    raise HTTPException(409, "目标日期没有已确认的交易日记录；任务未受理")
                from ..worker.scheduler import build_daily_bar_definition, build_security_master_definition
                builder = build_daily_bar_definition if dataset == "daily_bar" else build_security_master_definition
                definition = builder(reader.config_root, trade_date=target_day,
                    now=datetime.now(timezone.utc), is_trading_day=True, task_id=task_id)
                mode = "live"
                resumed = False

            from ..pipeline.inputs import submit_collection_task
            from ..storage.metadata import CollectionTaskScopeConflict, CollectionTaskSnapshotChanged
            try:
                accepted = await run_in_threadpool(submit_collection_task,
                    definition=definition, config_root=reader.config_root, data_root=reader.paths["data_root"],
                    redo="resume", mode=mode)
            except CollectionTaskScopeConflict as exc:
                raise HTTPException(409, f"任务范围冲突，未受理：{exc}") from exc
            except CollectionTaskSnapshotChanged as exc:
                raise HTTPException(409, f"正式数据已变化，请刷新后重试：{exc}") from exc
            return task_acceptance_response(accepted, dataset=dataset, resumed=resumed)
        except HTTPException:
            raise
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", None)
            if isinstance(code, int) and (code & 0xFF) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                raise HTTPException(503, "任务队列繁忙，请稍后重试", headers={"Retry-After": "1"}) from exc
            raise HTTPException(409, "任务队列或运行库不可用") from exc
        except (ValueError, KeyError, TypeError, OSError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.post("/api/tasks/{task_id}/resume", status_code=202)
    async def resume_task(task_id: str, request: Request):
        require_local_admin_request(request)
        if not can_write_data_root():
            raise HTTPException(409, "当前运行台配置的数据目录不可写；只读部署不能续跑任务")
        state = runtime_task(task_id)
        if state is None:
            raise HTTPException(404, "任务不存在")
        if state.get("status") == "published":
            raise HTTPException(409, "已发布任务不能续跑；如需重做，请创建新任务")
        reference = state.get("definition_ref", {})
        path = Path(reference.get("path", "")) if isinstance(reference, dict) else Path()
        if not _within(path, reader.paths["workspace_root"]):
            raise HTTPException(409, "归档任务只读，不能从页面续跑")
        if state.get("status") in {"waiting", "collecting", "checking", "committing"}:
            return task_acceptance_response({**state, "already_queued": True},
                                            dataset=state.get("dataset", ""), resumed=True)
        definition, mode = frozen_task_for_resume(task_id, state)
        try:
            from ..pipeline.inputs import submit_collection_task
            from ..storage.metadata import CollectionTaskScopeConflict, CollectionTaskSnapshotChanged
            accepted = await run_in_threadpool(submit_collection_task,
                definition=definition, config_root=reader.config_root, data_root=reader.paths["data_root"],
                redo="resume", mode=mode)
        except CollectionTaskScopeConflict as exc:
            raise HTTPException(409, f"续跑范围冲突，未受理：{exc}") from exc
        except CollectionTaskSnapshotChanged as exc:
            raise HTTPException(409, f"正式数据已变化，请重新核对后续跑：{exc}") from exc
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", None)
            if isinstance(code, int) and (code & 0xFF) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                raise HTTPException(503, "任务队列繁忙，请稍后重试", headers={"Retry-After": "1"}) from exc
            raise HTTPException(409, "任务队列或运行库不可用") from exc
        except (ValueError, KeyError, TypeError, OSError) as exc:
            raise HTTPException(409, f"冻结任务无法续跑：{exc}") from exc
        return task_acceptance_response(accepted, dataset=state.get("dataset", ""), resumed=True)

    @app.get("/api/tasks/{task_id}")
    def task(task_id: str):
        return reader.task(task_id)

    @app.get("/api/tasks/{task_id}/units")
    def task_units(
        task_id: str,
        status: str | None = None,
        q: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=1, le=100),
    ):
        return reader.task_units(task_id, status=status, query=q, page=page, page_size=page_size)

    @app.get("/api/raw/options")
    def raw_options():
        return reader.raw_options()

    @app.get("/api/raw/sources")
    def raw_sources(data_date: str):
        return reader.raw_sources(data_date)

    @app.get("/api/raw")
    def raw_records(
        data_date: str | None = None,
        provider: str | None = None,
        endpoint: str | None = None,
        q: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int = Query(25, ge=1, le=100),
    ):
        return reader.raw_records(
            data_date=data_date,
            provider=provider,
            endpoint=endpoint,
            query=q,
            page=page,
            page_size=page_size,
        )

    @app.get("/api/raw/{evidence_id}")
    def raw_detail(
        evidence_id: str,
        preview_bytes: int = Query(DEFAULT_PREVIEW_BYTES, ge=1024, le=MAX_PREVIEW_BYTES),
    ):
        return reader.raw_detail(evidence_id, preview_bytes=preview_bytes)

    @app.get("/api/raw/{evidence_id}/download")
    def raw_download(evidence_id: str):
        body, content_type, digest = reader.raw_body(evidence_id)
        name = f"{digest or body.stem}{body.suffix or '.bin'}"
        return FileResponse(body, media_type="application/octet-stream", filename=name)

    @app.get("/api/publications")
    def publications(
        dataset: str | None = None,
        asset_type: str | None = None,
        partition_key: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int = Query(25, ge=1, le=100),
    ):
        return reader.publications(dataset=dataset, asset_type=asset_type, partition_key=partition_key,
                                   page=page, page_size=page_size)

    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="启动证券数据运行台或重建页面查询索引")
    parser.add_argument("--config-root", type=Path, default=Path("config"))
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--rebuild-query-index", action="store_true",
                        help="从现有来源报告和原始清单重建可再生查询索引，然后退出")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help="允许绑定非本机地址；部署前仍需在反向代理层配置认证和 HTTPS",
    )
    args = parser.parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"} and not args.allow_network:
        parser.error("非本机监听地址必须显式使用 --allow-network")
    if args.rebuild_query_index:
        paths = load_storage_paths(args.config_root, data_root=args.data_root)
        source_result = rebuild_source_query_indexes(
            workspace_root=paths["workspace_root"], archive_root=paths["archive_root"],
            config_root=args.config_root,
            query_db_path=paths["data_root"] / SOURCE_QUERY_DATABASE)
        from ..storage.raw import RawObjectStore
        raw_indexes = RawObjectStore.rebuild_indexes(paths["raw_root"])
        print(json.dumps({**source_result, "raw_index_directories": raw_indexes}, ensure_ascii=False))
        return
    import uvicorn

    app = create_app(config_root=args.config_root, data_root=args.data_root)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
