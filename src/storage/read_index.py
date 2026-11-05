"""Rebuildable, file-backed selectors for the local read-only web API."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime
from pathlib import Path, PureWindowsPath
from typing import Any, Iterable


SOURCE_QUERY_INDEX = "source_rows_query_index.ndjson"
REPORT_FIELDS = {
    "dataset", "input_id", "provider", "endpoint", "data_date", "parameters",
    "asset_type", "source_rows", "raw_manifest", "normalization_version", "code_version",
    "applied_unit_contract", "returned_window", "status", "row_count", "original_row_count",
}


def _as_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value)
    try:
        return (date.fromisoformat(text[:10]) if "-" in text[:10]
                else datetime.strptime(text[:8], "%Y%m%d").date()).isoformat()
    except ValueError:
        return None


def _row_value(row: dict[str, Any], mapping: dict[str, Any], canonical: str,
               aliases: Iterable[str]) -> Any:
    keys = (mapping.get(canonical), *aliases)
    return next((row[key] for key in keys if isinstance(key, str) and key in row
                 and row[key] not in (None, "")), None)


def source_report_record(
    report_path: str | Path,
    *,
    run_root: str | Path,
    config_root: str | Path,
    report: dict[str, Any] | None = None,
    field_mapping: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Build selector metadata and row locators without copying source values."""
    path, root = Path(report_path).resolve(), Path(run_root).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        return None
    if report is None:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, UnicodeError):
            return None
    descriptor = report.get("source_rows")
    if not isinstance(descriptor, dict) or not isinstance(descriptor.get("path"), str):
        return None
    source_path = (root / descriptor["path"]).resolve()
    if not source_path.is_relative_to(root):
        return None

    report_data = {key: report[key] for key in REPORT_FIELDS if key in report}
    report_data["source_rows"] = dict(descriptor)
    try:
        payload = source_path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if not descriptor.get("sha256") or digest != descriptor["sha256"]:
            index_error = "来源行文件哈希不匹配"
            rows = []
        else:
            rows = json.loads(payload)
            if (not isinstance(rows, list) or not all(isinstance(item, dict) for item in rows)
                    or (descriptor.get("row_count") is not None and len(rows) != descriptor["row_count"])):
                index_error = "来源行文件不是匹配报告的对象数组"
                rows = []
            else:
                index_error = None
    except (OSError, ValueError, UnicodeError) as exc:
        index_error = str(exc)
        rows = []

    mappings: dict[str, Any] = dict(field_mapping or {})
    normalization_path = Path(config_root) / "normalization" / f"{report.get('dataset', '')}.yaml"
    if field_mapping is None and normalization_path.is_file():
        try:
            import yaml
            normalization = yaml.safe_load(normalization_path.read_text(encoding="utf-8")) or {}
            rule = next((item for item in normalization.get("rules", [])
                         if item.get("input_id") == report.get("input_id")
                         and item.get("version") == report.get("normalization_version")), {})
            mappings = rule.get("field_mapping", {}) or {}
        except (OSError, ValueError, TypeError):
            mappings = {}

    parameters = report.get("parameters") if isinstance(report.get("parameters"), dict) else {}
    request_symbol = parameters.get("symbol") or parameters.get("code") or report.get("request_symbol") or ""
    request_adjustment = str(parameters.get("adjust") or parameters.get("adjustment") or "")
    if request_adjustment:
        request_adjustment = {"forward": "qfq", "backward": "hfq"}.get(request_adjustment, request_adjustment)
    selectors = []
    for index, row in enumerate(rows):
        raw_symbol = _row_value(row, mappings, "stock_code", (
            "instrument_id", "symbol", "code", "stock_code", "证券代码", "基金代码", "SEC_CODE", "FUND_CODE", "hqzqdm"))
        raw_date = _row_value(row, mappings, "trade_date", (
            "trade_date", "bar_time", "date", "日期", "交易日期", "STAT_DATE"))
        date_basis = "来源行日期"
        if (raw_date is None and report.get("dataset") == "daily_bar"
                and report.get("provider") == "tencent" and report.get("endpoint") == "kline_daily"):
            raw_date = row.get("0")
            if raw_date is not None:
                date_basis = "来源行字段0（腾讯日线原始日期）"
        row_date = _as_date(raw_date)
        if row_date is None and report.get("dataset") == "security_snapshot" and parameters.get("trade_date"):
            row_date = _as_date(parameters.get("trade_date"))
            date_basis = "请求快照日期（来源行无日期，不代表独立历史状态证明）"
        row_name = _row_value(row, mappings, "stock_name", (
            "stock_name", "code_name", "name", "证券简称", "基金简称", "SEC_NAME", "FUND_ABBR", "hqzqjc"))
        selectors.append({
            "source_row_index": index,
            "raw_symbol": str(raw_symbol or request_symbol or ""),
            "raw_name": str(row_name or ""),
            "exchange": str(row.get("exchange") or ""),
            "row_asset_type": str(row.get("asset_type") or ""),
            "record_date": row_date,
            "date_basis": date_basis if row_date else "未提供可筛选业务日期",
        })

    return {
        "report_path": path.relative_to(root).as_posix(),
        "report": report_data,
        "source_rows_path": descriptor["path"].replace("\\", "/"),
        "source_rows_sha256": descriptor.get("sha256"),
        "source_row_count": descriptor.get("row_count"),
        "request_symbol": str(request_symbol),
        "adjustment": request_adjustment,
        "selectors": selectors,
        "index_error": index_error,
    }


def append_source_report(
    report_path: str | Path,
    *,
    run_root: str | Path,
    config_root: str | Path,
    field_mapping: dict[str, Any] | None = None,
) -> bool:
    """Atomically append one report's small selectors after its source rows exist."""
    from .parquet import PartitionLock

    root = Path(run_root).resolve()
    record = source_report_record(report_path, run_root=root, config_root=config_root,
                                  field_mapping=field_mapping)
    if record is None:
        return False
    index_path = root / SOURCE_QUERY_INDEX
    line = (json.dumps({"event": "upsert", **record}, ensure_ascii=False,
                       sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    with PartitionLock(root / ".source-query-index.lock", recover_stale=True):
        index_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(index_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            remaining = memoryview(line)
            while remaining:
                written = os.write(fd, remaining)
                if written <= 0:
                    raise OSError("source query index append made no progress")
                remaining = remaining[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
    return True


def _run_roots(root: Path) -> Iterable[Path]:
    if not root.exists():
        return
    for current, dirs, _ in os.walk(root):
        current_path = Path(current)
        relative = current_path.relative_to(root).parts
        if not relative:
            # Workspace replacement audits contain retained source reports and
            # are part of the old query contract. Other private trees (raw
            # evidence, checks and temporary work) are not query inputs.
            dirs[:] = [name for name in dirs if not name.startswith("_") or name == "_workspace_audit"]
        else:
            dirs[:] = [name for name in dirs if not name.startswith("_")]
        if (current_path / SOURCE_QUERY_INDEX).is_file():
            yield current_path


def rebuild_source_query_indexes(*, workspace_root: str | Path, archive_root: str | Path,
                                 config_root: str | Path) -> dict[str, int]:
    """One-time rebuild of derived selectors from retained reports and source rows."""
    roots = [Path(workspace_root).resolve(), Path(archive_root).resolve()]
    reports: dict[Path, list[dict[str, Any]]] = {}
    mappings: dict[tuple[str, str, str], dict[str, Any]] = {}
    report_count = 0
    for root in roots:
        if not root.exists():
            continue
        for current, dirs, files in os.walk(root):
            current_path = Path(current)
            relative = current_path.relative_to(root).parts
            if not relative:
                dirs[:] = [name for name in dirs if not name.startswith("_") or name == "_workspace_audit"]
            else:
                dirs[:] = [name for name in dirs if not name.startswith("_")]
            for name in files:
                if not name.endswith(".report.json"):
                    continue
                path = current_path / name
                try:
                    report = json.loads(path.read_text(encoding="utf-8"))
                    descriptor = report.get("source_rows")
                    if not isinstance(descriptor, dict):
                        continue
                    run_root = path.parents[3].resolve()
                    mapping_key = (str(report.get("dataset", "")), str(report.get("input_id", "")),
                                   str(report.get("normalization_version", "")))
                    if mapping_key not in mappings:
                        mapping: dict[str, Any] = {}
                        normalization_path = Path(config_root) / "normalization" / f"{mapping_key[0]}.yaml"
                        if normalization_path.is_file():
                            try:
                                import yaml
                                normalization = yaml.safe_load(normalization_path.read_text(encoding="utf-8")) or {}
                                rule = next((item for item in normalization.get("rules", [])
                                             if item.get("input_id") == mapping_key[1]
                                             and item.get("version") == mapping_key[2]), {})
                                mapping = rule.get("field_mapping", {}) or {}
                            except (OSError, ValueError, TypeError):
                                mapping = {}
                        mappings[mapping_key] = mapping
                    record = source_report_record(path, run_root=run_root,
                                                  config_root=config_root, report=report,
                                                  field_mapping=mappings[mapping_key])
                    if record is not None:
                        reports.setdefault(run_root, []).append({"event": "upsert", **record})
                except (OSError, ValueError, UnicodeError, IndexError):
                    continue
    for run_root, records in reports.items():
        index_path = run_root / SOURCE_QUERY_INDEX
        temporary = index_path.with_name(index_path.name + ".tmp")
        with temporary.open("wb") as stream:
            for record in records:
                stream.write((json.dumps(record, ensure_ascii=False, sort_keys=True,
                                         separators=(",", ":")) + "\n").encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, index_path)
        report_count += len(records)
    return {"indexed_reports": report_count, "index_files": len(reports)}


def load_source_query_records(*, workspace_root: str | Path, archive_root: str | Path,
                             cache: dict[Path, tuple[tuple[int, int], list[tuple[Path, dict[str, Any]]]]] | None = None
                             ) -> list[tuple[Path, dict[str, Any]]]:
    """Read compact per-run indexes; never fall back to scanning report files."""
    result: list[tuple[Path, dict[str, Any]]] = []
    seen: dict[str, tuple[Path, dict[str, Any]]] = {}
    current_indexes: set[Path] = set()
    for root in (Path(workspace_root).resolve(), Path(archive_root).resolve()):
        for run_root in _run_roots(root):
            index_path = run_root / SOURCE_QUERY_INDEX
            current_indexes.add(index_path)
            try:
                stat = index_path.stat()
                stamp = (stat.st_mtime_ns, stat.st_size)
                cached = cache.get(index_path) if cache is not None else None
                if cached is not None and cached[0] == stamp:
                    entries = cached[1]
                else:
                    entries = []
                    latest: dict[str, tuple[Path, dict[str, Any]]] = {}
                    with index_path.open("r", encoding="utf-8") as stream:
                        for line in stream:
                            try:
                                item = json.loads(line)
                            except ValueError:
                                # Ignore a partial final append after a process interruption.
                                continue
                            if item.get("event") != "upsert" or not isinstance(item.get("report_path"), str):
                                continue
                            relative_text = item["report_path"].replace("\\", "/")
                            relative = Path(relative_text)
                            windows_relative = PureWindowsPath(relative_text)
                            if (relative.is_absolute() or windows_relative.is_absolute() or windows_relative.drive
                                    or any(part in {"", ".", ".."} for part in relative.parts)):
                                continue
                            report_path = run_root / relative
                            latest[str(report_path)] = (report_path, item)
                    entries = list(latest.values())
                    if cache is not None:
                        cache[index_path] = (stamp, entries)
                for report_path, item in entries:
                    seen[str(report_path)] = (report_path, item)
            except OSError:
                continue
    if cache is not None:
        for index_path in set(cache) - current_indexes:
            cache.pop(index_path, None)
    result.extend(seen.values())
    return result
