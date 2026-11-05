"""Rebuildable, file-backed selectors for the local read-only web API."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from datetime import date, datetime
from contextlib import closing
from pathlib import Path
from typing import Any, Iterable


SOURCE_QUERY_INDEX = "source_rows_query_index.ndjson"
SOURCE_QUERY_DATABASE = "source_rows_query_index.sqlite3"
SOURCE_QUERY_SCHEMA_VERSION = 2
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
    query_db_path: str | Path,
    field_mapping: dict[str, Any] | None = None,
) -> bool:
    """Upsert one source report and row selectors into the rebuildable query database."""
    root = Path(run_root).resolve()
    record = source_report_record(report_path, run_root=root, config_root=config_root,
                                  field_mapping=field_mapping)
    if record is None:
        return False
    database = Path(query_db_path).resolve()
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(_connect_query_database(database)) as connection:
        _ensure_query_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        _store_query_record(connection, record, root)
        connection.commit()
    return True


def _connect_query_database(path: Path, *, read_only: bool = False) -> sqlite3.Connection:
    if read_only:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    else:
        connection = sqlite3.connect(path, timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _ensure_query_schema(connection: sqlite3.Connection, *, create_indexes: bool = True) -> None:
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS query_index_info (
            schema_version INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_reports (
            report_id INTEGER PRIMARY KEY,
            report_path TEXT NOT NULL UNIQUE,
            run_root TEXT NOT NULL,
            report_json TEXT NOT NULL,
            report_identity TEXT NOT NULL,
            dataset TEXT NOT NULL,
            input_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            data_date TEXT NOT NULL,
            parameters_json TEXT NOT NULL,
            request_symbol TEXT NOT NULL,
            request_market TEXT NOT NULL,
            request_code TEXT NOT NULL,
            adjustment TEXT NOT NULL,
            report_asset_type TEXT NOT NULL,
            source_rows_path TEXT NOT NULL,
            source_rows_sha256 TEXT NOT NULL,
            source_row_count INTEGER,
            index_error TEXT
        );
        CREATE TABLE IF NOT EXISTS source_selectors (
            report_id INTEGER NOT NULL REFERENCES source_reports(report_id) ON DELETE CASCADE,
            source_row_index INTEGER NOT NULL,
            raw_symbol TEXT NOT NULL,
            raw_name TEXT NOT NULL,
            raw_name_folded TEXT NOT NULL,
            exchange TEXT NOT NULL,
            row_asset_type TEXT NOT NULL,
            record_date TEXT,
            date_basis TEXT NOT NULL,
            row_market TEXT NOT NULL,
            row_code TEXT NOT NULL,
            PRIMARY KEY (report_id, source_row_index)
        );
    """)
    rows = connection.execute("SELECT schema_version FROM query_index_info").fetchall()
    if not rows:
        connection.execute("INSERT INTO query_index_info(schema_version) VALUES (?)",
                           (SOURCE_QUERY_SCHEMA_VERSION,))
    elif rows[-1][0] != SOURCE_QUERY_SCHEMA_VERSION:
        raise ValueError("来源查询数据库版本不匹配；请运行 stock-data-web --rebuild-query-index")
    connection.commit()
    if create_indexes:
        _create_query_indexes(connection)


def _create_query_indexes(connection: sqlite3.Connection) -> None:
    connection.executescript("""
        CREATE INDEX IF NOT EXISTS source_reports_filter_idx
            ON source_reports(dataset, provider, adjustment, request_code, data_date);
        CREATE INDEX IF NOT EXISTS source_reports_identity_idx
            ON source_reports(report_identity, report_path);
        CREATE INDEX IF NOT EXISTS source_selectors_security_idx
            ON source_selectors(row_code, row_market, record_date, report_id);
        CREATE INDEX IF NOT EXISTS source_selectors_date_idx
            ON source_selectors(record_date, report_id, row_code);
    """)


def _symbol_parts(value: Any) -> tuple[str, str]:
    text = str(value or "").strip().lower()
    match = re.fullmatch(r"(?:(xshg|xshe|bse|sh|sz|bj)[.:]?)?(\d{6})", text)
    if not match:
        return "", ""
    market = {"xshg": "sh", "xshe": "sz", "bse": "bj"}.get(match[1], match[1] or "")
    return market, match[2]


def _store_query_record(connection: sqlite3.Connection, record: dict[str, Any], run_root: Path) -> None:
    report = record["report"]
    report_path = (run_root / record["report_path"]).resolve()
    descriptor = report["source_rows"]
    parameters = report.get("parameters") if isinstance(report.get("parameters"), dict) else {}
    request_symbol = str(parameters.get("symbol") or parameters.get("code") or record.get("request_symbol") or "")
    request_market, request_code = _symbol_parts(request_symbol)
    request_adjustment = str(parameters.get("adjust") or parameters.get("adjustment") or "")
    request_adjustment = {"forward": "qfq", "backward": "hfq"}.get(request_adjustment, request_adjustment)
    identity = json.dumps([report.get("dataset"), report.get("input_id"), report.get("data_date"),
                          descriptor.get("sha256"), parameters], ensure_ascii=False,
                         sort_keys=True, separators=(",", ":"))
    report_json = json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    connection.execute("""
        INSERT INTO source_reports(report_path, run_root, report_json, report_identity, dataset, input_id,
            provider, endpoint, data_date, parameters_json, request_symbol, request_market, request_code,
            adjustment, report_asset_type, source_rows_path, source_rows_sha256, source_row_count, index_error)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(report_path) DO UPDATE SET run_root=excluded.run_root, report_json=excluded.report_json,
            report_identity=excluded.report_identity, dataset=excluded.dataset, input_id=excluded.input_id,
            provider=excluded.provider, endpoint=excluded.endpoint, data_date=excluded.data_date,
            parameters_json=excluded.parameters_json, request_symbol=excluded.request_symbol,
            request_market=excluded.request_market, request_code=excluded.request_code,
            adjustment=excluded.adjustment, report_asset_type=excluded.report_asset_type,
            source_rows_path=excluded.source_rows_path, source_rows_sha256=excluded.source_rows_sha256,
            source_row_count=excluded.source_row_count, index_error=excluded.index_error
    """, (str(report_path), str(run_root), report_json, identity, str(report.get("dataset") or ""),
          str(report.get("input_id") or ""), str(report.get("provider") or ""),
          str(report.get("endpoint") or ""), str(report.get("data_date") or ""),
          json.dumps(parameters, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
          request_symbol, request_market, request_code, request_adjustment,
          str(report.get("asset_type") or ""), str(descriptor.get("path") or ""),
          str(descriptor.get("sha256") or ""), descriptor.get("row_count"), record.get("index_error")))
    report_id = connection.execute("SELECT report_id FROM source_reports WHERE report_path=?",
                                   (str(report_path),)).fetchone()[0]
    connection.execute("DELETE FROM source_selectors WHERE report_id=?", (report_id,))
    selectors = []
    provider_market = {"sse": "sh", "szse": "sz", "bse": "bj"}.get(str(report.get("provider") or "").lower(), "")
    exchange_market = {"xshg": "sh", "xshe": "sz", "bse": "bj",
                       "sse": "sh", "szse": "sz"}
    for item in record.get("selectors", []):
        raw_symbol = str(item.get("raw_symbol") or request_symbol or "")
        market, code = _symbol_parts(raw_symbol)
        if not market:
            market = exchange_market.get(str(item.get("exchange") or "").lower(), provider_market)
        name = str(item.get("raw_name") or "")
        selectors.append((report_id, int(item.get("source_row_index", 0)), raw_symbol, name,
                          name.casefold(), str(item.get("exchange") or ""),
                          str(item.get("row_asset_type") or ""), item.get("record_date"),
                          str(item.get("date_basis") or ""), market, code))
    connection.executemany("""
        INSERT INTO source_selectors(report_id, source_row_index, raw_symbol, raw_name, raw_name_folded,
            exchange, row_asset_type, record_date, date_basis, row_market, row_code)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """, selectors)


def _run_roots(root: Path) -> Iterable[Path]:
    if not root.exists():
        return
    if (root / SOURCE_QUERY_INDEX).is_file():
        yield root
    try:
        date_roots = list(root.iterdir())
    except OSError:
        return
    for date_root in date_roots:
        if date_root.is_symlink() or not date_root.is_dir():
            continue
        if date_root.name == "_workspace_audit":
            # Replacement audits retain reports under one named run directory.
            try:
                run_roots = list(date_root.iterdir())
            except OSError:
                continue
            for run_root in run_roots:
                if (not run_root.is_symlink() and run_root.is_dir()
                        and (run_root / SOURCE_QUERY_INDEX).is_file()):
                    yield run_root
            continue
        if date_root.name.startswith("_"):
            continue
        try:
            dataset_roots = list(date_root.iterdir())
        except OSError:
            continue
        for dataset_root in dataset_roots:
            if dataset_root.is_symlink() or not dataset_root.is_dir() or dataset_root.name.startswith("_"):
                continue
            if (dataset_root / SOURCE_QUERY_INDEX).is_file():
                yield dataset_root
            # Daily batch reports are nested below the dataset by frozen task id.
            try:
                batch_roots = list(dataset_root.iterdir())
            except OSError:
                continue
            for batch_root in batch_roots:
                if (not batch_root.is_symlink() and batch_root.is_dir()
                        and not batch_root.name.startswith("_")
                        and (batch_root / SOURCE_QUERY_INDEX).is_file()):
                    yield batch_root


def rebuild_source_query_indexes(*, workspace_root: str | Path, archive_root: str | Path,
                                 config_root: str | Path, query_db_path: str | Path) -> dict[str, int]:
    """Stream retained reports into one indexed, rebuildable SQLite read model."""
    roots = [Path(workspace_root).resolve(), Path(archive_root).resolve()]
    database = Path(query_db_path).resolve()
    database.parent.mkdir(parents=True, exist_ok=True)
    temporary = database.with_name(database.name + ".rebuild.tmp")
    temporary.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        temporary.with_name(temporary.name + suffix).unlink(missing_ok=True)
    mappings: dict[tuple[str, str, str], dict[str, Any]] = {}
    report_count = 0
    connection = sqlite3.connect(temporary, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        _ensure_query_schema(connection, create_indexes=False)
        connection.execute("BEGIN")
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
                        if not isinstance(report.get("source_rows"), dict):
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
                        record = source_report_record(path, run_root=run_root, config_root=config_root,
                            report=report, field_mapping=mappings[mapping_key])
                        if record is not None:
                            _store_query_record(connection, record, run_root)
                            report_count += 1
                    except (OSError, ValueError, UnicodeError, IndexError, TypeError, KeyError):
                        continue
        connection.commit()
        _create_query_indexes(connection)
        connection.execute("ANALYZE")
        connection.commit()
    except BaseException:
        connection.rollback()
        connection.close()
        temporary.unlink(missing_ok=True)
        for suffix in ("-wal", "-shm"):
            temporary.with_name(temporary.name + suffix).unlink(missing_ok=True)
        raise
    else:
        connection.close()
    os.replace(temporary, database)
    for root in roots:
        for run_root in _run_roots(root):
            (run_root / SOURCE_QUERY_INDEX).unlink(missing_ok=True)
    return {"indexed_reports": report_count, "index_databases": 1}


def load_source_query_records(*, query_db_path: str | Path
                              ) -> list[tuple[Path, dict[str, Any]]]:
    """Load only compact report metadata; row selectors remain in SQLite."""
    path = Path(query_db_path).resolve()
    if not path.is_file():
        return []
    with closing(_connect_query_database(path, read_only=True)) as connection:
        version = connection.execute("SELECT schema_version FROM query_index_info LIMIT 1").fetchone()
        if version is None or version[0] != SOURCE_QUERY_SCHEMA_VERSION:
            raise ValueError("来源查询数据库版本不匹配；请运行 stock-data-web --rebuild-query-index")
        result = []
        for row in connection.execute("SELECT report_path, run_root, report_json, index_error FROM source_reports"):
            report = json.loads(row["report_json"])
            report["_query_index"] = {"index_error": row["index_error"]}
            result.append((Path(row["report_path"]), report))
        return result


def source_query_has_missing_asset_context(query_db_path: str | Path, *, dataset: str,
                                           start_date: str, end_date: str, provider: str = "",
                                           adjustment: str = "", target_market: str = "",
                                           target_code: str = "") -> bool:
    """Check whether filtered rows need frozen task metadata to resolve asset type."""
    path = Path(query_db_path).resolve()
    if not path.is_file():
        return False
    conditions = ["r.dataset=?", "r.report_asset_type=''", "s.record_date>=?", "s.record_date<=?"]
    parameters: list[Any] = [dataset, start_date, end_date]
    if provider:
        conditions.append("r.provider=?")
        parameters.append(provider)
    if adjustment:
        conditions.append("r.adjustment=?")
        parameters.append(adjustment)
    if target_code:
        conditions.extend(("s.row_code=?", "(s.row_market='' OR ?='' OR s.row_market=?)"))
        parameters.extend((target_code, target_market, target_market))
    with closing(_connect_query_database(path, read_only=True)) as connection:
        return connection.execute("""
            SELECT 1 FROM source_selectors s JOIN source_reports r ON r.report_id=s.report_id
            WHERE """ + " AND ".join(conditions) + " LIMIT 1", parameters).fetchone() is not None


def query_source_data_page(
    query_db_path: str | Path,
    *,
    dataset: str,
    security: str,
    target_market: str,
    target_code: str,
    start_date: str | None,
    end_date: str | None,
    provider: str,
    asset_type: str,
    adjustment: str,
    catalog: dict[tuple[str, str], dict[str, str]],
    references: dict[str, dict[str, Any]],
    page: int,
    page_size: int,
) -> dict[str, Any]:
    """Filter/count/order/page selectors in SQLite; fetch source rows only for that page."""
    path = Path(query_db_path).resolve()
    if not path.is_file():
        return {"items": [], "total": 0, "unknown_dates": 0, "conflict": None, "issues": []}
    if (start_date and end_date and not security and not provider and not asset_type and not adjustment):
        return _query_unfiltered_date_page(path, dataset=dataset, start_date=start_date,
            end_date=end_date, catalog=catalog, page=page, page_size=page_size)
    with closing(_connect_query_database(path, read_only=True)) as connection:
        version = connection.execute("SELECT schema_version FROM query_index_info LIMIT 1").fetchone()
        if version is None or version[0] != SOURCE_QUERY_SCHEMA_VERSION:
            raise ValueError("来源查询数据库版本不匹配；请运行 stock-data-web --rebuild-query-index")
        connection.executescript("""
            CREATE TEMP TABLE query_catalog (
                market TEXT NOT NULL, code TEXT NOT NULL, name TEXT NOT NULL,
                name_folded TEXT NOT NULL, asset_type TEXT NOT NULL, name_source_date TEXT NOT NULL,
                PRIMARY KEY(market, code)
            );
            CREATE TEMP TABLE query_task_context (
                report_path TEXT PRIMARY KEY, frozen_asset_type TEXT NOT NULL
            );
        CREATE INDEX query_catalog_code_idx ON query_catalog(code);
        """)
        catalog_rows = [(market, code, str(item.get("name") or ""),
                         str(item.get("name") or "").casefold(), str(item.get("asset_type") or ""),
                         str(item.get("name_source_date") or ""))
                        for (market, code), item in catalog.items() if market and code]
        connection.executemany("INSERT INTO query_catalog VALUES(?,?,?,?,?,?)", catalog_rows)
        context_rows = [(str(Path(report_path).resolve()), str(item.get("frozen_asset_type") or ""))
                        for report_path, item in references.items() if item.get("frozen_asset_type")]
        connection.executemany("INSERT OR REPLACE INTO query_task_context VALUES(?,?)", context_rows)

        conditions = ["r.dataset=?"]
        parameters: list[Any] = [dataset]
        if provider:
            conditions.append("r.provider=?")
            parameters.append(provider)
        if adjustment:
            conditions.append("r.adjustment=?")
            parameters.append(adjustment)
        if target_code:
            conditions.extend(("(r.request_code='' OR r.request_code=?)",
                               "(r.request_market='' OR ?='' OR r.request_market=?)"))
            parameters.extend((target_code, target_market, target_market))
        selector_conditions = []
        if target_code:
            selector_conditions.append("s.row_code=?")
            parameters.append(target_code)
        if selector_conditions:
            conditions.extend(selector_conditions)
        if start_date and end_date:
            conditions.append("((s.record_date>=? AND s.record_date<=?) OR s.record_date IS NULL)")
            parameters.extend((start_date, end_date))
        elif start_date:
            conditions.append("(s.record_date>=? OR s.record_date IS NULL)")
            parameters.append(start_date)
        elif end_date:
            conditions.append("(s.record_date<=? OR s.record_date IS NULL)")
            parameters.append(end_date)

        declared = ["NULLIF(located.row_asset_type,'')", "NULLIF(c.asset_type,'')",
                    "NULLIF(located.report_asset_type,'')", "NULLIF(located.frozen_asset_type,'')"]
        conflict_terms = [f"({left} IS NOT NULL AND {right} IS NOT NULL AND {left}<>{right})"
                          for index, left in enumerate(declared) for right in declared[index + 1:]]
        conflict_sql = " OR ".join(conflict_terms)
        ctes = f"""
            WITH located AS MATERIALIZED (
                SELECT r.report_path, r.run_root, r.report_json, r.report_identity, r.dataset,
                    r.input_id, r.provider, r.endpoint, r.data_date, r.parameters_json,
                    r.request_symbol, r.request_market, r.request_code, r.adjustment,
                    r.report_asset_type, r.source_rows_path, r.source_rows_sha256,
                    r.source_row_count, r.index_error,
                    s.source_row_index, s.raw_symbol, s.raw_name, s.raw_name_folded,
                    s.exchange, s.row_asset_type, s.record_date, s.date_basis,
                    s.row_code,
                    CASE WHEN s.row_market<>'' THEN s.row_market
                         WHEN (SELECT COUNT(DISTINCT c2.market) FROM query_catalog c2 WHERE c2.code=s.row_code)=1
                         THEN (SELECT MIN(c3.market) FROM query_catalog c3 WHERE c3.code=s.row_code)
                         ELSE '' END AS resolved_market,
                    tc.frozen_asset_type
                FROM source_reports r
                JOIN source_selectors s ON s.report_id=r.report_id
                LEFT JOIN query_task_context tc ON tc.report_path=r.report_path
                WHERE {' AND '.join(conditions)}
            ), mapped AS MATERIALIZED (
                SELECT located.*, c.name AS catalog_name, c.name_folded AS catalog_name_folded,
                    c.asset_type AS catalog_asset_type, c.name_source_date,
                    CASE WHEN located.raw_name<>'' THEN located.raw_name ELSE COALESCE(c.name,'') END AS resolved_name,
                    COALESCE({declared[0]}, {declared[1]}, {declared[2]}, {declared[3]}, '') AS effective_asset_type,
                    CASE WHEN {conflict_sql} THEN 1 ELSE 0 END AS asset_conflict,
                    CASE WHEN located.row_code<>'' THEN located.resolved_market||located.row_code
                         ELSE located.raw_symbol END AS resolved_symbol
                FROM located
                LEFT JOIN query_catalog c ON c.market=located.resolved_market AND c.code=located.row_code
            ), scoped AS MATERIALIZED (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY report_identity, source_row_index
                    ORDER BY report_path) AS duplicate_rank
                FROM mapped
                WHERE 1=1
        """
        if target_code:
            # Apply market disambiguation after exchange/catalog resolution.
            ctes += " AND (resolved_market='' OR ?='' OR resolved_market=?)\n"
            parameters.extend((target_market, target_market))
        if security and not target_code:
            ctes += (" AND instr(CASE WHEN raw_name_folded<>'' THEN raw_name_folded "
                     "ELSE COALESCE(catalog_name_folded,'') END,?)>0\n")
            parameters.append(security.casefold())
        ctes += """
            ), unique_rows AS MATERIALIZED (
                SELECT * FROM scoped WHERE duplicate_rank=1
            ), conflicts AS (
                SELECT resolved_market, row_code FROM unique_rows WHERE asset_conflict=1 LIMIT 1
            ), asset_rows AS MATERIALIZED (
                SELECT * FROM unique_rows WHERE (?='' OR effective_asset_type=?)
            ), date_rows AS MATERIALIZED (
                SELECT * FROM asset_rows
                WHERE (? IS NULL OR (record_date IS NOT NULL AND record_date>=?))
                  AND (? IS NULL OR (record_date IS NOT NULL AND record_date<=?))
            ), totals AS (
                SELECT (SELECT COUNT(*) FROM date_rows) AS total,
                    CASE WHEN ?=1 THEN
                        (SELECT COUNT(*) FROM asset_rows WHERE record_date IS NULL)
                         ELSE 0 END AS unknown_dates
            ), page_rows AS (
                SELECT * FROM date_rows
                ORDER BY COALESCE(record_date,'') DESC, resolved_symbol DESC, provider DESC
                LIMIT ? OFFSET ?
            )
            SELECT totals.total, totals.unknown_dates,
                (SELECT resolved_market||row_code FROM conflicts LIMIT 1) AS conflict,
                page_rows.report_path, page_rows.run_root, page_rows.report_json,
                page_rows.request_symbol, page_rows.adjustment, page_rows.provider, page_rows.endpoint,
                page_rows.source_rows_path, page_rows.source_rows_sha256, page_rows.source_row_count,
                page_rows.source_row_index, page_rows.record_date, page_rows.date_basis,
                page_rows.resolved_market, page_rows.row_code, page_rows.raw_symbol,
                page_rows.resolved_name, page_rows.effective_asset_type, page_rows.name_source_date
            FROM totals LEFT JOIN page_rows ON 1=1
            ORDER BY CASE WHEN page_rows.report_path IS NULL THEN 0 ELSE 1 END,
                     COALESCE(page_rows.record_date,'') DESC, page_rows.resolved_symbol DESC,
                     page_rows.provider DESC
        """
        parameters.extend((asset_type, asset_type, start_date, start_date, end_date, end_date,
                           int(bool(start_date or end_date)), page_size, (page - 1) * page_size))
        rows = connection.execute(ctes, parameters).fetchall()
        summary = rows[0] if rows else None
        if summary is None:
            return {"items": [], "total": 0, "unknown_dates": 0, "conflict": None, "issues": []}
        items = []
        for row in rows:
            if row["report_path"] is None:
                continue
            items.append({
                "report_path": Path(row["report_path"]), "run_root": Path(row["run_root"]),
                "report": json.loads(row["report_json"]), "request_symbol": row["request_symbol"],
                "adjustment": row["adjustment"], "provider": row["provider"], "endpoint": row["endpoint"],
                "source_rows_path": row["source_rows_path"], "source_rows_sha256": row["source_rows_sha256"],
                "source_row_count": row["source_row_count"], "source_row_index": row["source_row_index"],
                "record_date": date.fromisoformat(row["record_date"]) if row["record_date"] else None,
                "date_basis": row["date_basis"], "row_market": row["resolved_market"],
                "row_code": row["row_code"],
                "symbol": row["resolved_market"] + row["row_code"] if row["row_code"] else row["raw_symbol"],
                "name": row["resolved_name"], "asset_type": row["effective_asset_type"],
                "known": {"name": row["resolved_name"], "asset_type": row["effective_asset_type"],
                          "name_source_date": row["name_source_date"] or ""},
            })
        issue_conditions = ["dataset=?", "index_error IS NOT NULL"]
        issue_parameters: list[Any] = [dataset]
        if provider:
            issue_conditions.append("provider=?")
            issue_parameters.append(provider)
        if adjustment:
            issue_conditions.append("adjustment=?")
            issue_parameters.append(adjustment)
        if target_code:
            issue_conditions.extend(("(request_code='' OR request_code=?)",
                                     "(request_market='' OR ?='' OR request_market=?)"))
            issue_parameters.extend((target_code, target_market, target_market))
        issues = [dict(row) for row in connection.execute(
            "SELECT provider,input_id,index_error FROM source_reports WHERE " + " AND ".join(issue_conditions)
            + " LIMIT 50", issue_parameters)]
        return {"items": items, "total": int(summary["total"]),
                "unknown_dates": int(summary["unknown_dates"]),
                "conflict": ("证券类型上下文冲突: " + str(summary["conflict"])) if summary["conflict"] else None,
                "issues": issues}


def _query_unfiltered_date_page(path: Path, *, dataset: str, start_date: str, end_date: str,
                               catalog: dict[tuple[str, str], dict[str, str]], page: int,
                               page_size: int) -> dict[str, Any]:
    """Fast path used by the data page's default latest-date, all-securities view."""
    with closing(_connect_query_database(path, read_only=True)) as connection:
        version = connection.execute("SELECT schema_version FROM query_index_info LIMIT 1").fetchone()
        if version is None or version[0] != SOURCE_QUERY_SCHEMA_VERSION:
            raise ValueError("来源查询数据库版本不匹配；请运行 stock-data-web --rebuild-query-index")
        total = connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT r.report_identity, s.source_row_index
                FROM source_selectors s JOIN source_reports r ON r.report_id=s.report_id
                WHERE r.dataset=? AND s.record_date>=? AND s.record_date<=?
                GROUP BY r.report_identity, s.source_row_index
            )
        """, (dataset, start_date, end_date)).fetchone()[0]
        unknown_dates = connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT r.report_identity, s.source_row_index
                FROM source_selectors s JOIN source_reports r ON r.report_id=s.report_id
                WHERE r.dataset=? AND s.record_date IS NULL
                GROUP BY r.report_identity, s.source_row_index
            )
        """, (dataset,)).fetchone()[0]
        conflict = None
        for row in connection.execute("""
            SELECT s.row_asset_type, r.report_asset_type, s.row_market, s.row_code
            FROM source_selectors s JOIN source_reports r ON r.report_id=s.report_id
            WHERE r.dataset=? AND s.record_date>=? AND s.record_date<=?
        """, (dataset, start_date, end_date)):
            market, code = row["row_market"], row["row_code"]
            if not market and code:
                markets = {known_market for known_market, known_code in catalog if known_code == code}
                if len(markets) == 1:
                    market = markets.pop()
            known = catalog.get((market, code), {})
            declared = {str(value) for value in (row["row_asset_type"], known.get("asset_type"),
                                                  row["report_asset_type"]) if value}
            if len(declared) > 1:
                conflict = "证券类型上下文冲突: " + market + code
                break
        if conflict:
            return {"items": [], "total": total, "unknown_dates": unknown_dates,
                    "conflict": conflict, "issues": []}
        rows = connection.execute("""
            WITH ranked AS (
                SELECT s.report_id, s.source_row_index, s.record_date, r.provider,
                    CASE WHEN s.row_code<>'' THEN s.row_market||s.row_code ELSE s.raw_symbol END AS sort_symbol,
                    ROW_NUMBER() OVER(PARTITION BY r.report_identity, s.source_row_index
                        ORDER BY r.report_path) AS duplicate_rank
                FROM source_selectors s JOIN source_reports r ON r.report_id=s.report_id
                WHERE r.dataset=? AND s.record_date>=? AND s.record_date<=?
            ), page_keys AS (
                SELECT report_id, source_row_index FROM ranked WHERE duplicate_rank=1
                ORDER BY record_date DESC, sort_symbol DESC, provider DESC
                LIMIT ? OFFSET ?
            )
            SELECT r.report_path, r.run_root, r.report_json, r.request_symbol, r.adjustment,
                r.provider, r.endpoint, r.source_rows_path, r.source_rows_sha256, r.source_row_count,
                r.report_asset_type, s.source_row_index, s.raw_symbol, s.raw_name, s.exchange,
                s.row_asset_type, s.record_date, s.date_basis, s.row_market, s.row_code
            FROM page_keys k JOIN source_reports r ON r.report_id=k.report_id
                JOIN source_selectors s ON s.report_id=k.report_id AND s.source_row_index=k.source_row_index
            ORDER BY s.record_date DESC,
                CASE WHEN s.row_code<>'' THEN s.row_market||s.row_code ELSE s.raw_symbol END DESC,
                r.provider DESC
        """, (dataset, start_date, end_date, page_size, (page - 1) * page_size)).fetchall()
        items = []
        for row in rows:
            report = json.loads(row["report_json"])
            raw_symbol = row["raw_symbol"] or row["request_symbol"]
            market, code = row["row_market"], row["row_code"]
            if not market and code:
                markets = {known_market for known_market, known_code in catalog if known_code == code}
                if len(markets) == 1:
                    market = markets.pop()
            known = catalog.get((market, code), {})
            name = row["raw_name"] or known.get("name", "")
            effective_asset = (row["row_asset_type"] or known.get("asset_type")
                               or row["report_asset_type"] or "")
            items.append({
                "report_path": Path(row["report_path"]), "run_root": Path(row["run_root"]),
                "report": report, "request_symbol": row["request_symbol"],
                "adjustment": row["adjustment"], "provider": row["provider"], "endpoint": row["endpoint"],
                "source_rows_path": row["source_rows_path"], "source_rows_sha256": row["source_rows_sha256"],
                "source_row_count": row["source_row_count"], "source_row_index": row["source_row_index"],
                "record_date": date.fromisoformat(row["record_date"]) if row["record_date"] else None,
                "date_basis": row["date_basis"], "row_market": market, "row_code": code,
                "symbol": market + code if code else raw_symbol, "name": name,
                "asset_type": effective_asset,
                "known": {"name": name, "asset_type": known.get("asset_type") or effective_asset,
                          "name_source_date": known.get("name_source_date", "")},
            })
        issues = [dict(row) for row in connection.execute(
            "SELECT provider,input_id,index_error FROM source_reports "
            "WHERE dataset=? AND index_error IS NOT NULL LIMIT 50", (dataset,))]
        return {"items": items, "total": int(total), "unknown_dates": int(unknown_dates),
                "conflict": None, "issues": issues}
