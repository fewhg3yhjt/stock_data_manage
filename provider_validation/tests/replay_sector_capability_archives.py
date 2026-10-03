"""Rebuild the 2026-10 sector capability derivations without network access."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
import sys
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from stock_data_manage.providers.akshare.boards import AkShareBoardProvider  # noqa: E402
from stock_data_manage.providers.baostock import industry as baostock_industry  # noqa: E402
from stock_data_manage.providers.baostock.industry import (  # noqa: E402
    BaoStockIndustryMembershipProvider,
)


THS_RUN_ID = "2026-10-02-sector-capabilities-network-retry"
RESULTS_DIR = ROOT / "provider_validation/results"
CSRC_INPUT = RESULTS_DIR / "legacy/2026-10-01-security-board-coverage.json"
DERIVED_DIR = RESULTS_DIR / "2026-10-02-sector-derived"
SUMMARY_PATH = RESULTS_DIR / "legacy/2026-10-02-sector-derived-validation.json"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_url(url: str) -> str:
    parts = urlsplit(url)
    query = urlencode(parse_qsl(parts.query, keep_blank_values=True))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _write_csv(name: str, rows: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    target = DERIVED_DIR / name
    target.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0]) if rows else []
    with target.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return {
        "path": target.relative_to(ROOT).as_posix(),
        "rows": len(rows),
        "columns": fields,
        "sha256": _sha256(target.read_bytes()),
    }


class _ResultSet:
    error_code = "0"
    error_msg = ""

    def __init__(self, fields: list[str], rows: list[list[str]]) -> None:
        self.fields = fields
        self._rows = iter(rows)

    def next(self) -> bool:
        try:
            self._current = next(self._rows)
            return True
        except StopIteration:
            return False

    def get_row_data(self) -> list[str]:
        return self._current


class _BaoStockReplay:
    def __init__(self, securities: list[dict[str, Any]]) -> None:
        self.securities = securities

    def query_all_stock(self, *, day: str) -> _ResultSet:
        if day != "2026-09-30":
            raise AssertionError(f"unexpected query_all_stock day: {day}")
        return _ResultSet(
            ["code", "code_name", "tradeStatus"],
            [
                [
                    _source_code(row["code"]),
                    str(row.get("name", "")),
                    "1" if row.get("status") == "active" else "0",
                ]
                for row in self.securities
            ],
        )

    def query_stock_industry(self, *, date: str) -> _ResultSet:
        if date != "2026-09-30":
            raise AssertionError(f"unexpected query_stock_industry date: {date}")
        return _ResultSet(
            ["code", "code_name", "industry", "industryClassification", "updateDate"],
            [
                [
                    _source_code(row["code"]),
                    str(row.get("name", "")),
                    str(row.get("industry", "")),
                    str(row.get("industry_classification", "")),
                    str(row.get("classification_update_date", "")),
                ]
                for row in self.securities
                if row.get("has_industry")
            ],
        )


def _source_code(code: str) -> str:
    prefix = "sh" if str(code).startswith(("60", "68")) else "sz"
    return f"{prefix}.{code}"


@contextmanager
def _bao_session(client: _BaoStockReplay):
    yield client


def main() -> None:
    archive_dir = RESULTS_DIR / "raw" / THS_RUN_ID
    manifest_path = archive_dir / "manifest.ndjson"
    manifest = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines()]
    responses: dict[str, list[tuple[dict[str, Any], bytes]]] = defaultdict(list)
    raw_response_refs: list[dict[str, Any]] = []
    for record in manifest:
        if record.get("event") != "http_response" or record.get("outcome") != "response":
            continue
        compressed = (archive_dir / record["body_storage"]).read_bytes()
        body = gzip.decompress(compressed)
        digest = _sha256(body)
        if digest != record.get("body_sha256"):
            raise AssertionError(f"raw response hash mismatch: {record['body_storage']}")
        responses[record["url"]].append((record, body))
        raw_response_refs.append(
            {
                "provider": "ths via akshare",
                "endpoint": record.get("scope", {}).get("endpoint"),
                "url": record["url"],
                "method": record["method"],
                "status": record["status_code"],
                "body_sha256": digest,
                "body_bytes": len(body),
                "manifest_path": manifest_path.relative_to(ROOT).as_posix(),
            }
        )

    def offline_get(url: str, *_args: Any, **_kwargs: Any) -> requests.Response:
        key = _canonical_url(url)
        if not responses.get(key):
            raise RuntimeError(f"offline replay has no archived response for {key}")
        record, body = responses[key].pop(0)
        response = requests.Response()
        response.status_code = int(record["status_code"])
        response.headers.update(record.get("response_headers", {}))
        response._content = body
        response.url = url
        response.encoding = record.get("response_encoding") or "utf-8"
        return response

    original_get = requests.get
    requests.get = offline_get
    try:
        import akshare as ak

        ak_provider = AkShareBoardProvider(client=ak)
        directory = ak_provider.fetch_industry_list()
        semiconductor = next(row for row in directory.rows if row["board_name"] == "半导体")
        index_daily = ak_provider.fetch_industry_daily(
            "半导体",
            date(2026, 9, 1),
            date(2026, 10, 2),
            board_code=str(semiconductor["board_code"]),
        )
        industry_response = next(
            record
            for record in manifest
            if record.get("scope", {}).get("endpoint") == "stock_fund_flow_industry"
            and record.get("outcome") == "response"
        )
        concept_response = next(
            record
            for record in manifest
            if record.get("scope", {}).get("endpoint") == "stock_fund_flow_concept"
            and record.get("outcome") == "response"
        )
        industry_flow = ak_provider.fetch_fund_flow(
            "industry",
            period="即时",
            snapshot_at=datetime.fromisoformat(industry_response["fetched_at_utc"]),
        )
        concept_flow = ak_provider.fetch_fund_flow(
            "concept",
            period="即时",
            snapshot_at=datetime.fromisoformat(concept_response["fetched_at_utc"]),
        )
    finally:
        requests.get = original_get

    saved_probe = json.loads(CSRC_INPUT.read_text(encoding="utf-8"))
    saved_securities = saved_probe["securities"]["sh_sz"]
    client = _BaoStockReplay(saved_securities)
    original_session = baostock_industry.logged_in_session
    baostock_industry.logged_in_session = lambda: _bao_session(client)
    try:
        csrc_result = BaoStockIndustryMembershipProvider().fetch_snapshot(date(2026, 9, 30))
    finally:
        baostock_industry.logged_in_session = original_session

    DERIVED_DIR.mkdir(parents=True, exist_ok=True)
    derived = {
        "industry_directory": _write_csv("ths-industry-directory.csv", directory.rows),
        "industry_index_daily": _write_csv("ths-semiconductor-index-daily.csv", index_daily.rows),
        "industry_fund_flow": _write_csv("ths-industry-fund-flow-now.csv", industry_flow.rows),
        "concept_fund_flow": _write_csv("ths-concept-fund-flow-now.csv", concept_flow.rows),
        "csrc_industry_membership": _write_csv(
            "baostock-csrc-industry-membership.csv", csrc_result.rows
        ),
    }
    active_denominator = sum(row.get("status") == "active" for row in saved_securities)
    active_covered = sum(row["status"] == "active" for row in csrc_result.rows)
    validation = {
        "raw_response_hashes_verified": True,
        "industry_directory_codes_unique": len(directory.rows)
        == len({row["board_code"] for row in directory.rows}),
        "industry_index_dates_unique": len(index_daily.rows)
        == len({row["trade_date"] for row in index_daily.rows}),
        "industry_index_window_matches_request": all(
            "2026-09-01" <= row["trade_date"] <= "2026-10-02" for row in index_daily.rows
        ),
        "industry_flow_names_unique": len(industry_flow.rows)
        == len({row["board_name"] for row in industry_flow.rows}),
        "concept_flow_names_unique": len(concept_flow.rows)
        == len({row["board_name"] for row in concept_flow.rows}),
        "csrc_membership_codes_unique": len(csrc_result.rows)
        == len({row["stock_code"] for row in csrc_result.rows}),
    }
    if not all(validation.values()):
        raise AssertionError(f"semantic validation failed: {validation}")

    source_hash = _sha256(CSRC_INPUT.read_bytes())
    failed_manifest_path = (
        RESULTS_DIR / "raw/2026-10-02-sector-capabilities-v1/manifest.ndjson"
    )
    failed_manifest = [
        json.loads(line)
        for line in failed_manifest_path.read_text(encoding="utf-8").splitlines()
    ]
    initial_failures = [
        {
            "capability": record.get("scope", {}).get("capability"),
            "endpoint": record.get("scope", {}).get("endpoint"),
            "url": record.get("url"),
            "outcome": record.get("outcome"),
            "error_type": record.get("error_type"),
            "error": record.get("error"),
        }
        for record in failed_manifest
        if record.get("outcome") == "transport_error"
    ]
    summary = {
        "evidence_status": "THS outputs replayed offline from retained exact application response bodies; BaoStock Provider replayed from saved merged probe rows, not raw SDK rows",
        "run_id": THS_RUN_ID,
        "akshare_version": __import__("importlib.metadata", fromlist=["version"]).version("akshare"),
        "transformation_code_sha256": {
            "replay_script": _sha256(Path(__file__).read_bytes()),
            "akshare_boards_provider": _sha256(
                (ROOT / "src/stock_data_manage/providers/akshare/boards.py").read_bytes()
            ),
            "baostock_industry_provider": _sha256(
                (ROOT / "src/stock_data_manage/providers/baostock/industry.py").read_bytes()
            ),
        },
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "industry_directory": {"rows": len(directory.rows)},
            "industry_index_daily": {
                "board": dict(semiconductor),
                "requested_window": ["2026-09-01", "2026-10-02"],
                "rows": len(index_daily.rows),
                "first_date": index_daily.returned_first_key,
                "last_date": index_daily.returned_last_key,
            },
            "industry_fund_flow": {"period": "即时", "rows": len(industry_flow.rows)},
            "concept_fund_flow": {"period": "即时", "rows": len(concept_flow.rows)},
            "csrc_industry_membership": {
                "trade_date": csrc_result.trade_date.isoformat(),
                "rows_including_suspended": len(csrc_result.rows),
                "listed_denominator_including_suspended": csrc_result.coverage_denominator,
                "active_covered": active_covered,
                "active_denominator": active_denominator,
                "active_coverage_percent": round(active_covered * 100 / active_denominator, 4),
                "missing_symbols": list(csrc_result.missing_symbols),
                "source_evidence": CSRC_INPUT.relative_to(ROOT).as_posix(),
                "source_evidence_sha256": source_hash,
                "original_sdk_rows_available": False,
            },
        },
        "raw_response_refs": raw_response_refs,
        "initial_failed_attempts": {
            "manifest_path": failed_manifest_path.relative_to(ROOT).as_posix(),
            "count": len(initial_failures),
            "classification": "local network sandbox socket permission block (WinError 10013); not evidence of provider unavailability",
            "events": initial_failures,
        },
        "derived": derived,
        "validation": validation,
        "upstream_vs_project_provider_comparison": [
            {
                "upstream_function": "ak.stock_board_industry_name_ths()",
                "project_method": "AkShareBoardProvider.fetch_industry_list()",
                "parameters": {},
                "status_and_rows": "HTTP 200; 90 rows; 90 unique codes",
                "raw_endpoint": "GET https://q.10jqka.com.cn/thshy/detail/code/881272/",
            },
            {
                "upstream_function": "ak.stock_board_industry_index_ths(symbol, start_date, end_date)",
                "project_method": "AkShareBoardProvider.fetch_industry_daily(board_name, start_date, end_date, board_code)",
                "parameters": {"symbol": "半导体", "start_date": "20260901", "end_date": "20261002"},
                "status_and_rows": "HTTP 200; 21 rows; 2026-09-01..2026-09-30",
                "raw_endpoint": "GET https://d.10jqka.com.cn/v4/line/bk_881121/01/2026.js",
            },
            {
                "upstream_function": "ak.stock_fund_flow_industry(symbol='即时')",
                "project_method": "AkShareBoardProvider.fetch_fund_flow('industry', period='即时')",
                "parameters": {"symbol": "即时"},
                "status_and_rows": "HTTP 200 pages; 90 rows",
                "raw_endpoint_prefix": "http://data.10jqka.com.cn/funds/hyzjl/",
            },
            {
                "upstream_function": "ak.stock_fund_flow_concept(symbol='即时')",
                "project_method": "AkShareBoardProvider.fetch_fund_flow('concept', period='即时')",
                "parameters": {"symbol": "即时"},
                "status_and_rows": "HTTP 200 pages; 387 rows",
                "raw_endpoint_prefix": "http://data.10jqka.com.cn/funds/gnzjl/",
            },
        ],
        "transport_and_field_comparison": {
            "transport": "Project adapter calls the same AkShare functions and arguments; headers, dynamic THS v cookie, session, and pagination stay inside the installed AkShare version.",
            "request_headers": "Sanitized request/response headers are preserved in manifest.ndjson; Cookie and hexin-v values are redacted.",
            "http_status": "All retained THS application responses are HTTP 200; first isolated attempt failed before connection and is archived above.",
            "fields": "Provider checks the observed exact field set and row width before mapping rows.",
            "units_and_time": "Flow money units and index volume/amount units remain unconfirmed; fund-flow rows have no per-row trade date.",
            "routing": "All new capabilities remain validation_only; no production data was written.",
        },
        "baostock_provider_offline_replay": {
            "source_evidence": CSRC_INPUT.relative_to(ROOT).as_posix(),
            "source_evidence_sha256": source_hash,
            "request_parameters_reproduced": {
                "query_all_stock": {"day": "2026-09-30"},
                "query_stock_industry": {"date": "2026-09-30"},
            },
            "request_count": 2,
            "provider_output_rows": len(csrc_result.rows),
            "active_coverage_percent": round(active_covered * 100 / active_denominator, 4),
            "missing_active_symbols": list(csrc_result.missing_symbols),
            "independent_raw_sdk_comparison": False,
            "note": "Offline normalization replay from saved merged rows; the old evidence did not retain raw SDK field rows. A 2026-10-03 single-code request variant is preserved separately and is not counted as this Provider validation.",
        },
        "semantic_limits": {
            "flow_money_units": "source display units retained without conversion; unconfirmed",
            "flow_time": "instantaneous ranking snapshot; no per-row trade date returned",
            "flow_membership": "fund flow rows are not stock-to-board membership relations",
            "csrc_live_provider": "no same-parameter full-snapshot live request repeated; source evidence predates the Provider and raw SDK rows were not retained",
        },
    }
    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "summary": SUMMARY_PATH.relative_to(ROOT).as_posix(),
                "scope": summary["scope"],
                "derived": derived,
                "validation": validation,
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
