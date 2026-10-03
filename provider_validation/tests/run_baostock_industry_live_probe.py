#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Probe BaoStock's two industry-snapshot queries with persisted SDK-row evidence."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import sys
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "provider_validation" / "tests"))

from raw_response_archive import RawResponseArchive  # noqa: E402
from stock_data_manage.providers.baostock.industry import (  # noqa: E402
    BaoStockIndustryMembershipProvider,
)


class PacedSdkArchive(RawResponseArchive):
    """Wait three seconds after the first SDK query payload before the next query."""

    def __init__(self, run_id: str, root: Path) -> None:
        super().__init__(run_id, root)
        self._payload_count = 0

    def store_source_payload(self, payload: bytes, **kwargs: Any) -> str:
        digest = super().store_source_payload(payload, **kwargs)
        self._payload_count += 1
        if self._payload_count == 1:
            time.sleep(3.0)
        return digest


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Explicit BaoStock snapshot date: YYYY-MM-DD")
    parser.add_argument("--output", required=True, type=Path, help="New evidence directory inside the project")
    args = parser.parse_args()
    trade_date = date.fromisoformat(args.date)
    output = args.output if args.output.is_absolute() else ROOT / args.output
    output = output.resolve()
    output.relative_to(ROOT.resolve())
    output.mkdir(parents=True, exist_ok=False)
    policy = {
        "record_type": "baostock_low_frequency_live_probe_policy",
        "policy_version": "baostock-sdk-rows-v1",
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "provider": "BaoStock",
        "date_scope": trade_date.isoformat(),
        "request_scope": "one full SH/SZ security-universe query plus one full SH/SZ industry snapshot query",
        "query_count": 2,
        "minimum_interval_seconds_between_queries": 3,
        "evidence_representation": "SDK-decoded ResultSet fields and rows; BaoStock TCP wire bytes are not exposed",
        "raw_archive": "_raw/",
        "production_data_written": False,
    }
    (output / "probe-run-policy.json").write_text(json.dumps(policy, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    archive = PacedSdkArchive("baostock-industry", output / "_raw")
    started = dt.datetime.now(dt.timezone.utc)
    try:
        with archive:
            result = BaoStockIndustryMembershipProvider().fetch_snapshot(trade_date, raw_archive=archive)
    except Exception as exc:
        failure = {
            "record_type": "probe_failure",
            "provider": "baostock",
            "date_scope": trade_date.isoformat(),
            "error_type": type(exc).__name__,
            "error": str(exc)[:2000],
            "validation_time_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        (output / "result.json").write_text(json.dumps(failure, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        raise

    rows = [dict(row) for row in result.rows]
    parsed_path = output / "industry-membership.csv"
    fields = list(rows[0]) if rows else list(result.field_semantics)
    with parsed_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "record_type": "baostock_industry_probe_result",
        "status": "success" if rows else "failed",
        "date_scope": trade_date.isoformat(),
        "listed_symbol_count": len(result.requested_symbols),
        "classified_row_count": len(rows),
        "missing_symbol_count": len(result.missing_symbols),
        "missing_symbols": list(result.missing_symbols),
        "field_semantics": list(result.field_semantics),
        "decoded_rows_manifest": "_raw/baostock-industry/manifest.ndjson",
        "parsed_output": parsed_path.relative_to(ROOT).as_posix(),
        "parsed_output_sha256": sha256(parsed_path),
        "validation_time_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "elapsed_seconds": (dt.datetime.now(dt.timezone.utc) - started).total_seconds(),
    }
    (output / "result.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
