#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run selected a-stock-data probes with conservative per-host pacing and raw evidence."""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import importlib.util
import json
import re
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ROOT = Path(__file__).resolve().parents[2]
UPSTREAM = ROOT / "provider_validation" / "tests" / "source_snapshots" / "a-stock-data" / "a_stock_missing_capabilities.py"
DEFAULT_OUTPUT_ROOT = ROOT / "provider_validation" / "results" / "live-probes"
MIN_HOST_INTERVAL_SECONDS = 3.0
MIN_RETRY_BACKOFF_SECONDS = 5.0
MAX_RETRIES = 2
POLICY_VERSION = "rate-limited-live-probe-v1"


class HostPausedError(ConnectionError):
    pass


class ConservativeRetry(Retry):
    """Preserve limited transient retries, stop on access/rate blocks, and always back off."""

    def get_backoff_time(self) -> float:
        base = super().get_backoff_time()
        return max(MIN_RETRY_BACKOFF_SECONDS, base) if self.history else base

    def is_retry(self, method: str, status_code: int, has_retry_after: bool = False) -> bool:
        if status_code in (403, 429):
            return False
        return super().is_retry(method, status_code, has_retry_after)


def retry_policy() -> ConservativeRetry:
    return ConservativeRetry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        status=MAX_RETRIES,
        backoff_factor=MIN_RETRY_BACKOFF_SECONDS,
        backoff_max=300,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )


def configure_new_sessions(policy: Retry) -> tuple[Any, Any]:
    """Apply the probe retry policy to the upstream session and future requests sessions."""
    original_init = requests.Session.__init__

    def init_with_policy(session: requests.Session, *args: Any, **kwargs: Any) -> None:
        original_init(session, *args, **kwargs)
        session.mount("http://", HTTPAdapter(max_retries=policy))
        session.mount("https://", HTTPAdapter(max_retries=policy))

    requests.Session.__init__ = init_with_policy
    return original_init, requests.Session.send


def paced_send_wrapper(original_send: Any) -> Any:
    host_lock = threading.Lock()
    last_start: dict[str, float] = {}
    consecutive_errors: dict[str, int] = {}
    paused_hosts: dict[str, str] = {}

    def paced_send(session: requests.Session, request: Any, **kwargs: Any) -> Any:
        host = (urlparse(str(getattr(request, "url", ""))).hostname or "unknown").lower()
        with host_lock:
            if host in paused_hosts:
                raise HostPausedError(f"Probe host paused: {host}: {paused_hosts[host]}")
            now = time.monotonic()
            wait = MIN_HOST_INTERVAL_SECONDS - (now - last_start.get(host, 0.0))
            if wait > 0:
                time.sleep(wait)
            last_start[host] = time.monotonic()
        try:
            response = original_send(session, request, **kwargs)
        except Exception as exc:
            with host_lock:
                consecutive_errors[host] = consecutive_errors.get(host, 0) + 1
                if consecutive_errors[host] >= 2:
                    paused_hosts[host] = "two consecutive transport errors"
            raise
        status = int(getattr(response, "status_code", 0) or 0)
        with host_lock:
            if status in (403, 429):
                paused_hosts[host] = f"HTTP {status}; no further requests to this host in this run"
            elif status >= 500:
                consecutive_errors[host] = consecutive_errors.get(host, 0) + 1
                if consecutive_errors[host] >= 2:
                    paused_hosts[host] = f"{consecutive_errors[host]} consecutive server errors"
            else:
                consecutive_errors[host] = 0
        return response

    return paced_send


def load_upstream_module() -> Any:
    source_dir = str(UPSTREAM.parent)
    if source_dir not in sys.path:
        sys.path.insert(0, source_dir)
    spec = importlib.util.spec_from_file_location("a_stock_data_rate_limited_source_probe", UPSTREAM)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load upstream probe: {UPSTREAM}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def install_scope_policy(module: Any) -> Any:
    archive_cls = module.RawResponseArchive
    original_scope = archive_cls.scope

    @contextlib.contextmanager
    def scope_with_policy(self: Any, **metadata: str):
        metadata.update(
            {
                "probe_policy": POLICY_VERSION,
                "min_host_interval_seconds": str(MIN_HOST_INTERVAL_SECONDS),
                "retry_policy": f"max_{MAX_RETRIES}_retries; min_backoff_{MIN_RETRY_BACKOFF_SECONDS}s; no_retry_403_429",
            }
        )
        with original_scope(self, **metadata):
            yield

    archive_cls.scope = scope_with_policy
    return original_scope


def parse_ids(raw: str, module: Any) -> list[int]:
    ids = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        value = int(token)
        if value not in module.CAPABILITIES:
            raise ValueError(f"Unknown a-stock-data capability id: {value}")
        ids.append(value)
    if not ids:
        raise ValueError("Pass an explicit comma-separated --ids list; this runner never defaults to all endpoints.")
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate capability ids are not allowed.")
    return ids


def main() -> int:
    parser = argparse.ArgumentParser(description="Run explicit a-stock-data probe IDs with conservative rate limits.")
    parser.add_argument("--ids", required=True, help="Explicit capability IDs, for example 1,2; never accepts 'all'.")
    parser.add_argument("--output", type=Path, help="Evidence output directory; defaults to a unique live-probes run directory.")
    parser.add_argument("--code", default="600519")
    parser.add_argument("--start", default=(dt.datetime.now() - timedelta(days=30)).strftime("%Y%m%d"))
    parser.add_argument("--end", default=dt.datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--date", default=None)
    parser.add_argument("--report-date", default="20260930")
    parser.add_argument("--index", default="000300")
    parser.add_argument("--report-pages", type=int, default=1)
    parser.add_argument("--pdf-limit", type=int, default=1)
    parser.add_argument("--option-name", default="华夏上证50ETF期权")
    args = parser.parse_args()

    if args.report_pages < 1 or args.report_pages > 1 or args.pdf_limit < 1 or args.pdf_limit > 1:
        raise ValueError("Live probes are limited to one page and one PDF per capability.")
    if not re.fullmatch(r"\d{6}", re.sub(r"\D", "", args.code)):
        raise ValueError("--code must resolve to a six-digit security code.")

    module = load_upstream_module()
    ids = parse_ids(args.ids, module)
    run_stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_candidate = args.output or (DEFAULT_OUTPUT_ROOT / f"{POLICY_VERSION}-{run_stamp}")
    if not output_candidate.is_absolute():
        output_candidate = ROOT / output_candidate
    output = output_candidate.resolve()
    try:
        output.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise ValueError("Probe output must remain inside the project workspace.") from exc
    output.mkdir(parents=True, exist_ok=False)

    policy = retry_policy()
    module.SESSION.mount("http://", HTTPAdapter(max_retries=policy))
    module.SESSION.mount("https://", HTTPAdapter(max_retries=policy))
    original_init, original_send = configure_new_sessions(policy)
    requests.Session.send = paced_send_wrapper(original_send)
    original_scope = install_scope_policy(module)

    policy_manifest = {
        "record_type": "rate_limited_probe_run_policy",
        "policy_version": POLICY_VERSION,
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_script": UPSTREAM.relative_to(ROOT).as_posix(),
        "selected_capability_ids": ids,
        "min_interval_seconds_per_hostname": MIN_HOST_INTERVAL_SECONDS,
        "requests_are_serial": True,
        "max_retries_for_transient_errors": MAX_RETRIES,
        "min_retry_backoff_seconds": MIN_RETRY_BACKOFF_SECONDS,
        "retry_after_header_honored": True,
        "retry_statuses": [500, 502, 503, 504],
        "403_and_429_retry": False,
        "host_pause_rule": "pause immediately on HTTP 403/429; pause after two consecutive transport or 5xx failures",
        "max_report_pages": 1,
        "max_pdf_downloads_per_capability": 1,
        "fresh_output_directory": output.relative_to(ROOT).as_posix(),
        "raw_archive_subdirectory": "_raw/",
        "note": "The runner only executes explicitly listed IDs. A 403, 429, credential block, or unavailable result is retained as evidence and not retried automatically.",
    }
    (output / "probe-run-policy.json").write_text(json.dumps(policy_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    run_argv = [
        str(UPSTREAM), "--run", ",".join(map(str, ids)), "--output", str(output),
        "--sleep", str(MIN_HOST_INTERVAL_SECONDS), "--code", re.sub(r"\D", "", args.code),
        "--start", re.sub(r"\D", "", args.start), "--end", re.sub(r"\D", "", args.end),
        "--report-date", re.sub(r"\D", "", args.report_date), "--index", str(args.index),
        "--report-pages", "1", "--pdf-limit", "1", "--option-name", args.option_name,
    ]
    if args.date:
        run_argv.extend(["--date", re.sub(r"\D", "", args.date)])
    previous_argv = sys.argv
    try:
        sys.argv = run_argv
        print(json.dumps({"output": output.relative_to(ROOT).as_posix(), "ids": ids, "min_host_interval_seconds": MIN_HOST_INTERVAL_SECONDS}, ensure_ascii=False))
        return int(module.main())
    finally:
        sys.argv = previous_argv
        module.RawResponseArchive.scope = original_scope
        requests.Session.__init__ = original_init
        # RawResponseArchive restores the paced wrapper when its context exits.
        requests.Session.send = original_send


if __name__ == "__main__":
    raise SystemExit(main())
