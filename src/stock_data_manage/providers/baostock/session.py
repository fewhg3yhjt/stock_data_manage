from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
from threading import RLock

from ..contracts import FailureClass, ProviderContractError


_SDK_SESSION_LOCK = RLock()


@contextmanager
def logged_in_session(*, client=None) -> Iterator[object]:
    with _SDK_SESSION_LOCK:
        bs = client
        if bs is None:
            try:
                import baostock as bs
            except ImportError as exc:
                raise ProviderContractError(
                    "BaoStock dependency is not installed", FailureClass.CONNECTION, retryable=False
                ) from exc
        login = bs.login()
        if getattr(login, "error_code", None) != "0":
            raise ProviderContractError(
                f"BaoStock login failed: {login.error_code} {login.error_msg}",
                FailureClass.CONNECTION,
                retryable=True,
            )
        try:
            yield bs
        finally:
            bs.logout()


def source_code(symbol: str) -> str:
    text = str(symbol).lower()
    if len(text) < 8 or text[:2] not in {"sh", "sz"}:
        raise ProviderContractError(
            f"BaoStock does not support source symbol {symbol}",
            FailureClass.SCHEMA_CHANGED,
            retryable=False,
        )
    return f"{text[:2]}.{text[2:]}"


def read_rows(result_set: object) -> list[dict[str, object]]:
    error_code = getattr(result_set, "error_code", None)
    if error_code != "0":
        raise ProviderContractError(
            f"BaoStock query failed: {error_code} {getattr(result_set, 'error_msg', '')}",
            FailureClass.CONNECTION,
            retryable=True,
        )
    fields = getattr(result_set, "fields", [])
    fields = fields if isinstance(fields, list) else str(fields).split(",")
    rows: list[dict[str, object]] = []
    while result_set.next():
        rows.append(dict(zip(fields, result_set.get_row_data())))
    return rows


def _decoded_result_set(document):
    """Reproduce the external SDK ResultSet interface for archived decoded rows only."""
    from types import SimpleNamespace
    from collections.abc import Mapping
    fields, rows = document.get("fields", []), document.get("rows", [])
    fields = fields if isinstance(fields, list) else str(fields).split(",")
    if not all(isinstance(name, str) for name in fields) or not isinstance(rows, list):
        raise ValueError("invalid archived SDK fields/rows")
    position = -1
    def next_row():
        nonlocal position
        position += 1
        return position < len(rows)
    def current_row():
        row = rows[position]
        if not isinstance(row, Mapping) or set(row) != set(fields):
            raise ValueError("archived SDK row disagrees with declared fields")
        return [row[name] for name in fields]
    outcome = document.get("result", {})
    return SimpleNamespace(fields=fields, error_code=outcome.get("error_code", "0"),
                           error_msg=outcome.get("error_msg", ""), next=next_row, get_row_data=current_row)


@contextmanager
def captured_sdk_queries(store, *, mode, replay_manifest, evidence_roots, scope, code_version,
                         trade_date, pacer, interval_seconds, max_age_seconds, client=None, query_requests=None):
    """Capture the existing two SDK queries, preserving the live client/session and TCP boundary.

    Replay/cache ResultSets implement the SDK's external fields/next/get_row_data contract;
    they never open a socket and are not alternative Provider implementations.
    """
    with _SDK_SESSION_LOCK:
        from datetime import datetime, timezone
        from pathlib import Path
        from types import SimpleNamespace
        import json
        from ...storage.raw import RawObjectStore, sanitized_metadata, _SECRET_BODY

        events, used, selected = [], set(), {}
        last_payload_time = None
        query_scope = ({"trade_date": trade_date.isoformat(), "exchange_scope": "SH+SZ A shares"}
                       if trade_date else {"code_name": "ST", "exchange_scope": "SH+SZ"})
        archive_records = []
        if mode == "replay":
            manifest = Path(replay_manifest).resolve()
            archive_records = [(number, json.loads(line)) for number, line in
                               enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1)]

        def matches(record, name, parameters):
            return (record.get("event") == "source_payload" and record.get("provider") == "baostock"
                    and record.get("endpoint") == name
                    and (record.get("request_parameters") == parameters if record.get("request_parameters") is not None
                         else trade_date is not None and record.get("metadata", {}).get("trade_date") == trade_date.isoformat()))

        def lookup(name, parameters):
            if mode == "replay":
                found = next(((number, record) for number, record in archive_records
                              if number not in used and matches(record, name, parameters)), None)
                if found is None:
                    return None
                number, record = found
                return manifest, number, record, "replay"
            candidates = []
            now = datetime.now(timezone.utc)
            for root in evidence_roots:
                for path in Path(root).rglob("manifest.ndjson") if Path(root).exists() else ():
                    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                        try:
                            record = json.loads(line)
                            if (not matches(record, name, parameters) or record.get("mode") != "live"
                                    or record.get("code_version") != code_version or record.get("scope") != query_scope
                                    or record.get("sdk_status_code") != "0" or record.get("outcome") != "response"):
                                continue
                            age = (now - datetime.fromisoformat(record["fetched_at_utc"])).total_seconds()
                            if 0 <= age <= max_age_seconds:
                                candidates.append((age, path, number, record))
                        except (KeyError, ValueError, TypeError):
                            continue
            if not candidates:
                return None
            _, path, number, record = min(candidates, key=lambda item: item[0])
            return path.resolve(), number, record, "cached"

        requests = query_requests or {"query_all_stock": {"day": trade_date.isoformat()}, "query_stock_industry": {"date": trade_date.isoformat()}}
        if query_requests is not None and query_requests != {"query_stock_basic": {"code_name": "ST"}}:
            raise ValueError('only the verified ST name query is supported as an additional SDK scope')
        # Search evidence before authenticating or issuing either live SDK query.
        cached = {name: lookup(name, parameters) for name, parameters in requests.items()}
        needs_live = mode == "live" and any(record is None for record in cached.values())
        if needs_live and client is None:
            try:
                import baostock as client
            except ImportError as exc:
                raise ProviderContractError("BaoStock dependency is not installed", FailureClass.CONNECTION, retryable=False) from exc
        pacer.configure("baostock.sdk", max(3, interval_seconds), 1)

        def event_record(name, parameters, payload, metadata, *, capture_mode, source_ref=None):
            if _SECRET_BODY.search(payload):
                import hashlib
                failure = {"event": "sdk_query_failure", "provider": "baostock", "endpoint": name, "method": name,
                           "request_parameters": parameters, "scope": query_scope, "code_version": code_version,
                           "mode": capture_mode, "source_ref": source_ref, "outcome": "secret_retention_suppressed",
                           "body_sha256": hashlib.sha256(payload).hexdigest(), "payload_bytes": len(payload),
                           "redacted": True, "original_transport_bytes_available": False,
                           "fetched_at_utc": datetime.now(timezone.utc).isoformat()}
                store.append_event(failure)
                events.append(failure)
                raise ValueError("SDK payload secret retention suppressed; only hash/metadata saved")
            saved = store.write_bytes(payload, dataset="sdk_response", provider="baostock", endpoint=name,
                                      fetched_at=datetime.now(timezone.utc), attempt_id="sdk-payload", content_addressed=True)
            outcome = metadata.get("outcome")
            error = outcome.get("error_code", "0") if isinstance(outcome, dict) else "0"
            event = {"event": "source_payload", "provider": "baostock", "endpoint": name, "method": name,
                     "request_parameters": parameters, "scope": query_scope, "input_scope": sanitized_metadata(scope),
                     "code_version": code_version, "mode": capture_mode, "source_ref": source_ref,
                     "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "sdk_status_code": str(error),
                     "outcome": "response" if str(error) == "0" else "sdk_error",
                     "metadata": sanitized_metadata(metadata), "body_sha256": saved.content_hash, "payload_bytes": len(payload),
                     "body_storage": saved.path.relative_to(store.root).as_posix(), "byte_exact_archived_representation": True,
                     "original_transport_bytes_available": False,
                     "representation": "SDK-decoded ResultSet fields and rows; BaoStock TCP wire bytes are not exposed",
                     "request_options": {"minimum_query_interval_seconds": max(3, interval_seconds),
                                         "effective_concurrency": 1, "physical_requests_visible": False, "tcp_wire_bytes_visible": False}}
            store.append_event(event)
            events.append(event)
            return event

        def query(name, **parameters):
            if parameters != requests[name]:
                raise ValueError("SDK request disagrees with the bound snapshot date")
            found = cached[name]
            if found is not None:
                path, number, original, capture_mode = found
                if mode == "replay":
                    used.add(number)
                try:
                    payload = RawObjectStore.read_response(path, original)
                except Exception as exc:
                    failure = {"event": "sdk_query_failure", "provider": "baostock", "endpoint": name, "method": name,
                        "request_parameters": parameters, "scope": query_scope, "code_version": code_version, "mode": capture_mode,
                        "outcome": "archive_integrity_error", "error_type": type(exc).__name__,
                        "source_ref": {"manifest": str(path), "line": number, "expected_sha256": original.get("body_sha256")},
                        "fetched_at_utc": datetime.now(timezone.utc).isoformat()}
                    store.append_event(failure)
                    events.append(failure)
                    raise
                # Persist exact archived bytes before decoding the JSON ResultSet representation.
                selected[name] = event_record(name, parameters, payload, original.get("metadata", {}), capture_mode=capture_mode,
                    source_ref={"manifest": str(path), "line": number, "fetched_at_utc": original.get("fetched_at_utc")})
                return _decoded_result_set(json.loads(payload))
            if mode == "replay":
                failure = {"event": "sdk_query_failure", "provider": "baostock", "endpoint": name, "method": name,
                           "request_parameters": parameters, "scope": query_scope, "input_scope": sanitized_metadata(scope),
                           "code_version": code_version, "mode": "replay", "outcome": "replay_miss",
                           "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "error_type": "ValueError"}
                store.append_event(failure)
                events.append(failure)
                raise ValueError("no exact archived SDK request match; replay never falls back to network")
            try:
                with pacer.request("baostock.sdk"):
                    # The verified probe waited after persisting the first decoded payload,
                    # including time spent exhausting its lazy ResultSet, before query two.
                    if last_payload_time is not None:
                        remaining = max(3, interval_seconds) - (pacer.clock() - last_payload_time)
                        if remaining > 0:
                            pacer.wait(remaining)
                    return getattr(client, name)(**parameters)
            except Exception as exc:
                failure = {"event": "sdk_query_failure", "provider": "baostock", "endpoint": name, "method": name,
                           "request_parameters": parameters, "scope": query_scope, "code_version": code_version,
                           "mode": "live", "outcome": "transport_error", "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                           "error_type": type(exc).__name__}
                store.append_event(failure)
                events.append(failure)
                raise

        def persist_decoded(payload, *, provider, endpoint, representation, metadata):
            nonlocal last_payload_time
            if endpoint in selected:
                event = selected[endpoint]
                reference = store.write_bytes(payload, dataset="sdk_derived", provider=provider, endpoint=endpoint,
                                              fetched_at=datetime.now(timezone.utc), attempt_id="sdk-derived", content_addressed=True)
                store.append_event({"event": "sdk_derived", "endpoint": endpoint, "code_version": code_version,
                                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "row_count": metadata["row_count"],
                                    "body_storage": reference.path.relative_to(store.root).as_posix(), "body_sha256": reference.content_hash,
                                    "source_response_sha256": event["body_sha256"]})
            else:
                selected[endpoint] = event_record(endpoint, requests[endpoint], payload, metadata, capture_mode="live")
                last_payload_time = pacer.clock()

        def session_call(name):
            try:
                result = getattr(client, name)() if needs_live else SimpleNamespace(error_code="0", error_msg="")
            except Exception as exc:
                store.append_event({"event": "sdk_session", "method": name, "mode": "live", "actual_session_called": True,
                                    "outcome": "transport_error", "error_type": type(exc).__name__,
                                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "code_version": code_version})
                raise
            store.append_event({"event": "sdk_session", "method": name, "mode": "live" if needs_live else "replay" if mode == "replay" else "cached",
                                "actual_session_called": needs_live, "sdk_status_code": str(getattr(result, "error_code", "unknown")),
                                "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "code_version": code_version})
            return result

        sdk_client = SimpleNamespace(login=lambda: session_call("login"), logout=lambda: session_call("logout"),
                                     **{name: (lambda name=name, **kwargs: query(name, **kwargs)) for name in requests})
        yield sdk_client, SimpleNamespace(store_source_payload=persist_decoded), events
