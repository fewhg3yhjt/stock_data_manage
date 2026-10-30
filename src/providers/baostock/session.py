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
    while True:
        page_before = getattr(result_set, "cur_page_num", None)
        page_rows = getattr(result_set, "data", None)
        page_row_count = len(page_rows) if isinstance(page_rows, list) else None
        try:
            has_row = result_set.next()
        except Exception as exc:
            raise ProviderContractError(
                f"BaoStock pagination failed while reading page {page_before}: {type(exc).__name__}: {exc}",
                FailureClass.CONNECTION,
                retryable=True,
            ) from exc

        if has_row:
            rows.append(dict(zip(fields, result_set.get_row_data())))
            continue

        error_code = getattr(result_set, "error_code", None)
        error_msg = getattr(result_set, "error_msg", "")
        if error_code != "0":
            raise ProviderContractError(
                f"BaoStock pagination failed after page {page_before}: {error_code} {error_msg}",
                FailureClass.CONNECTION,
                retryable=True,
            )

        page_after = getattr(result_set, "cur_page_num", None)
        page_size = getattr(result_set, "per_page_count", None)
        try:
            full_page = page_row_count is not None and int(page_size) > 0 and page_row_count >= int(page_size)
        except (TypeError, ValueError):
            full_page = False
        if full_page and page_after == page_before:
            raise ProviderContractError(
                f"BaoStock pagination stopped after full page {page_before} without advancing; "
                "the SDK may have hidden a failed or empty next-page response",
                FailureClass.CONNECTION,
                retryable=True,
            )
        break
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


class _RecordingSocket:
    """Delegate the BaoStock socket while persisting bytes before returning them to its decoder."""

    def __init__(self, inner, on_receive):
        self._inner = inner
        self._on_receive = on_receive

    def recv(self, size, *args):
        chunk = self._inner.recv(size, *args)
        if chunk:
            self._on_receive(bytes(chunk))
        return chunk

    def recv_into(self, buffer, nbytes=None, *args):
        count = self._inner.recv_into(buffer, *args) if nbytes is None else self._inner.recv_into(buffer, nbytes, *args)
        if count:
            self._on_receive(bytes(memoryview(buffer)[:count]))
        return count

    def send(self, data, *args):
        return self._inner.send(data, *args)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _CapturedResultSet:
    """Finalize the active receive stream only after BaoStock has exhausted this result set."""

    def __init__(self, inner, finish):
        self._inner = inner
        self._finish = finish

    @property
    def error_code(self):
        value = getattr(self._inner, "error_code", None)
        if value != "0":
            self._finish("sdk_error")
        return value

    def next(self):
        try:
            has_row = self._inner.next()
        except Exception:
            self._finish("transport_error")
            raise
        if not has_row:
            self._finish("response")
        return has_row

    def get_row_data(self):
        return self._inner.get_row_data()

    def __getattr__(self, name):
        return getattr(self._inner, name)


@contextmanager
def captured_sdk_queries(store, *, mode, replay_manifest, evidence_roots, scope, code_version,
                         trade_date, pacer, interval_seconds, max_age_seconds, client=None, query_requests=None):
    """Capture existing BaoStock SDK requests and replay only exact archived ResultSets.

    Daily validation requests additionally persist socket ``recv`` bytes before the SDK
    decodes them. Other supported queries retain their existing decoded-payload contract.
    """
    with _SDK_SESSION_LOCK:
        from datetime import datetime, timezone
        from pathlib import Path
        from types import SimpleNamespace
        import json
        from ...storage.raw import RawObjectStore, sanitized_metadata, _SECRET_BODY

        events, used, selected = [], set(), {}
        last_payload_time = None
        active_capture = None
        query_scope = ({"trade_date": trade_date.isoformat(), "exchange_scope": "SH+SZ A shares"}
                       if trade_date else {"code_name": "ST", "exchange_scope": "SH+SZ"})
        if query_requests and 'query_trade_dates' in query_requests:
            query_scope = {'trade_date': trade_date.isoformat(),
                           'query_scope': 'native calendar and unfiltered universe metadata; selection occurs after retention'}
        archive_records = []
        if mode == "replay":
            manifest = Path(replay_manifest).resolve()
            archive_records = [(number, json.loads(line)) for number, line in
                               enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1)]

        def request_key(name, parameters):
            return json.dumps([name, sanitized_metadata(parameters)], ensure_ascii=False, sort_keys=True, separators=(",", ":"))

        def matches(record, name, parameters):
            return (record.get("event") == "source_payload" and record.get("provider") == "baostock"
                    and record.get("endpoint") == name
                    and (record.get("request_parameters") == parameters if record.get("request_parameters") is not None
                         else trade_date is not None and record.get("metadata", {}).get("trade_date") == trade_date.isoformat())
                    and (name != "query_history_k_data_plus" or
                         (record.get("scope") == query_scope
                          and record.get("original_transport_bytes_available") is True
                          and record.get("tcp_response_sha256"))))

        def lookup(name, parameters):
            if mode == "replay":
                found = next(((number, record) for number, record in archive_records
                              if (manifest.resolve(), number) not in used and matches(record, name, parameters)), None)
                if found is None:
                    return None
                number, record = found
                used.add((manifest.resolve(), number))
                return manifest, number, record, "replay"
            candidates = []
            now = datetime.now(timezone.utc)
            for root in evidence_roots:
                for path in Path(root).rglob("manifest.ndjson") if Path(root).exists() else ():
                    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                        try:
                            record = json.loads(line)
                            if ((path.resolve(), number) in used or not matches(record, name, parameters) or record.get("mode") != "live"
                                    or record.get("scope") != query_scope
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
            used.add((path.resolve(), number))
            return path.resolve(), number, record, "cached"

        requests = query_requests or {"query_all_stock": {"day": trade_date.isoformat()}, "query_stock_industry": {"date": trade_date.isoformat()}}
        request_plan = []
        for name, configured in requests.items():
            calls = configured if isinstance(configured, list) else [configured]
            if not calls or not all(isinstance(parameters, dict) for parameters in calls):
                raise ValueError("each BaoStock SDK request must declare one parameter mapping or an ordered list")
            request_plan.extend((name, parameters) for parameters in calls)
        daily_plan = bool(request_plan) and all(name == "query_history_k_data_plus" for name, _ in request_plan)
        if daily_plan:
            from .daily import BAOSTOCK_DAILY_SOURCE_FIELDS
            required = {"code", "fields", "start_date", "end_date", "frequency", "adjustflag"}
            for _, parameters in request_plan:
                if set(parameters) != required or parameters.get("frequency") != "d":
                    raise ValueError("BaoStock daily capture requires the complete frozen history-query parameter set")
                if parameters.get("fields", "").split(",") != list(BAOSTOCK_DAILY_SOURCE_FIELDS):
                    raise ValueError("BaoStock daily capture requires all supported daily fields in canonical order")
                if parameters.get("adjustflag") not in {"1", "2", "3"}:
                    raise ValueError("BaoStock daily capture requires an explicit supported adjustment flag")
            query_scope = sanitized_metadata(scope)
        elif query_requests is not None:
            day = trade_date.isoformat() if trade_date else None
            allowed = [{'query_stock_basic': {'code_name': 'ST'}},
                {'query_trade_dates': {'start_date': day, 'end_date': day}},
                {'query_trade_dates': {'start_date': day, 'end_date': day},
                 'query_all_stock': {'day': day}, 'query_stock_basic': {}}]
            if query_requests not in allowed or (day is None and query_requests != allowed[0]):
                raise ValueError('SDK query scope is outside verified ST, universe, calendar, or full daily-validation calls')
        # Search evidence before authenticating or issuing an SDK query.
        cached = [lookup(name, parameters) for name, parameters in request_plan]
        needs_live = mode == "live" and any(record is None for record in cached)
        if needs_live and client is None:
            try:
                import baostock as client
            except ImportError as exc:
                raise ProviderContractError("BaoStock dependency is not installed", FailureClass.CONNECTION, retryable=False) from exc
        pacer.configure("baostock.sdk", max(3, interval_seconds), 1)

        def event_record(name, parameters, payload, metadata, *, capture_mode, source_ref=None, transport_event=None):
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
                                      fetched_at=datetime.now(timezone.utc),
                                      attempt_id=f"sdk-payload-{parameters.get('code', 'query')}-{parameters.get('end_date', 'snapshot')}",
                                      body_archive=True)
            outcome = metadata.get("outcome")
            error = outcome.get("error_code", "0") if isinstance(outcome, dict) else "0"
            tcp_hash = None
            tcp_storage = None
            tcp_chunks = []
            if transport_event:
                tcp_hash = transport_event.get("tcp_response_sha256") or transport_event.get("body_sha256")
                tcp_storage = transport_event.get("tcp_response_storage") or transport_event.get("body_storage")
                tcp_chunks = transport_event.get("tcp_response_chunks") or transport_event.get("chunks", [])
            event = {"event": "source_payload", "provider": "baostock", "endpoint": name, "method": name,
                     "request_parameters": parameters, "scope": query_scope, "input_scope": sanitized_metadata(scope),
                     "code_version": code_version, "mode": capture_mode, "source_ref": source_ref,
                     "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "sdk_status_code": str(error),
                     "outcome": "response" if str(error) == "0" else "sdk_error",
                     "metadata": sanitized_metadata(metadata), "body_sha256": saved.content_hash, "payload_bytes": len(payload),
                     "body_storage": saved.path.relative_to(store.root).as_posix(), "byte_exact_archived_representation": True,
                     "original_transport_bytes_available": bool(tcp_hash),
                     "representation": ("SDK-decoded ResultSet fields and rows; exact socket recv byte stream is archived separately"
                                        if name == "query_history_k_data_plus" else
                                        "SDK-decoded ResultSet fields and rows; transport bytes are not exposed by this capture"),
                     **({"tcp_response_sha256": tcp_hash, "tcp_response_storage": tcp_storage,
                         "tcp_response_chunks": tcp_chunks} if tcp_hash else {}),
                     "request_options": {"minimum_query_interval_seconds": max(3, interval_seconds),
                                         "effective_concurrency": 1, "physical_requests_visible": daily_plan,
                                         "tcp_wire_bytes_visible": bool(tcp_hash)}}
            store.append_event(event)
            events.append(event)
            return event

        def record_tcp_chunk(capture, chunk):
            import hashlib
            from datetime import datetime, timezone
            if _SECRET_BODY.search(chunk):
                capture["redacted"] = True
            capture["raw_chunks"].append(chunk)
            capture["byte_count"] += len(chunk)
            digest = hashlib.sha256(chunk).hexdigest()
            item = {"sequence": len(capture["raw_chunks"]), "sha256": digest, "byte_count": len(chunk)}
            if capture["redacted"]:
                item["redacted"] = True
            else:
                reference = store.write_bytes(
                    chunk, dataset="tcp_response_chunk", provider="baostock", endpoint=capture["endpoint"],
                    fetched_at=datetime.now(timezone.utc),
                    attempt_id=f"{capture['label']}-chunk-{len(capture['raw_chunks']):04d}", body_archive=True,
                )
                item["body_storage"] = reference.path.relative_to(store.root).as_posix()
                store.append_event({
                    "event": "tcp_response_chunk", "provider": "baostock", "endpoint": capture["endpoint"],
                    "method": capture["endpoint"], "request_parameters": capture["parameters"],
                    "scope": query_scope, "code_version": code_version, "mode": "live",
                    "sequence": item["sequence"], "received_at_utc": datetime.now(timezone.utc).isoformat(),
                    "body_sha256": reference.content_hash, "body_storage": item["body_storage"],
                    "body_bytes": len(chunk), "representation": "socket recv bytes persisted before BaoStock SDK decoding",
                })
            capture["chunks"].append(item)

        def finish_tcp_capture(capture, *, outcome="response", error_type=None):
            import hashlib
            from datetime import datetime, timezone
            nonlocal active_capture
            if capture["finalized"]:
                return capture.get("event")
            capture["finalized"] = True
            if getattr(capture["socket_context"], "default_socket", None) is capture["wrapper"]:
                capture["socket_context"].default_socket = capture["original_socket"]
            wire = b"".join(capture["raw_chunks"])
            digest = hashlib.sha256(wire).hexdigest()
            reference = None
            redacted = capture["redacted"]
            if wire and not redacted:
                reference = store.write_bytes(
                    wire, dataset="tcp_response", provider="baostock", endpoint=capture["endpoint"],
                    fetched_at=datetime.now(timezone.utc), attempt_id=f"{capture['label']}-wire-response", body_archive=True,
                )
            actual_outcome = "secret_retention_suppressed" if redacted else outcome if wire else "no_response_bytes"
            event = {
                "event": "tcp_response", "provider": "baostock", "endpoint": capture["endpoint"],
                "method": capture["endpoint"], "request_parameters": capture["parameters"],
                "scope": query_scope, "input_scope": sanitized_metadata(scope), "code_version": code_version,
                "mode": "live", "outcome": actual_outcome, "error_type": error_type,
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "body_sha256": digest,
                "body_bytes": len(wire), "chunks": capture["chunks"], "redacted": redacted,
                "body_storage": reference.path.relative_to(store.root).as_posix() if reference else None,
                "original_transport_bytes_available": bool(reference),
                "representation": "ordered socket recv byte stream captured before BaoStock SDK decoding",
            }
            store.append_event(event)
            events.append(event)
            selected[capture["key"]] = event
            capture["event"] = event
            if active_capture is capture:
                active_capture = None
            return event

        def begin_tcp_capture(name, parameters):
            import baostock.common.context as socket_context
            original_socket = getattr(socket_context, "default_socket", None)
            if original_socket is None:
                raise RuntimeError("BaoStock SDK socket is not initialized after login")
            code = str(parameters["code"]).replace(".", "-")
            end_date = str(parameters["end_date"])
            capture = {
                "endpoint": name, "parameters": parameters, "key": request_key(name, parameters),
                "label": f"{code}-{end_date}", "socket_context": socket_context,
                "original_socket": original_socket, "raw_chunks": [], "chunks": [],
                "byte_count": 0, "redacted": False, "finalized": False,
            }
            wrapper = _RecordingSocket(original_socket, lambda chunk: record_tcp_chunk(capture, chunk))
            capture["wrapper"] = wrapper
            socket_context.default_socket = wrapper
            return capture

        request_cursor = 0

        def query(name, **parameters):
            nonlocal request_cursor, active_capture
            if request_cursor >= len(request_plan):
                raise ValueError("SDK issued more requests than the frozen capture plan")
            expected_name, expected_parameters = request_plan[request_cursor]
            if name != expected_name or parameters != expected_parameters:
                raise ValueError("SDK request order or parameters disagree with the frozen capture plan")
            found = cached[request_cursor]
            request_cursor += 1
            key = request_key(name, parameters)
            if found is not None:
                path, number, original, capture_mode = found
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
                selected[key] = event_record(name, parameters, payload, original.get("metadata", {}), capture_mode=capture_mode,
                    source_ref={"manifest": str(path), "line": number, "fetched_at_utc": original.get("fetched_at_utc")},
                    transport_event=original)
                return _decoded_result_set(json.loads(payload))
            if mode == "replay":
                failure = {"event": "sdk_query_failure", "provider": "baostock", "endpoint": name, "method": name,
                           "request_parameters": parameters, "scope": query_scope, "input_scope": sanitized_metadata(scope),
                           "code_version": code_version, "mode": "replay", "outcome": "replay_miss",
                           "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "error_type": "ValueError"}
                store.append_event(failure)
                events.append(failure)
                raise ValueError("no exact archived SDK request match; replay never falls back to network")
            capture = begin_tcp_capture(name, parameters) if daily_plan and name == "query_history_k_data_plus" else None
            active_capture = capture
            try:
                with pacer.request("baostock.sdk"):
                    # The verified probe waited after persisting the first decoded payload,
                    # including time spent exhausting its lazy ResultSet, before query two.
                    if last_payload_time is not None:
                        remaining = max(3, interval_seconds) - (pacer.clock() - last_payload_time)
                        if remaining > 0:
                            pacer.wait(remaining)
                    result = getattr(client, name)(**parameters)
                    if capture is not None:
                        return _CapturedResultSet(result, lambda outcome="response": finish_tcp_capture(capture, outcome=outcome))
                    return result
            except Exception as exc:
                if capture is not None:
                    finish_tcp_capture(capture, outcome="transport_error", error_type=type(exc).__name__)
                failure = {"event": "sdk_query_failure", "provider": "baostock", "endpoint": name, "method": name,
                           "request_parameters": parameters, "scope": query_scope, "code_version": code_version,
                           "mode": "live", "outcome": "transport_error", "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                           "error_type": type(exc).__name__}
                store.append_event(failure)
                events.append(failure)
                raise

        def persist_decoded(payload, *, provider, endpoint, representation, metadata):
            nonlocal last_payload_time
            parameters = metadata.get("request_parameters")
            if parameters is None:
                matches_for_endpoint = [item for name, item in request_plan if name == endpoint]
                if len(matches_for_endpoint) == 1:
                    parameters = matches_for_endpoint[0]
                elif matches_for_endpoint:
                    raise ValueError("SDK-derived payload must identify its exact request parameters")
                else:
                    raise ValueError("SDK-derived payload endpoint is outside the frozen capture plan")
            key = request_key(endpoint, parameters)
            event = selected.get(key)
            if event and event.get("event") == "tcp_response":
                selected[key] = event_record(
                    endpoint, parameters, payload, metadata, capture_mode="live",
                    source_ref={"event": "tcp_response", "body_sha256": event.get("body_sha256"),
                                "body_storage": event.get("body_storage")},
                    transport_event=event,
                )
            elif event:
                reference = store.write_bytes(
                    payload, dataset="sdk_derived", provider=provider, endpoint=endpoint,
                    fetched_at=datetime.now(timezone.utc),
                    attempt_id=f"sdk-derived-{parameters.get('code', 'query')}-{parameters.get('end_date', 'snapshot')}",
                    body_archive=True,
                )
                store.append_event({
                    "event": "sdk_derived", "provider": provider, "endpoint": endpoint, "code_version": code_version,
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(), "row_count": metadata["row_count"],
                    "request_parameters": parameters, "body_storage": reference.path.relative_to(store.root).as_posix(),
                    "body_sha256": reference.content_hash, "source_response_sha256": event["body_sha256"],
                    **({"tcp_response_sha256": event["tcp_response_sha256"]} if event.get("tcp_response_sha256") else {}),
                })
            else:
                if daily_plan:
                    raise ValueError("daily SDK rows cannot be archived without their captured TCP response")
                selected[key] = event_record(endpoint, parameters, payload, metadata, capture_mode="live")
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
                                     **{name: (lambda name=name, **kwargs: query(name, **kwargs))
                                        for name in dict.fromkeys(name for name, _ in request_plan)})
        archive = SimpleNamespace(store_source_payload=persist_decoded, captures_tcp_wire=daily_plan)
        try:
            yield sdk_client, archive, events
        finally:
            if active_capture is not None:
                finish_tcp_capture(active_capture, outcome="capture_context_closed_before_result_exhaustion")
