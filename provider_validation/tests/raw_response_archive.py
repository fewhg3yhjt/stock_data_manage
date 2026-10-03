"""Persist HTTP response bodies before provider parsers inspect them.

The archive is append-only. Each body is stored once by SHA-256 as gzip-compressed
application response bytes; manifest.ndjson records every request/response event.
No authorization headers, cookies, or secret-like query values are persisted.

Usage:
    with RawResponseArchive("2026-10-audit-v1") as archive:
        with archive.scope(capability="tencent_quote", request_scope="600519"):
            result = tencent_quote("600519")

This captures requests-based HTTP calls. Other transports (urllib, binary/TCP
clients, SDKs) must persist their raw source payload at the transport boundary.
"""

from __future__ import annotations

import contextlib
import contextvars
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import threading
from pathlib import Path
from typing import Any, Iterator, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_SECRET_NAME = re.compile(r"(token|secret|password|passwd|api[-_]?key|authorization|cookie|credential|signature|sign)", re.I)
_SECRET_VALUE = re.compile(
    rb"(?i)([\"']?(?:access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|"
    rb"authorization|password|passwd|session[_-]?token)[\"']?\s*[:=]\s*[\"'])[^\"']+"
)
_SECRET_TEXT = re.compile(
    r"(?i)((?:access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|"
    r"authorization|password|passwd|session[_-]?token)[=:\s]+)([^&\s,;]+)"
)
_THREAD_SCOPE: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "provider_probe_scope", default={}
)


def _safe_url(url: str) -> str:
    parts = urlsplit(url)
    query = [
        (key, "<redacted>" if _SECRET_NAME.search(key) else value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
    ]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _safe_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers.items():
        if _SECRET_NAME.search(str(key)):
            result[str(key)] = "<redacted>"
        elif str(key).lower() in {"location", "referer"}:
            result[str(key)] = _safe_url(str(value))
        else:
            result[str(key)] = str(value)
    return result


class RawResponseArchive:
    """Append-only archive for live HTTP responses made with ``requests``."""

    def __init__(self, run_id: str, root: str | Path | None = None) -> None:
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,80}", run_id):
            raise ValueError("run_id may contain only letters, digits, dot, underscore, and hyphen")
        base = (
            Path(root)
            if root is not None
            else Path(__file__).resolve().parents[1] / "results" / "raw"
        )
        self.directory = base / run_id
        self.body_directory = self.directory / "bodies"
        self.manifest_path = self.directory / "manifest.ndjson"
        self._lock = threading.Lock()
        self._original_send: Any = None
        self._send_wrapper: Any = None

    def __enter__(self) -> "RawResponseArchive":
        import requests

        self.body_directory.mkdir(parents=True, exist_ok=True)
        self._original_send = requests.Session.send

        def send_with_archive(session: Any, request: Any, **kwargs: Any) -> Any:
            started_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")
            request_record = {
                "event": "http_response",
                "fetched_at_utc": started_at,
                "scope": dict(_THREAD_SCOPE.get()),
                "method": str(getattr(request, "method", "")),
                "url": _safe_url(str(getattr(request, "url", ""))),
                "request_headers": _safe_headers(getattr(request, "headers", {})),
                "request_body_bytes": len(getattr(request, "body", b"") or b""),
            }
            request_body = getattr(request, "body", b"") or b""
            if isinstance(request_body, str):
                request_body = request_body.encode("utf-8", "replace")
            request_record["request_body_sha256"] = hashlib.sha256(request_body).hexdigest()
            try:
                response = self._original_send(session, request, **kwargs)
            except Exception as exc:
                self._append(
                    {
                        **request_record,
                        "outcome": "transport_error",
                        "error_type": type(exc).__name__,
                        "error": _SECRET_TEXT.sub(r"\1<redacted>", str(exc))[:1000],
                    }
                )
                raise

            # Accessing .content consumes a streamed response once and caches it
            # on requests.Response, so the caller receives the same body bytes.
            body = bytes(response.content)
            digest = hashlib.sha256(body).hexdigest()
            sensitive_body = _SECRET_VALUE.search(body) is not None
            body_path = None if sensitive_body else self.body_directory / f"{digest}.body.gz"
            if body_path is not None and not body_path.exists():
                temporary = body_path.with_name(f"{digest}.{threading.get_ident()}.tmp")
                with temporary.open("wb") as stream:
                    stream.write(gzip.compress(body, compresslevel=6, mtime=0))
                    stream.flush()
                    os.fsync(stream.fileno())
                if body_path.exists():
                    temporary.unlink(missing_ok=True)
                else:
                    temporary.replace(body_path)
            self._append(
                {
                    **request_record,
                    "outcome": "response",
                    "status_code": int(response.status_code),
                    "response_headers": _safe_headers(response.headers),
                    "content_type": response.headers.get("Content-Type"),
                    "response_encoding": getattr(response, "encoding", None),
                    "requests_decoded_body_bytes": len(body),
                    "body_sha256": digest,
                    "body_storage": (
                        str(body_path.relative_to(self.directory)) if body_path is not None else None
                    ),
                    "persistence_status": (
                        "suppressed_sensitive_content" if sensitive_body else "exact_application_body_saved"
                    ),
                }
            )
            return response

        self._send_wrapper = send_with_archive
        requests.Session.send = send_with_archive
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        import requests

        if self._original_send is not None:
            requests.Session.send = self._original_send

    @contextlib.contextmanager
    def scope(self, **metadata: str) -> Iterator[None]:
        current = dict(_THREAD_SCOPE.get())
        current.update({key: str(value) for key, value in metadata.items()})
        token = _THREAD_SCOPE.set(current)
        try:
            yield
        finally:
            _THREAD_SCOPE.reset(token)

    def store_source_payload(
        self,
        payload: bytes,
        *,
        provider: str,
        endpoint: str,
        representation: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        """Persist bytes returned by a non-HTTP SDK before domain transforms.

        Returns the SHA-256 of the uncompressed payload. For SDK/TCP providers,
        callers must describe whether these are wire bytes or SDK-decoded rows.
        """
        body = bytes(payload)
        digest = hashlib.sha256(body).hexdigest()
        if _SECRET_VALUE.search(body):
            self._append(
                {
                    "event": "source_payload",
                    "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
                    "scope": dict(_THREAD_SCOPE.get()),
                    "provider": provider,
                    "endpoint": endpoint,
                    "representation": representation,
                    "metadata": dict(metadata or {}),
                    "payload_bytes": len(body),
                    "body_sha256": digest,
                    "body_storage": None,
                    "persistence_status": "suppressed_sensitive_content",
                }
            )
            return digest
        body_path = self.body_directory / f"{digest}.body.gz"
        if not body_path.exists():
            temporary = body_path.with_suffix(body_path.suffix + ".tmp")
            with temporary.open("wb") as stream:
                stream.write(gzip.compress(body, compresslevel=6, mtime=0))
                stream.flush()
                os.fsync(stream.fileno())
            try:
                temporary.replace(body_path)
            except FileExistsError:
                temporary.unlink(missing_ok=True)
        self._append(
            {
                "event": "source_payload",
                "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
                "scope": dict(_THREAD_SCOPE.get()),
                "provider": provider,
                "endpoint": endpoint,
                "representation": representation,
                "metadata": dict(metadata or {}),
                "payload_bytes": len(body),
                "body_sha256": digest,
                "body_storage": str(body_path.relative_to(self.directory)),
                "persistence_status": "exact_application_payload_saved",
            }
        )
        return digest

    def find_cached_http_response(
        self,
        *,
        method: str,
        url: str,
        request_body: bytes = b"",
        scope: Mapping[str, str],
        max_age_seconds: int,
    ) -> dict[str, Any] | None:
        """Find a fresh, exact-scope archived response without making a request.

        A cache hit is returned only when method, redacted canonical URL, body
        hash, scope, successful status, retained body, and age all match. Include
        capability and code version in ``scope``. The caller remains responsible
        for confirming semantic completeness before reusing it.
        """
        if max_age_seconds < 0:
            raise ValueError("max_age_seconds must be nonnegative")
        root = self.directory.parent
        target_url = _safe_url(url)
        target_body_hash = hashlib.sha256(request_body).hexdigest()
        now = dt.datetime.now(dt.timezone.utc)
        manifests = sorted(
            root.glob("*/manifest.ndjson"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for manifest in manifests:
            archive_directory = manifest.parent
            try:
                lines = manifest.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in reversed(lines):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    record.get("event") != "http_response"
                    or record.get("outcome") != "response"
                    or int(record.get("status_code", 0)) < 200
                    or int(record.get("status_code", 0)) >= 300
                    or record.get("persistence_status") != "exact_application_body_saved"
                    or record.get("method", "").upper() != method.upper()
                    or record.get("url") != target_url
                    or record.get("request_body_sha256") != target_body_hash
                    or record.get("scope") != dict(scope)
                ):
                    continue
                fetched_at = dt.datetime.fromisoformat(record["fetched_at_utc"])
                age = (now - fetched_at.astimezone(dt.timezone.utc)).total_seconds()
                if age < 0 or age > max_age_seconds:
                    continue
                body_path = archive_directory / record["body_storage"]
                try:
                    payload = gzip.decompress(body_path.read_bytes())
                except OSError:
                    continue
                if hashlib.sha256(payload).hexdigest() != record.get("body_sha256"):
                    continue
                return {"payload": payload, "manifest": record, "age_seconds": age}
        return None

    def _append(self, item: Mapping[str, Any]) -> None:
        record = json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n"
        with self._lock, self.manifest_path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(record)
            stream.flush()
            os.fsync(stream.fileno())
