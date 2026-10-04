from __future__ import annotations

import hashlib
import json
import os
import re
import gzip
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from datetime import timezone


_SECRET_NAME = re.compile(r"token|secret|password|api[-_]?key|authorization|cookie|credential|signature|enckey|^sign$|^ut$|^hexin-v$", re.I)
_SECRET_BODY = re.compile(rb'''(?i)["']?(?:access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|authorization|password|session[_-]?token)["']?\s*[:=]\s*["']''')


def sanitized_url(url: str) -> str:
    parts = urlsplit(url)
    query = [(key, "<redacted>" if _SECRET_NAME.search(key) else value) for key, value in parse_qsl(parts.query, keep_blank_values=True)]
    host = parts.hostname or ""
    if parts.port:
        host += f":{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, urlencode(sorted(query)), ""))


def sanitized_headers(headers) -> dict[str, str]:
    return {str(k): "<redacted>" if _SECRET_NAME.search(str(k)) else
            sanitized_url(str(v)) if str(k).lower() in {"referer", "location"} else str(v) for k, v in headers.items()}


def sanitized_metadata(value):
    if isinstance(value, dict):
        return {str(k): "<redacted>" if _SECRET_NAME.search(str(k)) else sanitized_metadata(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [sanitized_metadata(v) for v in value]
    return sanitized_url(value) if isinstance(value, str) and value.startswith(("http://", "https://")) else value


@dataclass(frozen=True, slots=True)
class RawObjectRef:
    path: Path
    content_hash: str
    size_bytes: int


class RawObjectStore:
    """Immutable, content-verifiable storage for provider responses."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write_json(
        self,
        payload: Any,
        *,
        dataset: str,
        provider: str,
        endpoint: str,
        fetched_at: datetime,
        attempt_id: str,
    ) -> RawObjectRef:
        content = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return self.write_bytes(content, dataset=dataset, provider=provider, endpoint=endpoint,
                                fetched_at=fetched_at, attempt_id=attempt_id, suffix="json")

    def write_bytes(self, content: bytes, *, dataset: str, provider: str, endpoint: str,
                    fetched_at: datetime, attempt_id: str, suffix: str = "bin", content_addressed: bool = False) -> RawObjectRef:
        for component in (dataset, provider, endpoint, attempt_id, suffix):
            if not component or component in {".", ".."} or any(c in component for c in ("/", "\\", ":")):
                raise ValueError("invalid raw object path component")
        relative = Path("bodies", hashlib.sha256(content).hexdigest() + ".bin") if content_addressed else Path(
            fetched_at.date().isoformat(), dataset, provider, endpoint, f"{attempt_id}.{suffix}"
        )
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        expected_hash = hashlib.sha256(content).hexdigest()
        if target.exists():
            existing_hash = hashlib.sha256(target.read_bytes()).hexdigest()
            if existing_hash != expected_hash:
                raise FileExistsError(f"immutable raw object already exists with different content: {target}")
            return RawObjectRef(target, existing_hash, target.stat().st_size)

        temporary = target.with_name(f"{target.name}.{os.getpid()}.tmp")
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        return RawObjectRef(target, expected_hash, len(content))

    def append_event(self, event: dict[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / "manifest.ndjson").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def record_response(self, *, response, url, method, request_headers, scope, provider, endpoint,
                        code_version, mode="live", source_ref=None, fetched_at=None, request_options=None):
        """Persist application response bytes before the SDK/provider parser runs."""
        fetched_at = fetched_at or datetime.now(timezone.utc)
        body = bytes(response.content)
        digest = hashlib.sha256(body).hexdigest()
        redacted = bool(_SECRET_BODY.search(body))
        reference = None if redacted else self.write_bytes(
            body, dataset="input_response", provider=provider, endpoint=endpoint,
            fetched_at=fetched_at, attempt_id=digest, content_addressed=True,
        )
        event = {
            "event": "http_response", "method": method, "url": sanitized_url(url),
            "request_headers": sanitized_headers(request_headers), "scope": sanitized_metadata(scope),
            "request_options": sanitized_metadata(request_options or {}),
            "provider": provider, "endpoint": endpoint, "code_version": code_version,
            "fetched_at_utc": fetched_at.isoformat(), "mode": mode, "source_ref": source_ref,
            "outcome": "response", "status_code": int(response.status_code),
            "response_headers": sanitized_headers(response.headers),
            "content_type": response.headers.get("Content-Type", response.headers.get("content-type")),
            "encoding": response.encoding, "body_bytes": len(body), "body_sha256": digest,
            "body_storage": reference.path.relative_to(self.root).as_posix() if reference else None,
            "persistence_status": "suppressed_sensitive_content" if redacted else "exact_application_payload_saved",
        }
        self.append_event(event)
        if redacted:
            raise ValueError("sensitive response body retention suppressed; parsing is blocked")
        return event

    @staticmethod
    def read_response(manifest: str | Path, event: dict[str, Any]) -> bytes:
        root = Path(manifest).resolve().parent
        path = (root / event["body_storage"]).resolve()
        if not path.is_relative_to(root):
            raise ValueError("response reference escapes archive")
        body = gzip.decompress(path.read_bytes()) if path.name.endswith(".gz") else path.read_bytes()
        if hashlib.sha256(body).hexdigest() != event["body_sha256"]:
            raise ValueError("archived response hash mismatch")
        return body

    @staticmethod
    def find_cached_response(roots, *, url, method, scope, code_version, max_age_seconds, ignored_query_parameters=()):
        now = datetime.now(timezone.utc)
        def cache_url(value):
            cleaned = sanitized_url(value)
            if not ignored_query_parameters:
                return cleaned
            parts = urlsplit(cleaned)
            query = [(key, item) for key, item in parse_qsl(parts.query, keep_blank_values=True) if key not in ignored_query_parameters]
            return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))
        matches = []
        for root in roots:
            for manifest in Path(root).rglob("manifest.ndjson") if Path(root).exists() else ():
                for line in manifest.read_text(encoding="utf-8").splitlines():
                    try:
                        event = json.loads(line)
                        stamp = datetime.fromisoformat(event["fetched_at_utc"])
                        age = (now - stamp).total_seconds()
                        if (event.get("mode") == "live" and event.get("code_version") == code_version
                            and event.get("scope") == scope and event.get("method") == method
                            and cache_url(event["url"]) == cache_url(url)
                            and event.get("status_code") == 200 and event.get("body_storage")
                            and event.get("body_bytes", 0) > 0 and 0 <= age <= max_age_seconds):
                            matches.append((stamp, manifest, event))
                    except (KeyError, TypeError, ValueError):
                        continue
        for _, manifest, event in sorted(matches, key=lambda v: v[0], reverse=True):
            return manifest, event, RawObjectStore.read_response(manifest, event)
        return None

    @staticmethod
    def verify(reference: RawObjectRef) -> bool:
        if not reference.path.is_file() or reference.path.stat().st_size != reference.size_bytes:
            return False
        return hashlib.sha256(reference.path.read_bytes()).hexdigest() == reference.content_hash
