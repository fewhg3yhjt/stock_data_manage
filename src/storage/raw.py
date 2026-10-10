from __future__ import annotations

import hashlib
import json
import os
import re
import gzip
from dataclasses import dataclass
from datetime import date, datetime
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

    def __init__(self, root: str | Path, *, manifest_name: str = "manifest.ndjson", capture_root: str | Path | None = None,
                 capture_archive_root: str | Path | None = None) -> None:
        self.root = Path(root)
        if not re.fullmatch(r"manifest(?:\.[A-Za-z0-9_-]+)?\.ndjson", manifest_name):
            raise ValueError("invalid request manifest name")
        self.manifest = self.root / manifest_name
        self.capture_root = Path(capture_root) if capture_root else None
        self.capture_archive_root = Path(capture_archive_root) if capture_archive_root else None
        self.capture_manifests = set()

    @staticmethod
    def task_unit_path(raw_root: str | Path, *, provider: str, endpoint: str, data_date: str) -> Path:
        if date.fromisoformat(data_date).isoformat() != data_date:
            raise ValueError("data date must use YYYY-MM-DD")
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in (provider, endpoint)):
            raise ValueError("unsafe raw storage path component")
        return Path(raw_root).resolve() / "_tmp" / data_date / provider / endpoint

    @staticmethod
    def current_path(raw_root: str | Path, provider: str, endpoint: str, *, data_date: str, parameters) -> Path:
        """One active response set per source, data day and canonical request scope."""
        if date.fromisoformat(data_date).isoformat() != data_date:
            raise ValueError("data date must use YYYY-MM-DD")
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in (provider, endpoint)):
            raise ValueError("unsafe raw storage path component")
        return Path(raw_root).resolve() / data_date / provider / endpoint

    @staticmethod
    def request_manifest_name(parameters, *, input_id: str) -> str:
        """Different requests share an interface directory, never an active manifest."""
        from .integrity import row_hash
        label = re.sub(r"[^A-Za-z0-9_-]", "_", str(parameters.get("symbol") or parameters.get("code") or "all"))[:20]
        return f"manifest.{label}-{row_hash([input_id, sanitized_metadata(parameters)])[:12]}.ndjson"

    @staticmethod
    def copy_request(manifest: str | Path, destination: str | Path, *, verify: bool = True) -> Path:
        """Copy one response set and its exact bytes, without including adjacent requests."""
        import shutil
        source, target = Path(manifest), Path(destination)
        if verify:
            RawObjectStore.verify_manifest(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        for event in map(json.loads, source.read_text(encoding="utf-8").splitlines()):
            if event.get("body_storage"):
                body = (source.parent / event["body_storage"]).resolve()
                if not body.is_relative_to(source.parent.resolve()):
                    raise ValueError("response reference escapes archive")
                output = target.parent / event["body_storage"]
                if not output.resolve().is_relative_to(target.parent.resolve()):
                    raise ValueError("response output escapes archive")
                if not verify and not body.exists():
                    # Retain the manifest and integrity failure even when the
                    # lost bytes cannot be reconstructed; the request must refetch.
                    continue
                output.parent.mkdir(parents=True, exist_ok=True)
                if not output.exists():
                    shutil.copyfile(body, output)
                elif output.read_bytes() != body.read_bytes():
                    raise ValueError("existing response bytes differ")
        temporary = target.with_suffix(".tmp")
        shutil.copyfile(source, temporary)
        os.replace(temporary, target)
        return target

    @staticmethod
    def remove_request(manifest: str | Path) -> None:
        """Remove only this active request; keep bodies referenced by its neighbours."""
        path = Path(manifest)
        if not path.exists():
            return
        events = list(map(json.loads, path.read_text(encoding="utf-8").splitlines()))
        used = set()
        for other in path.parent.glob("manifest.*.ndjson"):
            if other != path:
                used.update(event["body_storage"] for event in map(json.loads, other.read_text(encoding="utf-8").splitlines())
                            if event.get("body_storage"))
        path.unlink()
        label = path.name.removeprefix("manifest.").removesuffix(".ndjson")
        (path.parent / ("_managed_task." + label + ".json")).unlink(missing_ok=True)
        for relative in {event["body_storage"] for event in events if event.get("body_storage")} - used:
            body = (path.parent / relative).resolve()
            if not body.is_relative_to(path.parent.resolve()):
                raise ValueError("response reference escapes archive")
            body.unlink(missing_ok=True)
        RawObjectStore.refresh_index(path, removed=True)

    @staticmethod
    def refresh_index(manifest: str | Path, *, removed: bool = False) -> None:
        path = Path(manifest)
        if path.name == "manifest.ndjson":
            return
        index = path.parent / "manifest.ndjson"
        records = {row["request_manifest"]: row for row in map(json.loads, index.read_text(encoding="utf-8").splitlines())} if index.exists() else {}
        if removed:
            records.pop(path.name, None)
        else:
            records[path.name] = {"event": "request_manifest", "request_manifest": path.name,
                                  "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        temporary = index.with_suffix(".tmp")
        if records:
            temporary.write_text("".join(json.dumps(records[key]) + "\n" for key in sorted(records)), encoding="utf-8")
            os.replace(temporary, index)
        else:
            index.unlink(missing_ok=True)
            bodies = path.parent / "bodies"
            if bodies.is_dir() and not any(bodies.iterdir()):
                bodies.rmdir()
            if path.parent.exists() and not any(path.parent.iterdir()):
                path.parent.rmdir()

    @staticmethod
    def preserve_request(manifest: str | Path, *, permitted_root: str | Path, archive_root: str | Path) -> Path | None:
        from uuid import uuid4
        path, permitted = Path(manifest).resolve(), Path(permitted_root).resolve()
        if not path.is_relative_to(permitted) or path == permitted:
            raise ValueError("cleanup path escapes its permitted scope")
        if not path.exists():
            return None
        target = Path(archive_root).resolve() / "_raw_audit" / uuid4().hex[:16] / "manifest.ndjson"
        try:
            RawObjectStore.verify_manifest(path)
            integrity_error = None
        except (ValueError, OSError) as exc:
            integrity_error = str(exc)
        RawObjectStore.copy_request(path, target, verify=False)
        if integrity_error:
            (target.parent / "integrity_failure.json").write_text(json.dumps({"status": "corrupt_evidence",
                "error": integrity_error, "source_manifest": str(path)}, ensure_ascii=False), encoding="utf-8")
        RawObjectStore.remove_request(path)
        return target

    @staticmethod
    def preserve_work_files(work: str | Path, *, archive_root: str | Path) -> Path | None:
        """Keep prior build conclusions without moving still-valid source candidates."""
        import shutil
        from uuid import uuid4
        work = Path(work)
        selected = [work / name for name in ("prepared.parquet", "coverage.json", "summary.json", "task.json")
                    if (work / name).exists()]
        if selected:
            target = Path(archive_root) / "_workspace_audit" / uuid4().hex[:12]
            target.mkdir(parents=True)
            for path in selected:
                shutil.move(str(path), target / path.name)
            return target
        return None

    @staticmethod
    def verify_manifest(manifest: str | Path) -> str:
        path = Path(manifest)
        events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        if not events:
            raise ValueError("empty raw manifest")
        for event in events:
            if event.get("request_manifest"):
                child = (path.parent / event["request_manifest"]).resolve()
                if child.parent != path.parent.resolve() or RawObjectStore.verify_manifest(child) != event["sha256"]:
                    raise ValueError("request index differs from response manifest")
            if event.get("body_storage"):
                RawObjectStore.read_response(path, event)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    @staticmethod
    def preserve_directory(directory: str | Path, *, permitted_root: str | Path,
                           archive_root: str | Path) -> Path | None:
        """Remove a managed working directory from active use, retaining exact evidence."""
        from uuid import uuid4
        directory, permitted = Path(directory).resolve(), Path(permitted_root).resolve()
        archive = Path(archive_root).resolve()
        if directory == permitted or not directory.is_relative_to(permitted):
            raise ValueError("cleanup path escapes its permitted scope")
        if directory.is_relative_to(archive) or archive.is_relative_to(directory):
            raise ValueError("evidence archive overlaps cleanup scope")
        if not directory.exists():
            return None
        destination = archive / "_raw_audit" / uuid4().hex[:16]
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.rename(directory, destination)
        return destination

    @staticmethod
    def promote_unit(*, temporary: str | Path, current: str | Path, raw_root: str | Path,
                     archive_root: str | Path, expected_hash: str) -> None:
        """Idempotent finalization; current data is a single managed source result."""
        temporary, current, root = Path(temporary).resolve(), Path(current).resolve(), Path(raw_root).resolve()
        if not temporary.is_relative_to(root / "_tmp") or temporary == root / "_tmp":
            raise ValueError("raw promotion requires a scoped _tmp directory")
        if not current.is_relative_to(root) or current == root or current.is_relative_to(root / "_tmp"):
            raise ValueError("invalid current raw destination")
        temporary_parts = temporary.relative_to(root / "_tmp").parts
        current_parts = current.relative_to(root).parts
        if (len(temporary_parts) != 4 or len(current_parts) != 4
                or temporary_parts != current_parts
                or date.fromisoformat(temporary_parts[0]).isoformat() != temporary_parts[0]):
            raise ValueError("raw promotion requires matching data-date partitions")
        if not temporary.exists():
            if RawObjectStore.verify_manifest(current) != expected_hash:
                raise ValueError("completed raw promotion does not match commit")
            return
        if RawObjectStore.verify_manifest(temporary) != expected_hash:
            raise ValueError("temporary raw data changed after validation")
        current.parent.mkdir(parents=True, exist_ok=True)
        from .parquet import PartitionLock
        with PartitionLock(current.parent / ".raw.lock", recover_stale=True):
            if current.exists():
                if not (current.parent / "manifest.ndjson").is_file():
                    raise ValueError("refusing to replace unmanaged existing raw data")
                RawObjectStore.preserve_request(current, permitted_root=root, archive_root=archive_root)
            RawObjectStore.copy_request(temporary, current)
            RawObjectStore.refresh_index(current)
            RawObjectStore.remove_request(temporary)

    def write_json(
        self,
        payload: Any,
        *,
        dataset: str,
        provider: str,
        endpoint: str,
        fetched_at: datetime,
        attempt_id: str,
        relative_path: str | Path | None = None,
    ) -> RawObjectRef:
        content = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return self.write_bytes(content, dataset=dataset, provider=provider, endpoint=endpoint,
                                fetched_at=fetched_at, attempt_id=attempt_id, suffix="json", relative_path=relative_path)

    def write_bytes(self, content: bytes, *, dataset: str, provider: str, endpoint: str,
                    fetched_at: datetime, attempt_id: str, suffix: str = "bin", content_addressed: bool = False,
                    relative_path: str | Path | None = None) -> RawObjectRef:
        for component in (dataset, provider, endpoint, attempt_id, suffix):
            if not component or component in {".", ".."} or any(c in component for c in ("/", "\\", ":")):
                raise ValueError("invalid raw object path component")
        relative = Path("bodies", hashlib.sha256(content).hexdigest() + ".bin") if content_addressed else Path(
            fetched_at.date().isoformat(), dataset, provider, endpoint, f"{attempt_id}.{suffix}"
        )
        if relative_path is not None:
            if content_addressed:
                raise ValueError("content-addressed bodies must retain their hash path")
            relative = Path(relative_path)
            if relative.is_absolute() or ".." in relative.parts or not relative.parts:
                raise ValueError("artifact path must stay within its store")
            if not (self.root / relative).resolve().is_relative_to(self.root.resolve()):
                raise ValueError("artifact path escapes its store")
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        expected_hash = hashlib.sha256(content).hexdigest()
        if target.exists():
            existing_hash = hashlib.sha256(target.read_bytes()).hexdigest()
            if existing_hash != expected_hash:
                raise FileExistsError(f"immutable raw object already exists with different content: {target}")
            if content_addressed and self.manifest.name != "manifest.ndjson":
                self.append_event({"event": "stored_artifact", "body_storage": relative.as_posix(), "body_sha256": existing_hash})
            return RawObjectRef(target, existing_hash, target.stat().st_size)

        temporary = target.with_name(f"{target.name}.{os.getpid()}.tmp")
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        if content_addressed and self.manifest.name != "manifest.ndjson":
            self.append_event({"event": "stored_artifact", "body_storage": relative.as_posix(), "body_sha256": expected_hash})
        return RawObjectRef(target, expected_hash, len(content))

    def append_event(self, event: dict[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with self.manifest.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def record_response(self, *, response, url, method, request_headers, scope, provider, endpoint,
                        code_version, mode="live", source_ref=None, fetched_at=None, request_options=None):
        """Persist application response bytes before the SDK/provider parser runs."""
        fetched_at = fetched_at or datetime.now(timezone.utc)
        if self.capture_root is not None:
            from zoneinfo import ZoneInfo
            stamp = datetime.fromisoformat(source_ref["fetched_at_utc"]) if source_ref and source_ref.get("fetched_at_utc") else fetched_at
            if stamp.tzinfo is None:
                raise ValueError("original source capture time must be timezone-aware")
            actual = stamp.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
            destination = self.task_unit_path(self.capture_root, provider=provider, endpoint=endpoint, data_date=actual)
            next_manifest = destination / self.manifest.name
            if next_manifest != self.manifest and next_manifest not in self.capture_manifests and next_manifest.exists():
                if self.capture_archive_root is None:
                    raise ValueError("existing captured request must be archived before replacing its response set")
                self.preserve_request(next_manifest, permitted_root=self.capture_root / "_tmp", archive_root=self.capture_archive_root)
            self.root, self.manifest = destination, next_manifest
            self.capture_manifests.add(self.manifest)
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
            for manifest in Path(root).rglob("manifest*.ndjson") if Path(root).exists() else ():
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
