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
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from datetime import timezone
from zoneinfo import ZoneInfo


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


def _safe_filename_part(value: object, *, limit: int = 32) -> str:
    return re.sub(r"[^A-Za-z0-9-]+", "-", str(value)).strip("-")[:limit] or "unknown"


def _local_capture_stamp(fetched_at: datetime) -> tuple[str, str]:
    local = fetched_at.astimezone(ZoneInfo("Asia/Shanghai"))
    return local.date().isoformat(), local.strftime("%H%M%S%f")


def response_body_path(*, scope, fetched_at: datetime) -> Path:
    """Build a readable, timestamped path for an HTTP body without a digest in its name."""
    scope = scope if isinstance(scope, dict) else {}
    parameters = scope.get("parameters") if isinstance(scope.get("parameters"), dict) else scope
    stamp, clock = _local_capture_stamp(fetched_at)
    label = [stamp]
    if scope.get("input_id"):
        label.append(f"i-{_safe_filename_part(scope['input_id'], limit=18)}")
    symbols = parameters.get("symbols")
    symbol = parameters.get("symbol") or parameters.get("code")
    if symbol:
        label.append(f"s-{_safe_filename_part(symbol, limit=12)}")
    elif isinstance(symbols, (list, tuple)) and symbols:
        label.append(f"s-{len(symbols)}-{_safe_filename_part(symbols[0], limit=8)}-{_safe_filename_part(symbols[-1], limit=8)}")
    elif symbols:
        label.append(f"s-{_safe_filename_part(symbols, limit=12)}")
    trade_date = parameters.get("trade_date") or parameters.get("day")
    start_date, end_date = parameters.get("start_date"), parameters.get("end_date")
    if trade_date:
        label.append(f"d-{_safe_filename_part(trade_date, limit=10)}")
    elif start_date or end_date:
        label.append(f"w-{_safe_filename_part(start_date or 'start', limit=10)}-{_safe_filename_part(end_date or 'end', limit=10)}")
    adjustment = parameters.get("adjustment") or parameters.get("adjust")
    if adjustment:
        label.append(f"a-{_safe_filename_part(adjustment, limit=8)}")
    if len(label) == 1:
        label.append(_safe_filename_part(scope.get("input_id", "all"), limit=18))
    label.append(clock[:6])
    return Path("bodies", "_".join(label) + ".bin")


def _archived_body_path(*, dataset: str, provider: str, endpoint: str, attempt_id: str,
                        fetched_at: datetime, suffix: str) -> Path:
    stamp, clock = _local_capture_stamp(fetched_at)
    label = (stamp, provider, endpoint, dataset, attempt_id, clock)
    return Path("bodies", "_".join(_safe_filename_part(part) for part in label) + f".{suffix}")


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
        if not re.fullmatch(r"manifest(?:\.[A-Za-z0-9_.%~-]+)?\.ndjson", manifest_name):
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
        parameters = sanitized_metadata(parameters)
        # Daily quote groups multiply source requests by three. Keep their persisted,
        # scope-readable names short enough for Windows archive/workspace paths.
        if (parameters.get("code") and parameters.get("adjust") in {"none", "qfq", "hfq"}
                and parameters.get("start") and parameters.get("end") and parameters.get("api")):
            code = _safe_filename_part(parameters["code"], limit=12)
            start = _safe_filename_part(parameters["start"], limit=10)
            end = _safe_filename_part(parameters["end"], limit=10)
            return f"manifest.{_safe_filename_part(input_id, limit=24)}_{code}_{parameters['adjust']}_{start}_{end}.ndjson"
        parts = [quote(str(input_id), safe="-_.")]
        for key, value in sorted(parameters.items()):
            if isinstance(value, (list, tuple)):
                first = _safe_filename_part(value[0], limit=12) if value else "empty"
                last = _safe_filename_part(value[-1], limit=12) if value else "empty"
                encoded = f"{len(value)}-items-{first}-to-{last}"
            else:
                plain = value if isinstance(value, str) else json.dumps(value, ensure_ascii=True, sort_keys=True,
                                                                         separators=(",", ":"), default=str)
                encoded = quote(plain, safe="-_.")
                if len(encoded) > 80:
                    encoded = f"{encoded[:48]}~{encoded[-20:]}~len{len(encoded)}"
            parts.append(f"{quote(str(key), safe='-_.')}-{encoded}")
        label = "_".join(parts)
        if len(label) > 220:
            raise ValueError("request scope is too long for a readable manifest filename")
        return f"manifest.{label}.ndjson"

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
        path, permitted = Path(manifest).resolve(), Path(permitted_root).resolve()
        if not path.is_relative_to(permitted) or path == permitted:
            raise ValueError("cleanup path escapes its permitted scope")
        if not path.exists():
            return None
        relative = path.relative_to(permitted)
        parts = relative.parts
        if (len(parts) == 4 and parts[2] == "kline_daily" and path.stem.startswith("manifest.ASTOCK-002-daily_")):
            # Daily redo archives need a compact but business-readable path on Windows.
            request_parts = path.stem.removeprefix("manifest.").split("_")
            if (len(request_parts) == 5 and request_parts[2] in {"none", "qfq", "hfq"}
                    and all(request_parts[index] for index in (1, 2, 3, 4))):
                date_key, provider, endpoint = parts[0], parts[1], parts[2]
                label = _safe_filename_part("_".join((date_key, provider, endpoint, request_parts[1],
                    request_parts[2], request_parts[3], request_parts[4])), limit=80)
            else:
                label = _safe_filename_part("_".join(parts[:-1]) + "_" + path.stem, limit=80)
        else:
            label = "_".join(_safe_filename_part(part, limit=48) for part in (*parts[:-1], path.stem))
        directory = Path(archive_root).resolve() / "_raw_audit" / label
        suffix = 2
        base = directory
        while directory.exists():
            directory = base.with_name(f"{base.name}_{suffix}")
            suffix += 1
        target = directory / "manifest.ndjson"
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
        work = Path(work)
        selected = [work / name for name in ("prepared.parquet", "coverage.json", "summary.json", "task.json", "universe_review.json")
                    if (work / name).exists()]
        if selected:
            target = Path(archive_root) / "_workspace_audit" / _safe_filename_part(
                f"{work.parent.name}_{work.name}", limit=96)
            suffix = 2
            base = target
            while target.exists():
                target = base.with_name(f"{base.name}_{suffix}")
                suffix += 1
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
        directory, permitted = Path(directory).resolve(), Path(permitted_root).resolve()
        archive = Path(archive_root).resolve()
        if directory == permitted or not directory.is_relative_to(permitted):
            raise ValueError("cleanup path escapes its permitted scope")
        if directory.is_relative_to(archive) or archive.is_relative_to(directory):
            raise ValueError("evidence archive overlaps cleanup scope")
        if not directory.exists():
            return None
        relative = directory.relative_to(permitted)
        label = _safe_filename_part("_".join(relative.parts), limit=120)
        destination = archive / "_raw_audit" / label
        suffix = 2
        base = destination
        while destination.exists():
            destination = base.with_name(f"{base.name}_{suffix}")
            suffix += 1
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.rename(directory, destination)
        return destination

    @staticmethod
    def promote_unit(*, temporary: str | Path, current: str | Path, raw_root: str | Path,
                     archive_root: str | Path, expected_hash: str, defer_cleanup: bool = False) -> None:
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
                if RawObjectStore.verify_manifest(current) == expected_hash:
                    if not defer_cleanup:
                        RawObjectStore.refresh_index(current)
                        RawObjectStore.remove_request(temporary)
                    return
                if not (current.parent / "manifest.ndjson").is_file():
                    raise ValueError("refusing to replace unmanaged existing raw data")
                RawObjectStore.preserve_request(current, permitted_root=root, archive_root=archive_root)
            RawObjectStore.copy_request(temporary, current)
            if not defer_cleanup:
                RawObjectStore.refresh_index(current)
                RawObjectStore.remove_request(temporary)

    @staticmethod
    def finalize_promotions(promotions, *, raw_root):
        """Rebuild derived indexes and clean owned temporary requests once per directory."""
        root = Path(raw_root).resolve()
        removals = {}
        current_directories = set()
        for promotion in promotions:
            temporary, current = Path(promotion['temporary']).resolve(), Path(promotion['current']).resolve()
            if not temporary.is_relative_to(root / '_tmp') or not current.is_relative_to(root) or current.is_relative_to(root / '_tmp'):
                raise ValueError('batch raw finalization escaped raw storage')
            if RawObjectStore.verify_manifest(current) != promotion['raw_hash']:
                raise ValueError('batch raw finalization has no matching promoted response')
            removals.setdefault(temporary.parent, set()).add(temporary)
            current_directories.add(current.parent)
        for directory, owned in removals.items():
            retained_bodies, removed_bodies = set(), set()
            for manifest in directory.glob('manifest.*.ndjson'):
                bodies = {event['body_storage'] for event in map(json.loads, manifest.read_text(encoding='utf-8').splitlines()) if event.get('body_storage')}
                (removed_bodies if manifest.resolve() in owned else retained_bodies).update(bodies)
            for manifest in owned:
                manifest.unlink(missing_ok=True)
                label = manifest.name.removeprefix('manifest.').removesuffix('.ndjson')
                (directory / ('_managed_task.' + label + '.json')).unlink(missing_ok=True)
            for relative in removed_bodies - retained_bodies:
                body = (directory / relative).resolve()
                if not body.is_relative_to(directory):
                    raise ValueError('temporary response body escaped its directory')
                body.unlink(missing_ok=True)
        for directory in current_directories | set(removals):
            manifests = sorted(directory.glob('manifest.*.ndjson'))
            target = directory / 'manifest.ndjson'
            if manifests:
                temporary = directory / 'manifest.index.tmp'
                temporary.write_text(''.join(json.dumps({'event': 'request_manifest', 'request_manifest': p.name,
                    'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}) + '\n' for p in manifests), encoding='utf-8')
                os.replace(temporary, target)
            else:
                target.unlink(missing_ok=True)

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
                    fetched_at: datetime, attempt_id: str, suffix: str = "bin", body_archive: bool = False,
                    relative_path: str | Path | None = None) -> RawObjectRef:
        from uuid import uuid4

        for component in (dataset, provider, endpoint, attempt_id, suffix):
            if not component or component in {".", ".."} or any(c in component for c in ("/", "\\", ":")):
                raise ValueError("invalid raw object path component")
        relative = _archived_body_path(dataset=dataset, provider=provider, endpoint=endpoint,
                                       attempt_id=attempt_id, fetched_at=fetched_at, suffix=suffix) if body_archive else Path(
            fetched_at.date().isoformat(), dataset, provider, endpoint, f"{attempt_id}.{suffix}"
        )
        if relative_path is not None:
            relative = Path(relative_path)
            if relative.is_absolute() or ".." in relative.parts or not relative.parts:
                raise ValueError("artifact path must stay within its store")
            if not (self.root / relative).resolve().is_relative_to(self.root.resolve()):
                raise ValueError("artifact path escapes its store")
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        expected_hash = hashlib.sha256(content).hexdigest()
        if body_archive and target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() != expected_hash:
            base = target
            suffix_number = 2
            while target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() != expected_hash:
                target = base.with_name(f"{base.stem}_{suffix_number}{base.suffix}")
                suffix_number += 1
        relative = target.relative_to(self.root)
        if target.exists():
            existing_hash = hashlib.sha256(target.read_bytes()).hexdigest()
            if existing_hash != expected_hash:
                raise FileExistsError(f"immutable raw object already exists with different content: {target}")
            if body_archive and self.manifest.name != "manifest.ndjson":
                self.append_event({"event": "stored_artifact", "body_storage": relative.as_posix(), "body_sha256": existing_hash})
            return RawObjectRef(target, existing_hash, target.stat().st_size)

        # Keep the temporary basename short: appending a PID to a readable
        # request name can exceed Windows' path limit in deeply nested runs.
        temporary = target.with_name(f".tmp-{uuid4().hex}.part")
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        if body_archive and self.manifest.name != "manifest.ndjson":
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
        capture_time = fetched_at
        if self.capture_root is not None:
            stamp = datetime.fromisoformat(source_ref["fetched_at_utc"]) if source_ref and source_ref.get("fetched_at_utc") else fetched_at
            if stamp.tzinfo is None:
                raise ValueError("original source capture time must be timezone-aware")
            capture_time = stamp
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
            fetched_at=fetched_at, attempt_id="http-response", body_archive=True, relative_path=response_body_path(
                scope=scope, fetched_at=capture_time
            ),
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
    def batch_cache_lookup(roots):
        """Scan evidence once for a task, keeping the existing exact/fresh live-cache contract."""
        index = {}
        unique_roots = sorted({Path(root).resolve() for root in roots}, key=lambda p: len(p.parts))
        selected = []
        for root in unique_roots:
            if not any(root.is_relative_to(parent) for parent in selected):
                selected.append(root)
        for root in selected:
            for manifest in root.rglob('manifest*.ndjson') if root.exists() else ():
                for line in manifest.read_text(encoding='utf-8').splitlines():
                    event = json.loads(line)
                    if (event.get('mode') == 'live' and event.get('outcome') == 'response'
                            and event.get('status_code') == 200 and event.get('body_storage') and event.get('body_bytes', 0) > 0):
                        key = (event['method'], sanitized_url(event['url']),
                               json.dumps(event.get('scope'), sort_keys=True, default=str))
                        index.setdefault(key, []).append((manifest, event))
        def lookup(_roots, *, url, method, scope, max_age_seconds, ignored_query_parameters=()):
            if ignored_query_parameters:
                raise ValueError('daily batch cache requires exact request URL matching')
            now = datetime.now(timezone.utc)
            key = (method, sanitized_url(url), json.dumps(sanitized_metadata(scope), sort_keys=True, default=str))
            matches = sorted(index.get(key, ()), key=lambda pair: pair[1]['fetched_at_utc'], reverse=True)
            for manifest, event in matches:
                age = (now - datetime.fromisoformat(event['fetched_at_utc'])).total_seconds()
                if 0 <= age <= max_age_seconds:
                    return manifest, event, RawObjectStore.read_response(manifest, event)
            return None
        return lookup

    @staticmethod
    def find_cached_response(roots, *, url, method, scope, max_age_seconds, ignored_query_parameters=()):
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
                        if (event.get("mode") == "live"
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
