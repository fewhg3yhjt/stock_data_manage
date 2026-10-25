"""Read-only local web console for task, raw-evidence and publication views."""
from __future__ import annotations

import argparse
import base64
import codecs
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from ..config.loader import load_storage_paths


ACTIVE_TASK_STATES = frozenset({"collecting", "checking", "committing"})
RESPONSE_EVENTS = frozenset({"http_response", "source_payload", "sdk_query_failure"})
STALE_AFTER_SECONDS = 300
MAX_PREVIEW_BYTES = 256 * 1024
DEFAULT_PREVIEW_BYTES = 64 * 1024
SENSITIVE_KEY = re.compile(
    r"(?:authorization|cookie|token|secret|password|passwd|api[-_]?key|credential|proxy|proxies)",
    re.IGNORECASE,
)
ASSET_ROOT = Path(__file__).with_name("static")


class SecurityHeadersMiddleware:
    """Add browser protections without buffering or changing response bodies."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        async def send_with_headers(message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(
                    [
                        (b"x-content-type-options", b"nosniff"),
                        (b"x-frame-options", b"DENY"),
                        (b"referrer-policy", b"no-referrer"),
                        (
                            b"content-security-policy",
                            b"default-src 'self'; style-src 'self'; script-src 'self'; "
                            b"img-src 'self' data:; connect-src 'self'; object-src 'none'; "
                            b"base-uri 'none'; frame-ancestors 'none'",
                        ),
                    ]
                )
                if scope.get("path", "").startswith("/api/"):
                    headers.append((b"cache-control", b"no-store"))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read managed JSON: {path.name}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"managed JSON must contain an object: {path.name}")
    return value


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _page(items: list[dict[str, Any]], page: int, page_size: int) -> dict[str, Any]:
    start = (page - 1) * page_size
    return {
        "items": items[start : start + page_size],
        "page": page,
        "page_size": page_size,
        "total": len(items),
        "pages": (len(items) + page_size - 1) // page_size,
    }


def _redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    pairs = [
        (key, "[已隐藏]" if SENSITIVE_KEY.search(key) else item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    return urlunsplit((parsed.scheme, parsed.netloc.rsplit("@", 1)[-1], parsed.path, urlencode(pairs), ""))


def _redact(value: Any, *, key: str = "") -> Any:
    if key and SENSITIVE_KEY.search(key):
        return "[已隐藏]"
    if isinstance(value, dict):
        return {str(name): _redact(item, key=str(name)) for name, item in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, tuple):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        value = re.sub(r"https?://[^\s\"<>]+", lambda match: _redact_url(match[0]), value)
        value = re.sub(r"(?i)(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+", "[已隐藏]", value)
        value = re.sub(r"(?i)(token|password|api[_-]?key|secret)=([^&\s]+)", r"\1=[已隐藏]", value)
        value = re.sub(r"(?<![A-Za-z])[A-Za-z]:[\\/][^\s\"<>]*", "[本地路径]", value)
    return value


def _within(path: Path, root: Path) -> bool:
    return path.resolve().is_relative_to(root.resolve())


def _component(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value) or value in {".", ".."}:
        raise HTTPException(status_code=404, detail="记录不存在")
    return value


def _token(path: Path, root: Path) -> str:
    relative = path.resolve().relative_to(root.resolve()).as_posix().encode("utf-8")
    return base64.urlsafe_b64encode(relative).decode("ascii").rstrip("=")


def _from_token(value: str, root: Path, *, manifest: bool = True) -> Path:
    try:
        padding = "=" * (-len(value) % 4)
        relative = base64.urlsafe_b64decode(value + padding).decode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise HTTPException(status_code=404, detail="记录不存在") from exc
    if "\x00" in relative:
        raise HTTPException(status_code=404, detail="记录不存在")
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root.resolve()) or not candidate.is_file():
        raise HTTPException(status_code=404, detail="记录不存在")
    if manifest and not (candidate.name.startswith("manifest") and candidate.suffix == ".ndjson"):
        raise HTTPException(status_code=404, detail="记录不存在")
    return candidate


def _iter_ndjson(path: Path) -> Iterable[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    yield value
    except (OSError, UnicodeError):
        return


class DashboardReader:
    """Build bounded read models from existing managed files without writing state."""

    def __init__(self, config_root: str | Path, data_root: str | Path | None = None) -> None:
        self.paths = load_storage_paths(config_root, data_root=data_root)

    def _task_documents(self) -> list[tuple[dict[str, Any], Path, str]]:
        candidates: list[tuple[dict[str, Any], Path, str]] = []
        roots = (
            (self.paths["workspace_root"], "workspace"),
            (self.paths["archive_root"], "archive"),
        )
        for root, location in roots:
            if not root.exists():
                continue
            for path in root.rglob("summary.json"):
                if not _within(path, root):
                    continue
                relative = path.relative_to(root)
                if relative.parts[0].startswith("_") and relative.parts[0] != "_workspace_audit":
                    continue
                try:
                    document = _read_json(path)
                except ValueError:
                    continue
                if isinstance(document.get("task_id"), str):
                    candidates.append((document, path, location))
        latest: dict[str, tuple[dict[str, Any], Path, str]] = {}
        for document, path, location in candidates:
            task_id = document["task_id"]
            current = latest.get(task_id)
            updated = _parse_time(document.get("generated_at_utc")) or datetime.fromtimestamp(
                path.stat().st_mtime, timezone.utc
            )
            if current is None:
                latest[task_id] = (document, path, location)
                continue
            previous = _parse_time(current[0].get("generated_at_utc")) or datetime.fromtimestamp(
                current[1].stat().st_mtime, timezone.utc
            )
            if updated >= previous:
                latest[task_id] = (document, path, location)
        return list(latest.values())

    @staticmethod
    def _task_card(document: dict[str, Any], location: str) -> dict[str, Any]:
        updated = _parse_time(document.get("generated_at_utc"))
        age = None
        if updated is not None:
            age = max(0, int((datetime.now(timezone.utc) - updated).total_seconds()))
        status = str(document.get("status", "unknown"))
        stale = status in ACTIVE_TASK_STATES and (age is None or age > STALE_AFTER_SECONDS)
        progress = document.get("progress") if isinstance(document.get("progress"), dict) else {}
        stages = progress.get("stages") if isinstance(progress.get("stages"), dict) else {}
        raw = stages.get("raw") if isinstance(stages.get("raw"), dict) else {}
        missing = document.get("missing_units")
        source_units = document.get("source_units")
        return {
            "task_id": document.get("task_id"),
            "dataset": document.get("dataset"),
            "status": status,
            "display_status": "stale" if stale else status,
            "stale": stale,
            "complete_today": bool(document.get("complete_today")),
            "data_date": document.get("data_date"),
            "requested_date": document.get("requested_date"),
            "generated_at_utc": document.get("generated_at_utc"),
            "age_seconds": age,
            "verification_mode": document.get("verification_mode"),
            "progress": progress,
            "completed_units": raw.get("completed"),
            "total_units": raw.get("total"),
            "missing_count": len(missing) if isinstance(missing, list) else 0,
            "source_unit_count": len(source_units) if isinstance(source_units, list) else 0,
            "error": _redact(document.get("error") or (document.get("final_check") or {}).get("error")),
            "location": location,
        }

    def tasks(
        self,
        *,
        status: str | None = None,
        dataset: str | None = None,
        query: str | None = None,
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        cards = [self._task_card(document, location) for document, _, location in self._task_documents()]
        if status:
            cards = [item for item in cards if item["display_status"] == status or item["status"] == status]
        if dataset:
            cards = [item for item in cards if item["dataset"] == dataset]
        if query:
            needle = query.casefold()
            cards = [
                item
                for item in cards
                if needle in str(item["task_id"]).casefold() or needle in str(item["dataset"]).casefold()
            ]
        cards.sort(key=lambda item: str(item.get("generated_at_utc") or ""), reverse=True)
        return _page(cards, page, page_size)

    def _find_task(self, task_id: str) -> tuple[dict[str, Any], Path, str]:
        for document, path, location in self._task_documents():
            if document.get("task_id") == task_id:
                return document, path, location
        raise HTTPException(status_code=404, detail="任务不存在")

    def task(self, task_id: str) -> dict[str, Any]:
        document, path, location = self._find_task(task_id)
        card = self._task_card(document, location)
        final_check = document.get("final_check") if isinstance(document.get("final_check"), dict) else {}
        coverage = final_check.get("coverage") if isinstance(final_check.get("coverage"), dict) else {}
        missing = document.get("missing_units") if isinstance(document.get("missing_units"), list) else []
        try:
            definition_path = path.with_name("task.json")
            if not _within(definition_path, path.parent):
                raise ValueError("unmanaged task definition")
            saved_definition = _read_json(definition_path)
        except ValueError:
            saved_definition = {}
        definition = {
            key: value
            for key, value in saved_definition.items()
            if key not in {"units"} and not SENSITIVE_KEY.search(str(key))
        }
        publication = document.get("publication") or {}
        # Full frozen symbol lists remain in task.json; limit the browser payload.
        for key, value in list(definition.items()):
            if isinstance(value, list) and len(value) > 200:
                definition[key] = {"count": len(value), "sample": value[:200]}
        for key, value in list(coverage.items()):
            if isinstance(value, list) and len(value) > 200:
                coverage[key] = {"count": len(value), "sample": value[:200]}
        final_check = {**final_check, "coverage": coverage}
        return {
            **card,
            "definition": _redact(definition),
            "definition_hash": document.get("definition_hash"),
            "final_check": _redact(final_check),
            "coverage": _redact(coverage),
            "missing_units": missing[:200],
            "missing_units_truncated": len(missing) > 200,
            "row_count": publication.get("row_count", document.get("row_count")),
            "fallback_used": document.get("fallback_used"),
            "fallback_reason": document.get("fallback_reason"),
            "published_hash": publication.get("manifest_sha256", document.get("published_hash")),
        }

    def task_units(
        self,
        task_id: str,
        *,
        status: str | None = None,
        query: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        document, _, _ = self._find_task(task_id)
        units = document.get("source_units")
        if not isinstance(units, list):
            units = []
        matching = [(index, item) for index, item in enumerate(units) if isinstance(item, dict)]
        if status:
            matching = [(index, item) for index, item in matching if item.get("status") == status]
        if query:
            needle = query.casefold()
            matching = [(index, item) for index, item in matching if any(
                needle in str(item.get(name, "")).casefold() for name in ("key", "symbol", "input_id", "error"))]
        total = len(matching)
        matching = matching[(page - 1) * page_size:page * page_size]
        safe: list[dict[str, Any]] = []
        allowed = {
            "key", "symbol", "input_id", "status", "provider", "endpoint", "row_count",
            "failure_class", "error", "coverage_rejected", "returned_window", "raw_hash",
            "source_response_hashes", "source_capture_window", "request_scope", "mode",
        }
        for index, item in matching:
            if not isinstance(item, dict):
                continue
            row = {name: item.get(name) for name in allowed if name in item}
            row["index"] = index
            row["evidence"] = []
            for name in ("raw_manifest", "current_raw"):
                reference = item.get(name)
                if reference:
                    manifest = self._evidence_path(str(reference))
                    if manifest:
                        row["evidence"] = self._manifest_records(manifest)
                        break
            safe.append(_redact(row))
        return {"items": safe, "total": total, "page": page, "page_size": page_size,
                "pages": (total + page_size - 1) // page_size}

    def _evidence_path(self, reference: str) -> Path | None:
        candidate = Path(reference).resolve()
        if any(_within(candidate, root) for root in self._evidence_roots()) and candidate.is_file():
            if candidate.name.startswith("manifest") and candidate.suffix == ".ndjson":
                return candidate
        return None

    def _evidence_roots(self) -> tuple[Path, ...]:
        return (self.paths["raw_root"], self.paths["archive_root"] / "_raw_evidence")

    def _manifest_records(self, path: Path) -> list[dict[str, Any]]:
        return [self._raw_record(path, self.paths["data_root"], event, index)
                for index, event in enumerate(_iter_ndjson(path))
                if event.get("event") in RESPONSE_EVENTS]

    def raw_options(self) -> dict[str, Any]:
        root = self.paths["raw_root"]
        dates: list[str] = []
        if root.exists():
            dates = sorted(
                [path.name for path in root.iterdir() if path.is_dir() and _within(path, root)
                 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.name)],
                reverse=True,
            )
        latest = dates[0] if dates else None
        return {"dates": dates, "latest_date": latest}

    def raw_sources(self, data_date: str) -> dict[str, Any]:
        _component(data_date)
        day = (self.paths["raw_root"] / data_date).resolve()
        root = self.paths["raw_root"].resolve()
        if not day.is_relative_to(root) or not day.is_dir() or data_date.startswith("_"):
            raise HTTPException(status_code=404, detail="原始数据日期不存在")
        sources = []
        for provider in sorted(path for path in day.iterdir() if path.is_dir() and _within(path, day)):
            endpoints = sorted(path.name for path in provider.iterdir() if path.is_dir() and _within(path, day))
            sources.append({"provider": provider.name, "endpoints": endpoints})
        return {"data_date": data_date, "sources": sources}

    @staticmethod
    def _raw_record(path: Path, root: Path, response: dict[str, Any], index: int) -> dict[str, Any]:
        relative = path.resolve().relative_to(root.resolve())
        parts = relative.parts
        body_storage = response.get("body_storage")
        body = (path.parent / str(body_storage)).resolve() if body_storage else None
        has_body = bool(body and _within(body, path.parent / "bodies") and body.is_file())
        return {
            "id": f"{_token(path, root)}.{index}",
            "data_date": next((part for part in parts if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part)), None),
            "provider": response.get("provider"),
            "endpoint": response.get("endpoint"),
            "manifest_name": path.name,
            "method": response.get("method"),
            "url": _redact_url(str(response.get("url", ""))),
            "scope": _redact(response.get("scope", {})),
            "fetched_at_utc": response.get("fetched_at_utc"),
            "mode": response.get("mode"),
            "outcome": response.get("outcome"),
            "status_code": response.get("status_code"),
            "event_kind": response.get("event"),
            "sdk_status_code": response.get("sdk_status_code"),
            "representation": response.get("representation"),
            "content_type": response.get("content_type"),
            "encoding": response.get("encoding"),
            "body_bytes": response.get("body_bytes", response.get("payload_bytes")),
            "body_sha256": response.get("body_sha256"),
            "error": _redact(response.get("error")),
            "has_body": has_body,
        }

    def raw_records(
        self,
        *,
        data_date: str | None,
        provider: str | None,
        endpoint: str | None,
        query: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, Any]:
        options = self.raw_options()
        chosen_date = data_date or options["latest_date"]
        if not chosen_date:
            return _page([], page, page_size)
        _component(chosen_date)
        if provider:
            _component(provider)
        if endpoint:
            _component(endpoint)
        day = (self.paths["raw_root"] / chosen_date).resolve()
        root = self.paths["raw_root"].resolve()
        if not day.is_relative_to(root) or not day.is_dir() or chosen_date.startswith("_"):
            raise HTTPException(status_code=404, detail="原始数据日期不存在")
        directories = [day]
        if provider:
            directories = [(day / provider).resolve()]
        if endpoint:
            if not provider:
                raise HTTPException(status_code=400, detail="选择接口前必须先选择来源")
            directories = [((day / provider) / endpoint).resolve()]
        manifests: list[Path] = []
        for directory in directories:
            if not directory.is_relative_to(day) or not directory.is_dir():
                continue
            manifests.extend(path for path in directory.rglob("manifest*.ndjson") if path.is_file() and _within(path, root))
        records = [record for path in manifests for record in self._manifest_records(path)]
        # manifest.ndjson can be an aggregate of the named request manifests.
        records = list({(item["provider"], item["endpoint"], item["fetched_at_utc"],
                         item["url"], item["body_sha256"], item["outcome"]): item for item in records}.values())
        if query:
            needle = query.casefold()
            records = [item for item in records if needle in json.dumps(item, ensure_ascii=False).casefold()]
        records.sort(key=lambda item: str(item.get("fetched_at_utc") or ""), reverse=True)
        result = _page(records, page, page_size)
        result["data_date"] = chosen_date
        return result

    def _raw_document(self, evidence_id: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        try:
            token, index_text = evidence_id.rsplit(".", 1)
            index = int(index_text)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="记录不存在") from exc
        root = self.paths["data_root"]
        path = _from_token(token, root)
        if not any(_within(path, allowed) for allowed in self._evidence_roots()):
            raise HTTPException(status_code=404, detail="记录不存在")
        for position, event in enumerate(_iter_ndjson(path)):
            if position == index and event.get("event") in RESPONSE_EVENTS:
                return path, self._raw_record(path, root, event, index), event
        raise HTTPException(status_code=404, detail="响应记录不存在")

    def raw_detail(self, evidence_id: str, *, preview_bytes: int) -> dict[str, Any]:
        path, record, response = self._raw_document(evidence_id)
        storage = response.get("body_storage")
        preview: dict[str, Any] = {"kind": "unavailable", "text": None, "truncated": False}
        if storage:
            body = (path.parent / str(storage)).resolve()
            if _within(body, path.parent / "bodies") and any(_within(body, root) for root in self._evidence_roots()) and body.is_file():
                with body.open("rb") as stream:
                    payload = stream.read(preview_bytes + 1)
                truncated = len(payload) > preview_bytes
                payload = payload[:preview_bytes]
                encoding = str(response.get("encoding") or "utf-8")
                text: str | None = None
                for candidate in (encoding, "utf-8", "gb18030"):
                    try:
                        decoder = codecs.getincrementaldecoder(candidate)(errors="strict")
                        text = decoder.decode(payload, final=not truncated)
                        break
                    except (UnicodeDecodeError, LookupError):
                        continue
                binary_signature = payload.startswith((b"PK\x03\x04", b"%PDF-", b"\x89PNG", b"\xff\xd8\xff", b"\xd0\xcf\x11\xe0"))
                if text is not None and not binary_signature and "\x00" not in text:
                    stripped = text.strip()
                    if stripped.startswith(("{", "[")):
                        try:
                            text = json.dumps(json.loads(text), ensure_ascii=False, indent=2)
                            kind = "json"
                        except json.JSONDecodeError:
                            kind = "text"
                    else:
                        kind = "text"
                    preview = {"kind": kind, "text": text, "truncated": truncated}
                else:
                    preview = {"kind": "binary", "text": None, "truncated": truncated}
        return {
            **record,
            "request": _redact({
                "method": response.get("method"),
                "url": response.get("url"),
                "headers": response.get("request_headers", {}),
                "scope": response.get("scope", {}),
                "parameters": response.get("request_parameters", {}),
                "options": response.get("request_options", {}),
            }),
            "response": _redact({
                "status_code": response.get("status_code"),
                "headers": response.get("response_headers", {}),
                "content_type": response.get("content_type"),
                "encoding": response.get("encoding"),
                "body_bytes": response.get("body_bytes"),
                "body_sha256": response.get("body_sha256"),
                "persistence_status": response.get("persistence_status"),
                "sdk_status_code": response.get("sdk_status_code"),
                "representation": response.get("representation"),
                "original_transport_bytes_available": response.get("original_transport_bytes_available"),
            }),
            "preview": preview,
        }

    def raw_body(self, evidence_id: str) -> tuple[Path, str | None, str | None]:
        path, record, response = self._raw_document(evidence_id)
        storage = response.get("body_storage")
        if not storage:
            raise HTTPException(status_code=404, detail="原始响应文件不存在")
        body = (path.parent / str(storage)).resolve()
        if (not _within(body, path.parent / "bodies")
                or not any(_within(body, root) for root in self._evidence_roots()) or not body.is_file()):
            raise HTTPException(status_code=404, detail="原始响应文件不存在")
        digest = hashlib.sha256()
        with body.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        if not record.get("body_sha256") or digest.hexdigest() != record["body_sha256"]:
            raise HTTPException(status_code=409, detail="原始响应哈希不匹配，下载已阻止")
        return body, record.get("content_type"), record.get("body_sha256")

    def publications(
        self,
        *,
        dataset: str | None = None,
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        root = self.paths["canonical_root"]
        records: list[dict[str, Any]] = []
        tasks = self._task_documents()
        if root.exists():
            for path in root.rglob("manifest.json"):
                if not _within(path, root):
                    continue
                try:
                    document = _read_json(path)
                except ValueError:
                    continue
                if dataset and document.get("dataset") != dataset:
                    continue
                expected = document.get("expected_count")
                actual = document.get("row_count")
                coverage = round(actual / expected * 100, 2) if isinstance(actual, int) and isinstance(expected, int) and expected else None
                records.append({
                    "dataset": document.get("dataset"),
                    "asset_type": document.get("asset_type"),
                    "partition_key": document.get("partition_key"),
                    "row_count": actual,
                    "expected_count": expected,
                    "coverage_percent": coverage,
                    "conflict_count": document.get("conflict_count"),
                    "quarantined_count": document.get("quarantined_count"),
                    "content_hash": document.get("content_hash"),
                    "size_bytes": document.get("size_bytes"),
                    "source_count": len(document.get("raw_refs", [])) if isinstance(document.get("raw_refs"), list) else 0,
                    "tasks": [task["task_id"] for task, _, _ in tasks
                              if (task.get("publication") or {}).get("manifest")
                              and Path(task["publication"]["manifest"]).resolve() == path.resolve()],
                    "evidence": [record for ref in document.get("raw_refs", [])
                                 if isinstance(ref, str) and (manifest := self._evidence_path(ref))
                                 for record in self._manifest_records(manifest)],
                    "publication_metadata": _redact(document.get("publication_metadata", {})),
                    "updated_at_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
                })
        records.sort(key=lambda item: (str(item.get("partition_key") or ""), str(item.get("dataset") or "")), reverse=True)
        return _page(records, page, page_size)

    def overview(self) -> dict[str, Any]:
        task_result = self.tasks(page=1, page_size=1_000_000)
        tasks = task_result["items"]
        releases = self.publications(page=1, page_size=6)
        counts: dict[str, int] = {}
        for task in tasks:
            key = str(task["display_status"])
            counts[key] = counts.get(key, 0) + 1
        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "task_counts": counts,
            "recent_tasks": tasks[:6],
            "recent_publications": releases["items"],
            "raw_latest_date": self.raw_options()["latest_date"],
        }


def create_app(
    *, config_root: str | Path = Path("config"), data_root: str | Path | None = None
) -> FastAPI:
    reader = DashboardReader(config_root, data_root)
    app = FastAPI(
        title="证券数据运行台",
        description="现有任务、原始证据和发布结果的只读视图",
        version="1.0",
        docs_url=None,
        redoc_url=None,
    )
    app.state.reader = reader
    app.add_middleware(SecurityHeadersMiddleware)
    app.mount("/static", StaticFiles(directory=ASSET_ROOT), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(ASSET_ROOT / "index.html", media_type="text/html")

    @app.get("/api/overview")
    def overview():
        return reader.overview()

    @app.get("/api/tasks")
    def tasks(
        status: str | None = None,
        dataset: str | None = None,
        q: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int = Query(25, ge=1, le=100),
    ):
        return reader.tasks(status=status, dataset=dataset, query=q, page=page, page_size=page_size)

    @app.get("/api/tasks/{task_id}")
    def task(task_id: str):
        return reader.task(task_id)

    @app.get("/api/tasks/{task_id}/units")
    def task_units(
        task_id: str,
        status: str | None = None,
        q: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=1, le=100),
    ):
        return reader.task_units(task_id, status=status, query=q, page=page, page_size=page_size)

    @app.get("/api/raw/options")
    def raw_options():
        return reader.raw_options()

    @app.get("/api/raw/sources")
    def raw_sources(data_date: str):
        return reader.raw_sources(data_date)

    @app.get("/api/raw")
    def raw_records(
        data_date: str | None = None,
        provider: str | None = None,
        endpoint: str | None = None,
        q: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int = Query(25, ge=1, le=100),
    ):
        return reader.raw_records(
            data_date=data_date,
            provider=provider,
            endpoint=endpoint,
            query=q,
            page=page,
            page_size=page_size,
        )

    @app.get("/api/raw/{evidence_id}")
    def raw_detail(
        evidence_id: str,
        preview_bytes: int = Query(DEFAULT_PREVIEW_BYTES, ge=1024, le=MAX_PREVIEW_BYTES),
    ):
        return reader.raw_detail(evidence_id, preview_bytes=preview_bytes)

    @app.get("/api/raw/{evidence_id}/download")
    def raw_download(evidence_id: str):
        body, content_type, digest = reader.raw_body(evidence_id)
        name = f"{digest or body.stem}{body.suffix or '.bin'}"
        return FileResponse(body, media_type="application/octet-stream", filename=name)

    @app.get("/api/publications")
    def publications(
        dataset: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int = Query(25, ge=1, le=100),
    ):
        return reader.publications(dataset=dataset, page=page, page_size=page_size)

    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="启动证券数据只读运行台")
    parser.add_argument("--config-root", type=Path, default=Path("config"))
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help="允许绑定非本机地址；部署前仍需在反向代理层配置认证和 HTTPS",
    )
    args = parser.parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"} and not args.allow_network:
        parser.error("非本机监听地址必须显式使用 --allow-network")
    import uvicorn

    app = create_app(config_root=args.config_root, data_root=args.data_root)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
