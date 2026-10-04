from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Sequence


class FailureClass(StrEnum):
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    RATE_LIMITED = "rate_limited"
    AUTH_REQUIRED = "auth_required"
    HTTP_5XX = "http_5xx"
    HTTP_ERROR = "http_error"
    HTML_RESPONSE = "html_response"
    INVALID_JSON = "invalid_json"
    SCHEMA_CHANGED = "schema_changed"
    TEMPORARY_EMPTY = "temporary_empty"
    TRUNCATED = "truncated"


class WindowStatus(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    TEMPORARY_EMPTY = "temporary_empty"
    TRUNCATED = "truncated"


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


@dataclass(frozen=True, slots=True)
class InputFetchResult:
    rows: tuple[Mapping[str, Any], ...]
    source_url: str | None = None
    empty_is_valid: bool = False
    source_rows: tuple[Mapping[str, Any], ...] | None = None
    mapping_context: Mapping[str, Any] = field(default_factory=dict)
    excluded_rows: tuple[Mapping[str, Any], ...] = ()


class ProviderContractError(RuntimeError):
    def __init__(
        self,
        message: str,
        failure_class: FailureClass,
        *,
        retryable: bool,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_class = failure_class
        self.retryable = retryable
        self.http_status = http_status


@dataclass(frozen=True, slots=True)
class EndpointContract:
    required_fields: frozenset[str]
    max_rows_per_request: int | None = None
    supports_pagination: bool = False

    def parse_json(self, response: HttpResponse) -> Any:
        _validate_http(response)
        try:
            return json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ProviderContractError(
                "provider returned invalid JSON", FailureClass.INVALID_JSON, retryable=False
            ) from exc

    def validate_rows(self, rows: Sequence[Mapping[str, Any]]) -> WindowStatus:
        if not rows:
            return WindowStatus.TEMPORARY_EMPTY
        for index, row in enumerate(rows):
            missing = self.required_fields - set(row)
            if missing:
                raise ProviderContractError(
                    f"row {index} is missing required fields: {sorted(missing)}",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
        if (
            self.max_rows_per_request is not None
            and len(rows) >= self.max_rows_per_request
            and not self.supports_pagination
        ):
            return WindowStatus.TRUNCATED
        return WindowStatus.COMPLETE


def assess_returned_window(
    *,
    requested_first: str,
    requested_last: str,
    returned_keys: Sequence[str],
    max_rows_per_request: int | None,
    supports_pagination: bool,
) -> WindowStatus:
    if not returned_keys:
        return WindowStatus.TEMPORARY_EMPTY
    ordered = sorted(returned_keys)
    if (
        max_rows_per_request is not None
        and len(returned_keys) >= max_rows_per_request
        and not supports_pagination
        and (ordered[0] > requested_first or ordered[-1] < requested_last)
    ):
        return WindowStatus.TRUNCATED
    if ordered[0] > requested_first or ordered[-1] < requested_last:
        return WindowStatus.PARTIAL
    return WindowStatus.COMPLETE


def _validate_http(response: HttpResponse) -> None:
    if response.status_code in {403, 429}:
        raise ProviderContractError(
            f"provider rate limited request with HTTP {response.status_code}",
            FailureClass.RATE_LIMITED,
            retryable=False,
            http_status=response.status_code,
        )
    if 500 <= response.status_code <= 599:
        raise ProviderContractError(
            f"provider failed with HTTP {response.status_code}",
            FailureClass.HTTP_5XX,
            retryable=True,
            http_status=response.status_code,
        )
    if not 200 <= response.status_code <= 299:
        raise ProviderContractError(
            f"provider failed with HTTP {response.status_code}",
            FailureClass.HTTP_ERROR,
            retryable=False,
            http_status=response.status_code,
        )
    content_type = next(
        (value for key, value in response.headers.items() if key.lower() == "content-type"), ""
    ).lower()
    leading = response.body.lstrip()[:32].lower()
    body_is_html = leading.startswith((b"<html", b"<!doctype", b"<script"))
    content_claims_html_without_json_body = (
        "text/html" in content_type and not leading.startswith((b"{", b"[", b"null"))
    )
    if body_is_html or content_claims_html_without_json_body:
        raise ProviderContractError(
            "provider returned HTML instead of data",
            FailureClass.HTML_RESPONSE,
            retryable=False,
        )
