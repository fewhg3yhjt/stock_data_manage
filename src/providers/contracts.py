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

import math, functools, re
import pandas as pd
from datetime import date as _date_cls, datetime, timezone

def _v39_json(response):
    """解析 JSON；不是 JSON 抛 RuntimeError。json 的解析错误是 ValueError 的子类，
    不转换会被调用方当成「确实没有数据」。"""
    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(f'{getattr(response, 'url', '')} 返回的不是 JSON，可能是错误页') from exc

def _v39_date(value):
    """'2026-09-18' / '20260918' / date 对象 → '2026-09-18'；其他写法抛 ValueError。"""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, _date_cls):
        return value.isoformat()
    text = str(value).strip()
    fmt = '%Y%m%d' if re.fullmatch('[0-9]{8}', text) else '%Y-%m-%d'
    return datetime.strptime(text, fmt).date().isoformat()

def _v39_src_date(value):
    """来源返回的日期 → 'YYYY-MM-DD'；认不出抛 RuntimeError（源格式变了，不是参数写错）。"""
    try:
        return _v39_date(value)
    except ValueError as exc:
        raise RuntimeError(f'来源返回了无法识别的日期 {value!r}') from exc

def _v39_num(value):
    """'1,234.50' → 1234.5；空串 / '-' / '--' / None → None；其他非数字抛 RuntimeError
    （来源给了认不出的值是「源的格式变了」，不能和参数错误的 ValueError 混在一起）。
    JSON 布尔值同样抛错：float(True)=1.0 会把格式错误静默写成价格 / 成交量。"""
    if value is None:
        return None
    if isinstance(value, bool):
        raise RuntimeError(f'来源在数值字段给了布尔值 {value!r}')
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (not math.isfinite(value)):
            return None
        return float(value)
    text = str(value).replace(',', '').strip()
    if text in ('', '-', '--', 'None', 'null'):
        return None
    try:
        number = float(text)
    except ValueError as exc:
        raise RuntimeError(f'来源返回了无法识别的数值 {value!r}') from exc
    return number if math.isfinite(number) else None

def _v39_rows(value, what):
    """来源里可能整段缺失的行列表：字段没有（None）按空处理，其余必须是对象列表。
    写成 `value or []` 会把 {} / '' / 0 这类结构改变也当成空表，静默丢掉整段数据。"""
    if value is None:
        return []
    if not isinstance(value, list) or not all((isinstance(row, dict) for row in value)):
        raise RuntimeError(f'{what} 应为对象列表，实际是 {type(value).__name__}: {str(value)[:120]}')
    return value

def _v39_req_num(value, what):
    """必填数值（价格、成交量）：在 _v39_num 之上，空值 / NaN / inf 也抛 RuntimeError，不能当缺失放过。"""
    number = _v39_num(value)
    if number is None:
        raise RuntimeError(f'来源的 {what} 为空或不是有限数值: {value!r}')
    return number

def _v39_contract(func):
    """统一异常契约：来源行缺字段时 row["X"] 会漏出 KeyError，调用方按「参数错 / 没数据」处理就会
    把「来源格式变了」当成正常情况。这里把它转成带函数名和字段名的 RuntimeError。
    （函数内所有按用户参数取字典的地方都先校验过参数，不会走到这里。）"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as exc:
            raise RuntimeError(f'{func.__name__}: 来源数据缺少字段 {exc}，格式可能已变') from exc
    return wrapper

def _v39_count(value, what):
    """来源自报的页数 / 条数 → 非负 int。只认 int 或纯数字串；bool 抛错（int(True)=1 会让
    「只返回 1 条」通过完整性核对），其他写法也抛 RuntimeError。"""
    text = str(value).strip() if isinstance(value, (int, str)) and (not isinstance(value, bool)) else ''
    if not re.fullmatch('[0-9]+', text):
        raise RuntimeError(f'{what} 不是非负整数: {value!r}')
    return int(text)

def _v39_frame(rows, source, url, columns=None):
    """统一出表：附 source / source_url / fetched_at。rows 为空时返回带列名的空表，
    是否允许为空由调用方判断（「确实没有」与「接口坏了」要分开处理）。"""
    frame = pd.DataFrame(rows, columns=columns)
    frame['source'] = source
    frame['source_url'] = url
    frame['fetched_at'] = datetime.now(timezone.utc).isoformat()
    return frame
