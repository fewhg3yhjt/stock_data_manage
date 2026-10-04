from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
from io import StringIO
import re
import time
from typing import Any, Callable, Mapping

import pandas as pd
import requests

from ..contracts import FailureClass, ProviderContractError


@dataclass(frozen=True, slots=True)
class ThsBoardSnapshot:
    board_type: str
    board_code: str
    board_name: str | None
    rows: tuple[Mapping[str, Any], ...]
    response_status: int
    source_url: str
    page: int = 1
    total_pages: int = 1


@dataclass(frozen=True, slots=True)
class ThsBoardListSnapshot:
    board_type: str
    rows: tuple[Mapping[str, Any], ...]
    response_status: int
    source_url: str
    page: int
    total_pages: int


@dataclass(slots=True)
class ThsBoardProvider:
    name: str = "ths"
    endpoint: str = "board_members"
    capability_version: str = "ths-board-members-v1"
    timeout_seconds: float = 20.0
    request_interval_seconds: float = 3.0
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/153 Safari/537.36"
    referer: str = "http://q.10jqka.com.cn/"
    cookie_v: str | None = None
    clock: Callable[[], float] = field(default=time.monotonic, repr=False)
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)
    _last_request_at: float | None = field(default=None, init=False, repr=False)

    def fetch_board_list(self, board_type: str, *, page: int = 1) -> ThsBoardListSnapshot:
        normalized_type = _board_type(board_type)
        page = _page_number(page)
        prefix = "thshy" if normalized_type == "industry" else "gn"
        url = _page_url(f"https://q.10jqka.com.cn/{prefix}/index/", page)
        response = self._get(url)
        table_html = _main_table_html(response.text)
        rows = _parse_board_list_rows(table_html, normalized_type)
        total_pages = _total_pages(response.text)
        if not rows:
            raise ProviderContractError(
                "THS board list is empty or changed",
                FailureClass.TEMPORARY_EMPTY,
                retryable=False,
            )
        return ThsBoardListSnapshot(
            normalized_type,
            rows,
            response.status_code,
            response.url,
            page,
            total_pages,
        )

    def fetch_members(self, board_type: str, board_code: str, *, page: int = 1) -> ThsBoardSnapshot:
        normalized_type = _board_type(board_type)
        page = _page_number(page)
        prefix = "thshy" if normalized_type == "industry" else "gn"
        url = _page_url(f"https://q.10jqka.com.cn/{prefix}/detail/code/{board_code}/", page)
        response = self._get(url)
        try:
            tables = pd.read_html(StringIO(response.text))
        except Exception as exc:
            raise ProviderContractError(
                "THS board response has no readable HTML table",
                FailureClass.SCHEMA_CHANGED,
                retryable=False,
            ) from exc
        table = next((item for item in tables if _has_member_columns(item)), None)
        if table is None or table.empty:
            raise ProviderContractError(
                "THS board member table is empty or changed",
                FailureClass.TEMPORARY_EMPTY,
                retryable=False,
            )
        rows = _member_rows(table, normalized_type, str(board_code), self.name)
        total_pages = _total_pages(response.text)
        return ThsBoardSnapshot(
            normalized_type,
            str(board_code),
            None,
            rows,
            response.status_code,
            response.url,
            page,
            total_pages,
        )

    def fetch_all_members(self, board_type: str, board_code: str) -> ThsBoardSnapshot:
        first = self.fetch_members(board_type, board_code, page=1)
        rows = list(first.rows)
        for page in range(2, first.total_pages + 1):
            rows.extend(self.fetch_members(board_type, board_code, page=page).rows)
        unique_rows = tuple({(row["board_code"], row["stock_code"]): row for row in rows}.values())
        return ThsBoardSnapshot(
            first.board_type,
            first.board_code,
            first.board_name,
            unique_rows,
            first.response_status,
            first.source_url,
            1,
            first.total_pages,
        )

    def _get(self, url: str) -> requests.Response:
        self._wait_before_request()
        headers = {"User-Agent": self.user_agent, "Referer": self.referer}
        if self.cookie_v:
            headers["Cookie"] = f"v={self.cookie_v}"
        response = requests.get(
            url,
            headers=headers,
            timeout=(8, self.timeout_seconds),
        )
        if _is_login_redirect(response):
            raise ProviderContractError(
                "THS request redirected to login",
                FailureClass.AUTH_REQUIRED,
                retryable=False,
                http_status=response.status_code,
            )
        if response.status_code >= 400:
            raise ProviderContractError(
                f"THS board request failed with HTTP {response.status_code}",
                FailureClass.HTTP_ERROR,
                retryable=False,
                http_status=response.status_code,
            )
        return response

    def _wait_before_request(self) -> None:
        now = self.clock()
        if self._last_request_at is not None:
            remaining = self.request_interval_seconds - (now - self._last_request_at)
            if remaining > 0:
                self.sleep(remaining)
        self._last_request_at = self.clock()


def _board_type(value: str) -> str:
    normalized = str(value).lower()
    if normalized in {"industry", "行业", "行业板块"}:
        return "industry"
    if normalized in {"concept", "概念", "概念板块"}:
        return "concept"
    raise ValueError(f"unsupported THS board type: {value}")


def _has_member_columns(frame: pd.DataFrame) -> bool:
    columns = {str(column) for column in frame.columns}
    return "代码" in columns and "名称" in columns


def _member_rows(
    table: pd.DataFrame, board_type: str, board_code: str, source: str
) -> tuple[Mapping[str, Any], ...]:
    rows: list[Mapping[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in table.to_dict("records"):
        if row.get("代码") in (None, "", "nan") or row.get("名称") in (None, "", "nan"):
            continue
        key = (board_code, str(row["代码"]).zfill(6))
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "board_type": board_type,
                "board_code": board_code,
                "stock_code": key[1],
                "stock_name": str(row["名称"]).strip(),
                "source": source,
            }
        )
    return tuple(rows)


def _main_table_html(text: str) -> str:
    match = re.search(r'<table[^>]*class=["\'][^"\']*m-pager-table[^"\']*["\'][^>]*>.*?</table>', text, re.I | re.S)
    return match.group(0) if match else ""


def _parse_board_list_rows(table_html: str, board_type: str) -> tuple[Mapping[str, Any], ...]:
    prefix = "thshy" if board_type == "industry" else "gn"
    pattern = re.compile(
        rf'''href=["'](?:https?://)?q\.10jqka\.com\.cn/{prefix}/detail/code/(\d+)/?["']'''
        r'[^>]*>(.*?)</a>',
        re.I | re.S,
    )
    rows: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for code, raw_name in pattern.findall(table_html):
        name = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", unescape(raw_name))).strip()
        if not name or code in seen:
            continue
        seen.add(code)
        rows.append(
            {
                "board_type": board_type,
                "board_code": code,
                "board_name": name,
                "source": "ths",
            }
        )
    return tuple(rows)


def _total_pages(text: str) -> int:
    match = re.search(r'<span[^>]*class=["\']page_info["\'][^>]*>\s*\d+\s*/\s*(\d+)', text, re.I)
    return int(match.group(1)) if match else 1


def _page_number(page: int) -> int:
    page = int(page)
    if page < 1:
        raise ValueError("page must be >= 1")
    return page


def _page_url(base_url: str, page: int) -> str:
    return base_url if page == 1 else f"{base_url}page/{page}/"


def _is_login_redirect(response: requests.Response) -> bool:
    final_url = str(getattr(response, "url", "")).lower()
    body = str(getattr(response, "text", "")).lower()
    return (
        "/account/login" in final_url
        or "upass.10jqka.com.cn/login" in body
        or "location.href" in body and "login" in body
    )
