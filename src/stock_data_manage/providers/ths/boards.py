from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from typing import Any, Mapping

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


@dataclass(slots=True)
class ThsBoardProvider:
    name: str = "ths"
    endpoint: str = "board_members"
    capability_version: str = "ths-board-members-v1"
    timeout_seconds: float = 20.0
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/153 Safari/537.36"
    referer: str = "http://q.10jqka.com.cn/"

    def fetch_members(self, board_type: str, board_code: str) -> ThsBoardSnapshot:
        normalized_type = _board_type(board_type)
        prefix = "thshy" if normalized_type == "industry" else "gn"
        url = f"https://q.10jqka.com.cn/{prefix}/detail/code/{board_code}/"
        response = requests.get(
            url,
            headers={"User-Agent": self.user_agent, "Referer": self.referer},
            timeout=(8, self.timeout_seconds),
        )
        if response.status_code >= 400:
            raise ProviderContractError(
                f"THS board request failed with HTTP {response.status_code}",
                FailureClass.HTTP_ERROR,
                retryable=False,
                http_status=response.status_code,
            )
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
        columns = [str(column) for column in table.columns]
        code_column = "代码"
        name_column = "名称"
        rows = tuple(
            {
                "board_type": normalized_type,
                "board_code": str(board_code),
                "stock_code": str(row[code_column]).zfill(6),
                "stock_name": str(row[name_column]).strip(),
                "source": self.name,
            }
            for row in table.to_dict("records")
            if row.get(code_column) not in (None, "", "nan")
        )
        return ThsBoardSnapshot(
            normalized_type,
            str(board_code),
            None,
            rows,
            response.status_code,
            response.url,
        )


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
