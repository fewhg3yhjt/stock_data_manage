from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..contracts import EndpointContract, FailureClass, ProviderContractError
from .realtime import EastMoneyRequestsTransport


@dataclass(frozen=True, slots=True)
class ShareholderCountRecord:
    symbol: str
    end_date: str
    previous_end_date: str | None
    holder_count: float | None
    previous_holder_count: float | None
    holder_count_change: float | None
    holder_count_change_pct: float | None
    notice_date: str | None
    average_market_cap: float | None
    average_hold_num: float | None
    total_market_cap: float | None
    change_reason: str | None


@dataclass(frozen=True, slots=True)
class ShareholderCountFetchResult:
    records: tuple[ShareholderCountRecord, ...]
    requested_symbols: tuple[str, ...]
    response_status: int


@dataclass(slots=True)
class EastMoneyShareholderCountProvider:
    transport: Any | None = None
    endpoint: str = "shareholder_count"
    name: str = "eastmoney"
    capability_version: str = "eastmoney-shareholder-count-v1"
    timeout_seconds: float = 20.0
    page_size: int = 10
    url: str = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    def __post_init__(self) -> None:
        if self.transport is None:
            self.transport = EastMoneyRequestsTransport()

    def fetch(self, symbols: Sequence[str]) -> ShareholderCountFetchResult:
        requested = tuple(symbols)
        records: list[ShareholderCountRecord] = []
        status = 200
        for symbol in requested:
            response = self.transport.get(
                self.url,
                params={
                    "reportName": "RPT_HOLDERNUM_DET",
                    "columns": "ALL",
                    "filter": f'(SECURITY_CODE="{symbol[2:]}")',
                    "pageNumber": "1",
                    "pageSize": str(self.page_size),
                    "sortColumns": "END_DATE",
                    "sortTypes": "-1",
                    "source": "WEB",
                    "client": "WEB",
                },
                timeout_seconds=self.timeout_seconds,
            )
            status = response.status_code
            payload = EndpointContract(frozenset()).parse_json(response)
            rows = _rows(payload)
            records.extend(
                ShareholderCountRecord(
                    symbol=symbol,
                    end_date=_text(row, "END_DATE") or "",
                    previous_end_date=_text(row, "PRE_END_DATE"),
                    holder_count=_number(row, "HOLDER_NUM"),
                    previous_holder_count=_number(row, "PRE_HOLDER_NUM"),
                    holder_count_change=_number(row, "HOLDER_NUM_CHANGE"),
                    holder_count_change_pct=_number(row, "HOLDER_NUM_RATIO"),
                    notice_date=_text(row, "HOLD_NOTICE_DATE"),
                    average_market_cap=_number(row, "AVG_MARKET_CAP"),
                    average_hold_num=_number(row, "AVG_HOLD_NUM"),
                    total_market_cap=_number(row, "TOTAL_MARKET_CAP"),
                    change_reason=_text(row, "CHANGE_REASON"),
                )
                for row in rows
            )
        return ShareholderCountFetchResult(tuple(records), requested, status)


def _rows(payload: object) -> list[Mapping[str, Any]]:
    if not isinstance(payload, dict):
        raise ProviderContractError("EastMoney shareholder response envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
    result = payload.get("result")
    rows = result.get("data") if isinstance(result, dict) else None
    if not isinstance(rows, list):
        raise ProviderContractError("EastMoney shareholder rows changed", FailureClass.SCHEMA_CHANGED, retryable=False)
    return [row for row in rows if isinstance(row, dict)]


def _text(row: Mapping[str, Any], name: str) -> str | None:
    value = row.get(name)
    return None if value in (None, "") else str(value)


def _number(row: Mapping[str, Any], name: str) -> float | None:
    value = row.get(name)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
