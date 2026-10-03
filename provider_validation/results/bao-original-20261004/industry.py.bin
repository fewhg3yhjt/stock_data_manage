from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date
import json
from typing import Any, Mapping, Sequence

from ..contracts import FailureClass, ProviderContractError
from .session import logged_in_session, read_rows


@dataclass(frozen=True, slots=True)
class BaoStockIndustryResult:
    rows: tuple[Mapping[str, Any], ...]
    requested_symbols: tuple[str, ...]
    trade_date: date
    coverage_denominator: int
    missing_symbols: tuple[str, ...]
    response_statuses: tuple[int, ...] = ()
    field_semantics: tuple[str, ...] = ()


@dataclass(slots=True)
class BaoStockIndustryMembershipProvider:
    """CSRC industry snapshot, preserving the verified two-query BaoStock flow."""

    name: str = "baostock"
    endpoint: str = "industry_membership"
    capability_version: str = "baostock-csrc-industry-v1"

    def fetch_snapshot(
        self,
        trade_date: date,
        symbols: Sequence[str] | None = None,
        *,
        raw_archive: Any | None = None,
    ) -> BaoStockIndustryResult:
        requested_filter = (
            {_canonical_symbol(item) for item in symbols} if symbols is not None else None
        )
        with logged_in_session() as bs:
            listed_result = bs.query_all_stock(day=trade_date.isoformat())
            listed_rows = _read_success(
                listed_result, "query_all_stock", raw_archive, trade_date
            )
            _archive_sdk_rows(
                raw_archive, "query_all_stock", listed_result, listed_rows, trade_date
            )

            industry_result = bs.query_stock_industry(date=trade_date.isoformat())
            industry_rows = _read_success(
                industry_result, "query_stock_industry", raw_archive, trade_date
            )
            _archive_sdk_rows(
                raw_archive, "query_stock_industry", industry_result, industry_rows, trade_date
            )

        securities: dict[str, dict[str, str]] = {}
        for row in listed_rows:
            raw_code = str(row.get("code", ""))
            code = _a_share_code(raw_code)
            if code is None:
                continue
            symbol = _symbol(raw_code)
            if requested_filter is not None and symbol not in requested_filter:
                continue
            securities[code] = {
                "symbol": symbol,
                "stock_name": str(row.get("code_name", "")),
                "status": "active" if str(row.get("tradeStatus", "")) == "1" else "suspended",
                "exchange": "XSHG" if raw_code.lower().startswith("sh.") else "XSHE",
            }

        if requested_filter is not None:
            absent_from_universe = requested_filter - {item["symbol"] for item in securities.values()}
            if absent_from_universe:
                raise ProviderContractError(
                    f"BaoStock A-share universe omitted requested symbols: {sorted(absent_from_universe)}",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )

        industries: dict[str, Mapping[str, Any]] = {}
        for row in industry_rows:
            code = _a_share_code(str(row.get("code", "")))
            if code:
                industries[code] = row

        rows: list[Mapping[str, Any]] = []
        missing_symbols: list[str] = []
        for code, security in sorted(securities.items()):
            industry = industries.get(code)
            industry_name = str(industry.get("industry", "")) if industry else ""
            if not industry_name:
                missing_symbols.append(security["symbol"])
                continue
            rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "stock_code": code,
                    "stock_name": security["stock_name"],
                    "exchange": security["exchange"],
                    "status": security["status"],
                    "industry_name": industry_name,
                    "classification": industry.get("industryClassification"),
                    "classification_update_date": industry.get("updateDate"),
                    "source": "baostock",
                }
            )
        if not securities:
            raise ProviderContractError(
                "BaoStock A-share universe returned no matching securities",
                FailureClass.TEMPORARY_EMPTY,
                retryable=False,
            )
        return BaoStockIndustryResult(
            tuple(rows),
            tuple(item["symbol"] for item in securities.values()),
            trade_date,
            len(securities),
            tuple(missing_symbols),
            field_semantics=(
                "stock_code", "stock_name", "exchange", "status", "industry_name",
                "classification", "classification_update_date",
            ),
        )


def _read_success(
    result_set: Any, endpoint: str, raw_archive: Any | None, trade_date: date
) -> list[dict[str, Any]]:
    error_code = getattr(result_set, "error_code", None)
    if error_code != "0":
        _archive_sdk_rows(
            raw_archive,
            endpoint,
            result_set,
            [],
            trade_date,
            failure={
                "error_code": error_code,
                "error_msg": getattr(result_set, "error_msg", ""),
            },
        )
        raise ProviderContractError(
            f"BaoStock {endpoint} failed: {error_code} {getattr(result_set, 'error_msg', '')}",
            FailureClass.CONNECTION,
            retryable=True,
        )
    rows = read_rows(result_set)
    if not rows:
        _archive_sdk_rows(raw_archive, endpoint, result_set, [], trade_date, failure={"outcome": "empty"})
        raise ProviderContractError(
            f"BaoStock {endpoint} returned no rows",
            FailureClass.TEMPORARY_EMPTY,
            retryable=False,
        )
    return rows


def _archive_sdk_rows(
    archive: Any | None,
    endpoint: str,
    result_set: Any,
    rows: Sequence[Mapping[str, Any]],
    trade_date: date,
    failure: Mapping[str, Any] | None = None,
) -> None:
    if archive is None:
        return
    payload = json.dumps(
        {
            "fields": getattr(result_set, "fields", []),
            "rows": list(rows),
            **({"result": dict(failure)} if failure else {}),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    representation = (
        "SDK-decoded ResultSet fields and rows; BaoStock TCP wire bytes are not exposed"
    )
    scope = archive.scope(
        provider="baostock",
        endpoint=endpoint,
        request_scope=f"trade_date={trade_date.isoformat()};exchange_scope=SH+SZ A shares",
        representation=representation,
    ) if hasattr(archive, "scope") else nullcontext()
    with scope:
        archive.store_source_payload(
            payload,
            provider="baostock",
            endpoint=endpoint,
            representation=representation,
            metadata={
                "row_count": len(rows),
                "trade_date": trade_date.isoformat(),
                "outcome": dict(failure) if failure else "success",
            },
        )


def _a_share_code(source_code: str) -> str | None:
    prefix, dot, code = str(source_code).lower().partition(".")
    if not dot or len(code) != 6 or not code.isdigit():
        return None
    if (prefix == "sh" and code.startswith(("60", "68"))) or (
        prefix == "sz" and code.startswith(("000", "001", "002", "003", "300", "301"))
    ):
        return code
    return None


def _symbol(source_code: str) -> str:
    prefix, _, code = str(source_code).lower().partition(".")
    return f"{prefix}{code}"


def _canonical_symbol(value: str) -> str:
    text = str(value).strip().lower().replace(".", "")
    if len(text) == 6 and text.isdigit():
        if text.startswith(("60", "68")):
            text = "sh" + text
        elif text.startswith(("000", "001", "002", "003", "300", "301")):
            text = "sz" + text
    if len(text) != 8 or text[:2] not in {"sh", "sz"} or not text[2:].isdigit():
        raise ValueError(f"BaoStock CSRC industry membership requires an SH/SZ A-share symbol: {value}")
    return text
