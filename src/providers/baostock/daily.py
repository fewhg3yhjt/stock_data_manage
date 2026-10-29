from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from typing import Any, Sequence

from ...domain import Adjustment, AssetType
from ..base import FetchResult
from ..contracts import FailureClass, ProviderContractError, WindowStatus
from .session import logged_in_session, read_rows, source_code


BAOSTOCK_DAILY_SOURCE_FIELDS = (
    "date", "code", "open", "high", "low", "close", "preclose", "volume", "amount",
    "adjustflag", "turn", "tradestatus", "pctChg", "peTTM", "pbMRQ", "psTTM",
    "pcfNcfTTM", "isST",
)


@dataclass(slots=True)
class BaoStockDailyProvider:
    name: str = "baostock"
    endpoint: str = "daily_history"
    capability_priority: int = 200
    capability_version: str = "baostock-daily-v2-all-fields"
    client: Any | None = None
    adjustment: Adjustment = Adjustment.NONE
    supported_asset_types: frozenset[AssetType] = frozenset(
        {AssetType.STOCK, AssetType.ETF, AssetType.INDEX}
    )
    start_date: date = date(1990, 1, 1)
    field_selection_policy: str = "all_supported_daily"

    def __post_init__(self) -> None:
        if self.field_selection_policy != "all_supported_daily":
            raise ValueError("BaoStock daily requests require the complete supported daily field set")

    def fetch_daily(self, symbols: Sequence[str], trade_date: date, *, raw_archive=None) -> FetchResult:
        if raw_archive is None or not getattr(raw_archive, "captures_tcp_wire", False):
            raise ValueError("BaoStock daily validation requires captured_sdk_queries and its raw archive")
        if self.client is None:
            raise ValueError("BaoStock daily validation requires the SDK client supplied by captured_sdk_queries")
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        returned_rows: list[dict[str, Any]] = []
        source_rows: list[dict[str, Any]] = []
        with logged_in_session(client=self.client) as bs:
            for symbol in requested:
                code = source_code(symbol)
                request_parameters = {
                    "code": code,
                    "fields": ",".join(BAOSTOCK_DAILY_SOURCE_FIELDS),
                    "start_date": self.start_date.isoformat(),
                    "end_date": trade_date.isoformat(),
                    "frequency": "d",
                    "adjustflag": _adjustflag(self.adjustment),
                }
                result = bs.query_history_k_data_plus(**request_parameters)
                source_items = read_rows(result)
                source_fields = result.fields if isinstance(result.fields, list) else str(result.fields).split(",")
                decoded_payload = json.dumps(
                    {"fields": source_fields, "rows": source_items,
                     "result": {"error_code": str(result.error_code), "error_msg": str(getattr(result, "error_msg", ""))}},
                    ensure_ascii=False, separators=(",", ":"), default=str,
                ).encode("utf-8")
                raw_archive.store_source_payload(
                    decoded_payload, provider="baostock", endpoint="query_history_k_data_plus",
                    representation="SDK-decoded ResultSet fields and rows; exact socket recv byte stream archived separately",
                    metadata={
                        "row_count": len(source_items), "trade_date": trade_date.isoformat(), "symbol": symbol,
                        "request_parameters": request_parameters, "outcome": "success",
                    },
                )
                parsed = []
                for item in source_items:
                    source_rows.append({"symbol": symbol, **item})
                    parsed.append({
                        "symbol": symbol,
                        "trade_date": item.get("date"),
                        "open": item.get("open"),
                        "high": item.get("high"),
                        "low": item.get("low"),
                        "close": item.get("close"),
                        "volume": item.get("volume"),
                        "amount": item.get("amount"),
                        "pre_close": item.get("preclose"),
                        "adjustflag": item.get("adjustflag"),
                    })
                returned_rows.extend(parsed)
                rows.extend(item for item in parsed if item["trade_date"] == trade_date.isoformat())
        status = WindowStatus.COMPLETE if rows else WindowStatus.TEMPORARY_EMPTY
        return FetchResult(
            tuple(rows), requested, status, (),
            BAOSTOCK_DAILY_SOURCE_FIELDS,
            ("volume:share", "amount:cny"), len(returned_rows),
            str(returned_rows[0]["trade_date"]) if returned_rows else None,
            str(returned_rows[-1]["trade_date"]) if returned_rows else None,
            self.adjustment, tuple(source_rows),
        )


def _adjustflag(adjustment: Adjustment) -> str:
    return {
        Adjustment.NONE: "3",
        Adjustment.BACKWARD: "1",
        Adjustment.FORWARD: "2",
    }[adjustment]
