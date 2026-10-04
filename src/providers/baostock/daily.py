from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from ...domain import Adjustment, AssetType
from ..base import FetchResult
from ..contracts import FailureClass, ProviderContractError, WindowStatus
from .session import logged_in_session, read_rows, source_code


@dataclass(slots=True)
class BaoStockDailyProvider:
    name: str = "baostock"
    endpoint: str = "daily_history"
    capability_priority: int = 200
    capability_version: str = "baostock-daily-v1"
    adjustment: Adjustment = Adjustment.NONE
    supported_asset_types: frozenset[AssetType] = frozenset(
        {AssetType.STOCK, AssetType.ETF, AssetType.INDEX}
    )
    start_date: date = date(1990, 1, 1)

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        returned_rows: list[dict[str, Any]] = []
        with logged_in_session() as bs:
            for symbol in requested:
                code = source_code(symbol)
                result = bs.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg",
                    start_date=self.start_date.isoformat(),
                    end_date=trade_date.isoformat(),
                    frequency="d",
                    adjustflag=_adjustflag(self.adjustment),
                )
                parsed = []
                for item in read_rows(result):
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
            ("trade_date", "open", "high", "low", "close", "volume", "amount", "adjustflag"),
            ("volume:share", "amount:cny"), len(returned_rows),
            str(returned_rows[0]["trade_date"]) if returned_rows else None,
            str(returned_rows[-1]["trade_date"]) if returned_rows else None,
            self.adjustment,
        )


def _adjustflag(adjustment: Adjustment) -> str:
    return {
        Adjustment.NONE: "3",
        Adjustment.BACKWARD: "1",
        Adjustment.FORWARD: "2",
    }[adjustment]
