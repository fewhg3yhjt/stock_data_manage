from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence

from ...domain import Adjustment
from ..contracts import FailureClass, ProviderContractError
from ..transport import RealtimeFetchResponse
from .session import logged_in_session, read_rows, source_code


@dataclass(slots=True)
class BaoStockMinuteProvider:
    name: str = "baostock"
    endpoint: str = "minute_5m"
    capability_version: str = "baostock-minute-5m-v1"
    frequency: int = 5
    adjustment: Adjustment = Adjustment.NONE

    def fetch_realtime_minute(self, symbols: Sequence[str], as_of: datetime) -> RealtimeFetchResponse:
        if self.frequency != 5:
            raise ProviderContractError(
                "BaoStock adapter only supports 5m in this implementation",
                FailureClass.SCHEMA_CHANGED,
                retryable=False,
            )
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        with logged_in_session() as bs:
            for symbol in requested:
                result = bs.query_history_k_data_plus(
                    source_code(symbol),
                    "date,time,code,open,high,low,close,volume,amount,adjustflag",
                    start_date=as_of.date().isoformat(),
                    end_date=as_of.date().isoformat(),
                    frequency="5",
                    adjustflag="3",
                )
                for item in read_rows(result):
                    timestamp = f"{item.get('date')} {item.get('time')}"
                    rows.append({
                        "symbol": symbol,
                        "trade_date": item.get("date"),
                        "bar_time": timestamp,
                        "open": item.get("open"),
                        "high": item.get("high"),
                        "low": item.get("low"),
                        "close": item.get("close"),
                        "volume": item.get("volume"),
                        "amount": item.get("amount"),
                    })
        return RealtimeFetchResponse(
            tuple(rows), requested, (),
            ("trade_date", "bar_time", "open", "high", "low", "close", "volume", "amount"),
            ("volume:share", "amount:cny"), len(rows),
            str(rows[0]["bar_time"]) if rows else None, str(rows[-1]["bar_time"]) if rows else None,
        )
