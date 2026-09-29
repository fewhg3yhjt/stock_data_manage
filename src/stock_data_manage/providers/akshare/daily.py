from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Mapping, Sequence

from ...domain import Adjustment, AssetType
from ..base import FetchResult
from ..contracts import FailureClass, ProviderContractError, WindowStatus
from .session import load_client, source_symbol


@dataclass(slots=True)
class AkShareDailyProvider:
    """Function-level AkShare daily adapter, initially validation-only."""

    asset_type: AssetType
    adjustment: Adjustment = Adjustment.NONE
    client: Any | None = None
    name: str = "akshare"
    capability_priority: int = 300
    capability_version: str = "akshare-daily-v1"
    timeout_seconds: float = 30.0
    start_date: date = date(1990, 1, 1)

    @property
    def endpoint(self) -> str:
        return {
            AssetType.STOCK: "stock_daily",
            AssetType.ETF: "etf_daily",
            AssetType.LOF: "lof_daily",
            AssetType.INDEX: "index_daily",
        }[self.asset_type]

    @property
    def supported_asset_types(self) -> frozenset[AssetType]:
        return frozenset({self.asset_type})

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        requested = tuple(symbols)
        rows: list[dict[str, Any]] = []
        returned_rows: list[dict[str, Any]] = []
        ak = self.client or load_client()
        for symbol in requested:
            frame = self._fetch_frame(ak, symbol, trade_date)
            parsed = [_normalize_row(symbol, row) for row in _records(frame)]
            returned_rows.extend(parsed)
            rows.extend(item for item in parsed if item["trade_date"] == trade_date.isoformat())
        return FetchResult(
            tuple(rows), requested,
            WindowStatus.COMPLETE if rows else WindowStatus.TEMPORARY_EMPTY,
            (),
            ("trade_date", "open", "high", "low", "close", "volume", "amount"),
            ("volume:source_defined", "amount:source_defined"),
            len(returned_rows),
            str(returned_rows[0]["trade_date"]) if returned_rows else None,
            str(returned_rows[-1]["trade_date"]) if returned_rows else None,
            self.adjustment,
        )

    def _fetch_frame(self, ak: Any, symbol: str, trade_date: date) -> Any:
        start = self.start_date.strftime("%Y%m%d")
        end = trade_date.strftime("%Y%m%d")
        adjust = _adjustment_parameter(self.adjustment, self.asset_type)
        code = source_symbol(symbol, keep_exchange=self.asset_type is AssetType.INDEX)
        try:
            if self.asset_type is AssetType.STOCK:
                return ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust=adjust)
            if self.asset_type is AssetType.ETF:
                return ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start, end_date=end, adjust=adjust)
            if self.asset_type is AssetType.LOF:
                return ak.fund_lof_hist_em(symbol=code, start_date=start, end_date=end, adjust=adjust)
            return ak.stock_zh_index_daily_em(symbol=code)
        except Exception as exc:
            raise ProviderContractError(
                f"AkShare {self.endpoint} request failed for {symbol}",
                FailureClass.CONNECTION,
                retryable=True,
            ) from exc


def _adjustment_parameter(adjustment: Adjustment, asset_type: AssetType) -> str:
    if asset_type is AssetType.INDEX:
        if adjustment is not Adjustment.NONE:
            raise ProviderContractError(
                "index daily bars cannot use adjusted AkShare history",
                FailureClass.SCHEMA_CHANGED,
                retryable=False,
            )
        return ""
    return {Adjustment.NONE: "", Adjustment.FORWARD: "qfq", Adjustment.BACKWARD: "hfq"}[adjustment]


def _records(frame: Any) -> list[Mapping[str, Any]]:
    if frame is None:
        return []
    to_dict = getattr(frame, "to_dict", None)
    if callable(to_dict):
        try:
            return list(to_dict("records") or [])
        except TypeError:
            return list(to_dict(orient="records") or [])
    if isinstance(frame, Sequence) and not isinstance(frame, (str, bytes, bytearray)):
        return [row for row in frame if isinstance(row, Mapping)]
    return []


def _normalize_row(symbol: str, row: Mapping[str, Any]) -> dict[str, Any]:
    values = {
        "symbol": symbol,
        "trade_date": _value(row, "日期", "date", "datetime"),
        "open": _value(row, "开盘", "open"),
        "high": _value(row, "最高", "high"),
        "low": _value(row, "最低", "low"),
        "close": _value(row, "收盘", "close"),
        "volume": _value(row, "成交量", "volume"),
        "amount": _value(row, "成交额", "amount"),
    }
    if values["trade_date"] is None or any(values[name] is None for name in ("open", "high", "low", "close")):
        raise ProviderContractError(
            f"AkShare daily row missing required fields for {symbol}",
            FailureClass.SCHEMA_CHANGED,
            retryable=False,
        )
    values["trade_date"] = str(values["trade_date"])[:10].replace("/", "-")
    return values


def _value(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return None
