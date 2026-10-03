from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..contracts import EndpointContract, FailureClass, ProviderContractError
from .realtime import EastMoneyRequestsTransport


@dataclass(frozen=True, slots=True)
class StockFundFlowRecord:
    symbol: str
    trade_date: str
    main_net_inflow: float | None
    super_large_net_inflow: float | None
    large_net_inflow: float | None
    medium_net_inflow: float | None
    small_net_inflow: float | None
    main_net_inflow_pct: float | None
    super_large_net_inflow_pct: float | None
    large_net_inflow_pct: float | None
    medium_net_inflow_pct: float | None
    small_net_inflow_pct: float | None
    close: float | None
    change_pct: float | None


@dataclass(frozen=True, slots=True)
class StockFundFlowFetchResult:
    records: tuple[StockFundFlowRecord, ...]
    requested_symbols: tuple[str, ...]
    response_status: int


@dataclass(slots=True)
class EastMoneyStockFundFlowProvider:
    transport: Any | None = None
    endpoint: str = "stock_fund_flow"
    name: str = "eastmoney"
    capability_version: str = "eastmoney-stock-fund-flow-v1"
    timeout_seconds: float = 20.0
    limit: int = 20
    url: str = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    ut: str = "fa5fd1943c7b386f172d6893dbbd1d0c"
    client: Any | None = None

    def fetch_history(self, code: str, *, source_payloads):
        from ..akshare.session import load_client
        from .realtime import history_stock_identity, history_input_result
        bare, market, _ = history_stock_identity(code)
        frame = (self.client or load_client()).stock_individual_fund_flow(stock=bare, market=market)
        return history_input_result(frame, code=code, source_payloads=source_payloads, source_url=self.url, fund_flow=True,
            required_fields=("日期", "收盘价", "涨跌幅", "主力净流入-净额", "超大单净流入-净额", "大单净流入-净额",
                             "中单净流入-净额", "小单净流入-净额"))

    def __post_init__(self) -> None:
        if self.transport is None:
            self.transport = EastMoneyRequestsTransport()

    def fetch_daily(self, symbols: Sequence[str]) -> StockFundFlowFetchResult:
        requested = tuple(symbols)
        records: list[StockFundFlowRecord] = []
        response_status = 200
        for symbol in requested:
            response = self.transport.get(
                self.url,
                params={
                    "secid": _secid(symbol),
                    "lmt": str(self.limit),
                    "klt": "101",
                    "fields1": "f1,f2,f3,f7",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63",
                    "ut": self.ut,
                },
                timeout_seconds=self.timeout_seconds,
            )
            response_status = response.status_code
            payload = EndpointContract(frozenset()).parse_json(response)
            data = payload.get("data") if isinstance(payload, dict) else None
            rows = data.get("klines") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                raise ProviderContractError(
                    "EastMoney stock fund-flow rows changed",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            for row in rows:
                if not isinstance(row, str):
                    continue
                values = row.split(",")
                if len(values) < 13:
                    continue
                records.append(StockFundFlowRecord(
                    symbol=symbol,
                    trade_date=values[0],
                    main_net_inflow=_number(values[1]),
                    super_large_net_inflow=_number(values[5]),
                    large_net_inflow=_number(values[4]),
                    medium_net_inflow=_number(values[3]),
                    small_net_inflow=_number(values[2]),
                    main_net_inflow_pct=_number(values[6]),
                    super_large_net_inflow_pct=_number(values[10]),
                    large_net_inflow_pct=_number(values[9]),
                    medium_net_inflow_pct=_number(values[8]),
                    small_net_inflow_pct=_number(values[7]),
                    close=_number(values[11]),
                    change_pct=_number(values[12]),
                ))
        return StockFundFlowFetchResult(tuple(records), requested, response_status)


def _secid(symbol: str) -> str:
    text = str(symbol).lower()
    if len(text) < 8 or text[:2] not in {"sh", "sz", "bj"}:
        raise ValueError(f"EastMoney requires exchange-prefixed symbol: {symbol}")
    return f"{'1' if text[:2] == 'sh' else '0'}.{text[2:]}"


def _number(value: object) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
