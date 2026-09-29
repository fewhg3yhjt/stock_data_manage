"""EastMoney low-frequency adapters."""

from .dividend import EastMoneyDividendProvider, DividendFetchResult
from .security_list import EastMoneySecurityListProvider
from .realtime import EastMoneyRealtimeQuoteProvider, IntradayTrendFetchResult, RealtimeQuoteFetchResult
from .fund_flow import EastMoneyStockFundFlowProvider, StockFundFlowFetchResult

__all__ = [
    "DividendFetchResult", "EastMoneyDividendProvider", "EastMoneySecurityListProvider",
    "EastMoneyRealtimeQuoteProvider", "IntradayTrendFetchResult", "RealtimeQuoteFetchResult",
    "EastMoneyStockFundFlowProvider", "StockFundFlowFetchResult",
]
