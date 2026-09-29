"""EastMoney low-frequency adapters."""

from .dividend import EastMoneyDividendProvider, DividendFetchResult
from .security_list import EastMoneySecurityListProvider
from .realtime import EastMoneyRealtimeQuoteProvider, IntradayTrendFetchResult, RealtimeQuoteFetchResult
from .fund_flow import EastMoneyStockFundFlowProvider, StockFundFlowFetchResult
from .financial import EastMoneyFinancialMainProvider, FinancialMainFetchResult
from .shareholder import EastMoneyShareholderCountProvider, ShareholderCountFetchResult

__all__ = [
    "DividendFetchResult", "EastMoneyDividendProvider", "EastMoneySecurityListProvider",
    "EastMoneyRealtimeQuoteProvider", "IntradayTrendFetchResult", "RealtimeQuoteFetchResult",
    "EastMoneyStockFundFlowProvider", "StockFundFlowFetchResult",
    "EastMoneyFinancialMainProvider", "FinancialMainFetchResult",
    "EastMoneyShareholderCountProvider", "ShareholderCountFetchResult",
]
