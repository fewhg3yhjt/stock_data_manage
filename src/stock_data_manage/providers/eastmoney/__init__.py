"""EastMoney low-frequency adapters."""

from .dividend import EastMoneyDividendProvider, DividendFetchResult
from .security_list import EastMoneySecurityListProvider
from .realtime import EastMoneyRealtimeQuoteProvider, IntradayTrendFetchResult, RealtimeQuoteFetchResult

__all__ = [
    "DividendFetchResult", "EastMoneyDividendProvider", "EastMoneySecurityListProvider",
    "EastMoneyRealtimeQuoteProvider", "IntradayTrendFetchResult", "RealtimeQuoteFetchResult",
]
