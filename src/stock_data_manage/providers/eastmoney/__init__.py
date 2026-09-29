"""EastMoney low-frequency adapters."""

from .dividend import EastMoneyDividendProvider, DividendFetchResult
from .security_list import EastMoneySecurityListProvider

__all__ = ["DividendFetchResult", "EastMoneyDividendProvider", "EastMoneySecurityListProvider"]
