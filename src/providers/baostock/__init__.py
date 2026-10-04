"""BaoStock SDK adapters."""

from .daily import BaoStockDailyProvider
from .industry import BaoStockIndustryMembershipProvider, BaoStockIndustryResult
from .minute import BaoStockMinuteProvider

__all__ = [
    "BaoStockDailyProvider",
    "BaoStockIndustryMembershipProvider",
    "BaoStockIndustryResult",
    "BaoStockMinuteProvider",
]
