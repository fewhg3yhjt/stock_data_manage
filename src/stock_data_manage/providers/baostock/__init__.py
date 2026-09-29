"""BaoStock SDK adapters."""

from .daily import BaoStockDailyProvider
from .minute import BaoStockMinuteProvider

__all__ = ["BaoStockDailyProvider", "BaoStockMinuteProvider"]
