"""Optional AkShare function-level adapters."""

from .daily import AkShareDailyProvider
from .boards import AkShareBoardProvider, AkShareBoardResult

__all__ = ["AkShareDailyProvider", "AkShareBoardProvider", "AkShareBoardResult"]
