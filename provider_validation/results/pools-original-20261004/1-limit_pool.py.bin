from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from ..akshare.session import load_client
from ..contracts import InputFetchResult, ProviderContractError, FailureClass


@dataclass(slots=True)
class EastMoneyLimitUpProvider:
    """Retain the verified stock_zt_pool_em call and its source column names."""
    client: Any | None = None
    name: str = "eastmoney"
    endpoint: str = "limit_up_pool"
    capability_version: str = "eastmoney-limit-up-input-v1"

    def fetch(self, date: date) -> InputFetchResult:
        client = self.client or load_client()
        frame = client.stock_zt_pool_em(date=date.strftime("%Y%m%d"))
        rows = tuple(frame.to_dict(orient="records"))
        if rows and any("代码" not in row or "最新价" not in row for row in rows):
            raise ProviderContractError("limit-up pool source columns changed", FailureClass.SCHEMA_CHANGED, retryable=False)
        # The SDK returns empty for both no pool and missing upstream data; cannot certify a valid empty.
        return InputFetchResult(rows, "https://push2ex.eastmoney.com/getTopicZTPool", empty_is_valid=False)
