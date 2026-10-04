from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any
from contextlib import contextmanager

from ..akshare.session import load_client
from ..contracts import InputFetchResult, ProviderContractError, FailureClass


@dataclass(slots=True)
class EastMoneyLimitUpProvider:
    """Retain the verified stock_zt_pool_em call and its source column names."""
    client: Any | None = None
    name: str = "eastmoney"
    endpoint: str = "limit_up_pool"
    capability_version: str = "eastmoney-limit-up-input-v1"

    def fetch(self, date: date, *, source_payloads=None) -> InputFetchResult:
        client = self.client or load_client()
        functions = {"limit_up_pool": ("stock_zt_pool_em", "getTopicZTPool"),
                     "broken_limit_pool": ("stock_zt_pool_zbgc_em", "getTopicZBPool"),
                     "limit_down_pool": ("stock_zt_pool_dtgc_em", "getTopicDTPool"),
                     "previous_limit_pool": ("stock_zt_pool_previous_em", "getYesterdayZTPool"),
                     "strong_stock_pool": ("stock_zt_pool_strong_em", "getTopicQSPool")}
        if self.endpoint not in functions:
            raise ValueError("unsupported stock pool endpoint")
        function, path = functions[self.endpoint]
        frame = getattr(client, function)(date=date.strftime("%Y%m%d"))
        context = {}
        if self.endpoint != "limit_up_pool" or source_payloads is not None:
            if source_payloads is None:
                raise ValueError("retained source responses required for stock-pool validation")
            payloads = tuple(source_payloads())
            if not payloads:
                raise ValueError("stock-pool response evidence is missing")
            for payload in payloads:
                data = payload.get("data") if isinstance(payload, dict) else None
                if not isinstance(payload, dict) or payload.get("rc") != 0:
                    raise ProviderContractError("stock-pool business response failed", FailureClass.SCHEMA_CHANGED, retryable=False)
                if data is None:
                    raise ProviderContractError("stock-pool data unavailable", FailureClass.TEMPORARY_EMPTY, retryable=False)
                if not isinstance(data, dict) or not isinstance(data.get("pool"), list):
                    raise ProviderContractError("stock-pool envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
                if str(data.get("qdate")) != date.strftime("%Y%m%d"):
                    raise ProviderContractError("stock-pool returned date disagrees with request", FailureClass.SCHEMA_CHANGED, retryable=False)
                if type(data.get("tc")) is not int or data["tc"] < 0:
                    raise ProviderContractError("stock-pool source total changed", FailureClass.SCHEMA_CHANGED, retryable=False)
                if len(data["pool"]) != data["tc"] or len(frame) != data["tc"]:
                    raise ProviderContractError("stock-pool response truncated", FailureClass.TRUNCATED, retryable=False)
                if not data["pool"]:
                    raise ProviderContractError("empty stock pool is not certified valid", FailureClass.TEMPORARY_EMPTY, retryable=False)
                if any(not isinstance(row, dict) or not isinstance(row.get("c"), str) or
                       len(row["c"]) != 6 or not row["c"].isdigit() for row in data["pool"]):
                    raise ProviderContractError("stock-pool security code changed", FailureClass.SCHEMA_CHANGED, retryable=False)
                if [str(value) for value in frame["代码"]] != [row["c"] for row in data["pool"]]:
                    raise ProviderContractError("stock-pool SDK security identity changed", FailureClass.SCHEMA_CHANGED, retryable=False)
                context.update(source_quote_date=date, source_total_count=data["tc"])
            import pandas as pd
            frame = frame.astype(object).where(pd.notna(frame), None)
        rows = tuple(frame.to_dict(orient="records"))
        if rows and any("代码" not in row or "最新价" not in row for row in rows):
            raise ProviderContractError("limit-up pool source columns changed", FailureClass.SCHEMA_CHANGED, retryable=False)
        # The SDK returns empty for both no pool and missing upstream data; cannot certify a valid empty.
        return InputFetchResult(rows, "https://push2ex.eastmoney.com/" + path,
                                empty_is_valid=False, mapping_context=context)


@contextmanager
def pool_replay_clock(function, manifest, endpoint):
    """The SDK's recent-history guard uses original capture time only during replay."""
    import json
    from pathlib import Path
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from unittest.mock import patch
    from urllib.parse import urlsplit
    matches = [json.loads(line) for line in Path(manifest).read_text(encoding="utf-8").splitlines()
               if urlsplit(json.loads(line).get("url", "")).path == "/"+endpoint]
    if not matches:
        raise ValueError("no archived pool request; replay never falls back to network")
    captured = datetime.fromisoformat(matches[-1]["fetched_at_utc"])
    if captured.tzinfo is None:
        raise ValueError("pool source capture time must be timezone-aware")
    class ReplayDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return captured.astimezone(tz) if tz else captured.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    namespace = getattr(function, "__globals__", {})
    with patch.dict(namespace, {"datetime": ReplayDateTime}):
        yield
