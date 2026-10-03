from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from ...domain import QualityStatus
from ...routing.capabilities import ProviderCapability
from ..base import FetchResult
from ..contracts import EndpointContract
from ..transport import HttpTransport, SnapshotFetchResponse, parse_tencent_snapshot_line


@dataclass(slots=True)
class TencentSnapshotProvider:
    transport: HttpTransport
    capability: ProviderCapability
    name: str = "tencent"
    endpoint: str = "bulk_snapshot"
    timeout_seconds: float = 10.0
    max_symbols_per_request: int = 100
    url: str = "https://qt.gtimg.cn/q="

    @property
    def capability_version(self) -> str:
        return self.capability.version

    @property
    def capability_priority(self) -> int:
        return self.capability.priority

    @property
    def quality_status(self) -> QualityStatus:
        return QualityStatus.PROVISIONAL

    source_method: str = "snapshot"

    def fetch_snapshot(self, symbols: Sequence[str], as_of: datetime) -> SnapshotFetchResponse:
        requested = tuple(symbols)
        if self.max_symbols_per_request < 1:
            raise ValueError("snapshot batch size must be positive")
        if not requested or len(set(requested)) != len(requested):
            raise ValueError("snapshot requires a non-empty, unique security scope")
        rows: list[dict[str, Any]] = []
        for offset in range(0, len(requested), max(1, self.max_symbols_per_request)):
            batch = requested[offset : offset + self.max_symbols_per_request]
            response = self.transport.get(self.url + ",".join(batch), params={}, timeout_seconds=self.timeout_seconds)
            if response.status_code >= 400:
                EndpointContract(frozenset()).parse_json(response)
            # The archived successful quote probe explicitly decodes Tencent bytes as GBK.
            for line in response.body.decode("gbk").splitlines():
                parsed = parse_tencent_snapshot_line(line)
                if parsed is not None and parsed["symbol"] in batch:
                    rows.append(parsed)
        return SnapshotFetchResponse(tuple(rows), requested)

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        response = self.fetch_snapshot(symbols, datetime(trade_date.year, trade_date.month, trade_date.day, 15, 16))
        rows = tuple(row for row in response.rows if row.get("trade_date") == trade_date.isoformat())
        return FetchResult(rows, response.requested_symbols)
