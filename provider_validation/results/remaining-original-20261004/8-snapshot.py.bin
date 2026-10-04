from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence
import re

from ...domain import QualityStatus
from ...routing.capabilities import ProviderCapability
from ..base import FetchResult
from ..contracts import EndpointContract, FailureClass, ProviderContractError
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

    @property
    def input_hosts(self) -> tuple[str, ...]:
        return (self.url,)

    def fetch_snapshot(self, symbols: Sequence[str], as_of: datetime) -> SnapshotFetchResponse:
        requested = tuple(symbols)
        if self.max_symbols_per_request < 1:
            raise ValueError("snapshot batch size must be positive")
        if not requested or len(set(requested)) != len(requested):
            raise ValueError("snapshot requires a non-empty, unique security scope")
        if any(not isinstance(symbol, str) or not re.fullmatch(r"(?:sh|sz|bj)\d{6}", symbol) for symbol in requested):
            raise ValueError("snapshot requires explicit canonical sh/sz/bj security codes")
        rows: list[dict[str, Any]] = []
        source_rows: list[dict[str, Any]] = []
        seen = set()
        for offset in range(0, len(requested), max(1, self.max_symbols_per_request)):
            batch = requested[offset : offset + self.max_symbols_per_request]
            response = self.transport.get(self.url + ",".join(batch), params={}, timeout_seconds=self.timeout_seconds)
            if response.status_code >= 400:
                EndpointContract(frozenset()).parse_json(response)
            # The archived successful quote probe explicitly decodes Tencent bytes as GBK.
            try:
                lines = response.body.decode("gbk").splitlines()
                for line in lines:
                    if not line.strip():
                        continue
                    match = re.fullmatch(r'\s*v_([^=]+)="(.*?)";?\s*', line)
                    if match is None:
                        raise ValueError("quote response envelope changed")
                    symbol, payload = match.groups()
                    if symbol not in batch:
                        raise ValueError("quote returned an unrequested security")
                    if symbol in seen:
                        raise ValueError("duplicate quote security")
                    seen.add(symbol)
                    if not payload or payload == "1":
                        continue  # Explicit no-quote marker, retained in response bytes.
                    parts = payload.split("~")
                    if len(parts) < 35 or parts[2] != symbol[2:]:
                        raise ValueError("quote fields or security identity changed")
                    if not re.fullmatch(r"\d{14}", parts[30]):
                        raise ValueError("quote timestamp format changed")
                    parsed = parse_tencent_snapshot_line(line)
                    if parsed is None:
                        raise ValueError("quote timestamp is missing")
                    raw = {name: parts[index] for name, index in (
                        ("name", 1), ("code", 2), ("price", 3), ("prev_close", 4), ("open", 5),
                        ("volume_lot", 6), ("datetime", 30), ("change", 31), ("change_pct", 32), ("high", 33), ("low", 34))}
                    exchange = {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[symbol[:2]]
                    raw.update(raw=payload, symbol=symbol,
                               instrument_id=f"{exchange}:{symbol[2:]}",
                               source_amount_37=parts[37] if len(parts) > 37 else None)
                    rows.append(parsed)
                    source_rows.append(raw)
            except (UnicodeDecodeError, ValueError) as exc:
                raise ProviderContractError(str(exc), FailureClass.SCHEMA_CHANGED, retryable=False) from exc
        if not rows:
            raise ProviderContractError("snapshot has no valid quotes", FailureClass.TEMPORARY_EMPTY, retryable=True)
        return SnapshotFetchResponse(tuple(rows), requested, tuple(source_rows), self.url)

    def fetch_daily(self, symbols: Sequence[str], trade_date: date) -> FetchResult:
        response = self.fetch_snapshot(symbols, datetime(trade_date.year, trade_date.month, trade_date.day, 15, 16))
        rows = tuple(row for row in response.rows if row.get("trade_date") == trade_date.isoformat())
        return FetchResult(rows, response.requested_symbols)
