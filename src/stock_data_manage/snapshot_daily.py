from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Callable, Mapping

from .domain import Adjustment, AssetType, BarRecord, Dataset, Exchange, QualityStatus
from .normalizer import Normalizer, NormalizationError


@dataclass(frozen=True, slots=True)
class SnapshotDailyResult:
    records: tuple[BarRecord, ...]
    rejected_symbols: tuple[str, ...]


class SnapshotDailyBuilder:
    """Turn a post-close snapshot into provisional daily bars.

    The builder deliberately delegates unit conversion and timestamp semantics
    to ``Normalizer``. A snapshot is never promoted to final quality here.
    """

    def __init__(
        self,
        normalizer: Normalizer,
        instrument_lookup: Callable[[str], tuple[str, Exchange, AssetType] | None],
    ) -> None:
        self.normalizer = normalizer
        self.instrument_lookup = instrument_lookup

    def build(
        self,
        snapshots: list[Mapping[str, object]],
        *,
        trade_date: date,
        provider: str,
        endpoint: str,
        capability_priority: int,
        capability_version: str,
        raw_object_path: str,
        fetch_time: datetime,
        adjustment: Adjustment = Adjustment.NONE,
    ) -> SnapshotDailyResult:
        records: list[BarRecord] = []
        rejected: list[str] = []
        for snapshot in snapshots:
            symbol = str(snapshot.get("symbol") or "")
            identity = self.instrument_lookup(symbol)
            if not symbol or identity is None:
                rejected.append(symbol or "<missing-symbol>")
                continue
            if str(snapshot.get("trade_date")) != trade_date.isoformat():
                rejected.append(symbol)
                continue
            if any(snapshot.get(field) in (None, "", "-") for field in ("open", "high", "low", "close")):
                rejected.append(symbol)
                continue
            instrument_id, exchange, asset_type = identity
            raw = dict(snapshot)
            raw.setdefault("trade_date", trade_date.isoformat())
            try:
                records.append(
                    self.normalizer.normalize_bar(
                        raw,
                        dataset=Dataset.DAILY_BAR,
                        instrument_id=instrument_id,
                        exchange=exchange,
                        asset_type=asset_type,
                        provider=provider,
                        endpoint=endpoint,
                        adjustment=adjustment,
                        capability_priority=capability_priority,
                        capability_version=capability_version,
                        raw_object_path=raw_object_path,
                        fetch_time=fetch_time,
                        quality_status=QualityStatus.PROVISIONAL,
                        source_method="snapshot",
                    )
                )
            except (NormalizationError, KeyError, TypeError, ValueError):
                rejected.append(symbol)
        return SnapshotDailyResult(tuple(records), tuple(dict.fromkeys(rejected)))
