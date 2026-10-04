from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from ..domain import Adjustment, AssetType, BarRecord, Dataset, Exchange, QualityStatus


SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class SnapshotAggregationResult:
    bars: tuple[BarRecord, ...]
    rejected_snapshots: int


class SnapshotMinuteAggregator:
    def __init__(self, *, min_samples: int = 2) -> None:
        self.min_samples = min_samples

    def aggregate(
        self,
        snapshots: Iterable[dict[str, Any]],
        *,
        instrument_id_by_symbol: dict[str, str],
        exchange: Exchange,
        asset_type: AssetType,
        trade_date: date,
        provider: str,
        endpoint: str,
        adjustment: Adjustment = Adjustment.NONE,
        capability_priority: int = 0,
        capability_version: str = "unknown",
        raw_object_path: str = "snapshot",
        fetch_time: datetime | None = None,
    ) -> SnapshotAggregationResult:
        grouped: dict[tuple[str, datetime], list[dict[str, Any]]] = {}
        rejected = 0
        for snapshot in snapshots:
            try:
                symbol = str(snapshot["symbol"])
                timestamp = _as_datetime(snapshot["timestamp"])
                price = Decimal(str(snapshot["price"]))
                cum_volume = Decimal(str(snapshot["cum_volume"]))
                cum_amount = Decimal(str(snapshot["cum_amount"]))
                if symbol not in instrument_id_by_symbol or timestamp.date() != trade_date:
                    raise ValueError("snapshot is outside requested instrument/date")
                minute = timestamp.replace(second=0, microsecond=0)
                grouped.setdefault((symbol, minute), []).append(
                    {
                        "timestamp": timestamp,
                        "price": price,
                        "cum_volume": cum_volume,
                        "cum_amount": cum_amount,
                    }
                )
            except (KeyError, TypeError, ValueError):
                rejected += 1

        by_symbol: dict[str, list[tuple[datetime, list[dict[str, Any]]]]] = {}
        for (symbol, minute), values in grouped.items():
            by_symbol.setdefault(symbol, []).append((minute, sorted(values, key=lambda item: item["timestamp"])))

        bars: list[BarRecord] = []
        fetch_time = fetch_time or datetime.now(SHANGHAI)
        for symbol, minutes in by_symbol.items():
            minutes.sort(key=lambda item: item[0])
            previous_volume: Decimal | None = None
            previous_amount: Decimal | None = None
            for minute, values in minutes:
                last = values[-1]
                flags: list[str] = []
                volume: Decimal | None = None
                amount: Decimal | None = None
                if previous_volume is not None:
                    volume = last["cum_volume"] - previous_volume
                    amount = last["cum_amount"] - (previous_amount or Decimal(0))
                    if volume < 0 or amount < 0:
                        volume = None
                        amount = None
                        flags.append("cumulative_reset")
                else:
                    flags.append("missing_cumulative_baseline")
                previous_volume = last["cum_volume"]
                previous_amount = last["cum_amount"]
                if len(values) < self.min_samples:
                    flags.append("incomplete_sampling")
                bars.append(
                    BarRecord(
                        dataset=Dataset.MINUTE_BAR_1M,
                        instrument_id=instrument_id_by_symbol[symbol],
                        trade_date=trade_date,
                        bar_time=minute.replace(tzinfo=SHANGHAI),
                        interval_minutes=1,
                        adjustment=adjustment,
                        open=values[0]["price"],
                        high=max(value["price"] for value in values),
                        low=min(value["price"] for value in values),
                        close=last["price"],
                        volume=volume,
                        amount=amount,
                        source_provider=provider,
                        endpoint=endpoint,
                        source_method="snapshot",
                        quality_status=QualityStatus.PROVISIONAL,
                        field_level="ohlcv",
                        capability_priority=capability_priority,
                        capability_version=capability_version,
                        raw_object_path=raw_object_path,
                        fetch_time=fetch_time,
                        source_symbol=symbol,
                        volume_semantics="interval",
                        freshness_class="provisional",
                        as_of=last["timestamp"],
                        flags=tuple(flags),
                    )
                )
        return SnapshotAggregationResult(tuple(sorted(bars, key=lambda bar: bar.key)), rejected)


def _as_datetime(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=SHANGHAI)
    return parsed.astimezone(SHANGHAI)
