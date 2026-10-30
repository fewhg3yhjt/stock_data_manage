from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from ..domain import Adjustment, BarRecord, Dataset
from ..quality.resolution import Conflict, resolve_records
from ..storage.hot import HotMinuteStore
from ..storage.parquet import CanonicalPartitionStore


@dataclass(frozen=True, slots=True)
class MinuteQueryResult:
    records: tuple[BarRecord, ...]
    conflicts: tuple[Conflict, ...]


class MinuteQuery:
    def __init__(self, *, canonical: CanonicalPartitionStore, hot: HotMinuteStore) -> None:
        self.canonical = canonical
        self.hot = hot

    def query(
        self,
        *,
        instrument_id: str,
        asset_type: str,
        start: datetime,
        end: datetime,
        interval_minutes: int = 1,
        adjustment: Adjustment = Adjustment.NONE,
    ) -> MinuteQueryResult:
        records: list[BarRecord] = []
        current = start.date()
        while current <= end.date():
            records.extend(
                record
                for record in self.canonical.read(
                    Dataset.MINUTE_BAR_1M if interval_minutes == 1 else Dataset.MINUTE_BAR_5M,
                    asset_type,
                    current.isoformat(),
                )
                if record.instrument_id == instrument_id
                and record.bar_time is not None
                and start <= record.bar_time <= end
                and record.interval_minutes == interval_minutes
                and record.adjustment == adjustment
            )
            current += timedelta(days=1)
        records.extend(
            self.hot.query(
                instrument_id,
                start,
                end,
                interval_minutes=interval_minutes,
                adjustment=adjustment,
            )
        )
        grouped: dict[tuple[object, ...], list[BarRecord]] = {}
        for record in records:
            grouped.setdefault(record.key, []).append(record)
        selected: list[BarRecord] = []
        conflicts: list[Conflict] = []
        for candidates in grouped.values():
            result = resolve_records(candidates)
            conflicts.extend(result.conflicts)
            if result.selected is not None:
                selected.append(result.selected)
        return MinuteQueryResult(tuple(sorted(selected, key=lambda record: record.key)), tuple(conflicts))


def query_daily(canonical_root, *, security="", start_date=None, end_date=None, asset_type="", adjustment="none", page=1, page_size=50):
    """Project published aggregate values only. Source-row evidence has a different consumer."""
    from pathlib import Path
    from ..domain import AssetType, QuoteStatus
    from ..storage.parquet import InvalidPartitionError
    if adjustment not in {a.value for a in Adjustment} or asset_type not in {"", *(a.value for a in AssetType)}:
        raise ValueError("unsupported daily asset or adjustment")
    if page < 1 or not 1 <= page_size <= 100 or (start_date and end_date and start_date > end_date):
        raise ValueError("invalid daily page or date range")
    root = Path(canonical_root).resolve()
    store = CanonicalPartitionStore(root)
    items, available = [], set()
    for descriptor in sorted(root.glob("*/daily_bar/*/manifest.json")):
        if not descriptor.resolve().is_relative_to(root) or not (descriptor.parent / "data.parquet").resolve().is_relative_to(root):
            raise InvalidPartitionError("daily partition escaped managed storage")
        day = date.fromisoformat(descriptor.parents[2].name)
        kind = descriptor.parent.name
        if (start_date and day < start_date) or (end_date and day > end_date) or (asset_type and kind != asset_type):
            continue
        for record in store.read_daily(kind, day.isoformat()):
            code = record.instrument_id.split(":")[1]
            symbol = {"XSHG":"sh", "XSHE":"sz", "BSE":"bj"}[record.instrument_id.split(":")[0]] + code
            if security and security.lower() not in {code, symbol, record.instrument_id.lower()}:
                continue
            available.update(name for name,q in record.quotes.items() if q.status is QuoteStatus.AVAILABLE)
            quote = record.quotes[adjustment]
            items.append({"instrument_id": record.instrument_id, "symbol": symbol, "trade_date": day.isoformat(),
                "asset_type": record.asset_type.value, "value_kind": record.value_kind, "quote_unit": record.quote_unit,
                "adjustment": adjustment, "status": quote.status.value, "reason": quote.reason,
                "values": {name: str(getattr(quote,name)) if getattr(quote,name) is not None else None
                           for name in ("open","high","low","close","volume","amount")},
                "provider": quote.source_provider, "has_source_evidence": bool(quote.raw_object_path),
                "flags": list(quote.flags),
                "quote_states": {name:q.status.value for name,q in record.quotes.items()}})
    start = (page-1)*page_size
    return {"items": items[start:start+page_size], "total": len(items), "page": page, "page_size": page_size,
        "pages": max(1,(len(items)+page_size-1)//page_size), "adjustment": adjustment,
        "available_adjustments": [a.value for a in Adjustment if a.value in available]}
