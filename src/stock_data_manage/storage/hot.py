from __future__ import annotations

import sqlite3
from contextlib import AbstractContextManager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from ..domain import Adjustment, BarRecord, Dataset, QualityStatus
from ..quality.resolution import FIELD_LEVEL_RANK, METHOD_RANK, QUALITY_RANK


class HotMinuteStore(AbstractContextManager["HotMinuteStore"]):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS hot_minute_bar (
                instrument_id TEXT NOT NULL,
                source_symbol TEXT,
                trade_date TEXT NOT NULL,
                bar_time TEXT NOT NULL,
                interval_minutes INTEGER NOT NULL,
                adjustment TEXT NOT NULL,
                open TEXT NOT NULL,
                high TEXT NOT NULL,
                low TEXT NOT NULL,
                close TEXT NOT NULL,
                volume TEXT,
                amount TEXT,
                source_provider TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                source_method TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                field_level TEXT NOT NULL,
                capability_priority INTEGER NOT NULL,
                capability_version TEXT NOT NULL,
                raw_object_path TEXT NOT NULL,
                fetch_time TEXT NOT NULL,
                volume_semantics TEXT,
                freshness_class TEXT,
                source_delay_seconds INTEGER,
                as_of TEXT,
                normalizer_version TEXT NOT NULL,
                resolution_policy_version TEXT NOT NULL,
                flags TEXT NOT NULL,
                resolution_rank TEXT NOT NULL,
                PRIMARY KEY (instrument_id, bar_time, interval_minutes, adjustment)
            )
            """
        )
        self.connection.commit()

    def upsert(self, record: BarRecord) -> bool:
        if record.dataset not in {Dataset.MINUTE_BAR_1M, Dataset.MINUTE_BAR_5M}:
            raise ValueError("hot store accepts minute bars only")
        if record.bar_time is None or record.interval_minutes is None:
            raise ValueError("minute key is incomplete")
        rank = _storage_rank(record)
        existing = self.connection.execute(
            """SELECT resolution_rank FROM hot_minute_bar
               WHERE instrument_id=? AND bar_time=? AND interval_minutes=? AND adjustment=?""",
            (record.instrument_id, record.bar_time.isoformat(), record.interval_minutes, record.adjustment.value),
        ).fetchone()
        if existing is not None and existing["resolution_rank"] > rank:
            return False
        values = _record_values(record, rank)
        placeholders = ",".join("?" for _ in values)
        columns = ",".join(values)
        updates = ",".join(
            f"{column}=excluded.{column}"
            for column in values
            if column not in {"instrument_id", "bar_time", "interval_minutes", "adjustment"}
        )
        self.connection.execute(
            f"INSERT INTO hot_minute_bar ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT(instrument_id,bar_time,interval_minutes,adjustment) DO UPDATE SET {updates}",
            tuple(values.values()),
        )
        self.connection.commit()
        return True

    def query(
        self,
        instrument_id: str,
        start: datetime,
        end: datetime,
        *,
        interval_minutes: int = 1,
        adjustment: Adjustment = Adjustment.NONE,
    ) -> list[BarRecord]:
        rows = self.connection.execute(
            """SELECT * FROM hot_minute_bar
               WHERE instrument_id=? AND bar_time>=? AND bar_time<=?
                 AND interval_minutes=? AND adjustment=?
               ORDER BY bar_time""",
            (instrument_id, start.isoformat(), end.isoformat(), interval_minutes, adjustment.value),
        ).fetchall()
        return [_row_to_record(row) for row in rows]

    def close(self) -> None:
        self.connection.close()

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def _storage_rank(record: BarRecord) -> str:
    # Fixed-width text preserves lexicographic ordering in SQLite.
    return ":".join(
        [
            f"{QUALITY_RANK[record.quality_status]:02d}",
            f"{FIELD_LEVEL_RANK.get(record.field_level, 0):02d}",
            f"{METHOD_RANK.get(record.source_method, 0):02d}",
            f"{record.capability_priority:08d}",
            record.source_provider,
            record.endpoint,
            record.fetch_time.isoformat(),
        ]
    )


def _record_values(record: BarRecord, rank: str) -> dict[str, object]:
    assert record.bar_time is not None and record.interval_minutes is not None
    return {
        "instrument_id": record.instrument_id,
        "source_symbol": record.source_symbol,
        "trade_date": record.trade_date.isoformat(),
        "bar_time": record.bar_time.isoformat(),
        "interval_minutes": record.interval_minutes,
        "adjustment": record.adjustment.value,
        "open": str(record.open),
        "high": str(record.high),
        "low": str(record.low),
        "close": str(record.close),
        "volume": None if record.volume is None else str(record.volume),
        "amount": None if record.amount is None else str(record.amount),
        "source_provider": record.source_provider,
        "endpoint": record.endpoint,
        "source_method": record.source_method,
        "quality_status": record.quality_status.value,
        "field_level": record.field_level,
        "capability_priority": record.capability_priority,
        "capability_version": record.capability_version,
        "raw_object_path": record.raw_object_path,
        "fetch_time": record.fetch_time.isoformat(),
        "volume_semantics": record.volume_semantics,
        "freshness_class": record.freshness_class,
        "source_delay_seconds": record.source_delay_seconds,
        "as_of": None if record.as_of is None else record.as_of.isoformat(),
        "normalizer_version": record.normalizer_version,
        "resolution_policy_version": record.resolution_policy_version,
        "flags": "|".join(record.flags),
        "resolution_rank": rank,
    }


def _row_to_record(row: sqlite3.Row) -> BarRecord:
    return BarRecord(
        dataset=Dataset.MINUTE_BAR_1M if row["interval_minutes"] == 1 else Dataset.MINUTE_BAR_5M,
        instrument_id=row["instrument_id"],
        source_symbol=row["source_symbol"],
        trade_date=date.fromisoformat(row["trade_date"]),
        bar_time=datetime.fromisoformat(row["bar_time"]),
        interval_minutes=row["interval_minutes"],
        adjustment=Adjustment(row["adjustment"]),
        open=Decimal(row["open"]),
        high=Decimal(row["high"]),
        low=Decimal(row["low"]),
        close=Decimal(row["close"]),
        volume=None if row["volume"] is None else Decimal(row["volume"]),
        amount=None if row["amount"] is None else Decimal(row["amount"]),
        source_provider=row["source_provider"],
        endpoint=row["endpoint"],
        source_method=row["source_method"],
        quality_status=QualityStatus(row["quality_status"]),
        field_level=row["field_level"],
        capability_priority=row["capability_priority"],
        capability_version=row["capability_version"],
        raw_object_path=row["raw_object_path"],
        fetch_time=datetime.fromisoformat(row["fetch_time"]),
        volume_semantics=row["volume_semantics"],
        freshness_class=row["freshness_class"],
        source_delay_seconds=row["source_delay_seconds"],
        as_of=None if row["as_of"] is None else datetime.fromisoformat(row["as_of"]),
        normalizer_version=row["normalizer_version"],
        resolution_policy_version=row["resolution_policy_version"],
        flags=tuple(filter(None, row["flags"].split("|"))),
    )
