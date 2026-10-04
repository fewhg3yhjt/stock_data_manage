from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Iterable, Mapping

import duckdb

from ..domain import AssetType, Exchange


@dataclass(frozen=True, slots=True)
class SecurityRecord:
    instrument_id: str
    symbol: str
    exchange: Exchange
    asset_type: AssetType
    name: str | None = None
    list_date: date | None = None
    delist_date: date | None = None
    status: str = "active"
    lot_size: int = 100
    canonical_volume_unit: str = "share"
    canonical_amount_unit: str = "CNY"
    publisher: str | None = None
    publish_date: date | None = None
    calculation_start_date: date | None = None
    is_backfilled_history: bool = False
    classification_conflict: bool = False
    evidence_sources: tuple[str, ...] = ()

    def is_expected_on(self, trade_date: date) -> bool:
        return (
            (self.list_date is None or self.list_date <= trade_date)
            and (self.delist_date is None or self.delist_date >= trade_date)
        )


@dataclass(frozen=True, slots=True)
class SymbolHistory:
    instrument_id: str
    provider: str
    symbol: str
    effective_from: date
    effective_to: date | None
    change_reason: str
    evidence_source: str


@dataclass(frozen=True, slots=True)
class ClassificationConflict:
    instrument_id: str
    existing_asset_type: AssetType
    candidate_asset_type: AssetType
    existing_source: str
    candidate_source: str


@dataclass(frozen=True, slots=True)
class SecurityMergeResult:
    records: tuple[SecurityRecord, ...]
    symbol_history: tuple[SymbolHistory, ...]
    classification_conflicts: tuple[ClassificationConflict, ...]
    missing_sources: tuple[str, ...]


def stable_instrument_id(exchange: Exchange, symbol: str) -> str:
    prefix = {Exchange.XSHG: "sh", Exchange.XSHE: "sz", Exchange.BSE: "bj"}[exchange]
    return f"{exchange.value}:{symbol.removeprefix(prefix)}"


def merge_security_sources(
    previous: Iterable[SecurityRecord],
    sources: Mapping[str, Iterable[SecurityRecord]],
    *,
    official_sources: frozenset[str] = frozenset({"exchange", "official_publisher"}),
) -> SecurityMergeResult:
    merged = {record.instrument_id: record for record in previous}
    source_of = {
        record.instrument_id: (record.evidence_sources[0] if record.evidence_sources else "previous")
        for record in merged.values()
    }
    aliases: list[SymbolHistory] = []
    conflicts: list[ClassificationConflict] = []
    missing: list[str] = []
    for source, candidates_iter in sources.items():
        candidates = tuple(candidates_iter)
        if not candidates:
            missing.append(source)
            continue
        for candidate in candidates:
            candidate = replace(
                candidate,
                evidence_sources=tuple(dict.fromkeys((*candidate.evidence_sources, source))),
            )
            existing = merged.get(candidate.instrument_id)
            if existing is None:
                merged[candidate.instrument_id] = candidate
                source_of[candidate.instrument_id] = source
                continue
            if existing.asset_type != candidate.asset_type:
                conflicts.append(
                    ClassificationConflict(
                        candidate.instrument_id,
                        existing.asset_type,
                        candidate.asset_type,
                        source_of[candidate.instrument_id],
                        source,
                    )
                )
                existing = replace(existing, classification_conflict=True)
            # A source omission never reaches this branch, so it cannot mark a
            # trusted record delisted. Explicit official status changes may.
            updates = {
                "name": candidate.name or existing.name,
                "list_date": candidate.list_date or existing.list_date,
                "publisher": candidate.publisher or existing.publisher,
                "publish_date": candidate.publish_date or existing.publish_date,
                "calculation_start_date": candidate.calculation_start_date or existing.calculation_start_date,
                "evidence_sources": tuple(dict.fromkeys((*existing.evidence_sources, source))),
            }
            if source in official_sources and candidate.delist_date is not None:
                updates["delist_date"] = candidate.delist_date
            if source in official_sources and candidate.status in {"active", "delisted", "suspended", "prelisted"}:
                updates["status"] = candidate.status
            merged[candidate.instrument_id] = replace(existing, **updates)
            if existing.symbol != candidate.symbol:
                aliases.append(
                    SymbolHistory(
                        candidate.instrument_id,
                        source,
                        existing.symbol,
                        candidate.list_date or date.min,
                        candidate.delist_date,
                        "code_migration",
                        source,
                    )
                )
    return SecurityMergeResult(
        tuple(sorted(merged.values(), key=lambda item: item.instrument_id)),
        tuple(aliases),
        tuple(conflicts),
        tuple(missing),
    )


def expected_security_set(records: Iterable[SecurityRecord], trade_date: date) -> tuple[SecurityRecord, ...]:
    return tuple(sorted((record for record in records if record.is_expected_on(trade_date)), key=lambda item: item.instrument_id))


class SecurityMasterStore(AbstractContextManager["SecurityMasterStore"]):
    def __init__(self, path: str = ":memory:") -> None:
        self.connection = duckdb.connect(path)
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS security_master (
                instrument_id VARCHAR PRIMARY KEY, symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL, asset_type VARCHAR NOT NULL,
                name VARCHAR, list_date DATE, delist_date DATE, status VARCHAR,
                lot_size INTEGER, canonical_volume_unit VARCHAR,
                canonical_amount_unit VARCHAR, publisher VARCHAR,
                publish_date DATE, calculation_start_date DATE,
                is_backfilled_history BOOLEAN, classification_conflict BOOLEAN,
                evidence_sources VARCHAR[] NOT NULL, updated_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS security_symbol_history (
                instrument_id VARCHAR NOT NULL, provider VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL, effective_from DATE NOT NULL,
                effective_to DATE, change_reason VARCHAR NOT NULL,
                evidence_source VARCHAR NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS security_symbol_history_unique
                ON security_symbol_history (instrument_id, provider, symbol, effective_from);
            CREATE TABLE IF NOT EXISTS security_master_history (
                instrument_id VARCHAR NOT NULL, field_name VARCHAR NOT NULL,
                old_value VARCHAR, new_value VARCHAR, effective_date DATE NOT NULL,
                detected_at TIMESTAMPTZ NOT NULL, source VARCHAR NOT NULL
            );
            """
        )

    def upsert(self, records: Iterable[SecurityRecord], aliases: Iterable[SymbolHistory] = ()) -> None:
        now = datetime.now(timezone.utc)
        for record in records:
            self.connection.execute(
                """
                INSERT INTO security_master VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (instrument_id) DO UPDATE SET
                    symbol=excluded.symbol, exchange=excluded.exchange,
                    asset_type=excluded.asset_type, name=excluded.name,
                    list_date=excluded.list_date, delist_date=excluded.delist_date,
                    status=excluded.status, lot_size=excluded.lot_size,
                    canonical_volume_unit=excluded.canonical_volume_unit,
                    canonical_amount_unit=excluded.canonical_amount_unit,
                    publisher=excluded.publisher, publish_date=excluded.publish_date,
                    calculation_start_date=excluded.calculation_start_date,
                    is_backfilled_history=excluded.is_backfilled_history,
                    classification_conflict=excluded.classification_conflict,
                    evidence_sources=excluded.evidence_sources, updated_at=excluded.updated_at
                """,
                [
                    record.instrument_id, record.symbol, record.exchange.value, record.asset_type.value,
                    record.name, record.list_date, record.delist_date, record.status, record.lot_size,
                    record.canonical_volume_unit, record.canonical_amount_unit, record.publisher,
                    record.publish_date, record.calculation_start_date, record.is_backfilled_history,
                    record.classification_conflict, list(record.evidence_sources), now,
                ],
            )
        for alias in aliases:
            self.connection.execute(
                "INSERT OR IGNORE INTO security_symbol_history VALUES (?, ?, ?, ?, ?, ?, ?)",
                [alias.instrument_id, alias.provider, alias.symbol, alias.effective_from, alias.effective_to, alias.change_reason, alias.evidence_source],
            )

    def record_changes(
        self,
        changes: Iterable[tuple[str, str, object, object, date, str]],
    ) -> None:
        detected_at = datetime.now(timezone.utc)
        for instrument_id, field_name, old_value, new_value, effective_date, source in changes:
            self.connection.execute(
                "INSERT INTO security_master_history VALUES (?, ?, ?, ?, ?, ?, ?)",
                [instrument_id, field_name, None if old_value is None else str(old_value), None if new_value is None else str(new_value), effective_date, detected_at, source],
            )

    def all(self) -> tuple[SecurityRecord, ...]:
        rows = self.connection.execute("SELECT * FROM security_master ORDER BY instrument_id").fetchall()
        return tuple(
            SecurityRecord(
                instrument_id=row[0], symbol=row[1], exchange=Exchange(row[2]), asset_type=AssetType(row[3]),
                name=row[4], list_date=row[5], delist_date=row[6], status=row[7], lot_size=row[8],
                canonical_volume_unit=row[9], canonical_amount_unit=row[10], publisher=row[11],
                publish_date=row[12], calculation_start_date=row[13], is_backfilled_history=row[14],
                classification_conflict=row[15], evidence_sources=tuple(row[16] or ()),
            )
            for row in rows
        )

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "SecurityMasterStore":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
