from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
import re
from typing import Iterable, Mapping

import duckdb

from ..domain import AssetType, Exchange
from ..storage.integrity import Manifest
from ..storage.parquet import InvalidPartitionError, PartitionBusyError, PartitionLock


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


def security_record_to_mapping(record: SecurityRecord) -> dict[str, object]:
    return {
        "instrument_id": record.instrument_id,
        "symbol": record.symbol,
        "exchange": record.exchange.value,
        "asset_type": record.asset_type.value,
        "name": record.name,
        "list_date": record.list_date.isoformat() if record.list_date else None,
        "delist_date": record.delist_date.isoformat() if record.delist_date else None,
        "status": record.status,
        "lot_size": record.lot_size,
        "canonical_volume_unit": record.canonical_volume_unit,
        "canonical_amount_unit": record.canonical_amount_unit,
        "publisher": record.publisher,
        "publish_date": record.publish_date.isoformat() if record.publish_date else None,
        "calculation_start_date": (record.calculation_start_date.isoformat()
                                    if record.calculation_start_date else None),
        "is_backfilled_history": record.is_backfilled_history,
        "classification_conflict": record.classification_conflict,
        "evidence_sources": list(record.evidence_sources),
    }


def security_record_from_mapping(row: Mapping[str, object]) -> SecurityRecord:
    def as_date(value):
        if value is None or value == "":
            return None
        return value if isinstance(value, date) else date.fromisoformat(str(value))

    return SecurityRecord(
        instrument_id=str(row["instrument_id"]),
        symbol=str(row["symbol"]),
        exchange=Exchange(str(row["exchange"])),
        asset_type=AssetType(str(row["asset_type"])),
        name=None if row.get("name") is None else str(row["name"]),
        list_date=as_date(row.get("list_date")),
        delist_date=as_date(row.get("delist_date")),
        status=str(row.get("status") or "unknown"),
        lot_size=int(row.get("lot_size") or (1 if str(row.get("asset_type")) == "index" else 100)),
        canonical_volume_unit=str(row.get("canonical_volume_unit") or "share"),
        canonical_amount_unit=str(row.get("canonical_amount_unit") or "CNY"),
        publisher=None if row.get("publisher") is None else str(row["publisher"]),
        publish_date=as_date(row.get("publish_date")),
        calculation_start_date=as_date(row.get("calculation_start_date")),
        is_backfilled_history=bool(row.get("is_backfilled_history", False)),
        classification_conflict=bool(row.get("classification_conflict", False)),
        evidence_sources=tuple(str(item) for item in (row.get("evidence_sources") or ())),
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


def normalize_symbol_successions(relations: Iterable[Mapping[str, object]]) -> tuple[dict[str, object], ...]:
    """Validate the small, explicit old-code to current-code list in security_master.yaml."""
    result = []
    old_keys, new_keys = set(), set()
    for relation in relations:
        if not isinstance(relation, Mapping):
            raise ValueError("symbol succession must be a mapping")
        market = Exchange(str(relation.get("market", "")).upper()).value
        asset_type = AssetType(str(relation.get("asset_type", "")).lower()).value
        old_symbol, new_symbol = str(relation.get("old_symbol", "")), str(relation.get("new_symbol", ""))
        if (len(old_symbol) != 6 or not old_symbol.isdigit() or len(new_symbol) != 6
                or not new_symbol.isdigit() or old_symbol == new_symbol):
            raise ValueError("symbol succession requires distinct six-digit old and new codes")
        effective_from = relation.get("effective_from")
        effective_from = effective_from.isoformat() if isinstance(effective_from, date) else date.fromisoformat(str(effective_from)).isoformat()
        evidence_refs = relation.get("evidence_refs")
        if (not isinstance(evidence_refs, (list, tuple)) or not evidence_refs
                or any(not isinstance(ref, str) or not ref.strip() for ref in evidence_refs)):
            raise ValueError("symbol succession requires persistent evidence references")
        old_key, new_key = (market, asset_type, old_symbol), (market, asset_type, new_symbol)
        if old_key in old_keys or new_key in new_keys:
            raise ValueError("symbol succession has an ambiguous old or new code")
        old_keys.add(old_key)
        new_keys.add(new_key)
        result.append({"market": market, "asset_type": asset_type, "old_symbol": old_symbol,
            "new_symbol": new_symbol, "effective_from": effective_from,
            "evidence_refs": list(evidence_refs)})
    return tuple(result)


def symbol_succession_for(relations: Iterable[Mapping[str, object]], *, market: str,
                          asset_type: str, symbol: str, as_of_date: date) -> dict[str, object] | None:
    """Resolve an explicitly evidenced linear code succession to its current code."""
    return _symbol_succession_for_normalized(normalize_symbol_successions(relations), market=market,
        asset_type=asset_type, symbol=symbol, as_of_date=as_of_date)


def _symbol_succession_for_normalized(relations: Iterable[Mapping[str, object]], *, market: str,
                                      asset_type: str, symbol: str,
                                      as_of_date: date) -> dict[str, object] | None:
    """Resolve a previously validated relation set without revalidating it per security."""
    code = str(symbol).lower()
    prefixes = {"XSHG": "sh", "XSHE": "sz", "BSE": "bj"}
    prefix = prefixes.get(str(market).upper(), "")
    if code.startswith(prefix) and prefix:
        code = code[len(prefix):]
    active = [relation for relation in relations
        if relation["market"] == str(market).upper() and relation["asset_type"] == str(asset_type).lower()
        and as_of_date >= date.fromisoformat(str(relation["effective_from"]))]
    next_by_old = {str(relation["old_symbol"]): relation for relation in active}
    previous_by_new = {str(relation["new_symbol"]): relation for relation in active}
    if code not in next_by_old and code not in previous_by_new:
        return None

    root, visited = code, {code}
    while root in previous_by_new:
        root = str(previous_by_new[root]["old_symbol"])
        if root in visited:
            raise ValueError("symbol succession contains a cycle")
        visited.add(root)

    current, chain, visited = root, [], {root}
    while current in next_by_old:
        relation = next_by_old[current]
        chain.append(relation)
        current = str(relation["new_symbol"])
        if current in visited:
            raise ValueError("symbol succession contains a cycle")
        visited.add(current)
    if not chain:
        return None
    evidence_refs = list(dict.fromkeys(ref for relation in chain for ref in relation["evidence_refs"]))
    return {**chain[0], "old_symbol": root, "new_symbol": current, "evidence_refs": evidence_refs,
        "transitions": list(chain)}


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
            merged[candidate.instrument_id] = replace(existing, **updates)
    return SecurityMergeResult(
        tuple(sorted(merged.values(), key=lambda item: item.instrument_id)),
        tuple(aliases),
        tuple(conflicts),
        tuple(missing),
    )


def expected_security_set(records: Iterable[SecurityRecord], trade_date: date) -> tuple[SecurityRecord, ...]:
    return tuple(sorted((record for record in records if record.is_expected_on(trade_date)), key=lambda item: item.instrument_id))


class SecurityMasterStore(AbstractContextManager["SecurityMasterStore"]):
    def __init__(self, path: str = ":memory:", *, read_only: bool = False) -> None:
        from pathlib import Path

        self.read_only = read_only
        if read_only and path != ":memory:" and not Path(path).is_file():
            raise FileNotFoundError(f"published security master database is missing: {path}")
        try:
            self.connection = duckdb.connect(path, read_only=read_only)
        except duckdb.Error as exc:
            if read_only:
                raise InvalidPartitionError(f"security master is not a readable published database: {path}") from exc
            raise
        if read_only:
            return
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
        if self.read_only:
            raise PermissionError("security master store is read-only")
        from uuid import uuid4

        import pyarrow as pa

        now = datetime.now(timezone.utc)
        # MERGE requires at most one source row per instrument. Preserve the old
        # sequential-upsert behavior by retaining the last record for each ID.
        record_rows = {
            record.instrument_id: {
                "instrument_id": record.instrument_id,
                "symbol": record.symbol,
                "exchange": record.exchange.value,
                "asset_type": record.asset_type.value,
                "name": record.name,
                "list_date": record.list_date,
                "delist_date": record.delist_date,
                "status": record.status,
                "lot_size": record.lot_size,
                "canonical_volume_unit": record.canonical_volume_unit,
                "canonical_amount_unit": record.canonical_amount_unit,
                "publisher": record.publisher,
                "publish_date": record.publish_date,
                "calculation_start_date": record.calculation_start_date,
                "is_backfilled_history": record.is_backfilled_history,
                "classification_conflict": record.classification_conflict,
                "evidence_sources": list(record.evidence_sources),
                "updated_at": now,
            }
            for record in records
        }
        if record_rows:
            relation_name = f"security_master_input_{uuid4().hex}"
            self.connection.register(relation_name, pa.Table.from_pylist(list(record_rows.values())))
            try:
                self.connection.execute(
                    f"""
                    MERGE INTO security_master AS target
                    USING {relation_name} AS source
                    ON target.instrument_id = source.instrument_id
                    WHEN MATCHED THEN UPDATE SET
                        symbol=source.symbol, exchange=source.exchange,
                        asset_type=source.asset_type, name=source.name,
                        list_date=source.list_date, delist_date=source.delist_date,
                        status=source.status, lot_size=source.lot_size,
                        canonical_volume_unit=source.canonical_volume_unit,
                        canonical_amount_unit=source.canonical_amount_unit,
                        publisher=source.publisher, publish_date=source.publish_date,
                        calculation_start_date=source.calculation_start_date,
                        is_backfilled_history=source.is_backfilled_history,
                        classification_conflict=source.classification_conflict,
                        evidence_sources=source.evidence_sources,
                        updated_at=source.updated_at
                    WHEN NOT MATCHED THEN INSERT (
                        instrument_id, symbol, exchange, asset_type, name, list_date, delist_date,
                        status, lot_size, canonical_volume_unit, canonical_amount_unit, publisher,
                        publish_date, calculation_start_date, is_backfilled_history,
                        classification_conflict, evidence_sources, updated_at
                    ) VALUES (
                        source.instrument_id, source.symbol, source.exchange, source.asset_type,
                        source.name, source.list_date, source.delist_date, source.status,
                        source.lot_size, source.canonical_volume_unit, source.canonical_amount_unit,
                        source.publisher, source.publish_date, source.calculation_start_date,
                        source.is_backfilled_history, source.classification_conflict,
                        source.evidence_sources, source.updated_at
                    )
                    """
                )
            finally:
                self.connection.unregister(relation_name)

        alias_rows = {}
        for alias in aliases:
            key = (alias.instrument_id, alias.provider, alias.symbol, alias.effective_from)
            alias_rows.setdefault(key, {
                "instrument_id": alias.instrument_id,
                "provider": alias.provider,
                "symbol": alias.symbol,
                "effective_from": alias.effective_from,
                "effective_to": alias.effective_to,
                "change_reason": alias.change_reason,
                "evidence_source": alias.evidence_source,
            })
        if alias_rows:
            relation_name = f"security_alias_input_{uuid4().hex}"
            self.connection.register(relation_name, pa.Table.from_pylist(list(alias_rows.values())))
            try:
                self.connection.execute(
                    f"""
                    INSERT INTO security_symbol_history
                    SELECT source.instrument_id, source.provider, source.symbol,
                           source.effective_from, source.effective_to,
                           source.change_reason, source.evidence_source
                    FROM {relation_name} AS source
                    WHERE NOT EXISTS (
                        SELECT 1 FROM security_symbol_history AS target
                        WHERE target.instrument_id=source.instrument_id
                          AND target.provider=source.provider
                          AND target.symbol=source.symbol
                          AND target.effective_from=source.effective_from
                    )
                    """
                )
            finally:
                self.connection.unregister(relation_name)

    def record_changes(
        self,
        changes: Iterable[tuple[str, str, object, object, date, str]],
    ) -> None:
        if self.read_only:
            raise PermissionError("security master store is read-only")
        from uuid import uuid4

        import pyarrow as pa

        detected_at = datetime.now(timezone.utc)
        rows = [
            {
                "instrument_id": instrument_id,
                "field_name": field_name,
                "old_value": None if old_value is None else str(old_value),
                "new_value": None if new_value is None else str(new_value),
                "effective_date": effective_date,
                "detected_at": detected_at,
                "source": source,
            }
            for instrument_id, field_name, old_value, new_value, effective_date, source in changes
        ]
        if rows:
            relation_name = f"security_history_input_{uuid4().hex}"
            self.connection.register(relation_name, pa.Table.from_pylist(rows))
            try:
                self.connection.execute(
                    f"""
                    INSERT INTO security_master_history
                    SELECT instrument_id, field_name, old_value, new_value,
                           effective_date, detected_at, source
                    FROM {relation_name}
                    """
                )
            finally:
                self.connection.unregister(relation_name)

    def publish(self, records: Iterable[SecurityRecord], *, aliases: Iterable[SymbolHistory] = (),
                changes: Iterable[tuple[str, str, object, object, date, str]] = ()) -> None:
        """Apply one complete master update and its history in one transaction."""
        if self.read_only:
            raise PermissionError("security master store is read-only")
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.upsert(records, aliases)
            self.record_changes(changes)
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def all(self) -> tuple[SecurityRecord, ...]:
        rows = self.connection.execute("SELECT * FROM security_master ORDER BY instrument_id").fetchall()
        return self._records(rows)

    @staticmethod
    def _records(rows) -> tuple[SecurityRecord, ...]:
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

    def count(self) -> int:
        return int(self.connection.execute("SELECT count(*) FROM security_master").fetchone()[0])

    def select_identities(self, identities: Iterable[tuple[str, str]]) -> tuple[SecurityRecord, ...]:
        keys = tuple(dict.fromkeys(identities))
        if len(keys) > 100 or any(
                not isinstance(identity, str)
                or re.fullmatch(r"(?:XSHG|XSHE|BSE):[0-9]{6}", identity) is None
                or asset not in {item.value for item in AssetType}
                for identity, asset in keys):
            raise ValueError("security master page identities are invalid")
        if not keys:
            return ()
        values = ",".join("(?,?)" for _ in keys)
        params = [value for key in keys for value in key]
        rows = self.connection.execute(
            f"WITH requested(instrument_id,asset_type) AS (VALUES {values}) "
            "SELECT security_master.* FROM security_master JOIN requested "
            "USING(instrument_id,asset_type) ORDER BY instrument_id", params).fetchall()
        return self._records(rows)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "SecurityMasterStore":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def read_published_security_master(directory: str | "Path", *,
                                   identities: Iterable[tuple[str, str]] | None = None
                                   ) -> tuple[Manifest, tuple[SecurityRecord, ...]]:
    """Verify the complete published master, optionally returning only requested identities."""
    from pathlib import Path

    root = Path(directory)
    if not root.exists():
        raise FileNotFoundError(f"published security master directory is missing: {root}")
    with PartitionLock(root / ".master-access.lock", recover_stale=True, timeout_seconds=1.0):
        if (root / "task-commit.json").exists():
            raise PartitionBusyError("security master publication is pending task finalization")
        descriptor = root / "manifest.json"
        database = root / "security_master.duckdb"
        if not descriptor.is_file() and not database.is_file():
            raise FileNotFoundError(f"published security master is missing: {root}")
        if not descriptor.is_file() or not database.is_file():
            raise InvalidPartitionError("published security master is incomplete")
        try:
            manifest = Manifest.load(descriptor)
        except (ValueError, TypeError, KeyError) as exc:
            raise InvalidPartitionError(f"security master manifest is invalid: {descriptor}") from exc
        if (manifest.dataset != "security_master" or manifest.asset_type != "security"
                or not manifest.verify(database)):
            raise InvalidPartitionError("security master manifest is incompatible or corrupt")
        try:
            with SecurityMasterStore(str(database), read_only=True) as store:
                row_count = store.count()
                records = store.all() if identities is None else store.select_identities(identities)
        except duckdb.Error as exc:
            raise InvalidPartitionError(f"published security master cannot be read: {database}") from exc
        if row_count != manifest.row_count:
            raise InvalidPartitionError("security master row count differs from manifest")
        return manifest, records
