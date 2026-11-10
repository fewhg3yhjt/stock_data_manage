"""Transactional SQLite storage for current daily bars and publication receipts."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from typing import Mapping

from ..domain.models import AssetType, DailyRecord, QuoteGroup, QuoteStatus


@dataclass(frozen=True, slots=True)
class NoTradeFact:
    reason: str
    evidence_ref: str

    def __post_init__(self) -> None:
        if not self.reason.strip() or not self.evidence_ref.strip():
            raise ValueError("no_trade requires a reason and stable evidence reference")


@dataclass(frozen=True, slots=True)
class DailyPublicationResult:
    """Receipt counters describe this run; summary fields describe the current partition."""
    publication_id: str
    idempotent: bool
    written_count: int
    no_trade_count: int
    superseded_count: int
    superseded_identities: tuple[str, ...]
    actual_count: int
    quote_counts: Mapping[str, Mapping[str, int]]
    data_revision: int


@dataclass(frozen=True, slots=True)
class DailyRecordPage:
    records: tuple[DailyRecord, ...]
    total: int
    available_adjustments: tuple[str, ...]


def initialize_schema(connection: sqlite3.Connection) -> None:
    """Create daily tables on an explicitly writable runtime connection."""
    if connection.in_transaction:
        raise RuntimeError("daily schema initialization requires no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise RuntimeError("SQLite foreign key enforcement could not be enabled")
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS daily_bar (
            instrument_id TEXT NOT NULL,
            trade_date DATE NOT NULL,
            asset_type TEXT NOT NULL,
            last_publication_id TEXT NOT NULL,
            PRIMARY KEY (instrument_id, trade_date),
            FOREIGN KEY (last_publication_id) REFERENCES daily_publication(publication_id)
        );
        CREATE INDEX IF NOT EXISTS daily_bar_asset_date_idx
            ON daily_bar(asset_type, trade_date, instrument_id);
        CREATE INDEX IF NOT EXISTS daily_bar_trade_date_identity_idx
            ON daily_bar(trade_date, instrument_id);
        CREATE TABLE IF NOT EXISTS daily_quote (
            instrument_id TEXT NOT NULL,
            trade_date DATE NOT NULL,
            adjustment TEXT NOT NULL CHECK(adjustment IN ('none','forward','backward')),
            status TEXT NOT NULL,
            open_text TEXT, high_text TEXT, low_text TEXT, close_text TEXT,
            volume_text TEXT, amount_text TEXT,
            source_provider TEXT NOT NULL, endpoint TEXT NOT NULL, raw_object_path TEXT NOT NULL,
            fetch_time TIMESTAMPTZ, normalizer_version TEXT NOT NULL,
            capability_version TEXT NOT NULL, reason TEXT NOT NULL, evidence_ref TEXT NOT NULL,
            flags_json TEXT NOT NULL, value_contract TEXT NOT NULL,
            PRIMARY KEY (instrument_id, trade_date, adjustment),
            FOREIGN KEY (instrument_id, trade_date) REFERENCES daily_bar(instrument_id, trade_date) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS daily_quote_status_idx
            ON daily_quote(adjustment, status, trade_date, instrument_id);
        CREATE TABLE IF NOT EXISTS daily_publication (
            publication_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            request_hash TEXT NOT NULL,
            task_id TEXT NOT NULL,
            first_execution_order INTEGER NOT NULL,
            trade_date DATE NOT NULL,
            asset_type TEXT NOT NULL,
            scope_json TEXT NOT NULL,
            candidate_ref TEXT NOT NULL,
            raw_evidence_refs_json TEXT NOT NULL,
            written_count INTEGER NOT NULL,
            no_trade_count INTEGER NOT NULL,
            superseded_count INTEGER NOT NULL,
            superseded_identities_json TEXT NOT NULL,
            committed_at TIMESTAMPTZ NOT NULL
        );
    """)


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    if value.is_zero():
        return "0"
    decimal_tuple = value.as_tuple()
    digits = list(decimal_tuple.digits)
    exponent = decimal_tuple.exponent
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    fractional = max(0, -exponent)
    integer = max(0, len(digits) + exponent)
    if fractional > 8 or integer > 30:
        raise ValueError("daily quote decimal exceeds Decimal(38,8) contract")
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _quote_values(record: DailyRecord, adjustment: str, quote: QuoteGroup) -> tuple[object, ...]:
    return (record.instrument_id, record.trade_date, adjustment, quote.status.value,
            _decimal_text(quote.open), _decimal_text(quote.high), _decimal_text(quote.low),
            _decimal_text(quote.close), _decimal_text(quote.volume), _decimal_text(quote.amount),
            quote.source_provider, quote.endpoint, quote.raw_object_path, quote.fetch_time,
            quote.normalizer_version, quote.capability_version, quote.reason, quote.evidence_ref,
            json.dumps(quote.flags, ensure_ascii=False), quote.value_contract)


def _record_signature(record: DailyRecord) -> object:
    return {"identity": record.instrument_id, "date": record.trade_date.isoformat(),
            "asset": record.asset_type.value,
            "quotes": {key: [quote.status.value, *[_decimal_text(getattr(quote, field))
                       for field in ("open", "high", "low", "close", "volume", "amount")],
                       quote.source_provider, quote.endpoint, quote.raw_object_path,
                       quote.fetch_time.isoformat() if quote.fetch_time else None,
                       quote.normalizer_version, quote.capability_version, quote.reason, quote.evidence_ref,
                       quote.flags, quote.value_contract] for key, quote in record.quotes.items()}}


def _row_to_record(bar: tuple[object, ...], quotes: list[tuple[object, ...]]) -> DailyRecord:
    from decimal import Decimal
    by_adjustment: dict[str, QuoteGroup] = {}
    for row in quotes:
        (instrument_id, trade_date, adjustment, status, op, hi, lo, close, volume, amount,
         provider, endpoint, raw_path, fetch_time, normalizer, capability, reason, evidence,
         flags_json, value_contract) = row
        by_adjustment[str(adjustment)] = QuoteGroup(
            status=QuoteStatus(str(status)), open=Decimal(op) if op is not None else None,
            high=Decimal(hi) if hi is not None else None, low=Decimal(lo) if lo is not None else None,
            close=Decimal(close) if close is not None else None, volume=Decimal(volume) if volume is not None else None,
            amount=Decimal(amount) if amount is not None else None, source_provider=str(provider),
            endpoint=str(endpoint), raw_object_path=str(raw_path), fetch_time=fetch_time,
            normalizer_version=str(normalizer), capability_version=str(capability), reason=str(reason),
            evidence_ref=str(evidence), flags=tuple(json.loads(flags_json)), value_contract=str(value_contract))
    return DailyRecord(str(bar[0]), bar[1], AssetType(str(bar[2])),
                       by_adjustment["none"], by_adjustment["forward"], by_adjustment["backward"])


class DailyBarStore:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    @staticmethod
    def _identity_chunks(values: list[str], size: int = 400):
        for start in range(0, len(values), size):
            yield values[start:start + size]

    def current_owners(self, scope: Mapping[str, object]) -> tuple[dict[str, object], ...]:
        """Return only current bars/no-trade facts inside a validated dated identity scope."""
        if scope.get("dataset") != "daily_bar" or scope.get("frequency") != "1d":
            raise ValueError("owner preview requires a daily_bar/1d scope")
        by_date = scope.get("securities_by_date")
        if not isinstance(by_date, Mapping) or not by_date:
            raise ValueError("owner preview requires dated identities")
        result: dict[tuple[str, str], dict[str, object]] = {}
        with self._read_snapshot():
            for day_text, identities_value in sorted(by_date.items()):
                day = date.fromisoformat(str(day_text))
                if type(day) is not date:
                    raise ValueError("owner preview date must be ISO date")
                identities = sorted(set(identities_value))
                if not identities or any(not isinstance(identity, str) or not re.fullmatch(r"(?:XSHG|XSHE|BSE):[0-9]{6}", identity)
                                         for identity in identities):
                    raise ValueError("owner preview identities must be stable market-qualified IDs")
                for batch in self._identity_chunks(identities):
                    placeholders = ",".join("?" for _ in batch)
                    bars = self.connection.execute(
                        f"""SELECT b.instrument_id,b.trade_date,b.asset_type,b.last_publication_id,
                                   p.task_id,p.first_execution_order
                            FROM daily_bar b LEFT JOIN daily_publication p ON p.publication_id=b.last_publication_id
                            WHERE b.trade_date=? AND b.instrument_id IN ({placeholders})""",
                        [day, *batch]).fetchall()
                    no_trades = self.connection.execute(
                        f"""SELECT i.instrument_id,i.partition_key,i.asset_type,i.last_publication_id,
                                   p.task_id,p.first_execution_order
                            FROM partition_item i LEFT JOIN daily_publication p ON p.publication_id=i.last_publication_id
                            WHERE i.dataset='daily_bar' AND i.adjustment='aggregate' AND i.status='no_trade'
                              AND i.partition_key=? AND i.instrument_id IN ({placeholders})""",
                        [day.isoformat(), *batch]).fetchall()
                    for row, status in [*((row, "bar") for row in bars), *((row, "no_trade") for row in no_trades)]:
                        identity = str(row[0])
                        key = (day.isoformat(), identity)
                        if key in result:
                            raise RuntimeError("daily identity has conflicting current bar and no_trade facts")
                        if row[4] is None or row[5] is None:
                            raise RuntimeError(f"current daily fact references missing publication: {identity}")
                        result[key] = {"trade_date": day.isoformat(), "asset_type": str(row[2]),
                            "instrument_id": identity, "publication_id": str(row[3]),
                            "task_id": str(row[4]), "first_execution_order": int(row[5]),
                            "status": status}
        return tuple(result[key] for key in sorted(result))

    def publication_receipts(self, *, task_id: str, trade_date: date | None = None,
                             asset_type: AssetType | None = None) -> tuple[dict[str, object], ...]:
        """Read this task's immutable delivery receipts without reading current bar snapshots."""
        if not task_id or (trade_date is not None and type(trade_date) is not date):
            raise ValueError("receipt query requires task ID and typed optional date")
        if asset_type is not None and not isinstance(asset_type, AssetType):
            raise ValueError("receipt query asset type must be typed")
        clauses, params = ["task_id=?"], [task_id]
        if trade_date is not None:
            clauses.append("trade_date=?")
            params.append(trade_date)
        if asset_type is not None:
            clauses.append("asset_type=?")
            params.append(asset_type.value)
        with self._read_snapshot():
            rows = self.connection.execute(
                "SELECT publication_id,idempotency_key,request_hash,task_id,first_execution_order,trade_date,asset_type,scope_json,candidate_ref,raw_evidence_refs_json,written_count,no_trade_count,superseded_count,superseded_identities_json,committed_at FROM daily_publication WHERE "
                + " AND ".join(clauses) + " ORDER BY committed_at,publication_id", params).fetchall()
        return tuple({"publication_id": row[0], "idempotency_key": row[1], "request_hash": row[2],
            "task_id": row[3], "first_execution_order": int(row[4]), "trade_date": row[5],
            "asset_type": AssetType(str(row[6])), "scope": tuple(json.loads(row[7])),
            "candidate_ref": row[8], "raw_evidence_refs": tuple(json.loads(row[9])),
            "written_count": int(row[10]), "no_trade_count": int(row[11]),
            "superseded_count": int(row[12]), "superseded_identities": tuple(json.loads(row[13])),
            "committed_at": row[14]} for row in rows)

    def _assert_task(self, task_id: str, order: int, day: date, asset: AssetType,
                     identities: set[str]) -> dict[str, object]:
        row = self.connection.execute(
            "SELECT t.status, t.first_execution_order, c.scope_json, c.active_scope_json, c.snapshot_json, t.state_json "
            "FROM collection_task t LEFT JOIN collection_task_context c USING(task_id) WHERE t.task_id=?",
            [task_id]).fetchone()
        if row is None or row[1] is None or int(row[1]) != order:
            raise ValueError("daily publication task has no matching persisted first execution order")
        if row[2] is None or row[3] is None or row[4] is None:
            raise ValueError("daily publication task context is missing")
        if row[0] not in {"collecting", "checking", "committing"}:
            raise ValueError("daily publication requires a currently claimed task")
        frozen = json.loads(row[2] or "{}")
        active = json.loads(row[3] or "{}")
        state = json.loads(row[5])
        snapshot = json.loads(row[4] or "{}")
        if "daily_overwrite_grant" in snapshot:
            state["daily_overwrite_grant"] = snapshot["daily_overwrite_grant"]
        for scope in (frozen, active):
            if scope.get("dataset") != "daily_bar" or scope.get("frequency") != "1d":
                raise ValueError("task scope is not a daily-bar scope")
            if asset.value not in set(scope.get("asset_types", ())):
                raise ValueError("publication asset is outside task scope")
            allowed = set(scope.get("securities_by_date", {}).get(day.isoformat(), ()))
            if not allowed or "*" in allowed or not identities <= allowed:
                raise ValueError("publication identity is outside frozen/current task scope")
        return state

    def publish_partition(self, *, trade_date: date, asset_type: AssetType, task_id: str,
                          first_execution_order: int, publication_id: str, idempotency_key: str,
                          scope: frozenset[str], records: tuple[DailyRecord, ...],
                          no_trade: Mapping[str, NoTradeFact], raw_evidence_refs: tuple[str, ...],
                          candidate_ref: str, committed_at: datetime,
                          authorization: Mapping[str, object] | None = None) -> DailyPublicationResult:
        if type(trade_date) is not date or not isinstance(asset_type, AssetType):
            raise ValueError("publication needs typed date and asset type")
        if committed_at.utcoffset() is None:
            raise ValueError("publication time must be timezone-aware")
        committed_at = committed_at.astimezone(timezone.utc)
        record_ids = {record.instrument_id for record in records}
        if len(record_ids) != len(records):
            raise ValueError("daily candidate contains a duplicate identity")
        no_trade_ids = set(no_trade)
        if record_ids & no_trade_ids:
            raise ValueError("one publication cannot mark an identity both traded and no_trade")
        if not record_ids and not no_trade_ids:
            raise ValueError("empty candidate cannot create a daily publication")
        all_ids = record_ids | no_trade_ids
        if any(not re.fullmatch(r"(?:XSHG|XSHE|BSE):[0-9]{6}", identity) for identity in all_ids):
            raise ValueError("publication identities must be market-qualified securities")
        if not all_ids <= set(scope):
            raise ValueError("candidate identity lies outside supplied publication scope")
        for record in records:
            if (record.trade_date != trade_date or record.asset_type is not asset_type
                    or not record.complete):
                raise ValueError("daily candidate must match date/asset and satisfy DailyRecord.complete")
        if any(not isinstance(fact, NoTradeFact) for fact in no_trade.values()):
            raise ValueError("no_trade values must carry explicit evidence")
        if self.connection.in_transaction:
            raise RuntimeError("daily publication owns its complete transaction and cannot nest")
        snapshot_row = self.connection.execute(
            "SELECT snapshot_json FROM collection_task_context WHERE task_id=?", [task_id]).fetchone()
        if snapshot_row is None:
            raise ValueError("daily publication task is not persisted")
        snapshot = json.loads(snapshot_row[0] or "{}")
        persisted_authorization = snapshot.get("daily_overwrite_grant")
        if not isinstance(persisted_authorization, Mapping) or not isinstance(persisted_authorization.get("owners"), list):
            raise ValueError("daily task snapshot lacks its frozen overwrite grant")
        if authorization is not None and authorization != persisted_authorization:
            raise ValueError("publication authorization differs from the persisted enqueue authorization")
        authorization = persisted_authorization
        record_by_identity = {record.instrument_id: record for record in records}
        prepared_quotes = {identity: tuple(_quote_values(record, adjustment, quote)
            for adjustment, quote in record.quotes.items())
            for identity, record in record_by_identity.items()}
        request = {"task_id": task_id, "order": first_execution_order, "date": trade_date.isoformat(),
                   "asset": asset_type.value, "scope": sorted(scope),
                   "records": [_record_signature(record) for record in records],
                   "no_trade": {key: [value.reason, value.evidence_ref] for key, value in sorted(no_trade.items())},
                   "raw": raw_evidence_refs, "candidate": candidate_ref,
                   "authorization": authorization}
        request_hash = hashlib.sha256(json.dumps(request, sort_keys=True, ensure_ascii=False,
                                                 separators=(",", ":")).encode()).hexdigest()
        self.connection.execute("PRAGMA foreign_keys=ON")
        if self.connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            raise RuntimeError("SQLite foreign key enforcement is required")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            prior = self.connection.execute(
                "SELECT publication_id, request_hash FROM daily_publication WHERE idempotency_key=?",
                [idempotency_key]).fetchone()
            if prior:
                if prior[1] != request_hash:
                    raise ValueError("idempotency key was already used with different publication content")
                # Delivery counts belong to this immutable receipt; quote counts and revision
                # are read from the currently visible partition and may reflect a later run.
                result = self._result(prior[0], True)
                self.connection.execute("COMMIT")
                return result
            persisted_task = self._assert_task(task_id, first_execution_order, trade_date, asset_type, set(scope))
            transaction_authorization = persisted_task.get("daily_overwrite_grant")
            if authorization != transaction_authorization:
                raise ValueError("publication authorization differs from the persisted enqueue authorization")
            if not all_ids <= set(scope):
                raise ValueError("candidate identity is outside persisted task scope")
            self.connection.execute("""INSERT INTO daily_publication VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, '[]', ?)""",
                [publication_id, idempotency_key, request_hash, task_id, first_execution_order,
                 trade_date, asset_type.value, json.dumps(sorted(scope)), candidate_ref,
                 json.dumps(raw_evidence_refs), committed_at])
            written, no_trade_written, superseded = 0, 0, []
            authorized_owners = {
                (str(owner["trade_date"]), str(owner["instrument_id"])): owner
                for owner in (authorization or {}).get("owners", ())
            }
            for identity in sorted(all_ids):
                existing_bar = self.connection.execute(
                    "SELECT last_publication_id,asset_type FROM daily_bar WHERE instrument_id=? AND trade_date=?",
                    [identity, trade_date]).fetchone()
                existing_no_trade = self.connection.execute(
                    "SELECT last_publication_id,asset_type FROM partition_item WHERE dataset='daily_bar' AND partition_key=? AND adjustment='aggregate' AND instrument_id=? AND status='no_trade'",
                    [trade_date.isoformat(), identity]).fetchone()
                if existing_bar and existing_no_trade:
                    raise RuntimeError(f"daily identity has both bar and no_trade facts: {identity}")
                existing = existing_bar or existing_no_trade
                owner_ref = None
                if existing:
                    old = self.connection.execute(
                        "SELECT task_id,first_execution_order FROM daily_publication WHERE publication_id=?", [existing[0]]).fetchone()
                    if old is None:
                        raise RuntimeError(f"current daily ownership references missing publication {existing[0]}")
                    if existing[1] != asset_type.value:
                        raise ValueError("current identity is already published under a different asset type")
                    status = "bar" if existing_bar else "no_trade"
                    owner_ref = {"trade_date": trade_date.isoformat(), "asset_type": existing[1],
                        "instrument_id": identity, "publication_id": existing[0], "task_id": old[0],
                        "first_execution_order": int(old[1]), "status": status}
                    authorized = authorized_owners.get((trade_date.isoformat(), identity))
                    if old[0] != task_id and (int(old[1]) > first_execution_order or authorized != owner_ref):
                        superseded.append(identity)
                        continue
                elif (trade_date.isoformat(), identity) in authorized_owners:
                    raise ValueError(f"daily owner disappeared since overwrite preview for {identity}; re-preview required")
                if identity in no_trade:
                    self.connection.execute("DELETE FROM daily_bar WHERE instrument_id=? AND trade_date=?", [identity, trade_date])
                    fact = no_trade[identity]
                    self.connection.execute("""INSERT INTO partition_item
                        (dataset,partition_key,adjustment,instrument_id,status,source_provider,asset_type,business_reason,evidence_ref,last_publication_id,updated_at)
                        VALUES ('daily_bar',?,'aggregate',?,'no_trade',NULL,?,?,?,?,?)
                        ON CONFLICT(dataset,partition_key,adjustment,instrument_id) DO UPDATE SET status='no_trade',source_provider=NULL,asset_type=excluded.asset_type,business_reason=excluded.business_reason,evidence_ref=excluded.evidence_ref,last_publication_id=excluded.last_publication_id,updated_at=excluded.updated_at""",
                        [trade_date.isoformat(), identity, asset_type.value, fact.reason, fact.evidence_ref, publication_id, committed_at])
                    no_trade_written += 1
                else:
                    self.connection.execute("DELETE FROM partition_item WHERE dataset='daily_bar' AND partition_key=? AND adjustment='aggregate' AND instrument_id=?", [trade_date.isoformat(), identity])
                    self.connection.execute("INSERT INTO daily_bar VALUES (?,?,?,?) ON CONFLICT(instrument_id,trade_date) DO UPDATE SET asset_type=excluded.asset_type,last_publication_id=excluded.last_publication_id",
                        [identity, trade_date, asset_type.value, publication_id])
                    self.connection.execute("DELETE FROM daily_quote WHERE instrument_id=? AND trade_date=?", [identity, trade_date])
                    self.connection.executemany("INSERT INTO daily_quote VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        prepared_quotes[identity])
                    written += 1
            self.connection.execute("""UPDATE daily_publication SET written_count=?,no_trade_count=?,superseded_count=?,superseded_identities_json=? WHERE publication_id=?""",
                [written, no_trade_written, len(superseded), json.dumps(superseded), publication_id])
            summary = self._rebuild_summary(trade_date, asset_type, committed_at) if written or no_trade_written else self._current_summary_values(trade_date, asset_type)
            self.connection.execute("COMMIT")
            return DailyPublicationResult(publication_id, False, written, no_trade_written, len(superseded),
                tuple(superseded), summary[0], summary[1], summary[2])
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def _rebuild_summary(self, day: date, asset: AssetType, now: datetime) -> tuple[int, Mapping[str, Mapping[str, int]], int, int]:
        actual = int(self.connection.execute("SELECT count(*) FROM daily_bar WHERE trade_date=? AND asset_type=?", [day, asset.value]).fetchone()[0])
        no_trade_count = int(self.connection.execute("SELECT count(*) FROM partition_item WHERE dataset='daily_bar' AND partition_key=? AND adjustment='aggregate' AND asset_type=? AND status='no_trade'", [day.isoformat(), asset.value]).fetchone()[0])
        counts: dict[str, dict[str, int]] = {}
        for adjustment in ("none", "forward", "backward"):
            row = self.connection.execute("""SELECT
                sum(CASE WHEN status='available' THEN 1 ELSE 0 END),
                sum(CASE WHEN status='unverified' THEN 1 ELSE 0 END),
                sum(CASE WHEN status='not_applicable' THEN 1 ELSE 0 END)
                FROM daily_quote q JOIN daily_bar b USING(instrument_id,trade_date)
                WHERE b.trade_date=? AND b.asset_type=? AND q.adjustment=?""", [day, asset.value, adjustment]).fetchone()
            counts[adjustment] = dict(zip(("available", "unverified", "not_applicable"), (int(value or 0) for value in row)))
        prior = self.connection.execute("SELECT data_revision FROM partition_status WHERE dataset='daily_bar' AND asset_type=? AND partition_key=? AND adjustment='aggregate'", [asset.value, day.isoformat()]).fetchone()
        revision = int(prior[0]) + 1 if prior else 1
        flattened = [counts[adjustment][status] for adjustment in ("none", "forward", "backward") for status in ("available", "unverified", "not_applicable")]
        self.connection.execute("""INSERT INTO partition_status
            (dataset,asset_type,partition_key,adjustment,expected_count,actual_count,status,manifest_path,content_hash,
             conflict_count,quarantined_count,no_trade_count,
             none_available_count,none_unverified_count,none_not_applicable_count,
             forward_available_count,forward_unverified_count,forward_not_applicable_count,
             backward_available_count,backward_unverified_count,backward_not_applicable_count,data_revision,
             coverage_reason,updated_at)
            VALUES ('daily_bar',?,?, 'aggregate',NULL,?,'published',NULL,NULL,0,0,?,?,?,?,?,?,?,?,?,?,?,'independent security-range evidence unavailable',?)
            ON CONFLICT(dataset,asset_type,partition_key,adjustment) DO UPDATE SET expected_count=excluded.expected_count,
             actual_count=excluded.actual_count,
             status='published',no_trade_count=excluded.no_trade_count,data_revision=excluded.data_revision,
             none_available_count=excluded.none_available_count,none_unverified_count=excluded.none_unverified_count,
             none_not_applicable_count=excluded.none_not_applicable_count,forward_available_count=excluded.forward_available_count,
             forward_unverified_count=excluded.forward_unverified_count,forward_not_applicable_count=excluded.forward_not_applicable_count,
             backward_available_count=excluded.backward_available_count,backward_unverified_count=excluded.backward_unverified_count,
             backward_not_applicable_count=excluded.backward_not_applicable_count,
             expected_count=NULL,coverage_reason=excluded.coverage_reason,updated_at=excluded.updated_at""",
             [asset.value, day.isoformat(), actual, no_trade_count, *flattened, revision, now])
        return actual, counts, revision, no_trade_count

    def _current_summary_values(self, day: date, asset: AssetType) -> tuple[int, Mapping[str, Mapping[str, int]], int, int]:
        row = self.connection.execute("""SELECT actual_count,none_available_count,none_unverified_count,none_not_applicable_count,
            forward_available_count,forward_unverified_count,forward_not_applicable_count,
            backward_available_count,backward_unverified_count,backward_not_applicable_count,data_revision,
            coalesce(no_trade_count,0) FROM partition_status WHERE dataset='daily_bar' AND asset_type=?
            AND partition_key=? AND adjustment='aggregate'""", [asset.value, day.isoformat()]).fetchone()
        if row is None:
            raise RuntimeError("current daily facts exist without their required partition summary")
        counts = {adjustment: dict(zip(("available", "unverified", "not_applicable"),
                 (int(value or 0) for value in row[start:start + 3])))
                  for adjustment, start in {"none": 1, "forward": 4, "backward": 7}.items()}
        return int(row[0]), counts, int(row[10] or 0), int(row[11] or 0)

    def _result(self, publication_id: str, idempotent: bool) -> DailyPublicationResult:
        row = self.connection.execute("SELECT trade_date,asset_type,written_count,no_trade_count,superseded_count,superseded_identities_json FROM daily_publication WHERE publication_id=?", [publication_id]).fetchone()
        summary = self.connection.execute("SELECT actual_count,none_available_count,none_unverified_count,none_not_applicable_count,forward_available_count,forward_unverified_count,forward_not_applicable_count,backward_available_count,backward_unverified_count,backward_not_applicable_count,data_revision FROM partition_status WHERE dataset='daily_bar' AND asset_type=? AND partition_key=? AND adjustment='aggregate'", [row[1], row[0].isoformat()]).fetchone()
        if summary is None:
            raise RuntimeError("daily publication receipt exists without its required partition summary")
        counts = {adjustment: dict(zip(("available", "unverified", "not_applicable"), (int(value or 0) for value in summary[start:start + 3])))
                  for adjustment, start in {"none": 1, "forward": 4, "backward": 7}.items()}
        return DailyPublicationResult(publication_id, idempotent, int(row[2]), int(row[3]), int(row[4]), tuple(json.loads(row[5])), int(summary[0]), counts, int(summary[10] or 0))

    @contextmanager
    def _read_snapshot(self):
        owned = not self.connection.in_transaction
        if owned:
            self.connection.execute("BEGIN")
        try:
            yield
        except Exception:
            if owned:
                self.connection.execute("ROLLBACK")
            raise
        else:
            if owned:
                self.connection.execute("COMMIT")

    def get(self, instrument_id: str, trade_date: date) -> DailyRecord | None:
        """Read one parent and its three quote rows in the same SQLite snapshot."""
        if type(trade_date) is not date:
            raise ValueError("trade_date must be a date")
        with self._read_snapshot():
            bar = self.connection.execute("SELECT instrument_id,trade_date,asset_type,last_publication_id FROM daily_bar WHERE instrument_id=? AND trade_date=?", [instrument_id, trade_date]).fetchone()
            if not bar:
                return None
            quotes = self.connection.execute("SELECT instrument_id,trade_date,adjustment,status,open_text,high_text,low_text,close_text,volume_text,amount_text,source_provider,endpoint,raw_object_path,fetch_time,normalizer_version,capability_version,reason,evidence_ref,flags_json,value_contract FROM daily_quote WHERE instrument_id=? AND trade_date=? ORDER BY adjustment", [instrument_id, trade_date]).fetchall()
        if len(quotes) != 3:
            raise RuntimeError("stored daily bar does not have exactly three quote groups")
        return _row_to_record(bar, quotes)

    def list_records(self, *, asset_type: AssetType | None = None, date_from: date | None = None,
                     date_to: date | None = None, instrument_id: str | None = None,
                     adjustment: str = "none", status: str | None = None,
                     limit: int = 100, offset: int = 0) -> tuple[tuple[DailyRecord, ...], int]:
        """Count and SQL-page first; then load quote groups for page rows only."""
        result = self._query_page(asset_type=asset_type, date_from=date_from, date_to=date_to,
            instrument_id=instrument_id, identity_date_ranges=None, adjustment=adjustment,
            status=status, limit=limit, offset=offset,
            include_available_adjustments=False)
        return result.records, result.total

    def query_page(self, *, asset_type: AssetType | None = None, date_from: date | None = None,
                   date_to: date | None = None, instrument_id: str | None = None,
                   identity_date_ranges: tuple[tuple[str, date | None, date | None, str | None], ...] | None = None,
                   adjustment: str = "none", status: str | None = None,
                   limit: int = 100, offset: int = 0) -> DailyRecordPage:
        """Return a filtered page and whole-result adjustment summary from one read snapshot."""
        return self._query_page(asset_type=asset_type, date_from=date_from, date_to=date_to,
            instrument_id=instrument_id, identity_date_ranges=identity_date_ranges,
            adjustment=adjustment, status=status, limit=limit, offset=offset,
            include_available_adjustments=True)

    def _query_page(self, *, asset_type: AssetType | None, date_from: date | None, date_to: date | None,
                    instrument_id: str | None, identity_date_ranges,
                    adjustment: str, status: str | None, limit: int, offset: int,
                    include_available_adjustments: bool) -> DailyRecordPage:
        if (adjustment not in {"none", "forward", "backward"} or limit < 1 or limit > 100
                or offset < 0 or (asset_type is not None and not isinstance(asset_type, AssetType))
                or (date_from is not None and type(date_from) is not date)
                or (date_to is not None and type(date_to) is not date)
                or (date_from is not None and date_to is not None and date_from > date_to)
                or (status is not None and status not in {item.value for item in QuoteStatus})
                or (instrument_id is not None and identity_date_ranges is not None)):
            raise ValueError("invalid daily query pagination or adjustment")
        clauses, params = [], []
        identity_ranges = (tuple(dict.fromkeys(identity_date_ranges))
                           if identity_date_ranges is not None else None)
        if asset_type is not None:
            clauses.append("b.asset_type=?")
            params.append(asset_type.value)
        if date_from is not None: clauses.append("b.trade_date>=?"); params.append(date_from)
        if date_to is not None: clauses.append("b.trade_date<=?"); params.append(date_to)
        if instrument_id is not None: clauses.append("b.instrument_id=?"); params.append(instrument_id)
        if identity_date_ranges is not None:
            if not identity_ranges:
                clauses.append("0=1")
            else:
                scope_clauses, scope_params = [], []
                for identity, first, last, scoped_asset in identity_ranges:
                    if (not isinstance(identity, str) or not re.fullmatch(r"(?:XSHG|XSHE|BSE):[0-9]{6}", identity)
                            or (first is not None and type(first) is not date)
                            or (last is not None and type(last) is not date)
                            or (first is not None and last is not None and first > last)
                            or (scoped_asset is not None and scoped_asset not in {item.value for item in AssetType})):
                        raise ValueError("invalid daily identity date scope")
                    parts = ["b.instrument_id=?"]
                    values: list[object] = [identity]
                    if scoped_asset is not None:
                        parts.append("b.asset_type=?"); values.append(scoped_asset)
                    if first is not None:
                        parts.append("b.trade_date>=?"); values.append(first)
                    if last is not None:
                        parts.append("b.trade_date<=?"); values.append(last)
                    scope_clauses.append("(" + " AND ".join(parts) + ")")
                    scope_params.extend(values)
                clauses.append("(" + " OR ".join(scope_clauses) + ")")
                params.extend(scope_params)
        where = " AND ".join(clauses) if clauses else "1=1"
        if status is not None:
            where += " AND EXISTS (SELECT 1 FROM daily_quote selected WHERE selected.instrument_id=b.instrument_id " \
                     "AND selected.trade_date=b.trade_date AND selected.adjustment=? AND selected.status=?)"
            selection_params = [adjustment, status]
        else:
            selection_params = []
        with self._read_snapshot():
            total = int(self.connection.execute(f"SELECT count(*) FROM daily_bar b WHERE {where}",
                [*params, *selection_params]).fetchone()[0])
            if include_available_adjustments:
                available_values = []
                if total:
                    for candidate_adjustment in ("none", "forward", "backward"):
                        quote_clauses = ["q.adjustment=?", "q.status=?"]
                        quote_params: list[object] = [candidate_adjustment, QuoteStatus.AVAILABLE.value]
                        if instrument_id is not None:
                            quote_clauses.append("q.instrument_id=?"); quote_params.append(instrument_id)
                        if date_from is not None:
                            quote_clauses.append("q.trade_date>=?"); quote_params.append(date_from)
                        if date_to is not None:
                            quote_clauses.append("q.trade_date<=?"); quote_params.append(date_to)
                        if identity_ranges is not None:
                            quote_scopes, quote_scope_params = [], []
                            for identity, first, last, _ in identity_ranges:
                                parts = ["q.instrument_id=?"]
                                values: list[object] = [identity]
                                if first is not None:
                                    parts.append("q.trade_date>=?"); values.append(first)
                                if last is not None:
                                    parts.append("q.trade_date<=?"); values.append(last)
                                quote_scopes.append("(" + " AND ".join(parts) + ")")
                                quote_scope_params.extend(values)
                            quote_clauses.append("(" + " OR ".join(quote_scopes) + ")"
                                if quote_scopes else "0=1")
                            quote_params.extend(quote_scope_params)
                        available_sql = f"SELECT 1 FROM daily_quote q JOIN daily_bar b USING(instrument_id,trade_date) " \
                            f"WHERE {where} AND {' AND '.join(quote_clauses)} LIMIT 1"
                        available_params = [*params, *selection_params, *quote_params]
                        if self.connection.execute(available_sql, available_params).fetchone():
                            available_values.append(candidate_adjustment)
                available = tuple(available_values)
            else:
                available = ()
            bars = self.connection.execute(f"SELECT b.instrument_id,b.trade_date,b.asset_type,b.last_publication_id FROM daily_bar b WHERE {where} ORDER BY b.trade_date,b.instrument_id LIMIT ? OFFSET ?", [*params, *selection_params, limit, offset]).fetchall()
            page_quotes = {}
            if bars:
                pairs = ",".join("(?,?)" for _ in bars)
                pair_params = [value for bar in bars for value in (bar[0], bar[1])]
                rows = self.connection.execute(
                    "SELECT instrument_id,trade_date,adjustment,status,open_text,high_text,low_text,close_text,volume_text,amount_text,source_provider,endpoint,raw_object_path,fetch_time,normalizer_version,capability_version,reason,evidence_ref,flags_json,value_contract "
                    f"FROM daily_quote WHERE (instrument_id,trade_date) IN ({pairs}) ORDER BY instrument_id,trade_date,adjustment",
                    pair_params).fetchall()
                for row in rows:
                    page_quotes.setdefault((row[0], row[1]), []).append(row)
        records = []
        for bar in bars:
            quotes = page_quotes.get((bar[0], bar[1]), ())
            if len(quotes) != 3:
                raise RuntimeError("stored daily bar does not have exactly three quote groups")
            records.append(_row_to_record(bar, quotes))
        return DailyRecordPage(tuple(records), total, available)

    def partition_summary(self, trade_date: date, asset_type: AssetType) -> dict[str, object] | None:
        cursor = self.connection.execute("SELECT * FROM partition_status WHERE dataset='daily_bar' AND asset_type=? AND partition_key=? AND adjustment='aggregate'", [asset_type.value, trade_date.isoformat()])
        row = cursor.fetchone()
        return dict(zip((column[0] for column in cursor.description), row)) if row else None
