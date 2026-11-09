from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from decimal import Decimal
from typing import Mapping
from urllib.parse import quote

from ..providers.probes import ProbeEvidence
from ..quality.resolution import Conflict
from ..domain import AttemptStatus, DividendEvent, ItemStatus
from ..worker.attempts import CollectionAttempt
from .integrity import Manifest, row_hash


sqlite3.register_adapter(date, lambda value: value.isoformat())


def _adapt_datetime_utc(value: datetime) -> str:
    normalized = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return normalized.isoformat()


sqlite3.register_adapter(datetime, _adapt_datetime_utc)
sqlite3.register_adapter(Decimal, str)
sqlite3.register_converter("DATE", lambda value: date.fromisoformat(value.decode("ascii")))
sqlite3.register_converter("BOOLEAN", lambda value: bool(int(value)))


def _convert_datetime_utc(value: bytes) -> datetime:
    parsed = datetime.fromisoformat(value.decode("ascii"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


sqlite3.register_converter("TIMESTAMPTZ", _convert_datetime_utc)
sqlite3.register_converter("DECIMAL_TEXT", lambda value: Decimal(value.decode("ascii")))


RUNTIME_DATABASE_BUSY_TIMEOUT_MS = 1_000


class CollectionTaskScopeConflict(ValueError):
    def __init__(self, task_id: str, conflicts: tuple[dict, ...]) -> None:
        self.task_id = task_id
        self.conflicts = conflicts
        super().__init__(f"collection scope overlaps active task {task_id}: {conflicts!r}")


class CollectionTaskSnapshotChanged(ValueError):
    pass


def open_runtime_database(
    path: str | Path,
    *,
    read_only: bool = False,
    purpose: str = "metadata",
) -> sqlite3.Connection:
    """Open one caller-owned SQLite connection with the runtime durability contract."""
    version = sqlite3.sqlite_version_info
    if not (version >= (3, 51, 3) or version in {(3, 50, 7), (3, 44, 6)}):
        raise RuntimeError(
            f"SQLite {sqlite3.sqlite_version} lacks the required WAL-reset fix "
            "(need >=3.51.3 or backported 3.50.7/3.44.6)"
        )
    database_path = str(path)
    if database_path == ":memory:":
        if read_only:
            raise FileNotFoundError(f"SQLite {purpose} database does not exist: {database_path}")
        target, uri = database_path, False
    else:
        file_path = Path(database_path)
        if read_only and not file_path.is_file():
            raise FileNotFoundError(f"SQLite {purpose} database does not exist: {database_path}")
        if not read_only:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.exists():
            with file_path.open("rb") as stream:
                header = stream.read(16)
            if header and header != b"SQLite format 3\x00":
                raise ValueError(
                    f"unsupported {purpose} database (expected SQLite; legacy DuckDB is not migrated): {database_path}"
                )
        if read_only:
            resolved_path = str(file_path.resolve()).replace(chr(92), "/")
            target = f"file:{quote(resolved_path, safe='/:')}?mode=ro"
            uri = True
        else:
            target, uri = database_path, False
    connection = sqlite3.connect(
        target,
        uri=uri,
        timeout=RUNTIME_DATABASE_BUSY_TIMEOUT_MS / 1000,
        isolation_level=None,
        detect_types=sqlite3.PARSE_DECLTYPES,
        check_same_thread=True,
    )
    connection.execute(f"PRAGMA busy_timeout={RUNTIME_DATABASE_BUSY_TIMEOUT_MS}")
    if not read_only:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
    return connection


@dataclass(frozen=True, slots=True)
class ProviderHealth:
    provider: str
    endpoint: str
    market: str
    asset_type: str
    dataset: str
    capability_version: str
    enabled: bool
    last_success_at: datetime | None
    last_failure_at: datetime | None
    opened_at: datetime | None
    open_until: datetime | None
    consecutive_failures: int
    last_probe_at: datetime | None
    probe_expires_at: datetime | None
    last_success_coverage: float | None
    http_403_count: int
    http_429_count: int
    last_failure_class: str | None
    last_error: str | None


class MetadataStore(AbstractContextManager["MetadataStore"]):
    def __init__(self, path: str | Path = ":memory:", *, read_only: bool = False) -> None:
        self.path = str(path)
        self.read_only = read_only
        self.connection = open_runtime_database(self.path, read_only=read_only)
        if not read_only:
            self._create_schema()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS partition_status (
                dataset VARCHAR NOT NULL,
                asset_type VARCHAR NOT NULL,
                partition_key VARCHAR NOT NULL,
                adjustment VARCHAR NOT NULL DEFAULT 'none',
                expected_count BIGINT,
                actual_count BIGINT NOT NULL,
                status VARCHAR NOT NULL,
                manifest_path VARCHAR,
                content_hash VARCHAR,
                conflict_count BIGINT NOT NULL,
                quarantined_count BIGINT NOT NULL,
                no_trade_count BIGINT,
                none_available_count BIGINT,
                none_unverified_count BIGINT,
                none_not_applicable_count BIGINT,
                forward_available_count BIGINT,
                forward_unverified_count BIGINT,
                forward_not_applicable_count BIGINT,
                backward_available_count BIGINT,
                backward_unverified_count BIGINT,
                backward_not_applicable_count BIGINT,
                data_revision BIGINT,
                coverage_reason VARCHAR,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (dataset, asset_type, partition_key, adjustment)
            );
            CREATE TABLE IF NOT EXISTS partition_item (
                dataset VARCHAR NOT NULL,
                partition_key VARCHAR NOT NULL,
                adjustment VARCHAR NOT NULL DEFAULT 'none',
                instrument_id VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                source_provider VARCHAR,
                asset_type VARCHAR,
                business_reason VARCHAR,
                evidence_ref VARCHAR,
                last_publication_id VARCHAR,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (dataset, partition_key, adjustment, instrument_id)
            );
            CREATE TABLE IF NOT EXISTS collection_attempt (
                attempt_id VARCHAR PRIMARY KEY,
                status VARCHAR NOT NULL,
                lease_owner VARCHAR,
                lease_acquired_at TIMESTAMPTZ,
                lease_expires_at TIMESTAMPTZ,
                raw_object_path VARCHAR,
                raw_content_hash VARCHAR,
                updated_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS collection_task (
                task_id VARCHAR PRIMARY KEY,
                definition_hash VARCHAR NOT NULL,
                dataset VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                state_json VARCHAR NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                queue_order INTEGER,
                first_accepted_at TIMESTAMPTZ,
                queued_at TIMESTAMPTZ,
                scope_json VARCHAR,
                snapshot_json VARCHAR,
                active_scope_json VARCHAR,
                first_execution_order BIGINT
            );
            CREATE INDEX IF NOT EXISTS collection_task_queue_idx
                ON collection_task(status, queue_order, task_id);
            CREATE INDEX IF NOT EXISTS collection_task_queue_order_idx
                ON collection_task(queue_order);
            CREATE UNIQUE INDEX IF NOT EXISTS collection_task_first_execution_order_idx
                ON collection_task(first_execution_order);
            CREATE TABLE IF NOT EXISTS conflict_log (
                conflict_id VARCHAR PRIMARY KEY,
                dataset VARCHAR NOT NULL,
                instrument_id VARCHAR NOT NULL,
                partition_key VARCHAR NOT NULL,
                field VARCHAR NOT NULL,
                provider_a VARCHAR NOT NULL,
                provider_b VARCHAR NOT NULL,
                value_a VARCHAR NOT NULL,
                value_b VARCHAR NOT NULL,
                diff_ratio VARCHAR NOT NULL,
                severity VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS provider_status (
                provider VARCHAR NOT NULL,
                endpoint VARCHAR NOT NULL,
                market VARCHAR NOT NULL,
                asset_type VARCHAR NOT NULL,
                dataset VARCHAR NOT NULL,
                capability_version VARCHAR NOT NULL,
                enabled BOOLEAN NOT NULL,
                last_success_at TIMESTAMPTZ,
                last_failure_at TIMESTAMPTZ,
                opened_at TIMESTAMPTZ,
                open_until TIMESTAMPTZ,
                cooldown_seconds BIGINT NOT NULL,
                consecutive_failures BIGINT NOT NULL,
                last_probe_at TIMESTAMPTZ,
                probe_expires_at TIMESTAMPTZ,
                last_success_coverage DOUBLE,
                http_403_count BIGINT NOT NULL,
                http_429_count BIGINT NOT NULL,
                last_failure_class VARCHAR,
                last_error VARCHAR,
                PRIMARY KEY (provider, endpoint, market, asset_type, dataset, capability_version)
            );
            CREATE TABLE IF NOT EXISTS capability_registry (
                provider VARCHAR NOT NULL,
                endpoint VARCHAR NOT NULL,
                capability_version VARCHAR NOT NULL,
                dataset VARCHAR NOT NULL,
                market VARCHAR NOT NULL,
                asset_type VARCHAR NOT NULL,
                code_prefix VARCHAR NOT NULL,
                frequency VARCHAR NOT NULL,
                adjustment VARCHAR NOT NULL,
                validated_at TIMESTAMPTZ NOT NULL,
                validation_expires_at TIMESTAMPTZ NOT NULL,
                status VARCHAR NOT NULL,
                eligible_for_selection BOOLEAN NOT NULL,
                row_count BIGINT NOT NULL,
                first_key VARCHAR,
                last_key VARCHAR,
                evidence_hash VARCHAR NOT NULL,
                request_scope_json VARCHAR NOT NULL DEFAULT '[]',
                response_status INTEGER,
                returned_window VARCHAR,
                field_semantics_json VARCHAR NOT NULL DEFAULT '[]',
                units_json VARCHAR NOT NULL DEFAULT '[]',
                failure_class VARCHAR,
                message VARCHAR,
                PRIMARY KEY (
                    provider, endpoint, capability_version, dataset, market,
                    asset_type, code_prefix, frequency, adjustment
                )
            );
            CREATE TABLE IF NOT EXISTS minute_partition_status (
                instrument_id VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                interval_minutes INTEGER NOT NULL,
                adjustment VARCHAR NOT NULL,
                expected_rows BIGINT NOT NULL,
                actual_rows BIGINT NOT NULL,
                coverage_ratio DOUBLE NOT NULL,
                first_timestamp TIMESTAMPTZ,
                last_timestamp TIMESTAMPTZ,
                status VARCHAR NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (instrument_id, trade_date, interval_minutes, adjustment)
            );
            CREATE TABLE IF NOT EXISTS dividend_event (
                source_security_code VARCHAR NOT NULL,
                ex_dividend_date DATE NOT NULL,
                record_date DATE,
                pretax_bonus_rmb DECIMAL_TEXT,
                bonus_ratio DECIMAL_TEXT,
                transfer_ratio DECIMAL_TEXT,
                assignment_progress VARCHAR,
                notice_date DATE,
                source_provider VARCHAR NOT NULL,
                endpoint VARCHAR NOT NULL,
                capability_version VARCHAR NOT NULL,
                raw_object_path VARCHAR NOT NULL,
                fetch_time TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (source_security_code, ex_dividend_date)
            );
            """
        )
        # A new SQLite file gets only the current schema. Legacy database
        # formats and older SQLite schemas require an explicit migration tool.

    def save_collection_task(self, state: dict) -> None:
        """Durable business progress in the existing database, not a second queue."""
        from .raw import sanitized_metadata
        if state["status"] not in {"waiting", "collecting", "checking", "committing", "failed", "published"}:
            raise ValueError("invalid collection task status")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            previous = self.connection.execute(
                "SELECT definition_hash, first_execution_order FROM collection_task WHERE task_id=?", [state["task_id"]]
            ).fetchone()
            if previous and previous[0] != state["definition_hash"]:
                raise ValueError("task definition is immutable; use a new task ID")
            if previous and previous[1] is not None:
                state = {**state, "first_execution_order": int(previous[1])}
            state_json = json.dumps(sanitized_metadata(state), ensure_ascii=False, default=str)
            self.connection.execute(
                """INSERT INTO collection_task (
                       task_id, definition_hash, dataset, status, state_json, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(task_id) DO UPDATE SET status=excluded.status,
                       state_json=excluded.state_json, updated_at=excluded.updated_at""",
                [state["task_id"], state["definition_hash"], state["dataset"], state["status"],
                 state_json, datetime.now(timezone.utc)],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    @staticmethod
    def _scope_overlaps(left: dict, right: dict) -> tuple[dict, ...]:
        if left.get("dataset") != right.get("dataset") or left.get("frequency") != right.get("frequency"):
            return ()
        left_by_date = left.get("securities_by_date", {})
        right_by_date = right.get("securities_by_date", {})
        conflicts = []
        for trade_date in sorted(set(left_by_date) & set(right_by_date)):
            left_ids, right_ids = set(left_by_date[trade_date]), set(right_by_date[trade_date])
            if "*" in left_ids:
                common_ids = tuple(sorted(right_ids))
            elif "*" in right_ids:
                common_ids = tuple(sorted(left_ids))
            else:
                common_ids = tuple(sorted(left_ids & right_ids))
            if common_ids:
                conflicts.append({"trade_date": trade_date, "securities": common_ids})
        return tuple(conflicts)

    @staticmethod
    def _scope_is_subset(candidate: dict, frozen: dict) -> bool:
        if candidate.get("dataset") != frozen.get("dataset") or candidate.get("frequency") != frozen.get("frequency"):
            return False
        candidate_by_date = candidate.get("securities_by_date", {})
        frozen_by_date = frozen.get("securities_by_date", {})
        for day, identities in candidate_by_date.items():
            allowed = set(frozen_by_date.get(day, ()))
            requested = set(identities)
            if not allowed or ("*" not in allowed and ("*" in requested or not requested <= allowed)):
                return False
        return True

    def partition_registry_snapshot(self, scope: dict) -> str:
        """Hash only the small SQLite publication registrations relevant to a scope."""
        dates = tuple(sorted(scope.get("securities_by_date", {})))
        assets = tuple(sorted(set(scope.get("asset_types", ()))))
        if not dates:
            return row_hash(())
        date_placeholders = ",".join("?" for _ in dates)
        clauses = [f"partition_key IN ({date_placeholders})"]
        parameters: list[object] = [*dates]
        if assets:
            clauses.append(f"asset_type IN ({','.join('?' for _ in assets)})")
            parameters.extend(assets)
        rows = self.connection.execute(
            """SELECT dataset, asset_type, partition_key, adjustment, status,
                      manifest_path, content_hash, updated_at
               FROM partition_status WHERE dataset=? AND """ + " AND ".join(clauses)
            + " ORDER BY dataset, asset_type, partition_key, adjustment",
            [scope["dataset"], *parameters],
        ).fetchall()
        return row_hash(rows)

    def enqueue_collection_task(self, state: dict, scope: dict, *, snapshot: dict,
                                frozen_scope: dict | None = None) -> dict:
        """Atomically validate a frozen scope and persist its FIFO position in the task row."""
        from .raw import sanitized_metadata

        by_date = scope.get("securities_by_date")
        if (not scope.get("dataset") or not scope.get("frequency") or not isinstance(by_date, dict)
                or not by_date or any(not identities for identities in by_date.values())):
            raise ValueError("collection scope requires dated, nonempty stable security identities")
        task_id = state["task_id"]
        frozen_scope_json = json.dumps(frozen_scope or scope, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        active_scope_json = json.dumps(scope, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        snapshot_json = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        state = {**state, "status": "waiting"}
        state_json = json.dumps(sanitized_metadata(state), ensure_ascii=False, default=str)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                "SELECT definition_hash, status, state_json, queue_order, first_accepted_at, scope_json, snapshot_json, first_execution_order "
                "FROM collection_task WHERE task_id=?", [task_id]
            ).fetchone()
            if existing:
                if existing[0] != state["definition_hash"]:
                    raise ValueError("task ID is frozen to a different definition")
                if existing[5] and not self._scope_is_subset(frozen_scope or scope, json.loads(existing[5])):
                    raise ValueError("task ID retry scope is outside its frozen collection scope")
                if existing[1] in {"waiting", "collecting", "checking", "committing"}:
                    self.connection.execute("COMMIT")
                    state = {**json.loads(existing[2]), "queue_order": existing[3], "already_queued": True}
                    if existing[7] is not None:
                        state["first_execution_order"] = int(existing[7])
                    return state

            if snapshot.get("registry_hash") != self.partition_registry_snapshot(scope):
                raise CollectionTaskSnapshotChanged("published partition registrations changed; preview and confirm again")

            active_rows = self.connection.execute(
                "SELECT task_id, active_scope_json FROM collection_task "
                "WHERE status IN ('waiting','collecting','checking','committing') AND task_id<>? ORDER BY queue_order, task_id",
                [task_id],
            ).fetchall()
            for active_id, active_scope in active_rows:
                if active_scope:
                    conflicts = self._scope_overlaps(scope, json.loads(active_scope))
                    if conflicts:
                        raise CollectionTaskScopeConflict(active_id, conflicts)

            next_order = self.connection.execute(
                "SELECT coalesce(max(queue_order), 0) + 1 FROM collection_task"
            ).fetchone()[0]
            first_accepted_at = existing[4] if existing else datetime.now(timezone.utc)
            queued_at = datetime.now(timezone.utc)
            state.update(queue_order=next_order, first_accepted_at=first_accepted_at.isoformat(),
                         queued_at=queued_at.isoformat())
            state_json = json.dumps(sanitized_metadata(state), ensure_ascii=False, default=str)
            self.connection.execute(
                """INSERT INTO collection_task (
                       task_id, definition_hash, dataset, status, state_json, updated_at,
                       queue_order, first_accepted_at, queued_at, scope_json, snapshot_json, active_scope_json
                   ) VALUES (?, ?, ?, 'waiting', ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(task_id) DO UPDATE SET
                       status='waiting', state_json=excluded.state_json, updated_at=excluded.updated_at,
                       queue_order=excluded.queue_order, queued_at=excluded.queued_at,
                       active_scope_json=excluded.active_scope_json""",
                [task_id, state["definition_hash"], state["dataset"], state_json, queued_at,
                 next_order, first_accepted_at, queued_at, frozen_scope_json, snapshot_json, active_scope_json],
            )
            self.connection.execute("COMMIT")
            return state
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def claim_next_collection_task(self) -> dict | None:
        """With collection.lock held, return the active head or promote one FIFO waiter."""
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                    "SELECT task_id, status, state_json, first_execution_order FROM collection_task "
                "WHERE status IN ('committing','collecting','checking') AND queue_order IS NOT NULL "
                "ORDER BY queue_order, task_id LIMIT 1"
            ).fetchone()
            if row is None:
                row = self.connection.execute(
                    "SELECT task_id, status, state_json, first_execution_order FROM collection_task WHERE status='waiting' "
                    "AND queue_order IS NOT NULL ORDER BY queue_order, task_id LIMIT 1"
                ).fetchone()
            if row is None:
                self.connection.execute("COMMIT")
                return None
            task_id, status, state_json, first_execution_order = row
            state = json.loads(state_json)
            if first_execution_order is None:
                first_execution_order = self.connection.execute(
                    "SELECT coalesce(max(first_execution_order), 0) + 1 FROM collection_task"
                ).fetchone()[0]
                self.connection.execute(
                    "UPDATE collection_task SET first_execution_order=? WHERE task_id=? AND first_execution_order IS NULL",
                    [first_execution_order, task_id],
                )
            state["first_execution_order"] = first_execution_order
            state_json = json.dumps(state, ensure_ascii=False, default=str)
            self.connection.execute("UPDATE collection_task SET state_json=? WHERE task_id=?", [state_json, task_id])
            if status == "waiting":
                state["status"] = "collecting"
                state_json = json.dumps(state, ensure_ascii=False, default=str)
                self.connection.execute(
                    "UPDATE collection_task SET status='collecting', state_json=?, updated_at=? WHERE task_id=?",
                    [json.dumps(state, ensure_ascii=False, default=str), datetime.now(timezone.utc), task_id],
                )
            self.connection.execute("COMMIT")
            return state
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def load_collection_task(self, task_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT state_json,first_execution_order FROM collection_task WHERE task_id=?", [task_id]
        ).fetchone()
        if row is None:
            return None
        state = json.loads(row[0])
        if row[1] is not None:
            state["first_execution_order"] = int(row[1])
        return state

    def collection_tasks(self, *, status: str | None = None) -> tuple[dict, ...]:
        rows = self.connection.execute(
            "SELECT state_json,first_execution_order FROM collection_task" + (" WHERE status=?" if status else "")
            + " ORDER BY task_id", [status] if status else [],
        ).fetchall()
        result = []
        for row in rows:
            state = json.loads(row[0])
            if row[1] is not None:
                state["first_execution_order"] = int(row[1])
            result.append(state)
        return tuple(result)

    def save_dividend_events(self, events: list[DividendEvent]) -> int:
        for event in events:
            self.connection.execute(
                """
                INSERT INTO dividend_event VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (source_security_code, ex_dividend_date) DO UPDATE SET
                    record_date=excluded.record_date,
                    pretax_bonus_rmb=excluded.pretax_bonus_rmb,
                    bonus_ratio=excluded.bonus_ratio,
                    transfer_ratio=excluded.transfer_ratio,
                    assignment_progress=excluded.assignment_progress,
                    notice_date=excluded.notice_date,
                    source_provider=excluded.source_provider,
                    endpoint=excluded.endpoint,
                    capability_version=excluded.capability_version,
                    raw_object_path=excluded.raw_object_path,
                    fetch_time=excluded.fetch_time
                """,
                [
                    event.source_security_code, event.ex_dividend_date, event.record_date,
                    event.pretax_bonus_rmb, event.bonus_ratio, event.transfer_ratio,
                    event.assignment_progress, event.notice_date, event.source_provider,
                    event.endpoint, event.capability_version, event.raw_object_path, event.fetch_time,
                ],
            )
        return len(events)

    def dividend_events(
        self, *, start_date: date | None = None, end_date: date | None = None
    ) -> list[dict[str, object]]:
        clauses = []
        params: list[object] = []
        if start_date is not None:
            clauses.append("ex_dividend_date >= ?")
            params.append(start_date)
        if end_date is not None:
            clauses.append("ex_dividend_date <= ?")
            params.append(end_date)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cursor = self.connection.execute(
            f"SELECT * FROM dividend_event {where} ORDER BY ex_dividend_date, source_security_code",
            params,
        )
        return [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]

    def save_attempt(self, attempt: CollectionAttempt, *, updated_at: datetime | None = None) -> None:
        now = updated_at or datetime.now(timezone.utc)
        self.connection.execute(
            """
            INSERT INTO collection_attempt VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (attempt_id) DO UPDATE SET
                status=excluded.status,
                lease_owner=excluded.lease_owner,
                lease_acquired_at=excluded.lease_acquired_at,
                lease_expires_at=excluded.lease_expires_at,
                raw_object_path=excluded.raw_object_path,
                raw_content_hash=excluded.raw_content_hash,
                updated_at=excluded.updated_at
            """,
            [
                attempt.attempt_id,
                attempt.status.value,
                attempt.lease_owner,
                attempt.lease_acquired_at,
                attempt.lease_expires_at,
                attempt.raw_object_path,
                attempt.raw_content_hash,
                now,
            ],
        )

    def claim_attempt(self, attempt: CollectionAttempt, *, updated_at: datetime) -> bool:
        """Reserve a new scheduler slot atomically using the existing attempt table."""
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            if self.load_attempt(attempt.attempt_id) is not None:
                self.connection.execute("ROLLBACK")
                return False
            self.save_attempt(attempt, updated_at=updated_at)
            self.connection.execute("COMMIT")
            return True
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def load_attempt(self, attempt_id: str) -> CollectionAttempt | None:
        row = self.connection.execute(
            """SELECT attempt_id, status, lease_owner, lease_acquired_at,
                      lease_expires_at, raw_object_path, raw_content_hash
               FROM collection_attempt WHERE attempt_id=?""",
            [attempt_id],
        ).fetchone()
        if row is None:
            return None
        return CollectionAttempt(
            attempt_id=row[0],
            status=AttemptStatus(row[1]),
            lease_owner=row[2],
            lease_acquired_at=row[3],
            lease_expires_at=row[4],
            raw_object_path=row[5],
            raw_content_hash=row[6],
        )

    def item_statuses(self, dataset: str, partition_key: str, *, adjustment: str = "none") -> dict[str, ItemStatus]:
        rows = self.connection.execute(
            """SELECT instrument_id, status FROM partition_item
               WHERE dataset=? AND partition_key=? AND adjustment=?""",
            [dataset, partition_key, adjustment],
        ).fetchall()
        return {instrument_id: ItemStatus(status) for instrument_id, status in rows}

    def record_partition_publish(
        self,
        *,
        manifest: Manifest,
        manifest_path: str | Path,
        item_statuses: Mapping[str, ItemStatus] | None = None,
        source_providers: Mapping[str, str | None] | None = None,
        updated_at: datetime | None = None,
    ) -> None:
        now = updated_at or datetime.now(timezone.utc)
        resolved_statuses = (
            dict(item_statuses)
            if item_statuses is not None
            else {key: ItemStatus(value) for key, value in manifest.item_statuses.items()}
        )
        resolved_providers = dict(source_providers or manifest.source_providers)
        unresolved = {
            ItemStatus.MISSING,
            ItemStatus.INVALID,
            ItemStatus.TEMPORARY_EMPTY,
        }
        conflict_states = {ItemStatus.CONFLICT, ItemStatus.QUARANTINED}
        if len(resolved_statuses) < manifest.expected_count or any(
            status in unresolved for status in resolved_statuses.values()
        ):
            status = "partial"
        elif (
            manifest.conflict_count
            or manifest.quarantined_count
            or any(status in conflict_states for status in resolved_statuses.values())
        ):
            status = "complete_with_conflicts"
        else:
            status = "complete_clean"
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                """
                INSERT INTO partition_status (
                    dataset, asset_type, partition_key, adjustment, expected_count, actual_count,
                    status, manifest_path, content_hash, conflict_count, quarantined_count, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (dataset, asset_type, partition_key, adjustment) DO UPDATE SET
                    expected_count=excluded.expected_count,
                    actual_count=excluded.actual_count,
                    status=excluded.status,
                    manifest_path=excluded.manifest_path,
                    content_hash=excluded.content_hash,
                    conflict_count=excluded.conflict_count,
                    quarantined_count=excluded.quarantined_count,
                    updated_at=excluded.updated_at
                """,
                [
                    manifest.dataset,
                    manifest.asset_type,
                    manifest.partition_key,
                    manifest.adjustment,
                    manifest.expected_count,
                    manifest.row_count,
                    status,
                    str(manifest_path),
                    manifest.content_hash,
                    manifest.conflict_count,
                    manifest.quarantined_count,
                    now,
                ],
            )
            if resolved_statuses:
                # Keep all item updates in the same transaction as the summary.
                instrument_ids = list(resolved_statuses)
                self.connection.executemany(
                    """INSERT INTO partition_item (
                           dataset, partition_key, adjustment, instrument_id, status, source_provider, asset_type,
                           business_reason, evidence_ref, last_publication_id, updated_at
                       ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
                       ON CONFLICT (dataset, partition_key, adjustment, instrument_id) DO UPDATE SET
                        status=excluded.status,
                        source_provider=excluded.source_provider,
                        updated_at=excluded.updated_at
                    """,
                    ([
                        manifest.dataset,
                        manifest.partition_key,
                        manifest.adjustment,
                        key,
                        resolved_statuses[key].value,
                        resolved_providers.get(key),
                        now,
                    ] for key in instrument_ids)
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def partition(self, dataset: str, asset_type: str, partition_key: str,
                  *, adjustment: str = "none") -> dict[str, object] | None:
        cursor = self.connection.execute(
            """SELECT * FROM partition_status
               WHERE dataset=? AND asset_type=? AND partition_key=? AND adjustment=?""",
            [dataset, asset_type, partition_key, adjustment],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([column[0] for column in cursor.description], row))

    def latest_partition(self, *, dataset: str, asset_type: str, through_date: str,
                         adjustment: str = "none") -> dict[str, object] | None:
        """Locate a dated publication through the existing index, never a current-directory alias."""
        cursor = self.connection.execute(
            """SELECT * FROM partition_status WHERE dataset=? AND asset_type=?
               AND partition_key <= ? AND status IN ('complete_clean', 'partial')
               AND adjustment=? ORDER BY partition_key DESC LIMIT 1""", [dataset, asset_type, through_date, adjustment])
        row = cursor.fetchone()
        return dict(zip([column[0] for column in cursor.description], row)) if row else None

    def record_conflicts(
        self,
        *,
        dataset: str,
        partition_key: str,
        conflicts: tuple[Conflict, ...],
        created_at: datetime | None = None,
    ) -> None:
        now = created_at or datetime.now(timezone.utc)
        for conflict in conflicts:
            instrument_id = str(conflict.canonical_key[0])
            conflict_id = row_hash(
                {
                    "dataset": dataset,
                    "partition_key": partition_key,
                    "canonical_key": conflict.canonical_key,
                    "field": conflict.field,
                    "provider_a": conflict.provider_a,
                    "provider_b": conflict.provider_b,
                    "value_a": conflict.value_a,
                    "value_b": conflict.value_b,
                }
            )
            self.connection.execute(
                """
                INSERT INTO conflict_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (conflict_id) DO NOTHING
                """,
                [
                    conflict_id,
                    dataset,
                    instrument_id,
                    partition_key,
                    conflict.field,
                    conflict.provider_a,
                    conflict.provider_b,
                    str(conflict.value_a),
                    str(conflict.value_b),
                    str(conflict.diff_ratio),
                    conflict.severity,
                    now,
                ],
            )

    def conflict_count(self, dataset: str, partition_key: str) -> int:
        row = self.connection.execute(
            "SELECT count(*) FROM conflict_log WHERE dataset=? AND partition_key=?",
            [dataset, partition_key],
        ).fetchone()
        return int(row[0]) if row is not None else 0

    def provider_health(
        self,
        *,
        provider: str,
        endpoint: str,
        market: str,
        asset_type: str,
        dataset: str,
        capability_version: str,
    ) -> ProviderHealth | None:
        row = self.connection.execute(
            """
            SELECT provider, endpoint, market, asset_type, dataset, capability_version,
                   enabled, last_success_at, last_failure_at, opened_at, open_until,
                   consecutive_failures, last_probe_at, probe_expires_at,
                   last_success_coverage, http_403_count, http_429_count,
                   last_failure_class, last_error
            FROM provider_status
            WHERE provider=? AND endpoint=? AND market=? AND asset_type=?
              AND dataset=? AND capability_version=?
            """,
            [provider, endpoint, market, asset_type, dataset, capability_version],
        ).fetchone()
        return ProviderHealth(*row) if row is not None else None

    def record_provider_failure(
        self,
        *,
        provider: str,
        endpoint: str,
        market: str,
        asset_type: str,
        dataset: str,
        capability_version: str,
        failure_class: str,
        error: str,
        now: datetime,
        cooldown_seconds: int,
        failure_threshold: int = 3,
        http_status: int | None = None,
    ) -> ProviderHealth:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            current = self.provider_health(
                provider=provider,
                endpoint=endpoint,
                market=market,
                asset_type=asset_type,
                dataset=dataset,
                capability_version=capability_version,
            )
            failures = (current.consecutive_failures if current else 0) + 1
            immediate_open = http_status in {403, 429} or failure_class == "rate_limited"
            should_open = immediate_open or failures >= failure_threshold
            opened_at = now if should_open else (current.opened_at if current else None)
            open_until = (
                now + timedelta(seconds=cooldown_seconds)
                if should_open
                else (current.open_until if current else None)
            )
            self.connection.execute(
                """
                INSERT INTO provider_status VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (provider, endpoint, market, asset_type, dataset, capability_version)
                DO UPDATE SET
                    last_failure_at=excluded.last_failure_at,
                    opened_at=excluded.opened_at,
                    open_until=excluded.open_until,
                    cooldown_seconds=excluded.cooldown_seconds,
                    consecutive_failures=excluded.consecutive_failures,
                    http_403_count=excluded.http_403_count,
                    http_429_count=excluded.http_429_count,
                    last_failure_class=excluded.last_failure_class,
                    last_error=excluded.last_error
                """,
                [
                    provider,
                    endpoint,
                    market,
                    asset_type,
                    dataset,
                    capability_version,
                    current.enabled if current else True,
                    current.last_success_at if current else None,
                    now,
                    opened_at,
                    open_until,
                    cooldown_seconds,
                    failures,
                    current.last_probe_at if current else None,
                    current.probe_expires_at if current else None,
                    current.last_success_coverage if current else None,
                    (current.http_403_count if current else 0) + (1 if http_status == 403 else 0),
                    (current.http_429_count if current else 0) + (1 if http_status == 429 else 0),
                    failure_class,
                    error,
                ],
            )
            result = self.provider_health(
                provider=provider,
                endpoint=endpoint,
                market=market,
                asset_type=asset_type,
                dataset=dataset,
                capability_version=capability_version,
            )
            assert result is not None
            self.connection.execute("COMMIT")
            return result
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def record_provider_success(
        self,
        *,
        provider: str,
        endpoint: str,
        market: str,
        asset_type: str,
        dataset: str,
        capability_version: str,
        now: datetime,
        coverage: float,
        probe_ttl: timedelta | None = None,
    ) -> ProviderHealth:
        if not 0 <= coverage <= 1:
            raise ValueError("coverage must be between zero and one")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            current = self.provider_health(
                provider=provider,
                endpoint=endpoint,
                market=market,
                asset_type=asset_type,
                dataset=dataset,
                capability_version=capability_version,
            )
            probe_success = probe_ttl is not None
            self.connection.execute(
                """
                INSERT INTO provider_status VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (provider, endpoint, market, asset_type, dataset, capability_version)
                DO UPDATE SET
                    last_success_at=excluded.last_success_at,
                    opened_at=excluded.opened_at,
                    open_until=excluded.open_until,
                    consecutive_failures=0,
                    last_probe_at=excluded.last_probe_at,
                    probe_expires_at=excluded.probe_expires_at,
                    last_success_coverage=excluded.last_success_coverage,
                    last_error=NULL
                """,
                [
                    provider,
                    endpoint,
                    market,
                    asset_type,
                    dataset,
                    capability_version,
                    current.enabled if current else True,
                    now,
                    current.last_failure_at if current else None,
                    None if probe_success else (current.opened_at if current else None),
                    None if probe_success else (current.open_until if current else None),
                    0,
                    0,
                    now if probe_success else (current.last_probe_at if current else None),
                    now + probe_ttl if probe_success and probe_ttl is not None else (current.probe_expires_at if current else None),
                    coverage,
                    current.http_403_count if current else 0,
                    current.http_429_count if current else 0,
                    current.last_failure_class if current else None,
                    None,
                ],
            )
            result = self.provider_health(
                provider=provider,
                endpoint=endpoint,
                market=market,
                asset_type=asset_type,
                dataset=dataset,
                capability_version=capability_version,
            )
            assert result is not None
            self.connection.execute("COMMIT")
            return result
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    @staticmethod
    def provider_available(health: ProviderHealth | None, now: datetime, *, for_probe: bool = False) -> bool:
        if health is None:
            return True
        if not health.enabled:
            return False
        if health.opened_at is None:
            return True
        if health.open_until is not None and now < health.open_until:
            return False
        return for_probe

    def save_probe_evidence(
        self,
        evidence: ProbeEvidence,
        *,
        dataset: str,
        market: str,
        asset_type: str,
        code_prefix: str = "",
        frequency: str = "daily",
        adjustment: str = "none",
    ) -> None:
        if evidence.adjustment != adjustment:
            raise ValueError(
                "probe evidence adjustment does not match capability registry adjustment"
            )
        self.connection.execute(
            """
            INSERT INTO capability_registry (
                provider, endpoint, capability_version, dataset, market, asset_type,
                code_prefix, frequency, adjustment, validated_at,
                validation_expires_at, status, eligible_for_selection, row_count,
                first_key, last_key, evidence_hash, request_scope_json,
                response_status, returned_window, field_semantics_json, units_json,
                failure_class, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (
                provider, endpoint, capability_version, dataset, market,
                asset_type, code_prefix, frequency, adjustment
            ) DO UPDATE SET
                validated_at=excluded.validated_at,
                validation_expires_at=excluded.validation_expires_at,
                status=excluded.status,
                eligible_for_selection=excluded.eligible_for_selection,
                row_count=excluded.row_count,
                first_key=excluded.first_key,
                last_key=excluded.last_key,
                 evidence_hash=excluded.evidence_hash,
                 request_scope_json=excluded.request_scope_json,
                 response_status=excluded.response_status,
                 returned_window=excluded.returned_window,
                 field_semantics_json=excluded.field_semantics_json,
                 units_json=excluded.units_json,
                 failure_class=excluded.failure_class,
                message=excluded.message
            """,
            [
                evidence.provider,
                evidence.endpoint,
                evidence.capability_version,
                dataset,
                market,
                asset_type,
                code_prefix,
                frequency,
                adjustment,
                evidence.validated_at,
                evidence.validation_expires_at,
                evidence.status,
                evidence.eligible_for_selection,
                evidence.row_count,
                evidence.first_key,
                evidence.last_key,
                evidence.evidence_hash,
                json.dumps(evidence.request_scope, ensure_ascii=False),
                evidence.response_status,
                evidence.returned_window,
                json.dumps(evidence.field_semantics, ensure_ascii=False),
                json.dumps(evidence.units, ensure_ascii=False),
                evidence.failure_class,
                evidence.message,
            ],
        )

    def capability_evidence(self, *, provider: str, endpoint: str, capability_version: str) -> tuple[dict[str, object], ...]:
        """Read the existing evidence rows without manufacturing validation dates."""
        cursor = self.connection.execute(
            """SELECT * FROM capability_registry
               WHERE provider=? AND endpoint=? AND capability_version=?
               ORDER BY dataset, market, asset_type, frequency, adjustment, code_prefix""",
            [provider, endpoint, capability_version],
        )
        columns = [column[0] for column in cursor.description]
        return tuple(dict(zip(columns, row)) for row in cursor.fetchall())

    def latest_probe(
        self,
        *,
        provider: str,
        endpoint: str,
        capability_version: str,
        dataset: str,
        market: str,
        asset_type: str,
        code_prefix: str = "",
        frequency: str = "daily",
        adjustment: str = "none",
    ) -> dict[str, object] | None:
        cursor = self.connection.execute(
            """
            SELECT * FROM capability_registry
            WHERE provider=? AND endpoint=? AND capability_version=? AND dataset=?
              AND market=? AND asset_type=? AND code_prefix=? AND frequency=?
              AND adjustment=?
            """,
            [
                provider,
                endpoint,
                capability_version,
                dataset,
                market,
                asset_type,
                code_prefix,
                frequency,
                adjustment,
            ],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([column[0] for column in cursor.description], row))

    def record_minute_completeness(
        self,
        *,
        instrument_id: str,
        trade_date: date,
        interval_minutes: int,
        adjustment: str,
        expected_rows: int,
        actual_rows: int,
        first_timestamp: datetime | None,
        last_timestamp: datetime | None,
        status: str | None = None,
        updated_at: datetime | None = None,
    ) -> None:
        if expected_rows < 0 or actual_rows < 0:
            raise ValueError("minute row counts cannot be negative")
        coverage = actual_rows / expected_rows if expected_rows else 1.0
        resolved_status = status or (
            "complete" if actual_rows >= expected_rows else "partial"
        )
        self.connection.execute(
            """
            INSERT INTO minute_partition_status VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (instrument_id, trade_date, interval_minutes, adjustment)
            DO UPDATE SET expected_rows=excluded.expected_rows,
                actual_rows=excluded.actual_rows, coverage_ratio=excluded.coverage_ratio,
                first_timestamp=excluded.first_timestamp, last_timestamp=excluded.last_timestamp,
                status=excluded.status, updated_at=excluded.updated_at
            """,
            [
                instrument_id,
                trade_date,
                interval_minutes,
                adjustment,
                expected_rows,
                actual_rows,
                coverage,
                first_timestamp,
                last_timestamp,
                resolved_status,
                updated_at or datetime.now(timezone.utc),
            ],
        )

    def minute_completeness(
        self, instrument_id: str, trade_date: date, interval_minutes: int, adjustment: str
    ) -> dict[str, object] | None:
        cursor = self.connection.execute(
            """SELECT * FROM minute_partition_status
               WHERE instrument_id=? AND trade_date=? AND interval_minutes=? AND adjustment=?""",
            [instrument_id, trade_date, interval_minutes, adjustment],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([column[0] for column in cursor.description], row))

    def close(self) -> None:
        self.connection.close()

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
