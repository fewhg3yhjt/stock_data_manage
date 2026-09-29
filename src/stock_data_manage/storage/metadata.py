from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Mapping

import duckdb

from ..providers.probes import ProbeEvidence
from ..quality.resolution import Conflict
from ..domain import AttemptStatus, DividendEvent, ItemStatus
from ..worker.attempts import CollectionAttempt
from .integrity import Manifest, row_hash


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
    def __init__(self, path: str | Path = ":memory:") -> None:
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(path))
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_status (
                dataset VARCHAR NOT NULL,
                asset_type VARCHAR NOT NULL,
                partition_key VARCHAR NOT NULL,
                expected_count BIGINT NOT NULL,
                actual_count BIGINT NOT NULL,
                status VARCHAR NOT NULL,
                manifest_path VARCHAR NOT NULL,
                content_hash VARCHAR NOT NULL,
                conflict_count BIGINT NOT NULL,
                quarantined_count BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (dataset, asset_type, partition_key)
            );
            CREATE TABLE IF NOT EXISTS partition_item (
                dataset VARCHAR NOT NULL,
                partition_key VARCHAR NOT NULL,
                instrument_id VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                source_provider VARCHAR,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (dataset, partition_key, instrument_id)
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
                pretax_bonus_rmb DECIMAL(38, 12),
                bonus_ratio DECIMAL(38, 12),
                transfer_ratio DECIMAL(38, 12),
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
        for statement in (
            "ALTER TABLE capability_registry ADD COLUMN IF NOT EXISTS request_scope_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE capability_registry ADD COLUMN IF NOT EXISTS response_status INTEGER",
            "ALTER TABLE capability_registry ADD COLUMN IF NOT EXISTS returned_window VARCHAR",
            "ALTER TABLE capability_registry ADD COLUMN IF NOT EXISTS field_semantics_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE capability_registry ADD COLUMN IF NOT EXISTS units_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE capability_registry ADD COLUMN IF NOT EXISTS adjustment VARCHAR DEFAULT 'none'",
        ):
            self.connection.execute(statement)

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

    def item_statuses(self, dataset: str, partition_key: str) -> dict[str, ItemStatus]:
        rows = self.connection.execute(
            """SELECT instrument_id, status FROM partition_item
               WHERE dataset=? AND partition_key=?""",
            [dataset, partition_key],
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
                INSERT INTO partition_status VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (dataset, asset_type, partition_key) DO UPDATE SET
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
            for instrument_id, item_status in resolved_statuses.items():
                self.connection.execute(
                    """
                    INSERT INTO partition_item VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (dataset, partition_key, instrument_id) DO UPDATE SET
                        status=excluded.status,
                        source_provider=excluded.source_provider,
                        updated_at=excluded.updated_at
                    """,
                    [
                        manifest.dataset,
                        manifest.partition_key,
                        instrument_id,
                        item_status.value,
                        resolved_providers.get(instrument_id),
                        now,
                    ],
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def partition(self, dataset: str, asset_type: str, partition_key: str) -> dict[str, object] | None:
        cursor = self.connection.execute(
            """SELECT * FROM partition_status
               WHERE dataset=? AND asset_type=? AND partition_key=?""",
            [dataset, asset_type, partition_key],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([column[0] for column in cursor.description], row))

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
        return result

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
        return result

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
