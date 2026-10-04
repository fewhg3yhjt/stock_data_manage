from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from ..domain import AttemptStatus


ALLOWED_TRANSITIONS: dict[AttemptStatus, frozenset[AttemptStatus]] = {
    AttemptStatus.PENDING: frozenset({AttemptStatus.LEASED}),
    AttemptStatus.LEASED: frozenset({AttemptStatus.FETCHING, AttemptStatus.RETRYABLE_FAILED}),
    AttemptStatus.FETCHING: frozenset(
        {
            AttemptStatus.RAW_COMMITTED,
            AttemptStatus.TEMPORARY_EMPTY,
            AttemptStatus.RETRYABLE_FAILED,
            AttemptStatus.TERMINAL_FAILED,
        }
    ),
    AttemptStatus.RAW_COMMITTED: frozenset(
        {AttemptStatus.NORMALIZED, AttemptStatus.QUARANTINED}
    ),
    AttemptStatus.NORMALIZED: frozenset(
        {AttemptStatus.VALIDATED, AttemptStatus.QUARANTINED}
    ),
    AttemptStatus.VALIDATED: frozenset(
        {AttemptStatus.PUBLISHED, AttemptStatus.CONFLICT, AttemptStatus.QUARANTINED}
    ),
    AttemptStatus.RETRYABLE_FAILED: frozenset({AttemptStatus.LEASED}),
    AttemptStatus.TEMPORARY_EMPTY: frozenset(
        {AttemptStatus.LEASED, AttemptStatus.CONFIRMED_NO_DATA}
    ),
    AttemptStatus.CONFLICT: frozenset({AttemptStatus.PUBLISHED, AttemptStatus.QUARANTINED}),
}


@dataclass(frozen=True, slots=True)
class CollectionAttempt:
    attempt_id: str
    status: AttemptStatus = AttemptStatus.PENDING
    lease_owner: str | None = None
    lease_acquired_at: datetime | None = None
    lease_expires_at: datetime | None = None
    raw_object_path: str | None = None
    raw_content_hash: str | None = None

    def transition(self, target: AttemptStatus) -> "CollectionAttempt":
        if target not in ALLOWED_TRANSITIONS.get(self.status, frozenset()):
            raise ValueError(f"illegal attempt transition: {self.status} -> {target}")
        return replace(self, status=target)

    def lease(
        self, *, owner: str, acquired_at: datetime, expires_at: datetime
    ) -> "CollectionAttempt":
        if expires_at <= acquired_at:
            raise ValueError("lease expiry must be after acquisition")
        leased = self.transition(AttemptStatus.LEASED)
        return replace(
            leased,
            lease_owner=owner,
            lease_acquired_at=acquired_at,
            lease_expires_at=expires_at,
        )

    def lease_expired(self, now: datetime) -> bool:
        return self.lease_expires_at is not None and now >= self.lease_expires_at

    @property
    def reusable_raw(self) -> bool:
        return bool(
            self.raw_object_path
            and self.raw_content_hash
            and self.status
            in {
                AttemptStatus.RAW_COMMITTED,
                AttemptStatus.NORMALIZED,
                AttemptStatus.VALIDATED,
            }
        )
