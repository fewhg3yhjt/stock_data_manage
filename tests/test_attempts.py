from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from stock_data_manage.attempts import CollectionAttempt
from stock_data_manage.domain import AttemptStatus


def test_attempt_state_machine_and_lease_expiry() -> None:
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    attempt = CollectionAttempt("attempt-1").lease(
        owner="worker-1", acquired_at=now, expires_at=now + timedelta(minutes=5)
    )
    assert not attempt.lease_expired(now + timedelta(minutes=4))
    assert attempt.lease_expired(now + timedelta(minutes=5))
    attempt = attempt.transition(AttemptStatus.FETCHING).transition(AttemptStatus.RAW_COMMITTED)
    attempt = replace(attempt, raw_object_path="raw/a", raw_content_hash="abc")
    assert attempt.reusable_raw
    assert attempt.transition(AttemptStatus.NORMALIZED).transition(AttemptStatus.VALIDATED).transition(
        AttemptStatus.PUBLISHED
    ).status is AttemptStatus.PUBLISHED


def test_http_success_cannot_skip_to_published() -> None:
    attempt = CollectionAttempt("attempt-1")
    with pytest.raises(ValueError, match="illegal"):
        attempt.transition(AttemptStatus.PUBLISHED)


def test_temporary_empty_is_retryable_not_complete() -> None:
    attempt = CollectionAttempt("attempt-1").lease(
        owner="worker",
        acquired_at=datetime(2026, 9, 13, tzinfo=timezone.utc),
        expires_at=datetime(2026, 9, 13, 1, tzinfo=timezone.utc),
    )
    attempt = attempt.transition(AttemptStatus.FETCHING).transition(AttemptStatus.TEMPORARY_EMPTY)
    assert attempt.transition(AttemptStatus.LEASED).status is AttemptStatus.LEASED

