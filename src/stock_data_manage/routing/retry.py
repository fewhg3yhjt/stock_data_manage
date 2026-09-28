from __future__ import annotations

from dataclasses import dataclass
from time import sleep as default_sleep
from typing import Callable, TypeVar

from ..providers.contracts import FailureClass, ProviderContractError


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    retry_wait_seconds: float = 1.0
    backoff_factor: float = 1.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if self.retry_wait_seconds < 0 or self.backoff_factor < 1:
            raise ValueError("retry waits must be non-negative and backoff_factor >= 1")


@dataclass(frozen=True, slots=True)
class RetryAttempt:
    attempt_number: int
    failure_class: str
    message: str


@dataclass(frozen=True, slots=True)
class RetryResult:
    value: object
    attempts: tuple[RetryAttempt, ...]


def execute_with_retry(
    operation: Callable[[], T],
    *,
    policy: RetryPolicy | None = None,
    sleep: Callable[[float], None] = default_sleep,
    on_failure: Callable[[RetryAttempt], None] | None = None,
) -> RetryResult:
    policy = policy or RetryPolicy()
    failures: list[RetryAttempt] = []
    for attempt_number in range(1, policy.max_attempts + 1):
        try:
            return RetryResult(operation(), tuple(failures))
        except Exception as exc:
            if isinstance(exc, ProviderContractError):
                failure_class = exc.failure_class.value
                retryable = exc.retryable
            else:
                failure_class = FailureClass.CONNECTION.value
                retryable = True
            failure = RetryAttempt(attempt_number, failure_class, str(exc))
            failures.append(failure)
            if on_failure is not None:
                on_failure(failure)
            if not retryable or attempt_number >= policy.max_attempts:
                raise
            sleep(policy.retry_wait_seconds * (policy.backoff_factor ** (attempt_number - 1)))
    raise AssertionError("retry loop must return or raise")


@dataclass(frozen=True, slots=True)
class FallbackResult:
    provider: str
    value: object
    attempts: tuple[RetryAttempt, ...]
    failed_providers: tuple[str, ...]


def execute_with_fallback(
    providers: list[tuple[str, Callable[[], T]]],
    *,
    retry_policy: RetryPolicy | None = None,
    sleep: Callable[[float], None] = default_sleep,
) -> FallbackResult:
    failures: list[str] = []
    attempts: list[RetryAttempt] = []
    for provider, operation in providers:
        try:
            result = execute_with_retry(operation, policy=retry_policy, sleep=sleep)
            attempts.extend(result.attempts)
            return FallbackResult(provider, result.value, tuple(attempts), tuple(failures))
        except Exception:
            failures.append(provider)
    raise RuntimeError(f"all providers failed: {', '.join(failures)}")
