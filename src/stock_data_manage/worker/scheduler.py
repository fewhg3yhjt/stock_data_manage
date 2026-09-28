from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Callable, Iterable
from zoneinfo import ZoneInfo

import yaml


SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class ScheduledJob:
    name: str
    local_time: time


def load_scheduled_jobs(path: str | Path) -> tuple[ScheduledJob, ...]:
    """Load the small, explicit Phase 1 schedule from YAML."""
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    raw_jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(raw_jobs, list):
        raise ValueError("schedule config must contain a jobs list")
    jobs: list[ScheduledJob] = []
    for item in raw_jobs:
        if not isinstance(item, dict) or not item.get("name") or not item.get("time"):
            raise ValueError("each schedule job requires name and time")
        if item.get("enabled", True) is False:
            continue
        try:
            hour, minute, *second = str(item["time"]).split(":")
            local_time = time(int(hour), int(minute), int(second[0]) if second else 0)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid schedule time: {item.get('time')!r}") from exc
        jobs.append(ScheduledJob(str(item["name"]), local_time))
    return tuple(sorted(jobs, key=lambda job: (job.local_time, job.name)))


DEFAULT_JOBS = (
    ScheduledJob("security_master_update", time(8, 0)),
    ScheduledJob("daily_reconciliation", time(8, 30)),
    ScheduledJob("realtime_minute_start", time(9, 25)),
    ScheduledJob("realtime_minute_stop", time(15, 5)),
    ScheduledJob("minute_reconciliation", time(15, 20)),
    ScheduledJob("daily_bar_sync", time(16, 20)),
    ScheduledJob("missing_partition_retry", time(20, 0)),
)


class PhaseOneScheduler:
    """A deterministic due-job calculator; the process runner remains pluggable."""

    def __init__(
        self,
        jobs: Iterable[ScheduledJob] = DEFAULT_JOBS,
        *,
        timezone: ZoneInfo = SHANGHAI,
    ) -> None:
        self.jobs = tuple(sorted(jobs, key=lambda job: (job.local_time, job.name)))
        self.timezone = timezone
        self._last_run: dict[tuple[str, object], datetime] = {}

    def due_jobs(self, now: datetime) -> tuple[ScheduledJob, ...]:
        local = now.astimezone(self.timezone)
        due: list[ScheduledJob] = []
        for job in self.jobs:
            key = (job.name, local.date())
            if local.time() < job.local_time or key in self._last_run:
                continue
            due.append(job)
        return tuple(due)

    def run_due(self, now: datetime, handlers: dict[str, Callable[[], object]]) -> tuple[str, ...]:
        local = now.astimezone(self.timezone)
        completed: list[str] = []
        for job in self.due_jobs(now):
            handler = handlers.get(job.name)
            if handler is None:
                continue
            handler()
            self._last_run[(job.name, local.date())] = now
            completed.append(job.name)
        return tuple(completed)
