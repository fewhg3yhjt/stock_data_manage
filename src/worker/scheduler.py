from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Callable, Iterable
from zoneinfo import ZoneInfo

import yaml

from ..config.loader import CollectionProfile, load_collection_profiles, load_input_capabilities
from ..domain import AssetType, Exchange
from ..domain.sessions import MarketSchedule, MarketSession
from ..service.instruments import SecurityRecord


SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class ScheduledJob:
    name: str
    local_time: time


@dataclass(frozen=True, slots=True)
class ScheduledInput:
    input_id: str
    profile: str
    slot: datetime
    symbols: tuple[str, ...]
    universe: str
    status: str
    reason: str | None = None


def collection_slot(profile: CollectionProfile, now: datetime, *,
                    schedule: MarketSchedule, trading_dates: Iterable[date]) -> datetime | None:
    """Calculate only today's slot. Minute jobs never catch up through lunch/overnight."""
    if now.tzinfo is None:
        raise ValueError("schedule time must be timezone-aware")
    local = now.astimezone(schedule.timezone)
    if profile.frequency_unit is None or profile.business_day == "source_calendar":
        return None
    if profile.business_day == "trading_day" and local.date() not in set(trading_dates):
        return None
    if profile.frequency_unit == "day":
        if profile.anchor_date is not None:
            distance = (local.date() - profile.anchor_date).days
            if distance < 0 or distance % profile.frequency_interval:
                return None
        slot = datetime.combine(local.date(), profile.at_time, schedule.timezone)
        return slot if local >= slot + timedelta(seconds=profile.settle_delay_seconds) else None
    interval = timedelta(minutes=profile.frequency_interval)
    effective = local - timedelta(seconds=profile.settle_delay_seconds)
    for session in schedule.sessions:
        start = datetime.combine(local.date(), session.start, schedule.timezone)
        end = datetime.combine(local.date(), session.end, schedule.timezone)
        if effective < start + interval:
            continue
        steps = (effective - start) // interval
        slot = start + steps * interval
        # A short trigger window prevents stale requests after a stopped worker resumes.
        trigger = slot + timedelta(seconds=profile.settle_delay_seconds)
        if slot <= end and trigger <= local < trigger + timedelta(seconds=profile.trigger_grace_seconds):
            return slot
    return None


def plan_input_collection(config_root: str | Path, *, now: datetime,
                          trading_dates: Iterable[date], securities: Iterable[SecurityRecord] = (),
                          symbols: Iterable[str] = ()) -> tuple[ScheduledInput, ...]:
    """Bind existing input contracts to their frequency and scope; no network or writes."""
    root = Path(config_root)
    collection = yaml.safe_load((root / "collection.yaml").read_text(encoding="utf-8"))
    market = collection["markets"]["CN_A"]
    schedule = MarketSchedule(timezone=ZoneInfo(market["timezone"]), sessions=tuple(
        MarketSession(time.fromisoformat(start), time.fromisoformat(end)) for start, end in market["sessions"]))
    profiles = {p.name: p for p in load_collection_profiles(root / "collection.yaml")}
    trading_dates, securities, supplied = tuple(trading_dates), tuple(securities), tuple(symbols)
    if len(set(supplied)) != len(supplied):
        raise ValueError("duplicate requested securities")
    jobs = []
    for contract in load_input_capabilities(root / "providers.yaml"):
        profile = profiles[contract.collection_profile]
        slot = collection_slot(profile, now, schedule=schedule, trading_dates=trading_dates)
        if slot is None:
            continue
        selected = supplied
        if profile.universe == "all_stock":
            prefix = {Exchange.XSHG: "sh", Exchange.XSHE: "sz", Exchange.BSE: "bj"}
            records = [r for r in securities if r.asset_type == AssetType.STOCK and r.is_expected_on(slot.date())]
            if any(r.classification_conflict for r in records):
                raise ValueError("full stock universe contains unresolved classification conflicts")
            selected = tuple(sorted(prefix[r.exchange] + r.symbol.removeprefix(prefix[r.exchange]) for r in records))
            if len(set(selected)) != len(selected):
                raise ValueError("duplicate securities in full stock universe")
        status, reason = "ready", None
        if not profile.scheduling_enabled:
            status, reason = "disabled", "scheduling_enabled=false"
        elif contract.implementation_status != "implemented_validation_only":
            status, reason = "blocked", contract.implementation_status
        elif contract.input_id == "ASTOCK-001":
            # Candidate migration is complete; no live bulk/capacity evidence authorizes scheduling.
            status, reason = "blocked", "snapshot_bulk_live_validation_pending"
        elif not selected:
            status, reason = "blocked", "explicit security scope is missing"
        elif profile.max_symbols is not None and len(selected) > profile.max_symbols:
            status, reason = "blocked", "security scope exceeds max_symbols; no silent truncation"
        elif profile.frequency_unit == "minute":
            # Single-symbol Kline calls consume the same host budget sequentially.
            budget = len(selected) * contract.base_requests_per_fetch * max(3, contract.request_interval_seconds)
            if budget + profile.settle_delay_seconds >= profile.frequency_interval * 60:
                status, reason = "blocked", "request budget does not fit the collection interval"
        jobs.append(ScheduledInput(contract.input_id, profile.name, slot, selected, profile.universe, status, reason))
    return tuple(sorted(jobs, key=lambda job: (job.slot, job.input_id)))


def load_scheduled_jobs(path: str | Path) -> tuple[ScheduledJob, ...]:
    """Load the small, explicit Phase 1 schedule from YAML."""
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    raw_jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(raw_jobs, list):
        raise ValueError("schedule config must contain a jobs list")
    jobs: list[ScheduledJob] = []
    for item in raw_jobs:
        if not isinstance(item, dict) or not item.get("name") or not (item.get("time") or item.get("collection_profile")):
            raise ValueError("each schedule job requires name and time")
        if item.get("enabled", True) is False:
            continue
        try:
            if item.get("collection_profile"):
                if item.get("time"):
                    raise ValueError("profile schedule must not duplicate its time")
                profile = next(p for p in load_collection_profiles(Path(path).parent / "collection.yaml")
                               if p.name == item["collection_profile"])
                if profile.frequency_unit != "day" or profile.at_time is None:
                    raise ValueError("named daily job requires a daily profile")
                if not profile.scheduling_enabled:
                    continue
                local_time = profile.at_time
            else:
                hour, minute, *second = str(item["time"]).split(":")
                local_time = time(int(hour), int(minute), int(second[0]) if second else 0)
        except (TypeError, ValueError, StopIteration) as exc:
            raise ValueError(f"invalid schedule time: {item.get('time')!r}") from exc
        jobs.append(ScheduledJob(str(item["name"]), local_time))
    return tuple(sorted(jobs, key=lambda job: (job.local_time, job.name)))


def build_security_master_definition(config_root, *, trade_date, now, is_trading_day, task_id=None):
    """Select configured current or dated inputs; never fabricate snapshot dates."""
    if now.tzinfo is None:
        raise ValueError('task time must be timezone-aware')
    day = date.fromisoformat(trade_date) if isinstance(trade_date, str) else trade_date
    today = now.astimezone(SHANGHAI).date()
    if day > today:
        raise ValueError('security task date cannot be in the future')
    if is_trading_day is not True:
        raise ValueError('security task requires a confirmed trading calendar date')
    root = Path(config_root)
    config = yaml.safe_load((root / 'collection.yaml').read_text(encoding='utf-8'))['security_master']
    selected = config['historical'] if day < today else config
    contracts = {c.input_id: c for c in load_input_capabilities(root / 'providers.yaml')}
    units = []
    for input_id in selected['source_inputs']:
        contract = contracts[input_id]
        if contract.dataset != 'security_snapshot':
            raise ValueError('security task source must be a security snapshot input')
        if day < today and contract.input_id in {'SECURITY-BSE-001', 'SECURITY-SSE-ETF-001'}:
            raise ValueError('current catalog cannot be selected for a historical task')
        context = {'config': dict(selected.get('source_config', {}).get(input_id, {}))}
        if any(binding.source == 'request.trade_date' for binding in contract.parameters):
            context['request'] = {'trade_date': day.isoformat()}
        if contract.trading_date_parameter:
            context['calendar'] = {'trading_dates': [day.isoformat()]}
        units.append({'key': input_id, 'input_id': input_id, 'context': context})
    if not units or len({u['key'] for u in units}) != len(units):
        raise ValueError('security master requires unique source inputs')
    definition = {'task_id': task_id or 'security-master-' + day.strftime('%Y%m%d'),
                  'dataset': 'security_master', 'trade_date': day.isoformat(), 'units': units}
    if day < today:
        definition.update(required_groups=selected['required_groups'], allow_previous=False)
    return definition


def plan_security_master_collection(config_root, *, now, is_trading_day):
    """Plan the business job with the same frequency calculator as source inputs."""
    if now.tzinfo is None:
        raise ValueError("schedule time must be timezone-aware")
    root = Path(config_root)
    payload = yaml.safe_load((root / "collection.yaml").read_text(encoding="utf-8"))
    config = payload["security_master"]
    profile = next(p for p in load_collection_profiles(root / "collection.yaml") if p.name == config["collection_profile"])
    local = now.astimezone(SHANGHAI)
    job = {"dataset": "security_master", "profile": profile.name, "trade_date": local.date().isoformat(),
           "task_id": "security-master-" + local.strftime("%Y%m%d"), "status": "not_due", "slot": None}
    if not profile.scheduling_enabled or not any(j.name == "security_master_update" for j in load_scheduled_jobs(root / "schedules.yaml")):
        return {**job, "status": "disabled", "reason": "security master schedule is disabled"}
    if is_trading_day is None:
        return {**job, "status": "blocked", "reason": "calendar coverage is missing for the requested day"}
    if not is_trading_day:
        return {**job, "reason": "confirmed non-trading day"}
    slot = collection_slot(profile, now, schedule=MarketSchedule(timezone=SHANGHAI), trading_dates=(local.date(),))
    if slot is None:
        return job
    return {**job, "slot": slot.isoformat(), "status": "ready",
            "definition": build_security_master_definition(root, trade_date=job['trade_date'], now=now, is_trading_day=True)}


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
