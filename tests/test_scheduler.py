from datetime import datetime
from zoneinfo import ZoneInfo

from stock_data_manage.scheduler import PhaseOneScheduler, load_scheduled_jobs


SHANGHAI = ZoneInfo("Asia/Shanghai")


def test_phase_one_schedule_is_configured_and_idempotent_per_day() -> None:
    scheduler = PhaseOneScheduler()
    morning = scheduler.due_jobs(datetime(2026, 9, 11, 8, 31, tzinfo=SHANGHAI))
    assert [job.name for job in morning] == ["security_master_update", "daily_reconciliation"]
    called = []
    assert scheduler.run_due(
        datetime(2026, 9, 11, 8, 31, tzinfo=SHANGHAI),
        {job.name: (lambda name=job.name: called.append(name)) for job in morning},
    ) == ("security_master_update", "daily_reconciliation")
    assert scheduler.due_jobs(datetime(2026, 9, 11, 8, 32, tzinfo=SHANGHAI)) == ()
    assert called == ["security_master_update", "daily_reconciliation"]


def test_schedule_yaml_skips_disabled_tdx_delayed_jobs() -> None:
    jobs = load_scheduled_jobs("config/schedules.yaml")
    assert jobs[0].name == "security_master_update"
    assert not [job for job in jobs if job.name.startswith("tdx_")]
