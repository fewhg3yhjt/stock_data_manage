from datetime import datetime
from zoneinfo import ZoneInfo

from stock_data_manage.worker.scheduler import PhaseOneScheduler, load_scheduled_jobs


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


from dataclasses import replace
from datetime import date, time
from pathlib import Path
import json
import shutil
import pytest
import yaml

from stock_data_manage.config.loader import load_collection_profiles
from stock_data_manage.domain import AssetType, Exchange
from stock_data_manage.domain.sessions import MarketSchedule
from stock_data_manage.pipeline.inputs import collect_due_inputs
from stock_data_manage.service.instruments import SecurityRecord
from stock_data_manage.worker.scheduler import collection_slot, plan_input_collection

ROOT = Path(__file__).resolve().parents[1]
DAY = date(2026, 9, 30)


def profiles():
    return {p.name: p for p in load_collection_profiles(ROOT / "config/collection.yaml")}


def stamp(hour, minute, second=0, day=DAY):
    return datetime.combine(day, time(hour, minute, second), SHANGHAI)


def edited_config(tmp_path, profile, **changes):
    root = tmp_path / "config"
    shutil.copytree(ROOT / "config", root)
    path = root / "collection.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["collection_profiles"][profile].update(changes)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return root


def test_tencent_profiles_separate_scope_frequency_and_request_speed():
    p = profiles()
    snapshot = p["tencent_market_snapshot"]
    assert (snapshot.frequency_unit, snapshot.frequency_interval, snapshot.at_time, snapshot.universe) == ("day", 1, time(15, 10), "all_stock")
    assert snapshot.max_symbols is None
    assert p["tencent_minute_5m"].refresh_interval_seconds == 300
    assert p["tencent_minute_1m"].refresh_interval_seconds == 60
    assert p["tencent_daily"].at_time == time(16, 20)
    assert all(not v.scheduling_enabled for v in p.values())


@pytest.mark.parametrize("hour,minute,second,expected", [
    (9, 30, 5, None), (9, 34, 59, None), (9, 35, 4, None),
    (9, 35, 5, (9, 35)), (9, 35, 34, (9, 35)), (9, 35, 35, None),
    (11, 30, 5, (11, 30)), (11, 31, 0, None), (12, 0, 0, None),
    (13, 0, 5, None), (13, 5, 5, (13, 5)), (15, 0, 5, (15, 0)),
    (15, 1, 0, None), (16, 0, 0, None),
])
def test_minute_slots_respect_delay_sessions_and_no_stale_catchup(hour, minute, second, expected):
    slot = collection_slot(profiles()["tencent_minute_5m"], stamp(hour, minute, second),
                           schedule=MarketSchedule(), trading_dates=[DAY])
    assert slot == (stamp(*expected) if expected else None)


def test_daily_slots_calendar_intervals_and_timezone():
    p = profiles()["tencent_market_snapshot"]
    assert collection_slot(p, stamp(15, 9), schedule=MarketSchedule(), trading_dates=[DAY]) is None
    assert collection_slot(p, stamp(15, 10), schedule=MarketSchedule(), trading_dates=[DAY]) == stamp(15, 10)
    assert collection_slot(p, stamp(20, 0), schedule=MarketSchedule(), trading_dates=[]) is None
    assert collection_slot(p, stamp(20, 0, day=date(2026, 10, 3)), schedule=MarketSchedule(), trading_dates=[DAY]) is None
    p = replace(p, frequency_interval=2, anchor_date=DAY)
    next_day = date(2026, 10, 1)
    assert collection_slot(p, stamp(20, 0, day=next_day), schedule=MarketSchedule(), trading_dates=[next_day]) is None
    two_days = date(2026, 10, 2)
    assert collection_slot(p, stamp(20, 0, day=two_days), schedule=MarketSchedule(), trading_dates=[two_days]) is not None
    with pytest.raises(ValueError, match="timezone"):
        collection_slot(p, datetime(2026, 9, 30), schedule=MarketSchedule(), trading_dates=[DAY])


def test_full_stock_selection_has_no_100_or_200_symbol_truncation():
    records = [SecurityRecord(f"XSHG:{600000+i}", str(600000+i), Exchange.XSHG, AssetType.STOCK) for i in range(201)]
    records += [SecurityRecord("BSE:920000", "920000", Exchange.BSE, AssetType.STOCK),
                SecurityRecord("XSHE:000001", "000001", Exchange.XSHE, AssetType.STOCK),
                SecurityRecord("XSHG:510300", "510300", Exchange.XSHG, AssetType.ETF),
                SecurityRecord("XSHG:600999", "600999", Exchange.XSHG, AssetType.STOCK, list_date=date(2026, 10, 1))]
    jobs = plan_input_collection(ROOT / "config", now=stamp(15, 10), trading_dates=[DAY], securities=records,
                                 symbols=["sz300750"])
    snapshot = next(j for j in jobs if j.input_id == "ASTOCK-001")
    assert len(snapshot.symbols) == 203
    assert "bj920000" in snapshot.symbols and "sz000001" in snapshot.symbols
    assert "sh510300" not in snapshot.symbols and "sh600999" not in snapshot.symbols
    assert snapshot.status == "disabled"


def test_snapshot_switch_to_minutes_keeps_full_scope_and_migration_gate(tmp_path):
    root = edited_config(tmp_path, "tencent_market_snapshot", scheduling_enabled=True,
                         frequency={"unit": "minute", "interval": 10, "at": "15:10"})
    records = [SecurityRecord("XSHG:600519", "600519", Exchange.XSHG, AssetType.STOCK)]
    job = next(j for j in plan_input_collection(root, now=stamp(9, 40), trading_dates=[DAY], securities=records)
               if j.input_id == "ASTOCK-001")
    assert job.universe == "all_stock" and job.symbols == ("sh600519",)
    assert (job.status, job.reason) == ("blocked", "migration_pending")


def test_minute_capacity_missing_scope_and_scope_limits_block_execution(tmp_path):
    root = edited_config(tmp_path, "tencent_minute_5m", scheduling_enabled=True)
    def job(symbols):
        return next(j for j in plan_input_collection(root, now=stamp(9, 35, 5), trading_dates=[DAY], symbols=symbols)
                    if j.input_id == "ASTOCK-002-5m")
    assert job([]).status == "blocked"
    assert "request budget" in job([f"sh{600000+i}" for i in range(200)]).reason
    assert "max_symbols" in job([f"sh{600000+i}" for i in range(201)]).reason
    assert job(["sz300750"]).status == "ready"
    with pytest.raises(ValueError, match="duplicate"):
        job(["sz300750", "sz300750"])


@pytest.mark.parametrize("changes", [
    {"frequency": {"unit": "second", "interval": 1}},
    {"frequency": {"unit": "minute", "interval": True}},
    {"frequency": {"unit": "minute", "interval": 0}},
    {"frequency": {"unit": "day", "interval": 2, "at": "15:10"}},
    {"frequency": {"unit": "day", "interval": 1}},
    {"refresh_interval_seconds": 60}, {"max_symbols": 100},
    {"frequency": {"unit": "minute", "interval": 1}, "settle_delay_seconds": 40},
    {"scheduling_enabled": "false"},
])
def test_invalid_frequency_config_is_rejected(tmp_path, changes):
    root = edited_config(tmp_path, "tencent_market_snapshot", **changes)
    with pytest.raises(ValueError):
        load_collection_profiles(root / "collection.yaml")


def test_candidate_tick_survives_restart_and_uses_existing_collector_and_shared_pacer(tmp_path):
    root = edited_config(tmp_path, "tencent_minute_5m", scheduling_enabled=True)
    calls = []
    def collector(**kwargs):
        calls.append(kwargs)
        path = tmp_path / f"result-{len(calls)}.json"
        path.write_text("{}", encoding="utf-8")
        return {"status": "candidate_complete", "row_count": 96, "report_path": str(path)}
    args = dict(config_root=root, output_root=tmp_path / "candidates", trading_dates=[DAY],
                symbols=["sz300750", "sh600519"], execute=True, replay_manifest=Path("fixture"), collector=collector)
    report = collect_due_inputs(now=stamp(9, 35, 5), **args)
    job = report["jobs"][0]
    assert (job["status"], job["coverage_denominator"], job["successful_symbol_count"]) == ("candidate_complete", 2, 2)
    assert calls[0]["pacer"] is calls[1]["pacer"]
    assert report["production_writes"] == 0 and not report["eligible_for_production_routing"]
    again = collect_due_inputs(now=stamp(9, 35, 6), **args)
    assert again["jobs"][0]["status"] == "already_attempted" and len(calls) == 2
    assert Path(again["jobs"][0]["previous_report_path"]).exists()
    collect_due_inputs(now=stamp(9, 40, 5), **args)
    assert len(calls) == 4
    assert Path(report["report_path"]).exists()


def test_failed_slot_does_not_hammer_source_on_every_tick(tmp_path):
    root = edited_config(tmp_path, "tencent_minute_5m", scheduling_enabled=True)
    calls = []
    def failing(**kwargs):
        calls.append(kwargs)
        raise RuntimeError("fixture transport failure")
    args = dict(config_root=root, output_root=tmp_path / "candidates", trading_dates=[DAY],
                symbols=["sz300750"], execute=True, replay_manifest=Path("fixture"), collector=failing)
    assert collect_due_inputs(now=stamp(9, 35, 5), **args)["jobs"][0]["status"] == "failed"
    assert collect_due_inputs(now=stamp(9, 35, 6), **args)["jobs"][0]["status"] == "already_attempted"
    assert len(calls) == 1


def test_daily_tick_passes_current_slot_date_and_does_not_change_qfq_semantics(tmp_path):
    root = edited_config(tmp_path, "tencent_daily", scheduling_enabled=True)
    calls = []
    def collector(**kwargs):
        calls.append(kwargs)
        return {"status": "failed", "report_path": "fixture-report.json"}
    report = collect_due_inputs(now=stamp(16, 20), config_root=root, output_root=tmp_path / "out",
        trading_dates=[DAY], symbols=["sh600519"], execute=True, replay_manifest=Path("fixture"), collector=collector)
    assert calls[0]["context"]["request"] == {"symbol": "sh600519", "start_date": DAY, "end_date": DAY}
    assert report["jobs"][-1]["status"] == "failed"


def test_planning_does_not_create_database_or_call_collector(tmp_path):
    def forbidden(**kwargs):
        raise AssertionError("planning must not collect")
    report = collect_due_inputs(now=stamp(15, 10), config_root=ROOT / "config", output_root=tmp_path / "out",
                               trading_dates=[DAY], collector=forbidden)
    assert report["jobs"][0]["status"] == "disabled"
    assert not (tmp_path / "out/schedule-attempts.duckdb").exists()
    assert Path(report["report_path"]).exists()


def test_scheduler_cannot_write_production_outputs():
    with pytest.raises(ValueError, match="production"):
        collect_due_inputs(now=stamp(15, 10), config_root=ROOT / "config", output_root=ROOT / "data/canonical",
                           trading_dates=[DAY])



def test_daily_time_edit_does_not_repeat_completed_day(tmp_path):
    root = edited_config(tmp_path, "tencent_daily", scheduling_enabled=True)
    calls = []
    def collector(**kwargs):
        calls.append(kwargs)
        path = tmp_path / "saved-candidate.json"
        path.write_text("{}", encoding="utf-8")
        return {"status": "candidate_complete", "report_path": str(path)}
    args = dict(config_root=root, output_root=tmp_path / "out", trading_dates=[DAY], symbols=["sh600519"],
                execute=True, replay_manifest=Path("fixture"), collector=collector)
    collect_due_inputs(now=stamp(16, 20), **args)
    path = root / "collection.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["collection_profiles"]["tencent_daily"]["frequency"]["at"] = "16:30"
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    report = collect_due_inputs(now=stamp(16, 30), **args)
    assert report["jobs"][-1]["status"] == "already_attempted"
    assert len(calls) == 1
