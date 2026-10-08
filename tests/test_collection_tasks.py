"""Persistent task behavior; synthetic fault fixtures are not live source evidence."""
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pyarrow.parquet as pq
import pytest
import yaml

from stock_data_manage.domain import Dataset
from stock_data_manage.pipeline.inputs import collect_task
from stock_data_manage.storage.integrity import Manifest, file_hash
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.storage.parquet import CanonicalPartitionStore, InvalidPartitionError, write_normalized_rows
from stock_data_manage.storage.raw import RawObjectStore
from stock_data_manage.worker.recovery import RecoveryScanner, recover_collection_tasks

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
DAY = "2026-09-30"


def daily_definition(task_id="daily", symbols=("sh600001", "sh600002", "sh600003")):
    return {"task_id": task_id, "dataset": "daily_bar", "trade_date": DAY, "adjustment": "forward",
            "asset_type": "stock", "units": [{"key": symbol, "symbol": symbol, "input_id": "ASTOCK-002-daily",
                "context": {"request": {"symbol": symbol, "start_date": DAY, "end_date": DAY}}} for symbol in symbols]}


def master_definition(task_id="master"):
    return {"task_id": task_id, "dataset": "security_master", "trade_date": DAY,
            "required_exchanges": ["XSHG", "XSHE"], "required_asset_types": ["stock"],
            "units": [{"key": "shenzhen-shanghai", "input_id": "SDA-BOARD-005", "context": {
                "request": {"trade_date": DAY}, "calendar": {"trading_dates": [DAY]}}}]}


class FaultCollector:
    def __init__(self):
        self.calls = []
        self.fail = set()
        self.invalid = set()
        self.prices = {}
        self.source_stock_codes = ("600001", "000001")
        self.catalog_rows = {}
        self.source_date = DAY

    def __call__(self, **options):
        task_id, key = options["task_unit"]
        self.calls.append((key, options["force_fetch"], options.get("replay_manifest")))
        root = Path(options["data_root"])
        raw = RawObjectStore.task_unit_path(root / "raw", task_id, key)
        store = RawObjectStore(raw)
        store.write_json({"task_id": task_id, "unit_key": key}, dataset="task", provider="fixture",
                         endpoint="fixture", fetched_at=datetime.now(timezone.utc), attempt_id="owner",
                         relative_path="_managed_task.json")
        body = json.dumps({"fixture": True, "security": key, "price": self.prices.get(key, "10")}).encode()
        store.record_response(response=SimpleNamespace(content=body, status_code=503 if key in self.fail else 200,
            headers={"Content-Type": "application/json"}, encoding="utf-8"), url="https://fixture.invalid/"+key,
            method="GET", request_headers={}, scope=options["context"], provider="fixture", endpoint="fixture",
            code_version="fixture-v1", mode="fixture")
        if key in self.fail:
            raise RuntimeError("synthetic transport failure")
        if options["input_id"] in {"SDA-BOARD-005", "SECURITY-BSE-001"}:
            dataset = "security_snapshot"
            rows = [{"trade_date": date.fromisoformat(DAY), "stock_code": code, "stock_name": "fixture stock",
                     "exchange": "XSHG" if code.startswith("6") else "XSHE", "status": "active", "source": "fixture"}
                    for code in self.source_stock_codes]
            rows = [{"trade_date": date.fromisoformat(self.source_date), "status": "active", "source": "fixture",
                     "stock_name": "fixture security", **row} for row in self.catalog_rows.get(key, rows)]
        else:
            dataset = "daily_bar"
            price = Decimal(self.prices.get(key, "10"))
            rows = [{"instrument_id": ("XSHG" if key.startswith("sh") else "XSHE")+":"+key[2:],
                     "trade_date": date.fromisoformat(DAY), "adjustment": "forward", "open": price,
                     "high": price, "low": Decimal("0") if key in self.invalid else price,
                     "close": price, "volume": None, "amount": None}]
        fields = yaml.safe_load((CONFIG / "datasets" / (dataset+".yaml")).read_text(encoding="utf-8"))["fields"]
        work = root / "task_workspace" / "fixture" / uuid4().hex
        artifact = work / "normalized.parquet"
        output = write_normalized_rows(artifact, rows, fields)
        return {"status": "candidate_complete", "run_directory": str(work),
                "normalized_parquet": {"path": "normalized.parquet", **output},
                "adapter_version": "fixture-v1", "normalization_version": "fixture-v1",
                "validation_time_utc": datetime.now(timezone.utc).isoformat(), "verification_mode": "fixture"}


def run(root, definition, collector, **options):
    options.setdefault("mode", "replay")
    return collect_task(definition=definition, config_root=CONFIG, data_root=root,
                        collector=collector, **options)


def read_bars(root):
    return CanonicalPartitionStore(root / "canonical").read(Dataset.DAILY_BAR, "stock", DAY)


def test_restart_resumes_only_missing_and_publishes_raw(tmp_path):
    collector = FaultCollector()
    collector.fail.add("sh600002")
    definition = daily_definition()
    first = run(tmp_path, definition, collector)
    assert first["status"] == "failed"
    assert not read_bars(tmp_path)
    assert RawObjectStore.task_unit_path(tmp_path / "raw", "daily", "sh600001").exists()
    # Database is closed between runs, reproducing a process restart.
    collector.fail.clear()
    second = run(tmp_path, definition, collector)
    assert second["status"] == "published"
    assert [call[0] for call in collector.calls] == ["sh600001", "sh600002", "sh600003", "sh600002"]
    assert len(read_bars(tmp_path)) == 3
    for record in read_bars(tmp_path):
        manifest = Path(record.raw_object_path)
        assert manifest.is_file()
        assert RawObjectStore.verify_manifest(manifest)
    assert not (tmp_path / "raw" / "_tmp" / "daily").exists()
    assert len(list((tmp_path / "raw" / "tencent" / "kline_daily").glob("scope-*"))) == 3


def test_invalid_security_is_refetched_not_reused(tmp_path):
    collector = FaultCollector()
    collector.invalid.add("sh600002")
    assert run(tmp_path, daily_definition(), collector)["status"] == "failed"
    collector.invalid.clear()
    assert run(tmp_path, daily_definition(), collector)["status"] == "published"
    assert [call[0] for call in collector.calls].count("sh600002") == 2
    assert [call[0] for call in collector.calls].count("sh600001") == 1


def test_selected_redo_updates_only_target_without_duplicates(tmp_path):
    collector = FaultCollector()
    definition = daily_definition()
    run(tmp_path, definition, collector)
    original = {row.instrument_id: row for row in read_bars(tmp_path)}
    collector.prices["sh600002"] = "20"
    run(tmp_path, definition, collector, redo="selected", symbols=["sh600002"])
    updated = {row.instrument_id: row for row in read_bars(tmp_path)}
    assert len(updated) == 3
    assert updated["XSHG:600002"].close == Decimal("20")
    assert updated["XSHG:600001"] == original["XSHG:600001"]
    assert collector.calls[-1][:2] == ("sh600002", True)
    assert Path(original["XSHG:600002"].raw_object_path).is_file()
    run(tmp_path, definition, collector, redo="selected", symbols=["sh600002"])
    assert len(read_bars(tmp_path)) == 3


def test_failed_full_redo_cannot_hide_missing_using_old_publication(tmp_path):
    collector = FaultCollector()
    definition = daily_definition()
    run(tmp_path, definition, collector)
    prior = read_bars(tmp_path)
    collector.fail.add("sh600002")
    failed = run(tmp_path, definition, collector, redo="full")
    assert failed["status"] == "failed"
    assert read_bars(tmp_path) == prior
    assert all(call[1] for call in collector.calls[-3:])
    collector.fail.clear()
    resumed = run(tmp_path, definition, collector)
    assert resumed["status"] == "published"
    assert collector.calls[-1][0] == "sh600002"


@pytest.mark.parametrize("stage", ["before_publish", "after_data_replace", "after_publish", "after_raw_promote"])
def test_commit_interruption_recovers_without_source_requests(tmp_path, stage):
    collector = FaultCollector()
    def crash(current):
        if current == stage:
            raise RuntimeError("simulated crash")
    with pytest.raises(RuntimeError, match="simulated crash"):
        run(tmp_path, daily_definition(), collector, failure_hook=crash)
    with pytest.raises(InvalidPartitionError, match="pending"):
        read_bars(tmp_path)
    with MetadataStore(tmp_path / "metadata/metadata.duckdb") as metadata:
        assert metadata.load_collection_task("daily")["status"] == "committing"
        # Legacy recovery must not expose a task awaiting raw finalization.
        assert RecoveryScanner(tmp_path / "canonical", metadata).recover().repaired_metadata_partitions == 0
    assert recover_collection_tasks(config_root=CONFIG, data_root=tmp_path) == ("daily",)
    assert len(collector.calls) == 3
    assert len(read_bars(tmp_path)) == 3
    assert recover_collection_tasks(config_root=CONFIG, data_root=tmp_path) == ()


def test_master_then_dynamic_daily_scope_and_new_listings(tmp_path):
    collector = FaultCollector()
    master = run(tmp_path, master_definition(), collector)
    assert master["status"] == "published"
    assert master["row_count"] == 2
    definition = {"task_id": "all", "dataset": "daily_bar", "trade_date": DAY,
                  "universe": "all_stock", "asset_type": "stock", "adjustment": "forward", "input_id": "ASTOCK-002-daily"}
    daily = run(tmp_path, definition, collector)
    assert daily["row_count"] == 2
    collector.source_stock_codes = ("600001", "000001", "600002")
    run(tmp_path, master_definition(), collector, redo="full")
    assert run(tmp_path, definition, collector)["no_op"]
    new_definition = {**definition, "task_id": "all-new"}
    assert run(tmp_path, new_definition, collector)["row_count"] == 3


def test_master_whole_source_failure_falls_back_without_claiming_today_complete(tmp_path):
    collector = FaultCollector()
    run(tmp_path, master_definition(), collector)
    collector.fail.add("shenzhen-shanghai")
    result = run(tmp_path, master_definition("master-next"), collector)
    assert result["status"] == "published"
    assert not result["complete_today"]
    assert result["commit"]["fallback_units"] == ["shenzhen-shanghai"]
    assert result["commit"]["raw_refs"]


def test_master_does_not_claim_etf_and_bse_coverage(tmp_path):
    definition = master_definition()
    del definition["required_exchanges"]
    del definition["required_asset_types"]
    result = run(tmp_path, definition, FaultCollector())
    assert result["status"] == "failed"
    assert "coverage" in result["error"]


def full_master_definition(task_id="complete-master", day=DAY):
    definition = master_definition(task_id)
    definition.pop("required_exchanges")
    definition.pop("required_asset_types")
    definition["trade_date"] = day
    definition["units"][0]["context"] = {"request": {"trade_date": day},
        "calendar": {"trading_dates": [day]}, "config": {"include_etf": True}}
    definition["units"].append({"key": "beijing", "input_id": "SECURITY-BSE-001", "context": {}})
    return definition


def full_catalog_collector():
    collector = FaultCollector()
    collector.catalog_rows = {
        "shenzhen-shanghai": [{"stock_code": code, "exchange": exchange, "asset_type": asset}
            for code, exchange, asset in [("600001", "XSHG", "stock"), ("000001", "XSHE", "stock"),
                                          ("511600", "XSHG", "etf"), ("159003", "XSHE", "etf")]],
        "beijing": [{"stock_code": "920001", "exchange": "BSE", "asset_type": "stock", "status": "unknown"}]}
    return collector


def master_manifest(root):
    return Manifest.load(root / "canonical/security_master/current/manifest.json")


def test_five_groups_and_missing_etf_whole_fallback_then_resume(tmp_path):
    collector = full_catalog_collector()
    assert run(tmp_path, full_master_definition(), collector)["status"] == "published"
    original = master_manifest(tmp_path)
    assert set(original.publication_metadata["coverage"]["group_counts"]) == {
        "XSHG/stock", "XSHE/stock", "BSE/stock", "XSHG/etf", "XSHE/etf"}
    assert not original.publication_metadata["coverage"]["independent_full_market_coverage_verified"]
    collector.source_date = "2026-10-01"
    lost = collector.catalog_rows["shenzhen-shanghai"].pop()
    definition = full_master_definition("next", collector.source_date)
    fallback = run(tmp_path, definition, collector)
    assert fallback["status"] == "published" and not fallback["complete_today"]
    assert fallback["security_check"]["missing_groups"] == ["XSHE/etf"]
    assert fallback["security_check"]["recollect_units"] == ["shenzhen-shanghai"]
    previous = master_manifest(tmp_path)
    assert previous.content_hash == original.content_hash and previous.raw_refs == original.raw_refs
    assert previous.publication_metadata["as_of_date"] == DAY
    assert previous.publication_metadata["requested_date"] == "2026-10-01"
    assert previous.publication_metadata["previous_age_days"] == 1
    assert fallback["commit"]["promotions"] == []
    assert RawObjectStore.task_unit_path(tmp_path / "raw", "next", "beijing").is_dir()
    collector.catalog_rows["shenzhen-shanghai"].append(lost)
    before = len(collector.calls)
    resumed = run(tmp_path, definition, collector)
    assert resumed["status"] == "published" and resumed["complete_today"]
    assert collector.calls[before:] == [("shenzhen-shanghai", True, None)]
    assert master_manifest(tmp_path).publication_metadata["as_of_date"] == "2026-10-01"
    assert not master_manifest(tmp_path).publication_metadata["fallback"]
    assert run(tmp_path, definition, collector)["no_op"]


def test_first_incomplete_catalog_keeps_raw_and_blocks_publication(tmp_path):
    collector = full_catalog_collector()
    collector.catalog_rows["shenzhen-shanghai"].pop()
    result = run(tmp_path, full_master_definition(), collector)
    assert result["status"] == "failed"
    assert result["security_check"]["missing_groups"] == ["XSHE/etf"]
    assert not (tmp_path / "canonical/security_master/current/manifest.json").exists()
    assert RawObjectStore.task_unit_path(tmp_path / "raw", "complete-master", "beijing").exists()


def test_invalid_catalog_date_refetched_instead_of_reusing_stale_response(tmp_path):
    collector = full_catalog_collector()
    run(tmp_path, full_master_definition(), collector)
    definition = full_master_definition("next", "2026-10-01")
    stale = run(tmp_path, definition, collector)
    assert stale["status"] == "published" and stale["fallback_used"]
    assert stale["data_date"] == DAY
    collector.source_date = "2026-10-01"
    before = len(collector.calls)
    resumed = run(tmp_path, definition, collector)
    assert resumed["complete_today"] and resumed["data_date"] == "2026-10-01"
    assert all(force and replay is None for key, force, replay in collector.calls[before:])


@pytest.mark.parametrize("redo", ["resume", "full"])
def test_disappearing_security_is_not_hidden_by_previous_rows(tmp_path, redo):
    collector = full_catalog_collector()
    run(tmp_path, full_master_definition(), collector)
    collector.catalog_rows["shenzhen-shanghai"][0]["stock_code"] = "600002"
    result = run(tmp_path, full_master_definition("replacement"), collector, redo=redo)
    assert result["security_check"]["missing_previous_securities"] == ["XSHG:600001"]
    if redo == "full":
        assert result["status"] == "failed"
    else:
        assert result["status"] == "published" and not result["complete_today"]
    rows = pq.read_table(tmp_path / "canonical/security_master/current/data.parquet").to_pylist()
    assert {r["symbol"] for r in rows} == {"600001", "000001", "511600", "159003", "920001"}


@pytest.mark.parametrize("reason", ["expired", "scope", "disabled", "legacy", "evidence", "future"])
def test_unusable_previous_catalog_is_rejected(tmp_path, reason):
    collector = full_catalog_collector()
    run(tmp_path, full_master_definition(), collector)
    original = master_manifest(tmp_path)
    collector.fail.add("beijing")
    day = "2026-10-02" if reason == "expired" else "2026-09-29" if reason == "future" else "2026-10-01"
    collector.source_date = day
    definition = full_master_definition("retry", day)
    if reason == "scope":
        definition["units"][0]["context"]["request"]["symbols"] = ["sh600001"]
    if reason == "disabled":
        definition["allow_previous"] = False
    if reason == "legacy":
        manifest_path = tmp_path / "canonical/security_master/current/manifest.json"
        payload = json.loads(manifest_path.read_text())
        payload.pop("publication_metadata")
        manifest_path.write_text(json.dumps(payload))
    if reason == "evidence":
        source = original.publication_metadata["sources"][0]
        Path(source["raw_ref"]).write_text("corrupt")
    result = run(tmp_path, definition, collector)
    assert result["status"] == "failed"
    assert master_manifest(tmp_path).content_hash == original.content_hash


@pytest.mark.parametrize("stage", ["after_data_replace", "after_publish"])
def test_master_fallback_commit_recovers_with_date_and_no_refetch(tmp_path, stage):
    collector = full_catalog_collector()
    run(tmp_path, full_master_definition(), collector)
    collector.fail.add("beijing")
    collector.source_date = "2026-10-01"
    def crash(current):
        if current == stage:
            raise RuntimeError("simulated master fallback crash")
    with pytest.raises(RuntimeError):
        run(tmp_path, full_master_definition("retry", collector.source_date), collector, failure_hook=crash)
    calls = len(collector.calls)
    assert recover_collection_tasks(config_root=CONFIG, data_root=tmp_path) == ("retry",)
    manifest = master_manifest(tmp_path)
    assert manifest.publication_metadata["as_of_date"] == DAY and manifest.publication_metadata["fallback"]
    assert len(collector.calls) == calls


def test_real_source_catalog_pairs_and_incompatible_dates(tmp_path):
    from stock_data_manage.providers.transport import RequestPacer
    definition = full_master_definition("real-pairs")
    definition["required_exchanges"] = ["XSHG", "XSHE"]
    definition["units"].pop()
    definition["units"][0]["replay_manifest"] = str(ROOT / "provider_validation/results/live-probes/baostock-industry-20260930-20261003/_raw/baostock-industry/manifest.ndjson")
    options = {"pacer": RequestPacer(wait=lambda _: None)}
    result = collect_task(definition=definition, config_root=CONFIG, data_root=tmp_path, mode="replay", collector_options=options)
    assert result["status"] == "published", result.get("error")
    assert result["row_count"] == 6916
    manifest = master_manifest(tmp_path)
    assert manifest.publication_metadata["coverage"]["group_counts"] == {
        "XSHG/stock": 2320, "XSHE/stock": 2904, "XSHG/etf": 943, "XSHE/etf": 749}
    mixed = full_master_definition("real-mixed")
    mixed["units"][0]["replay_manifest"] = definition["units"][0]["replay_manifest"]
    mixed["units"][1]["replay_manifest"] = str(ROOT / "provider_validation/results/raw/security-catalog-bse-20261007/manifest.ndjson")
    rejected = collect_task(definition=mixed, config_root=CONFIG, data_root=tmp_path, mode="replay", collector_options=options)
    assert rejected["status"] == "failed"
    assert "date differs" in rejected["units"]["beijing"]["error"]
    assert rejected["security_check"]["missing_groups"] == ["BSE/stock"]
    assert master_manifest(tmp_path).content_hash == manifest.content_hash


def test_corrupt_raw_is_refetched_and_scope_change_is_rejected(tmp_path):
    collector = FaultCollector()
    collector.fail.add("sh600002")
    definition = daily_definition()
    result = run(tmp_path, definition, collector)
    raw = Path(result["units"]["sh600001"]["raw_manifest"])
    event = json.loads(raw.read_text(encoding="utf-8").splitlines()[0])
    (raw.parent / event["body_storage"]).write_bytes(b"corrupt")
    collector.fail.clear()
    assert run(tmp_path, definition, collector)["status"] == "published"
    assert [call[0] for call in collector.calls].count("sh600001") == 2
    with pytest.raises(ValueError, match="scope changed"):
        run(tmp_path, daily_definition(symbols=("sh600001",)), collector)


def test_selected_scope_and_live_production_gate(tmp_path):
    with pytest.raises(ValueError, match="outside"):
        run(tmp_path, daily_definition(), FaultCollector(), redo="selected", symbols=["sh999999"])
    with pytest.raises(ValueError, match="blocked"):
        run(tmp_path, daily_definition(), FaultCollector(), mode="live")
    with pytest.raises(ValueError, match="isolated"):
        collect_task(definition=daily_definition(), config_root=CONFIG, data_root=ROOT / "data", mode="replay")
    with pytest.raises(ValueError, match="invalid task"):
        run(tmp_path, daily_definition(task_id="../raw"), FaultCollector())


@pytest.mark.parametrize("mode", ["replay", "live"])
@pytest.mark.parametrize("suffix", ["", "results/business-task/data"])
def test_business_tasks_reject_provider_validation_output_before_execution(mode, suffix):
    collector = FaultCollector()
    with pytest.raises(ValueError, match="outside provider_validation"):
        collect_task(definition=daily_definition(), config_root=CONFIG,
                     data_root=ROOT / "provider_validation" / suffix, mode=mode, collector=collector)
    assert collector.calls == []


def test_task_defaults_use_configured_storage_and_keep_live_qualification_gate(monkeypatch):
    from stock_data_manage.config.loader import load_storage_paths
    paths_used = []

    def resolve_paths(config_root, **kwargs):
        paths = load_storage_paths(config_root, **kwargs)
        paths_used.append(paths)
        return paths

    monkeypatch.setattr("stock_data_manage.pipeline.inputs.load_storage_paths", resolve_paths)
    collector = FaultCollector()
    with pytest.raises(ValueError, match="formal routing qualification"):
        collect_task(definition=daily_definition(), config_root=CONFIG, collector=collector)
    assert paths_used and all(paths["data_root"] == ROOT / "data" for paths in paths_used)
    assert collector.calls == []


def test_processing_retry_uses_saved_response_and_keeps_other_tasks_staging(tmp_path):
    collector = FaultCollector()
    definition = daily_definition(symbols=("sh600001",))
    def fail_mapping(**options):
        report = collector(**options)
        report.update(status="failed", failure_class="NormalizationError")
        return report
    first = run(tmp_path, definition, fail_mapping)
    assert first["status"] == "failed"
    pending = RawObjectStore.task_unit_path(tmp_path / "raw", "other", "unit")
    pending.mkdir(parents=True)
    (pending / "user-evidence.txt").write_text("keep", encoding="utf-8")
    assert run(tmp_path, definition, collector)["status"] == "published"
    replay = Path(collector.calls[-1][2])
    assert replay.is_file() and RawObjectStore.verify_manifest(replay)
    assert run(tmp_path, definition, collector, redo="full")["status"] == "published"
    assert (pending / "user-evidence.txt").read_text(encoding="utf-8") == "keep"


def test_dead_process_lock_is_recovered_but_live_lock_is_preserved(tmp_path):
    import subprocess
    import sys
    from stock_data_manage.storage.parquet import PartitionLock, PartitionLockedError
    lock = tmp_path / "abrupt.lock"
    script = "import os,sys; from pathlib import Path; from stock_data_manage.storage.parquet import PartitionLock; PartitionLock(Path(sys.argv[1])).__enter__(); os._exit(0)"
    subprocess.run([sys.executable, "-c", script, str(lock)], check=True)
    assert lock.exists()
    with PartitionLock(lock, recover_stale=True):
        with pytest.raises(PartitionLockedError):
            with PartitionLock(lock, recover_stale=True):
                pass
    assert not lock.exists()


def test_pending_commit_rejects_another_writer_and_completed_data_corruption(tmp_path):
    collector = FaultCollector()
    def crash(stage):
        if stage == "before_publish":
            raise RuntimeError("stop")
    with pytest.raises(RuntimeError):
        run(tmp_path, daily_definition(), collector, failure_hook=crash)
    with pytest.raises(InvalidPartitionError, match="another task"):
        CanonicalPartitionStore(tmp_path / "canonical").publish(dataset=Dataset.DAILY_BAR,
            asset_type="stock", partition_key=DAY, new_records=[], expected_count=0, run_id="unrelated")
    recover_collection_tasks(config_root=CONFIG, data_root=tmp_path)
    data = tmp_path / "canonical/daily_bar/asset_type=stock" / ("trade_date=" + DAY) / "data.parquet"
    data.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="integrity"):
        run(tmp_path, daily_definition(), collector)


def test_real_archived_source_inputs_end_to_end(tmp_path):
    archive = ROOT / "provider_validation/results/live-probes/baostock-industry-20260930-20261003/_raw/baostock-industry/manifest.ndjson"
    definition = master_definition("real-master")
    definition["units"][0]["replay_manifest"] = str(archive)
    result = collect_task(definition=definition, config_root=CONFIG, data_root=tmp_path, mode="replay")
    assert result["status"] == "published", result.get("error")
    assert result["row_count"] == 5224
    tencent = ROOT / "provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson"
    # Existing evidence covers this window; a one-day task selects the archived day.
    day = "2026-09-18"
    definition = {"task_id": "real-daily", "dataset": "daily_bar", "trade_date": day,
                  "adjustment": "forward", "asset_type": "stock", "units": [{
        "key": "sh600519", "symbol": "sh600519", "input_id": "ASTOCK-002-daily", "replay_manifest": str(tencent),
        "context": {"request": {"symbol": "600519", "start_date": "2026-09-01", "end_date": day}}}]}
    daily = collect_task(definition=definition, config_root=CONFIG, data_root=tmp_path, mode="replay")
    assert daily["status"] == "published", daily.get("error")
    assert daily["row_count"] == 1
