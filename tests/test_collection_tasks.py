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
        raw = RawObjectStore.task_unit_path(root / "raw", task_id, key, data_date=options["data_date"])
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
                     "trade_date": date.fromisoformat(options["data_date"]), "adjustment": "forward", "open": price,
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
    assert RawObjectStore.task_unit_path(tmp_path / "raw", "daily", "sh600001", data_date=DAY).exists()
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
    assert not (tmp_path / "raw" / "_tmp" / DAY / "daily").exists()
    assert len(list((tmp_path / "raw" / "tencent" / "kline_daily" / DAY).glob("scope-*"))) == 3


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


def save_fixture_qualification(root, input_id, context):
    """Synthetic four-layer artifacts exercise gating; these are not live source evidence."""
    from stock_data_manage.config.loader import load_input_capabilities
    from stock_data_manage.routing.factory import build_input_provider
    from stock_data_manage.providers.probes import ProbeEvidence
    from stock_data_manage.storage.integrity import row_hash
    from datetime import timedelta
    import inspect
    contract = next(c for c in load_input_capabilities(CONFIG / "providers.yaml") if c.input_id == input_id)
    adapter = build_input_provider(contract, providers_path=CONFIG / "providers.yaml")
    version = adapter.capability_version
    if "trading_dates" in context.get("calendar",{}):
        context["calendar"]["trading_dates"]=[date.fromisoformat(value) if isinstance(value,str) else value for value in context["calendar"]["trading_dates"]]
    params = contract.bind_parameters(context)
    work = root / "qualification-fixtures" / input_id
    work.mkdir(parents=True, exist_ok=True)
    raw_store = RawObjectStore(work / "raw")
    event = raw_store.record_response(response=SimpleNamespace(content=b'{"synthetic_transport_fixture": true}',status_code=200,
        headers={"Content-Type":"application/json"},encoding="utf-8"),url="https://fixture.invalid/catalog",method="GET",
        request_headers={},provider=contract.provider,endpoint=contract.endpoint,scope={},code_version="fixture-v1",mode="live")
    raw = raw_store.root / "manifest.ndjson"
    report = {"input_id":input_id,"adapter_version":version,"status":"candidate_complete","mode":"live",
        "parameters":{k: str(v) if isinstance(v,date) else v for k,v in params.items()},"code_version":"fixture-v1",
        "validation_time_utc":datetime.now(timezone.utc).isoformat(),"live_http_calls":1,
        "raw_manifest":{"path":str(raw),"sha256":file_hash(raw)},
        "config_files":[{"path":str(CONFIG/name),"sha256":file_hash(CONFIG/name)} for name in ("providers.yaml","collection.yaml","datasets/security_snapshot.yaml","normalization/security_snapshot.yaml")],
        "code_files":[{"path":str(path),"sha256":file_hash(path)} for path in (ROOT/"src/routing/factory.py",Path(inspect.getfile(type(adapter))))],
        "synthetic_fixture":True}
    rows = full_catalog_collector().catalog_rows["beijing" if input_id=="SECURITY-BSE-001" else "shenzhen-shanghai"]
    groups = {row["exchange"]+"/"+row["asset_type"]:1 for row in rows}
    semantic = {"input_id":input_id,"passed":True,"independent_full_market_coverage_verified":True,
                "code_version":"fixture-v1","group_counts":groups,"source_response_hashes":[event["body_sha256"]],"synthetic_fixture":True}
    for name, payload in (("live.json",report),("semantic.json",semantic)):
        (work/name).write_text(json.dumps(payload),encoding="utf-8")
    (work/"contract.xml").write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"/></testsuites>')
    canonical_rows=[{"instrument_id":row["exchange"]+":"+row["stock_code"],"symbol":row["stock_code"],
                     "exchange":row["exchange"],"asset_type":row["asset_type"],"status":row.get("status","active")} for row in rows]
    pq.write_table(__import__('pyarrow').Table.from_pylist(canonical_rows),work/"data.parquet")
    manifest = Manifest.from_file(work/"data.parquet",dataset="security_master",row_count=len(rows),first_key=None,last_key=None,
        raw_refs=(str(raw),),publication_metadata={"coverage_passed":True,"fallback":False})
    manifest.write_atomic(work/"manifest.json")
    artifacts = {"contract_tests":"contract.xml","live_report":"live.json","semantic_check":"semantic.json","publication_manifest":"manifest.json"}
    proof = {"input_id":input_id,"capability_version":version,
             **{layer:{"path":str(work/name),"sha256":file_hash(work/name)} for layer,name in artifacts.items()}}
    proof_path = work/"qualification.json"
    proof_path.write_text(json.dumps(proof),encoding="utf-8")
    now = datetime.now(timezone.utc)
    scope = ("input:"+input_id,"parameters:"+row_hash({k:v for k,v in params.items() if k!="trade_date"}),"qualification:"+str(proof_path))
    with MetadataStore(root/"metadata/metadata.duckdb") as metadata:
        for group in groups:
            exchange, asset = group.split('/')
            metadata.save_probe_evidence(ProbeEvidence(provider=contract.provider,endpoint=contract.endpoint,capability_version=version,
                validated_at=now,validation_expires_at=now+timedelta(days=1),status="complete",eligible_for_selection=True,
                row_count=1,first_key=None,last_key=None,evidence_hash=file_hash(proof_path),request_scope=scope,
                field_semantics=("stock_code","exchange","asset_type","trade_date","status"),units=("identity:exchange/code",)),
                dataset="security_master",market=exchange,asset_type=asset,frequency="snapshot")
    return contract, proof_path


@pytest.mark.parametrize("failure", [None,"hash","expired","scope","semantic","cooldown","version","code","pending","replay","different_body"])
def test_security_source_qualification_requires_verified_current_artifacts(tmp_path, failure):
    from stock_data_manage.routing.factory import check_security_input_qualification
    from datetime import timedelta
    context = full_master_definition()["units"][0]["context"]
    contract,path = save_fixture_qualification(tmp_path,"SDA-BOARD-005",context)
    proof = json.loads(path.read_text())
    with MetadataStore(tmp_path/"metadata/metadata.duckdb") as metadata:
        now = datetime.now(timezone.utc)
        if failure=="hash": path.write_text('{}')
        if failure=="scope": context["request"]["symbols"]=["sh600001"]
        if failure=="expired": now += timedelta(days=2)
        if failure=="cooldown":
            metadata.record_provider_failure(provider=contract.provider,endpoint=contract.endpoint,capability_version=proof["capability_version"],
                market="XSHG",asset_type="stock",dataset="security_master",failure_class="rate_limited",error="fixture 429",now=now,cooldown_seconds=30)
        if failure in {"semantic","version","code"}:
            if failure=="version": proof["capability_version"]="old-fixture-version"
            else:
                layer = "semantic_check" if failure=="semantic" else "live_report"
                artifact = Path(proof[layer]["path"])
                doc = json.loads(artifact.read_text())
                if failure=="semantic":doc["independent_full_market_coverage_verified"]=False
                else: doc["code_files"][0]["sha256"]="wrong-fixture-code-hash"
                artifact.write_text(json.dumps(doc))
                proof[layer]["sha256"]=file_hash(artifact)
            path.write_text(json.dumps(proof))
            metadata.connection.execute("UPDATE capability_registry SET evidence_hash=?",[file_hash(path)])
        if failure=="pending":(Path(proof["publication_manifest"]["path"]).parent/"task-commit.json").write_text('{}')
        if failure in {"replay", "different_body"}:
            # End-to-end replay stores its own manifest, but must retain the
            # verified response bytes rather than a similarly shaped dataset.
            manifest_path = Path(proof["publication_manifest"]["path"])
            original = Manifest.load(manifest_path)
            store = RawObjectStore(tmp_path / "replay-e2e")
            body = b'{"synthetic_transport_fixture": true}' if failure == "replay" else b'{"different": true}'
            store.record_response(response=SimpleNamespace(content=body,status_code=200,headers={},encoding="utf-8"),
                url="https://fixture.invalid/replay",method="GET",request_headers={},provider=contract.provider,
                endpoint=contract.endpoint,scope={},code_version="fixture-v1",mode="replay")
            from dataclasses import replace
            replace(original,raw_refs=(str(store.root / "manifest.ndjson"),)).write_atomic(manifest_path)
            proof["publication_manifest"]["sha256"] = file_hash(manifest_path)
            path.write_text(json.dumps(proof))
            metadata.connection.execute("UPDATE capability_registry SET evidence_hash=?",[file_hash(path)])
        result=check_security_input_qualification(contract,context,config_root=CONFIG,metadata=metadata,now=now)
        assert result["eligible"] is (failure in {None, "replay"}), result


def test_qualified_schedule_uses_same_task_for_idempotence_and_fallback_retry(tmp_path):
    from stock_data_manage.pipeline.inputs import collect_due_inputs
    from stock_data_manage.worker.scheduler import SHANGHAI
    now = datetime.now(timezone.utc).astimezone(SHANGHAI).replace(hour=8,minute=0,second=0,microsecond=0)
    # If tests run after 08:00, use an evaluation time later than fixture registration.
    if now < datetime.now(timezone.utc).astimezone(SHANGHAI): now=datetime.now(timezone.utc).astimezone(SHANGHAI)
    day=now.date().isoformat()
    collector=full_catalog_collector();collector.source_date=day
    collector.catalog_rows["SDA-BOARD-005"]=collector.catalog_rows["shenzhen-shanghai"]
    collector.catalog_rows["SECURITY-BSE-001"]=collector.catalog_rows["beijing"]
    definition=full_master_definition(day=day)
    for unit in definition["units"]:save_fixture_qualification(tmp_path,unit["input_id"],unit["context"])
    prior={**definition,"task_id":"earlier-publication"}
    for unit in prior["units"]:unit["key"]=unit["input_id"]
    assert run(tmp_path,prior,collector)["status"]=="published"
    collector.fail.add("SECURITY-BSE-001")
    now=max(now,datetime.now(timezone.utc).astimezone(SHANGHAI))
    args=dict(now=now,config_root=CONFIG,data_root=tmp_path,dataset="security_master",execute=True,mode="live",
              calendar_days=[{"trade_date":day,"is_trading_day":True}],collector=collector)
    first=collect_due_inputs(**args)
    assert first["jobs"][0]["status"]=="published",first
    assert not first["jobs"][0]["complete_today"] and first["jobs"][0]["fallback_used"]
    collector.fail.clear()
    second=collect_due_inputs(**args)
    assert second["jobs"][0]["complete_today"] and not second["jobs"][0]["fallback_used"]
    assert len(collector.calls)==5
    third=collect_due_inputs(**args)
    assert third["jobs"][0]["no_op"] and len(collector.calls)==5
    assert third["production_writes"]==0


@pytest.mark.parametrize("http_status", [403, 429])
def test_catalog_live_http_failure_blocks_later_calls(tmp_path, http_status):
    collector = full_catalog_collector()
    definition = full_master_definition()
    contract, proof = save_fixture_qualification(tmp_path, "SDA-BOARD-005", definition["units"][0]["context"])
    save_fixture_qualification(tmp_path, "SECURITY-BSE-001", definition["units"][1]["context"])

    def forbidden(**options):
        report = collector(**options)
        if options["input_id"] == contract.input_id:
            report.update(status="failed", responses=[{"mode": "live", "status_code": http_status}])
        return report

    assert run(tmp_path, definition, forbidden, mode="live")["status"] == "failed"
    count = len(collector.calls)
    assert run(tmp_path, definition, collector, mode="live")["status"] == "failed"
    assert len(collector.calls) == count
    with MetadataStore(tmp_path / "metadata/metadata.duckdb") as metadata:
        health = metadata.provider_health(provider=contract.provider, endpoint=contract.endpoint,
            capability_version=json.loads(proof.read_text())["capability_version"], dataset="security_master",
            market="XSHG", asset_type="stock")
        assert health.last_error == f"HTTP {http_status}"
        assert health.http_403_count == int(http_status == 403)
        assert health.http_429_count == int(http_status == 429)


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
    assert RawObjectStore.task_unit_path(tmp_path / "raw", "next", "beijing", data_date="2026-10-01").is_dir()
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
    assert RawObjectStore.task_unit_path(tmp_path / "raw", "complete-master", "beijing", data_date=DAY).exists()


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
    pending = RawObjectStore.task_unit_path(tmp_path / "raw", "other", "unit", data_date=DAY)
    pending.mkdir(parents=True)
    (pending / "user-evidence.txt").write_text("keep", encoding="utf-8")
    assert run(tmp_path, definition, collector)["status"] == "published"
    replay = Path(collector.calls[-1][2])
    assert replay.is_file() and RawObjectStore.verify_manifest(replay)
    assert run(tmp_path, definition, collector, redo="full")["status"] == "published"
    assert (pending / "user-evidence.txt").read_text(encoding="utf-8") == "keep"


def test_daily_raw_dates_are_isolated_during_failure_resume_and_full_redo(tmp_path):
    collector = FaultCollector()
    first = run(tmp_path, daily_definition(symbols=("sh600001",)), collector)
    old_raw = Path(first["units"]["sh600001"]["current_raw"])
    old_hashes = {str(p.relative_to(old_raw)): file_hash(p) for p in old_raw.rglob("*") if p.is_file()}
    next_day = "2026-10-09"
    definition = daily_definition("next-day", symbols=("sh600001",))
    definition["trade_date"] = next_day
    definition["units"][0]["context"]["request"].update(start_date=next_day, end_date=next_day)
    collector.fail.add("sh600001")
    assert run(tmp_path, definition, collector)["status"] == "failed"
    assert RawObjectStore.task_unit_path(tmp_path / "raw", "next-day", "sh600001", data_date=next_day).exists()
    collector.fail.clear()
    published = run(tmp_path, definition, collector)
    new_raw = Path(published["units"]["sh600001"]["current_raw"])
    assert old_raw.parent.name == DAY and new_raw.parent.name == next_day
    assert new_raw != old_raw
    # A full redo must clear only this task's target day, including when another
    # date has a same-named staging directory.
    untouched = RawObjectStore.task_unit_path(tmp_path / "raw", "next-day", "pending", data_date=DAY)
    untouched.mkdir(parents=True)
    (untouched / "sentinel.txt").write_text("keep", encoding="utf-8")
    collector.prices["sh600001"] = "20"
    assert run(tmp_path, definition, collector, redo="full")["status"] == "published"
    assert {str(p.relative_to(old_raw)): file_hash(p) for p in old_raw.rglob("*") if p.is_file()} == old_hashes
    assert (untouched / "sentinel.txt").read_text(encoding="utf-8") == "keep"
    assert len(list(new_raw.parent.glob("scope-*"))) == 1
    bars = CanonicalPartitionStore(tmp_path / "canonical").read(Dataset.DAILY_BAR, "stock", next_day)
    assert len(bars) == 1 and bars[0].close == Decimal("20")


def test_selected_redo_preserves_other_current_raw_scopes(tmp_path):
    collector = FaultCollector()
    definition = daily_definition()
    initial = run(tmp_path, definition, collector)
    before = {key: RawObjectStore.verify_manifest(Path(entry["current_raw"]) / "manifest.ndjson")
              for key, entry in initial["units"].items()}
    collector.prices["sh600002"] = "20"
    updated = run(tmp_path, definition, collector, redo="selected", symbols=["sh600002"])
    after = {key: RawObjectStore.verify_manifest(Path(entry["current_raw"]) / "manifest.ndjson")
             for key, entry in updated["units"].items()}
    assert before["sh600001"] == after["sh600001"]
    assert before["sh600003"] == after["sh600003"]
    assert before["sh600002"] != after["sh600002"]
    assert len(list((tmp_path / "raw/tencent/kline_daily" / DAY).glob("scope-*"))) == 3


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
