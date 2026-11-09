from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

from ..domain import Adjustment, AssetType, DailyRecord, QuoteGroup, QuoteStatus, Dataset, Exchange
from ..routing.capabilities import ProviderCapability
from ..routing.router import plan_realtime_collection
from ..storage.integrity import file_hash
from ..storage.daily import DailyBarStore, initialize_schema
from ..storage.metadata import MetadataStore
from ..storage.integrity import row_hash


@dataclass(frozen=True, slots=True)
class OfflineAcceptanceReport:
    test_run_id: str
    started_at: str
    finished_at: str
    scope: dict[str, object]
    checks: dict[str, dict[str, object]]
    gates: dict[str, str]
    metrics: dict[str, object]
    known_gaps: tuple[str, ...]
    conclusion: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def run_offline_acceptance(
    root: str | Path,
    *,
    end_date: date = date(2026, 9, 11),
    test_run_id: str | None = None,
) -> OfflineAcceptanceReport:
    started = datetime.now(timezone.utc)
    run_id = test_run_id or f"offline-components-{started:%Y%m%d-%H%M%S-%f}"
    root_path = Path(root)
    replay_dates = _weekdays_ending(end_date, 20)
    instrument_id = "XSHG:600519"
    replay_no_ops = 0
    replay_mismatches = 0
    receipt_refs = []
    with MetadataStore(root_path / "metadata.sqlite3") as metadata:
        initialize_schema(metadata.connection)
        daily_store = DailyBarStore(metadata.connection)
        scope = {"dataset": "daily_bar", "frequency": "1d", "asset_types": ["stock"],
                 "securities_by_date": {day.isoformat(): [instrument_id] for day in replay_dates}}
        queue_state = {"task_id": run_id, "dataset": "daily_bar", "definition_hash": row_hash(scope),
                       "status": "waiting"}
        metadata.enqueue_collection_task(queue_state, scope,
            snapshot={"registry_hash": metadata.partition_registry_snapshot(scope)},
            daily_owner_refs=daily_store.current_owners(scope))
        claimed = metadata.claim_next_collection_task()
        if not claimed or claimed["task_id"] != run_id:
            raise RuntimeError("offline acceptance task was not the claimed FIFO head")
        authorization = claimed["daily_overwrite_grant"]
        for trade_date in replay_dates:
            record = _acceptance_bar(trade_date, instrument_id)
            descriptor = root_path / "daily-candidates" / run_id / trade_date.isoformat() / "descriptor.json"
            descriptor.parent.mkdir(parents=True, exist_ok=True)
            descriptor.write_text(json.dumps({"task_id": run_id, "trade_date": trade_date.isoformat(),
                "instrument_id": instrument_id, "source": "synthetic acceptance fixture"}, sort_keys=True), encoding="utf-8")
            candidate_ref = json.dumps({"path": str(descriptor), "sha256": file_hash(descriptor)},
                                       sort_keys=True, separators=(",", ":"))
            publication_args = dict(trade_date=trade_date, asset_type=AssetType.STOCK, task_id=run_id,
                first_execution_order=claimed["first_execution_order"],
                publication_id=f"{run_id}:{trade_date.isoformat()}",
                idempotency_key=f"{run_id}:{trade_date.isoformat()}", scope=frozenset({instrument_id}),
                records=(record,), no_trade={}, raw_evidence_refs=("fixture/acceptance.json",),
                candidate_ref=candidate_ref, committed_at=datetime.now(timezone.utc), authorization=authorization)
            first = daily_store.publish_partition(**publication_args)
            replay = daily_store.publish_partition(**publication_args)
            replay_no_ops += int(replay.idempotent)
            replay_mismatches += int(daily_store.get(instrument_id, trade_date) != record)
            receipt_refs.append({"publication_id": first.publication_id, "candidate_ref": candidate_ref})
        claimed.update(status="published", phase="finished")
        metadata.save_collection_task(claimed)

        recovery_day = end_date + timedelta(days=1)
        recovery_record = _acceptance_bar(recovery_day, "XSHG:600520")
        recovery_scope = {"dataset": "daily_bar", "frequency": "1d", "asset_types": ["stock"],
                          "securities_by_date": {recovery_day.isoformat(): [recovery_record.instrument_id]}}
        recovery_id = f"{run_id}-transaction-retry-{started:%Y%m%d-%H%M%S-%f}"
        recovery_state = {"task_id": recovery_id, "dataset": "daily_bar", "definition_hash": row_hash(recovery_scope),
                          "status": "waiting"}
        metadata.enqueue_collection_task(recovery_state, recovery_scope,
            snapshot={"registry_hash": metadata.partition_registry_snapshot(recovery_scope)},
            daily_owner_refs=daily_store.current_owners(recovery_scope))
        recovery_claim = metadata.claim_next_collection_task()
        recovery_authorization = recovery_claim["daily_overwrite_grant"]
        recovery_descriptor = root_path / "daily-candidates" / recovery_id / "descriptor.json"
        recovery_descriptor.parent.mkdir(parents=True, exist_ok=True)
        recovery_descriptor.write_text(json.dumps({"task_id": recovery_id, "trade_date": recovery_day.isoformat(),
            "instrument_id": recovery_record.instrument_id, "source": "synthetic acceptance fixture"}, sort_keys=True), encoding="utf-8")
        recovery_candidate_ref = json.dumps({"path": str(recovery_descriptor), "sha256": file_hash(recovery_descriptor)},
                                            sort_keys=True, separators=(",", ":"))
        recovery_args = dict(trade_date=recovery_day, asset_type=AssetType.STOCK, task_id=recovery_id,
            first_execution_order=recovery_claim["first_execution_order"], publication_id=f"{recovery_id}:publication",
            idempotency_key=f"{recovery_id}:commit", scope=frozenset({recovery_record.instrument_id}),
            records=(recovery_record,), no_trade={}, raw_evidence_refs=("fixture/acceptance.json",),
            candidate_ref=recovery_candidate_ref, committed_at=datetime.now(timezone.utc),
            authorization=recovery_authorization)
        prior_record = daily_store.get(recovery_record.instrument_id, recovery_day)
        metadata.connection.execute("CREATE TRIGGER acceptance_fail_quote BEFORE INSERT ON daily_quote BEGIN SELECT RAISE(ABORT,'acceptance injected failure'); END")
        transaction_rollback = False
        try:
            daily_store.publish_partition(**recovery_args)
        except Exception as exc:
            transaction_rollback = "acceptance injected failure" in str(exc)
        finally:
            metadata.connection.execute("DROP TRIGGER acceptance_fail_quote")
        rollback_clean = (daily_store.get(recovery_record.instrument_id, recovery_day) == prior_record
                          and metadata.connection.execute("SELECT count(*) FROM daily_publication WHERE task_id=?", [recovery_id]).fetchone()[0] == 0)
        retried = daily_store.publish_partition(**recovery_args)
        replayed = daily_store.publish_partition(**recovery_args)
        recovery_passed = transaction_rollback and rollback_clean and retried.written_count == 1 and replayed.idempotent
        recovery_claim.update(status="published", phase="finished")
        metadata.save_collection_task(recovery_claim)

    capability = ProviderCapability(
        provider="fixture",
        endpoint="native_1m",
        version="acceptance-v1",
        datasets=frozenset({Dataset.MINUTE_BAR_1M}),
        exchanges=frozenset({Exchange.XSHG}),
        asset_types=frozenset({AssetType.STOCK}),
        frequencies=frozenset({1}),
        adjustments=frozenset({Adjustment.NONE}),
        priority=100,
        validated_at=started - timedelta(days=1),
        validation_expires_at=started + timedelta(days=1),
        max_symbols_per_request=100,
        request_interval_seconds=1,
        effective_concurrency=1,
    )
    watchlist = [f"sh{index:06d}" for index in range(200)]
    capacity_plan = plan_realtime_collection(
        universe_type="watchlist",
        symbols=watchlist,
        capabilities=[capability],
        max_watchlist_symbols=200,
        cycle_deadline_seconds=50,
    )
    finished = datetime.now(timezone.utc)
    checks = {
        "storage_replay": {
            "expected": {"no_op_partitions": len(replay_dates), "readback_mismatches": 0},
            "actual": {"no_op_partitions": replay_no_ops, "readback_mismatches": replay_mismatches},
            "status": "PASS" if replay_no_ops == len(replay_dates) and replay_mismatches == 0 else "FAIL",
            "evidence": receipt_refs,
        },
        "daily_table_transaction_recovery": {
            "expected": {"injected_transaction_failure": True, "rollback_clean": True,
                         "retry_written_count": 1, "same_key_replay_idempotent": True},
            "actual": {"injected_transaction_failure": transaction_rollback, "rollback_clean": rollback_clean,
                       "retry_written_count": retried.written_count, "same_key_replay_idempotent": replayed.idempotent},
            "status": "PASS" if recovery_passed else "FAIL",
            "evidence": {"task_id": recovery_id, "publication_id": retried.publication_id,
                         "candidate_ref": recovery_candidate_ref},
        },
        "minute_capacity_estimate": {
            "expected": {"watchlist_symbols": 200, "cycle_deadline_seconds": 50},
            "actual": {"watchlist_symbols": len(watchlist), "estimated_cycle_seconds": capacity_plan.estimated_cycle_seconds},
            "status": "PASS" if len(watchlist) == 200 and capacity_plan.estimated_cycle_seconds <= 50 else "FAIL",
            "evidence": {"provider": "fixture", "max_symbols_per_request": 100, "request_interval_seconds": 1},
        },
    }
    return OfflineAcceptanceReport(
        test_run_id=run_id,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        scope={"data_origin": "synthetic", "network_calls": 0,
               "replay_dates": [day.isoformat() for day in replay_dates],
               "date_basis": "weekdays, not exchange trading calendar", "replay_instruments": [instrument_id],
               "adjustments": [item.value for item in Adjustment], "capacity_mode": "planner estimate only",
               "runtime_root": str(root_path)},
        checks=checks,
        gates={
            "A": "NOT_RUN_TEST_SUITE_AND_STATIC_CHECKS_REQUIRED",
            "B": "NOT_RUN_LIVE_PROVIDER_REQUIRED",
            "C": "PARTIAL_COMPONENT_ONLY" if checks["storage_replay"]["status"] == "PASS" else "FAIL_COMPONENT_CHECK",
            "D": "PARTIAL_COMPONENT_ONLY" if checks["daily_table_transaction_recovery"]["status"] == "PASS" else "FAIL_COMPONENT_CHECK",
            "E": "PARTIAL_ESTIMATE_ONLY" if checks["minute_capacity_estimate"]["status"] == "PASS" else "FAIL_COMPONENT_CHECK",
            "F": "NOT_STARTED_REAL_TRADING_DAYS_REQUIRED",
        },
        metrics={
            "replay_weekdays": len(replay_dates),
            "replay_partition_no_ops": replay_no_ops,
            "replay_readback_mismatches": replay_mismatches,
            "watchlist_symbols": len(watchlist),
            "estimated_cycle_seconds": capacity_plan.estimated_cycle_seconds,
            "daily_table_transaction_rollback": transaction_rollback,
            "daily_table_retry_written_count": retried.written_count,
        },
        known_gaps=(
            "A Gate requires actual test-suite and static-check evidence",
            "B Gate requires live Sina/Tencent/TDX probes and verified Normalization Rules",
            "C/D Gates require full business scope and the complete fault matrix",
            "E Gate requires measured throughput and latency, not a synthetic budget estimate",
            "F Gate requires five consecutive real trading days",
        ),
        conclusion=("OFFLINE_COMPONENT_CHECKS_PASS_PROJECT_GATES_PENDING" if all(
            check["status"] == "PASS" for check in checks.values()) else "OFFLINE_COMPONENT_CHECKS_FAILED"),
    )


def _weekdays_ending(end_date: date, count: int) -> tuple[date, ...]:
    result: list[date] = []
    cursor = end_date
    while len(result) < count:
        if cursor.weekday() < 5:
            result.append(cursor)
        cursor -= timedelta(days=1)
    return tuple(reversed(result))


def _acceptance_bar(trade_date: date, instrument_id: str) -> DailyRecord:
    def group(price):
        return QuoteGroup(QuoteStatus.AVAILABLE, open=Decimal(price), high=Decimal(price),
            low=Decimal(price), close=Decimal(price), volume=Decimal("1000"), amount=Decimal("10000"),
            source_provider="fixture", endpoint="history", capability_version="acceptance-v1",
            normalizer_version="acceptance-v1", raw_object_path="fixture/acceptance.json",
            fetch_time=datetime.combine(trade_date, time(16), tzinfo=timezone.utc))
    return DailyRecord(instrument_id, trade_date, AssetType.STOCK, group("10"), group("9"), group("11"))
