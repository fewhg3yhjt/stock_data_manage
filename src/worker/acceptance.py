from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from ..domain import Adjustment, AssetType, DailyRecord, QuoteGroup, QuoteStatus, Dataset, Exchange, ItemStatus
from ..routing.capabilities import ProviderCapability
from ..routing.router import plan_realtime_collection
from ..storage.integrity import file_hash
from ..storage.metadata import MetadataStore
from ..storage.parquet import CanonicalPartitionStore
from .recovery import RecoveryScanner


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
    canonical = CanonicalPartitionStore(root_path / "canonical")
    # Keep a fresh fault-injection partition per run; never clear prior evidence.
    recovery_root = root_path / f"recovery-{started:%Y%m%d-%H%M%S-%f}"
    replay_dates = _weekdays_ending(end_date, 20)
    instrument_id = "XSHG:600519"
    replay_no_ops = 0
    replay_mismatches = 0
    manifests = []
    with MetadataStore(root_path / "metadata.duckdb") as metadata:
        for trade_date in replay_dates:
            record = _acceptance_bar(trade_date, instrument_id)
            kwargs = dict(
                dataset=Dataset.DAILY_BAR,
                asset_type=AssetType.STOCK.value,
                partition_key=trade_date.isoformat(),
                expected_count=1,
                item_statuses={instrument_id: ItemStatus.SUCCESS},
                source_providers={instrument_id: "fixture"},
            )
            canonical.publish(new_records=[record], run_id=f"{run_id}-{trade_date}", **kwargs)
            rerun = canonical.publish(new_records=[], run_id=f"{run_id}-rerun-{trade_date}", **kwargs)
            replay_no_ops += int(rerun.no_op)
            replay_mismatches += int(canonical.read_daily(AssetType.STOCK.value, trade_date.isoformat()) != [record])
            manifests.append({"path": str(rerun.manifest_path), "sha256": file_hash(rerun.manifest_path)})
            metadata.record_partition_publish(
                manifest=rerun.manifest,
                manifest_path=rerun.manifest_path,
                item_statuses={instrument_id: ItemStatus.SUCCESS},
                source_providers={instrument_id: "fixture"},
            )

        recovery_record = _acceptance_bar(end_date, "XSHG:600520")

        class ExpectedPowerLoss(RuntimeError):
            pass

        interrupted = False

        def interrupt(stage: str) -> None:
            if stage == "after_data_replace":
                raise ExpectedPowerLoss("simulated acceptance power loss")

        try:
            canonical_recovery = CanonicalPartitionStore(recovery_root)
            canonical_recovery.publish(
                dataset=Dataset.DAILY_BAR,
                asset_type=AssetType.STOCK.value,
                partition_key=end_date.isoformat(),
                new_records=[recovery_record],
                expected_count=1,
                run_id=f"{run_id}-recovery",
                item_statuses={recovery_record.instrument_id: ItemStatus.SUCCESS},
                source_providers={recovery_record.instrument_id: "fixture"},
                failure_hook=interrupt,
            )
        except ExpectedPowerLoss:
            interrupted = True
        recovery_report = RecoveryScanner(recovery_root, metadata).recover()

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
            "evidence": manifests,
        },
        "publication_recovery": {
            "expected": {"injected_interruption": True, "recovered_partitions_min": 1, "invalid_final_partitions": 0},
            "actual": {"injected_interruption": interrupted,
                       "recovered_partitions": recovery_report.promoted_temporary_partitions + recovery_report.repaired_metadata_partitions,
                       "invalid_final_partitions": len(recovery_report.invalid_final_partitions)},
            "status": "PASS" if interrupted and not recovery_report.invalid_final_partitions and (
                recovery_report.promoted_temporary_partitions + recovery_report.repaired_metadata_partitions >= 1) else "FAIL",
            "evidence": {"root": str(recovery_root), "invalid_final_partitions": list(recovery_report.invalid_final_partitions)},
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
            "D": "PARTIAL_COMPONENT_ONLY" if checks["publication_recovery"]["status"] == "PASS" else "FAIL_COMPONENT_CHECK",
            "E": "PARTIAL_ESTIMATE_ONLY" if checks["minute_capacity_estimate"]["status"] == "PASS" else "FAIL_COMPONENT_CHECK",
            "F": "NOT_STARTED_REAL_TRADING_DAYS_REQUIRED",
        },
        metrics={
            "replay_weekdays": len(replay_dates),
            "replay_partition_no_ops": replay_no_ops,
            "replay_readback_mismatches": replay_mismatches,
            "watchlist_symbols": len(watchlist),
            "estimated_cycle_seconds": capacity_plan.estimated_cycle_seconds,
            "recovery_promoted_temporary_partitions": recovery_report.promoted_temporary_partitions,
            "recovery_repaired_metadata_partitions": recovery_report.repaired_metadata_partitions,
            "recovery_invalid_final_partitions": len(recovery_report.invalid_final_partitions),
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
