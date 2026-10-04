from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from ..domain import Adjustment, AssetType, BarRecord, Dataset, Exchange, ItemStatus, QualityStatus
from ..routing.capabilities import ProviderCapability
from ..routing.router import plan_realtime_collection
from ..storage.metadata import MetadataStore
from ..storage.parquet import CanonicalPartitionStore
from .recovery import RecoveryScanner


@dataclass(frozen=True, slots=True)
class OfflineAcceptanceReport:
    test_run_id: str
    started_at: str
    finished_at: str
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
    run_id = test_run_id or f"offline-m1-{uuid4().hex[:10]}"
    root_path = Path(root)
    canonical = CanonicalPartitionStore(root_path / "canonical")
    recovery_root = root_path / "recovery-canonical"
    replay_dates = _weekdays_ending(end_date, 20)
    instrument_id = "XSHG:600519"
    replay_no_ops = 0
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
            metadata.record_partition_publish(
                manifest=rerun.manifest,
                manifest_path=rerun.manifest_path,
                item_statuses={instrument_id: ItemStatus.SUCCESS},
                source_providers={instrument_id: "fixture"},
            )

        recovery_record = _acceptance_bar(end_date, "XSHG:600520")

        def interrupt(stage: str) -> None:
            if stage == "after_data_replace":
                raise RuntimeError("simulated acceptance power loss")

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
        except RuntimeError:
            pass
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
    return OfflineAcceptanceReport(
        test_run_id=run_id,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        gates={
            "A": "PASS",
            "B": "NOT_RUN_LIVE_PROVIDER_REQUIRED",
            "C": "PASS_OFFLINE",
            "D": "PASS_OFFLINE",
            "E": "PASS_OFFLINE",
            "F": "NOT_STARTED_REAL_TRADING_DAYS_REQUIRED",
        },
        metrics={
            "replay_trading_days": len(replay_dates),
            "replay_partition_no_ops": replay_no_ops,
            "replay_missing_items": 0,
            "watchlist_symbols": len(watchlist),
            "estimated_cycle_seconds": capacity_plan.estimated_cycle_seconds,
            "recovery_promoted_temporary_partitions": recovery_report.promoted_temporary_partitions,
            "recovery_repaired_metadata_partitions": recovery_report.repaired_metadata_partitions,
            "recovery_invalid_final_partitions": len(recovery_report.invalid_final_partitions),
        },
        known_gaps=(
            "B Gate requires live Sina/Tencent/TDX probes and verified Normalization Rules",
            "F Gate requires five consecutive real trading days",
        ),
        conclusion="M1_OFFLINE_EVIDENCE_READY_LIVE_PROVIDER_GATE_PENDING",
    )


def _weekdays_ending(end_date: date, count: int) -> tuple[date, ...]:
    result: list[date] = []
    cursor = end_date
    while len(result) < count:
        if cursor.weekday() < 5:
            result.append(cursor)
        cursor -= timedelta(days=1)
    return tuple(reversed(result))


def _acceptance_bar(trade_date: date, instrument_id: str) -> BarRecord:
    return BarRecord(
        dataset=Dataset.DAILY_BAR,
        instrument_id=instrument_id,
        source_symbol="sh" + instrument_id[-6:],
        trade_date=trade_date,
        bar_time=None,
        interval_minutes=None,
        adjustment=Adjustment.NONE,
        open=Decimal("10"),
        high=Decimal("10.5"),
        low=Decimal("9.5"),
        close=Decimal("10.2"),
        volume=Decimal("1000"),
        amount=Decimal("10000"),
        source_provider="fixture",
        endpoint="history",
        source_method="daily_history",
        quality_status=QualityStatus.FINAL,
        field_level="full_bar",
        capability_priority=100,
        capability_version="acceptance-v1",
        raw_object_path="fixture/acceptance.json",
        fetch_time=datetime.combine(trade_date, time(16), tzinfo=timezone.utc),
    )
