from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable
from uuid import uuid4

from ..domain import (
    Adjustment,
    AssetType,
    AttemptStatus,
    Dataset,
    Exchange,
    ItemStatus,
    QualityStatus,
)
from ..providers.base import DailyProvider
from ..providers.contracts import FailureClass, ProviderContractError
from ..quality.normalization import Normalizer
from ..quality.publication import PublicationPolicy
from ..quality.validation import validate_bar
from ..routing.router import calculate_missing_set
from ..storage.metadata import MetadataStore
from ..storage.parquet import CanonicalPartitionStore, PublishResult
from ..storage.raw import RawObjectStore
from ..worker.attempts import CollectionAttempt


@dataclass(frozen=True, slots=True)
class Instrument:
    instrument_id: str
    source_symbol: str
    exchange: Exchange
    asset_type: AssetType


@dataclass(frozen=True, slots=True)
class CollectionRunResult:
    publish: PublishResult
    item_statuses: dict[str, ItemStatus]
    requested_by_provider: dict[str, tuple[str, ...]]
    attempt_ids: tuple[str, ...]


class PublicationThresholdExceeded(ValueError):
    pass


def prepare_daily_task_publication(state, units, source_rows, paths, config_root, trade_date):
    """Resolve persisted normalized inputs before touching the existing canonical partition."""
    import yaml
    from decimal import Decimal
    from ..domain import BarRecord
    from ..quality.publication import PublicationPolicy
    from ..quality.resolution import resolve_records

    definition = state["definition"]
    adjustment = Adjustment(definition.get("adjustment", "forward"))
    asset_type = definition.get("asset_type", "stock")
    AssetType(asset_type)
    expected = {}
    for unit in units:
        symbol = unit["symbol"]
        if not symbol.startswith(("sh", "sz", "bj")) or not symbol[2:].isdigit():
            raise ValueError("daily task requires an exchange-qualified security symbol")
        exchange = {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[symbol[:2]]
        expected[symbol] = exchange + ":" + symbol[2:]
    selected = set(state.get("selected_symbols", ()))
    target_ids = {expected[symbol] for symbol in selected} if state["redo"] == "selected" else set(expected.values())
    records = {}
    for key, source in source_rows.items():
        if selected and source["symbol"] not in selected:
            continue
        contract, report = source["contract"], source["entry"]["report"]
        for row in source["rows"]:
            if row["trade_date"] != trade_date or row["adjustment"] != adjustment.value:
                raise ValueError("normalized input date or adjustment differs from task scope")
            if row["instrument_id"] != expected[source["symbol"]]:
                raise ValueError("normalized input security differs from requested security")
            record = BarRecord(dataset=Dataset.DAILY_BAR, instrument_id=row["instrument_id"],
                source_symbol=source["symbol"], trade_date=trade_date, adjustment=adjustment,
                open=row["open"], high=row["high"], low=row["low"], close=row["close"],
                volume=row.get("volume"), amount=row.get("amount"), source_provider=contract.provider,
                endpoint=contract.endpoint, source_method="daily_history", quality_status=QualityStatus.FINAL,
                field_level="full_bar" if row.get("amount") is not None else "ohlcv" if row.get("volume") is not None else "ohlc",
                capability_priority=0, capability_version=report.get("adapter_version", "unknown"),
                normalizer_version=report.get("normalization_version", "unknown"),
                raw_object_path=source["raw_ref"], fetch_time=datetime.fromisoformat(report["validation_time_utc"]))
            if not validate_bar(record).valid:
                raise ValueError("normalized daily candidate failed business validity")
            records.setdefault(record.key, []).append(record)
    accepted = []
    for candidates in records.values():
        resolved = resolve_records(candidates)
        if resolved.quarantined:
            raise ValueError("new daily candidates have a hard source conflict")
        accepted.append(resolved.selected)
    store = CanonicalPartitionStore(paths["canonical_root"])
    existing = store.read(Dataset.DAILY_BAR, asset_type, trade_date.isoformat())
    available = {record.instrument_id for record in accepted}
    if state["redo"] != "full":
        available.update(record.instrument_id for record in existing if record.adjustment is adjustment
                         and record.instrument_id not in target_ids and record.quality_status is QualityStatus.FINAL)
    # A resumed incomplete task has no prior published candidate: its successful
    # source units are in accepted. Explicit selected redo can preserve other keys.
    statuses = {instrument_id: ItemStatus.SUCCESS.value if instrument_id in available else ItemStatus.MISSING.value
                for instrument_id in expected.values()}
    settings = yaml.safe_load((Path(config_root) / "datasets/daily_bar.yaml").read_text(encoding="utf-8"))["dataset"]["publication"]
    policy = PublicationPolicy(Decimal(str(settings["max_missing_ratio"])), settings["max_missing_count"])
    if not policy.allows(expected_count=len(expected), actual_count=len(available & set(expected.values()))):
        raise PublicationThresholdExceeded("daily task coverage rejected; missing=" + ",".join(
            sorted(set(expected.values()) - available)))
    outside = [record for record in existing if record.adjustment is not adjustment or record.instrument_id not in expected.values()]
    return {"records": accepted, "partition_directory": str(store.partition_directory(Dataset.DAILY_BAR, asset_type, trade_date.isoformat())),
            "asset_type": asset_type, "partition_key": trade_date.isoformat(), "adjustment": adjustment.value,
            "expected_count": len(expected) + len(outside), "item_statuses": statuses,
            "replace_ids": sorted(target_ids)}


class DailyCollectionService:
    """Small end-to-end daily collection path used by schedulers and replay tests."""

    def __init__(
        self,
        *,
        providers: Iterable[DailyProvider],
        normalizer: Normalizer,
        raw_store: RawObjectStore,
        canonical_store: CanonicalPartitionStore,
        metadata: MetadataStore,
        publication_policy: PublicationPolicy,
        clock: Callable[[], datetime] | None = None,
        provider_cooldown_seconds: int = 300,
        provider_failure_threshold: int = 3,
    ) -> None:
        self.providers = tuple(providers)
        self.normalizer = normalizer
        self.raw_store = raw_store
        self.canonical_store = canonical_store
        self.metadata = metadata
        self.publication_policy = publication_policy
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.provider_cooldown_seconds = provider_cooldown_seconds
        self.provider_failure_threshold = provider_failure_threshold

    def collect(
        self,
        *,
        instruments: Iterable[Instrument],
        trade_date: date,
        run_id: str,
        adjustment: Adjustment = Adjustment.NONE,
    ) -> CollectionRunResult:
        universe = tuple(instruments)
        if not universe:
            raise ValueError("daily collection universe cannot be empty")
        asset_types = {instrument.asset_type for instrument in universe}
        if len(asset_types) != 1:
            raise ValueError("one collection partition may contain only one asset type")
        asset_type = next(iter(asset_types))
        if adjustment is Adjustment.FORWARD and asset_type is AssetType.INDEX:
            raise ValueError("index daily bars cannot use forward adjustment")
        partition_key = trade_date.isoformat()
        by_id = {instrument.instrument_id: instrument for instrument in universe}
        if len(by_id) != len(universe):
            raise ValueError("instrument_id must be unique within a universe")
        by_symbol = {instrument.source_symbol: instrument for instrument in universe}
        if len(by_symbol) != len(universe):
            raise ValueError("source_symbol must be unique within a provider collection partition")

        stored_statuses = self.metadata.item_statuses(Dataset.DAILY_BAR.value, partition_key)
        statuses = (
            {
                instrument_id: status
                for instrument_id, status in stored_statuses.items()
                if instrument_id in by_id
            }
            if adjustment is Adjustment.NONE
            else {}
        )
        all_existing = self.canonical_store.read(Dataset.DAILY_BAR, asset_type.value, partition_key)
        existing = [
            record
            for record in all_existing
            if record.adjustment is adjustment
        ]
        other_adjustment_count = len(all_existing) - len(existing)
        providers_by_item: dict[str, str | None] = {
            record.instrument_id: record.source_provider for record in existing
        }
        for record in existing:
            statuses[record.instrument_id] = (
                ItemStatus.SUCCESS
                if record.quality_status is QualityStatus.FINAL
                else ItemStatus.TEMPORARY_EMPTY
            )

        accepted = []
        requested_by_provider: dict[str, tuple[str, ...]] = {}
        validated_attempts: list[CollectionAttempt] = []
        attempt_ids: list[str] = []

        for provider in self.providers:
            if getattr(provider, "adjustment", Adjustment.NONE) is not adjustment:
                continue
            supported_asset_types = getattr(provider, "supported_asset_types", frozenset(AssetType))
            if asset_type not in supported_asset_types:
                continue
            missing_ids = calculate_missing_set(by_id, statuses)
            if not missing_ids:
                break
            eligible_instruments = []
            for instrument in universe:
                if instrument.instrument_id not in missing_ids:
                    continue
                health = self.metadata.provider_health(
                    provider=provider.name,
                    endpoint=provider.endpoint,
                    market=instrument.exchange.value,
                    asset_type=instrument.asset_type.value,
                    dataset=Dataset.DAILY_BAR.value,
                    capability_version=provider.capability_version,
                )
                if self.metadata.provider_available(health, self.clock()):
                    eligible_instruments.append(instrument)
            requested = tuple(instrument.source_symbol for instrument in eligible_instruments)
            if not requested:
                continue
            requested_by_provider[provider.name] = requested
            attempt_id = f"{run_id}-{provider.name}-{uuid4().hex[:12]}"
            attempt_ids.append(attempt_id)
            now = self.clock()
            attempt = CollectionAttempt(attempt_id).lease(
                owner="daily-collector", acquired_at=now, expires_at=now + timedelta(minutes=10)
            )
            attempt = attempt.transition(AttemptStatus.FETCHING)
            self.metadata.save_attempt(attempt, updated_at=now)
            try:
                response = provider.fetch_daily(requested, trade_date)
                if getattr(response, "adjustment", adjustment) is not adjustment:
                    raise ProviderContractError(
                        "provider returned a different adjustment than requested",
                        FailureClass.SCHEMA_CHANGED,
                        retryable=False,
                    )
            except Exception as exc:
                attempt = attempt.transition(AttemptStatus.RETRYABLE_FAILED)
                self.metadata.save_attempt(attempt, updated_at=self.clock())
                failure_class = (
                    exc.failure_class.value
                    if isinstance(exc, ProviderContractError)
                    else FailureClass.CONNECTION.value
                )
                http_status = exc.http_status if isinstance(exc, ProviderContractError) else None
                for exchange in {instrument.exchange for instrument in eligible_instruments}:
                    self.metadata.record_provider_failure(
                        provider=provider.name,
                        endpoint=provider.endpoint,
                        market=exchange.value,
                        asset_type=asset_type.value,
                        dataset=Dataset.DAILY_BAR.value,
                        capability_version=provider.capability_version,
                        failure_class=failure_class,
                        error=str(exc),
                        now=self.clock(),
                        cooldown_seconds=self.provider_cooldown_seconds,
                        failure_threshold=self.provider_failure_threshold,
                        http_status=http_status,
                    )
                continue

            raw = self.raw_store.write_json(
                {
                    "provider": provider.name,
                    "endpoint": provider.endpoint,
                    "trade_date": trade_date.isoformat(),
                    "requested_symbols": list(requested),
                    "adjustment": adjustment.value,
                    "rows": [dict(row) for row in response.rows],
                },
                dataset=Dataset.DAILY_BAR.value,
                provider=provider.name,
                endpoint=provider.endpoint,
                fetched_at=now,
                attempt_id=attempt_id,
            )
            attempt = replace(
                attempt,
                raw_object_path=str(raw.path),
                raw_content_hash=raw.content_hash,
            )
            if not response.rows:
                attempt = attempt.transition(AttemptStatus.TEMPORARY_EMPTY)
                self.metadata.save_attempt(attempt, updated_at=self.clock())
                for symbol in requested:
                    statuses[by_symbol[symbol].instrument_id] = ItemStatus.TEMPORARY_EMPTY
                for exchange in {instrument.exchange for instrument in eligible_instruments}:
                    self.metadata.record_provider_failure(
                        provider=provider.name,
                        endpoint=provider.endpoint,
                        market=exchange.value,
                        asset_type=asset_type.value,
                        dataset=Dataset.DAILY_BAR.value,
                        capability_version=provider.capability_version,
                        failure_class=FailureClass.TEMPORARY_EMPTY.value,
                        error="provider returned no rows",
                        now=self.clock(),
                        cooldown_seconds=self.provider_cooldown_seconds,
                        failure_threshold=self.provider_failure_threshold,
                    )
                continue

            attempt = attempt.transition(AttemptStatus.RAW_COMMITTED)
            self.metadata.save_attempt(attempt, updated_at=self.clock())
            for raw_row in response.rows:
                symbol = str(raw_row["symbol"])
                instrument = by_symbol.get(symbol)
                if instrument is None or instrument.instrument_id not in missing_ids:
                    continue
                try:
                    record = self.normalizer.normalize_bar(
                        raw_row,
                        dataset=Dataset.DAILY_BAR,
                        instrument_id=instrument.instrument_id,
                        exchange=instrument.exchange,
                        asset_type=instrument.asset_type,
                        provider=provider.name,
                        endpoint=provider.endpoint,
                        adjustment=adjustment,
                        capability_priority=provider.capability_priority,
                        capability_version=provider.capability_version,
                        raw_object_path=str(raw.path),
                        fetch_time=now,
                        quality_status=getattr(
                            provider, "quality_status", QualityStatus.FINAL
                        ),
                        source_method=getattr(provider, "source_method", "daily_history"),
                    )
                except Exception:
                    statuses[instrument.instrument_id] = ItemStatus.INVALID
                    continue
                if not validate_bar(record).valid:
                    statuses[instrument.instrument_id] = ItemStatus.INVALID
                    continue
                accepted.append(record)
                if record.quality_status is QualityStatus.FINAL:
                    statuses[instrument.instrument_id] = ItemStatus.SUCCESS
                else:
                    statuses[instrument.instrument_id] = ItemStatus.TEMPORARY_EMPTY
                providers_by_item[instrument.instrument_id] = provider.name
            for symbol in response.missing_symbols:
                statuses[by_symbol[symbol].instrument_id] = ItemStatus.TEMPORARY_EMPTY
            for exchange in {instrument.exchange for instrument in eligible_instruments}:
                exchange_symbols = {
                    instrument.source_symbol
                    for instrument in eligible_instruments
                    if instrument.exchange is exchange
                }
                returned = response.returned_symbols & exchange_symbols
                if returned:
                    self.metadata.record_provider_success(
                        provider=provider.name,
                        endpoint=provider.endpoint,
                        market=exchange.value,
                        asset_type=asset_type.value,
                        dataset=Dataset.DAILY_BAR.value,
                        capability_version=provider.capability_version,
                        now=self.clock(),
                        coverage=len(returned) / len(exchange_symbols),
                    )
                else:
                    self.metadata.record_provider_failure(
                        provider=provider.name,
                        endpoint=provider.endpoint,
                        market=exchange.value,
                        asset_type=asset_type.value,
                        dataset=Dataset.DAILY_BAR.value,
                        capability_version=provider.capability_version,
                        failure_class=FailureClass.TEMPORARY_EMPTY.value,
                        error="provider returned no rows for market scope",
                        now=self.clock(),
                        cooldown_seconds=self.provider_cooldown_seconds,
                        failure_threshold=self.provider_failure_threshold,
                    )
            attempt = attempt.transition(AttemptStatus.NORMALIZED).transition(AttemptStatus.VALIDATED)
            self.metadata.save_attempt(attempt, updated_at=self.clock())
            validated_attempts.append(attempt)

        for instrument_id in by_id:
            statuses.setdefault(instrument_id, ItemStatus.MISSING)

        available_ids = {
            record.instrument_id
            for record in (*existing, *accepted)
            if record.instrument_id in by_id
        }
        if not self.publication_policy.allows(
            expected_count=len(universe), actual_count=len(available_ids)
        ):
            missing_count = len(universe) - len(available_ids)
            missing_ratio = missing_count / len(universe)
            raise PublicationThresholdExceeded(
                "daily_bar publication rejected: "
                f"missing_count={missing_count}, missing_ratio={missing_ratio:.6f}"
            )

        publish = self.canonical_store.publish(
            dataset=Dataset.DAILY_BAR,
            asset_type=asset_type.value,
            partition_key=partition_key,
            new_records=accepted,
            expected_count=len(universe) + other_adjustment_count,
            run_id=run_id,
            item_statuses=statuses,
            source_providers=providers_by_item,
        )
        self.metadata.record_partition_publish(
            manifest=publish.manifest,
            manifest_path=publish.manifest_path,
            item_statuses=statuses,
            source_providers=providers_by_item,
            updated_at=self.clock(),
        )
        self.metadata.record_conflicts(
            dataset=Dataset.DAILY_BAR.value,
            partition_key=partition_key,
            conflicts=publish.conflicts,
            created_at=self.clock(),
        )
        for attempt in validated_attempts:
            self.metadata.save_attempt(
                attempt.transition(AttemptStatus.PUBLISHED), updated_at=self.clock()
            )
        return CollectionRunResult(
            publish=publish,
            item_statuses=dict(statuses),
            requested_by_provider=requested_by_provider,
            attempt_ids=tuple(attempt_ids),
        )
