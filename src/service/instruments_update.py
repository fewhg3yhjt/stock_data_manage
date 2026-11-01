from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Mapping, Protocol

from ..domain import AssetType, Exchange
from .instruments import (
    SecurityMasterStore,
    SecurityRecord,
    SecurityMergeResult,
    merge_security_sources,
    stable_instrument_id,
)


class SecurityListProvider(Protocol):
    name: str

    def fetch_security_list(self) -> list[Mapping[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class SecurityMasterUpdateResult:
    merge: SecurityMergeResult
    source_errors: dict[str, str]
    changed_fields: int


def parse_security_row(row: Mapping[str, Any], *, source: str) -> SecurityRecord:
    exchange = Exchange(str(row["exchange"]).upper())
    symbol = str(row.get("symbol") or row.get("code"))
    if symbol.startswith(("sh", "sz", "bj")):
        canonical_symbol = symbol[2:]
    else:
        canonical_symbol = symbol
    asset_type = AssetType(str(row.get("asset_type") or row.get("instrument_type") or "stock").lower())
    instrument_id = str(row.get("instrument_id") or stable_instrument_id(exchange, symbol))
    return SecurityRecord(
        instrument_id=instrument_id,
        symbol=canonical_symbol,
        exchange=exchange,
        asset_type=asset_type,
        name=None if row.get("name") is None else str(row["name"]),
        list_date=_as_date(row.get("list_date")),
        delist_date=_as_date(row.get("delist_date")),
        status=str(row.get("status") or "active"),
        lot_size=int(row.get("lot_size") or (100 if asset_type is not AssetType.INDEX else 1)),
        canonical_volume_unit=str(row.get("canonical_volume_unit") or "share"),
        canonical_amount_unit=str(row.get("canonical_amount_unit") or "CNY"),
        publisher=None if row.get("publisher") is None else str(row["publisher"]),
        publish_date=_as_date(row.get("publish_date")),
        calculation_start_date=_as_date(row.get("calculation_start_date")),
        is_backfilled_history=bool(row.get("is_backfilled_history", False)),
        evidence_sources=(source,),
    )


def apply_security_lifecycle_overrides(records, overrides, *, as_of_date):
    """Apply the small, user-confirmed lifecycle exception list to a dated master."""
    from collections.abc import Mapping as MappingABC

    records = list(records)
    overrides = tuple(overrides or ())
    if not overrides:
        return tuple(records)
    if as_of_date is None:
        raise ValueError("security lifecycle overrides require a dated master snapshot")
    snapshot_date = _as_date(as_of_date)
    seen = set()
    positions = {(item.exchange, item.asset_type, item.symbol): index for index, item in enumerate(records)}
    ids = {item.instrument_id: item for item in records}
    for item in overrides:
        if not isinstance(item, MappingABC):
            raise ValueError("security lifecycle override must be a mapping")
        exchange = Exchange(str(item.get("market", "")).upper())
        asset_type = AssetType(str(item.get("asset_type", "")).lower())
        symbol = str(item.get("symbol", ""))
        if len(symbol) != 6 or not symbol.isdigit():
            raise ValueError("security lifecycle override requires a six-digit symbol")
        key = (exchange, asset_type, symbol)
        if key in seen:
            raise ValueError("duplicate security lifecycle override")
        seen.add(key)
        delist_date = _as_date(item.get("delist_date"))
        name = str(item.get("name", "")).strip()
        evidence_refs = item.get("evidence_refs")
        if delist_date is None or not name or not isinstance(evidence_refs, (list, tuple)) or not evidence_refs:
            raise ValueError("security lifecycle override requires a name, inclusive delist_date and evidence references")
        if any(not isinstance(ref, str) or not ref.strip() for ref in evidence_refs):
            raise ValueError("security lifecycle override evidence reference is empty")

        index = positions.get(key)
        if index is not None:
            record = records[index]
            records[index] = replace(record, delist_date=delist_date,
                status="delisted" if snapshot_date > delist_date else record.status,
                name=record.name or name,
                evidence_sources=tuple(dict.fromkeys((*record.evidence_sources, "user_confirmed_lifecycle_override"))))
            continue
        if snapshot_date <= delist_date:
            continue
        instrument_id = stable_instrument_id(exchange, symbol)
        collision = ids.get(instrument_id)
        if collision is not None:
            raise ValueError("security lifecycle override conflicts with another asset type at the same market code")
        record = SecurityRecord(instrument_id=instrument_id, symbol=symbol, exchange=exchange,
            asset_type=asset_type, name=name, delist_date=delist_date, status="delisted",
            lot_size=1 if asset_type is AssetType.INDEX else 100,
            evidence_sources=("user_confirmed_lifecycle_override",))
        positions[key] = len(records)
        ids[instrument_id] = record
        records.append(record)
    return tuple(sorted(records, key=lambda item: item.instrument_id))


class SecurityMasterUpdater:
    def __init__(self, store: SecurityMasterStore, providers: list[SecurityListProvider]) -> None:
        self.store = store
        self.providers = tuple(providers)

    def update(self, *, effective_date: date) -> SecurityMasterUpdateResult:
        previous = self.store.all()
        source_rows: dict[str, list[SecurityRecord]] = {}
        errors: dict[str, str] = {}
        for provider in self.providers:
            try:
                rows = provider.fetch_security_list()
                source_rows[provider.name] = [parse_security_row(row, source=provider.name) for row in rows]
            except Exception as exc:
                source_rows[provider.name] = []
                errors[provider.name] = str(exc)
        merged = merge_security_sources(previous, source_rows)
        changes = []
        before = {record.instrument_id: record for record in previous}
        for record in merged.records:
            old = before.get(record.instrument_id)
            if old is None:
                changes.append((record.instrument_id, "created", None, record.symbol, effective_date, ",".join(record.evidence_sources)))
                continue
            for field in ("symbol", "name", "asset_type", "status", "list_date", "delist_date"):
                old_value = getattr(old, field)
                new_value = getattr(record, field)
                if old_value != new_value:
                    changes.append((record.instrument_id, field, old_value, new_value, effective_date, ",".join(record.evidence_sources)))
        self.store.upsert(merged.records, merged.symbol_history)
        self.store.record_changes(changes)
        return SecurityMasterUpdateResult(merged, errors, len(changes))


def prepare_security_publication(source_rows, previous=(), *, lifecycle_overrides=(), as_of_date=None):
    """Build from normalized source candidates with the existing identity and merge rules."""
    parsed = {}
    for source, rows in source_rows.items():
        parsed[source] = [parse_security_row({
            **row, "code": row.get("symbol", row.get("stock_code", row.get("code"))),
            "name": row.get("name", row.get("stock_name")),
        }, source=source) for row in rows]
    merged = merge_security_sources(previous, parsed)
    return replace(merged, records=apply_security_lifecycle_overrides(merged.records,
        lifecycle_overrides, as_of_date=as_of_date))


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
