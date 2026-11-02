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
    normalize_symbol_successions,
    symbol_succession_for,
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
    """Apply date-bounded manual lifecycle and status decisions to a dated master."""
    from collections.abc import Mapping as MappingABC

    records = list(records)
    overrides = tuple(overrides or ())
    if not overrides:
        return tuple(records)
    if as_of_date is None:
        raise ValueError("security lifecycle overrides require a dated master snapshot")
    snapshot_date = _as_date(as_of_date)
    lifetime_ranges = {}
    status_ranges = {}
    status_decisions = {}
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
        name = str(item.get("name", "")).strip()
        evidence_refs = item.get("evidence_refs")
        if not name or not isinstance(evidence_refs, (list, tuple)) or not evidence_refs:
            raise ValueError("security lifecycle override requires a name and evidence references")
        if any(not isinstance(ref, str) or not ref.strip() for ref in evidence_refs):
            raise ValueError("security lifecycle override evidence reference is empty")

        if "status" in item:
            if "list_date" in item or "delist_date" in item:
                raise ValueError("status interval cannot be combined with listing-date bounds")
            status = str(item.get("status", "")).lower()
            effective_from = _as_date(item.get("effective_from"))
            effective_to = _as_date(item.get("effective_to"))
            if status not in {"active", "suspended"} or effective_from is None:
                raise ValueError("status override requires active/suspended status and effective_from")
            if effective_to is not None and effective_to < effective_from:
                raise ValueError("status override effective_to precedes effective_from")
            lifetime = lifetime_ranges.get(key)
            if lifetime is not None and (effective_from < (lifetime[0] or date.min)
                    or (lifetime[1] is not None and (effective_to is None or effective_to > lifetime[1]))):
                raise ValueError("status interval falls outside configured listing-date bounds")
            intervals = status_ranges.setdefault(key, [])
            for prior_from, prior_to in intervals:
                if effective_from <= (prior_to or date.max) and prior_from <= (effective_to or date.max):
                    raise ValueError("overlapping security status override intervals")
            intervals.append((effective_from, effective_to))
            if not (effective_from <= snapshot_date and (effective_to is None or snapshot_date <= effective_to)):
                continue
            index = positions.get(key)
            if index is None:
                # A status interval changes an existing security; it does not
                # create a security row or make a partial source snapshot fail.
                continue
            status_decisions[key] = (status, name)
            continue

        list_date = _as_date(item.get("list_date"))
        delist_date = _as_date(item.get("delist_date"))
        if list_date is None and delist_date is None:
            raise ValueError("security lifecycle override requires list_date, delist_date, or a status interval")
        if delist_date is not None and list_date is not None and delist_date < list_date:
            raise ValueError("security lifecycle delist_date precedes list_date")
        if key in lifetime_ranges:
            raise ValueError("duplicate security lifecycle override")
        for effective_from, effective_to in status_ranges.get(key, ()):
            if (list_date is not None and effective_from < list_date
                    or delist_date is not None and (effective_to is None or effective_to > delist_date)):
                raise ValueError("status interval falls outside configured listing-date bounds")
        lifetime_ranges[key] = (list_date, delist_date)

        index = positions.get(key)
        if index is not None:
            record = records[index]
            status = record.status
            if delist_date is not None and snapshot_date > delist_date:
                status = "delisted"
            elif list_date is not None and snapshot_date < list_date:
                status = "prelisted"
            records[index] = replace(record,
                list_date=list_date if list_date is not None else record.list_date,
                delist_date=delist_date if delist_date is not None else record.delist_date,
                status=status,
                name=record.name or name,
                evidence_sources=tuple(dict.fromkeys((*record.evidence_sources, "user_confirmed_lifecycle_override"))))
            continue
        if delist_date is None or snapshot_date <= delist_date:
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
    for key, (status, name) in status_decisions.items():
        index = positions[key]
        record = records[index]
        records[index] = replace(record, status=status, name=record.name or name,
            evidence_sources=tuple(dict.fromkeys((*record.evidence_sources, "user_confirmed_lifecycle_override"))))
    return tuple(sorted(records, key=lambda item: item.instrument_id))


def security_lifecycle_override_for(overrides, *, market, asset_type, symbol, as_of_date):
    """Return the one configured lifecycle decision that applies to a symbol/date."""
    from collections.abc import Mapping as MappingABC

    exchange = Exchange(str(market).upper())
    asset = AssetType(str(asset_type).lower())
    code = str(symbol)
    day = _as_date(as_of_date)
    if day is None:
        raise ValueError("security lifecycle lookup requires a date")
    matches = []
    lifetime_ranges = {}
    status_ranges = {}
    for item in tuple(overrides or ()):
        if not isinstance(item, MappingABC):
            raise ValueError("security lifecycle override must be a mapping")
        if (Exchange(str(item.get("market", "")).upper()) != exchange
                or AssetType(str(item.get("asset_type", "")).lower()) != asset
                or str(item.get("symbol", "")) != code):
            continue
        if "status" in item:
            if "list_date" in item or "delist_date" in item:
                raise ValueError("status interval cannot be combined with listing-date bounds")
            status = str(item.get("status", "")).lower()
            effective_from = _as_date(item.get("effective_from"))
            effective_to = _as_date(item.get("effective_to"))
            if status not in {"active", "suspended"} or effective_from is None:
                raise ValueError("status override requires active/suspended status and effective_from")
            if effective_to is not None and effective_to < effective_from:
                raise ValueError("status override effective_to precedes effective_from")
            lifetime = lifetime_ranges.get((exchange, asset, code))
            if lifetime is not None and (effective_from < (lifetime[0] or date.min)
                    or (lifetime[1] is not None and (effective_to is None or effective_to > lifetime[1]))):
                raise ValueError("status interval falls outside configured listing-date bounds")
            intervals = status_ranges.setdefault((exchange, asset, code), [])
            for prior_from, prior_to in intervals:
                if effective_from <= (prior_to or date.max) and prior_from <= (effective_to or date.max):
                    raise ValueError("overlapping security status override intervals")
            intervals.append((effective_from, effective_to))
            if effective_from <= day and (effective_to is None or day <= effective_to):
                matches.append({**item, "status": status, "effective_from": effective_from,
                    "effective_to": effective_to, "override_kind": "status"})
        else:
            list_date, delist_date = _as_date(item.get("list_date")), _as_date(item.get("delist_date"))
            if list_date is None and delist_date is None:
                raise ValueError("security lifecycle override requires list_date or delist_date")
            if delist_date is not None and list_date is not None and delist_date < list_date:
                raise ValueError("security lifecycle delist_date precedes list_date")
            key = (exchange, asset, code)
            if key in lifetime_ranges:
                raise ValueError("duplicate security lifecycle override")
            for effective_from, effective_to in status_ranges.get(key, ()):
                if (list_date is not None and effective_from < list_date
                        or delist_date is not None and (effective_to is None or effective_to > delist_date)):
                    raise ValueError("status interval falls outside configured listing-date bounds")
            lifetime_ranges[key] = (list_date, delist_date)
            if list_date is not None and day < list_date:
                matches.append({**item, "status": "prelisted", "list_date": list_date,
                    "delist_date": delist_date, "override_kind": "lifetime"})
            elif delist_date is not None and day > delist_date:
                matches.append({**item, "status": "delisted", "list_date": list_date,
                    "delist_date": delist_date, "override_kind": "lifetime"})
    if len(matches) > 1:
        raise ValueError("multiple security lifecycle overrides apply to one symbol/date")
    return matches[0] if matches else None


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


def prepare_security_publication(source_rows, previous=(), *, status_sources=(), symbol_successions=(),
                                 lifecycle_overrides=(), as_of_date=None):
    """Build from normalized source candidates with the existing identity and merge rules."""
    relations = normalize_symbol_successions(symbol_successions)

    def apply_succession(record):
        if as_of_date is None:
            return record
        relation = symbol_succession_for(relations, market=record.exchange.value,
            asset_type=record.asset_type.value, symbol=record.symbol, as_of_date=as_of_date)
        if relation is None:
            return record
        return replace(record,
            instrument_id=stable_instrument_id(record.exchange, str(relation["old_symbol"])),
            symbol=str(relation["new_symbol"]))

    mapped_previous = [apply_succession(record) for record in previous]
    if len({record.instrument_id for record in mapped_previous}) != len(mapped_previous):
        raise ValueError("previous security master contains duplicate rows after confirmed code succession")
    parsed = {}
    for source, rows in source_rows.items():
        records = [parse_security_row({
            **row, "code": row.get("symbol", row.get("stock_code", row.get("code"))),
            "name": row.get("name", row.get("stock_name")),
        }, source=source) for row in rows]
        parsed[source] = [apply_succession(record) for record in records]
    merged = merge_security_sources(mapped_previous, parsed,
        official_sources=frozenset({"exchange", "official_publisher", *status_sources}))
    return replace(merged, records=apply_security_lifecycle_overrides(merged.records,
        lifecycle_overrides, as_of_date=as_of_date))


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
