from __future__ import annotations

from dataclasses import dataclass
from contextlib import closing
from datetime import date, datetime, timedelta
from decimal import InvalidOperation
import re
import sqlite3

from ..domain import Adjustment, BarRecord, Dataset
from ..quality.resolution import Conflict, resolve_records
from ..storage.hot import HotMinuteStore
from ..storage.parquet import CanonicalPartitionStore


@dataclass(frozen=True, slots=True)
class MinuteQueryResult:
    records: tuple[BarRecord, ...]
    conflicts: tuple[Conflict, ...]


class MinuteQuery:
    def __init__(self, *, canonical: CanonicalPartitionStore, hot: HotMinuteStore) -> None:
        self.canonical = canonical
        self.hot = hot

    def query(
        self,
        *,
        instrument_id: str,
        asset_type: str,
        start: datetime,
        end: datetime,
        interval_minutes: int = 1,
        adjustment: Adjustment = Adjustment.NONE,
    ) -> MinuteQueryResult:
        records: list[BarRecord] = []
        current = start.date()
        while current <= end.date():
            records.extend(
                record
                for record in self.canonical.read(
                    Dataset.MINUTE_BAR_1M if interval_minutes == 1 else Dataset.MINUTE_BAR_5M,
                    asset_type,
                    current.isoformat(),
                )
                if record.instrument_id == instrument_id
                and record.bar_time is not None
                and start <= record.bar_time <= end
                and record.interval_minutes == interval_minutes
                and record.adjustment == adjustment
            )
            current += timedelta(days=1)
        records.extend(
            self.hot.query(
                instrument_id,
                start,
                end,
                interval_minutes=interval_minutes,
                adjustment=adjustment,
            )
        )
        grouped: dict[tuple[object, ...], list[BarRecord]] = {}
        for record in records:
            grouped.setdefault(record.key, []).append(record)
        selected: list[BarRecord] = []
        conflicts: list[Conflict] = []
        for candidates in grouped.values():
            result = resolve_records(candidates)
            conflicts.extend(result.conflicts)
            if result.selected is not None:
                selected.append(result.selected)
        return MinuteQueryResult(tuple(sorted(selected, key=lambda record: record.key)), tuple(conflicts))


class DailyQueryNotReady(RuntimeError):
    """The configured SQLite database is absent, legacy, or lacks the daily schema."""


class DailyQueryBusy(RuntimeError):
    """SQLite could not provide a short read because the database was busy."""


class DailyQueryIntegrityError(RuntimeError):
    """Stored daily data could not be decoded according to its business contract."""


class DailyQueryInputError(ValueError):
    """A daily query parameter is invalid."""


def _daily_identity_scope(security, *, asset_type, start_date, end_date, relations):
    if not security:
        return None
    text = str(security).strip().lower()
    match = re.fullmatch(r"(?:(xshg|xshe|bse|sh|sz|bj)[.:]?)?(\d{6})", text)
    if not match:
        raise DailyQueryInputError("invalid daily security code")
    market_alias = {"xshg": "XSHG", "sh": "XSHG", "xshe": "XSHE", "sz": "XSHE",
                    "bse": "BSE", "bj": "BSE"}
    requested_market = market_alias.get(match[1]) if match[1] else None
    code = match[2]
    normalized = relations
    markets = (requested_market,) if requested_market else ("XSHG", "XSHE", "BSE")
    ranges = []
    for market in markets:
        ranges.append((f"{market}:{code}", start_date, end_date, None))
        for asset in sorted({str(item["asset_type"]) for item in normalized
                             if item["market"] == market
                             and (not asset_type or item["asset_type"] == asset_type)}):
            edges = [item for item in normalized if item["market"] == market and item["asset_type"] == asset]
            graph = {}
            for item in edges:
                effective = date.fromisoformat(str(item["effective_from"]))
                old, new = str(item["old_symbol"]), str(item["new_symbol"])
                graph.setdefault(old, []).append((new, effective))
                graph.setdefault(new, []).append((old, effective))
            pending = [(code, None)]
            earliest = {code: None}
            while pending:
                current, threshold = pending.pop()
                for neighbor, effective in graph.get(current, ()):
                    next_threshold = max(threshold, effective) if threshold else effective
                    prior = earliest.get(neighbor)
                    if neighbor not in earliest or (prior is not None and next_threshold < prior):
                        earliest[neighbor] = next_threshold
                        pending.append((neighbor, next_threshold))
            for related_code, effective in earliest.items():
                if related_code == code or effective is None:
                    continue
                first = max(start_date, effective) if start_date else effective
                if end_date is None or first <= end_date:
                    ranges.append((f"{market}:{related_code}", first, end_date, asset))
    deduped = tuple(dict.fromkeys(ranges))
    return tuple((identity, first, last, scoped_asset) for identity, first, last, scoped_asset in deduped)


def query_daily(metadata_path, *, security="", start_date=None, end_date=None, asset_type="", adjustment="none", page=1, page_size=50,
                symbol_successions=()):
    """Project current published daily bars from the configured read-only SQLite runtime."""
    from ..domain import AssetType
    from ..storage.daily import DailyBarStore
    from ..storage.metadata import open_runtime_database
    from .instruments import normalize_symbol_successions, symbol_succession_for

    if adjustment not in {a.value for a in Adjustment} or asset_type not in {"", *(a.value for a in AssetType)}:
        raise DailyQueryInputError("unsupported daily asset or adjustment")
    if (page < 1 or not 1 <= page_size <= 100
            or (start_date is not None and type(start_date) is not date)
            or (end_date is not None and type(end_date) is not date)
            or (start_date and end_date and start_date > end_date)):
        raise DailyQueryInputError("invalid daily page or date range")
    try:
        relations = normalize_symbol_successions(symbol_successions)
    except ValueError as exc:
        raise DailyQueryIntegrityError("configured daily symbol succession is invalid") from exc
    scope = _daily_identity_scope(security, asset_type=asset_type, start_date=start_date,
        end_date=end_date, relations=relations)
    def is_busy(exc):
        code = getattr(exc, "sqlite_errorcode", None)
        if code is not None and (int(code) & 0xFF) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
            return True
        return "locked" in str(exc).lower() or "busy" in str(exc).lower()

    try:
        connection = open_runtime_database(metadata_path, read_only=True, purpose="daily")
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        raise DailyQueryNotReady("configured SQLite runtime database schema is not ready") from exc
    except sqlite3.Error as exc:
        if is_busy(exc):
            raise DailyQueryBusy("configured SQLite runtime database is busy") from exc
        raise DailyQueryNotReady("configured SQLite runtime database cannot be opened") from exc
    try:
        with closing(connection):
            required = {
                "daily_bar": {"instrument_id", "trade_date", "asset_type", "last_publication_id"},
                "daily_quote": {"instrument_id", "trade_date", "adjustment", "status", "open_text",
                    "high_text", "low_text", "close_text", "volume_text", "amount_text", "source_provider",
                    "endpoint", "raw_object_path", "fetch_time", "normalizer_version", "capability_version",
                    "reason", "evidence_ref", "flags_json", "value_contract"},
            }
            for table, expected in required.items():
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", [table]).fetchone()
                columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")} if exists else set()
                if not expected <= columns:
                    raise DailyQueryNotReady(f"daily table schema is not ready: {table}")
            identity_ranges = scope
            try:
                result = DailyBarStore(connection).query_page(
                    asset_type=AssetType(asset_type) if asset_type else None,
                    date_from=start_date, date_to=end_date, identity_date_ranges=identity_ranges,
                    adjustment=adjustment, limit=page_size, offset=(page-1)*page_size)
            except (ValueError, RuntimeError, InvalidOperation, KeyError, TypeError) as exc:
                raise DailyQueryIntegrityError("published daily rows failed integrity checks") from exc
    except DailyQueryNotReady:
        raise
    except sqlite3.OperationalError as exc:
        if is_busy(exc):
            raise DailyQueryBusy("configured SQLite runtime database is busy") from exc
        if "no such table" in str(exc).lower() or "no such column" in str(exc).lower():
            raise DailyQueryNotReady("configured SQLite runtime database schema is not ready") from exc
        raise DailyQueryIntegrityError("configured SQLite runtime database could not read daily data") from exc
    except sqlite3.DatabaseError as exc:
        raise DailyQueryIntegrityError("configured SQLite runtime database is damaged") from exc

    items = []
    try:
        for record in result.records:
            code = record.instrument_id.split(":", 1)[1]
            market = record.instrument_id.split(":", 1)[0]
            prefix = {"XSHG": "sh", "XSHE": "sz", "BSE": "bj"}[market]
            relation = symbol_succession_for(relations, market=market, asset_type=record.asset_type.value,
                symbol=code, as_of_date=record.trade_date)
            relation_detail = None
            symbol = prefix + code
            if relation:
                old_symbol = prefix + str(relation["old_symbol"])
                current_symbol = prefix + str(relation["new_symbol"])
                symbol = current_symbol
                relation_detail = {**relation, "old_symbol": old_symbol, "new_symbol": current_symbol}
            quote = record.quotes[adjustment]
            items.append({"instrument_id": record.instrument_id, "symbol": symbol,
                "symbol_relation": relation_detail, "trade_date": record.trade_date.isoformat(),
                "asset_type": record.asset_type.value, "value_kind": record.value_kind,
                "quote_unit": record.quote_unit, "adjustment": adjustment,
                "status": quote.status.value, "value_contract": quote.value_contract, "reason": quote.reason,
                "values": {name: str(getattr(quote, name)) if getattr(quote, name) is not None else None
                    for name in ("open", "high", "low", "close", "volume", "amount")},
                "provider": quote.source_provider, "has_source_evidence": bool(quote.raw_object_path),
                "flags": list(quote.flags),
                "quote_states": {name: group.status.value for name, group in record.quotes.items()}})
    except (InvalidOperation, KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise DailyQueryIntegrityError("published daily rows failed integrity checks") from exc
    total = result.total
    return {"items": items, "total": total, "page": page, "page_size": page_size,
        "pages": max(1, (total+page_size-1)//page_size), "adjustment": adjustment,
        "available_adjustments": [a.value for a in Adjustment if a.value in result.available_adjustments]}
