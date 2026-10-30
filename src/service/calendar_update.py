from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Protocol

from .calendar import CalendarDay, CalendarMergeResult, TradingCalendarStore, merge_calendar_sources


class CalendarProvider(Protocol):
    name: str
    priority: int

    def fetch_calendar(self, start: date, end: date) -> list[CalendarDay]: ...


@dataclass(frozen=True, slots=True)
class CalendarUpdateResult:
    merge: CalendarMergeResult
    source_errors: dict[str, str]
    source_manifest: Path | None = None


class TradingCalendarUpdater:
    def __init__(self, store: TradingCalendarStore, providers: list[CalendarProvider], *, evidence_root: Path | None = None) -> None:
        self.store = store
        self.providers = tuple(providers)
        self.evidence_root = Path(evidence_root) if evidence_root is not None else Path("data/task_workspace/trading_calendar")

    def update(self, *, start: date, end: date) -> CalendarUpdateResult:
        sources: dict[str, list[CalendarDay]] = {}
        errors: dict[str, str] = {}
        for provider in self.providers:
            try:
                sources[provider.name] = [
                    CalendarDay(day.trade_date, day.is_trading_day, provider.name, provider.priority, day.evidence)
                    for day in provider.fetch_calendar(start, end)
                ]
            except Exception as exc:
                sources[provider.name] = []
                errors[provider.name] = str(exc)
        previous = tuple(day for day in self.store.all() if start <= day.trade_date <= end)
        merged = merge_calendar_sources(sources, previous=previous)
        self.store.upsert(merged.days)
        return CalendarUpdateResult(merged, errors)

    def update_baostock_day(self, *, trade_date: date, provider, evidence_root: Path | None = None) -> CalendarUpdateResult:
        """Store one explicit open/closed answer without a catalog publication pipeline."""
        import json
        evidence_root = Path(evidence_root) if evidence_root is not None else self.evidence_root
        fetched_at = datetime.now(timezone.utc)
        run_root = (evidence_root / trade_date.isoformat() / "baostock" / "query_trade_dates"
                    / ("refresh-" + fetched_at.strftime("%H%M%S%f")))
        run_root.mkdir(parents=True, exist_ok=True)
        from ..providers.baostock.session import logged_in_session
        from ..providers.baostock.industry import _archive_sdk_rows, _read_success
        from ..storage.raw import RawObjectStore
        raw = RawObjectStore(run_root)

        class CalendarArchive:
            def __init__(self, store):
                self.store = store

            def store_source_payload(self, payload, *, provider, endpoint, representation, metadata):
                now = datetime.now(timezone.utc)
                decoded = json.loads(payload)
                sdk_status = decoded.get("result", {}).get("error_code", "0")
                outcome = metadata.get("outcome")
                if outcome == "success":
                    classification = "response"
                elif isinstance(outcome, dict) and outcome.get("outcome") == "empty":
                    classification = "empty_response"
                else:
                    classification = "sdk_error"
                ref = self.store.write_bytes(payload, dataset="calendar_response", provider=provider,
                    endpoint=endpoint, fetched_at=now, attempt_id="query-trade-dates", suffix="json",
                    relative_path="response.json")
                self.store.append_event({"event": "source_payload", "provider": provider, "endpoint": endpoint,
                    "method": endpoint, "request_parameters": {"start_date": trade_date.isoformat(),
                    "end_date": trade_date.isoformat()}, "fetched_at_utc": now.isoformat(),
                    "sdk_status_code": str(sdk_status),
                    "outcome": classification, "metadata": metadata,
                    "body_sha256": ref.content_hash, "body_storage": ref.path.relative_to(self.store.root).as_posix(),
                    "representation": representation, "original_transport_bytes_available": False})

        with logged_in_session(client=provider.client) as client:
            result = client.query_trade_dates(start_date=trade_date.isoformat(), end_date=trade_date.isoformat())
            rows = _read_success(result, "query_trade_dates", CalendarArchive(raw), trade_date)
            _archive_sdk_rows(CalendarArchive(raw), "query_trade_dates", result, rows, trade_date)
        if (len(rows) != 1 or rows[0].get("calendar_date") != trade_date.isoformat()
                or rows[0].get("is_trading_day") not in {"0", "1"}):
            raise ValueError("calendar source returned an invalid date or status")
        row = {"trade_date": trade_date, "is_trading_day": rows[0]["is_trading_day"] == "1"}
        manifest_hash = hashlib.sha256(raw.manifest.read_bytes()).hexdigest()
        response_hash = json.loads(raw.manifest.read_text(encoding="utf-8").splitlines()[-1])["body_sha256"]
        evidence_value = {"source": "baostock", "request": f"query_trade_dates({trade_date})",
            "fetched_at_utc": fetched_at.isoformat(), "manifest": str(raw.manifest.relative_to(evidence_root)),
            "manifest_sha256": manifest_hash, "response_sha256": response_hash}
        day = CalendarDay(trade_date, row["is_trading_day"], "baostock", 20,
                          json.dumps(evidence_value, sort_keys=True, separators=(",", ":")))
        previous = tuple(item for item in self.store.all() if item.trade_date == trade_date)
        merged = merge_calendar_sources({"baostock": [day]}, previous=previous)
        self.store.upsert(merged.days)
        return CalendarUpdateResult(merged, {}, raw.manifest)

    def update_from_input_report(self, report_path: Path, *, live_report_path: Path,
                                 start: date, end: date, now: datetime,
                                 max_source_age_days: int = 30) -> CalendarUpdateResult:
        """Import revalidated positive dates; retain the original live provenance."""
        import json
        from collections import Counter
        from ..providers.sina.calendar import SinaTradingCalendarProvider
        from ..storage.integrity import file_hash
        from ..storage.raw import RawObjectStore
        if end < start or now.tzinfo is None or max_source_age_days < 1:
            raise ValueError("invalid calendar import window or source age")

        def load(path):
            document=json.loads(Path(path).read_text(encoding="utf-8"))
            if (document.get("input_id"),document.get("dataset"),document.get("provider"),document.get("endpoint")) != (
                    "ASTOCK-070","trading_calendar","sina","trading_calendar") or document.get("parameters") != {}:
                raise ValueError("calendar import requires the verified full positive-date input")
            if document.get("status") != "candidate_complete":
                raise ValueError("calendar source report is incomplete")
            if document.get("run_directory"):
                base=Path(document["run_directory"]).resolve()
            else:
                saved=Path(path).resolve()
                if tuple(p.name for p in saved.parents[:3]) != ("trading_calendar","sina","input_report"):
                    raise ValueError("calendar report location does not identify its source run")
                date.fromisoformat(saved.parents[3].name)
                base=saved.parents[4]
            manifest=(base/document["raw_manifest"]["path"]).resolve()
            if not manifest.is_relative_to(base) or RawObjectStore.verify_manifest(manifest) != document["raw_manifest"]["sha256"]:
                raise ValueError("calendar raw evidence differs")
            events=[json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
            return document,base,manifest,events

        current,base,raw,events=load(report_path)
        original,_,original_raw,original_events=load(live_report_path)
        if current.get("mode") not in {"live","replay"}:
            raise ValueError("calendar revalidation mode is invalid")
        if original.get("mode") != "live" or not original.get("live_http_calls"):
            raise ValueError("calendar import has no original live source evidence")
        fetched=[datetime.fromisoformat(e["fetched_at_utc"]) for e in original_events
                 if e.get("mode")=="live" and e.get("outcome")=="response" and e.get("status_code")==200]
        if not fetched or any(t.tzinfo is None or not timedelta(0)<=now-t<=timedelta(days=max_source_age_days) for t in fetched):
            raise ValueError("calendar source evidence is expired or future-dated")
        adapter=SinaTradingCalendarProvider()
        if original["adapter_version"] != current["adapter_version"] or current["adapter_version"] != adapter.capability_version:
            raise ValueError("calendar source adapter contract changed")
        for descriptor in current["config_files"]:
            if file_hash(Path(descriptor["path"])) != descriptor["sha256"]:
                raise ValueError("calendar revalidation configuration changed")
        original_hashes=Counter(e["body_sha256"] for e in original_events if e.get("body_sha256"))
        current_hashes=Counter(e["body_sha256"] for e in events if e.get("body_sha256"))
        if not original_hashes or original_hashes != current_hashes:
            raise ValueError("calendar revalidation does not use the original source bytes")
        output=(base/current["output"]["path"]).resolve()
        if not output.is_relative_to(base) or file_hash(output) != current["output"]["sha256"]:
            raise ValueError("calendar derived dates differ")
        if Counter(current["output"].get("source_response_hashes",())) != original_hashes:
            raise ValueError("calendar derived dates do not reference the source")
        rows=json.loads(output.read_text(encoding="utf-8"))
        if not rows or len(rows) != current["row_count"] or len(rows) != current["output"]["row_count"]:
            raise ValueError("calendar date count differs")
        days=[date.fromisoformat(row["trade_date"]) for row in rows]
        if len(set(days)) != len(days) or any(row["source"] != "sina" for row in rows):
            raise ValueError("calendar dates are duplicated or source labels differ")
        evidence=json.dumps({"live_report":str(Path(live_report_path).resolve()),"live_report_sha256":file_hash(Path(live_report_path)),
            "revalidation_report":str(Path(report_path).resolve()),"revalidation_sha256":file_hash(Path(report_path)),
            "original_raw":str(original_raw),"raw":str(raw),"raw_sha256":current["raw_manifest"]["sha256"],
            "source_meaning":"Sina/SDK positive trading dates; missing dates remain unknown"},ensure_ascii=False)
        selected=[CalendarDay(day,True,"sina",10,evidence) for day in days if start<=day<=end]
        if not selected:
            raise ValueError("calendar source has no positive dates in the requested window")
        previous=tuple(day for day in self.store.all() if start<=day.trade_date<=end)
        merged=merge_calendar_sources({"sina":selected},previous=previous)
        self.store.upsert(merged.days)
        return CalendarUpdateResult(merged,{})
