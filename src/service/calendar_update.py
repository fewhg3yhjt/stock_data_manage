from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
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


class TradingCalendarUpdater:
    def __init__(self, store: TradingCalendarStore, providers: list[CalendarProvider]) -> None:
        self.store = store
        self.providers = tuple(providers)

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

    def update_from_input_report(self, report_path: Path, *, live_report_path: Path,
                                 start: date, end: date, now: datetime,
                                 max_source_age_days: int = 30) -> CalendarUpdateResult:
        """Import revalidated positive dates; retain the original live provenance."""
        import json
        import inspect
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
        old_dependency=original["sdk_dependency"]
        old_source_hash=old_dependency.get("original_source_sha256")
        if old_source_hash is None and not old_dependency.get("redacted"):
            # Earlier reports used sha256 for the unredacted SDK source bytes.
            old_source_hash=old_dependency["sha256"]
        if original["adapter_version"] != current["adapter_version"] or old_source_hash != current["sdk_dependency"]["original_source_sha256"]:
            raise ValueError("calendar source adapter or SDK parsing contract changed")
        adapter=SinaTradingCalendarProvider()
        adapter_path=Path(inspect.getfile(type(adapter))).resolve()
        if current["adapter_version"] != adapter.capability_version or not any(
                str(d["path"]).replace("\\","/").endswith("/providers/sina/calendar.py")
                and d["sha256"]==file_hash(adapter_path) for d in original["code_files"]):
            raise ValueError("calendar original live adapter code differs")
        for descriptor in (*current["code_files"],*current["config_files"]):
            if file_hash(Path(descriptor["path"])) != descriptor["sha256"]:
                raise ValueError("calendar revalidation code or configuration changed")
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
