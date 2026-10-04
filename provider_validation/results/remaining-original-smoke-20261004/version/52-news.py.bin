from datetime import datetime, timedelta, timezone
import re

import requests

from ..contracts import InputFetchResult


class WallStreetCNNewsProvider:
    """Preserve the successful V3.9 flash-news request and parser contract."""
    capability_version = "wallstreetcn-news-input-v1"
    url = "https://api-one-wscn.awtmt.com/apiv1/content/lives"

    def fetch_macro_calendar(self, *, start, end, country=None, min_importance=1):
        """Original seven-day slices, source checks and local country/importance filter."""
        from datetime import date
        def day(value):
            if isinstance(value,datetime):return value.date()
            if isinstance(value,date):return value
            value=str(value).strip()
            return datetime.strptime(value,"%Y%m%d" if re.fullmatch(r"[0-9]{8}",value) else "%Y-%m-%d").date()
        first,last=day(start),day(end)
        if first>last or (last-first).days>91:raise ValueError("calendar window must be within 92 days")
        if isinstance(min_importance,bool) or str(min_importance) not in ("1","2","3","4"):
            raise ValueError("importance must be between 1 and 4")
        zone=timezone(timedelta(hours=8));today=datetime.now(zone).date();cursor=first
        source_rows=[];by_id={};url="https://api-one-wscn.awtmt.com/apiv1/finance/macrodatas"
        headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"}
        blank=lambda value:None if value is None or value=="" else value
        while cursor<=last:
            stop=min(cursor+timedelta(days=6),last)
            params={"start":int(datetime(cursor.year,cursor.month,cursor.day,tzinfo=zone).timestamp()),
                    "end":int(datetime(stop.year,stop.month,stop.day,23,59,59,tzinfo=zone).timestamp())}
            try:
                response=requests.request("GET",url,params=params,data=None,headers=headers,timeout=(10,40),allow_redirects=True)
                response.raise_for_status();payload=response.json()
            except (requests.RequestException,ValueError) as exc:raise RuntimeError(f"calendar request failed: {type(exc).__name__}") from exc
            if not isinstance(payload,dict) or payload.get("code")!=20000:raise RuntimeError("calendar business response changed")
            items=payload.get("data").get("items") if isinstance(payload.get("data"),dict) else None
            if items is None:items=[]
            if not isinstance(items,list) or not all(isinstance(row,dict) for row in items):raise RuntimeError("calendar item structure changed")
            if not items and stop-cursor==timedelta(days=6) and cursor<=today:raise RuntimeError("started full week cannot be empty")
            for item in items:
                stamp=item.get("public_date");level=item.get("importance")
                if "id" not in item or isinstance(stamp,bool) or not isinstance(stamp,(int,float)):
                    raise RuntimeError("calendar item id or timestamp changed")
                try:when=datetime.fromtimestamp(stamp,zone)
                except (ValueError,OverflowError,OSError) as exc:raise RuntimeError("calendar timestamp invalid") from exc
                if not cursor<=when.date()<=stop:raise RuntimeError("calendar item outside requested slice")
                if isinstance(level,bool) or not isinstance(level,int) or level not in (1,2,3,4):raise RuntimeError("calendar importance changed")
                if item["id"] in by_id:raise RuntimeError("duplicate calendar event id")
                source_rows.append(item)
                by_id[item["id"]]={"source_event_id":str(item["id"]),"time":when.strftime("%Y-%m-%d %H:%M"),
                    "country":item.get("country"),"title":item.get("title"),
                    "kind":{"FD":"data","FE":"event"}.get(item.get("calendar_type"),item.get("calendar_type")),
                    "importance":level,"actual":blank(item.get("actual")),"forecast":blank(item.get("forecast")),
                    "previous":blank(item.get("previous")),"revised":blank(item.get("revised")),"unit":item.get("unit") or None,"period":item.get("period") or None}
            cursor=stop+timedelta(days=1)
        if not by_id:raise ValueError("calendar window has no source events")
        rows=sorted(by_id.values(),key=lambda row:row["time"])
        selected=tuple(row for row in rows if (not country or row["country"]==country) and row["importance"]>=int(min_importance))
        if not selected:raise ValueError("calendar local filters returned no events")
        selected_ids={row["source_event_id"] for row in selected}
        return InputFetchResult(selected,source_rows=tuple(source_rows),source_url=response.url,
            excluded_rows=tuple(row for row in source_rows if str(row["id"]) not in selected_ids),
            mapping_context={"requested_start":first.isoformat(),"requested_end":last.isoformat(),"country":country,"min_importance":int(min_importance)})

    def fetch(self, *, channel="global-channel", limit=50, cursor=None):
        if not re.fullmatch(r"[a-z0-9-]+-channel", str(channel)):
            raise ValueError("invalid news channel")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer between 1 and 100")
        params = {"channel": channel, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"}
        try:
            response = requests.request("GET", self.url, params=params, data=None, headers=headers,
                                        timeout=(10, 40), allow_redirects=True)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"news transport failed: {type(exc).__name__}") from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("news source returned non-JSON data") from exc
        if not isinstance(payload, dict) or payload.get("code") != 20000 or not isinstance(payload.get("data"), dict):
            raise RuntimeError("news source business response changed")
        items = payload["data"].get("items")
        if not isinstance(items, list) or not items or not all(isinstance(item, dict) for item in items):
            raise RuntimeError("news items must be a nonempty object list")
        rows = []
        try:
            for item in items:
                stamp = item["display_time"]
                if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
                    raise TypeError("invalid display_time")
                channels = item.get("channels")
                if channels is None:
                    channels = []
                if not isinstance(channels, list) or not all(isinstance(value, str) for value in channels):
                    raise TypeError("invalid channels")
                rows.append({"id": item["id"], "time": datetime.fromtimestamp(stamp, timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
                    "title": item.get("title") or "", "content": (item.get("content_text") or "").strip(),
                    "importance": item.get("score"), "channels": ",".join(channels), "url": item.get("uri") or ""})
        except (KeyError, TypeError, AttributeError, ValueError, OverflowError, OSError) as exc:
            raise RuntimeError(f"news item format changed: {type(exc).__name__}") from exc
        return InputFetchResult(tuple(rows), source_rows=tuple(items), source_url=response.url,
            mapping_context={"next_cursor": payload["data"].get("next_cursor"), "requested_limit": limit, "channel": channel})
