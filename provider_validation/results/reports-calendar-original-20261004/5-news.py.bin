from datetime import datetime, timedelta, timezone
import re

import requests

from ..contracts import InputFetchResult


class WallStreetCNNewsProvider:
    """Preserve the successful V3.9 flash-news request and parser contract."""
    capability_version = "wallstreetcn-news-input-v1"
    url = "https://api-one-wscn.awtmt.com/apiv1/content/lives"

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
