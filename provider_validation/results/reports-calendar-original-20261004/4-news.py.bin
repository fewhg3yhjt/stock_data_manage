from datetime import datetime

from ..akshare.session import load_client
from ..contracts import InputFetchResult


class SinaGlobalNewsProvider:
    """The successful source is Sina; no unverified EastMoney fallback."""
    capability_version = "sina-global-news-input-v1"
    input_hosts = ("https://zhibo.sina.com.cn",)

    def __init__(self, client=None):
        self.client = client

    def fetch(self, *, source_payloads):
        self.client = self.client or load_client()
        frame = self.client.stock_info_global_sina()
        payloads = list(source_payloads())
        if len(payloads) != 1:
            raise RuntimeError("one successful global-news response is required")
        payload = payloads[0]
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict) or not isinstance(result.get("status"), dict) or result["status"].get("code") != 0:
            raise RuntimeError("global-news business response changed")
        data = result.get("data")
        feed = data.get("feed") if isinstance(data, dict) else None
        items = feed.get("list") if isinstance(feed, dict) else None
        if not isinstance(items, list) or not items or not all(isinstance(item, dict) for item in items):
            raise RuntimeError("global-news response must contain nonempty source items")
        if len(items) > 20 or len(items) != len(frame) or set(frame.columns) != {"时间", "内容"}:
            raise RuntimeError("global-news SDK fields or count changed")
        source_rows = tuple(frame.to_dict(orient="records"))
        for row in source_rows:
            if not isinstance(row["内容"], str) or not row["内容"] or not isinstance(row["时间"], str):
                raise RuntimeError("global-news text fields changed")
            datetime.strptime(row["时间"], "%Y-%m-%d %H:%M:%S")
        return InputFetchResult(source_rows, source_rows=source_rows,
            source_url="https://zhibo.sina.com.cn/api/zhibo/feed", mapping_context={"requested_limit": 20})
