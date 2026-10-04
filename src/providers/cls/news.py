from contextlib import contextmanager
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from ..akshare.session import load_client
from ..contracts import InputFetchResult


class CLSTelegraphProvider:
    """Keep the observed AkShare request, signature, retries and Beijing-time parser."""
    capability_version = "cls-telegraph-input-v1"
    input_hosts = ("https://www.cls.cn",)

    def __init__(self, client=None):
        self.client = client

    def fetch(self, *, source_payloads):
        self.client = self.client or load_client()
        frame = self.client.stock_info_global_cls()
        payloads = list(source_payloads())
        if len(payloads) != 1:
            raise RuntimeError("one successful telegraph response is required")
        payload = payloads[0]
        if not isinstance(payload, dict) or payload.get("errno") != 0 or not isinstance(payload.get("data"), dict):
            raise RuntimeError("telegraph business response changed")
        items = payload["data"].get("roll_data")
        if not isinstance(items, list) or not items or not all(isinstance(item, dict) for item in items):
            raise RuntimeError("telegraph response must contain nonempty source items")
        if len(items) > 20 or len(items) != len(frame) or set(frame.columns) != {"标题", "内容", "发布日期", "发布时间"}:
            raise RuntimeError("telegraph SDK fields or count changed")
        source_rows = tuple(frame.to_dict(orient="records"))
        rows = []
        for row in source_rows:
            if not isinstance(row["内容"], str) or not row["内容"] or not isinstance(row["标题"], str):
                raise RuntimeError("telegraph text fields changed")
            rows.append({**row, "published_at": datetime.combine(row["发布日期"], row["发布时间"]).isoformat()})
        return InputFetchResult(tuple(rows), source_rows=source_rows,
            source_url="https://www.cls.cn/v1/roll/get_roll_list", mapping_context={"requested_limit": 20})


@contextmanager
def telegraph_replay_clock(function, manifest):
    """Replay the archived query cutoff without changing live or process-wide clocks."""
    matches = []
    for line in Path(manifest).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        url = urlsplit(record.get("url", ""))
        if record.get("event") == "http_response" and url.hostname == "www.cls.cn" and url.path == "/v1/roll/get_roll_list":
            matches.append(parse_qs(url.query))
    if not matches or not matches[0].get("last_time"):
        raise ValueError("no original telegraph query cutoff; replay never falls back to network")
    cutoff = int(matches[0]["last_time"][0])
    namespace = function.__globals__
    clock = namespace["time"]
    with patch.dict(namespace, {"time": SimpleNamespace(time=lambda: cutoff, sleep=clock.sleep)}):
        yield cutoff
