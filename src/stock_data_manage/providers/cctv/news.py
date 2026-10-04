from datetime import datetime
import html
import re

import requests

from ..contracts import InputFetchResult


class CCTVNewsProvider:
    """Verified day-page titles and links; article content remains unverified."""
    capability_version = "cctv-news-index-input-v1"

    def fetch(self, *, date, with_content=False):
        if with_content is not False:
            raise ValueError("article content is not backed by archived source responses")
        text_date = date.isoformat() if hasattr(date, "isoformat") else str(date).strip()
        day = datetime.strptime(text_date, "%Y%m%d" if re.fullmatch(r"[0-9]{8}", text_date) else "%Y-%m-%d").date()
        url = f"https://tv.cctv.com/lm/xwlb/day/{day:%Y%m%d}.shtml"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"}
        try:
            response = requests.request("GET", url, params=None, data=None, headers=headers,
                                        timeout=(10, 40), allow_redirects=True)
            if response.status_code != 404:
                response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"CCTV transport failed: {type(exc).__name__}") from exc
        if response.status_code == 404:
            raise ValueError("CCTV day page has not been published or is unavailable")
        text = response.content.decode("utf-8", "replace")
        rows = []
        for chunk in text.split("<li")[1:]:
            link = re.search(r'href="([^"]*/VIDE[^"]+)"', chunk)
            if not link:
                continue
            href = link.group(1)
            title = (re.search(r'title="([^"]+)"', chunk) or re.search(r'class="title">(.*?)</div>', chunk, re.S)
                     or re.search(r"<a[^>]*>(.*?)</a>", chunk, re.S))
            title = html.unescape(re.sub(r"<[^>]+>", "", title.group(1))).strip() if title else ""
            if not title or re.match(r"《新闻联播》\s*\d{8}|新闻联播完整版", title):
                continue
            title = re.sub(r"^\[视频\]", "", title).strip()
            rows.append({"date": day.isoformat(), "title": title, "url": ("https:"+href) if href.startswith("//") else href})
        if not rows:
            raise RuntimeError("CCTV day-page structure changed or contains no news items")
        return InputFetchResult(tuple(rows), source_url=url,
            mapping_context={"broadcast_date": day.isoformat(), "with_content": False})
