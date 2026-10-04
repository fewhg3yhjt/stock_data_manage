from datetime import datetime
import math
import re

import requests

from ..contracts import InputFetchResult


class ChinamoneyRepoRateProvider:
    """Original fixing CSV transport, field checks, date sorting and uniqueness."""
    capability_version = "chinamoney-repo-input-v1"

    def fetch(self, *, kind="FR"):
        kind = str(kind).upper()
        if kind not in {"FR", "FDR"}:
            raise ValueError("kind must be FR or FDR")
        name = "frr" if kind == "FR" else "fdr"
        url = f"https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/{name}-chrt.csv"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                   "Referer": "https://www.chinamoney.com.cn/chinese/bkfrr/"}
        try:
            response = requests.request("GET", url, params=None, data=None, headers=headers,
                                        timeout=(10, 40), allow_redirects=True)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"fixing transport failed: {type(exc).__name__}") from exc
        rows, source = [], []
        try:
            for line in response.content.decode("utf-8-sig").splitlines():
                if not line.strip():
                    continue
                parts = line.split(",")
                if len(parts) != 9 or any(part.strip() for part in parts[1:6]):
                    raise RuntimeError("fixing CSV nine-column layout changed")
                day = datetime.strptime(parts[0].strip(), "%Y%m%d" if re.fullmatch(r"[0-9]{8}", parts[0].strip()) else "%Y-%m-%d").date().isoformat()
                row = {"date": day}
                original = {"date": parts[0]}
                for tenor, value in zip(("001", "007", "014"), parts[6:9]):
                    number = float(value.replace(",", "").strip())
                    if not math.isfinite(number):
                        raise RuntimeError("required fixing rate is not finite")
                    row[kind+tenor] = number
                    original[kind+tenor] = value
                rows.append(row)
                source.append(original)
        except (ValueError, UnicodeError) as exc:
            raise RuntimeError("fixing source date or required rate changed") from exc
        if not rows or len({row["date"] for row in rows}) != len(rows):
            raise RuntimeError("fixing CSV is empty or contains duplicate dates")
        rows.sort(key=lambda row: row["date"])
        return InputFetchResult(tuple(rows), source_url=url, source_rows=tuple(source),
            mapping_context={"rate_kind": kind})
