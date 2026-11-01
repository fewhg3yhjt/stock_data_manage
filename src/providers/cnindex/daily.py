"""国证指数日线接口。采集响应与解析响应严格分离。"""
from datetime import date
import json
import math
import re

from ..contracts import FailureClass, InputFetchResult, ProviderContractError


class CnindexDailyProvider:
    """Capture and parse one exact CNIndex daily index response."""

    name = "cnindex"
    endpoint = "index-daily"
    capability_version = "cnindex-index-daily-v1"
    input_hosts = ("http://hq.cnindex.com.cn",)
    url = "http://hq.cnindex.com.cn/market/market/getIndexDailyDataWithDataFormat"
    expected_fields = (
        "timestamp", "current", "high", "open", "low", "close", "chg",
        "percent", "amount", "volume", "avg",
    )

    def __init__(self, transport):
        self.transport = transport

    def capture_window(self, code, start, end, frequency="day", asset_type=None):
        if not re.fullmatch(r"(?:sh|sz)\d{6}", str(code).lower()) or asset_type != "index":
            raise ValueError("CNIndex daily input is restricted to explicitly routed Shanghai or Shenzhen indices")
        if frequency != "day":
            raise ValueError("CNIndex daily input supports only frequency=day")
        start_day = start.isoformat() if isinstance(start, date) else date.fromisoformat(str(start)).isoformat()
        end_day = end.isoformat() if isinstance(end, date) else date.fromisoformat(str(end)).isoformat()
        if start_day != end_day:
            raise ValueError("CNIndex daily unit must request exactly one target date")
        numeric_code = str(code).lower()[2:]
        response = self.transport.get(self.url, params={
            "indexCode": numeric_code,
            "startDate": start_day,
            "endDate": end_day,
            "frequency": frequency,
        }, timeout_seconds=20)
        target = date.fromisoformat(start_day)
        return {"symbol": str(code).lower(), "start": target, "end": target,
            "frequency": frequency, "path": "/market/market/getIndexDailyDataWithDataFormat",
            "segments": ((target, target, response, "hq.cnindex.com.cn"),)}

    def parse_window(self, captured):
        segments = captured.get("segments", ())
        if len(segments) != 1:
            raise ProviderContractError("CNIndex daily response count changed", FailureClass.SCHEMA_CHANGED, retryable=False)
        _, _, response, _ = segments[0]
        try:
            payload = json.loads(response.text)
        except (ValueError, UnicodeError) as exc:
            raise ProviderContractError("CNIndex daily response is not valid JSON", FailureClass.INVALID_JSON,
                                        retryable=False) from exc
        data = payload.get("data") if isinstance(payload, dict) else None
        code = str(captured["symbol"])[2:]
        if (not isinstance(payload, dict) or str(payload.get("code")) != "200"
                or not isinstance(data, dict) or str(data.get("indexCode", "")) != code
                or data.get("item") != list(self.expected_fields) or not isinstance(data.get("data"), list)):
            raise ProviderContractError("CNIndex daily response envelope or field order changed",
                                        FailureClass.SCHEMA_CHANGED, retryable=False)
        target = captured["start"].isoformat()
        source_rows, rows = [], []
        for values in data["data"]:
            if not isinstance(values, list) or len(values) != len(self.expected_fields):
                raise ProviderContractError("CNIndex daily row width changed", FailureClass.SCHEMA_CHANGED,
                                            retryable=False)
            source = dict(zip((str(index) for index in range(len(values))), values))
            source.update(symbol=captured["symbol"], source_index_code=code)
            source_rows.append(source)
            try:
                row_date = date.fromisoformat(str(values[0])[:10]).isoformat()
            except ValueError as exc:
                raise ProviderContractError("CNIndex daily date changed", FailureClass.SCHEMA_CHANGED,
                                            retryable=False) from exc
            if row_date != target:
                continue
            for field in ("high", "open", "low", "close"):
                value = values[self.expected_fields.index(field)]
                try:
                    numeric = float(value)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise ProviderContractError("CNIndex daily price field is invalid: " + field,
                                                FailureClass.SCHEMA_CHANGED, retryable=False) from exc
                if isinstance(value, bool) or value is None or not math.isfinite(numeric) or numeric <= 0:
                    raise ProviderContractError("CNIndex daily price field is invalid: " + field,
                                                FailureClass.SCHEMA_CHANGED, retryable=False)
            numeric = {field: float(values[self.expected_fields.index(field)])
                for field in ("high", "open", "low", "close")}
            if not numeric["low"] <= min(numeric["open"], numeric["close"]) <= max(numeric["open"], numeric["close"]) <= numeric["high"]:
                raise ProviderContractError("CNIndex daily price ordering is invalid",
                                            FailureClass.SCHEMA_CHANGED, retryable=False)
            rows.append({"symbol": captured["symbol"], "trade_date": row_date,
                "open": values[3], "high": values[2], "low": values[4], "close": values[5],
                "volume": None, "amount": None, "adjustment": "none", "value_contract": "full_ohlc"})
        if not rows:
            raise ProviderContractError("CNIndex daily response has no target-date row",
                                        FailureClass.TEMPORARY_EMPTY, retryable=False)
        if len(rows) != 1:
            raise ProviderContractError("CNIndex daily response has duplicate target-date rows",
                                        FailureClass.SCHEMA_CHANGED, retryable=False)
        return InputFetchResult(tuple(rows), source_url=self.url, source_rows=tuple(source_rows),
            mapping_context={"adjustment": "none", "value_contract": "full_ohlc",
                "returned_row_count": len(source_rows), "volume_semantics": "unverified",
                "amount_semantics": "unverified"})
