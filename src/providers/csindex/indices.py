from datetime import date, datetime
from io import BytesIO
import math
import re
import pandas as pd

from ..akshare.session import load_client
from ..contracts import InputFetchResult


class CsindexDailyProvider:
    """Retained-response adapter for the two explicit CSI daily value contracts."""
    name = "csindex"
    endpoint = "index-perf"
    capability_version = "csindex-index-perf-close-v1"
    input_hosts = ("https://www.csindex.com.cn",)
    url = "https://www.csindex.com.cn/csindex-home/perf/index-perf"

    def __init__(self, transport, *, value_contract="close_only"):
        if value_contract not in {"close_only", "full_ohlc"}:
            raise ValueError("CSI daily value contract must be close_only or full_ohlc")
        self.transport = transport
        self.value_contract = value_contract
        if value_contract == "full_ohlc":
            self.capability_version = "csindex-index-perf-ohlc-v1"

    def capture_window(self, code, start, end, adjust="none", api="index-perf", asset_type=None):
        from datetime import date
        match = re.fullmatch(r"sh(\d{6})", str(code).lower())
        if not match or asset_type != "index" or adjust != "none" or api != "index-perf":
            raise ValueError("CSI index-perf is restricted to configured Shanghai index units")
        start_day = start.isoformat() if isinstance(start, date) else date.fromisoformat(str(start)).isoformat()
        end_day = end.isoformat() if isinstance(end, date) else date.fromisoformat(str(end)).isoformat()
        if end_day != start_day:
            raise ValueError("CSI daily unit must request exactly one target date")
        response = self.transport.get(self.url, params={
            "indexCode": match.group(1), "startDate": start_day.replace("-", ""), "endDate": end_day.replace("-", "")},
            timeout_seconds=15)
        return {"symbol": str(code).lower(), "start": date.fromisoformat(start_day),
            "end": date.fromisoformat(end_day), "adjust": "none", "api": api,
            "path": "/csindex-home/perf/index-perf", "segments": ((date.fromisoformat(start_day),
                date.fromisoformat(end_day), response, "www.csindex.com.cn"),)}

    def parse_window(self, captured):
        import json
        from datetime import date
        from ..contracts import ProviderContractError, FailureClass
        segments = captured.get("segments", ())
        if len(segments) != 1:
            raise ProviderContractError("CSI daily response count changed", FailureClass.SCHEMA_CHANGED, retryable=False)
        _, _, response, _ = segments[0]
        try:
            payload = json.loads(response.text)
        except (ValueError, UnicodeError) as exc:
            raise ProviderContractError("CSI daily response is not valid JSON", FailureClass.INVALID_JSON, retryable=False) from exc
        if not isinstance(payload, dict) or str(payload.get("code")) != "200" or payload.get("success") is not True or not isinstance(payload.get("data"), list):
            raise ProviderContractError("CSI daily response envelope changed", FailureClass.SCHEMA_CHANGED, retryable=False)
        code = captured["symbol"][2:]
        target = captured["start"].strftime("%Y%m%d")
        source_rows = payload["data"]
        rows = []
        for item in source_rows:
            if not isinstance(item, dict) or not {"tradeDate", "indexCode", "close", "open", "high", "low"} <= set(item):
                raise ProviderContractError("CSI daily field set changed", FailureClass.SCHEMA_CHANGED, retryable=False)
            if str(item["indexCode"]).zfill(6) != code:
                raise ProviderContractError("CSI returned a different index code", FailureClass.SCHEMA_CHANGED, retryable=False)
            if str(item["tradeDate"]) != target:
                continue
            prices = {name: item.get(name) for name in ("open", "high", "low", "close")}
            if self.value_contract == "close_only":
                if any(prices[name] is not None for name in ("open", "high", "low")):
                    raise ProviderContractError("CSI close-only response unexpectedly contains OHLC", FailureClass.SCHEMA_CHANGED, retryable=False)
                try:
                    close = float(prices["close"])
                except (TypeError, ValueError, OverflowError) as exc:
                    raise ProviderContractError("CSI close-only response has an invalid close", FailureClass.SCHEMA_CHANGED, retryable=False) from exc
                if isinstance(prices["close"], bool) or not math.isfinite(close) or close <= 0:
                    raise ProviderContractError("CSI close-only response has an invalid close", FailureClass.SCHEMA_CHANGED, retryable=False)
                prices.update(open=None, high=None, low=None)
            else:
                try:
                    numeric = {name: float(value) for name, value in prices.items()}
                except (TypeError, ValueError, OverflowError) as exc:
                    raise ProviderContractError("CSI full OHLC response has a missing or nonnumeric price", FailureClass.SCHEMA_CHANGED, retryable=False) from exc
                if any(isinstance(prices[name], bool) or not math.isfinite(value) or value <= 0
                       for name, value in numeric.items()):
                    raise ProviderContractError("CSI full OHLC response has an invalid price", FailureClass.SCHEMA_CHANGED, retryable=False)
                if not numeric["low"] <= min(numeric["open"], numeric["close"]) <= max(numeric["open"], numeric["close"]) <= numeric["high"]:
                    raise ProviderContractError("CSI full OHLC response has invalid price ordering", FailureClass.SCHEMA_CHANGED, retryable=False)
            rows.append({"symbol": captured["symbol"], "trade_date": captured["start"].isoformat(),
                **prices, "volume": None, "amount": None, "adjustment": "none",
                "value_contract": self.value_contract})
        if not rows:
            raise ProviderContractError(
                "CSI daily response has no target-date row", FailureClass.TEMPORARY_EMPTY, retryable=False
            )
        return InputFetchResult(tuple(rows), source_url=self.url, source_rows=tuple(source_rows),
            mapping_context={"adjustment": "none", "value_contract": self.value_contract, "returned_row_count": len(source_rows)})


class CsindexConstituentProvider:
    """Original SDK constituent and weight workbooks; each endpoint is independent."""
    input_hosts = ("https://oss-ch.csindex.com.cn",)
    capability_version = "csindex-constituents-input-v1"

    def __init__(self, endpoint, client=None):
        self.endpoint, self.client = endpoint, client

    def fetch(self, *, index_code, source_responses):
        if not re.fullmatch(r'\d{6}', index_code):
            raise ValueError('an explicit six-digit index code is required')
        self.client = self.client or load_client()
        if self.endpoint == 'index_valuation':
            frame=self.client.stock_zh_index_value_csindex(symbol=index_code)
            responses=list(source_responses())
            if len(responses)!=2 or responses[0]['body']!=responses[1]['body'] or responses[0]['url']!=responses[1]['url']:
                raise ValueError('original Excel format-check and parse requests must return the same workbook')
            raw=pd.read_excel(BytesIO(responses[0]['body']))
            columns=('日期','指数代码','指数中文全称','指数中文简称','指数英文全称','指数英文简称','市盈率1','市盈率2','股息率1','股息率2')
            if raw.empty or len(raw)!=len(frame) or raw.shape[1]!=10 or tuple(frame.columns)!=columns:
                raise ValueError('valuation workbook fields or coverage changed')
            raw.columns=columns
            for row in raw.to_dict(orient='records'):
                datetime.strptime(str(row['日期']),'%Y%m%d')
                if str(row['指数代码']).zfill(6)!=index_code:raise ValueError('valuation returned another index')
                for field in columns[6:]:
                    if pd.isna(row[field]):continue
                    if isinstance(row[field],bool) or not math.isfinite(float(row[field])):raise ValueError('invalid valuation source number')
            if frame['日期'].isna().any() or frame.duplicated(['日期','指数代码']).any():raise ValueError('invalid valuation date or duplicate')
            rows=tuple(frame.astype(object).where(frame.notna(),None).to_dict(orient='records'))
            return InputFetchResult(rows,source_rows=tuple(raw.astype(object).where(raw.notna(),None).to_dict(orient='records')),
                source_url=responses[0]['url'],mapping_context={'index_code':index_code,'pagination_completeness_verified':False,'source_total_count':len(raw)})
        weighted = self.endpoint == 'weights'
        function = self.client.index_stock_cons_weight_csindex if weighted else self.client.index_stock_cons_csindex
        frame = function(symbol=index_code)
        responses = list(source_responses())
        if len(responses) != 1:
            raise ValueError('one exact constituent workbook is required')
        raw = pd.read_excel(BytesIO(responses[0]['body']))
        columns = ('日期','指数代码','指数名称','指数英文名称','成分券代码','成分券名称','成分券英文名称','交易所','交易所英文名称') + (('权重',) if weighted else ())
        if raw.empty or raw.shape[1] != len(columns) or tuple(frame.columns) != columns or len(raw) != len(frame):
            raise ValueError('constituent workbook fields or coverage changed')
        raw.columns = columns
        for row in raw.to_dict(orient='records'):
            datetime.strptime(str(row['日期']), '%Y%m%d')
            if str(row['指数代码']).zfill(6) != index_code or not re.fullmatch(r'\d{6}',str(row['成分券代码']).zfill(6)):
                raise ValueError('constituent workbook ignored requested index or invalid constituent')
            if weighted and (isinstance(row['权重'],bool) or not math.isfinite(float(row['权重']))):
                raise ValueError('source weight is not a finite number')
        if frame.duplicated(['日期','成分券代码']).any() or frame['日期'].isna().any():
            raise ValueError('duplicate constituent or invalid source date')
        rows = tuple(frame.astype(object).where(frame.notna(),None).to_dict(orient='records'))
        return InputFetchResult(rows,source_rows=tuple(raw.to_dict(orient='records')),source_url=responses[0]['url'],
            mapping_context={'index_code':index_code,'source_total_count':len(raw),'coverage_complete':True})
