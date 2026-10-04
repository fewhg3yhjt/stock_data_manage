from datetime import datetime
import html
import re
import time as _report_clock
import requests

from ..akshare.session import load_client
from ..contracts import InputFetchResult


_report_last = [0.0]
_REPORT_ROW = re.compile(
    r'<tr>\s*<td>\d+</td>\s*<td class="tal f14">\s*<a[^>]*?title="([^"]*)"[^>]*?'
    r'href="([^"]*?/rptid/(\d+)/[^"]*)"[^>]*>.*?</a>\s*</td>\s*'
    r'<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*</tr>',re.S)


class SinaGlobalNewsProvider:
    """Sina news and report lists retain their distinct successful source contracts."""
    capability_version = "sina-global-news-input-v1"
    input_hosts = ("https://zhibo.sina.com.cn",)

    def __init__(self, client=None):
        self.client = client

    def fetch_dragon_tiger_daily(self, *, date, source_responses):
        """The successful Sina daily list; no EastMoney alternate endpoint."""
        import math
        from io import StringIO
        import pandas as pd
        self.client = self.client or load_client()
        frame = self.client.stock_lhb_detail_daily_sina(date=date.strftime('%Y%m%d'))
        responses = list(source_responses())
        columns = ('序号','股票代码','股票名称','收盘价','对应值','成交量','成交额','指标')
        if len(responses)!=1 or frame.empty or tuple(frame.columns)!=columns:
            raise ValueError('Sina daily billboard response or fields changed')
        if f'tradedate={date.isoformat()}' not in responses[0]['url']:
            raise ValueError('Sina billboard requires explicit day')
        for row in frame.to_dict(orient='records'):
            if not re.fullmatch(r'\d{6}',row['股票代码']) or not row['指标']:
                raise ValueError('Sina billboard security or reason changed')
            if any(pd.isna(row[key]) or not math.isfinite(float(row[key])) for key in columns[3:7]):
                raise ValueError('Sina billboard source number changed')
        parsed=frame.astype(object).where(frame.notna(),None).to_dict(orient='records')
        rows = tuple({'trade_date':date, 'source_row_number':i+1, **r} for i,r in enumerate(parsed))
        return InputFetchResult(rows,source_rows=rows,source_url=responses[0]['url'],mapping_context={'source_total_count':len(rows)})

    def fetch_reports(self, *, page=1):
        """Only the archived latest-market page; preserve GBK and false-empty retry."""
        if type(page) is not int or page != 1:
            raise ValueError("only the observed latest market page 1 is enabled")
        url="https://vip.stock.finance.sina.com.cn/q/go.php/vReport_List/kind/lastest/index.phtml"
        headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                 "Referer":"https://finance.sina.com.cn/"}
        for attempt in range(2):
            wait=6.0-(_report_clock.time()-_report_last[0])
            if wait>0:_report_clock.sleep(wait)
            try:
                response=requests.request("GET",url,params={"p":page},data=None,headers=headers,timeout=(10,40),allow_redirects=True)
                response.raise_for_status()
            except requests.RequestException as exc:
                raise RuntimeError(f"report transport failed: {type(exc).__name__}") from exc
            finally:_report_last[0]=_report_clock.time()
            text=response.content.decode("gbk","replace")
            if "没有找到相关内容" not in text:break
        if "tb_01" not in text or "研究员" not in text:
            raise RuntimeError("report table structure changed")
        rows=[]
        for title,href,report_id,kind,day,org,author in _REPORT_ROW.findall(text):
            try:
                day=day.strip()
                day=datetime.strptime(day,"%Y%m%d" if re.fullmatch(r"[0-9]{8}",day) else "%Y-%m-%d").date().isoformat()
            except ValueError as exc:raise RuntimeError("report source date changed") from exc
            strip=lambda fragment:html.unescape(re.sub(r"<[^>]+>","",fragment)).strip()
            rows.append({"date":day,"title":html.unescape(title).strip(),"type":kind.strip(),"org":strip(org),"author":strip(author),
                         "report_id":report_id,"url":"https:"+href if href.startswith("//") else href})
        numbered=len(re.findall(r"<tr>\s*<td>\d+</td>",text))
        if len(rows)!=numbered or (not rows and "没有找到相关内容" not in text):
            raise RuntimeError("report numbered rows were not completely parsed")
        return InputFetchResult(tuple(rows),source_rows=tuple(rows),source_url=response.url,
            empty_is_valid=not rows and attempt==1 and "没有找到相关内容" in text,
            mapping_context={"page":page,"empty_page_attempts":attempt+1})

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
