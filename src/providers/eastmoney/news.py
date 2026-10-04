"""Original search SDK page, preserving keyword-search scope and raw article fields."""
import json
import re
from datetime import datetime
from ..akshare.session import load_client
from ..contracts import InputFetchResult
from .realtime import history_stock_identity


class EastMoneyStockNewsProvider:
    capability_version='eastmoney-stock-news-input-v1'
    input_hosts=('https://search-api-web.eastmoney.com',)
    def __init__(self,client=None):self.client=client

    def fetch_announcements(self, *, date, source_responses):
        import math
        from urllib.parse import urlsplit, parse_qs
        self.client = self.client or load_client()
        frame = self.client.stock_notice_report(symbol='全部', date=date.strftime('%Y%m%d'))
        responses = list(source_responses())
        if not responses:
            raise ValueError('original announcement responses are required')
        payloads = [json.loads(r['body']) for r in responses]
        if any(p.get('success') != 1 or p.get('error') not in ('', None) for p in payloads):
            raise ValueError('announcement business status changed')
        total = payloads[0].get('data', {}).get('total_hits')
        if isinstance(total, bool) or not isinstance(total, int) or total <= 0:
            raise ValueError('announcement positive source total changed')
        pages = math.ceil(total / 100)
        if len(responses) != pages + 1:
            raise ValueError('announcement original discovery request and all pages required')
        items = []
        for page, (response, payload) in enumerate(zip(responses[1:], payloads[1:]), 1):
            query = parse_qs(urlsplit(response['url']).query)
            if query.get('page_index') != [str(page)] or query.get('begin_time') != [date.isoformat()] or query.get('end_time') != [date.isoformat()] or 'stock_list' in query:
                raise ValueError('announcement response differs from explicit whole-market date/page scope')
            data = payload.get('data', {})
            rows = data.get('list')
            if data.get('total_hits') != total or not isinstance(rows, list) or len(rows) != min(100, total - (page-1)*100):
                raise ValueError('announcement page count changed')
            items.extend(rows)
        if len(frame) != total or tuple(frame.columns) != ('代码','名称','公告标题','公告类型','公告日期','网址'):
            raise ValueError('announcement SDK skipped rows or fields changed')
        parsed = frame.astype(object).where(frame.notna(), None).to_dict('records')
        for item, row in zip(items, parsed):
            if not item.get('art_code') or str(item.get('notice_date', ''))[:10] != date.isoformat() or row['公告日期'] != date:
                raise ValueError('announcement article identity/date changed')
            codes, columns = item.get('codes'), item.get('columns')
            if not codes or not columns:
                raise ValueError('announcement issuer/type metadata missing')
            selected = codes[0] if len(codes) == 1 else next((c for c in codes if c['ann_type'].startswith('A')), None)
            if selected is None or row['代码'] != selected['stock_code'] or row['名称'] != selected['short_name'] or row['公告标题'] != item['title'] or row['公告类型'] != columns[0]['column_name']:
                raise ValueError('announcement SDK disagrees with raw selected issuer/article')
            if row['网址'] != f"https://data.eastmoney.com/notices/detail/{row['代码']}/{item['art_code']}.html":
                raise ValueError('announcement original link changed')
            row['source_article_code'] = item['art_code']
        return InputFetchResult(tuple(parsed), source_rows=tuple(items), source_url=responses[0]['url'],
            mapping_context={'requested_date': date, 'source_total_count': total, 'source_page_count': pages,
                             'pagination_completeness_verified': True,
                             'scope_meaning': 'whole-market dated announcement index; SDK selects first A issuer; no document contents'})
    def fetch(self,*,code,source_responses):
        code=history_stock_identity(code)[0]
        self.client=self.client or load_client()
        frame=self.client.stock_news_em(symbol=code)
        responses=list(source_responses())
        if len(responses)!=1:raise ValueError('one original search page is required')
        text=responses[0]['body'].decode('utf-8')
        match=re.fullmatch(r'\s*jQuery\d+_\d+\((.*)\)\s*;?\s*',text,re.S)
        if not match:raise ValueError('news JSONP envelope changed')
        payload=json.loads(match.group(1))
        if payload.get('code')!=0:raise ValueError('news search business status changed')
        items=payload.get('result',{}).get('cmsArticleWebOld')
        if not isinstance(items,list) or not items or len(items)>10 or len(items)!=len(frame):
            raise ValueError('original news first-page count changed')
        if tuple(frame.columns)!=('关键词','新闻标题','新闻内容','发布时间','文章来源','新闻链接'):
            raise ValueError('original news fields changed')
        for item in items:
            if not {'code','date','title','content','mediaName'}<=set(item) or not item['code'] or not item['title']:
                raise ValueError('news source identity or fields missing')
            datetime.strptime(item['date'],'%Y-%m-%d %H:%M:%S')
        if len({r['code'] for r in items})!=len(items):raise ValueError('duplicate article identity')
        rows=tuple({'source_article_code':item['code'],**row} for item,row in zip(items,frame.astype(object).where(frame.notna(),None).to_dict(orient='records')))
        return InputFetchResult(rows,source_rows=tuple(items),source_url=responses[0]['url'],
            mapping_context={'keyword':code,'page':1,'page_size':10,'pagination_completeness_verified':False,
                'source_hits_total':payload.get('hitsTotal'),
                'scope_meaning':'keyword search, not verified issuer ownership'})
