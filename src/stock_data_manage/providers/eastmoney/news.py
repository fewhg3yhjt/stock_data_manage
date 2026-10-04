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
