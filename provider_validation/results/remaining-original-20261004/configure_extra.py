"""Add only the two newly captured original-source contracts; leave YAML aliases intact."""
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parents[3]
items=[('ASTOCK-031','stock_news','eastmoney','stock_news',{
    'source_article_code':'source_article_code','keyword':'关键词','title':'新闻标题','content':'新闻内容',
    'published_text':'发布时间','media':'文章来源','url':'新闻链接'},[],['snapshot_at','source_article_code'],
    '个股关键词新闻候选输入'),
    ('ASTOCK-069','index_valuation','csindex','index_valuation',{
    'trade_date':'日期','name':'指数中文全称','pe_1':'市盈率1','pe_2':'市盈率2','dividend_yield_1':'股息率1','dividend_yield_2':'股息率2'},
    ['pe_1','pe_2','dividend_yield_1','dividend_yield_2'],['snapshot_at','index_code','trade_date'],'中证指数估值候选输入')]
for id,dataset,provider,endpoint,mapping,numbers,key,display in items:
    fields={'snapshot_at':dict(type='datetime',required=True)}
    if id=='ASTOCK-069':fields['index_code']=dict(type='string',required=True)
    for name in mapping:
        fields[name]=dict(type='decimal' if name in numbers else 'date' if name=='trade_date' else 'string',required=name in key)
        if name in numbers:fields[name]['unit']='unverified'
    template=dict(dataset=dict(name=dataset,display_name=display,primary_key=key),fields=fields)
    context=dict(snapshot_at='source_snapshot_at')
    if id=='ASTOCK-069':context['index_code']='index_code'
    normalization=dict(dataset=dataset,rules=[dict(id=dataset+'-input-v1',input_id=id,provider=provider,endpoint=endpoint,
        field_mapping=mapping,context_fields=context,null_values=[None,''],unverified_fields=numbers,status='pending_validation',
        version='remaining-native-sdk-input-v1',evidence_refs=[id],notes='原SDK传输保持；精确响应先保存，全部来源字段另存。新闻仅关键词首页10条，不认证发行人归属；估值两种口径不合并。')])
    for folder,value in [('datasets',template),('normalization',normalization)]:
        path=ROOT/'config'/folder/(dataset+'.yaml');assert not path.exists()
        path.write_bytes(yaml.safe_dump(value,allow_unicode=True,sort_keys=False).encode())
    path=ROOT/'config/providers.yaml';text=path.read_text(encoding='utf-8')
    start=text.index('  '+id+':');end=text.find('\n  ASTOCK-',start+1)
    block=text[start:end]
    block=block.replace('implementation_status: unimplemented','implementation_status: implemented_validation_only').replace('request_limit_enforcement: unimplemented','request_limit_enforcement: call_boundary_only')
    block=block.replace('    provider: '+('script_composite' if id=='ASTOCK-069' else provider),'    dataset: '+dataset+'\n    runtime_method: fetch\n    provider: '+provider,1)
    block=block.replace('    limitations:\n','    limitations:\n    - 原SDK单标的实时小探针补精确响应；仅候选，未启用生产路由\n    - '+('关键词搜索首页最多10条，不能认证发行人所属新闻全集' if id=='ASTOCK-031' else '原中证工作簿20个日期；市盈率和股息率两口径独立保留，单位未认证')+'\n',1)
    text=text[:start]+block+text[end:]
    path.write_bytes(text.encode())
