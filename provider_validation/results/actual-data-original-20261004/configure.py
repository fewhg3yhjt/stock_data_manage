"""Apply the user-approved scope corrections, preserving unrelated YAML and anchors."""
from pathlib import Path
import json
import hashlib
import re
import yaml

ROOT = Path(__file__).resolve().parents[3]
providers = ROOT / 'config/providers.yaml'
text = providers.read_text(encoding='utf-8')
old = yaml.safe_load(text)['input_capabilities']
new = {}
def configure(key, **changes):
    value = dict(old[key])
    value.update(changes)
    new[key] = value
    return value

for key, changes in {
 'ASTOCK-014': dict(endpoint='concept_directory', display_name='同花顺概念目录（返回样本范围）',
     collection_profile='reference_daily', dataset='concept_directory', runtime_method='fetch_concept_list', base_requests_per_fetch=41,
     limitations=['原 SDK 合并目录和概念时间表，归档含 401 和重复内容页；仅认证返回的名称及代码，不认证全量、热度或成员关系。']),
 'ASTOCK-044': dict(provider='baostock', display_name='BaoStock 沪深 ST 名称筛选名单', dataset='st_name_list', runtime_method='fetch_st_list',
     limitations=['按原 code_name=ST 查询并筛选 type=1/status=1；名称规则不是官方风险标志，无北交所、价格或涨跌幅。']),
 'ASTOCK-087': dict(endpoint='dated_announcements', display_name='东财全市场指定日期公告目录', dataset='dated_announcements',
     runtime_method='fetch_announcements', parameters={'date': {'type':'date','source':'request.date','required':True}},
     request_shape='date_snapshot', base_requests_per_fetch=9,
     limitations=['明确公告发布日期、全部分类、全市场范围；保存全部页原响应及文章编码，沿用原 SDK 第一 A 类发行人选择，无正文、无单股过滤。']),
}.items():
    configure(key, **changes, implementation_status='implemented_validation_only', request_limit_enforcement='call_boundary_only',
              forbidden_sources=['request.symbol','request.symbols','request.start_date','request.end_date','request.trade_date','config.limit','config.count'])
configure('ASTOCK-037', implementation_status='blocked', display_name='F10 原复合验证记录（已拆分）',
          limitations=['原复合记录不是可执行的完整 F10 接口；成功子项分别见 ASTOCK-037-profile、ASTOCK-037-events、ASTOCK-037-business。失败子项不接入。'])
for key, provider, endpoint, display, dataset, method, parameters, shape, status, limit in [
 ('ASTOCK-037-profile','cninfo','company_profile','巨潮公司概况','company_profile','fetch',old['ASTOCK-011']['parameters'],'single_symbol','implemented_validation_only','仅一只明确股票；注册资金原值保留，金额单位未认证；不称为完整 F10。'),
 ('ASTOCK-037-events','eastmoney','company_events','东财全市场指定日期公司动态','company_events','fetch_company_events',{'date':{'type':'date','source':'request.date','required':True}},'date_snapshot','implemented_validation_only','原成功请求实际日期为 2023-08-08，75 条为全市场动态；调用必须明确日期，无个股筛选。'),
 ('ASTOCK-037-business','ths','main_business','同花顺主营业务','main_business','fetch_main_business',old['ASTOCK-011']['parameters'],'single_symbol','unimplemented','旧样本为 600519 的 1 条主营业务；原 HTML 未保留，仅哈希，补采原 SDK 超过 120 秒。待可回放证据，不标成来源不可用。'),
]:
    new[key] = dict(provider=provider,endpoint=endpoint,display_name=display,dataset=dataset,runtime_method=method,parameters=parameters,
        data_kind='table',data_frequency='snapshot',request_shape=shape,collection_profile='event_daily',
        implementation_status=status,evidence_refs=['ASTOCK-037'],request_interval_seconds=3,effective_concurrency=1,
        request_limit_enforcement='call_boundary_only' if status=='implemented_validation_only' else 'unimplemented',base_requests_per_fetch=1,
        limitations=[limit],forbidden_sources=['request.symbols','request.start_date','request.end_date','request.trade_date','config.limit','config.count']+
            (['request.symbol'] if key.endswith('events') else []))
for key in ['ASTOCK-014','ASTOCK-037','ASTOCK-044','ASTOCK-087']:
    block = yaml.safe_dump({key:new[key]},allow_unicode=True,sort_keys=False).rstrip()
    block = '\n'.join('  '+line for line in block.splitlines())+'\n'
    text, count = re.subn(r'^  '+re.escape(key)+r':\n.*?(?=^  [A-Z][A-Z0-9-]+:|\Z)',lambda _:block,text,flags=re.M|re.S)
    assert count==1
children = {k:v for k,v in new.items() if k.startswith('ASTOCK-037-')}
children_text = yaml.safe_dump(children,allow_unicode=True,sort_keys=False)
children_text = re.sub(r'([&*])id(\d+)',r'\1actual_id\2',children_text)
text += '\n'+'\n'.join('  '+line for line in children_text.rstrip().splitlines())+'\n'
providers.write_bytes(text.encode())

def dataset(name, display, keys, mapping, context, required, date_fields=(), decimal_fields=(), unverified=()):
    fields = {key: {'type': 'datetime' if key=='snapshot_at' else 'date' if key in date_fields else 'decimal' if key in decimal_fields else 'string',
                    'required':key in required} for key in [*context,*mapping]}
    for key in unverified: fields[key]['unit']='unverified'
    item = next((k,v) for k,v in new.items() if v.get('dataset')==name)
    key, cap = item
    docs = {
      'datasets': {'dataset':{'name':name,'display_name':display,'primary_key':keys},'fields':fields},
      'normalization': {'dataset':name,'rules':[dict(id=name+'-actual-input-v1',input_id=key,provider=cap['provider'],endpoint=cap['endpoint'],
        field_mapping=mapping,context_fields=context,null_values=[None,''],unverified_fields=list(unverified),status='pending_validation',
        version='actual-data-input-v1',evidence_refs=cap['evidence_refs'],notes='按原成功数据范围接入；snapshot_at 为原响应捕获时间，不是来源生效日期。原字段独立留证，不自动启用生产。')]}
    }
    for directory, document in docs.items():
        p=ROOT/'config'/directory/(name+'.yaml');assert not p.exists();p.write_bytes(yaml.safe_dump(document,allow_unicode=True,sort_keys=False).encode())

dataset('concept_directory','同花顺概念目录返回样本',['snapshot_at','board_code'],{'board_name':'name','board_code':'code'},
        {'snapshot_at':'source_snapshot_at','board_type':'board_type','source':'source'},['snapshot_at','board_type','board_code','board_name','source'])
dataset('st_name_list','BaoStock 沪深 ST 名称筛选',['snapshot_at','exchange','stock_code'],
        {'stock_code':'code','exchange':'market','stock_name':'name','st_type':'st_type'},
        {'snapshot_at':'source_snapshot_at','source':'source'},['snapshot_at','stock_code','exchange','stock_name','st_type','source'])
profile_map = dict(zip(['company_name','english_name','former_name','stock_code','stock_name','b_share_code','b_share_name','h_share_code','h_share_name',
    'index_names','market','industry_name','legal_representative','registered_capital_source_value','established_date','listed_date','website','email',
    'phone','fax','registered_address','office_address','postal_code','main_business','business_scope','introduction'],
    ['公司名称','英文名称','曾用简称','A股代码','A股简称','B股代码','B股简称','H股代码','H股简称','入选指数','所属市场','所属行业','法人代表','注册资金',
     '成立日期','上市日期','官方网站','电子邮箱','联系电话','传真','注册地址','办公地址','邮政编码','主营业务','经营范围','机构简介']))
profile_map['registered_capital']='注册资金'
dataset('company_profile','巨潮公司概况原字段',['snapshot_at','stock_code'],profile_map,{'snapshot_at':'source_snapshot_at'},
        ['snapshot_at','company_name','stock_code','stock_name'],date_fields=['established_date','listed_date'],
        decimal_fields=['registered_capital'],unverified=['registered_capital'])
dataset('company_events','东财全市场指定日期公司动态',['snapshot_at','source_row_number'],
        {'source_row_number':'序号','stock_code':'代码','stock_name':'简称','event_type':'事件类型','content':'具体事项','source_date':'交易日'},
        {'snapshot_at':'source_snapshot_at'},['snapshot_at','source_row_number','stock_code','event_type','content','source_date'],date_fields=['source_date'])
dataset('dated_announcements','东财指定日期全市场公告目录',['snapshot_at','source_article_code'],
        {'source_article_code':'source_article_code','stock_code':'代码','stock_name':'名称','title':'公告标题','notice_type':'公告类型','publication_date':'公告日期','url':'网址'},
        {'snapshot_at':'source_snapshot_at'},['snapshot_at','source_article_code','stock_code','title','publication_date','url'],date_fields=['publication_date'])

# Reuse the already persisted 317-row original SDK payload; never call the source to recreate a manifest.
source=ROOT/'provider_validation/results/remaining-st-evidence-20261004'
result=json.loads((source/'result.json').read_text(encoding='utf-8'))
payload=(source/'sdk-payload.json').read_bytes();assert hashlib.sha256(payload).hexdigest()==result['source_payload_sha256']
target=ROOT/'provider_validation/results/actual-data-original-20261004/st';target.mkdir()
(target/'sdk-payload.json').write_bytes(payload)
event=dict(event='source_payload',provider='baostock',endpoint='query_stock_basic',method='query_stock_basic',request_parameters={'code_name':'ST'},
    scope={'code_name':'ST','exchange_scope':'SH+SZ'},fetched_at_utc=result['validation_time_utc'],mode='live',outcome='response',sdk_status_code='0',
    code_version=result['source_sha256'],metadata={'row_count':317,'code_name':'ST'},body_storage='sdk-payload.json',body_sha256=result['source_payload_sha256'],
    payload_bytes=len(payload),original_transport_bytes_available=False,representation=result['representation'],
    original_result_path=str(source.relative_to(ROOT)/'result.json'),original_result_sha256=hashlib.sha256((source/'result.json').read_bytes()).hexdigest())
(target/'manifest.ndjson').write_bytes((json.dumps(event,ensure_ascii=False)+'\n').encode())
print('configured actual scope inputs and source-linked SDK manifest')
