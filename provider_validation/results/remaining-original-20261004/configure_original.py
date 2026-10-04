"""Original-source input schemas; leave production definitions and shared parameter anchors intact."""
from pathlib import Path
import re,yaml
ROOT=Path(__file__).resolve().parents[3]

def main():
    date_field={'snapshot_at':('datetime',None),'trade_date':('date','date')}
    package={**date_field,'source_security_code':('string','code'),'market':('string','market'),'name':('string','name'),
        **{k:('decimal',k) for k in ('prev_close','open','high','low','close','volume','amount')}}
    ticks={**date_field,'source_security_code':('string','code'),'time':('string','time'),'source_sequence':('integer','seq'),'side':('string','side'),
        **{k:('decimal',k) for k in ('price','change','volume','amount')}}
    etf={**date_field,'exchange':('string','exchange'),'source_security_code':('string','code'),'name':('string','name'),
        'etf_type':('string','etf_type'),'shares':('decimal','shares_10k')}
    curve={**date_field,'curve':('string','curve'),**{'yield_'+t:('decimal',t) for t in ('3m','6m','1y','3y','5y','7y','10y','30y')}}
    futures={**date_field,'exchange':('string','exchange'),'contract_code':('string','symbol'),'product':('string','product'),
        **{k:('decimal',k) for k in ('open','high','low','close','settle','pre_settle','volume','open_interest','oi_change','turnover_10k')}}
    options={**date_field,'exchange':('string','exchange'),'contract_code':('string','symbol'),'series':('string','series'),'option_type':('string','option_type'),
        **{k:('decimal',k) for k in ('strike','open','high','low','close','settle','pre_settle','volume','open_interest','oi_change','turnover_10k','delta','iv_pct','series_iv_pct')}}
    rank={**date_field,'exchange':('string','exchange'),'contract_code':('string','symbol'),'level':('string','level'),'rank':('integer','rank'),
        **{k:('string',k) for k in ('volume_member','long_member','short_member')},**{k:('decimal',k) for k in ('volume','volume_chg','long_oi','long_chg','short_oi','short_chg')}}
    spot={**date_field,'instrument':('string','instrument'),**{k:('decimal',k) for k in ('open','high','low','close')}}
    changes={'snapshot_at':('datetime',None),'source_row_number':('integer','source_row_number'),'source_security_code':('string','代码'),
        'event_time':('string','时间'),'name':('string','名称'),'event_type':('string','异动类型'),'source_type':('string','板块'),'source_details':('string','相关信息')}
    entries={
        'ASTOCK-003':('tdx_daily_package','fetch_daily_package',package,['snapshot_at','trade_date','market','source_security_code']),
        'ASTOCK-004':('tencent_ticks','fetch_ticks',ticks,['snapshot_at','trade_date','source_security_code','source_sequence']),
        'ASTOCK-030':('etf_shares','fetch_etf',etf,['snapshot_at','trade_date','exchange','source_security_code']),
        'ASTOCK-063':('bond_yield_curve','fetch',curve,['snapshot_at','trade_date','curve']),
        'ASTOCK-071':('official_futures_daily','fetch_futures',futures,['snapshot_at','trade_date','exchange','contract_code']),
        'ASTOCK-072':('official_options_daily','fetch_options',options,['snapshot_at','trade_date','exchange','contract_code']),
        'ASTOCK-073':('position_rank','fetch_rank',rank,['snapshot_at','trade_date','exchange','level','contract_code','rank']),
        'ASTOCK-077':('sge_spot','fetch',spot,['snapshot_at','trade_date','instrument']),
        'ASTOCK-051':('intraday_changes','fetch_intraday_changes',changes,['snapshot_at','source_row_number']),
    }
    providers=ROOT/'config/providers.yaml';text=providers.read_text(encoding='utf-8');doc=yaml.safe_load(text)
    for input_id,(dataset,method,fields,primary) in entries.items():
        contract=doc['input_capabilities'][input_id]
        m=re.search(rf'^  {input_id}:\n.*?(?=^  [A-Z][A-Z0-9_-]*:|\Z)',text,re.M|re.S);block=m.group(0)
        block=block.replace(f'  {input_id}:\n',f'  {input_id}:\n    dataset: {dataset}\n    runtime_method: {method}\n',1)
        block=block.replace('implementation_status: unimplemented','implementation_status: implemented_validation_only',1).replace('request_limit_enforcement: unimplemented','request_limit_enforcement: call_boundary_only',1)
        block=block.replace('    limitations:\n','    limitations:\n    - 仅原成功明确接口，保留原请求和完整解析校验，不内嵌来源或日期回退；标准数值单位未认证\n',1)
        if input_id=='ASTOCK-051':block+='    base_requests_per_fetch: 22\n'
        if input_id in {'ASTOCK-003','ASTOCK-030','ASTOCK-071','ASTOCK-072','ASTOCK-073'} and 'trading_date_parameter:' not in block:block+='    trading_date_parameter: date\n'
        if input_id=='ASTOCK-063':block=block.replace('        source: config.curve\n','        source: config.curve\n        default: all\n')
        text=text[:m.start()]+block+text[m.end():]
        schema={'dataset':{'name':dataset,'display_name':contract['display_name']+'候选输入','primary_key':primary},'fields':{}}
        numeric=[k for k,v in fields.items() if v[0]=='decimal']
        for key,(kind,source) in fields.items():
            schema['fields'][key]={'type':kind,'required':key in primary}
            if key in numeric:schema['fields'][key]['unit']='unverified'
        rule={'id':dataset+'-input-v1','input_id':input_id,'provider':contract['provider'],'endpoint':contract['endpoint'],
            'field_mapping':{k:v[1] for k,v in fields.items() if v[1]},'context_fields':{'snapshot_at':'source_snapshot_at'},'null_values':[None,''],
            'unverified_fields':numeric,'status':'pending_validation','version':'remaining-original-input-v1','evidence_refs':[input_id],
            'notes':'保留原独立接口及原解析。原数字单独留证，标准数字单位未认证；无来源或日期回退，生产关闭。'}
        if input_id=='ASTOCK-051':rule['notes']+=' 来源只有日内时钟，禁止将响应采集日期推断为交易日；22类型完整覆盖按各来源tc核对。'
        for folder,body in [('datasets',schema),('normalization',{'dataset':dataset,'rules':[rule]})]:
            target=ROOT/'config'/folder/(dataset+'.yaml');assert not target.exists();target.write_bytes(yaml.safe_dump(body,allow_unicode=True,sort_keys=False).encode())
    providers.write_bytes(text.encode())
    print('configured',list(entries))

if __name__=='__main__':main()
