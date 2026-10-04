"""Add only source-backed dataset mappings to the existing configuration."""
from pathlib import Path
import json,re,yaml
ROOT=Path(__file__).resolve().parents[3]

def main():
    entries={
      'ASTOCK-067':('index_constituents','fetch',{'snapshot_at':('datetime',None),'index_code':('string','指数代码'),'trade_date':('date','日期'),
        'source_security_code':('string','成分券代码'),'name':('string','成分券名称'),'index_name':('string','指数名称'),'exchange_name':('string','交易所')},[],['snapshot_at','index_code','trade_date','source_security_code']),
      'ASTOCK-068':('index_weights','fetch',{'snapshot_at':('datetime',None),'index_code':('string','指数代码'),'trade_date':('date','日期'),
        'source_security_code':('string','成分券代码'),'name':('string','成分券名称'),'index_name':('string','指数名称'),'exchange_name':('string','交易所'),'weight':('decimal','权重')},['weight'],['snapshot_at','index_code','trade_date','source_security_code']),
      'ASTOCK-055':('option_risk','fetch_risk',{'snapshot_at':('datetime',None),'trade_date':('date','TRADE_DATE'),'security_id':('string','SECURITY_ID'),
        'contract_code':('string','CONTRACT_ID'),'name':('string','CONTRACT_SYMBOL'),'delta':('decimal','DELTA_VALUE'),'theta':('decimal','THETA_VALUE'),
        'gamma':('decimal','GAMMA_VALUE'),'vega':('decimal','VEGA_VALUE'),'rho':('decimal','RHO_VALUE'),'implied_volatility':('decimal','IMPLC_VOLATLTY')},
        ['delta','theta','gamma','vega','rho','implied_volatility'],['snapshot_at','trade_date','security_id']),
      'ASTOCK-085':('sina_dragon_tiger_daily','fetch_dragon_tiger_daily',{'snapshot_at':('datetime',None),'trade_date':('date','trade_date'),
        'source_security_code':('string','股票代码'),'source_row_number':('integer','source_row_number'),'name':('string','股票名称'),'listing_reason':('string','指标'),
        'close_price':('decimal','收盘价'),'reference_value':('decimal','对应值'),'volume':('decimal','成交量'),'amount':('decimal','成交额')},
        ['close_price','reference_value','volume','amount'],['snapshot_at','trade_date','source_row_number']),
    }
    fields={'snapshot_at':('datetime',None),'source_security_code':('string','source_security_code'),'report_type':('string','report_type'),
        'report_date':('date','报告日'),'source_publish_text':('string','公告日期'),'source_update_text':('string','更新日期'),
        'currency_text':('string','币种'),'audit_text':('string','是否审计'),'source_type_text':('string','类型'),'source_text':('string','数据源')}
    metadata={'报告日','公告日期','更新日期','币种','是否审计','类型','数据源'}
    numeric=[]
    for table in ('balance','income','cashflow'):
        rows=json.loads((Path(__file__).parent/'sdk-replay'/table/'parsed.json').read_text(encoding='utf-8'))
        for key in rows[0]:
            if key not in metadata and key not in fields:
                fields[key]=('decimal',key);numeric.append(key)
    entries['ASTOCK-039']=('sina_financial_statements','fetch',fields,numeric,['snapshot_at','source_security_code','report_type','report_date'])
    providers=ROOT/'config/providers.yaml'
    text=providers.read_text(encoding='utf-8')
    document=yaml.safe_load(text)
    for input_id,(dataset,method,fields,numeric,primary) in entries.items():
        contract=document['input_capabilities'][input_id]
        section=re.search(rf'^  {input_id}:\n.*?(?=^  [A-Z][A-Z0-9_-]*:|\Z)',text,re.M|re.S)
        block=section.group(0)
        block=block.replace(f'  {input_id}:\n',f'  {input_id}:\n    dataset: {dataset}\n    runtime_method: {method}\n',1)
        block=block.replace('implementation_status: unimplemented','implementation_status: implemented_validation_only',1)
        block=block.replace('request_limit_enforcement: unimplemented','request_limit_enforcement: call_boundary_only',1)
        block=block.replace('    limitations:\n','    limitations:\n    - 仅原成功来源和明确范围，原SDK字段和响应保留；不内嵌来源或日期回退，标准数字单位待认证\n',1)
        if input_id=='ASTOCK-039':block+='    base_requests_per_fetch: 3\n'
        if input_id=='ASTOCK-055':block+='    trading_date_parameter: date\n'
        text=text[:section.start()]+block+text[section.end():]
        schema={'dataset':{'name':dataset,'display_name':contract['display_name']+'候选输入','primary_key':primary},'fields':{}}
        for key,(kind,source) in fields.items():
            definition={'type':kind,'required':key in primary}
            if key in numeric:definition['unit']='unverified'
            schema['fields'][key]=definition
        rule={'id':dataset+'-input-v1','input_id':input_id,'provider':contract['provider'],'endpoint':contract['endpoint'],
            'field_mapping':{k:v[1] for k,v in fields.items() if v[1] is not None},'context_fields':{'snapshot_at':'source_snapshot_at'},
            'null_values':[None,''],'unverified_fields':numeric,'status':'pending_validation','version':'remaining-sdk-input-v1','evidence_refs':[input_id],
            'notes':'原始响应与SDK所有字段单独保留；标准数字列不认证单位。快照时间沿用原响应时间；明确来源和日期，无适配器内部回退。'}
        if input_id=='ASTOCK-039':rule['transforms']={'report_date':{'format':'%Y%m%d'}}
        for folder,body in [('datasets',schema),('normalization',{'dataset':dataset,'rules':[rule]})]:
            target=ROOT/'config'/folder/(dataset+'.yaml')
            assert not target.exists()
            target.write_bytes(yaml.safe_dump(body,allow_unicode=True,sort_keys=False).encode('utf-8'))
    providers.write_bytes(text.encode('utf-8'))
    print('configured',list(entries))

if __name__=='__main__':main()
