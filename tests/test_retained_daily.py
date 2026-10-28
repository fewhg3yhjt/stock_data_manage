"""Synthetic transport fixtures validate exact scope and production-write guards."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
import pytest
from stock_data_manage.config.loader import load_input_capabilities
from stock_data_manage.pipeline.inputs import collect_input, collect_task
from stock_data_manage.providers.probes import ProbeEvidence
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.routing.factory import build_input_provider, check_daily_input_qualification
from stock_data_manage.storage.integrity import file_hash, row_hash
from stock_data_manage.storage.metadata import MetadataStore

ROOT=Path(__file__).resolve().parents[1]
CONFIG=ROOT/'config'
CONTEXT={'request':{'symbol':'sh600519','start_date':'2026-10-09','end_date':'2026-10-09'},'config':{'api':'newfqkline','adjust':'qfq'}}


@pytest.fixture
def tmp_path(tmp_path_factory):
    return tmp_path_factory.mktemp("d")


@pytest.fixture
def qualified(tmp_path, monkeypatch):
    from urllib.parse import urlsplit, parse_qs
    def send(session, request, **kwargs):
        param=parse_qs(urlsplit(request.url).query)['param'][0]
        key='day' if '/kline/kline' in request.url else 'qfqday' if param.endswith(',qfq') else 'hfqday'
        response=requests.Response();response.status_code=200
        row=['2026-10-09','10','11','12','9','100']
        if key!='day': row+=['0','0','10','0']
        response._content=json.dumps({'code':0,'data':{'sh600519':{key:[row]}}}).encode()
        response.headers={'Content-Type':'application/json'};response.encoding='utf-8'
        response.url=request.url;response.request=request
        return response
    monkeypatch.setattr(requests.Session,'send',send)
    pairs={}
    units=[]
    for name,api,token in [('forward','newfqkline','qfq'),('none','kline','none'),('backward','newfqkline','hfq')]:
        context={'request':dict(CONTEXT['request']),'config':{'api':api,'adjust':token}}
        live=collect_input(input_id='ASTOCK-002-daily',context=context,config_root=CONFIG,
            output_root=tmp_path/(name+'-live'),mode='live',pacer=RequestPacer(wait=lambda _:None))
        raw=Path(live['run_directory'])/live['raw_manifest']['path']
        current=collect_input(input_id='ASTOCK-002-daily',context=context,config_root=CONFIG,
            output_root=tmp_path/(name+'-current'),mode='replay',replay_manifest=raw,pacer=RequestPacer(wait=lambda _:None))
        assert live['status']==current['status']=='candidate_complete'
        pairs[name]=(context,live,current,raw)
        units.append({'key':'sh600519' if name=='forward' else 'sh600519__'+name,
            'symbol':'sh600519','input_id':'ASTOCK-002-daily','context':context,'adjustment':name,'replay_manifest':str(raw)})
    definition={'task_id':'fixture-retained','dataset':'daily_bar','trade_date':'2026-10-09','asset_type':'stock','units':units}
    result=collect_task(definition=definition,config_root=CONFIG,data_root=tmp_path/'replayed',mode='replay',
        collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert result['status']=='published',result.get('error')
    contract=next(c for c in load_input_capabilities(CONFIG/'providers.yaml') if c.input_id=='ASTOCK-002-daily')
    version=build_input_provider(contract,providers_path=CONFIG/'providers.yaml').capability_version
    for name,(context,live,current,raw) in pairs.items():
        semantic={'passed':True,'instrument_id':'XSHG:600519','asset_type':'stock','parameters':live['parameters'],
            'source_response_hashes':live['output']['source_response_hashes'],'code_version':current['code_version']}
        (tmp_path/(name+'-semantic.json')).write_text(json.dumps(semantic),encoding='utf-8')
        (tmp_path/'contract.xml').write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"/></testsuites>')
        artifacts={'contract_tests':tmp_path/'contract.xml','semantic_check':tmp_path/(name+'-semantic.json'),
            'live_report':Path(live['report_path']),'revalidation_report':Path(current['report_path']),
            'publication_manifest':Path(result['published_manifest']),'raw_manifest':raw}
        proof={'input_id':contract.input_id,'capability_version':version,'asset_type':'stock',
            'allowed_modes':['retained','live'],
            **{k:{'path':str(p),'sha256':file_hash(p)} for k,p in artifacts.items()}}
        proof_path=tmp_path/(name+'-qualification.json');proof_path.write_text(json.dumps(proof),encoding='utf-8')
        now=datetime.now(timezone.utc)
        evidence=ProbeEvidence(provider=contract.provider,endpoint=contract.endpoint,capability_version=version,
            validated_at=now,validation_expires_at=now+timedelta(hours=1),status='complete',eligible_for_selection=True,
            row_count=1,first_key=None,last_key=None,evidence_hash=file_hash(proof_path),adjustment=name,
            request_scope=('input:'+contract.input_id,'parameters:'+row_hash(contract.bind_parameters(context)),
                           'qualification:'+str(proof_path)),field_semantics=('trade_date','open','high','low','close','adjustment'),units=('price:CNY',))
        root=tmp_path/'formal'
        with MetadataStore(root/'metadata/metadata.duckdb') as metadata:
            metadata.save_probe_evidence(evidence,dataset='daily_bar',market='XSHG',asset_type='stock',
                code_prefix='sh600519',adjustment=name)
    return contract,root,definition,pairs['forward'][3],tmp_path/'forward-qualification.json'


def test_exact_live_task_publishes_and_blocks_retained_only_qualification(qualified):
    contract,root,definition,raw,proof=qualified
    [unit.pop('replay_manifest',None) for unit in definition['units']]
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='live',redo='full',
        collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert result['status']=='published'
    assert result['units']['sh600519']['report']['live_http_calls']==1
    p=json.loads(proof.read_text());p['allowed_modes']=['retained'];proof.write_text(json.dumps(p))
    with MetadataStore(root/'metadata/metadata.duckdb') as metadata:
        metadata.connection.execute('UPDATE capability_registry SET evidence_hash=?',[file_hash(proof)])
    with pytest.raises(ValueError,match='execution mode'):
        collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='live',redo='full')


def test_daily_input_uses_yaml_interval_across_requests(qualified,monkeypatch):
    contract,root,definition,raw,proof=qualified
    clock=[0.0];starts=[];send=requests.Session.send
    def observed(session,request,**kwargs):
        starts.append(clock[0])
        return send(session,request,**kwargs)
    monkeypatch.setattr(requests.Session,'send',observed)
    pacer=RequestPacer(clock=lambda:clock[0],wait=lambda n:clock.__setitem__(0,clock[0]+n))
    for n in range(2):
        result=collect_input(input_id=contract.input_id,context=CONTEXT,config_root=CONFIG,
            output_root=root.parent/('paced-'+str(n)),mode='live',evidence_root=root.parent/'empty',
            pacer=pacer,force_fetch=True)
        assert result['status']=='candidate_complete' and result['request_interval_seconds']==0.3
    assert starts==[0.0,0.3]


@pytest.mark.parametrize('damage',['none','hash','source','parameters','code','artifact'])
def test_checkpoint_revalidation_requires_same_source_output_and_current_code(qualified,damage):
    from stock_data_manage.pipeline.inputs import _task_unit_valid
    contract,root,definition,raw,proof=qualified
    with MetadataStore(root.parent/'replayed/metadata/metadata.duckdb') as metadata:
        entry=metadata.load_collection_task(definition['task_id'])['units']['sh600519']
    p=json.loads(proof.read_text());report=Path(p['revalidation_report']['path'])
    current=json.loads(report.read_text())
    current['run_directory']=str(report.parents[4])
    entry['report']['code_files'][0]['sha256']='prior-runtime-code'
    if damage=='source':current['output']['source_response_hashes']=['different-response']
    elif damage=='parameters':current['parameters']['code']='sh600000'
    elif damage=='code':current['code_files'][0]['sha256']='different-code'
    elif damage=='artifact':current['output']['sha256']='different-artifact'
    path=root.parent/'checkpoint-revalidation.json';path.write_text(json.dumps(current))
    entry['revalidation_report']={'path':str(path),'sha256':file_hash(path)}
    if damage=='hash':path.write_text('{}')
    assert _task_unit_valid(entry)==(damage=='none')


@pytest.mark.parametrize('boundary',['allowed','date','api','market','asset','symbol_type','retained','expired','hash','cooldown','code'])
def test_live_market_scope_preserves_evidence_and_request_boundaries(qualified,boundary):
    contract,root,definition,raw,path=qualified
    proof=json.loads(path.read_text())
    proof['live_scope']={'market':'XSHG','asset_type':'stock','symbol_prefixes':['sh6'],'sample_context':CONTEXT}
    path.write_text(json.dumps(proof),encoding='utf-8')
    context={'request':{'symbol':'sh600000','start_date':'2026-10-09','end_date':'2026-10-09'},'config':dict(CONTEXT['config'])}
    asset='stock';mode='live';now=datetime.now(timezone.utc)
    with MetadataStore(root/'metadata/metadata.duckdb') as metadata:
        metadata.connection.execute("UPDATE capability_registry SET evidence_hash=? WHERE adjustment='forward'",[file_hash(path)])
        if boundary=='date':context['request']['start_date']='2026-10-08'
        elif boundary=='api':context['config']={'api':'fqkline','adjust':'qfq'}
        elif boundary=='market':context['request']['symbol']='sz000001'
        elif boundary=='asset':asset='etf'
        elif boundary=='symbol_type':context['request']['symbol']='sh510300'
        elif boundary=='retained':mode='retained'
        elif boundary=='expired':now+=timedelta(days=2)
        elif boundary=='hash':path.write_text('{}')
        elif boundary=='code':
            report=Path(proof['revalidation_report']['path']);r=json.loads(report.read_text())
            r['code_files'][0]['sha256']='bad';report.write_text(json.dumps(r))
            proof['revalidation_report']['sha256']=file_hash(report);path.write_text(json.dumps(proof))
            metadata.connection.execute("UPDATE capability_registry SET evidence_hash=? WHERE adjustment='forward'",[file_hash(path)])
        elif boundary=='cooldown':
            metadata.record_provider_failure(provider=contract.provider,endpoint=contract.endpoint,
                capability_version=proof['capability_version'],dataset='daily_bar',market='XSHG',asset_type='stock',
                failure_class='forbidden',error='fixture',now=now,cooldown_seconds=300,failure_threshold=1)
        result=check_daily_input_qualification(contract,context,asset_type=asset,config_root=CONFIG,
            metadata=metadata,now=now,execution_mode=mode)
    assert result['eligible']==(boundary=='allowed'),result
    if result['eligible']:
        assert result['symbol']=='sh600000' and not result['security_observed']
        assert result['source_response_hashes']==[] and result['sample_response_hashes']


def test_live_throttling_opens_existing_cooldown_and_blocks_next_call(qualified,monkeypatch):
    contract,root,definition,raw,proof=qualified
    [unit.pop('replay_manifest',None) for unit in definition['units']]
    count=[]
    def denied(session,request,**kwargs):
        count.append(request.url)
        response=requests.Response();response.status_code=429;response._content=b'{}'
        response.headers={'Content-Type':'application/json'};response.encoding='utf-8'
        response.url=request.url;response.request=request
        return response
    monkeypatch.setattr(requests.Session,'send',denied)
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='live',redo='full',
        collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert result['status']=='failed' and count
    previous=len(count)
    with pytest.raises(ValueError,match='cooling down'):
        collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='live',redo='resume')
    assert len(count)==previous


def test_selected_live_preflight_checks_only_selected_security(qualified,monkeypatch):
    contract,root,definition,raw,proof=qualified
    definition['task_id']='selected-scope'
    [unit.pop('replay_manifest',None) for unit in definition['units']]
    other={'key':'sh600000','symbol':'sh600000','input_id':'ASTOCK-002-daily',
        'context':{'request':{'symbol':'sh600000','start_date':'2026-10-09','end_date':'2026-10-09'}}}
    definition['units'].append(other)
    import stock_data_manage.routing.factory as factory
    original=factory.check_daily_input_qualification
    checked=[]
    def observed(contract,context,**kwargs):
        checked.append(context['request']['symbol'])
        return original(contract,context,**kwargs)
    monkeypatch.setattr(factory,'check_daily_input_qualification',observed)
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='live',redo='selected',
        symbols=['sh600519'],collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert checked==['sh600519']*6
    assert result['units']['sh600519']['status']=='candidate_complete'
    assert 'sh600000' not in result['units']


def test_qualified_retained_task_publishes_with_zero_network_and_complete_progress(qualified, monkeypatch):
    contract,root,definition,raw,proof=qualified
    def forbidden(*args,**kwargs):raise AssertionError('retained must never request network')
    monkeypatch.setattr(requests.Session,'send',forbidden)
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained',
        collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert result['status']=='published' and result['verification_mode']=='retained'
    assert result['units']['sh600519']['report']['live_http_calls']==0
    summary=json.loads(Path(result['summary']['path']).read_text(encoding='utf-8'))
    assert all(s['percent']==100 for s in summary['progress']['stages'].values())
    assert (root/'raw/2026-10-09/tencent/kline_daily/manifest.ndjson').exists()


def test_interrupted_retained_publication_recovers_all_four_progress_steps(qualified):
    from stock_data_manage.worker.recovery import recover_collection_tasks
    contract,root,definition,raw,proof=qualified
    def interrupt(stage):
        if stage=='before_publish':raise RuntimeError('fixture interruption')
    with pytest.raises(RuntimeError,match='fixture interruption'):
        collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained',failure_hook=interrupt)
    assert recover_collection_tasks(config_root=CONFIG,data_root=root)==(definition['task_id'],)
    with MetadataStore(root/'metadata/metadata.duckdb') as metadata:
        result=metadata.load_collection_task(definition['task_id'])
    assert result['status']=='published'
    assert result['progress']['stages']['publication']['completed']==3
    assert result['progress']['stages']['raw']['reused']==3


@pytest.mark.parametrize('damage',['expired','scope','hash','current_code','cooldown'])
def test_retained_qualification_rejects_invalid_evidence_before_raw_writes(qualified, damage):
    contract,root,definition,raw,proof=qualified
    with MetadataStore(root/'metadata/metadata.duckdb') as metadata:
        if damage=='expired':metadata.connection.execute("UPDATE capability_registry SET validation_expires_at=?",[datetime.now(timezone.utc)-timedelta(seconds=1)])
        elif damage=='scope':definition['units'][0]['context']={'request':{'symbol':'sh600519','start_date':'2026-10-08','end_date':'2026-10-09'},'config':dict(CONTEXT['config'])}
        elif damage=='hash':proof.write_text('{}')
        elif damage=='current_code':
            p=json.loads(proof.read_text());current=Path(p['revalidation_report']['path']);r=json.loads(current.read_text());r['code_files'][0]['sha256']='bad'
            current.write_text(json.dumps(r));p['revalidation_report']['sha256']=file_hash(current);proof.write_text(json.dumps(p))
            metadata.connection.execute('UPDATE capability_registry SET evidence_hash=?',[file_hash(proof)])
        else:
            metadata.record_provider_failure(provider=contract.provider,endpoint=contract.endpoint,
                capability_version=build_input_provider(contract,providers_path=CONFIG/'providers.yaml').capability_version,
                dataset='daily_bar',market='XSHG',asset_type='stock',failure_class='forbidden',error='fixture',
                now=datetime.now(timezone.utc),cooldown_seconds=300,failure_threshold=1)
    reasons = {'expired':'expired or not yet valid','scope':'scope differs','hash':'hash differs',
        'current_code':'code or configuration changed','cooldown':'cooling down'}
    with pytest.raises(ValueError,match=reasons[damage]):
        collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained')
    assert not list((root/'raw').rglob('*a-qfq*.bin'))
    assert not (root/'canonical/2026-10-09/daily_bar/stock/manifest.json').exists()
