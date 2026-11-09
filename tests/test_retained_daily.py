"""Synthetic transport fixtures validate exact scope and production-write guards."""
import json
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import requests
import pytest
from test_daily_collection_e2e import batch_entries
from stock_data_manage.config.loader import load_input_capabilities
from stock_data_manage.domain import AssetType
from stock_data_manage.pipeline.inputs import collect_input, collect_task
from stock_data_manage.providers.probes import ProbeEvidence
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.routing.factory import build_input_provider, check_daily_input_qualification
from stock_data_manage.storage.integrity import file_hash, row_hash
from stock_data_manage.storage.daily import DailyBarStore
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
        key='day' if '/kline/kline' in request.url or param.endswith(',none') else 'qfqday' if param.endswith(',qfq') else 'hfqday'
        response=requests.Response();response.status_code=200
        row=['2026-10-09','10','11','12','9','100']
        if '/newfqkline/get' in request.url: row+=['0','0','10','0']
        response._content=json.dumps({'code':0,'data':{'sh600519':{key:[row]}}}).encode()
        response.headers={'Content-Type':'application/json'};response.encoding='utf-8'
        response.url=request.url;response.request=request
        return response
    monkeypatch.setattr(requests.Session,'send',send)
    pairs={}
    units=[]
    for name,api,token in [('forward','newfqkline','qfq'),('none','newfqkline','none'),('backward','newfqkline','hfq')]:
        context={'request':dict(CONTEXT['request']),'config':{'api':api,'adjust':token}}
        live=collect_input(input_id='ASTOCK-002-daily',context=context,config_root=CONFIG,
            output_root=tmp_path/(name+'-live'),mode='live',security_asset_type='stock',pacer=RequestPacer(wait=lambda _:None))
        raw=Path(live['run_directory'])/live['raw_manifest']['path']
        current=collect_input(input_id='ASTOCK-002-daily',context=context,config_root=CONFIG,
            output_root=tmp_path/(name+'-current'),mode='replay',replay_manifest=raw,security_asset_type='stock',pacer=RequestPacer(wait=lambda _:None))
        assert live['status']==current['status']=='candidate_complete'
        pairs[name]=(context,live,current,raw)
        units.append({'key':'sh600519' if name=='forward' else 'sh600519__'+name,
            'symbol':'sh600519','input_id':'ASTOCK-002-daily','context':context,'adjustment':name,'replay_manifest':str(raw)})
    definition={'task_id':'fixture-retained','dataset':'daily_bar','trade_date':'2026-10-09','asset_type':'stock','units':units}
    result=collect_task(definition=definition,config_root=CONFIG,data_root=tmp_path/'replayed',mode='replay',
        collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert result['status']=='published',result.get('error')
    evidence_db=tmp_path/'replayed'/'metadata'/'metadata.sqlite3'
    with MetadataStore(evidence_db,read_only=True) as metadata:
        receipt=next(row for row in DailyBarStore(metadata.connection).publication_receipts(
            task_id=definition['task_id'],trade_date=date.fromisoformat(definition['trade_date']),
            asset_type=AssetType.STOCK)
            if row['publication_id']==result['publication_id'])
    publication_ref={'database_path':str(evidence_db),'task_id':definition['task_id'],
        'publication_id':receipt['publication_id'],'request_hash':receipt['request_hash']}
    contract=next(c for c in load_input_capabilities(CONFIG/'providers.yaml') if c.input_id=='ASTOCK-002-daily')
    version=build_input_provider(contract,providers_path=CONFIG/'providers.yaml').capability_version
    for name,(context,live,current,raw) in pairs.items():
        semantic={'passed':True,'instrument_id':'XSHG:600519','asset_type':'stock','parameters':live['parameters'],
            'source_response_hashes':live['output']['source_response_hashes'],'code_version':current['code_version']}
        (tmp_path/(name+'-semantic.json')).write_text(json.dumps(semantic),encoding='utf-8')
        (tmp_path/'contract.xml').write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"/></testsuites>')
        artifacts={'contract_tests':tmp_path/'contract.xml','semantic_check':tmp_path/(name+'-semantic.json'),
            'live_report':Path(live['report_path']),'revalidation_report':Path(current['report_path']),
            'raw_manifest':raw}
        proof={'input_id':contract.input_id,'capability_version':version,'asset_type':'stock',
            'allowed_modes':['retained','live'],
            'publication_ref':publication_ref,
            **{k:{'path':str(p),'sha256':file_hash(p)} for k,p in artifacts.items()}}
        proof_path=tmp_path/(name+'-qualification.json');proof_path.write_text(json.dumps(proof),encoding='utf-8')
        now=datetime.now(timezone.utc)
        evidence=ProbeEvidence(provider=contract.provider,endpoint=contract.endpoint,capability_version=version,
            validated_at=now,validation_expires_at=now+timedelta(hours=1),status='complete',eligible_for_selection=True,
            row_count=1,first_key=None,last_key=None,evidence_hash=file_hash(proof_path),adjustment=name,
            request_scope=('input:'+contract.input_id,'parameters:'+row_hash(contract.bind_parameters(context)),
                           'qualification:'+str(proof_path)),field_semantics=('trade_date','open','high','low','close','adjustment'),units=('price:CNY',))
        root=tmp_path/'formal'
        with MetadataStore(root/'metadata/metadata.sqlite3') as metadata:
            metadata.save_probe_evidence(evidence,dataset='daily_bar',market='XSHG',asset_type='stock',
                code_prefix='sh600519',adjustment=name)
    return contract,root,definition,pairs['forward'][3],tmp_path/'forward-qualification.json'


@pytest.mark.parametrize('symbol', ['sh000001', 'sz399001'])
def test_typed_index_daily_input_preserves_exchange_and_retains_response(tmp_path, monkeypatch, symbol):
    from urllib.parse import parse_qs, urlsplit
    from stock_data_manage.storage.raw import RawObjectStore
    calls = []

    def send(session, request, **kwargs):
        param = parse_qs(urlsplit(request.url).query)['param'][0]
        calls.append(param)
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({'code': 0, 'data': {symbol: {
            'day': [['2026-10-09', '3000', '3001', '3002', '2999', '100', {}, '0', '200', '0', '0']]
        }}}).encode()
        response.headers = {'Content-Type': 'application/json'}
        response.encoding = 'utf-8'
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.Session, 'send', send)
    context = {'request': {'symbol': symbol, 'start_date': '2026-10-09', 'end_date': '2026-10-09'},
               'config': {'api': 'newfqkline', 'adjust': 'none'}}
    report = collect_input(input_id='ASTOCK-002-daily', context=context, config_root=CONFIG,
                           output_root=tmp_path/'index', mode='live', security_asset_type='index',
                           pacer=RequestPacer(wait=lambda _: None))
    assert report['status'] == 'candidate_complete', report.get('error')
    assert calls == [f'{symbol},day,2026-10-09,2026-10-09,640,none']
    raw = Path(report['run_directory'])/report['raw_manifest']['path']
    RawObjectStore.verify_manifest(raw)
    rows = json.loads((Path(report['run_directory'])/report['output']['path']).read_text(encoding='utf-8'))
    assert rows[0]['instrument_id'] == ('XSHG:' if symbol.startswith('sh') else 'XSHE:') + symbol[2:]
    assert rows[0]['open'] == '3000' and rows[0]['volume'] is None


def test_typed_index_daily_input_rejects_adjusted_capture_before_network(tmp_path, monkeypatch):
    monkeypatch.setattr(requests.Session, 'send', lambda *a, **kw: pytest.fail('index adjustment reached network'))
    report = collect_input(input_id='ASTOCK-002-daily',
        context={'request': {'symbol': 'sh000001', 'start_date': '2026-10-09', 'end_date': '2026-10-09'},
                 'config': {'api': 'newfqkline', 'adjust': 'qfq'}},
        config_root=CONFIG, output_root=tmp_path/'adjusted-index', mode='live', security_asset_type='index')
    assert report['status'] == 'failed'
    assert 'only without adjustment' in report['error']


def test_exact_live_task_publishes_and_blocks_retained_only_qualification(qualified,monkeypatch):
    contract,root,definition,raw,proof=qualified
    [unit.pop('replay_manifest',None) for unit in definition['units']]
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='live',redo='full',
        collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert result['status']=='published'
    assert batch_entries(result)['sh600519']['live_http_calls']==1
    with MetadataStore(root/'metadata/metadata.sqlite3',read_only=True) as metadata:
        initial_receipts=DailyBarStore(metadata.connection).publication_receipts(task_id=definition['task_id'])
    assert len(initial_receipts)==1
    p=json.loads(proof.read_text());p['allowed_modes']=['retained'];proof.write_text(json.dumps(p))
    with MetadataStore(root/'metadata/metadata.sqlite3') as metadata:
        metadata.connection.execute("UPDATE capability_registry SET evidence_hash=? WHERE adjustment='forward'",[file_hash(proof)])
    calls=[];send=requests.Session.send
    def observed(session,request,**kwargs):
        calls.append(request.url)
        return send(session,request,**kwargs)
    monkeypatch.setattr(requests.Session,'send',observed)
    blocked=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='live',redo='full')
    assert blocked['status']=='failed' and not blocked['complete_today'] and calls==[]
    reasons=json.loads(Path(blocked['qualification_ref']['path']).read_text(encoding='utf-8'))
    assert 'execution mode' in ' '.join(reasons['ASTOCK-002-daily']['reasons'])
    with MetadataStore(root/'metadata/metadata.sqlite3',read_only=True) as metadata:
        assert DailyBarStore(metadata.connection).publication_receipts(task_id=definition['task_id'])==initial_receipts


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


@pytest.mark.parametrize('boundary',['allowed','date','api','market','asset','symbol_type','retained','expired','hash','cooldown','code'])
def test_live_market_scope_preserves_evidence_and_request_boundaries(qualified,boundary):
    contract,root,definition,raw,path=qualified
    proof=json.loads(path.read_text())
    proof['live_scope']={'market':'XSHG','asset_type':'stock','symbol_prefixes':['sh6'],'sample_context':CONTEXT}
    path.write_text(json.dumps(proof),encoding='utf-8')
    context={'request':{'symbol':'sh600000','start_date':'2026-10-09','end_date':'2026-10-09'},'config':dict(CONTEXT['config'])}
    asset='stock';mode='live';now=datetime.now(timezone.utc)
    with MetadataStore(root/'metadata/metadata.sqlite3') as metadata:
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
            r['code_files']=[{'path':'missing-source.py','sha256':'bad'}];report.write_text(json.dumps(r))
            proof['revalidation_report']['sha256']=file_hash(report);path.write_text(json.dumps(proof))
            metadata.connection.execute("UPDATE capability_registry SET evidence_hash=? WHERE adjustment='forward'",[file_hash(path)])
        elif boundary=='cooldown':
            metadata.record_provider_failure(provider=contract.provider,endpoint=contract.endpoint,
                capability_version=proof['capability_version'],dataset='daily_bar',market='XSHG',asset_type='stock',
                failure_class='forbidden',error='fixture',now=now,cooldown_seconds=300,failure_threshold=1)
        result=check_daily_input_qualification(contract,context,asset_type=asset,config_root=CONFIG,
            metadata=metadata,now=now,execution_mode=mode)
    assert result['eligible']==(boundary in {'allowed','code'}),result
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
    blocked=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='live',redo='resume')
    assert blocked['status']=='failed' and not blocked['complete_today']
    reasons=json.loads(Path(blocked['qualification_ref']['path']).read_text(encoding='utf-8'))
    assert 'cooling down' in ' '.join(reasons['ASTOCK-002-daily']['reasons'])
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
    assert checked==['sh600519']*3
    assert batch_entries(result)['sh600519']['status']=='candidate_complete'
    assert 'sh600000' not in batch_entries(result)


def test_qualified_retained_task_publishes_with_zero_network_and_complete_progress(qualified, monkeypatch):
    contract,root,definition,raw,proof=qualified
    def forbidden(*args,**kwargs):raise AssertionError('retained must never request network')
    monkeypatch.setattr(requests.Session,'send',forbidden)
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained',
        collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert result['status']=='published' and result['verification_mode']=='retained'
    assert len(result['publication_refs'])==1
    assert result['publication_id']==result['publication_refs'][0]['publication_id']
    assert result['publication_refs'][0]['row_count']==1
    assert result['current_partition_count']==result['publication_refs'][0]['current_partition_count']==1
    assert batch_entries(result)['sh600519']['live_http_calls']==0
    summary=json.loads(Path(result['summary']['path']).read_text(encoding='utf-8'))
    assert all(s['percent']==100 for s in summary['progress']['stages'].values())
    assert (root/'raw/2026-10-09/tencent/kline_daily/manifest.ndjson').exists()
    with MetadataStore(root/'metadata/metadata.sqlite3',read_only=True) as metadata:
        record=DailyBarStore(metadata.connection).get('XSHG:600519',date(2026,10,9))
        assert record is not None and record.forward.close==Decimal('11')
        receipts=DailyBarStore(metadata.connection).publication_receipts(task_id=definition['task_id'])
        assert len(receipts)==1 and receipts[0]['publication_id']==result['publication_id']


@pytest.mark.parametrize('task_status',['failed','committing'])
def test_daily_qualification_uses_committed_receipt_when_task_is_partial(qualified,task_status):
    contract,root,definition,raw,proof=qualified
    evidence_db=root.parent/'replayed'/'metadata'/'metadata.sqlite3'
    with MetadataStore(evidence_db) as metadata:
        state=metadata.load_collection_task(definition['task_id'])
        assert state['status']=='published'
        state['status']=task_status
        metadata.connection.execute('UPDATE collection_task SET status=?,state_json=? WHERE task_id=?',
            [task_status,json.dumps(state,ensure_ascii=False,default=str),definition['task_id']])
        receipt=DailyBarStore(metadata.connection).publication_receipts(task_id=definition['task_id'])
        assert len(receipt)==1 and receipt[0]['written_count']==1
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained',
        collector_options={'pacer':RequestPacer(wait=lambda _:None)})
    assert result['status']=='published'


def test_interrupted_retained_receipt_commit_recovers_without_republishing(qualified):
    from stock_data_manage.worker.recovery import recover_collection_tasks
    contract,root,definition,raw,proof=qualified
    def interrupt(stage):
        if stage=='after_receipt_commit':raise RuntimeError('fixture receipt interruption')
    with pytest.raises(RuntimeError,match='fixture receipt interruption'):
        collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained',failure_hook=interrupt)
    assert recover_collection_tasks(config_root=CONFIG,data_root=root)==(definition['task_id'],)
    with MetadataStore(root/'metadata/metadata.sqlite3') as metadata:
        result=metadata.load_collection_task(definition['task_id'])
    assert result['status']=='published'
    assert result['progress']['stages']['publication']['completed']==1
    assert result['progress']['stages']['raw']['reused']==3
    with MetadataStore(root/'metadata/metadata.sqlite3',read_only=True) as metadata:
        receipts=DailyBarStore(metadata.connection).publication_receipts(task_id=definition['task_id'])
        record=DailyBarStore(metadata.connection).get('XSHG:600519',date(2026,10,9))
    assert len(receipts)==1 and receipts[0]['written_count']==1
    assert record is not None and record.forward.close==Decimal('11')


@pytest.mark.parametrize('damage',['expired','scope','hash','configuration','cooldown',
    'missing_receipt','wrong_request_hash','candidate','raw','revalidation_report_hash',
    'revalidation_source_hashes','revalidation_parameters'])
def test_retained_qualification_rejects_invalid_evidence_before_raw_writes(qualified, damage):
    contract,root,definition,raw,proof=qualified
    with MetadataStore(root/'metadata/metadata.sqlite3') as metadata:
        if damage.startswith('revalidation_'):
            baseline=check_daily_input_qualification(contract,CONTEXT,asset_type='stock',config_root=CONFIG,
                metadata=metadata,now=datetime.now(timezone.utc),execution_mode='retained')
            assert baseline['eligible'],baseline
        if damage=='expired':metadata.connection.execute("UPDATE capability_registry SET validation_expires_at=?",[datetime.now(timezone.utc)-timedelta(seconds=1)])
        elif damage=='scope':definition['units'][0]['context']={'request':{'symbol':'sh600519','start_date':'2026-10-08','end_date':'2026-10-09'},'config':dict(CONTEXT['config'])}
        elif damage=='hash':proof.write_text('{}')
        elif damage.startswith('revalidation_'):
            p=json.loads(proof.read_text());current=Path(p['revalidation_report']['path'])
            if damage=='revalidation_report_hash':
                current.write_text(current.read_text(encoding='utf-8')+' ',encoding='utf-8')
            else:
                report=json.loads(current.read_text(encoding='utf-8'))
                if damage=='revalidation_source_hashes':
                    report['output']['source_response_hashes']=['0'*64]
                else:
                    report['parameters']['code']='sh600000'
                current.write_text(json.dumps(report),encoding='utf-8')
                p['revalidation_report']['sha256']=file_hash(current)
                proof.write_text(json.dumps(p),encoding='utf-8')
                metadata.connection.execute('UPDATE capability_registry SET evidence_hash=? WHERE adjustment=?',
                    [file_hash(proof),'forward'])
        elif damage=='configuration':
            p=json.loads(proof.read_text());current=Path(p['revalidation_report']['path']);r=json.loads(current.read_text());r['config_files'][0]['sha256']='bad'
            current.write_text(json.dumps(r));p['revalidation_report']['sha256']=file_hash(current);proof.write_text(json.dumps(p))
            metadata.connection.execute('UPDATE capability_registry SET evidence_hash=?',[file_hash(proof)])
        elif damage in {'missing_receipt','wrong_request_hash','candidate','raw'}:
            p=json.loads(proof.read_text())
            publication=p['publication_ref']
            if damage=='missing_receipt':
                publication['publication_id']='missing-publication'
            elif damage=='wrong_request_hash':
                publication['request_hash']='0'*64
            else:
                with MetadataStore(publication['database_path'],read_only=True) as evidence_metadata:
                    receipt=DailyBarStore(evidence_metadata.connection).publication_receipts(
                        task_id=publication['task_id'],trade_date=date.fromisoformat(definition['trade_date']),
                        asset_type=AssetType.STOCK)[0]
                    task_state=evidence_metadata.load_collection_task(publication['task_id'])
                run_root=Path(task_state['definition_ref']['path']).parent
                descriptor_ref=json.loads(receipt['candidate_ref'])
                descriptor_path=run_root/descriptor_ref['path']
                descriptor=json.loads(descriptor_path.read_text(encoding='utf-8'))
                if damage=='candidate':
                    target=run_root/descriptor['candidate']['path']
                    with target.open('ab') as stream: stream.write(b'corruption')
                else:
                    target=Path(descriptor['raw_evidence'][0]['path'])
                    events=[json.loads(line) for line in target.read_text(encoding='utf-8').splitlines() if line.strip()]
                    events[-1]['url']=str(events[-1].get('url',''))+'?changed=1'
                    target.write_text('\n'.join(json.dumps(event,ensure_ascii=False) for event in events)+'\n',encoding='utf-8')
            proof.write_text(json.dumps(p),encoding='utf-8')
            metadata.connection.execute('UPDATE capability_registry SET evidence_hash=? WHERE adjustment=?',
                [file_hash(proof),'forward'])
        else:
            metadata.record_provider_failure(provider=contract.provider,endpoint=contract.endpoint,
                capability_version=build_input_provider(contract,providers_path=CONFIG/'providers.yaml').capability_version,
                dataset='daily_bar',market='XSHG',asset_type='stock',failure_class='forbidden',error='fixture',
                now=datetime.now(timezone.utc),cooldown_seconds=300,failure_threshold=1)
    reasons = {'expired':'expired or not yet valid','scope':'single frozen date','hash':'hash differs',
        'configuration':'configuration changed','cooldown':'cooling down',
        'missing_receipt':'daily publication receipt is missing or ambiguous',
        'wrong_request_hash':'daily publication receipt request hash differs',
        'candidate':'daily publication candidate is missing or changed',
        'raw':'daily publication original response evidence changed',
        'revalidation_report_hash':'daily qualification layer hash differs: revalidation_report',
        'revalidation_source_hashes':'daily revalidation does not retain live response bytes',
        'revalidation_parameters':'daily Provider report scope differs'}
    if damage=='scope':
        with pytest.raises(ValueError,match='frozen logical security, request code and single date'):
            collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained')
    else:
        result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained')
        assert result['status']=='failed' and not result['complete_today']
        qualification=json.loads(Path(result['qualification_ref']['path']).read_text(encoding='utf-8'))
        assert reasons[damage] in ' '.join(qualification['ASTOCK-002-daily']['reasons'])
    assert not list((root/'raw').rglob('*a-qfq*.bin'))
    response_events=[]
    if (root/'raw').exists():
        for manifest in (root/'raw').rglob('manifest.ndjson'):
            for line in manifest.read_text(encoding='utf-8').splitlines():
                if line.strip():
                    event=json.loads(line)
                    if event.get('event')=='http_response' and event.get('status_code')==200:
                        response_events.append(event)
    assert response_events==[]
    with MetadataStore(root/'metadata/metadata.sqlite3',read_only=True) as metadata:
        rows,total=DailyBarStore(metadata.connection).list_records(asset_type=AssetType.STOCK,
            date_from=date(2026,10,9),date_to=date(2026,10,9),limit=10)
        assert rows==() and total==0
        assert DailyBarStore(metadata.connection).publication_receipts(task_id=definition['task_id'])==()


def test_daily_qualification_rejects_candidate_without_committed_receipt(qualified):
    contract,root,definition,raw,proof=qualified
    candidate_definition=deepcopy(definition)
    candidate_definition['task_id']='candidate-only'
    evidence_db=root.parent/'replayed'/'metadata'/'metadata.sqlite3'
    with MetadataStore(evidence_db,read_only=True) as evidence_metadata:
        owners=DailyBarStore(evidence_metadata.connection).current_owners({
            'dataset':'daily_bar','frequency':'1d','asset_types':['stock'],
            'securities_by_date':{definition['trade_date']:['XSHG:600519']}})
    assert len(owners)==1
    def interrupt(stage):
        if stage=='after_candidate_before_checkpoint':
            raise RuntimeError('fixture candidate without receipt')
    with pytest.raises(RuntimeError,match='candidate without receipt'):
        collect_task(definition=candidate_definition,config_root=CONFIG,data_root=root.parent/'replayed',
            mode='replay',failure_hook=interrupt,overwrite=True,confirmed_conflicts=owners)
    with MetadataStore(evidence_db,read_only=True) as evidence_metadata:
        state=evidence_metadata.load_collection_task('candidate-only')
        assert state is not None
        assert DailyBarStore(evidence_metadata.connection).publication_receipts(task_id='candidate-only')==()
    p=json.loads(proof.read_text())
    p['publication_ref']['task_id']='candidate-only'
    p['publication_ref']['publication_id']='candidate-only-publication'
    proof.write_text(json.dumps(p),encoding='utf-8')
    with MetadataStore(root/'metadata/metadata.sqlite3') as metadata:
        metadata.connection.execute('UPDATE capability_registry SET evidence_hash=? WHERE adjustment=?',
            [file_hash(proof),'forward'])
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained')
    assert result['status']=='failed' and not result['complete_today']
    qualification=json.loads(Path(result['qualification_ref']['path']).read_text(encoding='utf-8'))
    assert 'daily publication receipt is missing or ambiguous' in ' '.join(
        qualification['ASTOCK-002-daily']['reasons'])
    assert not list((root/'raw').rglob('*a-qfq*.bin'))
    with MetadataStore(root/'metadata/metadata.sqlite3',read_only=True) as metadata:
        assert DailyBarStore(metadata.connection).publication_receipts(task_id=definition['task_id'])==()


def test_processing_resume_uses_existing_capture_qualification_without_renewing_it(qualified, monkeypatch):
    contract,root,definition,raw,proof=qualified
    def interrupt(stage):
        if stage=='after_candidate_before_checkpoint':
            raise RuntimeError('synthetic processing interruption')
    with pytest.raises(RuntimeError):
        collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained',failure_hook=interrupt)
    with MetadataStore(root/'metadata/metadata.sqlite3') as metadata:
        state=metadata.load_collection_task(definition['task_id'])
        original=dict(state['qualification_ref'])
        metadata.connection.execute('UPDATE capability_registry SET validation_expires_at=?',[datetime.now(timezone.utc)-timedelta(seconds=1)])
    def forbidden(*args,**kwargs):raise AssertionError('processing resume must never send HTTP')
    monkeypatch.setattr(requests.Session,'send',forbidden)
    result=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained')
    assert result['status']=='published' and result['qualification_ref']==original
    assert file_hash(Path(original['path']))==original['sha256']


def test_existing_archive_preserves_retained_qualification_references(qualified):
    from stock_data_manage.worker.recovery import archive_published_task
    from stock_data_manage.config.loader import load_storage_paths
    contract,root,definition,raw,proof=qualified
    evidence_root=root.parent/'replayed'
    evidence_paths=load_storage_paths(CONFIG,data_root=evidence_root)
    with MetadataStore(evidence_paths['metadata_path']) as evidence_metadata:
        evidence_task=evidence_metadata.load_collection_task(definition['task_id'])
        archive_published_task(Path(evidence_task['definition_ref']['path']).parent,metadata=evidence_metadata,
            **{name:evidence_paths[name] for name in ['workspace_root','archive_root','canonical_root','raw_root']})
    state=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained')
    paths=load_storage_paths(CONFIG,data_root=root)
    with MetadataStore(paths['metadata_path']) as metadata:
        archive_published_task(Path(state['definition_ref']['path']).parent,metadata=metadata,
            **{name:paths[name] for name in ['workspace_root','archive_root','canonical_root','raw_root']})
    resumed=collect_task(definition=definition,config_root=CONFIG,data_root=root,mode='retained')
    assert resumed['no_op']
    for entry in batch_entries(resumed).values():
        reference=entry['qualification_ref']
        assert file_hash(Path(reference['path']))==reference['sha256']


@pytest.mark.parametrize('grant_scope',['valid','wrong_market','wrong_asset','short_prefix'])
def test_live_batch_checks_each_market_grant_once_then_matches_all_units(qualified, monkeypatch, grant_scope):
    import stock_data_manage.routing.factory as factory
    contract,root,definition,raw,proof=qualified
    units=[]
    with MetadataStore(root/'metadata/metadata.sqlite3') as metadata:
        for unit in definition['units']:
            name=unit['adjustment']
            path=root.parent/(name+'-qualification.json')
            document=json.loads(path.read_text())
            document['live_scope']={'market':'XSHG','asset_type':'stock','symbol_prefixes':['sh6'],'sample_context':unit['context']}
            if grant_scope=='wrong_market':document['live_scope']['market']='XSHE'
            elif grant_scope=='wrong_asset':document['live_scope']['asset_type']='etf'
            elif grant_scope=='short_prefix':document['live_scope']['symbol_prefixes']=['sh']
            path.write_text(json.dumps(document))
            metadata.connection.execute('UPDATE capability_registry SET evidence_hash=? WHERE adjustment=?',[file_hash(path),name])
            for symbol in ['sh600519','sh600000']:
                current=deepcopy(unit)
                current.update(key=symbol+'__'+name,symbol=symbol,asset_type='stock')
                current['context']['request']['symbol']=symbol
                units.append(current)
        checked=[]
        original=factory.check_daily_input_qualification
        def observed(*args,**kwargs):
            checked.append(args[1]['request']['symbol'])
            return original(*args,**kwargs)
        monkeypatch.setattr(factory,'check_daily_input_qualification',observed)
        if grant_scope!='valid':
            with pytest.raises(ValueError,match='qualification blocked'):
                factory.check_daily_batch_qualification(contract,units,config_root=CONFIG,metadata=metadata,
                    now=datetime.now(timezone.utc),execution_mode='live')
            return
        result=factory.check_daily_batch_qualification(contract,units,config_root=CONFIG,metadata=metadata,
            now=datetime.now(timezone.utc),execution_mode='live')
    assert len(result['grants'])==3 and len(result['unit_grants'])==6
    assert checked==['sh600519']*3
