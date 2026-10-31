"""Batch boundaries and crash recovery, using explicitly synthetic archived HTTP bytes."""
import json
from pathlib import Path
import pytest

from test_daily_collection_e2e import source_task, CONFIG, DAY
from stock_data_manage.pipeline.inputs import collect_task
from stock_data_manage.providers.tencent.daily import TencentDailyProvider
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.config.loader import load_storage_paths
from stock_data_manage.storage.integrity import file_hash


def run(definition, root, **kwargs):
    return collect_task(definition=definition, config_root=CONFIG, data_root=root, mode='replay', **kwargs)


def test_entire_batch_captured_before_any_parse_and_processing_has_no_http(tmp_path, monkeypatch):
    definition = source_task(tmp_path, symbols=('sh600001', 'sz000001'))
    calls = []
    capture, parse = TencentDailyProvider.capture_window, TencentDailyProvider.parse_window
    def captured(self, *args, **kwargs):
        calls.append('capture')
        return capture(self, *args, **kwargs)
    def parsed(self, *args, **kwargs):
        assert calls.count('capture') == 6
        calls.append('parse')
        return parse(self, *args, **kwargs)
    monkeypatch.setattr(TencentDailyProvider, 'capture_window', captured)
    monkeypatch.setattr(TencentDailyProvider, 'parse_window', parsed)
    result = run(definition, tmp_path / 'run')
    assert result['status'] == 'published'
    assert result['data_date'] == DAY
    assert calls == ['capture'] * 6 + ['parse'] * 6
    assert 'units' not in result and 'definition' not in result
    work = Path(result['definition_ref']['path']).parent
    assert (work / 'summary.json').stat().st_size < 12000
    assert len(list(work.rglob('*.manifest.json'))) == 6
    for path in work.rglob('*.report.json'):
        assert json.loads(path.read_text(encoding='utf-8'))['processing_http_calls'] == 0
    plan = work / 'task.json'
    stamp = plan.stat().st_mtime_ns
    assert run(definition, tmp_path / 'run')['no_op']
    assert plan.stat().st_mtime_ns == stamp


@pytest.mark.parametrize('boundary', ['after_raw_before_unit_index', 'after_candidate_before_checkpoint',
    'before_publish', 'after_data_replace', 'after_publish', 'after_raw_promote'])
def test_resume_at_real_stage_boundaries(tmp_path, boundary):
    definition = source_task(tmp_path)
    def failure(stage):
        if stage == boundary:
            raise RuntimeError(boundary)
    with pytest.raises(RuntimeError, match=boundary):
        run(definition, tmp_path / 'run', failure_hook=failure)
    result = run(definition, tmp_path / 'run')
    assert result['status'] == 'published'
    assert result['row_count'] == 1
    for path in Path(result['definition_ref']['path']).parent.rglob('*.manifest.json'):
        entry = json.loads(path.read_text(encoding='utf-8'))
        assert Path(entry['raw_manifest']).is_file()


def test_failed_required_stock_group_blocks_only_its_asset_partition(tmp_path):
    definition = source_task(tmp_path, corrupt='forward')
    definition.pop('asset_type', None)
    definition.pop('task', None)
    definition['asset_types'] = ['stock', 'etf']
    definition['universe'] = 'static'
    definition['units'][0]['asset_type'] = 'stock'
    etf = source_task(tmp_path, symbols=('sh510300',))['units'][0]
    etf['asset_type'] = 'etf'
    definition['units'].append(etf)
    result = run(definition, tmp_path / 'run')
    assert result['status'] == 'failed'
    assert result['partitions']['stock']['status'] == 'blocked'
    assert result['partitions']['etf']['status'] == 'published'
    paths = load_storage_paths(CONFIG, data_root=tmp_path / 'run')
    assert not (paths['canonical_root'] / DAY / 'daily_bar' / 'stock' / 'data.parquet').exists()


def test_frozen_request_rejects_scope_changes_and_old_checkpoints(tmp_path):
    definition = source_task(tmp_path)
    run(definition, tmp_path / 'run')
    with pytest.raises(ValueError, match='frozen scope'):
        run({**definition, 'trade_date': '2026-10-08'}, tmp_path / 'run')
    paths = load_storage_paths(CONFIG, data_root=tmp_path / 'run')
    with MetadataStore(paths['metadata_path']) as metadata:
        state = metadata.load_collection_task(definition['task_id'])
        state.pop('format_version')
        metadata.save_collection_task(state)
    with pytest.raises(ValueError, match='read-only'):
        run(definition, tmp_path / 'run')


@pytest.mark.parametrize('kind', ['raw_corrupt', 'raw_missing', 'candidate_corrupt', 'index_missing'])
def test_published_resume_rechecks_evidence_and_never_overwrites_good_publication(tmp_path, kind):
    from test_daily_collection_e2e import batch_entries
    from stock_data_manage.storage.raw import RawObjectStore
    definition = source_task(tmp_path)
    state = run(definition, tmp_path/'run')
    canonical = Path(state['published_manifest']).with_name('data.parquet')
    before = file_hash(canonical)
    entry = next(iter(batch_entries(state).values()))
    if kind == 'candidate_corrupt':
        Path(entry['artifact']).write_bytes(b'corrupt synthetic candidate')
    elif kind == 'index_missing':
        next(Path(state['definition_ref']['path']).parent.rglob('*.manifest.json')).unlink()
    else:
        manifest = Path(entry['raw_manifest'])
        event = json.loads(manifest.read_text(encoding='utf-8').splitlines()[0])
        body = manifest.parent/event['body_storage']
        body.unlink() if kind == 'raw_missing' else body.write_bytes(b'corrupt synthetic body')
    with pytest.raises((ValueError, OSError)):
        run(definition, tmp_path/'run')
    assert file_hash(canonical) == before


def test_pending_commit_blocks_another_task_before_capture(tmp_path, monkeypatch):
    definition = source_task(tmp_path)
    def stop(stage):
        if stage == 'before_publish':
            raise RuntimeError('power loss')
    with pytest.raises(RuntimeError):
        run(definition, tmp_path/'run', failure_hook=stop)
    def forbidden(*args, **kwargs):
        raise AssertionError('pending commit must block capture')
    monkeypatch.setattr(TencentDailyProvider, 'capture_window', forbidden)
    with pytest.raises(ValueError, match='pending commit'):
        run({**definition, 'task_id':'other'}, tmp_path/'run')


def test_partial_publication_keeps_missing_visible_without_backwards_network_flow(tmp_path, monkeypatch):
    import shutil
    import yaml
    config = tmp_path/'config'
    shutil.copytree(CONFIG, config)
    path = config/'datasets/daily_bar.yaml'
    doc = yaml.safe_load(path.read_text(encoding='utf-8'))
    doc['dataset']['publication'].update(max_missing_ratio=0.5, max_missing_count=1)
    path.write_text(yaml.safe_dump(doc), encoding='utf-8')
    definition = source_task(tmp_path, symbols=('sh600001','sh600002'))
    bad = Path(definition['units'][1]['replay_manifests']['forward'])
    event = json.loads(bad.read_text(encoding='utf-8').splitlines()[0])
    import hashlib
    body = bad.parent/event['body_storage']
    body.write_bytes(b'<html>synthetic invalid source</html>')
    event['body_sha256'] = hashlib.sha256(body.read_bytes()).hexdigest()
    event['body_bytes'] = body.stat().st_size
    bad.write_text(json.dumps(event)+'\n', encoding='utf-8')
    result = collect_task(definition=definition, config_root=config, data_root=tmp_path/'run', mode='replay')
    assert result['status'] == 'published' and not result['complete_today']
    assert result['row_count'] == 1 and result['missing_count'] == 1
    def forbidden(*args, **kwargs):
        raise AssertionError('processing failures must not automatically refetch')
    monkeypatch.setattr(TencentDailyProvider, 'capture_window', forbidden)
    resumed = collect_task(definition=definition, config_root=config, data_root=tmp_path/'run', mode='replay')
    assert not resumed.get('no_op') and not resumed['complete_today']


@pytest.mark.parametrize('tampered', [False, True])
def test_existing_archive_command_preserves_batch_definition_reports_and_resume(tmp_path, tampered):
    from stock_data_manage.worker.recovery import archive_published_task
    from test_daily_collection_e2e import batch_entries
    definition = source_task(tmp_path)
    root = tmp_path/'run'
    state = run(definition, root)
    work = Path(state['definition_ref']['path']).parent
    entries = batch_entries(state)
    plan_hash = file_hash(work/'task.json')
    report_hashes = {key:entry['report_hash'] for key,entry in entries.items()}
    if tampered:
        Path(next(iter(entries.values()))['artifact']).write_bytes(b'corrupt synthetic artifact')
    paths = load_storage_paths(CONFIG, data_root=root)
    with MetadataStore(paths['metadata_path']) as metadata:
        options = dict(workspace_root=paths['workspace_root'], archive_root=paths['archive_root'],
            canonical_root=paths['canonical_root'], raw_root=paths['raw_root'], metadata=metadata)
        if tampered:
            with pytest.raises(ValueError, match='candidate changed'):
                archive_published_task(work, **options)
            assert work.exists()
            return
        archive = archive_published_task(work, **options)
    assert not work.exists() and file_hash(archive/'task.json') == plan_hash
    resumed = run(definition, root)
    assert resumed['no_op']
    assert {k:e['report_hash'] for k,e in batch_entries(resumed).items()} == report_hashes


def test_preparation_freezes_current_rules_and_resume_uses_them(tmp_path):
    import shutil
    config = tmp_path/'config'
    shutil.copytree(CONFIG, config)
    definition = source_task(tmp_path)
    def stop(stage):
        if stage == 'after_raw_before_unit_index':
            raise RuntimeError('power loss')
    with pytest.raises(RuntimeError):
        collect_task(definition=definition, config_root=config, data_root=tmp_path/'run', mode='replay', failure_hook=stop)
    (config/'normalization/daily_bar.yaml').write_text('rules: []', encoding='utf-8')
    result = collect_task(definition=definition, config_root=config, data_root=tmp_path/'run', mode='replay')
    assert result['status'] == 'published'


def test_one_frozen_plan_small_checkpoints_shared_pacer_and_immediate_failure(tmp_path, monkeypatch):
    import stock_data_manage.pipeline.inputs as inputs
    import stock_data_manage.pipeline.daily as daily
    definition = source_task(tmp_path, symbols=('sh600001','sz000001'), corrupt='backward')
    saves, pacers, definitions = [], [], []
    original_save, capture, save_json = inputs._save_daily_batch, daily.capture_daily_unit, daily.save_batch_json
    def observed_save(state, paths, metadata):
        saves.append((state.get('phase'), len(json.dumps(state)), state.get('progress',{}).get('stages',{}).get('processing',{}).get('failed',0)))
        return original_save(state, paths, metadata)
    def observed_capture(**kwargs):
        pacers.append(id(kwargs['pacer']))
        return capture(**kwargs)
    def observed_json(path, value):
        if Path(path).name == 'task.json':
            definitions.append(path)
        return save_json(path, value)
    monkeypatch.setattr(inputs, '_save_daily_batch', observed_save)
    monkeypatch.setattr(daily, 'capture_daily_unit', observed_capture)
    monkeypatch.setattr(daily, 'save_batch_json', observed_json)
    result = run(definition, tmp_path/'run')
    assert result['status'] == 'failed'
    assert len(definitions) == 1 and len(set(pacers)) == 1 and len(pacers) == 6
    assert all(size < 12000 for _,size,_ in saves)
    assert any(phase == 'process' and failures for phase,_,failures in saves)


def test_yaml_target_expands_stock_etf_and_index_and_freezes_published_master(tmp_path):
    from test_collection_tasks import run as master_run, master_definition, FaultCollector
    from stock_data_manage.pipeline.daily import resolve_daily_definition, expand_daily_units
    from stock_data_manage.pipeline.inputs import _task_units
    collector = FaultCollector()
    collector.catalog_rows['shenzhen-shanghai'] = [
        {'stock_code':'600001','exchange':'XSHG','asset_type':'stock'},
        {'stock_code':'510300','exchange':'XSHG','asset_type':'etf'},
        {'stock_code':'000001','exchange':'XSHG','asset_type':'index'}]
    master = master_definition()
    master['required_groups'] = [['XSHG',a] for a in ['stock','etf','index']]
    assert master_run(tmp_path, master, collector)['status'] == 'published'
    definition = resolve_daily_definition({'task_id':'whole-market','task':'market_daily','trade_date':master['trade_date']},CONFIG)
    units = _task_units(definition, load_storage_paths(CONFIG,data_root=tmp_path))
    expanded = expand_daily_units(units, definition)
    assert len(units) == 3 and len(expanded) == 5
    assert {u['context']['config']['adjust'] for u in expanded if u['asset_type']=='stock'} == {'none','qfq','hfq'}
    assert all(u['context']['config']['adjust']=='none' for u in expanded if u['asset_type']!='stock')


@pytest.mark.parametrize('asset', ['stock','etf','index'])
def test_explicit_source_parameter_conflicts_are_rejected_before_capture(tmp_path, monkeypatch, asset):
    definition = source_task(tmp_path)
    definition['asset_type'] = asset
    definition['units'][0]['context']['config'] = {'api':'kline','adjust':'none'}
    monkeypatch.setattr(TencentDailyProvider,'capture_window',lambda *a,**k: pytest.fail('must not capture conflicting source parameters'))
    with pytest.raises(ValueError, match='acquisition policy'):
        run(definition,tmp_path/'run')


def test_different_dates_and_selected_redo_preserve_other_current_raw(tmp_path, monkeypatch):
    import test_daily_collection_e2e as fixtures
    from test_daily_collection_e2e import batch_entries
    definition = source_task(tmp_path/'original', symbols=('sh600001','sh600002'))
    root = tmp_path/'run'
    state = run(definition, root)
    paths = load_storage_paths(CONFIG,data_root=root)
    other_raw = {p:file_hash(p) for p in (paths['raw_root']/DAY).rglob('manifest.*sh600002*.ndjson')}
    assert len(other_raw) == 3
    selected = run(definition, root, redo='selected', symbols=['sh600001'])
    assert selected['selected_symbols'] == ['sh600001']
    assert all(file_hash(p) == digest for p,digest in other_raw.items())
    first_date = {p:file_hash(p) for p in (paths['raw_root']/DAY).rglob('manifest.*.ndjson')}
    monkeypatch.setattr(fixtures,'DAY','2026-10-12')
    later = fixtures.source_task(tmp_path/'later', corrupt='forward', task_id='later')
    assert run(later,root)['status'] == 'failed'
    assert all(file_hash(p) == digest for p,digest in first_date.items())


def test_failed_scope_and_live_gate_do_not_capture_or_write_production(tmp_path, monkeypatch):
    import requests
    definition = source_task(tmp_path)
    def forbidden(*args,**kwargs):
        raise AssertionError('invalid scope must not capture')
    monkeypatch.setattr(requests.Session,'send',forbidden)
    with pytest.raises(ValueError,match='frozen scope'):
        result = run(definition,tmp_path/'good')
        run({**definition,'trade_date':'2026-10-08'},tmp_path/'good')
    with pytest.raises(ValueError,match='frozen scope|frozen|scope'):
        run(definition,tmp_path/'selected',redo='selected',symbols=['sh999999'])
    live = json.loads(json.dumps(definition))
    for unit in live['units']:
        unit.pop('replay_manifests')
    with pytest.raises(ValueError,match='qualification blocked'):
        collect_task(definition=live,config_root=CONFIG,data_root=tmp_path/'live',mode='live')
    assert not list((tmp_path/'live/raw').rglob('*.ndjson'))
    with pytest.raises(ValueError,match='isolated'):
        collect_task(definition=definition,config_root=CONFIG,data_root=CONFIG.parent/'data',mode='replay')


def test_unindexed_response_recovers_only_for_its_own_frozen_task(tmp_path):
    definition=source_task(tmp_path)
    def interrupt(stage):
        if stage=='after_raw_before_unit_index':
            raise RuntimeError('power loss')
    with pytest.raises(RuntimeError):
        run(definition,tmp_path/'run',failure_hook=interrupt)
    with pytest.raises(ValueError,match='another task/attempt'):
        run({**definition,'task_id':'different-task'},tmp_path/'run')
    assert run(definition,tmp_path/'run')['status']=='published'


def test_capture_preserves_native_host_fallback_before_processing(tmp_path, monkeypatch):
    import requests
    from stock_data_manage.config.loader import load_input_capabilities,load_input_field_contract
    from stock_data_manage.routing.factory import build_input_provider
    from stock_data_manage.pipeline.daily import capture_daily_unit,process_daily_unit
    from stock_data_manage.providers.transport import RequestPacer
    contract=next(c for c in load_input_capabilities(CONFIG/'providers.yaml') if c.input_id=='ASTOCK-002-daily')
    provider=build_input_provider(contract,providers_path=CONFIG/'providers.yaml')
    calls=[]
    def send(session,request,**kwargs):
        calls.append(request.url)
        response=requests.Response()
        response.status_code=503 if len(calls)==1 else 200
        response._content=json.dumps({'data':{'sh600001':{'day':[[DAY,'10','10','10','10','100']]}}}).encode()
        response.headers={'Content-Type':'application/json'}
        response.encoding='utf-8';response.url=request.url;response.request=request
        return response
    monkeypatch.setattr(requests.Session,'send',send)
    unit={'key':'sh600001__none','symbol':'sh600001','asset_type':'stock','input_id':contract.input_id,'adjustment':'none',
        'context':{'request':{'symbol':'sh600001','start_date':DAY,'end_date':DAY},'config':{'api':'kline','adjust':'none'}}}
    paths=load_storage_paths(CONFIG,data_root=tmp_path/'run')
    entry=capture_daily_unit(unit=unit,contract=contract,provider=provider,paths=paths,task_id='fallback-fixture',
        trade_date=DAY,pacer=RequestPacer(wait=lambda _:None))
    assert len(calls)==2 and entry['live_http_calls']==2
    events=[json.loads(line) for line in Path(entry['raw_manifest']).read_text().splitlines()]
    assert [e['status_code'] for e in events if e.get('outcome')=='response']==[503,200]
    field_contract,rule=load_input_field_contract(CONFIG,contract)
    result=process_daily_unit(unit=unit,entry=entry,provider=provider,contract=contract,field_contract=field_contract,
        rule=rule,work=tmp_path/'processing')
    assert result['process_status']=='candidate_complete'
