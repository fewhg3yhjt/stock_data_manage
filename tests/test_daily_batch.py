"""Batch boundaries and crash recovery, using explicitly synthetic archived HTTP bytes."""
import json
from datetime import date
from pathlib import Path
import shutil
import pytest
import yaml

from test_daily_collection_e2e import source_task, CONFIG, DAY
from stock_data_manage.pipeline.inputs import collect_task
from stock_data_manage.providers.tencent.daily import TencentDailyProvider
from stock_data_manage.storage.metadata import MetadataStore
from stock_data_manage.config.loader import load_storage_paths
from stock_data_manage.storage.integrity import file_hash


def run(definition, root, *, config_root=CONFIG, **kwargs):
    return collect_task(definition=definition, config_root=config_root, data_root=root, mode='replay', **kwargs)


def strict_daily_config(tmp_path):
    config = tmp_path / 'strict-config'
    shutil.copytree(CONFIG, config)
    path = config / 'datasets' / 'daily_bar.yaml'
    document = yaml.safe_load(path.read_text(encoding='utf-8'))
    document['dataset']['publication']['max_missing_ratio'] = 0
    document['dataset']['publication']['max_missing_count'] = 0
    path.write_text(yaml.safe_dump(document, sort_keys=False, allow_unicode=True), encoding='utf-8')
    return config


def test_daily_batch_processes_while_capture_continues_and_writes_candidate_batches(tmp_path, monkeypatch):
    from threading import Event

    definition = source_task(tmp_path, symbols=('sh600001', 'sz000001'))
    calls = []
    first_parse = Event()
    capture, parse = TencentDailyProvider.capture_window, TencentDailyProvider.parse_window
    def captured(self, *args, **kwargs):
        if calls.count('capture') == 1:
            assert first_parse.wait(5), 'processing did not start while the next request was pending'
        calls.append('capture')
        return capture(self, *args, **kwargs)
    def parsed(self, *args, **kwargs):
        calls.append('parse')
        result = parse(self, *args, **kwargs)
        first_parse.set()
        return result
    monkeypatch.setattr(TencentDailyProvider, 'capture_window', captured)
    monkeypatch.setattr(TencentDailyProvider, 'parse_window', parsed)
    result = run(definition, tmp_path / 'run')
    assert result['status'] == 'published'
    assert result['data_date'] == DAY
    assert calls.count('capture') == 6 and calls.count('parse') == 6
    assert calls.index('parse') < calls.index('capture', 1)
    assert 'units' not in result and 'definition' not in result
    work = Path(result['definition_ref']['path']).parent
    assert (work / 'summary.json').stat().st_size < 12000
    assert len(list(work.rglob('*.manifest.json'))) == 6
    candidates = [json.loads(path.read_text(encoding='utf-8')) for path in work.rglob('*.manifest.json')]
    candidates = [entry for entry in candidates if entry.get('process_status') == 'candidate_complete']
    assert len(candidates) >= 2
    assert len({entry['artifact'] for entry in candidates}) < len(candidates)
    for entry in candidates:
        assert Path(entry['artifact']).is_file()
        assert isinstance(entry['artifact_row_index'], int)
    for path in work.rglob('*.report.json'):
        assert json.loads(path.read_text(encoding='utf-8'))['processing_http_calls'] == 0
    plan = work / 'task.json'
    stamp = plan.stat().st_mtime_ns
    assert run(definition, tmp_path / 'run')['no_op']
    assert plan.stat().st_mtime_ns == stamp


def test_selected_redo_replaces_selected_batch_and_preserves_other_raw(tmp_path):
    from test_daily_collection_e2e import batch_entries

    definition = source_task(tmp_path / 'fixtures', symbols=('sh600001', 'sh600002'))
    root = tmp_path / 'run'
    run(definition, root)
    paths = load_storage_paths(CONFIG, data_root=root)
    unchanged = {path: file_hash(path) for path in (paths['raw_root'] / DAY).rglob(
        'manifest.*sh600002*.ndjson')}
    assert len(unchanged) == 3

    selected = run(definition, root, redo='selected', symbols=['sh600001'])

    assert selected['status'] == 'published'
    assert selected['selected_symbols'] == ['sh600001']
    assert all(file_hash(path) == digest for path, digest in unchanged.items())
    entries = batch_entries(selected)
    selected_entries = [entry for key, entry in entries.items() if key.startswith('sh600001__')]
    assert len(selected_entries) == 3
    assert all(entry['process_status'] == 'candidate_complete' for entry in selected_entries)
    assert len({entry['artifact'] for entry in selected_entries}) == 1


def test_selected_redo_does_not_complete_frozen_scope_and_resume_processes_remaining_asset(tmp_path, monkeypatch):
    from stock_data_manage.providers.tencent.daily import TencentDailyProvider
    from stock_data_manage.quality.normalization import NormalizationError
    from stock_data_manage.storage.daily import DailyBarStore
    from datetime import date

    definition = source_task(tmp_path / 'stock-fixtures', symbols=('sh600001', 'sh600002', 'sh600003'),
        task_id='selected-frozen-scope')
    for unit in definition['units']:
        unit['asset_type'] = 'stock'
    etf_unit = source_task(tmp_path / 'etf-fixtures', symbols=('sh510300',), task_id='etf-fixture')['units'][0]
    etf_unit['asset_type'] = 'etf'
    definition.pop('asset_type')
    definition['asset_types'] = ['stock', 'etf']
    definition['universe'] = 'static'
    definition['units'].extend([etf_unit])
    config = strict_daily_config(tmp_path)
    import yaml
    policy_path = config / 'datasets' / 'daily_bar.yaml'
    policy = yaml.safe_load(policy_path.read_text(encoding='utf-8'))
    policy['dataset']['publication'].update(max_missing_ratio=0.67, max_missing_count=2)
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False, allow_unicode=True), encoding='utf-8')

    original_parse = TencentDailyProvider.parse_window
    def fail_two_stock_symbols(provider, captured):
        if captured['symbol'] in {'sh600002', 'sh600003'}:
            raise NormalizationError('synthetic candidate processing failure')
        return original_parse(provider, captured)
    monkeypatch.setattr(TencentDailyProvider, 'parse_window', fail_two_stock_symbols)
    root = tmp_path / 'run'
    first = run(definition, root, config_root=config)
    assert first['status'] == 'published' and first['complete_today'] is False
    assert sum(item['row_count'] for item in first['publication_refs']) == 2
    assert (first['frozen_security_count'], first['delivered_security_count'],
        first['undelivered_security_count']) == (4, 2, 2)

    monkeypatch.setattr(TencentDailyProvider, 'parse_window', original_parse)
    selected = run(definition, root, config_root=config, redo='selected', symbols=['sh600002'])
    assert selected['status'] == 'published' and selected['complete_today'] is False
    assert [item['asset_type'] for item in selected['publication_refs']] == ['stock']
    assert selected['partitions']['stock']['row_count'] == 1
    assert (selected['frozen_security_count'], selected['delivered_security_count'],
        selected['undelivered_security_count']) == (4, 3, 1)
    assert selected['row_count'] == 1

    seen = []
    def track_remaining(provider, captured):
        seen.append(captured['symbol'])
        return original_parse(provider, captured)
    monkeypatch.setattr(TencentDailyProvider, 'parse_window', track_remaining)
    resumed = run(definition, root, config_root=config, redo='resume')
    assert resumed['status'] == 'published' and resumed['complete_today'] is True
    assert (resumed['frozen_security_count'], resumed['delivered_security_count'],
        resumed['undelivered_security_count']) == (4, 4, 0)
    assert resumed['row_count'] == 1
    assert set(seen) == {'sh600003'}
    paths = load_storage_paths(config, data_root=root)
    with MetadataStore(paths['metadata_path'], read_only=True) as metadata:
        store = DailyBarStore(metadata.connection)
        assert metadata.partition('daily_bar', 'stock', DAY, adjustment='aggregate')['actual_count'] == 3
        assert metadata.partition('daily_bar', 'etf', DAY, adjustment='aggregate')['actual_count'] == 1
        records, total = store.list_records(date_from=date.fromisoformat(DAY), date_to=date.fromisoformat(DAY),
            asset_type=None, adjustment='none')
        assert len(records) == total == 4


def test_live_daily_fixed_route_does_not_require_expiring_symbol_qualification(tmp_path, monkeypatch):
    import requests
    import stock_data_manage.pipeline.daily as daily
    import stock_data_manage.routing.factory as routing
    from stock_data_manage.providers.transport import RequestPacer

    definition = source_task(tmp_path)
    definition["task"] = "stock_daily"
    definition["units"][0].pop("replay_manifests")
    captured = []
    qualified = []

    def capture(**kwargs):
        captured.append(kwargs["unit"]["key"])
        raise requests.ConnectionError("synthetic stop before network")

    def qualification(*args, **kwargs):
        qualified.append(kwargs.get("input_id"))
        raise AssertionError("explicit daily_fixed task must not pre-block on symbol qualification")

    monkeypatch.setattr(daily, "capture_daily_unit", capture)
    monkeypatch.setattr(routing, "check_daily_batch_qualification", qualification)
    result = collect_task(definition=definition, config_root=CONFIG, data_root=tmp_path / "live",
        mode="live", collector_options={"pacer": RequestPacer(wait=lambda _: None)})
    assert result["status"] == "failed"
    assert len(captured) == 3 and not qualified
    work = Path(result["definition_ref"]["path"]).parent
    assert not list(work.glob("qualification.prepare-*.json"))


@pytest.mark.parametrize('boundary', ['after_raw_before_unit_index', 'after_candidate_before_checkpoint',
    'after_receipt_commit', 'after_daily_projection', 'after_raw_promote'])
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
    definition = source_task(tmp_path, corrupt='none')
    definition.pop('asset_type', None)
    definition.pop('task', None)
    definition['asset_types'] = ['stock', 'etf']
    definition['universe'] = 'static'
    definition['units'][0]['asset_type'] = 'stock'
    etf = source_task(tmp_path, symbols=('sh510300',))['units'][0]
    etf['asset_type'] = 'etf'
    definition['units'].append(etf)
    config = strict_daily_config(tmp_path)
    result = run(definition, tmp_path / 'run', config_root=config)
    assert result['status'] == 'failed'
    assert result['partitions']['stock']['status'] == 'blocked'
    assert result['partitions']['etf']['status'] == 'published'
    paths = load_storage_paths(config, data_root=tmp_path / 'run')
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


@pytest.mark.parametrize('kind', ['raw_corrupt', 'raw_missing', 'candidate_corrupt'])
def test_published_resume_rechecks_evidence_and_never_overwrites_good_publication(tmp_path, kind):
    from test_daily_collection_e2e import batch_entries
    from stock_data_manage.storage.raw import RawObjectStore
    from stock_data_manage.storage.daily import DailyBarStore
    definition = source_task(tmp_path)
    state = run(definition, tmp_path/'run')
    paths = load_storage_paths(CONFIG, data_root=tmp_path/'run')
    with MetadataStore(paths['metadata_path'], read_only=True) as metadata:
        before = DailyBarStore(metadata.connection).get('XSHG:600001', date.fromisoformat(DAY))
    entry = next(iter(batch_entries(state).values()))
    if kind == 'candidate_corrupt':
        work = Path(state['definition_ref']['path']).parent
        descriptor_ref = state['partitions']['stock']['descriptor_ref']
        descriptor = json.loads((work / descriptor_ref['path']).read_text(encoding='utf-8'))
        (work / descriptor['candidate']['path']).write_bytes(b'corrupt synthetic candidate')
    else:
        manifest = Path(entry['raw_manifest'])
        event = json.loads(manifest.read_text(encoding='utf-8').splitlines()[0])
        body = manifest.parent/event['body_storage']
        body.unlink() if kind == 'raw_missing' else body.write_bytes(b'corrupt synthetic body')
    with pytest.raises((ValueError, OSError)):
        run(definition, tmp_path/'run')
    with MetadataStore(paths['metadata_path'], read_only=True) as metadata:
        after = DailyBarStore(metadata.connection).get('XSHG:600001', date.fromisoformat(DAY))
    assert after == before


def test_same_date_pending_commit_allows_disjoint_waiter_and_rejects_overlap(tmp_path, monkeypatch):
    from stock_data_manage.domain import AssetType
    from stock_data_manage.pipeline.inputs import submit_collection_task, run_collection_queue_once
    from stock_data_manage.storage.daily import DailyBarStore
    from stock_data_manage.storage.metadata import CollectionTaskScopeConflict
    from stock_data_manage.providers.transport import RequestPacer

    original_pacer_init = RequestPacer.__init__
    monkeypatch.setattr(RequestPacer, '__init__',
        lambda self, **kwargs: original_pacer_init(self, wait=lambda _seconds: None, **kwargs))
    business = tmp_path / 'run'
    first_definition = source_task(tmp_path / 'first-fixtures', task_id='commit-a')
    def stop(stage):
        if stage == 'after_receipt_commit':
            raise RuntimeError('power loss')
    with pytest.raises(RuntimeError):
        run(first_definition, business, failure_hook=stop)

    paths = load_storage_paths(CONFIG, data_root=business)
    scope_a = {'dataset': 'daily_bar', 'frequency': '1d', 'asset_types': ['stock'],
        'securities_by_date': {DAY: ['XSHG:600001']}}
    with MetadataStore(paths['metadata_path'], read_only=True) as metadata:
        state_a = metadata.load_collection_task('commit-a')
        assert state_a['status'] == 'committing'
        store = DailyBarStore(metadata.connection)
        receipt_a = store.publication_receipts(task_id='commit-a')
        records, count = store.list_records(asset_type=AssetType.STOCK,
            date_from=date.fromisoformat(DAY), date_to=date.fromisoformat(DAY))
        owners_a = store.current_owners(scope_a)
    assert len(receipt_a) == 1
    assert count == len(records) == 1 and records[0].instrument_id == 'XSHG:600001'
    assert len(owners_a) == 1 and owners_a[0]['task_id'] == 'commit-a'

    second_definition = source_task(tmp_path / 'second-fixtures', symbols=('sh600002',), task_id='waiter-b')
    conflict_definition = source_task(tmp_path / 'conflict-fixtures', task_id='conflict-c')
    capture = TencentDailyProvider.capture_window
    raw_root = paths['raw_root']
    raw_before_submissions = {path.relative_to(raw_root).as_posix(): file_hash(path)
        for path in raw_root.rglob('*') if path.is_file()}
    def forbidden(*args, **kwargs):
        raise AssertionError('submission must not capture provider data')
    monkeypatch.setattr(TencentDailyProvider, 'capture_window', forbidden)
    accepted_b = submit_collection_task(definition=second_definition, config_root=CONFIG, data_root=business,
        mode='replay', collector_options={'pacer': None})
    assert accepted_b['task_id'] == 'waiter-b' and accepted_b['status'] == 'waiting'

    with pytest.raises(CollectionTaskScopeConflict) as captured:
        submit_collection_task(definition=conflict_definition, config_root=CONFIG, data_root=business,
            mode='replay',
            collector_options={'pacer': None})
    assert captured.value.task_id == 'commit-a'
    with pytest.raises(CollectionTaskScopeConflict) as captured_overwrite:
        submit_collection_task(definition=conflict_definition, config_root=CONFIG, data_root=business,
            mode='replay', overwrite=True, confirmed_conflicts=owners_a,
            collector_options={'pacer': None})
    assert captured_overwrite.value.task_id == 'commit-a'
    paths = load_storage_paths(CONFIG, data_root=business)
    assert not (paths['workspace_root'] / DAY / 'daily_bar' / 'conflict-c').exists()
    assert {path.relative_to(raw_root).as_posix(): file_hash(path)
        for path in raw_root.rglob('*') if path.is_file()} == raw_before_submissions
    with MetadataStore(paths['metadata_path'], read_only=True) as metadata:
        assert metadata.load_collection_task('conflict-c') is None
        assert metadata.load_collection_task('waiter-b')['status'] == 'waiting'
        assert len(DailyBarStore(metadata.connection).publication_receipts(task_id='conflict-c')) == 0

    monkeypatch.setattr(TencentDailyProvider, 'capture_window', capture)
    recovered_a = run_collection_queue_once(config_root=CONFIG, data_root=business)
    assert recovered_a['task_id'] == 'commit-a' and recovered_a['status'] == 'published'
    published_b = run(second_definition, business)
    assert published_b['task_id'] == 'waiter-b' and published_b['status'] == 'published'
    with MetadataStore(paths['metadata_path'], read_only=True) as metadata:
        store = DailyBarStore(metadata.connection)
        records, count = store.list_records(asset_type=AssetType.STOCK,
            date_from=date.fromisoformat(DAY), date_to=date.fromisoformat(DAY))
        assert {record.instrument_id for record in records} == {'XSHG:600001', 'XSHG:600002'}
        assert count == 2
        assert metadata.load_collection_task('commit-a')['status'] == 'published'
        assert metadata.load_collection_task('waiter-b')['status'] == 'published'
        assert metadata.load_collection_task('conflict-c') is None
        assert not store.publication_receipts(task_id='conflict-c')


def test_recovery_reloads_daily_state_after_execution_lock_is_acquired(tmp_path, monkeypatch):
    import stock_data_manage.storage.parquet as parquet
    import stock_data_manage.worker.recovery as recovery

    business = tmp_path / 'recovery-race'
    paths = load_storage_paths(CONFIG, data_root=business)
    initial = {'task_id': 'recovery-race', 'dataset': 'daily_bar', 'definition_hash': 'frozen',
        'format_version': 'daily-table-batch-v1', 'status': 'committing', 'definition': {'trade_date': DAY}}
    with MetadataStore(paths['metadata_path']) as metadata:
        metadata.save_collection_task(initial)

    class CompletingLock:
        def __init__(self, *_args, **_kwargs):
            pass
        def __enter__(self):
            with MetadataStore(paths['metadata_path']) as metadata:
                state = metadata.load_collection_task('recovery-race')
                state['status'] = 'published'
                metadata.save_collection_task(state)
            return self
        def __exit__(self, *_args):
            return False

    def forbidden(*_args, **_kwargs):
        raise AssertionError('recovery must recheck state after locking')
    monkeypatch.setattr(parquet, 'PartitionLock', CompletingLock)
    import stock_data_manage.pipeline.inputs as inputs
    monkeypatch.setattr(inputs, '_finish_daily_batch', forbidden)
    assert recovery.recover_collection_tasks(config_root=CONFIG, data_root=business) == ()
    with MetadataStore(paths['metadata_path'], read_only=True) as metadata:
        assert metadata.load_collection_task('recovery-race')['status'] == 'published'


def test_missing_required_unadjusted_response_stays_visible_without_refetch_on_resume(tmp_path, monkeypatch):
    import shutil
    import yaml
    config = tmp_path/'config'
    shutil.copytree(CONFIG, config)
    path = config/'datasets/daily_bar.yaml'
    doc = yaml.safe_load(path.read_text(encoding='utf-8'))
    doc['dataset']['publication'].update(max_missing_ratio=0.5, max_missing_count=1)
    path.write_text(yaml.safe_dump(doc), encoding='utf-8')
    definition = source_task(tmp_path, symbols=('sh600001','sh600002'))
    bad = Path(definition['units'][1]['replay_manifests']['none'])
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
    from stock_data_manage.storage.raw import RawObjectStore
    original_verify = RawObjectStore.verify_manifest
    verified = []
    def track_verify(path):
        verified.append(str(path))
        return original_verify(path)
    monkeypatch.setattr(RawObjectStore, 'verify_manifest', staticmethod(track_verify))
    resumed = collect_task(definition=definition, config_root=config, data_root=tmp_path/'run', mode='replay')
    assert not resumed.get('no_op') and not resumed['complete_today']
    assert len(verified) < 20


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


def test_one_frozen_plan_optional_processing_failure_keeps_partial_publish_status(tmp_path, monkeypatch):
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
    assert result['status'] == 'published' and result['complete_today'] is False
    assert result['missing_count'] == 2
    assert len(definitions) == 1 and len(set(pacers)) == 1 and len(pacers) == 6
    assert all(size < 12000 for _,size,_ in saves)
    assert any(phase == 'process' and failures for phase,_,failures in saves)


def test_yaml_target_expands_stock_etf_and_index_and_freezes_published_master(tmp_path):
    from copy import deepcopy
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
    owned_units = deepcopy(units)
    expanded_owned = expand_daily_units(owned_units, definition, _owned_units=True)
    assert expanded_owned == expanded
    assert len(units) == 3 and len(expanded) == 5
    assert {u['context']['config']['adjust'] for u in expanded if u['asset_type']=='stock'} == {'none','qfq','hfq'}
    assert all(u['context']['config']['adjust']=='none' for u in expanded if u['asset_type']!='stock')


def test_manual_lifecycle_overrides_are_applied_to_the_dated_daily_scope():
    import yaml
    from stock_data_manage.pipeline.inputs import _apply_manual_security_lifecycle_overrides

    policy = yaml.safe_load((CONFIG / "datasets/security_master.yaml").read_text(encoding="utf-8"))
    units = [
        {"symbol": "sz002717", "instrument_id": "XSHE:002717", "asset_type": "stock", "status": "unknown"},
        {"symbol": "sz399960", "instrument_id": "XSHE:399960", "asset_type": "index", "status": "unknown"},
        {"symbol": "sz399332", "instrument_id": "XSHE:399332", "asset_type": "index", "status": "unknown"},
        {"symbol": "sh000999", "instrument_id": "XSHG:000999", "asset_type": "index", "status": "unknown"},
    ]
    retained, exclusions = _apply_manual_security_lifecycle_overrides(
        units, policy["symbol_lifecycle_overrides"], trade_date="2026-10-29")
    by_symbol = {unit["symbol"]: unit for unit in retained}

    assert by_symbol["sz002717"]["status"] == "suspended"
    assert by_symbol["sz002717"]["status_evidence"]["basis"] == "manual_security_lifecycle_override"
    assert by_symbol["sz002717"]["status_evidence"]["effective_from"] == "2026-10-13"
    assert not {"sz399960", "sz399332", "sh000999"} & set(by_symbol)
    assert {item["symbol"] for item in exclusions} == {"sz399960", "sz399332", "sh000999"}
    assert all(item["status"] == "delisted" for item in exclusions)


def test_daily_batch_freezes_manual_suspension_and_does_not_request_that_security(tmp_path, monkeypatch):
    import json
    import test_daily_collection_e2e as fixtures

    target_day = "2026-10-29"
    monkeypatch.setattr(fixtures, "DAY", target_day)
    definition = fixtures.source_task(tmp_path / "responses", symbols=("sh600001",), task_id="lifecycle-batch")
    definition["units"].append({"key": "sz002717", "symbol": "sz002717", "instrument_id": "XSHE:002717",
        "input_id": "ASTOCK-002-daily", "context": {"request": {"symbol": "sz002717",
            "start_date": target_day, "end_date": target_day}}})

    result = run(definition, tmp_path / "business")

    assert result["status"] == "published" and result["complete_today"]
    assert result["source_unit_count"] == 3
    suspended = result["suspended_securities"]
    assert len(suspended) == 1
    assert suspended[0]["symbol"] == "sz002717"
    assert suspended[0]["status_evidence"]["basis"] == "manual_security_lifecycle_override"
    frozen = json.loads(Path(result["definition_ref"]["path"]).read_text(encoding="utf-8"))
    assert all(unit["symbol"] != "sz002717" for unit in frozen["units"])
    coverage_path = next(Path(result["definition_ref"]["path"]).parent.glob("coverage.stock.attempt-*.json"))
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    assert coverage["expected_count"] == 1 and coverage["suspended_count"] == 1
    assert coverage["item_statuses"]["XSHE:002717"] == "no_trade"
    assert any(issue["code"] == "confirmed_suspended" and issue["symbol"] == "sz002717"
        for issue in coverage["issues"])


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
    later_result = run(later,root)
    assert later_result['status'] == 'published' and later_result['complete_today'] is False
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
    live_result = collect_task(definition=live,config_root=CONFIG,data_root=tmp_path/'live',mode='live')
    assert live_result['status'] == 'failed'
    gate = json.loads(Path(live_result['qualification_ref']['path']).read_text(encoding='utf-8'))
    assert all(not item['eligible'] for item in gate.values())
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
    from stock_data_manage.storage.read_index import SOURCE_QUERY_DATABASE
    result=process_daily_unit(unit=unit,entry=entry,provider=provider,contract=contract,field_contract=field_contract,
        rule=rule,work=tmp_path/'processing',config_root=CONFIG,
        query_db_path=paths['data_root']/SOURCE_QUERY_DATABASE)
    assert result['process_status']=='candidate_complete'
