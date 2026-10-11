from io import StringIO
from stock_data_manage.worker.progress import TaskProgress, ConsoleProgress


def test_failed_completion_is_not_success_and_unknown_total_has_no_percent():
    clock = [0.0]
    progress = TaskProgress(2, clock=lambda: clock[0])
    progress.begin('raw')
    clock[0] = 3.0
    progress.finish('raw', 'a')
    progress.finish('raw', 'b', 'failed')
    stage = progress.snapshot()['stages']['raw']
    assert stage['percent'] == 100 and stage['success_percent'] == 50
    assert stage['status'] == 'failed' and stage['elapsed_seconds'] == 3
    unknown = TaskProgress(None)
    unknown.finish('raw', 'one')
    assert unknown.snapshot()['stages']['raw']['percent'] is None


def test_restore_preserves_completed_publication_steps_without_double_counting():
    progress = TaskProgress(2, reused=['symbol'])
    progress.finish('publication', 'check')
    progress.finish('publication', 'canonical')
    stage = progress.snapshot()['stages']['publication']
    assert stage['total'] == 2 and stage['completed'] == 0 and stage['percent'] == 0
    assert stage['status'] == 'running'
    restored = TaskProgress.restore(progress.snapshot())
    restored.finish('publication', 'canonical')
    restored.finish('publication', 'raw_promote')
    restored.finish('publication', 'metadata')
    stages = restored.snapshot()['stages']
    assert stages['raw']['reused'] == 1
    assert stages['publication']['completed'] == 2
    assert stages['publication']['percent'] == 100
    assert stages['publication']['current_action'] == 'metadata'


def test_console_keeps_counts_failures_reuse_and_no_op_explicit():
    stream = StringIO()
    progress = TaskProgress(2, reused=['existing'])
    progress.finish('raw', 'missing', 'failed')
    ConsoleProgress(stream)(progress.snapshot())
    assert '2/2' in stream.getvalue() and '失败1' in stream.getvalue() and '复用1' in stream.getvalue()
    ConsoleProgress(stream)({'no_op': True})
    assert '无需执行' in stream.getvalue()


def test_publication_rejection_stops_elapsed_time_without_claiming_full_completion():
    clock=[0.0]
    progress=TaskProgress(1,clock=lambda:clock[0])
    progress.finish('publication','check','failed')
    clock[0]=10.0
    stage=progress.snapshot()['stages']['publication']
    assert stage['status']=='failed' and stage['percent']==0
    assert stage['completed']==0 and stage['failed']==1
    assert stage['elapsed_seconds']==0


def test_legacy_publication_steps_restore_as_unit_counts():
    progress = TaskProgress(2)
    progress.finish('publication', 'check')
    snapshot = progress.snapshot()
    snapshot['version'] = 1
    snapshot['stages']['publication'].update(total=4, completed=1, percent=25)
    restored = TaskProgress.restore(snapshot)
    assert restored.snapshot()['stages']['publication']['total'] == 2
    assert restored.snapshot()['stages']['publication']['completed'] == 0
    for step in ['canonical', 'raw_promote', 'metadata']:
        restored.finish('publication', step)
    assert restored.snapshot()['stages']['publication']['completed'] == 2


def test_publication_reuse_counts_units_and_console_does_not_count_actions():
    progress = TaskProgress(2)
    progress.finish('publication', 'check')
    for step in ['canonical', 'raw_promote', 'metadata']:
        progress.finish('publication', step, 'reused')
    stage = progress.snapshot()['stages']['publication']
    assert stage['reused'] == 2 and stage['success'] == 0
    stream = StringIO()
    ConsoleProgress(stream)(progress.snapshot())
    assert '4/4' not in stream.getvalue() and '登记结果' in stream.getvalue()
