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
    progress = TaskProgress(1, reused=['symbol'])
    progress.finish('publication', 'check')
    progress.finish('publication', 'canonical')
    restored = TaskProgress.restore(progress.snapshot())
    restored.finish('publication', 'canonical')
    restored.finish('publication', 'raw_promote')
    restored.finish('publication', 'metadata')
    stages = restored.snapshot()['stages']
    assert stages['raw']['reused'] == 1
    assert stages['publication']['completed'] == 4
    assert stages['publication']['percent'] == 100


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
    assert stage['status']=='failed' and stage['percent']==25
    assert stage['elapsed_seconds']==0
