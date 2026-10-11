"""Shared task progress; reports operations without owning scheduling or data."""
from datetime import datetime, timezone
from time import monotonic


class TaskProgress:
    def __init__(self, total, *, reused=(), callback=None, clock=monotonic):
        self.clock, self.callback = clock, callback
        self.started = clock()
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.stages = {name: {'total': count, 'items': {}, 'started': None, 'ended': None}
                       for name, count in [('raw', total), ('processing', total), ('publication', 4)]}
        for name in ('raw', 'processing'):
            self.stages[name]['items'].update({key: 'reused' for key in reused})
        self.last_emit = float('-inf')

    def begin(self, stage):
        if self.stages[stage]['started'] is None:
            self.stages[stage]['started'] = self.clock()
        self.emit(force=True)

    def finish(self, stage, key, status='success'):
        self.begin(stage) if self.stages[stage]['started'] is None else None
        self.stages[stage]['items'][key] = status
        if (stage == 'publication' and status == 'failed') or (self.stages[stage]['total'] is not None and len(self.stages[stage]['items']) >= self.stages[stage]['total']):
            self.stages[stage]['ended'] = self.clock()
        self.emit()

    def snapshot(self):
        now = self.clock()
        stages = {}
        for name, stage in self.stages.items():
            counts = {status: sum(value == status for value in stage['items'].values())
                      for status in ('success', 'failed', 'skipped', 'reused')}
            completed = len(stage['items'])
            total = stage['total']
            status = 'pending' if stage['started'] is None and not completed else 'running'
            if total is not None and completed >= total:
                status = 'failed' if counts['failed'] or counts['skipped'] else 'completed'
            if stage['ended'] is not None and counts['failed']:
                status = 'failed'
            stages[name] = {'status': status, 'total': total, 'completed': completed, **counts,
                'percent': round(100 * completed / total, 2) if total else None,
                'success_percent': round(100 * (counts['success'] + counts['reused']) / total, 2) if total else None,
                'elapsed_seconds': round((stage['ended'] if stage['ended'] is not None else now) - stage['started'], 3) if stage['started'] is not None else 0}
            if name == 'publication':
                stages[name]['steps'] = dict(stage['items'])
        return {'version': 1, 'started_at_utc': self.started_at,
                'elapsed_seconds': round(now - self.started, 3), 'stages': stages}

    @classmethod
    def restore(cls, snapshot, callback=None):
        progress = cls(snapshot['stages']['raw']['total'], callback=callback)
        progress.started_at = snapshot['started_at_utc']
        progress.started -= snapshot['elapsed_seconds']
        now = progress.clock()
        for name, previous in snapshot['stages'].items():
            stage = progress.stages[name]
            stage['total'] = previous['total']
            if name == 'publication':
                stage['items'] = dict(previous.get('steps', {}))
            else:
                stage['items'] = {f'{status}-{index}': status for status in ('success','failed','skipped','reused')
                                  for index in range(previous[status])}
            if previous['status'] != 'pending':
                stage['started'] = now - previous['elapsed_seconds']
                if previous['status'] in {'completed','failed'}:
                    stage['ended'] = now
        return progress

    def emit(self, force=False):
        now = self.clock()
        if force or now - self.last_emit >= 1:
            self.last_emit = now
            if self.callback:
                self.callback(self.snapshot())


class ConsoleProgress:
    """Keep machine-readable task JSON on stdout; show progress on stderr."""
    def __init__(self, stream):
        self.stream = stream
        self.drawn = False

    def __call__(self, snapshot):
        if snapshot.get('no_op'):
            self.stream.write('任务已完成，本次复用现有结果，无需执行。\n')
            self.stream.flush()
            return
        lines = []
        for name, title in [('raw', '原响应采集'), ('processing', '解析与映射'), ('publication', '检查与发布')]:
            stage = snapshot['stages'][name]
            percent = stage['percent']
            count = f"{stage['completed']}/{stage['total'] if stage['total'] is not None else '?'}"
            filled = int((percent or 0) / 5)
            bar = '[' + '#' * filled + '-' * (20 - filled) + ']'
            ratio = f'{percent:.1f}%' if percent is not None else '总量未知'
            seconds = int(stage['elapsed_seconds'])
            elapsed = f"{stage['elapsed_seconds']:.1f}秒" if seconds < 60 else f"{seconds//60:02d}:{seconds%60:02d}"
            status_text = {'pending':'等待','running':'进行中','failed':'失败','completed':'完成'}[stage['status']]
            lines.append(f"{title} {bar} {count} {ratio} 成功{stage['success']} 失败{stage['failed']} "
                         f"复用{stage['reused']} 跳过{stage['skipped']} 耗时{elapsed} {status_text}")
        tty = self.stream.isatty()
        prefix = '\x1b[3A' if tty and self.drawn else ''
        self.stream.write(prefix + '\n'.join(('\x1b[2K' if tty else '') + line for line in lines) + '\n')
        self.stream.flush()
        self.drawn = True
