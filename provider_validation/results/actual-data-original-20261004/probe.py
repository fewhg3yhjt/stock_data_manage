"""Original SDK calls; bounded by the caller, safe source evidence before parsing."""
import hashlib
import inspect
import json
import re
import sys
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'src'))
import akshare
from stock_data_manage.storage.raw import RawObjectStore, _SECRET_BODY
from stock_data_manage.providers.transport import captured_requests, RequestPacer
from stock_data_manage.pipeline.inputs import _json_value

if sys.argv[1] == '--bounded':
    input_id, prefix = sys.argv[2:4]
    try:
        completed = subprocess.run([sys.executable, __file__, input_id, prefix], timeout=120)
        sys.exit(completed.returncode)
    except subprocess.TimeoutExpired:
        target = ROOT / 'provider_validation/results' / prefix / input_id / 'cancellation.json'
        target.write_bytes(json.dumps({'input_id': input_id, 'outcome': 'external_probe_deadline',
            'seconds': 120, 'source_unavailable': False, 'transport_timeout_changed': False,
            'validation_time_utc': datetime.now(timezone.utc).isoformat()}).encode())
        print(input_id, 'bounded probe cancelled; source availability undetermined', flush=True)
        sys.exit(0)

input_id = sys.argv[1]
cases = {
    'ASTOCK-011': ('stock_profit_forecast_ths', {'symbol': '600519'}),
    'ASTOCK-037-business': ('stock_zyjs_ths', {'symbol': '600519'}),
    'ASTOCK-087': ('stock_notice_report', {'symbol': '全部', 'date': '20261001'}),
}
function, parameters = cases[input_id]
directory = ROOT / 'provider_validation/results' / (sys.argv[2] if len(sys.argv) > 2 else 'actual-data-live-20261004') / input_id
directory.mkdir(parents=True, exist_ok=False)
store = RawObjectStore(directory / '_raw')
original_record = store.record_response
version = hashlib.sha256(inspect.getsource(getattr(akshare, function)).encode()).hexdigest()
result = {'input_id': input_id, 'function': function, 'parameters': parameters,
          'code_version': version, 'production_writes': 0, 'eligible_for_production_routing': False,
          'archive_search': 'actual-data-original-20261004/matching-responses.json'}

def retain(**kwargs):
    response = kwargs['response']
    body = response.content
    original_hash = hashlib.sha256(body).hexdigest()
    # Login-form JS may contain password assignments. Never persist any assigned
    # literal: retain a redacted representation and the exact original hash instead.
    if _SECRET_BODY.search(body):
        pattern = rb'''(?i)(["']?(?:access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|authorization|password|session[_-]?token)["']?\s*[:=]\s*)(["'])(?:\\.|(?!\2)[^\\\r\n])*\2'''
        safe, count = re.subn(pattern, rb'\1null', body)
        if not count or _SECRET_BODY.search(safe):
            return original_record(**kwargs)  # conservative suppression on unknown syntax
        store.append_event({'event': 'response_redaction', 'original_body_sha256': original_hash,
                            'original_body_bytes': len(body), 'redacted_body_sha256': hashlib.sha256(safe).hexdigest(),
                            'redacted_literal_count': count, 'reason': 'credential-like literals in HTML suppressed',
                            'transformation_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                            'exact_original_bytes_retained': False, 'fetched_at_utc': datetime.now(timezone.utc).isoformat()})
        response._content = safe
        append = store.append_event
        def linked(event):
            if event.get('event') == 'http_response':
                event.update(redacted=True, original_body_sha256=original_hash,
                             original_body_bytes=len(body), exact_original_bytes_retained=False,
                             persistence_status='redacted_application_payload_saved')
            append(event)
        with patch.object(store, 'append_event', linked):
            return original_record(**kwargs)
    return original_record(**kwargs)

try:
    with patch.object(store, 'record_response', retain), captured_requests(
            store, provider='original-sdk', endpoint=function, scope=parameters,
            code_version=version, pacer=RequestPacer(), sdk_retry_policy=True, probe_host_pause=True):
        frame = getattr(akshare, function)(**parameters)
        rows = _json_value(frame.astype(object).where(frame.notna(), None).to_dict('records'))
        parsed = directory / 'parsed.json'
        parsed.write_bytes(json.dumps(rows, ensure_ascii=False, indent=2).encode())
        result.update(status='parsed', row_count=len(rows), fields=list(frame.columns),
                      parsed_sha256=hashlib.sha256(parsed.read_bytes()).hexdigest())
except Exception as exc:
    result.update(status='failed', failure_class=type(exc).__name__, error=str(exc))
manifest = store.root / 'manifest.ndjson'
result.update(manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest() if manifest.exists() else None)
(directory / 'result.json').write_bytes(json.dumps(result, ensure_ascii=False, indent=2).encode())
print(input_id, result['status'], result.get('row_count'), result.get('failure_class'), flush=True)
