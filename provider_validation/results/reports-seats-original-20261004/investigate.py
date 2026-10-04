"""Persist baseline and exact original successful report/seat response scope."""
from pathlib import Path
import hashlib, json, sys
from urllib.parse import parse_qs, urlsplit
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'src'))
from stock_data_manage.storage.raw import RawObjectStore
target = Path(__file__).parent
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
names = ['src/stock_data_manage/providers/eastmoney/financial.py','src/stock_data_manage/pipeline/inputs.py',
    'src/stock_data_manage/routing/factory.py','config/providers.yaml','tests/test_input_collection.py',
    'provider_validation/tests/replay_input_capabilities.py','.gitattributes','CODE_STRUCTURE.md','PROJECT_PROGRESS.md']
before = []
for index, name in enumerate(names):
    p = ROOT/name; snapshot = target/(str(index)+'-'+p.name+'.bin')
    assert not snapshot.exists(); snapshot.write_bytes(p.read_bytes())
    before.append(dict(path=name,snapshot=snapshot.relative_to(ROOT).as_posix(),sha256=sha(snapshot)))
manifest = ROOT/'provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson'
source = ROOT/'provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py'
rows = []
for line in manifest.read_text(encoding='utf-8').splitlines():
    event = json.loads(line); query = parse_qs(urlsplit(event.get('url','')).query,keep_blank_values=True)
    if 'reportapi.eastmoney.com/report/list' not in event.get('url','') and query.get('reportName',[''])[0] not in {'RPT_BILLBOARD_DAILYDETAILSBUY','RPT_BILLBOARD_DAILYDETAILSSELL','RPT_BILLBOARD_PERFORMANCESTOCK'}: continue
    payload = json.loads(RawObjectStore.read_response(manifest,event))
    data = payload.get('result') or payload
    items = data.get('data') or []
    rows.append(dict(query=query,body_sha256=event['body_sha256'],status=event['status_code'],headers=event['request_headers'],
        request_options=event.get('request_options'),payload_keys=list(payload),metadata={k:v for k,v in data.items() if k!='data'},
        row_count=len(items),fields=list(items[0]) if items else [],first=items[0] if items else None))
meta = manifest.parents[2]/'17_龙虎榜席位/data/_meta.json'
report = dict(before=before,source_code=source.relative_to(ROOT).as_posix(),source_code_sha256=sha(source),
    raw_manifest=manifest.relative_to(ROOT).as_posix(),raw_manifest_sha256=sha(manifest),events=rows,
    seat_original_selection=json.loads(meta.read_text(encoding='utf-8')),seat_original_selection_sha256=sha(meta),
    boundary='explicit report code/window and explicit seat code/day; no implicit endpoint/source/date fallback, candidates only')
(target/'investigation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(report['seat_original_selection'],ensure_ascii=False))
print(json.dumps(rows,ensure_ascii=False,indent=2))
