"""Freeze implementation baseline and inspect exact successful market event requests."""
from pathlib import Path
import collections, hashlib, json, sys
from urllib.parse import parse_qs, urlsplit
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'src'))
from stock_data_manage.storage.raw import RawObjectStore

def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()

target = Path(__file__).parent
paths = ['src/stock_data_manage/providers/eastmoney/financial.py', 'src/stock_data_manage/pipeline/inputs.py',
         'src/stock_data_manage/routing/factory.py', 'config/providers.yaml', 'tests/test_input_collection.py',
         'provider_validation/tests/replay_input_capabilities.py', '.gitattributes', 'CODE_STRUCTURE.md', 'PROJECT_PROGRESS.md']
before = []
for i, name in enumerate(paths):
    source = ROOT / name
    snapshot = target / (str(i) + '-' + source.name + '.bin')
    if not snapshot.exists(): snapshot.write_bytes(source.read_bytes())
    assert sha(source) == sha(snapshot)
    before.append(dict(path=name, snapshot=snapshot.relative_to(ROOT).as_posix(), sha256=sha(snapshot)))
manifest = ROOT / 'provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson'
source = ROOT / 'provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py'
events = []
for line in manifest.read_text(encoding='utf-8').splitlines():
    event = json.loads(line)
    query = parse_qs(urlsplit(event.get('url', '')).query)
    if query.get('reportName', [''])[0] not in {'RPT_DAILYBILLBOARD_DETAILSNEW', 'RPT_LIFT_STAGE', 'RPT_LIFT_STOCKHOLDER'}: continue
    body = RawObjectStore.read_response(manifest, event)
    data = json.loads(body)
    result = data.get('result') or {}
    rows = result.get('data') or []
    events.append(dict(query=query, sha256=event['body_sha256'], status=event['status_code'], count=result.get('count'),
                       pages=result.get('pages'), row_count=len(rows), keys=list(rows[0]) if rows else [],
                       first=rows[0] if rows else None,
                       duplicate_identity_count=len(rows)-len({(r.get('SECURITY_CODE'),r.get('TRADE_DATE',r.get('FREE_DATE')),r.get('EXPLANATION',r.get('FREE_SHARES_TYPE'))) for r in rows})))
report = dict(before=before, source_code=source.relative_to(ROOT).as_posix(), source_code_sha256=sha(source),
              raw_manifest=manifest.relative_to(ROOT).as_posix(), raw_manifest_sha256=sha(manifest), events=events,
              boundary='Only selected successful EastMoney SDK endpoints; no provider source/date fallback; candidate only.')
(target / 'investigation.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
print(json.dumps(events, ensure_ascii=False, indent=2))
