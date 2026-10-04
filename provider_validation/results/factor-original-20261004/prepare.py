from pathlib import Path
import hashlib,json
ROOT=Path(__file__).resolve().parents[3]
out=Path(__file__).resolve().parent
paths=['src/stock_data_manage/providers/sina/daily.py','src/stock_data_manage/providers/transport.py','src/stock_data_manage/pipeline/inputs.py','src/stock_data_manage/routing/factory.py','config/providers.yaml','tests/test_input_collection.py','provider_validation/tests/replay_input_capabilities.py','.gitattributes','CODE_STRUCTURE.md']
refs=[]
for i,name in enumerate(paths):
    source=ROOT/name;target=out/f'{i}-{source.name}.bin'
    assert not target.exists()
    target.write_bytes(source.read_bytes())
    refs.append(dict(path=name,snapshot=target.relative_to(ROOT).as_posix(),sha256=hashlib.sha256(target.read_bytes()).hexdigest()))
archive=ROOT/'provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson'
records=[json.loads(line) for line in archive.read_text(encoding='utf-8').splitlines() if '/sh600519/qfq.js' in line or '/sh600519/hfq.js' in line]
source=ROOT/'provider_validation/tests/source_snapshots/a-stock-data/a_stock_missing_capabilities.py'
(out/'investigation.json').write_text(json.dumps(dict(before=refs,source_code=source.relative_to(ROOT).as_posix(),source_code_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),raw_manifest=archive.relative_to(ROOT).as_posix(),raw_manifest_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),source_records=records,scope='ASTOCK-006: both original qfq/hfq requests; selected candidate only, no price recomputation or production routing'),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print('factor baseline and original request references saved')
