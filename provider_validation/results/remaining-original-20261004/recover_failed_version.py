"""Recover the earlier failed verification run's exact execution bytes by hash."""
from pathlib import Path
import hashlib,json,re
ROOT=Path(__file__).resolve().parents[3]
results=ROOT/'provider_validation/results'
target=results/'remaining-final-20261004'
refs={}
for p in target.rglob('report.json'):
    report=json.loads(p.read_text(encoding='utf-8'))
    for ref in report.get('code_files',[])+report.get('config_files',[]):refs[ref['path']]=ref['sha256']
available={}
for root in [results/'remaining-original-20261004',results/'remaining-original-smoke-20261004/version',results/'remaining-sdk-smoke-20261004/version']:
    for p in root.glob('*.bin'):available[hashlib.sha256(p.read_bytes()).hexdigest()]=p.read_bytes()
for name in refs:
    p=Path(name)
    if p.exists():available[hashlib.sha256(p.read_bytes()).hexdigest()]=p.read_bytes()
source=(ROOT/'src/stock_data_manage/pipeline/inputs.py').read_text(encoding='utf-8')
source=source.replace('"ASTOCK-031","ASTOCK-039"','"ASTOCK-039"').replace('"ASTOCK-068","ASTOCK-069","ASTOCK-085"','"ASTOCK-068","ASTOCK-085"')
source=source.replace('                  Path(__file__).parents[1] / "providers/eastmoney/news.py",\n','')
source=source.replace('"ASTOCK-031":"stock_news_em", "ASTOCK-069":"stock_zh_index_value_csindex", ','')
source=source.replace('|cookie)["\']',')["\']')
source=re.sub(r"                if input_id in \{'ASTOCK-031','ASTOCK-069'\}:\n.*?(?=                code_version =)",'',source,flags=re.S)
source=re.sub(r"                    if input_id in \{'ASTOCK-031','ASTOCK-069'\}:\n.*?(?=                    report\[\"source_sdk_transport_policy\"\]|                if |            )",'',source,flags=re.S)
# Remove the native policy block explicitly (its following shared lines remain intact).
start=source.find("                    if input_id in {'ASTOCK-031','ASTOCK-069'}:")
if start>=0:
    end=source.find('\n                ',start+1)
    source=source[:start]+source[end+1:]
source=source.replace("cache_ignored_query_parameters=(\"_\",) if is_reportapi else (),\n                    native_transport='curl_cffi' if input_id=='ASTOCK-031' else 'pandas_urllib' if input_id=='ASTOCK-069' else None))", "cache_ignored_query_parameters=(\"_\",) if is_reportapi else ()))")
source=source.replace("'发布时间' if input_id=='ASTOCK-031' else ",'').replace("{'ASTOCK-067','ASTOCK-068','ASTOCK-069'}","{'ASTOCK-067','ASTOCK-068'}")
available[hashlib.sha256(source.encode()).hexdigest()]=source.encode()
missing=[dict(path=n,sha256=h) for n,h in refs.items() if h not in available]
out=target/'version';out.mkdir(exist_ok=False)
index=[]
for i,(name,digest) in enumerate(refs.items()):
    if digest not in available:continue
    p=out/(str(i)+'-'+Path(name).name+'.bin');p.write_bytes(available[digest])
    index.append(dict(path=name,sha256=digest,snapshot=str(p.relative_to(ROOT))))
(out/'index.json').write_bytes((json.dumps(index,ensure_ascii=False,indent=2)+'\n').encode())
(out/'recovery.json').write_bytes((json.dumps(dict(recovered=len(index),missing=missing,validation_result='failed guard assertion; partial development evidence only'),ensure_ascii=False,indent=2)+'\n').encode())
print('recovered',len(index),'missing',missing)
