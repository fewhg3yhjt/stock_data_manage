"""Make unsupported filters explicit in the same existing contracts."""
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
path=ROOT/'config/providers.yaml';text=path.read_text(encoding='utf-8')
full=['ASTOCK-003','ASTOCK-030','ASTOCK-051','ASTOCK-055','ASTOCK-071','ASTOCK-072','ASTOCK-073','ASTOCK-085']
singles=['ASTOCK-004','ASTOCK-031','ASTOCK-039','ASTOCK-067','ASTOCK-068','ASTOCK-069','ASTOCK-077','ASTOCK-063']
for id in full+singles:
    start=text.index('  '+id+':');end=text.find('\n  ASTOCK-',start+1)
    block=text[start:end];assert '    forbidden_sources:' not in block
    forbidden=['request.symbols','config.limit','config.count']
    if id in full or id in ['ASTOCK-067','ASTOCK-068','ASTOCK-069','ASTOCK-077','ASTOCK-063']:forbidden.append('request.symbol')
    if id not in ['ASTOCK-063']:forbidden+=['request.start_date','request.end_date']
    if id in singles:forbidden.append('request.trade_date')
    if id in ['ASTOCK-071','ASTOCK-072']:forbidden.append('request.contract')
    block+='\n    forbidden_sources:\n'+''.join('    - '+f+'\n' for f in forbidden).rstrip()
    if id=='ASTOCK-069':block+='\n    base_requests_per_fetch: 2'
    if id=='ASTOCK-073':block+='\n    base_requests_per_fetch: 8'
    if id=='ASTOCK-003':block+='\n    base_requests_per_fetch: 3'
    text=text[:start]+block+text[end:]
path.write_bytes(text.encode())
