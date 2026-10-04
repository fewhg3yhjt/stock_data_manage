"""Seal only this task's artifacts and compare all staged bytes before committing."""
from pathlib import Path
from datetime import datetime,timezone
import argparse,hashlib,json,subprocess
ROOT=Path(__file__).resolve().parents[3]
RESULTS=ROOT/'provider_validation/results'
FINAL=RESULTS/'remaining-final-v2-20261004'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--staged',action='store_true');args=parser.parse_args()
    target=FINAL/('staged-check.json' if args.staged else 'artifact-index.json')
    assert not target.exists(),'sealed artifacts are immutable'
    if not args.staged:
        files=[ROOT/p for p in json.loads((Path(__file__).parent/'task-files.json').read_text(encoding='utf-8'))]
        files += [p for directory in RESULTS.glob('remaining-*-20261004') for p in directory.rglob('*')
            if p.is_file() and '__pycache__' not in p.parts and p.suffix!='.pyc' and p.name not in {'artifact-index.json','staged-check.json'}]
        refs=[dict(path=p.relative_to(ROOT).as_posix(),sha256=sha(p),bytes=p.stat().st_size) for p in sorted(set(files))]
        result=dict(time_utc=datetime.now(timezone.utc).isoformat(),files=refs,excluded_generated_paths=['artifact-index.json','staged-check.json'],
            scope='only this task source/config/docs and remaining-* evidence; unrelated user files and private test roots excluded',production_writes=0)
    else:
        index=json.loads((FINAL/'artifact-index.json').read_text(encoding='utf-8'))
        expected={r['path']:r['sha256'] for r in index['files']}
        expected[(FINAL/'artifact-index.json').relative_to(ROOT).as_posix()]=sha(FINAL/'artifact-index.json')
        names=subprocess.check_output(['git','diff','--cached','--name-only','-z'],cwd=ROOT).decode().split('\0')
        names=[n for n in names if n]
        assert set(names)==set(expected),(set(names)-set(expected),set(expected)-set(names))
        assert 'AGENTS.md' not in names
        assert not any(p.lower().endswith(('.env','.pem','.key')) or '/.env' in p.lower() for p in names)
        process=subprocess.Popen(['git','cat-file','--batch'],cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
        try:
            for name in names:
                process.stdin.write((':'+name+'\n').encode());process.stdin.flush()
                header=process.stdout.readline().split();assert header[1]==b'blob'
                size=int(header[2]);content=process.stdout.read(size);assert process.stdout.read(1)==b'\n'
                assert hashlib.sha256(content).hexdigest()==expected[name]==sha(ROOT/name),name
        finally:
            process.stdin.close();process.wait();assert process.returncode==0
        subprocess.run(['git','diff','--cached','--check'],cwd=ROOT,check=True)
        result=dict(result='passed',validation_time_utc=datetime.now(timezone.utc).isoformat(),staged_files_verified=len(names),
            artifact_index_sha256=sha(FINAL/'artifact-index.json'),unrelated_user_files_staged=False,
            bytes_equal=True,production_writes=0,scope='all indexed files plus index itself; this staged-check record is added after the check')
    target.write_bytes((json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode())
    print('sealed',target.name,'files',len(result.get('files',[])) or result.get('staged_files_verified'))
if __name__=='__main__':main()
