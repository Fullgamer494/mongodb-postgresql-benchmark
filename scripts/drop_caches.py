#!/usr/bin/env python3
"""Manual host cache flush helper. Default mode only prints a dry-run."""
import argparse,json,os,subprocess,sys
from datetime import datetime,timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--apply',action='store_true',help='explicitly run sync and write 3 to /proc/sys/vm/drop_caches'); p.add_argument('--log',type=Path,default=ROOT/'results/cache_operations.jsonl'); a=p.parse_args()
    record={'timestamp_utc':datetime.now(timezone.utc).isoformat(),'operation':'sync; echo 3 > /proc/sys/vm/drop_caches','mode':'apply' if a.apply else 'dry_run','effective_uid':os.geteuid()}
    if not a.apply:
        record['status']='not_applied'; print(json.dumps(record,sort_keys=True)); return 0
    if os.geteuid()!=0:
        record.update(status='blocked',reason='requires root; no sudo escalation performed'); print(json.dumps(record,sort_keys=True),file=sys.stderr); return 2
    try:
        subprocess.run(['sync'],check=True)
        Path('/proc/sys/vm/drop_caches').write_text('3\n')
        record['status']='applied'
    except Exception as exc:
        record.update(status='error',reason=f'{type(exc).__name__}: {exc}')
    a.log.parent.mkdir(parents=True,exist_ok=True)
    with a.log.open('a',encoding='utf-8') as f: f.write(json.dumps(record,sort_keys=True)+'\n')
    print(json.dumps(record,sort_keys=True)); return 0 if record['status']=='applied' else 1
if __name__=='__main__': raise SystemExit(main())
