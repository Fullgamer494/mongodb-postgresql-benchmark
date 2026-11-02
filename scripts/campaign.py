#!/usr/bin/env python3
"""Deterministic campaign planner, checkpoint and serial executor."""
from __future__ import annotations
import argparse, hashlib, itertools, json, random, subprocess, sys, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
MATRIX=ROOT/'config/matrix.json'
PROFILES=ROOT/'config/resource_profiles.json'

def sha(path: Path) -> str: return hashlib.sha256(path.read_bytes()).hexdigest()
def config_hash() -> str:
    h=hashlib.sha256()
    for path in (MATRIX, PROFILES, ROOT/'docker-compose.yml', ROOT/'config/images.lock.json', ROOT/'config/mongod.yml', ROOT/'contracts/results_header.csv'):
        h.update(path.relative_to(ROOT).as_posix().encode()+b'\0'+path.read_bytes())
    return h.hexdigest()
def make_plan(kind: str) -> dict:
    matrix=json.loads(MATRIX.read_text()); profiles=json.loads(PROFILES.read_text()); cfg=config_hash()
    queries=matrix['base_queries'] if kind=='base' else matrix['extended_queries']
    cells=list(itertools.product(matrix['profiles'],queries,matrix['concurrency'],range(1,int(matrix['repetitions'])+1)))
    rng=random.Random(int(matrix['schedule_seed'])); rng.shuffle(cells)
    runs=[]
    for profile,query,concurrency,rep in cells:
        pair=[{'engine':engine,'profile':profile,'query':query,'treatment':matrix['q2_treatments'].get(engine,{}).get(query,{}),'concurrency':concurrency,'run_number':rep} for engine in matrix['engines']]
        if rng.randrange(2): pair.reverse()
        runs.extend(pair)
    seconds=int(matrix['warmup_seconds'])+int(matrix['measurement_seconds'])+int(matrix['cooldown_seconds'])
    return {'schema_version':1,'campaign_id':f"{kind}-{cfg[:12]}",'mode':kind,'config_hash':cfg,'schedule_seed':matrix['schedule_seed'],'schedule_policy':'seeded_shuffle_of_cells; paired engines adjacent; order randomized per pair; one cell and one engine at a time','expected_runs':int(matrix['expected_runs'][kind]),'actual_plan_runs':len(runs),'per_run_minimum_seconds':seconds,'minimum_duration_seconds':seconds*len(runs),'minimum_duration_hms':f"{seconds*len(runs)//3600:02d}:{seconds*len(runs)%3600//60:02d}:{seconds*len(runs)%60:02d}",'resources':profiles,'cache_protocol':'alternative_no_host_drop_caches; no host operation executed','runs':runs}
def cmd(args:list[str], *, check=True):
    p=subprocess.run(args,cwd=ROOT,text=True,capture_output=True)
    if check and p.returncode: raise RuntimeError(f"comando falló ({p.returncode}): {' '.join(args)}\n{p.stdout[-1200:]}\n{p.stderr[-1200:]}")
    return p
def prom_ready() -> bool:
    for query in ('up{job="cadvisor"}','up{job="node-exporter"}'):
        url='http://127.0.0.1:9090/api/v1/query?'+urllib.parse.urlencode({'query':query})
        try:
            with urllib.request.urlopen(url,timeout=5) as r: doc=json.load(r)
            if doc.get('status')!='success' or not doc.get('data',{}).get('result') or any(float(x['value'][1])!=1 for x in doc['data']['result']): return False
        except Exception: return False
    return True
def engine_service(engine:str)->str: return engine
def is_terminal_engine_timeout(quality:dict)->bool:
    """A server-enforced query timeout is a preserved engine outcome, not infra."""
    for error in quality.get('errors',[]):
        message=str(error.get('message','')).lower()
        if error.get('phase') not in ('warmup','measurement','drain'):
            continue
        if error.get('type')=='ExecutionTimeout' and ('maxtimemsexpired' in message or 'operation exceeded time limit' in message):
            return True
        if error.get('type')=='QueryCanceled' and 'statement timeout' in message:
            return True
    return False
def resume_action(attempts:list[dict], allow_remediation_retry:bool=False)->str:
    """Never repeats recorded results; allows one new attempt for infra-only failures."""
    if not attempts: return 'run'
    last=attempts[-1].get('classification')
    if last in ('experimental_result','success'): return 'skip'
    if last=='infrastructure_error' and len(attempts)<2: return 'retry_infrastructure_once'
    if last=='infrastructure_error' and len(attempts)==2 and allow_remediation_retry: return 'retry_after_remediation_once'
    return 'block'

def execute(plan:dict, checkpoint:Path, resume:bool, output_root:Path, allow_remediation_retry:bool=False):
    if not prom_ready(): raise RuntimeError('Prometheus/cAdvisor/Node Exporter no están listos; campaña bloqueada antes de medir')
    if checkpoint.exists():
        if not resume: raise FileExistsError(f'Checkpoint existente; use --resume: {checkpoint}')
        state=json.loads(checkpoint.read_text())
        if state.get('config_hash')!=plan['config_hash'] or state.get('campaign_id')!=plan['campaign_id']:
            raise RuntimeError('checkpoint pertenece a otra configuración/matriz')
        if state.get('output_root',str(ROOT/'results/official'))!=str(output_root):
            raise RuntimeError('output-root no coincide con el checkpoint')
    else:
        if resume: raise FileNotFoundError(f'no existe checkpoint para reanudar: {checkpoint}')
        state={**{k:v for k,v in plan.items() if k!='runs'},'output_root':str(output_root),'status':'running','created_at_utc':datetime.now(timezone.utc).isoformat(),'attempts':[],'completed_keys':[],'blocked_keys':[]}
    state['status']='running'
    completed=set(state.get('completed_keys',[])); attempts_by_key={}
    for a in state['attempts']: attempts_by_key.setdefault(a['key'],[]).append(a)
    checkpoint.parent.mkdir(parents=True,exist_ok=True)
    def save(): checkpoint.write_text(json.dumps(state,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    work_dir=checkpoint.parent/f"{checkpoint.stem}.work"
    work_dir.mkdir(parents=True,exist_ok=True)
    env_file=work_dir/'resources.env'
    for i,cell in enumerate(plan['runs'],1):
        key='|'.join([cell['engine'],cell['profile'],cell['query'],str(cell['concurrency']),str(cell['run_number']),plan['config_hash']])
        if key in completed: continue
        attempts=attempts_by_key.get(key,[])
        if attempts:
            action=resume_action(attempts,allow_remediation_retry)
            if action=='block':
                state['blocked_keys'].append({'key':key,'reason':'intento previo no es un éxito reproducible ni un fallo de infraestructura reintentable'}); state['status']='blocked'; save(); break
            if action=='skip':
                continue
        profile=json.loads(PROFILES.read_text())[cell['profile']]
        env_file.write_text(f"LAB_PROFILE={cell['profile']}\nLAB_CPUS={float(profile['cpus']):g}\nLAB_MEMORY_MIB={int(profile['memory_mib'])}\n")
        compose=['docker','compose','--env-file','.env','--env-file',str(env_file)]
        other='postgresql' if cell['engine']=='mongodb' else 'mongodb'
        cmd(compose+['stop',other],check=False)
        cmd(compose+['up','-d','--no-deps','--wait','--wait-timeout','120',engine_service(cell['engine'])])
        # Keep evidence from every retry. preflight.py intentionally refuses to
        # overwrite existing evidence, so retries need their own output path.
        preflight=work_dir/f"preflight-{i:03d}-attempt-{len(attempts)+1:02d}.json"
        pf=cmd([sys.executable,'scripts/preflight.py','--engine',cell['engine'],'--profile',cell['profile'],'--output',str(preflight)],check=False)
        if pf.returncode or not json.loads(preflight.read_text()).get('ready') or not prom_ready():
            state['blocked_keys'].append({'key':key,'reason':'preflight o telemetría no listos','output':pf.stdout[-1000:]+pf.stderr[-1000:]}); state['status']='blocked'; save(); break
        engine_code={'mongodb':'mg','postgresql':'pg'}[cell['engine']]; profile_code={'PROFILE_A_BASELINE':'a','PROFILE_B_MEDIUM':'b','PROFILE_C_SEVERE':'c'}[cell['profile']]
        run_id=f"{engine_code}-{cell['query'].lower()}-{profile_code}-c{cell['concurrency']}-r{cell['run_number']}-o-{int(time.time())}-{i:03d}"
        attempt={'key':key,'run_id':run_id,'cell':cell,'started_at_utc':datetime.now(timezone.utc).isoformat(),'classification':'running'}
        state['attempts'].append(attempt); attempts_by_key.setdefault(key,[]).append(attempt); save()
        print(json.dumps({'event':'cell_started','index':i,'total':plan['expected_runs'],'run_id':run_id,'cell':cell},ensure_ascii=False,sort_keys=True),flush=True)
        p=cmd([sys.executable,'-m','benchmark_runner','--engine',cell['engine'],'--profile',cell['profile'],'--query',cell['query'],'--concurrency',str(cell['concurrency']),'--run-number',str(cell['run_number']),'--mode','official','--run-id',run_id,'--output-root',str(output_root)],check=False)
        result=output_root/run_id
        attempt['returncode']=p.returncode; attempt['result_directory']=str(result.relative_to(ROOT)); attempt['finished_at_utc']=datetime.now(timezone.utc).isoformat()
        if (result/'quality.json').exists():
            quality=json.loads((result/'quality.json').read_text()); status=quality.get('status'); attempt['status']=status
            if status in ('ok','spill','oom'):
                attempt['classification']='experimental_result'; completed.add(key); state['completed_keys']=sorted(completed)
            elif is_terminal_engine_timeout(quality):
                attempt['classification']='engine_error_result'
                attempt['classification_reason']='MongoDB enforced maxTimeMS before a valid measurement window; preserve status=error without fabricating metrics.'
                completed.add(key); state['completed_keys']=sorted(completed)
            else:
                # Do not silently repeat engine errors or unclassified outcomes.
                attempt['classification']='infrastructure_error' if not quality.get('telemetry_complete') or quality.get('spill',{}).get('status')=='unknown' or quality.get('oom',{}).get('status')=='unknown' else 'engine_error'
        else: attempt['classification']='infrastructure_error'; attempt['status']='no_quality_artifact'
        attempt['stdout_tail']=p.stdout[-1000:]; attempt['stderr_tail']=p.stderr[-1000:]
        print(json.dumps({'event':'cell_finished','index':i,'total':plan['expected_runs'],'run_id':run_id,'status':attempt.get('status'),'classification':attempt['classification']},ensure_ascii=False,sort_keys=True),flush=True)
        if attempt['classification'] in ('engine_error','infrastructure_error'):
            state['status']='paused_after_error'; save(); break
        save()
    if state['status']=='running': state['status']='complete' if len(completed)>=plan['expected_runs'] else 'incomplete'
    state['updated_at_utc']=datetime.now(timezone.utc).isoformat(); save()
    return state
def main():
    ap=argparse.ArgumentParser(description=__doc__); ap.add_argument('--mode',choices=('base','extended'),default='extended'); ap.add_argument('--execute',action='store_true'); ap.add_argument('--resume',action='store_true'); ap.add_argument('--retry-infrastructure-after-remediation',action='store_true',help='autoriza un reintento adicional de infraestructura, una vez, tras documentar una corrección'); ap.add_argument('--checkpoint',type=Path); ap.add_argument('--output-root',type=Path,default=Path('results/official'),help='directorio separado para esta campaña') ; args=ap.parse_args()
    plan=make_plan(args.mode)
    if plan['actual_plan_runs']!=plan['expected_runs']: raise SystemExit(f"Error matriz: {plan['actual_plan_runs']} != {plan['expected_runs']}")
    if not args.execute:
        print(json.dumps(plan,ensure_ascii=False,sort_keys=True,indent=2)); return 0
    checkpoint=args.checkpoint or ROOT/'results/campaigns'/f"{plan['campaign_id']}.checkpoint.json"
    checkpoint=(ROOT/checkpoint).resolve() if not checkpoint.is_absolute() else checkpoint.resolve()
    output_root=(ROOT/args.output_root).resolve() if not args.output_root.is_absolute() else args.output_root.resolve()
    state=execute(plan,checkpoint,args.resume,output_root,args.retry_infrastructure_after_remediation)
    print(json.dumps({'campaign_id':state['campaign_id'],'status':state['status'],'completed':len(state['completed_keys']),'attempts':len(state['attempts']),'checkpoint':str(checkpoint.relative_to(ROOT)) if checkpoint.is_relative_to(ROOT) else str(checkpoint),'output_root':str(output_root.relative_to(ROOT)) if output_root.is_relative_to(ROOT) else str(output_root)},ensure_ascii=False,sort_keys=True))
    return 0 if state['status']=='complete' else 1
if __name__=='__main__':
    try: raise SystemExit(main())
    except (RuntimeError,FileExistsError,FileNotFoundError,ValueError) as e: print(f'Campaña detenida: {e}',file=sys.stderr); raise SystemExit(2)
