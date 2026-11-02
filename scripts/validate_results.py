#!/usr/bin/env python3
"""Validate fixed CSV contracts and run sidecars; optionally audit campaign completeness."""
from __future__ import annotations
import argparse,csv,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
HEADER=next(csv.reader((ROOT/'contracts/results_header.csv').open(encoding='utf-8')))
VALID={'ok','spill','oom','error'}
def is_terminal_engine_timeout(quality:dict)->bool:
    for error in quality.get('errors',[]):
        message=str(error.get('message','')).lower()
        if error.get('phase') not in ('warmup','measurement','drain'):
            continue
        if error.get('type')=='ExecutionTimeout' and ('maxtimemsexpired' in message or 'operation exceeded time limit' in message):
            return True
        if error.get('type')=='QueryCanceled' and 'statement timeout' in message:
            return True
    return False
def validate(root:Path, expected_keys:set[tuple]|None=None)->dict:
    errors=[]; runs=[]; seen=set(); completed_cells=set(); unmeasured_attempts=0
    if not root.exists(): return {'status':'error','errors':[f'no existe {root}'],'run_count':0,'rows':[]}
    for folder in sorted(p for p in root.iterdir() if p.is_dir()):
        summary=folder/'summary.csv'; manifest=folder/'manifest.json'; quality=folder/'quality.json'
        if not all(x.is_file() for x in (summary,manifest,quality)):
            errors.append(f'{folder.name}: falta summary.csv, manifest.json o quality.json'); continue
        try:
            with summary.open(newline='',encoding='utf-8') as f:
                reader=csv.reader(f); header=next(reader); rows=list(reader)
            if header!=HEADER: errors.append(f'{folder.name}: cabecera CSV no coincide con contrato')
            if len(rows)!=1 or len(rows[0])!=len(HEADER): errors.append(f'{folder.name}: se esperaba exactamente una fila con {len(HEADER)} campos'); continue
            row=dict(zip(HEADER,rows[0])); m=json.loads(manifest.read_text()); q=json.loads(quality.read_text())
            if m.get('run_id')!=folder.name or q.get('run_id')!=folder.name: errors.append(f'{folder.name}: run_id inconsistente')
            if row['status'] not in VALID: errors.append(f"{folder.name}: estado CSV inválido {row['status']!r}")
            if q.get('status')!=row['status']: errors.append(f'{folder.name}: status difiere entre CSV y quality')
            cell=tuple(row[k] for k in ('engine','profile','query_id','concurrency','run_number'))
            measured=q.get('measurement_valid') is True
            engine_timeout=is_terminal_engine_timeout(q)
            if expected_keys is not None and cell not in expected_keys: errors.append(f'{folder.name}: celda fuera del plan {cell}')
            if measured or engine_timeout:
                if cell in completed_cells: errors.append(f'{folder.name}: resultado terminal duplicado para la celda {cell}')
                completed_cells.add(cell)
            else:
                if row['status']!='error': errors.append(f'{folder.name}: resultado {row["status"]!r} sin ventana de medición válida')
                unmeasured_attempts+=1
            if folder.name in seen: errors.append(f'{folder.name}: run_id duplicado')
            seen.add(folder.name)
            for name in ('requests.jsonl','auxiliary.json','telemetry/metrics.json','host_events/evidence.json'):
                if not (folder/name).is_file(): errors.append(f'{folder.name}: falta {name}')
            runs.append({'run_id':folder.name,'cell':cell,'status':row['status'],'measurement_valid':q.get('measurement_valid'),'engine_timeout_result':engine_timeout,'unmeasured_attempt':not measured and not engine_timeout})
        except Exception as exc: errors.append(f'{folder.name}: lectura inválida {type(exc).__name__}: {exc}')
    if expected_keys is not None:
        observed=set(completed_cells)
        missing=expected_keys-observed; extra=observed-expected_keys
        if missing: errors.append(f'faltan {len(missing)} celdas esperadas')
        if extra: errors.append(f'hay {len(extra)} celdas fuera del plan')
    return {'status':'ok' if not errors else 'error','root':str(root),'run_count':len(runs),'completed_count':len(completed_cells),'unmeasured_attempt_count':unmeasured_attempts,'rows':runs,'errors':errors,'expected_count':len(expected_keys) if expected_keys is not None else None}
def main()->int:
    ap=argparse.ArgumentParser(description=__doc__); ap.add_argument('--root',type=Path,default=ROOT/'results/official'); ap.add_argument('--plan',type=Path,help='Plan JSON de run_benchmarks.sh --mode base|extended'); ap.add_argument('--output',type=Path); a=ap.parse_args()
    expected=None
    if a.plan:
        plan=json.loads(a.plan.read_text()); expected={(r['engine'],r['profile'],r['query'],str(r['concurrency']),str(r['run_number'])) for r in plan['runs']}
    report=validate(a.root,expected); raw=json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n'
    if a.output:
        out=a.output if a.output.is_absolute() else ROOT/a.output
        if out.exists(): print(f'No se sobrescribe: {out}',file=sys.stderr); return 2
        out.parent.mkdir(parents=True,exist_ok=True); out.write_text(raw,encoding='utf-8')
    print(raw,end=''); return 0 if report['status']=='ok' else 1
if __name__=='__main__': raise SystemExit(main())
