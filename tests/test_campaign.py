import csv,json,tempfile,unittest,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from campaign import make_plan,resume_action
from validate_results import HEADER,validate
class CampaignTests(unittest.TestCase):
    def test_resume_never_repeats_results_and_bounds_infrastructure_retry(self):
        self.assertEqual(resume_action([]),'run')
        self.assertEqual(resume_action([{'classification':'experimental_result'}]),'skip')
        self.assertEqual(resume_action([{'classification':'infrastructure_error'}]),'retry_infrastructure_once')
        self.assertEqual(resume_action([{'classification':'infrastructure_error'},{'classification':'infrastructure_error'}]),'block')
        self.assertEqual(resume_action([{'classification':'engine_error'}]),'block')
    def test_base_and_extended_expansions_are_deterministic_and_paired(self):
        for mode,count in [('base',108),('extended',162)]:
            a=make_plan(mode); b=make_plan(mode)
            self.assertEqual(a,b); self.assertEqual(a['actual_plan_runs'],count); self.assertEqual(a['expected_runs'],count)
            self.assertEqual(a['minimum_duration_seconds'],count*420)
            self.assertEqual(a['minimum_duration_hms'],'12:36:00' if mode=='base' else '18:54:00')
            self.assertTrue(all(a['runs'][i]['profile']==a['runs'][i+1]['profile'] and a['runs'][i]['query']==a['runs'][i+1]['query'] and a['runs'][i]['concurrency']==a['runs'][i+1]['concurrency'] and a['runs'][i]['run_number']==a['runs'][i+1]['run_number'] and a['runs'][i]['engine']!=a['runs'][i+1]['engine'] for i in range(0,count,2)))
    def test_validator_accepts_known_fixture_and_detects_bad_header(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t); run=root/'r1'; (run/'telemetry').mkdir(parents=True); (run/'host_events').mkdir()
            row=['1','mongodb','PROFILE_A_BASELINE','Q1','1','1','1.0','1','1','1','','1','false','ok']
            (run/'summary.csv').write_text(','.join(HEADER)+'\n'+','.join(row)+'\n')
            (run/'manifest.json').write_text(json.dumps({'run_id':'r1'})); (run/'quality.json').write_text(json.dumps({'run_id':'r1','status':'ok','measurement_valid':True}))
            for x in ('requests.jsonl','auxiliary.json','telemetry/metrics.json','host_events/evidence.json'): (run/x).write_text('{}\n')
            self.assertEqual(validate(root)['status'],'ok')
            (run/'summary.csv').write_text('wrong\n'+','.join(row)+'\n')
            report=validate(root); self.assertEqual(report['status'],'error'); self.assertTrue(any('cabecera' in e for e in report['errors']))
if __name__=='__main__': unittest.main()
