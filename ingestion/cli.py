import argparse,json,sys
from pathlib import Path
from .pipeline import run_pipeline
p=argparse.ArgumentParser(); p.add_argument('--input',required=True); p.add_argument('--db',default='outputs/ingestion.db'); p.add_argument('--fail-after',type=int); a=p.parse_args()
r=run_pipeline(a.input,a.db,a.fail_after); Path('outputs').mkdir(exist_ok=True); Path('outputs/last_run.json').write_text(json.dumps(r,indent=2)); print(json.dumps(r,indent=2)); sys.exit(0 if r['status']=='success' else 1)
