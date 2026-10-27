import argparse,json,random
from datetime import datetime,timedelta,timezone
from pathlib import Path
def generate(output,records,seed):
 rng=random.Random(seed); start=datetime(2026,1,1,tzinfo=timezone.utc); rows=[]; ncustomers=max(5,records//2)
 for i in range(records):
  cust=rng.randint(1,ncustomers); t=start+timedelta(minutes=rng.randint(0,max(1,records*4))); op='delete' if i>0 and i%23==0 else 'upsert'
  r={'event_id':f'evt_{i:07d}','customer_id':f'cust_{cust:06d}','source_updated_at':t.isoformat().replace('+00:00','Z'),'operation':op,'source_file':Path(output).name}
  if op=='upsert': r.update(name=f'Customer {cust}',email=None if i%11==0 else f'customer{cust}@example.test',amount=round(rng.uniform(0,10000),2))
  rows.append(r)
 rows += [{'event_id':'invalid_missing_name','customer_id':'bad1','source_updated_at':'2026-01-01T00:00:00Z','operation':'upsert','source_file':Path(output).name,'amount':5},{'event_id':'invalid_negative','customer_id':'bad2','name':'Invalid','email':None,'amount':-1,'source_updated_at':'2026-01-01T00:00:00Z','operation':'upsert','source_file':Path(output).name}]
 if rows: rows.append(dict(rows[0]))
 p=Path(output); p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',encoding='utf-8') as f:
  for r in rows:f.write(json.dumps(r)+'\n')
  f.write('{malformed json\n')
 print(f'Generated {len(rows)+1} lines; seed={seed}')
if __name__=='__main__':
 p=argparse.ArgumentParser(); p.add_argument('--output',default='data/sample_events.jsonl'); p.add_argument('--records',type=int,default=30); p.add_argument('--seed',type=int,default=42); a=p.parse_args(); generate(a.output,a.records,a.seed)
