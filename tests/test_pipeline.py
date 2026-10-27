import json
from ingestion.pipeline import run_pipeline,connect
def ev(e='e1',c='c1',t='2026-01-01T00:00:00Z',amount=10,op='upsert',**kw):
 r={'event_id':e,'customer_id':c,'source_updated_at':t,'operation':op,'source_file':'test.jsonl'}
 if op=='upsert':r.update(name='Alice',email=None,amount=amount)
 r.update(kw); return r
def src(tmp,name,rows,malformed=False):
 p=tmp/name
 with p.open('w') as f:
  for r in rows:f.write(json.dumps(r)+'\n')
  if malformed:f.write('{bad json\n')
 return str(p)
def query(db,q):
 c=connect(db); x=c.execute(q).fetchall(); c.close(); return x
def test_happy_rerun(tmp_path):
 db=str(tmp_path/'x.db'); p=src(tmp_path,'a.jsonl',[ev(),ev('e2','c2')]); a=run_pipeline(p,db); b=run_pipeline(p,db)
 assert a['applied_count']==2 and b['applied_count']==0 and b['duplicate_count']==2
 assert len(query(db,'select * from customers'))==2

def test_empty(tmp_path):
 p=src(tmp_path,'empty.jsonl',[]); r=run_pipeline(p,str(tmp_path/'x.db')); assert r['status']=='success' and r['input_count']==0

def test_bad_json_missing_null_and_schema_change(tmp_path):
 db=str(tmp_path/'x.db'); missing={'event_id':'e2','customer_id':'c2','source_updated_at':'2026-01-01T00:00:00Z','operation':'upsert','source_file':'x'}
 p=src(tmp_path,'bad.jsonl',[ev('e1'),missing,ev('e3',name=None),ev('e4',unexpected='field')],True); r=run_pipeline(p,db)
 assert r['rejected_count']==4 and len(query(db,'select * from quarantine'))==4

def test_duplicate_id_in_batch(tmp_path):
 db=str(tmp_path/'x.db'); p=src(tmp_path,'dup.jsonl',[ev('same',amount=10),ev('same',amount=20)]); r=run_pipeline(p,db)
 assert r['rejected_count']==1 and query(db,'select amount from customers')[0][0]==10

def test_late_event_does_not_overwrite_newer(tmp_path):
 db=str(tmp_path/'x.db'); p1=src(tmp_path,'new.jsonl',[ev('new','c1','2026-01-03T00:00:00Z',30)]); p2=src(tmp_path,'late.jsonl',[ev('old','c1','2026-01-02T00:00:00Z',20)])
 run_pipeline(p1,db); r=run_pipeline(p2,db); assert r['applied_count']==0 and query(db,'select amount from customers')[0][0]==30 and len(query(db,'select * from processed_events'))==2

def test_tombstone_blocks_older_resurrection(tmp_path):
 db=str(tmp_path/'x.db'); run_pipeline(src(tmp_path,'a.jsonl',[ev()]),db); run_pipeline(src(tmp_path,'d.jsonl',[ev('del','c1','2026-01-02T00:00:00Z',op='delete')]),db); run_pipeline(src(tmp_path,'old.jsonl',[ev('old','c1','2025-12-31T00:00:00Z')]),db)
 row=query(db,'select is_deleted,amount from customers')[0]; assert row[0]==1 and row[1] is None

def test_invalid_operation_and_negative_amount(tmp_path):
 db=str(tmp_path/'x.db'); p=src(tmp_path,'bad.jsonl',[ev('e1',amount=-2),ev('e2',op='merge')]); r=run_pipeline(p,db); assert r['rejected_count']==2

def test_failure_rollback_retry(tmp_path):
 db=str(tmp_path/'x.db'); p=src(tmp_path,'batch.jsonl',[ev('e1','c1'),ev('e2','c2'),ev('e3','c3')]); a=run_pipeline(p,db,fail_after=1)
 assert a['status']=='failed' and len(query(db,'select * from customers'))==0 and len(query(db,'select * from processed_events'))==0
 b=run_pipeline(p,db); assert b['status']=='success' and b['applied_count']==3

def test_watermark_and_late_processing(tmp_path):
 db=str(tmp_path/'x.db'); run_pipeline(src(tmp_path,'a.jsonl',[ev('e1','c1','2026-01-10T00:00:00Z')]),db); run_pipeline(src(tmp_path,'b.jsonl',[ev('e2','c2','2026-01-01T00:00:00Z')]),db)
 assert query(db,"select value from pipeline_state where key='watermark'")[0][0]=='2026-01-10T00:00:00Z' and len(query(db,'select * from processed_events'))==2
