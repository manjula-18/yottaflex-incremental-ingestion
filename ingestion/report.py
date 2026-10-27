import argparse,json
from pathlib import Path
from .pipeline import connect
p=argparse.ArgumentParser(); p.add_argument('--db',default='outputs/ingestion.db'); p.add_argument('--output',default='outputs/report.json'); a=p.parse_args(); c=connect(a.db)
r={'active_target_rows':c.execute('SELECT COUNT(*) FROM customers WHERE is_deleted=0').fetchone()[0],'deleted_tombstones':c.execute('SELECT COUNT(*) FROM customers WHERE is_deleted=1').fetchone()[0],'processed_unique_events':c.execute('SELECT COUNT(*) FROM processed_events').fetchone()[0],'quarantined_records':c.execute('SELECT COUNT(*) FROM quarantine').fetchone()[0],'active_amount_total':round(c.execute('SELECT COALESCE(SUM(amount),0) FROM customers WHERE is_deleted=0').fetchone()[0],2),'watermark':c.execute("SELECT value FROM pipeline_state WHERE key='watermark'").fetchone()[0],'duplicate_target_keys':c.execute('SELECT COUNT(*) FROM (SELECT customer_id FROM customers GROUP BY customer_id HAVING COUNT(*)>1)').fetchone()[0]}
c.close(); Path(a.output).parent.mkdir(parents=True,exist_ok=True); Path(a.output).write_text(json.dumps(r,indent=2)); print(json.dumps(r,indent=2))
