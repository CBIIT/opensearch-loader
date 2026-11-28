"""Summarize completed, matched five-percent benchmark runs."""
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
RUNS={"thread_queries_kafka":"1795807480","process1_kafka":"1795807777","process2_kafka":"1795807298","process4_kafka":"1795808074","process2_direct":"1795807094"}
rows=[]
reference=None
for name,stamp in RUNS.items():
 p=ROOT/'tmp/kafka-five-percent'/('parallel-'+stamp)/'results.json'
 d=json.loads(p.read_text())
 docs={x['index']:(x['documents'],x['operations'],x['content_digest']) for x in d['indices']}
 if reference is None:reference=docs
 assert d['success'] and len(docs)==21 and docs==reference,(name,'validation mismatch')
 file=next(x for x in d['indices'] if x['index']=='dashboard_file')
 rows.append({'name':name,'artifact':str(p),'total_seconds':d['wall_seconds'],'dashboard_file_seconds':file['seconds'],'query_seconds_including_wait':file['query_seconds'],'documents':file['documents'],'write_operations':file['operations'],'matches_reference':True})
result={'scope':'5% source-root sample, all 21 indices; normalized array order; about/model full','index_tasks':4,'shared_write_workers':2,'max_batch_documents':5000,'runs':rows,'full_dataset_40_minutes_verified':False}
out=ROOT/'tmp/parallel-research/final-comparison.json';out.write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
