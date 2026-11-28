import json,time,hashlib
from pathlib import Path
import mgclient,yaml
root=Path(__file__).resolve().parents[2]
original=next(s for s in yaml.safe_load(Path('/tmp/opensearch-indices-4.9.0-review.yaml').read_text())['indices'] if s['index_name']=='gs_file')['initial_query']['query']
revised=(root/'queries/gs_file_initial.cypher').read_text()
c=mgclient.connect(host='127.0.0.1',port=17689);c.autocommit=True
q=c.cursor();q.execute('MATCH (f:file) RETURN count(f), count(DISTINCT f.file_id), count(f.file_id)');counts=q.fetchone();assert len(set(counts))==1,counts
result={'counts':counts,'pages':[]}
for skip in (0,300000,570000,620000):
 pages=[];timings=[]
 for query in (original,revised):
  start=time.monotonic();q.execute(query,{'skip':skip,'limit':5000});pages.append(q.fetchall());timings.append(time.monotonic()-start)
 assert pages[0]==pages[1],skip
 row={'skip':skip,'rows':len(pages[1]),'original_seconds':timings[0],'revised_seconds':timings[1],'equal':True};result['pages'].append(row);print(row,flush=True)
ids=set();started=time.monotonic()
for skip in range(0,counts[0]+5000,5000):
 q.execute(revised,{'skip':skip,'limit':5000});rows=q.fetchall()
 for r in rows:
  assert r[0] not in ids,r[0]
  ids.add(r[0])
 if not rows:break
assert len(ids)==counts[0]
result.update(full_query_seconds=time.monotonic()-started,unique_documents=len(ids),success=True)
(root/'tmp/parallel-research/gs-file-query-validation.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True);c.close()
