"""One shared two-consumer Kafka pipeline fed by independent Loader query threads."""
import json,time,threading,hashlib
from concurrent.futures import ThreadPoolExecutor,as_completed
from copy import deepcopy
from benchmark import ROOT,OUT,Filtered,Config,Loader,normalized
from opensearchpy.helpers import scan
from transport import KafkaWrites,MAX_DOCUMENTS
from opensearch_loader.partitioned import ParallelWrites
from opensearch_loader.opensearch_client import OpenSearchClient

def run(specs,workers,memgraph_port=17688,sample_percent=5,query_processes=False,query_workers=None,write_mode="kafka"):
 assert 1<=workers<=4
 stamp=str(int(time.time()));out=OUT/('parallel-'+stamp);out.mkdir()
 (out/'manifest.json').write_text(json.dumps({'query_processes':query_processes,'sample_percent':sample_percent,'index_workers':workers,'consumers':2,'max_batch_documents':MAX_DOCUMENTS,'memgraph_port':memgraph_port},indent=2))
 print('ARTIFACT_DIR',str(out),flush=True)
 client=OpenSearchClient('http://127.0.0.1:19204');assert client.client.cluster.health()['number_of_data_nodes']==2
 setup_start=time.monotonic()
 query_pool=None
 if query_processes:
  from process_queries import make_pool,ready
  query_pool=make_pool(query_workers or workers,'127.0.0.1',memgraph_port)
  list(query_pool.map(ready,range((query_workers or workers)*4)))
 writer=KafkaWrites(client,'os-loader-parallel-'+stamp,out) if write_mode=='kafka' else ParallelWrites(client,2);setup_seconds=time.monotonic()-setup_start;start=time.monotonic();results=[];active=0;peak=0;lock=threading.Lock()
 def task(i,spec):
  nonlocal active,peak
  cfg=Config();assets=ROOT/'tmp/all-index-worker-validation';cfg.config={'memgraph':{'host':'127.0.0.1','port':memgraph_port},'opensearch':{'host':'http://127.0.0.1:19204'},'clear_existing_indices':False,'allow_index_creation':True,'about_file':str(assets/'about.yaml')}
  loader=Loader(cfg)
  if query_pool:
   from process_queries import ProcessQueries
   loader.memgraph.close();loader.memgraph=ProcessQueries(query_pool)
  original=loader.opensearch;wrapper=Filtered(writer);loader.opensearch=wrapper;loader.partition_mode=True
  s=deepcopy(spec);name=s['index_name'];target=f'bench_kafka_parallel_{stamp}_{i}';s['index_name']=target;t=time.monotonic()
  with lock:active+=1;peak=max(peak,active)
  print('START',name,'active',active,flush=True)
  try:
   if s.get('type')=='model':loader.read_model([str(assets/'cds-model.yml'),str(assets/'cds-model-props.yml')])
   count=loader._process_index_serial(s)
   if write_mode=="kafka":writer.drain(target)
   actual=client.client.count(index=target)['count'];expected_count=len(wrapper.ids[target]) if target in wrapper.ids else count;assert expected_count==actual,(name,expected_count,actual)
   load_seconds=time.monotonic()-t
   hashes=sorted((hit['_id'],hashlib.sha256(json.dumps(normalized(hit['_source']),sort_keys=True).encode()).hexdigest()) for hit in scan(client.client,index=target))
   digest=hashlib.sha256(json.dumps(hashes).encode()).hexdigest()
   row={'content_digest':digest,'index':name,'seconds':load_seconds,'documents':actual,'query_seconds':sum(sum(v) for v in loader.query_timings.values()),'query_timings':dict(loader.query_timings),'operations':wrapper.ops,'error':None}
   client.client.indices.delete(index=target)
   return row
  except Exception as e:return {'index':name,'seconds':time.monotonic()-t,'error':repr(e),'test_index':target}
  finally:
   with lock:active-=1
   loader.opensearch=original;loader.close()
 try:
  with ThreadPoolExecutor(max_workers=workers) as pool:
   jobs={pool.submit(task,i,s):s['index_name'] for i,s in enumerate(specs)}
   for f in as_completed(jobs):
    try:row=f.result()
    except Exception as e:row={'index':jobs[f],'error':repr(e)}
    results.append(row);print('RESULT',json.dumps(row),flush=True)
    (out/'progress.json').write_text(json.dumps(results,indent=2))
  drain_error=None
  try:
   if write_mode=="kafka":writer.drain()
  except Exception as e:drain_error=repr(e)
  result={'query_processes':query_processes,'sample_percent':sample_percent,'max_batch_documents':MAX_DOCUMENTS,'wall_seconds':time.monotonic()-start,'index_workers':workers,'peak_active_indices':peak,'consumers':2,'write_mode':write_mode,'query_workers':query_workers or workers,'setup_seconds':setup_seconds,'consumer_busy_seconds_sum':writer.child_seconds if write_mode=='kafka' else None,'peak_pending_messages':writer.peak if write_mode=='kafka' else None,'indices':results,'drain_error':drain_error,'success':drain_error is None and all(not r['error'] for r in results)}
  (out/'results.json').write_text(json.dumps(result,indent=2));print('FINAL',json.dumps(result),flush=True)
 finally:
  writer.shutdown();client.client.close()
  if query_pool:query_pool.shutdown(wait=True,cancel_futures=True)
