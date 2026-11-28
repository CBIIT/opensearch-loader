"""Local-only 21-index sampled A/B. Does not modify production loader or deployment."""
import sys,json,time,re,logging,hashlib,math,subprocess,argparse
from pathlib import Path
from copy import deepcopy
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
import yaml,mgclient
from opensearch_loader.config import Config
from opensearch_loader.loader import Loader
from opensearch_loader.partitioned import ParallelWrites
from opensearchpy.helpers import scan
from opensearchpy.exceptions import TransportError
from transport import KafkaWrites
OUT=ROOT/'tmp/kafka-five-percent';OUT.mkdir(exist_ok=True)
logging.basicConfig(filename=OUT/'loader.log',level=logging.INFO,format='%(asctime)s %(message)s')
logging.getLogger('opensearch').setLevel(logging.WARNING)
class Filtered:
 def __init__(self,w):self.w=w;self.ids={};self.ops=0
 def __getattr__(self,k):return getattr(self.w,k)
 def create_index(self,*args,**kwargs):
  self.w.create_index(*args,**kwargs)
  target=args[0]
  for attempt in range(15):
   self.w.client.cluster.reroute()
   try:h=self.w.client.cluster.health(index=target,wait_for_status='green',timeout='5s',request_timeout=10)
   except TransportError as exc:
    if exc.status_code!=408:raise
    continue
   if not h.get('timed_out'):return
  raise RuntimeError('Test index shard allocation failed')
 def bulk_upsert(self,index,docs,key=None,**kw):
  key=key or kw.pop('id_field',None)
  if not key:raise ValueError('id_field is required')
  self.ids.setdefault(index,set()).update(str(d[key]) for d in docs if d.get(key));self.ops+=sum(bool(d.get(key)) for d in docs);self.w.bulk_upsert(index,docs,key,**kw)
 def bulk_update(self,index,docs,key=None,**kw):
  key=key or kw.pop('id_field',None)
  if not key:raise ValueError('id_field is required')
  docs=[d for d in docs if str(d.get(key)) in self.ids.get(index,set())];self.ops+=len(docs)
  if docs:self.w.bulk_update(index,docs,key,**kw)

def normalized(v):
 if isinstance(v,dict):return {k:normalized(x) for k,x in v.items()}
 if isinstance(v,list):return sorted([normalized(x) for x in v],key=lambda x:json.dumps(x,sort_keys=True))
 return v

def restart_memgraph():
 docker=['docker','--context','colima'];container='os-partition-benchmark-memgraph'
 def snapshots():
  return set(subprocess.check_output(docker+['exec',container,'ls','-1','/var/lib/memgraph/snapshots'],text=True).splitlines())
 before=snapshots()
 subprocess.run(['docker','--context','colima','restart','os-partition-benchmark-memgraph'],check=True,stdout=subprocess.DEVNULL)
 for attempt in range(60):
  try:
   c=mgclient.connect(host='127.0.0.1',port=17688);c.autocommit=True;q=c.cursor();q.execute('MATCH (f:file) RETURN count(f)');assert q.fetchone()[0]==622840;c.close()
   # Exact new file generated during this restart; never delete a pre-existing snapshot.
   new=snapshots()-before
   if len(new)==1:
    name=next(iter(new))
    if re.fullmatch(r'\d{20}_timestamp_5006',name):
     print('REMOVE_RESTART_SNAPSHOT',name,flush=True)
     subprocess.run(docker+['exec',container,'rm','--','/var/lib/memgraph/snapshots/'+name],check=True)
   return
  except Exception:time.sleep(1)
 raise RuntimeError('Memgraph did not recover')

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--mode',choices=['both','direct','kafka'],default='both');parser.add_argument('--index-workers',type=int,default=1);parser.add_argument('--memgraph-port',type=int,default=17688);parser.add_argument('--skip-memgraph-restart',action='store_true');parser.add_argument('--sample-percent',type=float,default=5);parser.add_argument('--query-processes',action='store_true');parser.add_argument('--query-workers',type=int);parser.add_argument('--dashboard-first',action='store_true');parser.add_argument('--write-mode',choices=['kafka','direct'],default='kafka');args=parser.parse_args()
 if not 0<args.sample_percent<=100:parser.error('sample-percent must be between 0 and 100')
 if not args.skip_memgraph_restart:restart_memgraph()
 source=Path('/tmp/opensearch-indices-4.9.0-review.yaml');specs=yaml.safe_load(source.read_text())['indices'];samples={}
 c=mgclient.connect(host='127.0.0.1',port=args.memgraph_port);c.autocommit=True;q=c.cursor()
 for s in specs:
  if 'initial_query' not in s:continue
  for query in [s['initial_query']]+s.get('update_queries',[]):
   m=re.match(r'\s*MATCH\s*\((\w+):(\w+)\)',query['query']);assert m,query
   alias,label=m.groups()
   if label not in samples:
    q.execute(f'MATCH (n:{label}) RETURN count(n)');total=q.fetchone()[0]
    q.execute(f'MATCH (n:{label}) RETURN id(n) ORDER BY id(n) LIMIT $n',{'n':max(1,math.ceil(total*args.sample_percent/100))})
    samples[label]={'total':total,'ids':[r[0] for r in q.fetchall()]}
   query['query']=f'UNWIND $benchmark_nodes AS node_id MATCH ({alias}:{label}) WHERE id({alias})=node_id WITH {alias}\n'+query['query']
   query.setdefault('variables',{})['benchmark_nodes']=samples[label]['ids']
 c.close();(OUT/'sampling.json').write_text(json.dumps(samples));print('SAMPLES', {k:(v['total'],len(v['ids'])) for k,v in samples.items()},flush=True)
 if args.dashboard_first:specs.sort(key=lambda s:s['index_name']!='dashboard_file')
 if args.index_workers>1:
  from parallel_run import run
  return run(specs,args.index_workers,args.memgraph_port,args.sample_percent,args.query_processes,args.query_workers,args.write_mode)
 results=[];expected={};stamp=str(int(time.time()))
 for mode in (['direct','kafka'] if args.mode=='both' else [args.mode]):
  cfg=Config();assets=ROOT/'tmp/all-index-worker-validation';cfg.config={'memgraph':{'host':'127.0.0.1','port':17688},'opensearch':{'host':'http://127.0.0.1:19204'},'clear_existing_indices':False,'allow_index_creation':True,'about_file':str(assets/'about.yaml')}
  loader=Loader(cfg);original=loader.opensearch
  health=original.client.cluster.health();assert health['number_of_data_nodes']==2,health
  loader.read_model([str(assets/'cds-model.yml'),str(assets/'cds-model-props.yml')])
  writer=ParallelWrites(original,2) if mode=='direct' else KafkaWrites(original,'os-loader-bench-'+stamp,OUT)
  wrapper=Filtered(writer);loader.opensearch=wrapper;loader.partition_mode=True
  round_start=time.monotonic();rows=[]
  try:
   for i,base in enumerate(specs):
    s=deepcopy(base);name=s['index_name'];target=f'bench_kafka_{stamp}_{mode}_{i}';s['index_name']=target
    loader.memgraph.close();restart_memgraph()
    from opensearch_loader.memgraph_client import MemgraphClient
    loader.memgraph=MemgraphClient(**cfg.config['memgraph'])
    print('START',mode,name,flush=True);start=time.monotonic();before=sum(sum(v) for v in loader.query_timings.values());ops=wrapper.ops
    count=loader._process_index_serial(s)
    if mode=='kafka':writer.drain()
    elapsed=time.monotonic()-start
    actual=original.client.count(index=target)['count'];assert actual==count,(name,count,actual)
    hashes={h['_id']:hashlib.sha256(json.dumps(normalized(h['_source']),sort_keys=True).encode()).hexdigest() for h in scan(original.client,index=target)}
    if mode=='direct':expected[name]=hashes
    elif name in expected:assert hashes==expected[name],f'Content mismatch: {name}'
    row={'index':name,'documents':actual,'seconds':elapsed,'query_seconds':sum(sum(v) for v in loader.query_timings.values())-before,'write_operations':wrapper.ops-ops,'count_validated':True,'contents_compared':mode=='kafka' and name in expected}
    rows.append(row);(OUT/f'{mode}.json').write_text(json.dumps(rows,indent=2));print('RESULT',mode,json.dumps(row),flush=True)
    # Only this experiment's disposable index; keep disk usage bounded.
    assert target.startswith('bench_kafka_'+stamp);original.client.indices.delete(index=target)
    wrapper.ids.pop(target,None)
   result={'mode':mode,'wall_seconds_including_validation':time.monotonic()-round_start,'index_load_seconds_sum':sum(r['seconds'] for r in rows),'indices':rows}
   if mode=='kafka':result.update(consumer_busy_seconds_sum=writer.child_seconds,ack_wait_seconds=writer.wait_seconds,peak_pending_messages=writer.peak)
   results.append(result);(OUT/'results.json').write_text(json.dumps(results,indent=2));print('ROUND',json.dumps(result),flush=True)
  finally:writer.shutdown();loader.opensearch=original;loader.close()
 print('ALL 21 INDEXES COMPLETED; comparison='+str(args.mode=='both'),flush=True)
if __name__=='__main__':main()
