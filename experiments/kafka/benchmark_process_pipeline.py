"""Exercise the production Loader.load path on the same 5% sample, no Kafka."""
import hashlib
import json
import logging
import re
import sys
import time
from pathlib import Path
from unittest.mock import patch
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from benchmark import Filtered, normalized
from opensearch_loader.config import Config
from opensearch_loader.loader import Loader
from opensearch_loader.opensearch_client import OpenSearchClient
from opensearch_loader.process_pipeline import ProcessWrites
from opensearchpy.helpers import scan

stamp = str(int(time.time()))
out = ROOT / 'tmp/parallel-research' / ('no-kafka-4x4-' + stamp)
out.mkdir()
print('ARTIFACT_DIR', out, flush=True)
logging.basicConfig(level=logging.INFO, force=True, format='%(asctime)s %(message)s',
                    handlers=[logging.FileHandler(out/'loader.log')])
logging.getLogger('opensearch').setLevel(logging.WARNING)
specs = yaml.safe_load(Path('/tmp/opensearch-indices-4.9.0-review.yaml').read_text())['indices']
samples = json.loads((ROOT/'tmp/kafka-five-percent/sampling.json').read_text())
assert len(samples['file']['ids']) == 31142
specs.sort(key=lambda s:s['index_name'] != 'dashboard_file')
names = {}
for i, spec in enumerate(specs):
    original = spec['index_name']
    target = 'bench_process_' + stamp + '_' + str(i)
    names[target] = original
    spec['index_name'] = target
    if 'initial_query' not in spec:
        continue
    for query in [spec['initial_query']] + spec.get('update_queries', []):
        alias, label = re.match(r'\s*MATCH\s*\((\w+):(\w+)\)',query['query']).groups()
        query['query'] = f'UNWIND $benchmark_nodes AS node_id MATCH ({alias}:{label}) WHERE id({alias})=node_id WITH {alias}\n' + query['query']
        query.setdefault('variables',{})['benchmark_nodes'] = samples[label]['ids']
spec_path = out/'indices.yaml'
spec_path.write_text(yaml.safe_dump({'indices':specs}))
assets=ROOT/'tmp/all-index-worker-validation'
config=Config()
config.config={'memgraph':{'host':'127.0.0.1','port':17689},'opensearch':{'host':'http://127.0.0.1:19204'},
 'index_spec_file':str(spec_path),'selected_indices':[], 'index_workers':4,'query_workers':4,'write_workers':4,'process_pipeline':True,
 'clear_existing_indices':False,'allow_index_creation':True,'about_file':str(assets/'about.yaml'),
 'model_files':[str(assets/'cds-model.yml'),str(assets/'cds-model-props.yml')]}
baseline=json.loads((ROOT/'tmp/kafka-five-percent/parallel-1795807480/results.json').read_text())
reference={r['index']:r for r in baseline['indices']}
rows=[]
import threading
result_lock=threading.Lock()
original_process=Loader._process_index

def checked_process(child,spec):
    started=time.monotonic()
    count=original_process(child,spec)
    seconds=time.monotonic()-started
    target=spec['index_name'];name=names[target]
    client=child.opensearch.client
    docs=client.count(index=target)['count']
    hashes=sorted((h['_id'],hashlib.sha256(json.dumps(normalized(h['_source']),sort_keys=True).encode()).hexdigest()) for h in scan(client,index=target))
    digest=hashlib.sha256(json.dumps(hashes).encode()).hexdigest()
    matched=docs==reference[name]['documents'] and digest==reference[name]['content_digest']
    row={'index':name,'seconds':seconds,'documents':docs,'content_matches':matched,'content_digest':digest}
    with result_lock:
        rows.append(row)
        (out/'progress.json').write_text(json.dumps(rows,indent=2))
        print('RESULT',json.dumps(row),flush=True)
    if not matched:raise AssertionError('Content mismatch: '+name)
    # Free only this verified test index before starting another test index.
    client.indices.delete(index=target)
    return count

loader=Loader(config)
started=time.monotonic()
try:
    with patch('opensearch_loader.process_pipeline.ProcessWrites',side_effect=lambda *a,**kw:Filtered(ProcessWrites(*a,**kw))), patch.object(Loader,'_process_index',checked_process):
        loader.load()
    elapsed=time.monotonic()-started
    result={'success':len(rows)==21 and all(r['content_matches'] for r in rows),'sample_percent':5,'index_workers':4,'query_workers':4,'write_workers':4,'kafka':False,'wall_seconds':elapsed,'indices':rows}
    (out/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
finally:
    loader.close()
