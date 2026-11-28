"""Local full-data benchmark: four producer processes, four Kafka consumers."""
import argparse, json, logging, os, time, sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import get_context
import yaml
from benchmark import ROOT, Config, Loader
from transport import KafkaWrites


def run_index(spec, out_path, stamp, position):
    out=Path(out_path); name=spec['index_name']; target=f'bench_full_kafka_{stamp}_{position}'
    logging.basicConfig(filename=out/f'{name}.log',level=logging.INFO,force=True,format='%(asctime)s %(message)s')
    logging.getLogger('opensearch').setLevel(logging.WARNING)
    assets=ROOT/'tmp/all-index-worker-validation'
    cfg=Config();cfg.config={'memgraph':{'host':'127.0.0.1','port':17689},'opensearch':{'host':'http://127.0.0.1:19204'},'clear_existing_indices':False,'allow_index_creation':True,'about_file':str(assets/'about.yaml')}
    loader=Loader(cfg); original=loader.opensearch
    channel=out/name;channel.mkdir()
    writer=KafkaWrites(original,f'os-full-{stamp}-{position}',channel,consumers=1,max_payload_bytes=5*1024*1024)
    loader.opensearch=writer;loader.partition_mode=True
    started=time.monotonic()
    print('START',name,'producer_pid',os.getpid(),'consumer_pid',writer.children[0].pid,flush=True)
    try:
        if spec.get('type')=='model':loader.read_model([str(assets/'cds-model.yml'),str(assets/'cds-model-props.yml')])
        spec=dict(spec,index_name=target)
        count=loader._process_index_serial(spec)
        writer.drain(target);original.refresh_index(target)
        actual=original.client.count(index=target)['count']
        if actual!=count:raise AssertionError((name,count,actual))
        row={'index':name,'seconds':time.monotonic()-started,'documents':actual,'operations':writer.ops,'query_timings':dict(loader.query_timings),'consumer_busy_seconds':writer.child_seconds,'ack_wait_seconds':writer.wait_seconds,'peak_pending':writer.peak,'error':None}
        # Only disposable benchmark output; avoid exhausting local Docker disk.
        original.client.indices.delete(index=target)
        return row
    except Exception as exc:
        return {'index':name,'seconds':time.monotonic()-started,'error':repr(exc),'test_index':target}
    finally:
        writer.shutdown();loader.opensearch=original;loader.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--selected-indices',nargs='+');args=parser.parse_args()
    stamp=str(int(time.time()));out=ROOT/'tmp/parallel-research'/f'full-kafka-4x4-{stamp}';out.mkdir()
    specs=yaml.safe_load(Path('/tmp/opensearch-indices-4.9.0-review.yaml').read_text())['indices']
    if args.selected_indices:
        specs=[s for s in specs if s['index_name'] in args.selected_indices]
        if set(args.selected_indices)!={s['index_name'] for s in specs}:raise ValueError('Unknown index')
    manifest={'sample_percent':100,'index_workers':4,'producer_processes':4,'consumer_processes':4,'batch_documents':5000,'max_payload_bytes':5*1024*1024,'indices':len(specs),'started_epoch':time.time()}
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2));print('ARTIFACT_DIR',out,flush=True)
    started=time.monotonic();rows=[]
    with ProcessPoolExecutor(max_workers=4,mp_context=get_context('spawn')) as pool:
        jobs={pool.submit(run_index,s,str(out),stamp,i):s['index_name'] for i,s in enumerate(specs)}
        for future in as_completed(jobs):
            try:row=future.result()
            except Exception as exc:row={'index':jobs[future],'error':repr(exc)}
            rows.append(row);print('RESULT',json.dumps(row),flush=True)
            (out/'progress.json').write_text(json.dumps(rows,indent=2))
    result={**manifest,'wall_seconds':time.monotonic()-started,'indices':rows,'success':len(rows)==len(specs) and all(not r['error'] for r in rows)}
    (out/'results.json').write_text(json.dumps(result,indent=2));print('FINAL',json.dumps(result),flush=True)

if __name__=='__main__':main()
