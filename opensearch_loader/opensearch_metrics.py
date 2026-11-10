"""Read-only OpenSearch telemetry using the loader's existing authenticated client."""
import json
import threading
from contextlib import contextmanager


@contextmanager
def opensearch_monitor(client, logger, interval=60):
    stop = threading.Event()
    denied = set()

    def request(label, path, fields, event):
        if label in denied:
            return
        try:
            result = client.transport.perform_request(
                'GET', path, params={'filter_path': fields, 'timeout': '5s'},
                timeout=7)
            logger.info('%s %s', label, json.dumps(
                {'event': event, 'scope': 'opensearch_cluster', 'data': result}, sort_keys=True))
        except Exception as exc:
            status = getattr(exc, 'status_code', None)
            if status in (401, 403):
                denied.add(label)
            # Avoid logging response bodies, authentication headers or secrets.
            logger.warning('%s unavailable event=%s error_type=%s status=%s%s',
                           label, event, type(exc).__name__, status,
                           ' polling disabled for this endpoint' if label in denied else '')

    def sample(event):
        request('OPENSEARCH_NODE_METRICS',
                '/_nodes/stats/os,process,jvm,fs,thread_pool,indices',
                '_nodes.total,_nodes.successful,_nodes.failed,'
                'nodes.*.timestamp,nodes.*.roles,nodes.*.os.cpu,nodes.*.os.mem,'
                'nodes.*.process.cpu,nodes.*.jvm.uptime_in_millis,'
                'nodes.*.jvm.mem,nodes.*.jvm.gc.collectors,'
                'nodes.*.fs.total,nodes.*.fs.io_stats.total,'
                'nodes.*.thread_pool.write,nodes.*.thread_pool.bulk,'
                'nodes.*.indices.indexing,nodes.*.indices.merges,'
                'nodes.*.indices.refresh,nodes.*.indices.flush', event)

    request('OPENSEARCH_NODE_INFO', '/_nodes/os,jvm',
            '_nodes.total,_nodes.successful,_nodes.failed,nodes.*.version,nodes.*.roles,'
            'nodes.*.os.available_processors,nodes.*.os.allocated_processors,'
            'nodes.*.os.arch,nodes.*.jvm.mem.heap_max_in_bytes', 'start')
    sample('start')

    def poll():
        while not stop.wait(interval):
            sample('sample')

    thread = threading.Thread(target=poll, name='opensearch-metrics', daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()
        sample('end')
