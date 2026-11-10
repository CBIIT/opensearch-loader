"""Read-only OpenSearch telemetry using the loader's existing authenticated client."""
import json
import threading
from contextlib import contextmanager
from urllib.parse import quote


def log_index_settings(client, logger, index_name):
    """Log only performance-related settings, including effective defaults."""
    keys = (
        'index.refresh_interval', 'index.number_of_shards',
        'index.number_of_replicas', 'index.auto_expand_replicas',
        'index.translog.durability', 'index.translog.sync_interval',
        'index.translog.flush_threshold_size',
        'index.merge.scheduler.max_thread_count',
        'index.codec', 'index.routing.allocation.total_shards_per_node',
    )
    try:
        response = client.transport.perform_request(
            'GET', '/' + quote(index_name, safe='') + '/_settings',
            params={'include_defaults': 'true', 'flat_settings': 'true'}, timeout=7)
        for name, values in response.items():
            explicit = values.get('settings', {})
            defaults = values.get('defaults', {})
            selected = {}
            for key in keys:
                if key in explicit:
                    selected[key] = {'value': explicit[key], 'source': 'settings'}
                elif key in defaults:
                    selected[key] = {'value': defaults[key], 'source': 'defaults'}
                else:
                    selected[key] = {'value': None, 'source': 'unavailable'}
            logger.info('OPENSEARCH_INDEX_SETTINGS %s', json.dumps(
                {'index': name, 'settings': selected}, sort_keys=True))
    except Exception as exc:
        # Diagnostic failures must not interrupt loading or expose response bodies.
        logger.warning('OPENSEARCH_INDEX_SETTINGS unavailable index=%s error_type=%s status=%s',
                       index_name, type(exc).__name__, getattr(exc, 'status_code', None))


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
