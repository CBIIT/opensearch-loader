"""Deterministic dashboard_file subset for short, comparable benchmark runs."""
import copy
import math
import re


def prepare_benchmark(indices, fraction, memgraph, logger):
    if not 0 < fraction <= 1:
        raise ValueError('benchmark_fraction must be greater than 0 and at most 1')
    if fraction == 1:
        return indices
    if len(indices) != 1 or indices[0]['index_name'] != 'dashboard_file':
        raise ValueError('Subset benchmarks require selected_indices=["dashboard_file"]')
    spec = copy.deepcopy(indices[0])
    # Validate every supported anchor before querying or touching OpenSearch.
    queries = [spec['initial_query']] + spec.get('update_queries', [])
    for item in queries:
        query = item['query'].replace('\\n', '\n')
        if re.match(r'^\s*MATCH \(f:file\)\s+WITH\b', query):
            query = query.replace('MATCH (f:file)',
                'MATCH (f:file) WHERE f.file_id <= $benchmark_file_id_max', 1)
        elif item.get('name') == 'Studies' and 'OPTIONAL MATCH (s)<--(f:file)\nWITH' in query:
            query = query.replace('OPTIONAL MATCH (s)<--(f:file)\nWITH',
                'OPTIONAL MATCH (s)<--(f:file)\nWHERE f.file_id <= $benchmark_file_id_max\nWITH', 1)
        else:
            raise ValueError('Unsupported benchmark query shape: ' + item.get('name', 'Initial Query'))
        item['query'] = query
    total = memgraph.execute_query(
        'MATCH (f:file) RETURN count(DISTINCT f.file_id) AS total SKIP $skip LIMIT $limit',
        {'skip': 0, 'limit': 1})[0]['total']
    if not total:
        raise ValueError('No file IDs available for benchmarking')
    target = max(1, math.ceil(total * fraction))
    cutoff = memgraph.execute_query(
        'MATCH (f:file) WHERE f.file_id IS NOT NULL '
        'WITH DISTINCT f.file_id AS file_id ORDER BY file_id SKIP $skip LIMIT $limit '
        'RETURN max(file_id) AS cutoff', {'skip': 0, 'limit': target})[0]['cutoff']
    if cutoff is None:
        raise ValueError('Could not select benchmark file ID boundary')
    for item in queries:
        item['variables'] = dict(item.get('variables') or {}, benchmark_file_id_max=cutoff)
    spec['index_name'] = 'dashboard_file_benchmark'
    logger.info('BENCHMARK fraction=%s source_documents=%s target_documents=%s '
                'index=%s selection=file_id_order; timings are subset-only, not a full-run estimate',
                fraction, total, target, spec['index_name'])
    return [spec]
