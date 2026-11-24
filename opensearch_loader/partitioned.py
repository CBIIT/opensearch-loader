"""Read each query once and split only OpenSearch writes between four workers."""

from concurrent.futures import ThreadPoolExecutor, wait
import logging
from time import monotonic
from zlib import crc32

logger = logging.getLogger("OpenSearchLoader")
WRITE_WORKERS = 4


def partition_for_id(value):
    """Stable assignment for numeric IDs, UUIDs, and other nonempty IDs."""
    if value is None or not str(value):
        raise ValueError("Parallel writes require a nonempty file_id")
    value = str(value).lower()
    if value.isascii() and value.isdecimal():
        return int(value) % WRITE_WORKERS
    if value[-1] in "0123456789abcdef":
        return int(value[-1], 16) % WRITE_WORKERS
    return crc32(value.encode('utf-8')) % WRITE_WORKERS


class ParallelWrites:
    """Synchronous batch boundary, concurrent disjoint-ID writes within a batch.

    No database queries run on these workers. All futures are drained even on
    failure, so loader retries cannot race with writes from the previous attempt.
    The same ID always goes to the same single-thread executor, in input order.
    """

    def __init__(self, client):
        self.delegate = client
        self.pools = [ThreadPoolExecutor(max_workers=1,
                      thread_name_prefix=f"dashboard-write-{p}") for p in range(WRITE_WORKERS)]

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def _write(self, method, index_name, documents, id_field, **kwargs):
        partitions = [[] for _ in range(WRITE_WORKERS)]
        for document in documents:
            partitions[partition_for_id(document.get(id_field))].append(document)
        started = monotonic()
        futures = [pool.submit(getattr(self.delegate, method), index_name, rows,
                               id_field, **kwargs)
                   for pool, rows in zip(self.pools, partitions) if rows]
        if futures:
            wait(futures)
            for future in futures:
                future.result()
        logger.info("%s:%s: Parallel %s completed: documents=%s workers=%s split=%s wall=%.3fs",
                    index_name, kwargs.get('query_name'), method, len(documents),
                    WRITE_WORKERS, "/".join(str(len(rows)) for rows in partitions), monotonic() - started)

    def bulk_upsert(self, index_name, documents, id_field, **kwargs):
        self._write('bulk_upsert', index_name, documents, id_field, **kwargs)

    def bulk_update(self, index_name, documents, id_field, **kwargs):
        self._write('bulk_update', index_name, documents, id_field, **kwargs)

    def shutdown(self):
        for pool in self.pools:
            pool.shutdown(wait=True)


def load_partitioned(loader, spec):
    """Read once and write directly to dashboard_file with four workers."""
    if spec.get("index_name") != "dashboard_file" or spec.get("id_field") != "file_id":
        raise ValueError("Parallel loading supports dashboard_file/file_id only")
    original = loader.opensearch
    writes = ParallelWrites(original)
    previous_mode = getattr(loader, 'partition_mode', False)
    started = monotonic()
    try:
        loader.opensearch = writes
        loader.partition_mode = True  # Propagate validation failures.
        logger.info("Parallel load started: index=%s workers=%s reader_pipelines=1",
                    spec['index_name'], WRITE_WORKERS)
        # The normal index-creation configuration applies. All workers write to
        # the same index, and each batch completes before the next one starts.
        count = loader._process_query_index(spec)
    finally:
        writes.shutdown()
        loader.opensearch = original
        loader.partition_mode = previous_mode
    logger.info("Parallel load completed: index=%s workers=%s documents=%s wall=%.1fs",
                spec['index_name'], WRITE_WORKERS, count, monotonic() - started)
    return count
