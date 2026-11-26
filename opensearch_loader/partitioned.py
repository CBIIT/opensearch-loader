"""Read each query once and split OpenSearch writes between ID-routed workers."""

from concurrent.futures import ThreadPoolExecutor, wait
from contextvars import copy_context
import logging
from time import monotonic
from zlib import crc32

logger = logging.getLogger("OpenSearchLoader")
WRITE_WORKERS = 4


def partition_for_id(value, workers=None):
    """Stable assignment for numeric IDs, UUIDs, and other nonempty IDs."""
    if value is None or not str(value):
        raise ValueError("Parallel writes require a nonempty document ID")
    value = str(value).lower()
    workers = WRITE_WORKERS if workers is None else workers
    if value.isascii() and value.isdecimal():
        return int(value) % workers
    if value[-1] in "0123456789abcdef":
        return int(value[-1], 16) % workers
    return crc32(value.encode('utf-8')) % workers


class ParallelWrites:
    """Synchronous batch boundary, concurrent disjoint-ID writes within a batch.

    No database queries run on these workers. All futures are drained even on
    failure, so loader retries cannot race with writes from the previous attempt.
    The same ID always goes to the same single-thread executor, in input order.
    """

    def __init__(self, client, workers=None):
        self.delegate = client
        self.workers = WRITE_WORKERS if workers is None else workers
        if isinstance(self.workers, bool) or not isinstance(self.workers, int) or self.workers < 1:
            raise ValueError('write_workers must be a positive integer')
        self.pools = [ThreadPoolExecutor(max_workers=1,
                      thread_name_prefix=f"opensearch-write-{p}") for p in range(self.workers)]

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def _write(self, method, index_name, documents, id_field, **kwargs):
        partitions = [[] for _ in range(self.workers)]
        skipped = 0
        for document in documents:
            doc_id = document.get(id_field)
            # Match bulk_upsert/_process_update_batch: optional graph matches
            # can yield rows with no target document. Skip these before routing.
            if not doc_id:
                skipped += 1
                continue
            partitions[partition_for_id(doc_id, self.workers)].append(document)
        if skipped:
            logger.warning("%s:%s: Skipping %s/%s rows with missing or empty %s before parallel %s",
                           index_name, kwargs.get('query_name'), skipped,
                           len(documents), id_field, method)
        started = monotonic()
        futures = [pool.submit(copy_context().run, getattr(self.delegate, method), index_name, rows,
                               id_field, **kwargs)
                   for pool, rows in zip(self.pools, partitions) if rows]
        if futures:
            wait(futures)
            for future in futures:
                future.result()
        logger.info("%s:%s: Parallel %s completed: documents=%s skipped=%s workers=%s split=%s wall=%.3fs",
                    index_name, kwargs.get('query_name'), method, len(documents) - skipped, skipped,
                    self.workers, "/".join(str(len(rows)) for rows in partitions), monotonic() - started)

    def bulk_upsert(self, index_name, documents, id_field, **kwargs):
        self._write('bulk_upsert', index_name, documents, id_field, **kwargs)

    def bulk_update(self, index_name, documents, id_field, **kwargs):
        self._write('bulk_update', index_name, documents, id_field, **kwargs)

    def upsert_pages(self, index_name, pages):
        """Keep about-page IDs and sources unchanged while writing concurrently."""
        partitions = [[] for _ in range(self.workers)]
        for doc_id, page in pages:
            partitions[partition_for_id(doc_id, self.workers)].append((doc_id, page))

        def write(rows):
            for doc_id, page in rows:
                self.delegate.upsert_document(index_name, doc_id, page)

        futures = [pool.submit(copy_context().run, write, rows)
                   for pool, rows in zip(self.pools, partitions) if rows]
        if futures:
            wait(futures)
            for future in futures:
                future.result()

    def shutdown(self):
        for pool in self.pools:
            pool.shutdown(wait=True)


def load_partitioned(loader, spec):
    """Run any index handler with configurable ID-partitioned write workers."""
    original = loader.opensearch
    writes = ParallelWrites(original, loader.config.get('write_workers'))
    previous_mode = getattr(loader, 'partition_mode', False)
    started = monotonic()
    try:
        loader.opensearch = writes
        loader.partition_mode = True  # Propagate validation failures.
        logger.info("Parallel load started: index=%s workers=%s reader_pipelines=1",
                    spec['index_name'], writes.workers)
        # The normal index-creation configuration applies. All workers write to
        # the same index, and each batch completes before the next one starts.
        count = loader._process_index_serial(spec)
    finally:
        writes.shutdown()
        loader.opensearch = original
        loader.partition_mode = previous_mode
    logger.info("Parallel load completed: index=%s workers=%s documents=%s wall=%.1fs",
                spec['index_name'], writes.workers, count, monotonic() - started)
    return count
