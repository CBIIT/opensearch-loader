"""Read each query once and split only OpenSearch writes between two workers."""

from concurrent.futures import ThreadPoolExecutor, wait
from copy import deepcopy
import logging
import re
from time import monotonic
from uuid import uuid4

logger = logging.getLogger("OpenSearchLoader")
EVEN_DIGITS = "02468ace"


def partition_for_id(value):
    """Numeric IDs and hexadecimal UUIDs use their final digit's parity."""
    if value is None or not str(value):
        raise ValueError("Parallel writes require a nonempty file_id")
    return 0 if str(value).lower()[-1] in EVEN_DIGITS else 1


class ParallelWrites:
    """Synchronous batch boundary, concurrent disjoint-ID writes within a batch.

    No database queries run on these workers. Both futures are drained even on
    failure, so loader retries cannot race with writes from the previous attempt.
    The same ID always goes to the same single-thread executor, in input order.
    """

    def __init__(self, client):
        self.delegate = client
        self.pools = [ThreadPoolExecutor(max_workers=1,
                      thread_name_prefix=f"dashboard-write-{p}") for p in (0, 1)]

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def _write(self, method, index_name, documents, id_field, **kwargs):
        partitions = [[], []]
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
        logger.info("%s:%s: Parallel %s completed: documents=%s split=%s/%s wall=%.3fs",
                    index_name, kwargs.get('query_name'), method, len(documents),
                    len(partitions[0]), len(partitions[1]), monotonic() - started)

    def bulk_upsert(self, index_name, documents, id_field, **kwargs):
        self._write('bulk_upsert', index_name, documents, id_field, **kwargs)

    def bulk_update(self, index_name, documents, id_field, **kwargs):
        self._write('bulk_update', index_name, documents, id_field, **kwargs)

    def shutdown(self):
        for pool in self.pools:
            pool.shutdown(wait=True)


def load_partitioned(loader, spec):
    """Keep the existing opt-in/alias parameters, using one physical index."""
    if spec.get("index_name") != "dashboard_file" or spec.get("id_field") != "file_id":
        raise ValueError("Parallel loading supports dashboard_file/file_id only")
    if loader.config.get_test_mode():
        raise ValueError("Alias publication requires a full run, not test_mode")
    if not loader.config.get_allow_index_creation():
        raise ValueError("Parallel loading requires allow_index_creation=true")
    alias = loader.config.get("partition_alias", "dashboard_file_parallel")
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,150}", alias) or alias == "dashboard_file":
        raise ValueError("Use a separate lowercase partition_alias, e.g. dashboard_file_parallel")
    original = loader.opensearch
    api = original.client.indices
    if api.exists(index=alias) and not api.exists_alias(name=alias):
        raise ValueError(f"Alias name {alias} is already a physical index")
    target = f"{alias}-{uuid4().hex}"
    current = deepcopy(spec)
    current['index_name'] = target
    current['number_of_shards'] = 2
    writes = ParallelWrites(original)
    previous_mode = getattr(loader, 'partition_mode', False)
    started = monotonic()
    try:
        loader.opensearch = writes
        loader.partition_mode = True  # Validation failures must prevent publication.
        # One loader, one Memgraph connection, unchanged queries and pagination.
        # Each bulk call waits for both writers: initial creation and subsequent
        # query order are therefore preserved by the existing loader.
        count = loader._process_query_index(current)
    finally:
        writes.shutdown()
        loader.opensearch = original
        loader.partition_mode = previous_mode
    actions = []
    if api.exists_alias(name=alias):
        for old_index in api.get_alias(name=alias):
            actions.append({"remove": {"index": old_index, "alias": alias}})
    actions.append({"add": {"index": target, "alias": alias}})
    api.update_aliases(body={"actions": actions})
    logger.info("Published search alias %s -> %s; documents=%s wall time %.1fs",
                alias, target, count, monotonic() - started)
    return count
