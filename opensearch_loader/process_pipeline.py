"""Shared query/write subprocesses; no Kafka and no Prefect child flow import."""
import logging
import os
import pickle
import queue
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import ExitStack
from pathlib import Path

from .memgraph_client import MemgraphClient
from .partitioned import partition_for_id

logger = logging.getLogger('OpenSearchLoader')


def positive_workers(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError('Process worker counts must be positive integers')
    return value


class Worker:
    def __init__(self, kind, config, timeout=3600):
        self.lock = threading.Lock()
        self.closed = False
        parent, child = socket.socketpair()
        parent.settimeout(60)
        self.socket = parent
        self.stream = parent.makefile('rwb')
        env = os.environ.copy()
        root = str(Path(__file__).resolve().parents[1])
        env['PYTHONPATH'] = root + os.pathsep + env.get('PYTHONPATH', '')
        try:
            self.process = subprocess.Popen(
                [sys.executable, '-m', 'opensearch_loader.process_worker', str(child.fileno())],
                pass_fds=(child.fileno(),), env=env, stdin=subprocess.DEVNULL)
        except BaseException:
            self.stream.close(); parent.close()
            raise
        finally:
            child.close()
        try:
            pickle.dump((kind, config), self.stream); self.stream.flush()
            ok, result = pickle.load(self.stream)
            if not ok:
                raise RuntimeError(result)
            parent.settimeout(timeout)
            logger.info('Process worker ready: kind=%s pid=%s', kind, self.process.pid)
        except BaseException:
            self.close()
            raise

    def call(self, method, *args, **kwargs):
        with self.lock:
            if self.closed:
                raise RuntimeError('Process worker is closed')
            try:
                pickle.dump((method, args, kwargs), self.stream); self.stream.flush()
                ok, result = pickle.load(self.stream)
            except (OSError, EOFError, ValueError, pickle.PickleError) as exc:
                self.close()
                raise RuntimeError('Process worker IPC failed; worker terminated before retry') from exc
            if not ok:
                raise RuntimeError(result)
            return result

    def close(self):
        if self.closed:
            return
        self.closed = True
        # Kill before allowing a failed request to be retried, preventing late writes.
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill(); self.process.wait()
        try:
            self.stream.close()
        except OSError:
            pass  # A dead peer can fail the buffered stream close too.
        finally:
            self.socket.close()


class QueryPool:
    def __init__(self, config, workers=4, timeout=3600):
        self.workers = []
        self.available = queue.Queue()
        try:
            for _ in range(positive_workers(workers)):
                worker = Worker('query', config, timeout)
                self.workers.append(worker); self.available.put(worker)
        except BaseException:
            self.close()
            raise

    def execute(self, query, parameters):
        worker = self.available.get()
        started = time.monotonic()
        try:
            return worker.call('execute_query', query, parameters)
        finally:
            logger.info('Query process finished: pid=%s wall=%.3fs', worker.process.pid, time.monotonic()-started)
            self.available.put(worker)

    def close(self):
        for worker in self.workers:
            worker.close()


class ProcessQueries(MemgraphClient):
    def __init__(self, pool):
        self.pool = pool
        self.connection = None
        self.last_query_time = 0.0

    def execute_query(self, query, parameters=None):
        self.validate_read_only(query)
        self.validate_pagination_params(query)
        return self.pool.execute(query, parameters or {})

    def close(self):
        pass  # Pool lifetime belongs to the top-level loader.


class ProcessWrites:
    """Four shared serial lanes routed by ID; each call completes before returning."""
    def __init__(self, delegate, config, workers=4, timeout=3600):
        self.delegate = delegate
        self.workers = positive_workers(workers)
        self.children = []
        self.pools = []
        try:
            for _ in range(self.workers):
                self.children.append(Worker('write', config, timeout))
                self.pools.append(ThreadPoolExecutor(max_workers=1))
        except BaseException:
            self.close()
            raise

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def _write(self, method, index, documents, id_field, **kwargs):
        partitions = [[] for _ in range(self.workers)]
        for doc in documents:
            if doc.get(id_field):
                partitions[partition_for_id(doc[id_field], self.workers)].append(doc)
        skipped = len(documents) - sum(map(len, partitions))
        if skipped:
            logger.warning('%s: Skipping %s rows without %s', index, skipped, id_field)
        started = time.monotonic()
        futures = []
        for lane, rows in enumerate(partitions):
            if rows:
                futures.append(self.pools[lane].submit(
                    self.children[lane].call, method, index, rows, id_field, **kwargs))
        # Drain ALL lanes before raising; retries cannot overlap previous writes.
        wait(futures)
        for future in futures:
            future.result()
        logger.info('%s:%s: Process writes completed: workers=%s documents=%s wall=%.3fs',
                    index, kwargs.get('query_name'), self.workers, sum(map(len, partitions)), time.monotonic()-started)

    def bulk_upsert(self, index, documents, id_field, **kwargs):
        self._write('bulk_upsert', index, documents, id_field, **kwargs)

    def bulk_update(self, index, documents, id_field, **kwargs):
        self._write('bulk_update', index, documents, id_field, **kwargs)

    def upsert_pages(self, index, pages):
        futures = [self.pools[partition_for_id(doc_id, self.workers)].submit(
            self.children[partition_for_id(doc_id, self.workers)].call,
            'upsert_document', index, doc_id, page) for doc_id, page in pages]
        wait(futures)
        for future in futures:
            future.result()

    def upsert_document(self, index, doc_id, document):
        self.upsert_pages(index, [(doc_id, document)])

    def close(self):
        for pool in self.pools:
            pool.shutdown(wait=True)
        for child in self.children:
            child.close()

    shutdown = close



class QueuedProcessWrites(ProcessWrites):
    """Bounded outstanding batches, delivered to ordered subprocess lanes over IPC.

    The semaphore bounds queued AND executing batches across all indexes. Threads
    only drive socket RPC; OpenSearch calls run in the existing child processes.
    Failures stay attached to their index so a loader retry cannot hide a failure.
    """
    def __init__(self, delegate, config, workers=4, timeout=3600, queue_batches=10):
        self.capacity = positive_workers(queue_batches)
        self.slots = threading.BoundedSemaphore(self.capacity)
        self.condition = threading.Condition()
        self.pending = {}
        self.errors = {}
        super().__init__(delegate, config, workers, timeout)

    def _check(self, index):
        if index in self.errors:
            raise RuntimeError('Queued process write failed for ' + index) from self.errors[index]

    def _execute(self, lane, method, index, args, kwargs):
        with self.condition:
            self._check(index)
        started = time.monotonic()
        result = self.children[lane].call(method, index, *args, **kwargs)
        logger.info('%s: Queue batch ACK: lane=%s method=%s wall=%.3fs',
                    index, lane, method, time.monotonic() - started)
        return result

    def _submit(self, lane, method, index, args, kwargs):
        self.slots.acquire()  # Backpressure: querying pauses only when queue is full.
        try:
            with self.condition:
                self._check(index)
                future = self.pools[lane].submit(self._execute, lane, method, index, args, kwargs)
                self.pending[future] = index
        except BaseException:
            self.slots.release()
            raise
        def completed(done):
            with self.condition:
                error = done.exception()
                if error is not None:
                    self.errors.setdefault(index, error)
                self.pending.pop(done, None)
                self.slots.release()
                self.condition.notify_all()
        future.add_done_callback(completed)

    def _write(self, method, index, documents, id_field, **kwargs):
        # Bound each queued message by both count and serialized size. Each ID
        # always takes the same serial lane, including across query boundaries.
        partitions = [[] for _ in range(self.workers)]
        sizes = [0] * self.workers
        for doc in documents:
            if not doc.get(id_field):
                continue  # Same missing-ID behavior as synchronous ProcessWrites.
            lane = partition_for_id(doc[id_field], self.workers)
            encoded = pickle.dumps(doc)
            if len(encoded) > 5 * 1024 * 1024:
                raise ValueError('Document exceeds 5 MiB process queue limit')
            if partitions[lane] and (len(partitions[lane]) >= 5000 or sizes[lane] + len(encoded) > 5 * 1024 * 1024):
                self._submit(lane, method, index, (partitions[lane], id_field), kwargs)
                partitions[lane] = []; sizes[lane] = 0
            partitions[lane].append(pickle.loads(encoded))
            sizes[lane] += len(encoded)
        for lane, rows in enumerate(partitions):
            if rows:
                self._submit(lane, method, index, (rows, id_field), kwargs)
        logger.info('%s:%s: Batches queued (not yet acknowledged): documents=%s capacity=%s',
                    index, kwargs.get('query_name'), len(documents), self.capacity)

    def upsert_pages(self, index, pages):
        for doc_id, page in pages:
            payload = pickle.dumps(page)
            if len(payload) > 5 * 1024 * 1024:
                raise ValueError('Document exceeds 5 MiB process queue limit')
            self._submit(partition_for_id(doc_id, self.workers), 'upsert_document',
                         index, (doc_id, pickle.loads(payload)), {})

    def drain(self, index=None):
        with self.condition:
            while any(index is None or name == index for name in self.pending.values()):
                self.condition.wait()
            if index is not None:
                self._check(index)
            elif self.errors:
                self._check(next(iter(self.errors)))

    def refresh_index(self, index):
        self.drain(index)
        self.delegate.refresh_index(index)
        logger.info('%s: Queue drained and index refreshed', index)

    def close(self):
        try:
            self.drain()
        finally:
            super().close()

    shutdown = close


def load_with_processes(loader):
    """Own exactly one shared set of workers for the complete flow run."""
    old_memgraph = loader.memgraph
    with ExitStack() as stack:
        if loader.config.get('task_kafka', False):
            from .task_kafka import TaskKafka
            stack.enter_context(TaskKafka())
        query_pool = QueryPool(loader.config.get_memgraph_config(), loader.config.get('query_workers', 4))
        stack.callback(query_pool.close)
        writer_class = QueuedProcessWrites if loader.config.get('process_write_queue', False) else ProcessWrites
        options = {'queue_batches': loader.config.get('write_queue_batches', 10)} if writer_class is QueuedProcessWrites else {}
        if loader.config.get('task_kafka', False):
            from .kafka_transport import KafkaWrites
            writer_class = KafkaWrites
            options = {'queue_batches': loader.config.get('write_queue_batches', 10)}
        writer = writer_class(loader.opensearch, loader.config.get_opensearch_config(), loader.config.get('write_workers', 4), **options)
        stack.callback(writer.close)
        loader._query_pool = query_pool
        loader._shared_writer = writer
        loader.memgraph = ProcessQueries(query_pool)
        logger.info('Process pipeline started: query_workers=%s write_workers=%s index_workers=%s kafka=%s',
                    len(query_pool.workers), writer.workers, loader.config.get('index_workers', 1), loader.config.get('task_kafka', False))
        try:
            return loader._load()
        finally:
            loader.memgraph = old_memgraph
            loader._query_pool = None
            loader._shared_writer = None
