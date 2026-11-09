"""Bounded producer/consumer streaming and conservative pagination removal."""
import re
import time
from queue import Queue, Full
from threading import Event, Thread


def without_pagination(query):
    # Mask literals, identifiers and comments so their contents cannot be rewritten.
    tokens = r"'(?:\\.|''|[^'\\])*'|\"(?:\\.|\"\"|[^\"\\])*\"|`(?:``|[^`])*`|//[^\n]*|/\*[\s\S]*?\*/"
    masked = re.sub(tokens, lambda m: ' ' * len(m.group()), query)
    matches = list(re.finditer(r'\bSKIP\s+\$skip\b\s+LIMIT\s+\$limit\b', masked, re.I))
    if len(matches) > 1:
        raise ValueError('Streaming requires at most one pagination pair')
    if matches:
        m = matches[0]
        query = query[:m.start()] + ' ' + query[m.end():]
        masked = masked[:m.start()] + ' ' + masked[m.end():]
    if re.search(r'\$(skip|limit)\b|\b(SKIP|LIMIT)\b', masked, re.I):
        raise ValueError('Unsupported pagination in streaming query')
    return query


def queued_batches(source, capacity, metrics=None):
    """Yield FIFO batches; stop and join producer on consumer failure/close."""
    if type(capacity) is not int or capacity < 1:
        raise ValueError('stream_queue_batches must be a positive integer')
    metrics = metrics if metrics is not None else {}
    queue = Queue(maxsize=capacity)
    cancelled = Event()
    finished = object()
    failures = []

    def put(item):
        while not cancelled.is_set():
            started = time.monotonic()
            try:
                queue.put(item, timeout=0.1)
                return True
            except Full:
                pass
            finally:
                metrics['put_wait_s'] = metrics.get('put_wait_s', 0) + time.monotonic() - started
        return False

    def produce():
        iterator = None
        try:
            iterator = iter(source())
            while not cancelled.is_set():
                try:
                    batch = next(iterator)
                except StopIteration:
                    break
                if not put(batch):
                    break
        except BaseException as exc:
            failures.append(exc)
        finally:
            try:
                if iterator is not None and hasattr(iterator, 'close'):
                    iterator.close()
            except BaseException as exc:
                failures.append(exc)
            put(finished)

    thread = Thread(target=produce, name='memgraph-stream-producer')
    thread.start()
    try:
        while True:
            started = time.monotonic()
            batch = queue.get()
            metrics['get_wait_s'] = metrics.get('get_wait_s', 0) + time.monotonic() - started
            if batch is finished:
                if failures:
                    raise failures[0]
                break
            yield batch
    finally:
        cancelled.set()
        thread.join()
