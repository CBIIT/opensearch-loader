"""Disk-backed document assembly before any OpenSearch writes."""
import json
import logging
import sqlite3
import tempfile
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logger = logging.getLogger('OpenSearchLoader')


def merge_document(current, patch):
    """Match partial-document updates: recurse objects, replace arrays/scalars/null."""
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(current.get(key), dict):
            merge_document(current[key], value)
        else:
            current[key] = value
    return current


class DocumentStore:
    def __init__(self, path, dumps=json.dumps):
        self.dumps = dumps
        # A single merge worker owns writes; caller access resumes only after it joins.
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=NORMAL')
        self.db.execute('PRAGMA cache_size=-32768')  # 32 MiB SQLite page cache
        self.db.execute('CREATE TABLE documents (id TEXT PRIMARY KEY, body TEXT NOT NULL)')
        self.db.create_function('merge_document', 2, lambda old, patch:
                                self.dumps(merge_document(json.loads(old), json.loads(patch))))

    def add_page(self, rows, id_field, update=False):
        skipped = 0
        def encoded():
            nonlocal skipped
            for row in rows:
                doc_id = row.get(id_field)
                if not doc_id:
                    skipped += 1
                    continue
                body = {k: v for k, v in row.items() if k != id_field} if update else row
                if update:
                    yield self.dumps(body), str(doc_id)
                else:
                    yield str(doc_id), self.dumps(body)
        before = self.db.total_changes
        with self.db:  # One transaction per fetched page, not per document.
            if update:
                self.db.executemany(
                    'UPDATE documents SET body=merge_document(body, ?) WHERE id=?', encoded())
            else:
                self.db.executemany('INSERT OR REPLACE INTO documents VALUES (?, ?)', encoded())
        return self.db.total_changes - before, skipped

    def batches(self, count=2000, max_bytes=15 * 1024 * 1024):
        batch, size = [], 0
        for (body,) in self.db.execute('SELECT body FROM documents'):
            length = len(body.encode('utf-8'))
            if batch and (len(batch) >= count or size + length > max_bytes):
                yield batch
                batch, size = [], 0
            batch.append(json.loads(body))
            size += length
        if batch:
            yield batch

    def count(self):
        return self.db.execute('SELECT count(*) FROM documents').fetchone()[0]

    def close(self):
        self.db.close()


def stage_pages(store, pages, id_field, update, validate, queue_batches=10):
    """Read on the caller thread; serialize SQLite merges on one worker.

    At most queue_batches pages are submitted (including the active merge).
    The caller must not access the store until this function returns.
    """
    pending = deque()
    totals = dict(rows=0, affected=0, invalid=0, merge_s=0.0, wait_s=0.0)

    def merge(rows):
        started = time.monotonic()
        affected, invalid = store.add_page(rows, id_field, update=update)
        return len(rows), affected, invalid, time.monotonic() - started

    def collect():
        started = time.monotonic()
        rows, affected, invalid, seconds = pending.popleft().result()
        totals['wait_s'] += time.monotonic() - started
        totals['rows'] += rows
        totals['affected'] += affected
        totals['invalid'] += invalid
        totals['merge_s'] += seconds

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='sqlite-merge') as executor:
        try:
            for rows in pages:
                if not rows:
                    continue
                validate(rows)
                pending.append(executor.submit(merge, rows))
                if len(pending) >= queue_batches:
                    collect()
            while pending:
                collect()
        finally:
            for future in pending:
                future.cancel()
            if hasattr(pages, 'close'):
                pages.close()
    return totals


def load_staged_index(loader, spec, mapping):
    """Read all phases into SQLite, then send each completed document once."""
    name, id_field = spec['index_name'], spec['id_field']
    initial = spec.get('initial_query')
    if not initial or not initial.get('query'):
        raise ValueError(f'initial_query.query is required for {name}')
    directory = Path(tempfile.mkdtemp(prefix='opensearch-staging-'))
    path = directory / 'documents.sqlite'
    store = None
    success = False
    started = time.monotonic()
    try:
        # Use the same JSON conversion as OpenSearch (dates and other supported types).
        store = DocumentStore(path, dumps=loader.opensearch.client.transport.serializer.dumps)
        logger.info('%s: SQLITE_STAGING path=%s async_merge=true queue_batches=10', name, path)
        phases = [(initial, 'Initial Query', False)] + [
            (query, query.get('name') or 'unnamed', True)
            for query in spec.get('update_queries', [])]
        for query, label, update in phases:
            if not query.get('query'):
                logger.warning('%s:%s: Missing query, skipping', name, label)
                continue
            phase_started = time.monotonic()
            pages = loader.memgraph.execute_paginated_query(
                query['query'], parameters=query.get('variables', {}),
                page_size=query.get('page_size', 10000), index_name=name,
                query_name=label, test_mode=loader.config.get_test_mode() if update else False)
            def validate(rows):
                if not loader._validate_query_fields(name, rows, mapping):
                    raise ValueError(f'{name}:{label}: Unmapped fields; staging aborted')
            metrics = stage_pages(store, pages, id_field, update, validate)
            rows_seen = metrics['rows']
            changed = metrics['affected']
            skipped = metrics['invalid']
            merge_s = metrics['merge_s']
            read_s = loader.memgraph.last_query_time
            loader.query_timings[f'{name}:{label}'].append(read_s)
            logger.info('%s:%s: STAGING_QUERY_METRICS rows=%s affected=%s invalid_ids=%s '
                        'unmatched_updates=%s read_s=%.3f merge_s=%.3f wall_s=%.3f '
                        'queue_wait_s=%.3f queue_batches=10',
                        name, label, rows_seen, changed, skipped,
                        rows_seen - skipped - changed if update else 0,
                        read_s, merge_s, time.monotonic() - phase_started, metrics['wait_s'])
        total = store.count()
        logger.info('%s: STAGING_READY documents=%s stage_s=%.3f',
                    name, total, time.monotonic() - started)
        # Preserve the existing recreation behavior, but delay it until staging succeeds.
        if loader.config.get_clear_existing_indices():
            loader.opensearch.delete_index(name)
        if loader.config.get_allow_index_creation():
            loader.opensearch.create_index(name, mapping=mapping, force=True)
        write_started = time.monotonic()
        sent = 0
        for batch in store.batches():
            for attempt in range(2):
                try:
                    loader.opensearch.bulk_upsert(name, batch, id_field, query_name='Final documents')
                    break
                except Exception:
                    if attempt:
                        raise
                    logger.warning('%s: Retrying final document batch', name)
            sent += len(batch)
            logger.info('%s: STAGING_WRITE_PROGRESS documents=%s/%s', name, sent, total)
        loader.opensearch.refresh_index(name)
        logger.info('%s: STAGING_COMPLETE documents=%s write_s=%.3f total_s=%.3f',
                    name, sent, time.monotonic() - write_started, time.monotonic() - started)
        success = True
        return sent
    finally:
        if store is not None:
            store.close()
        if success:
            for file in directory.iterdir():
                file.unlink()
            directory.rmdir()
        else:
            logger.error('%s: Staging failed; local diagnostic files retained at %s', name, directory)
