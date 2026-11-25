"""Build each document from all queries on the worker, then write it to OpenSearch once.

The partial-update path rewrites every document once per update query. This path
keeps the same queries and merge rules but merges rows in a local SQLite file and
bulk-indexes the finished documents into a new versioned index behind an alias.
"""

import json
import logging
import os
import sqlite3
import tempfile
import time
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from opensearchpy.exceptions import OpenSearchException
from opensearchpy.helpers import parallel_bulk

logger = logging.getLogger("OpenSearchLoader")

WRITE_BATCH_BYTES = 10 * 1024 * 1024
WRITE_THREADS = 4
SWAP_RETRIES = 20
SWAP_RETRY_SECONDS = 30


def _json_default(value):
    """Serialize values the way opensearch-py's JSONSerializer does."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Unable to serialize {value!r} (type: {type(value)})")


def _dumps(value) -> str:
    return json.dumps(value, default=_json_default)


def deep_merge(target: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    """Merge like an OpenSearch partial update: objects merge field by field, anything else is replaced."""
    for key, value in updates.items():
        current = target.get(key)
        if isinstance(value, dict) and isinstance(current, dict):
            deep_merge(current, value)
        else:
            target[key] = value
    return target


class MergeStore:
    """Documents keyed by ID in a local SQLite file, deleted on close."""

    def __init__(self, directory: str | None = None, name: str = "merge"):
        handle, self.path = tempfile.mkstemp(prefix=f"{name}-", suffix=".sqlite", dir=directory)
        os.close(handle)
        self.conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, doc TEXT NOT NULL)")

    def put_initial(self, rows: Iterable[dict[str, Any]], id_field: str) -> tuple[int, int]:
        """Store initial-query rows; a repeated ID replaces the earlier row, like a bulk index."""
        stored = skipped = 0
        self.conn.execute("BEGIN")
        for row in rows:
            doc_id = row.get(id_field)
            if not doc_id:
                skipped += 1
                continue
            self.conn.execute("INSERT OR REPLACE INTO docs VALUES (?, ?)",
                              (str(doc_id), _dumps(row)))
            stored += 1
        self.conn.execute("COMMIT")
        return stored, skipped

    def apply_update(self, rows: Iterable[dict[str, Any]], id_field: str) -> tuple[int, int]:
        """Merge update rows into stored documents; rows for unknown IDs are dropped (doc_as_upsert=False)."""
        merged = dropped = 0
        self.conn.execute("BEGIN")
        for row in rows:
            doc_id = row.get(id_field)
            fields = {k: v for k, v in row.items() if k != id_field}
            if not doc_id or not fields:
                dropped += 1
                continue
            found = self.conn.execute("SELECT doc FROM docs WHERE id = ?", (str(doc_id),)).fetchone()
            if found is None:
                dropped += 1
                continue
            doc = deep_merge(json.loads(found[0]), json.loads(_dumps(fields)))
            self.conn.execute("UPDATE docs SET doc = ? WHERE id = ?", (_dumps(doc), str(doc_id)))
            merged += 1
        self.conn.execute("COMMIT")
        return merged, dropped

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM docs").fetchone()[0]

    def documents(self) -> Iterator[tuple[str, str]]:
        """Yield (id, JSON source) pairs in ID order."""
        yield from self.conn.execute("SELECT id, doc FROM docs ORDER BY id")

    def close(self):
        try:
            self.conn.close()
        finally:
            if os.path.exists(self.path):
                os.remove(self.path)


def versioned_name(index_name: str) -> str:
    return f"{index_name}__{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"


def _write_documents(client, index: str, store: MergeStore) -> int:
    """Bulk-index every stored document with a few threads and a small queue."""
    def actions():
        for doc_id, source in store.documents():
            yield {"_index": index, "_id": doc_id, "_source": source}

    written = 0
    failures: list[Any] = []
    for ok, item in parallel_bulk(client, actions(), thread_count=WRITE_THREADS, queue_size=WRITE_THREADS,
                                  chunk_size=5000, max_chunk_bytes=WRITE_BATCH_BYTES,
                                  raise_on_error=False, request_timeout=120):
        if ok:
            written += 1
        else:
            failures.append(item)
    if failures:
        raise RuntimeError(f"{index}: {len(failures)} documents failed to index, first: {failures[0]}")
    return written


def _swap_alias(client, alias: str, new_index: str):
    """Point the alias at new_index, retrying while a snapshot blocks the change."""
    for attempt in range(1, SWAP_RETRIES + 1):
        actions = []
        if client.indices.exists_alias(name=alias):
            for old in client.indices.get_alias(name=alias):
                actions.append({"remove": {"index": old, "alias": alias}})
        elif client.indices.exists(index=alias):
            # A concrete index from the old loader holds the name; remove it in the same atomic call.
            actions.append({"remove_index": {"index": alias}})
        actions.append({"add": {"index": new_index, "alias": alias}})
        try:
            client.indices.update_aliases(body={"actions": actions})
            return [a["remove"]["index"] for a in actions if "remove" in a]
        except Exception as exc:
            if "snapshot_in_progress" not in str(exc) or attempt == SWAP_RETRIES:
                raise
            logger.warning(f"{alias}: snapshot in progress, retrying alias swap in {SWAP_RETRY_SECONDS}s "
                           f"({attempt}/{SWAP_RETRIES})")
            time.sleep(SWAP_RETRY_SECONDS)


def _delete_old(client, indices: list[str]):
    """Best effort: an old index that cannot be deleted now is left for the next run."""
    for index in indices:
        try:
            client.indices.delete(index=index)
            logger.info(f"Deleted previous index {index}")
        except OpenSearchException as exc:
            logger.warning(f"Could not delete previous index {index}: {exc}")


def _delete_orphans(client, alias: str):
    """Remove versions of this index left behind by earlier runs that are no longer aliased."""
    live = set(client.indices.get_alias(name=alias)) if client.indices.exists_alias(name=alias) else set()
    try:
        found = client.indices.get(index=f"{alias}__*", ignore_unavailable=True, allow_no_indices=True)
    except OpenSearchException as exc:
        logger.warning(f"{alias}: could not list earlier versions: {exc}")
        return
    _delete_old(client, [name for name in found if name not in live])


def _run_query(loader, index_name: str, query_config: dict[str, Any], query_name: str,
               mapping: dict[str, Any], test_mode: bool) -> Iterator[list[dict[str, Any]]]:
    """Page through a query, validating fields on the first page and recording Memgraph time."""
    query = query_config.get('query')
    if not query:
        raise ValueError(f"{index_name}:{query_name}: query is required")
    validated = False
    try:
        for page in loader.memgraph.execute_paginated_query(
                query, parameters=query_config.get('variables', {}),
                page_size=query_config.get('page_size', 10000),
                index_name=index_name, query_name=query_name, test_mode=test_mode):
            if not page:
                continue
            if not validated:
                if not loader._validate_query_fields(index_name, page, mapping):
                    raise ValueError(f"{index_name}:{query_name}: query returned unmapped fields")
                validated = True
            yield page
    finally:
        if loader.memgraph.last_query_time > 0:
            loader.query_timings[f"{index_name}:{query_name}"].append(loader.memgraph.last_query_time)


def load_merged(loader, index_config: dict[str, Any]) -> int:
    """Run every query for one index, merge on the worker, write once, then swap the alias."""
    index_name = index_config['index_name']
    id_field = index_config.get('id_field')
    if not id_field:
        raise ValueError("id_field is required in index configuration")
    if not index_config.get('mapping'):
        raise ValueError(f"Index {index_name} is missing required 'mapping' configuration")
    mapping = loader._parse_mapping(index_config['mapping'])
    if not index_config.get('initial_query'):
        raise ValueError(f"initial_query is required for index {index_name}")

    client = loader.opensearch.client
    store = MergeStore(loader.config.get('merge_dir'), name=index_name)
    started = time.monotonic()
    try:
        for page in _run_query(loader, index_name, index_config['initial_query'], "Initial Query", mapping, False):
            _, skipped = store.put_initial(page, id_field)
            if skipped:
                logger.warning(f"{index_name}:Initial Query: skipped {skipped} rows with missing {id_field}")
        total = store.count()
        logger.info(f"{index_name}:Initial Query: {total} documents staged")

        test_mode = loader.config.get_test_mode()
        for update in index_config.get('update_queries', []):
            name = update.get('name') or 'unnamed'
            merged = dropped = 0
            for page in _run_query(loader, index_name, update, name, mapping, test_mode):
                m, d = store.apply_update(page, id_field)
                merged += m
                dropped += d
            logger.info(f"{index_name}:{name}: merged {merged} rows, dropped {dropped}")
        merge_seconds = time.monotonic() - started

        _delete_orphans(client, index_name)
        new_index = versioned_name(index_name)
        client.indices.create(index=new_index, body={
            "settings": {
                "number_of_shards": index_config.get('number_of_shards', 2),
                "number_of_replicas": 0,
                "refresh_interval": "-1",
                "index.mapping.nested_objects.limit": 100000,
            },
            "mappings": {"dynamic": False, "properties": mapping},
        })
        write_started = time.monotonic()
        try:
            written = _write_documents(client, new_index, store)
            client.indices.put_settings(index=new_index, body={"index": {"refresh_interval": None}})
            client.indices.refresh(index=new_index)
            if written != total:
                raise RuntimeError(f"{index_name}: wrote {written} of {total} documents")
        except Exception:
            _delete_old(client, [new_index])
            raise
        write_seconds = time.monotonic() - write_started

        replaced = _swap_alias(client, index_name, new_index)
        _delete_old(client, replaced)
        logger.info(f"{index_name}: {written} documents written once to {new_index}; "
                    f"memgraph+merge={merge_seconds:.0f}s opensearch_write={write_seconds:.0f}s")
        return written
    finally:
        store.close()
