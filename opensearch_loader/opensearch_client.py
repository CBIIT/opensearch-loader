"""OpenSearch client for index management and document upsert."""

import logging
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import List, Dict, Any, Optional
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk
from requests_aws4auth import AWS4Auth
from botocore.session import Session
from .bulk_metrics import MeasuredBulkClient
from .opensearch_metrics import log_index_settings

logger = logging.getLogger("OpenSearchLoader")

# Explicit HTTP chunk size; the helper otherwise defaults to 500 actions.
BULK_CHUNK_SIZE = 5000
UPDATE_WORKERS = 4


class OpenSearchClient:
    """Client for managing OpenSearch indices and documents."""
    
    def __init__(self, host: str, use_ssl: bool = False,
                 verify_certs: bool = False, username: Optional[str] = None,
                 password: Optional[str] = None):
        """Initialize OpenSearch client.
        
        Args:
            host: OpenSearch host URL
            use_ssl: Whether to use SSL
            verify_certs: Whether to verify SSL certificates
            username: Optional username for authentication
            password: Optional password for authentication
        """
        # Normalize host to a list for OpenSearch library
        hosts = [host]
        timeout_seconds = 60
        http_auth = (username, password) if username and password else None

        if 'amazonaws.com' in host:
            http_auth = AWS4Auth(
                refreshable_credentials=Session().get_credentials(),
                region='us-east-1',
                service='es'
            )
        
        self.client = OpenSearch(
            hosts=hosts,
            http_auth=http_auth,
            port=443,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            timeout=timeout_seconds
        )
        logger.info(f"Connected to OpenSearch at {host}")
    
    def index_exists(self, index_name: str) -> bool:
        """Check if an index exists.
        
        Args:
            index_name: Name of the index
            
        Returns:
            True if index exists, False otherwise
        """
        return self.client.indices.exists(index=index_name)
    
    def delete_index(self, index_name: str):
        """Delete an index.
        
        Args:
            index_name: Name of the index to delete
        """
        if self.index_exists(index_name):
            self.client.indices.delete(index=index_name)
            logger.info(f"Deleted index: {index_name}")
        else:
            logger.info(f"Index does not exist, skipping deletion: {index_name}")
    
    def create_index(self, index_name: str, id_field: Optional[str] = None,
                     mapping: Optional[Dict[str, Any]] = None, force: bool = False):
        """Create an index with basic settings.
        
        Args:
            index_name: Name of the index to create
            id_field: Optional field name to map as keyword type (used if mapping not provided)
            mapping: Optional custom mapping dictionary (overrides id_field if provided)
            force: If True, delete existing index before creating (default: False)
        """
        # Force delete if requested (used when we want to ensure clean recreation)
        if force and self.index_exists(index_name):
            logger.info(f"Force recreating index: {index_name}")
            self.delete_index(index_name)
        
        if not self.index_exists(index_name):
            body = {}
            
            # Add settings
            body["settings"] = {
                "number_of_shards": 1,
                "index.mapping.nested_objects.limit": 100000
            }
            
            # Add mappings
            if mapping:
                # Use provided custom mapping
                # Disable dynamic mapping to prevent auto-detection of field types
                body["mappings"] = {
                    "dynamic": False,  # Disable dynamic mapping - only use explicit mappings
                    "properties": mapping
                }
                logger.info(f"Created index: {index_name} with custom mapping (dynamic mapping disabled)")
            elif id_field:
                # Use id_field to create simple mapping
                body["mappings"] = {
                    "properties": {
                        id_field: {
                            "type": "keyword"
                        }
                    }
                }
                logger.info(f"Created index: {index_name} with {id_field} as keyword")
            else:
                # No mapping specified - OpenSearch will auto-detect field types
                logger.info(f"Created index: {index_name} (auto-detect field types)")
            
            self.client.indices.create(index=index_name, body=body)
        else:
            logger.warning(f"Index already exists: {index_name}. Skipping creation. Use force=True to recreate.")
        log_index_settings(self.client, logger, index_name)
    
    def refresh_index(self, index_name: str):
        """Refresh an index to make all operations visible.
        
        Args:
            index_name: Name of the index to refresh
        """
        self.client.indices.refresh(index=index_name)
        logger.debug(f"Refreshed index: {index_name}")
    
    def get_document(self, index_name: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get a document by ID.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            
        Returns:
            Document source if found, None otherwise
        """
        try:
            response = self.client.get(index=index_name, id=doc_id)
            return response.get('_source')
        except Exception as e:
            if 'not_found' in str(e).lower():
                return None
            raise
    
    def upsert_document(self, index_name: str, doc_id: str, document: Dict[str, Any]):
        """Upsert a single document.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            document: Document source
        """
        self.client.index(
            index=index_name,
            id=doc_id,
            body=document,
            refresh=True
        )
    
    def merge_document(self, index_name: str, doc_id: str, updates: Dict[str, Any]):
        """Merge updates into an existing document.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            updates: Dictionary of fields to update/add
        """
        # Get existing document
        existing = self.get_document(index_name, doc_id)
        
        if existing:
            # Merge: overwrite provided fields, preserve others
            merged = {**existing, **updates}
        else:
            # Document doesn't exist, create new one
            merged = updates
        
        # Upsert the merged document
        self.upsert_document(index_name, doc_id, merged)
    
    def bulk_upsert(self, index_name: str, documents: List[Dict[str, Any]], id_field: str,
                    query_name: Optional[str] = None):
        """Bulk upsert documents.
        
        Args:
            index_name: Name of the index
            documents: List of document dictionaries
            id_field: Field name to use as document ID
            query_name: Optional query name for logging prefix
        """
        actions = []
        for doc in documents:
            doc_id = doc.get(id_field)
            if not doc_id:
                logger.warning(f"Document missing {id_field}, skipping: {doc}")
                continue
            
            # Keep id_field in document source (stored as both _id and as a keyword field)
            # The id_field will be stored with its original name as a keyword type
            action = {
                "_index": index_name,
                "_id": str(doc_id),
                "_source": doc
            }
            actions.append(action)
        
        if actions:
            success, failed = bulk(
                MeasuredBulkClient(self.client, index_name, query_name, "index"),
                actions, refresh=False, chunk_size=BULK_CHUNK_SIZE)
            if index_name and query_name:
                logger.debug(f"{index_name}:{query_name}: Bulk upserted {success} documents")
            elif index_name:
                logger.debug(f"{index_name}: Bulk upserted {success} documents")
            else:
                logger.debug(f"Bulk upserted {success} documents to {index_name}")
            if failed:
                logger.warning(f"Failed to upsert {len(failed)} documents")
        else:
            logger.warning("No documents to upsert")
    
    def bulk_update(self, index_name: str, updates: List[Dict[str, Any]], id_field: str,
                    query_name: Optional[str] = None):
        """Bulk update existing documents using OpenSearch Update API.
        
        Only updates existing documents - missing documents are skipped.
        
        Args:
            index_name: Name of the index
            updates: List of update dictionaries
            id_field: Field name to use as document ID
            query_name: Optional query name for logging prefix
        """
        if not updates:
            logger.warning("No updates to process")
            return
        
        # The same normalized OpenSearch ID always belongs to one serial lane.
        # Wait for all lanes before returning so callers retain phase/retry barriers.
        lanes = [[] for _ in range(UPDATE_WORKERS)]
        for update in updates:
            doc_id = str(update.get(id_field))
            lanes[zlib.crc32(doc_id.encode('utf-8')) % UPDATE_WORKERS].append(update)
        stopped = Event()
        started = time.monotonic()

        def write_lane(rows):
            try:
                for offset in range(0, len(rows), BULK_CHUNK_SIZE):
                    if stopped.is_set():
                        return
                    self._process_update_batch(
                        index_name, rows[offset:offset + BULK_CHUNK_SIZE],
                        id_field, query_name=query_name)
            except Exception:
                stopped.set()
                raise

        status = 'failed'
        try:
            with ThreadPoolExecutor(max_workers=UPDATE_WORKERS, thread_name_prefix='opensearch-update') as pool:
                futures = [pool.submit(write_lane, rows) for rows in lanes if rows]
                for future in futures:
                    future.result()
            status = 'complete'
        finally:
            logger.info(
                '%s:%s: UPDATE_WORKER_METRICS status=%s workers=%s '
                'lane_rows=%s wall_s=%.3f chunk_size=%s',
                index_name, query_name, status, UPDATE_WORKERS, [len(rows) for rows in lanes],
                time.monotonic() - started, BULK_CHUNK_SIZE)

    def _process_update_batch(self, index_name: str, updates: List[Dict[str, Any]], id_field: str,
                             query_name: Optional[str] = None):
        """Process a single batch of update operations.
        
        Args:
            index_name: Name of the index
            updates: List of update dictionaries for this batch
            id_field: Field name to use as document ID
            query_name: Optional query name for logging prefix
        """
        actions = []
        for update in updates:
            doc_id = update.get(id_field)
            if not doc_id:
                logger.warning(f"Update missing {id_field}, skipping: {update}")
                continue
            
            # Remove id_field from update doc (it's used as _id, not as a field to update)
            update_doc = {k: v for k, v in update.items() if k != id_field}
            
            if not update_doc:
                logger.warning(f"Update for {doc_id} has no fields to update, skipping")
                continue
            
            action = {
                "_op_type": "update",
                "_index": index_name,
                "_id": str(doc_id),
                "doc": update_doc,
                "doc_as_upsert": False  # Don't create documents if they don't exist
            }
            actions.append(action)
        
        if not actions:
            logger.warning("No valid updates to process in this batch")
            return
        
        # Execute bulk update
        try:
            success, failed = bulk(
                MeasuredBulkClient(self.client, index_name, query_name, "update"),
                actions, refresh=False, chunk_size=BULK_CHUNK_SIZE)
            if index_name and query_name:
                logger.debug(f"{index_name}:{query_name}: Bulk updated {success} documents")
            elif index_name:
                logger.debug(f"{index_name}: Bulk updated {success} documents")
            else:
                logger.debug(f"Bulk updated {success} documents in {index_name}")
            
            if failed:
                # Count missing documents separately
                missing_count = 0
                other_errors = []
                for item in failed:
                    # Handle different error structures from opensearchpy
                    error_info = item.get('update', {}).get('error', {})
                    if not error_info:
                        # Try alternative structure
                        error_info = item.get('error', {})
                    
                    error_type = error_info.get('type', '')
                    error_reason = str(error_info).lower()
                    
                    if ('document_missing_exception' in error_type or 
                        'not_found' in error_type or 
                        'not_found' in error_reason or
                        'document_missing' in error_reason):
                        missing_count += 1
                    else:
                        other_errors.append(item)
                
                if missing_count > 0:
                    if index_name and query_name:
                        logger.debug(f"{index_name}:{query_name}: Skipped {missing_count} missing documents")
                    elif index_name:
                        logger.debug(f"{index_name}: Skipped {missing_count} missing documents")
                    else:
                        logger.debug(f"Skipped {missing_count} missing documents in {index_name}")
                
                if other_errors:
                    logger.warning(f"Failed to update {len(other_errors)} documents in {index_name}")
        except Exception as e:
            logger.error(f"Error executing bulk update: {e}")
            raise

