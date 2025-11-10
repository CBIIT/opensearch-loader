"""OpenSearch client for index management and document upsert."""

import logging
from typing import List, Dict, Any, Optional
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

logger = logging.getLogger(__name__)


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
        
        http_auth = (username, password) if username and password else None
        
        self.client = OpenSearch(
            hosts=hosts,
            http_auth=http_auth,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            ssl_show_warn=False
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
    
    def create_index(self, index_name: str, id_field: Optional[str] = None):
        """Create an index with basic settings.
        
        Args:
            index_name: Name of the index to create
            id_field: Optional field name to map as keyword type
        """
        if not self.index_exists(index_name):
            # Create index with mappings if id_field is provided
            if id_field:
                body = {
                    "mappings": {
                        "properties": {
                            id_field: {
                                "type": "keyword"
                            }
                        }
                    }
                }
                self.client.indices.create(index=index_name, body=body)
                logger.info(f"Created index: {index_name} with {id_field} as keyword")
            else:
                # Create index with basic settings (OpenSearch will auto-detect field types)
                self.client.indices.create(index=index_name)
                logger.info(f"Created index: {index_name}")
        else:
            logger.info(f"Index already exists: {index_name}")
    
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
    
    def bulk_upsert(self, index_name: str, documents: List[Dict[str, Any]], id_field: str):
        """Bulk upsert documents.
        
        Args:
            index_name: Name of the index
            documents: List of document dictionaries
            id_field: Field name to use as document ID
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
            success, failed = bulk(self.client, actions, refresh=True)
            logger.debug(f"Bulk upserted {success} documents to {index_name}")
            if failed:
                logger.warning(f"Failed to upsert {len(failed)} documents")
        else:
            logger.warning("No documents to upsert")
    
    def bulk_update(self, index_name: str, updates: List[Dict[str, Any]], id_field: str):
        """Bulk update existing documents using OpenSearch Update API.
        
        Only updates existing documents - missing documents are skipped.
        
        Args:
            index_name: Name of the index
            updates: List of update dictionaries
            id_field: Field name to use as document ID
        """
        if not updates:
            logger.warning("No updates to process")
            return
        
        # Hard coded batch size for OpenSearch operations
        BATCH_SIZE = 5000
        
        # Process in batches
        total_updates = len(updates)
        num_batches = (total_updates + BATCH_SIZE - 1) // BATCH_SIZE  # Ceiling division
        
        logger.debug(f"Processing {total_updates} updates in {num_batches} batches of {BATCH_SIZE}")
        
        for i in range(0, total_updates, BATCH_SIZE):
            batch = updates[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            logger.debug(f"Processing batch {batch_num}/{num_batches} ({len(batch)} updates)")
            self._process_update_batch(index_name, batch, id_field)
    
    def _process_update_batch(self, index_name: str, updates: List[Dict[str, Any]], id_field: str):
        """Process a single batch of update operations.
        
        Args:
            index_name: Name of the index
            updates: List of update dictionaries for this batch
            id_field: Field name to use as document ID
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
            success, failed = bulk(self.client, actions, refresh=True)
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
                    logger.debug(f"Skipped {missing_count} missing documents in {index_name}")
                
                if other_errors:
                    logger.warning(f"Failed to update {len(other_errors)} documents in {index_name}")
        except Exception as e:
            logger.error(f"Error executing bulk update: {e}")
            raise

