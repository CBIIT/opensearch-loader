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
    
    def create_index(self, index_name: str):
        """Create an index with basic settings.
        
        Args:
            index_name: Name of the index to create
        """
        if not self.index_exists(index_name):
            # Create index with basic settings
            # OpenSearch will auto-detect field types
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
            
            # Remove id_field from document source (it will be stored as _id)
            doc_source = {k: v for k, v in doc.items() if k != id_field}
            
            action = {
                "_index": index_name,
                "_id": str(doc_id),
                "_source": doc_source
            }
            actions.append(action)
        
        if actions:
            success, failed = bulk(self.client, actions, refresh=True)
            logger.info(f"Bulk upserted {success} documents to {index_name}")
            if failed:
                logger.warning(f"Failed to upsert {len(failed)} documents")
        else:
            logger.warning("No documents to upsert")
    
    def bulk_merge(self, index_name: str, updates: List[Dict[str, Any]], id_field: str):
        """Bulk merge updates into existing documents.
        
        Args:
            index_name: Name of the index
            updates: List of update dictionaries
            id_field: Field name to use as document ID
        """
        # Fetch existing documents and merge
        merged_docs = []
        for update in updates:
            doc_id = update.get(id_field)
            if not doc_id:
                logger.warning(f"Update missing {id_field}, skipping: {update}")
                continue
            
            existing = self.get_document(index_name, str(doc_id))
            
            if existing:
                # Merge: overwrite provided fields, preserve others
                merged = {**existing, **update}
            else:
                # Document doesn't exist, create new one
                merged = update
            
            merged_docs.append(merged)
        
        # Bulk upsert merged documents
        if merged_docs:
            self.bulk_upsert(index_name, merged_docs, id_field)

