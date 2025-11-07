"""Main loader orchestration logic."""

import logging
from typing import Dict, Any, List
from pathlib import Path

from .config import Config, load_index_spec
from .memgraph_client import MemgraphClient
from .opensearch_client import OpenSearchClient

logger = logging.getLogger(__name__)


class Loader:
    """Main loader that orchestrates data synchronization."""
    
    def __init__(self, config: Config):
        """Initialize loader with configuration.
        
        Args:
            config: Configuration object
        """
        self.config = config
        
        # Initialize Memgraph client
        mg_config = config.get_memgraph_config()
        self.memgraph = MemgraphClient(
            host=mg_config.get('host', 'localhost'),
            port=mg_config.get('port', 7687),
            username=mg_config.get('username'),
            password=mg_config.get('password')
        )
        
        # Initialize OpenSearch client
        os_config = config.get_opensearch_config()
        host = os_config.get('host', 'http://localhost:9200')
        self.opensearch = OpenSearchClient(
            host=host,
            use_ssl=os_config.get('use_ssl', False),
            verify_certs=os_config.get('verify_certs', False),
            username=os_config.get('username'),
            password=os_config.get('password')
        )
    
    def close(self):
        """Close all client connections."""
        self.memgraph.close()
    
    def load(self):
        """Load data from Memgraph to OpenSearch."""
        # Load index specification
        index_spec_file = self.config.get_index_spec_file()
        if not index_spec_file:
            raise ValueError("index_spec_file not specified in configuration")
        
        if not Path(index_spec_file).exists():
            raise FileNotFoundError(f"Index specification file not found: {index_spec_file}")
        
        index_spec = load_index_spec(index_spec_file)
        indices = index_spec.get('indices', [])
        
        if not indices:
            raise ValueError("No indices defined in specification file")
        
        logger.info(f"Processing {len(indices)} indices")
        
        # Process each index
        for index_config in indices:
            self._process_index(index_config)
    
    def _process_index(self, index_config: Dict[str, Any]):
        """Process a single index.
        
        Args:
            index_config: Index configuration dictionary
        """
        index_name = index_config.get('index_name')
        if not index_name:
            raise ValueError("index_name is required in index configuration")
        
        id_field = index_config.get('id_field')
        if not id_field:
            raise ValueError("id_field is required in index configuration")
        
        logger.info(f"Processing index: {index_name}")
        
        # Delete index if configured
        if self.config.get_clear_existing_indices():
            logger.info(f"Deleting index (if exists): {index_name}")
            self.opensearch.delete_index(index_name)
        
        # Create index if configured
        if self.config.get_allow_index_creation():
            logger.info(f"Creating index (if not exists): {index_name}")
            self.opensearch.create_index(index_name)
        
        # Execute initial query
        initial_query_config = index_config.get('initial_query')
        if not initial_query_config:
            raise ValueError(f"initial_query is required for index {index_name}")
        
        query = initial_query_config.get('query')
        if not query:
            raise ValueError(f"initial_query.query is required for index {index_name}")
        
        variables = initial_query_config.get('variables', {})
        page_size = initial_query_config.get('page_size')
        
        logger.info(f"Executing initial query for {index_name}")
        
        try:
            if page_size:
                # Use pagination
                documents = self.memgraph.execute_paginated_query(
                    query, parameters=variables, page_size=page_size
                )
            else:
                # Execute without pagination
                documents = self.memgraph.execute_query(query, parameters=variables)
            
            if documents:
                logger.info(f"Upserting {len(documents)} initial documents to {index_name}")
                self.opensearch.bulk_upsert(index_name, documents, id_field)
            else:
                logger.warning(f"No documents returned from initial query for {index_name}")
        except ValueError as e:
            logger.error(f"Error executing initial query for {index_name}: {e}. Skipping to next query.")
            return
        
        # Execute update queries
        update_queries = index_config.get('update_queries', [])
        for update_query_config in update_queries:
            self._process_update_query(index_name, id_field, update_query_config)
    
    def _process_update_query(self, index_name: str, id_field: str,
                             update_query_config: Dict[str, Any]):
        """Process an update query.
        
        Args:
            index_name: Name of the OpenSearch index
            id_field: Field name to use as document ID
            update_query_config: Update query configuration
        """
        query_name = update_query_config.get('name', 'unnamed')
        query = update_query_config.get('query')
        if not query:
            logger.warning(f"Update query '{query_name}' missing query, skipping")
            return
        
        variables = update_query_config.get('variables', {})
        page_size = update_query_config.get('page_size')
        
        logger.info(f"Executing update query '{query_name}' for {index_name}")
        
        try:
            if page_size:
                # Use pagination
                updates = self.memgraph.execute_paginated_query(
                    query, parameters=variables, page_size=page_size
                )
            else:
                # Execute without pagination
                updates = self.memgraph.execute_query(query, parameters=variables)
            
            if updates:
                logger.info(f"Merging {len(updates)} updates for {index_name}")
                self.opensearch.bulk_merge(index_name, updates, id_field)
            else:
                logger.info(f"No updates returned from query '{query_name}' for {index_name}")
        except ValueError as e:
            logger.error(f"Error executing update query '{query_name}' for {index_name}: {e}. Continuing to next query.")
            return

