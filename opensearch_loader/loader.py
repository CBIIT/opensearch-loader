"""Main loader orchestration logic."""

import logging
import time
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
        
        # Filter indices based on selected_indices configuration
        # If selected_indices is None or empty list, process all indices
        selected_indices = self.config.get_selected_indices()
        if selected_indices:
            # Trim all selected index names (defensive trimming)
            selected_set = {name.strip() for name in selected_indices if name}
            
            # Create a map of index_name to index_config for efficient lookup
            index_map = {}
            for index_config in indices:
                index_name = index_config.get('index_name')
                if index_name:
                    # Trim index name for comparison
                    trimmed_name = index_name.strip()
                    index_map[trimmed_name] = index_config
            
            # Check for selected indices that don't exist and log warnings
            for selected_name in selected_set:
                if selected_name not in index_map:
                    logger.warning(f"Selected index '{selected_name}' does not exist in indices file. Skipping.")
            
            # Filter indices to only those in selected_set
            filtered_indices = [index_config for index_config in indices 
                              if index_config.get('index_name', '').strip() in selected_set]
            
            if not filtered_indices:
                logger.warning("No valid indices found after filtering. Nothing to process.")
                return
            
            logger.info(f"Filtering enabled: {len(selected_set)} index(es) selected, {len(filtered_indices)} will be processed")
            indices = filtered_indices
        else:
            logger.info(f"Processing all {len(indices)} indices")
        
        start_time = time.time()
        
        # Process each index
        for index_config in indices:
            try:
                self._process_index(index_config)
            except Exception as e:
                logger.error(f"Error processing index {index_config.get('index_name', 'unknown')}: {e}. Skipping to next index.")
                continue
        
        total_time = time.time() - start_time
        logger.info(f"Total execution time: {total_time:.2f} seconds")
    
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
        
        logger.info(f"Starting index: {index_name}")
        
        # Delete index if configured
        if self.config.get_clear_existing_indices():
            logger.info(f"Deleting index (if exists): {index_name}")
            self.opensearch.delete_index(index_name)
        
        # Create index if configured
        if self.config.get_allow_index_creation():
            logger.info(f"Creating index (if not exists): {index_name}")
            self.opensearch.create_index(index_name, id_field=id_field)
        
        # Execute initial query
        initial_query_config = index_config.get('initial_query')
        if not initial_query_config:
            raise ValueError(f"initial_query is required for index {index_name}")
        
        query = initial_query_config.get('query')
        if not query:
            raise ValueError(f"initial_query.query is required for index {index_name}")
        
        variables = initial_query_config.get('variables', {})
        page_size = initial_query_config.get('page_size', 10000)  # Default to 10000
        
        logger.info(f"Starting initial query for {index_name}")
        
        total_documents = 0
        page_num = 0
        
        try:
            # Process pages incrementally
            for page_documents in self.memgraph.execute_paginated_query(
                query, parameters=variables, page_size=page_size
            ):
                page_num += 1
                
                if not page_documents:
                    continue
                
                # Process this page with retry logic
                try:
                    logger.info(f"Loading page {page_num} to OpenSearch: {len(page_documents)} documents")
                    self.opensearch.bulk_upsert(index_name, page_documents, id_field)
                    total_documents += len(page_documents)
                    logger.info(f"Completed loading page {page_num} to OpenSearch: {len(page_documents)} documents (total: {total_documents})")
                except Exception as e:
                    # Retry once
                    logger.warning(f"Error processing page {page_num} for {index_name}, retrying: {e}")
                    try:
                        logger.info(f"Retrying loading page {page_num} to OpenSearch: {len(page_documents)} documents")
                        self.opensearch.bulk_upsert(index_name, page_documents, id_field)
                        total_documents += len(page_documents)
                        logger.info(f"Retry successful for page {page_num}: {len(page_documents)} documents (total: {total_documents})")
                    except Exception as retry_error:
                        logger.error(f"Retry failed for page {page_num} for {index_name}: {retry_error}. Skipping entire index.")
                        raise
            
            if total_documents == 0:
                logger.warning(f"No documents returned from initial query for {index_name}")
            else:
                logger.info(f"Completed initial query for {index_name}: {total_documents} total documents processed")
        except ValueError as e:
            logger.error(f"Error executing initial query for {index_name}: {e}. Skipping to next query.")
            raise
        
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
        page_size = update_query_config.get('page_size', 10000)  # Default to 10000
        
        logger.info(f"Starting update query '{query_name}' for {index_name}")
        
        total_updates = 0
        page_num = 0
        
        try:
            # Process pages incrementally
            for page_updates in self.memgraph.execute_paginated_query(
                query, parameters=variables, page_size=page_size
            ):
                page_num += 1
                
                if not page_updates:
                    continue
                
                # Process this page with retry logic
                try:
                    logger.info(f"Loading page {page_num} to OpenSearch: {len(page_updates)} updates")
                    self.opensearch.bulk_update(index_name, page_updates, id_field)
                    total_updates += len(page_updates)
                    logger.info(f"Completed loading page {page_num} to OpenSearch: {len(page_updates)} updates (total: {total_updates})")
                except Exception as e:
                    # Retry once
                    logger.warning(f"Error processing page {page_num} for update query '{query_name}' in {index_name}, retrying: {e}")
                    try:
                        logger.info(f"Retrying loading page {page_num} to OpenSearch: {len(page_updates)} updates")
                        self.opensearch.bulk_update(index_name, page_updates, id_field)
                        total_updates += len(page_updates)
                        logger.info(f"Retry successful for page {page_num}: {len(page_updates)} updates (total: {total_updates})")
                    except Exception as retry_error:
                        logger.error(f"Retry failed for page {page_num} for update query '{query_name}' in {index_name}: {retry_error}. Skipping entire index.")
                        raise
            
            if total_updates == 0:
                logger.info(f"No updates returned from query '{query_name}' for {index_name}")
            else:
                logger.info(f"Completed update query '{query_name}' for {index_name}: {total_updates} total updates processed")
        except ValueError as e:
            logger.error(f"Error executing update query '{query_name}' for {index_name}: {e}. Continuing to next query.")
            raise

