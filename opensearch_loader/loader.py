"""Main loader orchestration logic."""

import logging
import time
import os
import yaml
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from collections import defaultdict

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
        
        # Statistics tracking
        self.index_stats: List[Dict[str, Any]] = []
        # Query timing: key is "index:query_name", value is list of execution times
        self.query_timings: Dict[str, List[float]] = defaultdict(list)
        
        # Model instance (initialized if model_files are configured)
        self.model = None
    
    def close(self):
        """Close all client connections."""
        self.memgraph.close()
    
    def _format_time(self, seconds: float) -> str:
        """Format seconds into minutes:seconds format.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted string like "2m 35s" or "45s"
        """
        if seconds < 0:
            return "N/A"
        
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        
        if minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"
    
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
        # This filtering applies to ALL index types: query-based, about_file, and model indices
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
            # This applies to all index types (query-based, about_file, model)
            filtered_indices = [index_config for index_config in indices 
                              if index_config.get('index_name', '').strip() in selected_set]
            
            if not filtered_indices:
                logger.warning("No valid indices found after filtering. Nothing to process.")
                return
            
            logger.info(f"Filtering enabled: {len(selected_set)} index(es) selected, {len(filtered_indices)} will be processed")
            indices = filtered_indices
        else:
            logger.info(f"Processing all {len(indices)} indices")
        
        # Initialize model if model_files are configured AND there are model indices to process
        model_files = self.config.get_model_files()
        if model_files:
            # Check if any of the filtered indices are model type
            has_model_indices = any(
                index_config.get('type') == 'model' 
                for index_config in indices
            )
            if has_model_indices:
                self.read_model(model_files)
            else:
                logger.debug("Model files configured but no model indices in selected/filtered indices, skipping model initialization")
        
        start_time = time.time()
        
        # Process each index
        for index_config in indices:
            index_name = index_config.get('index_name', 'unknown')
            index_start_time = time.time()
            
            try:
                document_count = self._process_index(index_config)
                index_end_time = time.time()
                index_duration = index_end_time - index_start_time
                
                # Record successful index statistics
                self.index_stats.append({
                    'index_name': index_name,
                    'document_count': document_count,
                    'duration': index_duration,
                    'error': False
                })
            except Exception as e:
                index_end_time = time.time()
                index_duration = index_end_time - index_start_time
                error_occurred = True
                logger.error(f"Error processing index {index_name}: {e}. Skipping to next index.")
                
                # Record failed index statistics
                self.index_stats.append({
                    'index_name': index_name,
                    'document_count': 'ERROR',
                    'duration': index_duration,
                    'error': True
                })
                continue
        
        total_time = time.time() - start_time
        
        # Print and save summary
        self._print_summary(total_time)
        
        # Save query timings
        self._save_query_timings()
    
    def _process_index(self, index_config: Dict[str, Any]) -> int:
        """Process a single index.
        
        Args:
            index_config: Index configuration dictionary
            
        Returns:
            Number of documents loaded
        """
        index_name = index_config.get('index_name')
        if not index_name:
            raise ValueError("index_name is required in index configuration")
        
        index_type = index_config.get('type')
        
        logger.info(f"Starting index: {index_name}")
        
        # Route to appropriate handler based on index type
        if index_type == 'about_file':
            return self._process_about_file_index(index_config)
        elif index_type == 'model':
            return self._process_model_index(index_config)
        else:
            # Default: treat as query-based index
            return self._process_query_index(index_config)
    
    def _get_default_about_mapping(self) -> Dict[str, Any]:
        """Get default mapping for about file indices.
        
        Returns:
            Default mapping dictionary
        """
        return {
            'page': {'type': 'search_as_you_type'},
            'title': {'type': 'search_as_you_type'},
            'primaryContentImage': {'type': 'text'},
            'content': {'type': 'object'}
        }
    
    def _get_default_model_mapping(self, subtype: str) -> Dict[str, Any]:
        """Get default mapping for model indices based on subtype.
        
        Args:
            subtype: Model subtype ('node', 'property', or 'value')
            
        Returns:
            Default mapping dictionary
        """
        if subtype == 'node':
            return {
                'node': {'type': 'search_as_you_type'},
                'node_kw': {'type': 'keyword'}
            }
        elif subtype == 'property':
            return {
                'node': {'type': 'search_as_you_type'},
                'property': {'type': 'search_as_you_type'},
                'property_kw': {'type': 'keyword'},
                'property_description': {'type': 'search_as_you_type'},
                'property_required': {'type': 'search_as_you_type'},
                'property_type': {'type': 'search_as_you_type'}
            }
        elif subtype == 'value':
            return {
                'node': {'type': 'search_as_you_type'},
                'property': {'type': 'search_as_you_type'},
                'property_description': {'type': 'search_as_you_type'},
                'property_required': {'type': 'search_as_you_type'},
                'property_type': {'type': 'search_as_you_type'},
                'value': {'type': 'search_as_you_type'},
                'value_kw': {'type': 'keyword'}
            }
        else:
            # Unknown subtype, return empty mapping
            return {}
    
    def _process_about_file_index(self, index_config: Dict[str, Any]) -> int:
        """Process an about file index.
        
        Args:
            index_config: Index configuration dictionary
            
        Returns:
            Number of documents loaded
        """
        index_name = index_config.get('index_name')
        # Use provided mapping or default
        mapping = index_config.get('mapping') or self._get_default_about_mapping()
        about_file = self.config.get_about_file()
        
        if not about_file:
            logger.warning(f'"about_file" not set in configuration file, {index_name} will not be loaded!')
            return 0
        
        return self.load_about_page(index_name, mapping, about_file)
    
    def _process_model_index(self, index_config: Dict[str, Any]) -> int:
        """Process a model index.
        
        Args:
            index_config: Index configuration dictionary
            
        Returns:
            Number of documents loaded
        """
        index_name = index_config.get('index_name')
        subtype = index_config.get('subtype')
        
        if not self.model:
            logger.warning(f'"model_files" not set in configuration file, {index_name} will not be loaded!')
            return 0
        
        if not subtype:
            logger.warning(f'"subtype" not specified for model index {index_name}, will not be loaded!')
            return 0
        
        # Use provided mapping or auto-generate based on subtype
        mapping = index_config.get('mapping') or self._get_default_model_mapping(subtype)
        
        return self.load_model(index_name, mapping, subtype)
    
    def _process_query_index(self, index_config: Dict[str, Any]) -> int:
        """Process a query-based index (default behavior).
        
        Args:
            index_config: Index configuration dictionary
            
        Returns:
            Number of documents loaded from initial query
        """
        index_name = index_config.get('index_name')
        id_field = index_config.get('id_field')
        if not id_field:
            raise ValueError("id_field is required in index configuration")
        
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
        
        query_name = "Initial Query"
        logger.info(f"{index_name}:{query_name}: Starting initial query")
        
        # Track query execution time (will be set after generator is exhausted)
        query_key = f"{index_name}:{query_name}"
        
        total_documents = 0
        page_num = 0
        
        try:
            # Process pages incrementally
            # Note: Query execution time is tracked inside execute_paginated_query
            # and will be available in self.memgraph.last_query_time after the loop
            for page_documents in self.memgraph.execute_paginated_query(
                query, parameters=variables, page_size=page_size,
                index_name=index_name, query_name=query_name
            ):
                page_num += 1
                
                if not page_documents:
                    continue
                
                # Process this page with retry logic
                try:
                    logger.info(f"{index_name}:{query_name}: Loading page {page_num} to OpenSearch: {len(page_documents)} documents")
                    self.opensearch.bulk_upsert(index_name, page_documents, id_field, query_name=query_name)
                    total_documents += len(page_documents)
                    logger.info(f"{index_name}:{query_name}: Completed loading page {page_num} to OpenSearch: {len(page_documents)} documents (total: {total_documents})")
                except Exception as e:
                    # Retry once
                    logger.warning(f"Error processing page {page_num} for {index_name}, retrying: {e}")
                    try:
                        logger.info(f"{index_name}:{query_name}: Retrying loading page {page_num} to OpenSearch: {len(page_documents)} documents")
                        self.opensearch.bulk_upsert(index_name, page_documents, id_field, query_name=query_name)
                        total_documents += len(page_documents)
                        logger.info(f"{index_name}:{query_name}: Retry successful for page {page_num}: {len(page_documents)} documents (total: {total_documents})")
                    except Exception as retry_error:
                        logger.error(f"Retry failed for page {page_num} for {index_name}: {retry_error}. Skipping entire index.")
                        raise
            
            if total_documents == 0:
                logger.warning(f"No documents returned from initial query for {index_name}")
            else:
                logger.info(f"{index_name}:{query_name}: Completed initial query: {total_documents} total documents processed")
        except ValueError as e:
            logger.error(f"Error executing initial query for {index_name}: {e}. Skipping to next query.")
            raise
        finally:
            # Record query execution time (only Memgraph query time, not OpenSearch operations)
            # This is set after the generator is exhausted
            query_duration = self.memgraph.last_query_time
            if query_duration > 0:
                self.query_timings[query_key].append(query_duration)
        
        # Refresh index after all initial upsert operations complete
        self.opensearch.refresh_index(index_name)
        
        # Execute update queries
        update_queries = index_config.get('update_queries', [])
        for update_query_config in update_queries:
            self._process_update_query(index_name, id_field, update_query_config)
        
        # Refresh index after all update queries complete
        if update_queries:
            self.opensearch.refresh_index(index_name)
        
        return total_documents
    
    def load_about_page(self, index_name: str, mapping: Dict[str, Any], file_name: str) -> int:
        """Load about page content from YAML file.
        
        Args:
            index_name: Name of the index
            mapping: Index mapping configuration
            file_name: Path to about file YAML
            
        Returns:
            Number of pages loaded
        """
        logger.info('Indexing content from about page')
        if not os.path.isfile(file_name):
            raise ValueError(f'"{file_name}" is not a file!')
        
        # Delete and recreate index with mapping
        if self.config.get_clear_existing_indices():
            logger.info(f"Deleting index (if exists): {index_name}")
            self.opensearch.delete_index(index_name)
        
        if self.config.get_allow_index_creation():
            logger.info(f"Creating index (if not exists): {index_name}")
            self.opensearch.create_index(index_name, mapping=mapping)
        
        # Load and index pages
        page_count = 0
        with open(file_name) as file_obj:
            about_file = yaml.safe_load(file_obj)
            if not about_file:
                logger.warning(f"About file {file_name} is empty")
                return 0
            
            for page in about_file:
                page_num = page.get('page')
                if page_num is None:
                    logger.warning(f"Skipping page without 'page' field: {page}")
                    continue
                
                logger.info(f'Indexing about page "{page_num}"')
                doc_id = f'page{page_num}'
                self.opensearch.upsert_document(index_name, doc_id, page)
                page_count += 1
        
        # Refresh index
        self.opensearch.refresh_index(index_name)
        
        return page_count
    
    def read_model(self, model_files: List[str]):
        """Read and initialize model from files.
        
        Args:
            model_files: List of model YAML file paths
        """
        from .schema import Schema
        from .props import Props
        
        # Validate model files exist
        for file_name in model_files:
            if not os.path.isfile(file_name):
                raise ValueError(f'"{file_name}" is not a file!')
        
        # Initialize model with default type mapping
        props = Props()
        self.model = Schema(model_files, props)
        logger.info("Model loaded successfully")
    
    def get_model_data(self, subtype: str):
        """Generate model data documents for indexing.
        
        Args:
            subtype: Type of model data ('node', 'property', or 'value')
            
        Yields:
            Dictionary documents for indexing
        """
        from .schema import PROPERTIES, PROP_TYPE, DESCRIPTION, REQUIRED, ENUM, PROP_ENUM
        
        if not self.model:
            return
        
        nodes = self.model.nodes
        for node_name, obj in nodes.items():
            # Safely get properties, defaulting to empty dict if PROPERTIES key doesn't exist
            props = obj.get(PROPERTIES, {})
            
            if subtype == 'node':
                yield {
                    'id': node_name,
                    'type': 'node',
                    'node': node_name,
                    'node_name': node_name,
                    'node_kw': node_name
                }
            else:
                for prop_name, prop in props.items():
                    # Skip relationship based properties
                    if "@relation" in prop.get(PROP_TYPE, ''):
                        continue
                    
                    if subtype == 'property':
                        yield {
                            'id': f"{node_name}_{prop_name}",
                            'type': 'property',
                            'node': node_name,
                            'node_name': node_name,
                            'property': prop_name,
                            'property_name': prop_name,
                            'property_kw': prop_name,
                            'property_description': prop.get(DESCRIPTION, ''),
                            'property_required': prop.get(REQUIRED, False),
                            'property_type': PROP_ENUM if ENUM in prop else prop.get(PROP_TYPE, 'String')
                        }
                    elif subtype == 'value' and ENUM in prop:
                        for value in prop[ENUM]:
                            yield {
                                'id': f"{node_name}_{prop_name}_{value}",
                                'type': 'value',
                                'node': node_name,
                                'node_name': node_name,
                                'property': prop_name,
                                'property_name': prop_name,
                                'property_description': prop.get(DESCRIPTION, ''),
                                'property_required': prop.get(REQUIRED, False),
                                'property_type': PROP_ENUM,
                                'value': value,
                                'value_kw': value
                            }
    
    def load_model(self, index_name: str, mapping: Dict[str, Any], subtype: str) -> int:
        """Load model data into index.
        
        Args:
            index_name: Name of the index
            mapping: Index mapping configuration
            subtype: Type of model data ('node', 'property', or 'value')
            
        Returns:
            Number of documents loaded
        """
        logger.info(f'Indexing data model (subtype: {subtype})')
        if not self.model:
            logger.warning(f'Data model is not loaded, {index_name} will not be loaded!')
            return 0
        
        # Delete and recreate index with mapping
        if self.config.get_clear_existing_indices():
            logger.info(f"Deleting index (if exists): {index_name}")
            self.opensearch.delete_index(index_name)
        
        if self.config.get_allow_index_creation():
            logger.info(f"Creating index (if not exists): {index_name}")
            self.opensearch.create_index(index_name, mapping=mapping)
        
        # Collect all documents for bulk loading
        documents = list(self.get_model_data(subtype))
        
        if documents:
            logger.info(f"Loading {len(documents)} model documents into {index_name}")
            self.opensearch.bulk_upsert(index_name, documents, id_field='id', query_name=f"Model-{subtype}")
            logger.info(f"Completed loading model data into {index_name}")
        else:
            logger.warning(f"No model data generated for subtype {subtype}")
        
        # Refresh index
        self.opensearch.refresh_index(index_name)
        
        return len(documents)
    
    def _process_update_query(self, index_name: str, id_field: str,
                             update_query_config: Dict[str, Any]):
        """Process an update query.
        
        Args:
            index_name: Name of the OpenSearch index
            id_field: Field name to use as document ID
            update_query_config: Update query configuration
        """
        query_name = update_query_config.get('name')
        query = update_query_config.get('query')
        if not query:
            query_display = query_name if query_name else 'unnamed'
            logger.warning(f"Update query '{query_display}' missing query, skipping")
            return
        
        # Warn if query name is missing
        if not query_name:
            logger.warning(f"{index_name}: Update query missing name, proceeding without query name prefix")
            query_name = "unnamed"
        
        variables = update_query_config.get('variables', {})
        page_size = update_query_config.get('page_size', 10000)  # Default to 10000
        
        if query_name:
            logger.info(f"{index_name}:{query_name}: Starting update query")
        else:
            logger.info(f"{index_name}: Starting update query")
        
        # Track query execution time (will be set after generator is exhausted)
        query_key = f"{index_name}:{query_name}"
        
        total_updates = 0
        page_num = 0
        
        try:
            # Process pages incrementally
            # Note: Query execution time is tracked inside execute_paginated_query
            # and will be available in self.memgraph.last_query_time after the loop
            for page_updates in self.memgraph.execute_paginated_query(
                query, parameters=variables, page_size=page_size,
                index_name=index_name, query_name=query_name
            ):
                page_num += 1
                
                if not page_updates:
                    continue
                
                # Process this page with retry logic
                try:
                    if query_name:
                        logger.info(f"{index_name}:{query_name}: Loading page {page_num} to OpenSearch: {len(page_updates)} updates")
                    else:
                        logger.info(f"{index_name}: Loading page {page_num} to OpenSearch: {len(page_updates)} updates")
                    self.opensearch.bulk_update(index_name, page_updates, id_field, query_name=query_name)
                    total_updates += len(page_updates)
                    if query_name:
                        logger.info(f"{index_name}:{query_name}: Completed loading page {page_num} to OpenSearch: {len(page_updates)} updates (total: {total_updates})")
                    else:
                        logger.info(f"{index_name}: Completed loading page {page_num} to OpenSearch: {len(page_updates)} updates (total: {total_updates})")
                except Exception as e:
                    # Retry once
                    query_display = query_name if query_name else 'unnamed'
                    logger.warning(f"Error processing page {page_num} for update query '{query_display}' in {index_name}, retrying: {e}")
                    try:
                        if query_name:
                            logger.info(f"{index_name}:{query_name}: Retrying loading page {page_num} to OpenSearch: {len(page_updates)} updates")
                        else:
                            logger.info(f"{index_name}: Retrying loading page {page_num} to OpenSearch: {len(page_updates)} updates")
                        self.opensearch.bulk_update(index_name, page_updates, id_field, query_name=query_name)
                        total_updates += len(page_updates)
                        if query_name:
                            logger.info(f"{index_name}:{query_name}: Retry successful for page {page_num}: {len(page_updates)} updates (total: {total_updates})")
                        else:
                            logger.info(f"{index_name}: Retry successful for page {page_num}: {len(page_updates)} updates (total: {total_updates})")
                    except Exception as retry_error:
                        query_display = query_name if query_name else 'unnamed'
                        logger.error(f"Retry failed for page {page_num} for update query '{query_display}' in {index_name}: {retry_error}. Skipping entire index.")
                        raise
            
            if total_updates == 0:
                if query_name:
                    logger.info(f"{index_name}:{query_name}: No updates returned from query")
                else:
                    logger.info(f"{index_name}: No updates returned from query")
            else:
                if query_name:
                    logger.info(f"{index_name}:{query_name}: Completed update query: {total_updates} total updates processed")
                else:
                    logger.info(f"{index_name}: Completed update query: {total_updates} total updates processed")
        except ValueError as e:
            query_display = query_name if query_name else 'unnamed'
            logger.error(f"Error executing update query '{query_display}' for {index_name}: {e}. Continuing to next query.")
            raise
        finally:
            # Record query execution time (only Memgraph query time, not OpenSearch operations)
            # This is set after the generator is exhausted
            query_duration = self.memgraph.last_query_time
            if query_duration > 0:
                self.query_timings[query_key].append(query_duration)
    
    def _print_summary(self, total_time: float):
        """Print and save loading summary.
        
        Args:
            total_time: Total execution time in seconds
        """
        # Create logs directory if it doesn't exist
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H:%M")
        summary_filename = logs_dir / f"{timestamp}.loading-summary"
        
        # Build summary text
        lines = []
        lines.append("=" * 80)
        lines.append("Index Loading Summary")
        lines.append("=" * 80)
        lines.append("")
        lines.append(f"{'Index Name':<40} {'Documents':<15} {'Time':<15}")
        lines.append("-" * 80)
        
        for stat in self.index_stats:
            index_name = stat['index_name']
            doc_count = stat['document_count']
            duration = stat['duration']
            error = stat['error']
            
            # Format document count
            if error:
                doc_str = "ERROR"
            else:
                doc_str = str(doc_count)
            
            # Format time
            time_str = self._format_time(duration)
            
            lines.append(f"{index_name:<40} {doc_str:<15} {time_str:<15}")
        
        lines.append("-" * 80)
        lines.append(f"{'Total':<40} {'':<15} {self._format_time(total_time):<15}")
        lines.append("=" * 80)
        
        summary_text = "\n".join(lines)
        
        # Print to console
        logger.info("\n" + summary_text)
        
        # Save to file
        try:
            with open(summary_filename, 'w') as f:
                f.write(summary_text)
            logger.info(f"Summary saved to {summary_filename}")
        except Exception as e:
            logger.error(f"Failed to save summary to file: {e}")
    
    def _save_query_timings(self):
        """Save query execution timing data to file."""
        if not self.query_timings:
            return
        
        # Create logs directory if it doesn't exist
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H:%M")
        timing_filename = logs_dir / f"{timestamp}.query-timing"
        
        # Build timing text
        lines = []
        lines.append("Query Execution Times (Average)")
        lines.append("=" * 80)
        lines.append("")
        lines.append(f"{'Query':<60} {'Avg Time (s)':<20}")
        lines.append("-" * 80)
        
        # Sort by query key for consistent output
        for query_key in sorted(self.query_timings.keys()):
            timings = self.query_timings[query_key]
            if timings:
                avg_time = sum(timings) / len(timings)
                lines.append(f"{query_key:<60} {avg_time:.4f}")
        
        lines.append("=" * 80)
        
        timing_text = "\n".join(lines)
        
        # Save to file (do not print to console)
        try:
            with open(timing_filename, 'w') as f:
                f.write(timing_text)
            logger.debug(f"Query timings saved to {timing_filename}")
        except Exception as e:
            logger.error(f"Failed to save query timings to file: {e}")

