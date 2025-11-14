"""Main loader orchestration logic."""

import logging
import time
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
            Number of documents loaded from initial query
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
        
        # Execute update queries
        update_queries = index_config.get('update_queries', [])
        for update_query_config in update_queries:
            self._process_update_query(index_name, id_field, update_query_config)
        
        return total_documents
    
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

