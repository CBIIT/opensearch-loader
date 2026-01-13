"""Memgraph client with read-only query validation."""

import logging
import re
import time
from typing import List, Dict, Any, Optional, Iterator
from neo4j import GraphDatabase

logger = logging.getLogger("OpenSearchLoader")

# Cypher keywords that indicate write operations
WRITE_KEYWORDS = {
    'CREATE', 'SET', 'DELETE', 'REMOVE', 'MERGE', 'DETACH', 'DROP',
    'FOREACH'
}


class MemgraphClient:
    """Client for executing read-only queries against Memgraph."""
    
    def __init__(self, host: str = "localhost", port: int = 7687,
                 username: Optional[str] = None, password: Optional[str] = None):
        """Initialize Memgraph client.
        
        Args:
            host: Memgraph host
            port: Memgraph port
            username: Optional username
            password: Optional password
        """
        uri = f"bolt://{host}:{port}"
        auth = (username, password) if username and password else None
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.last_query_time = 0.0  # Track total query execution time for last paginated query
        logger.info(f"Connected to Memgraph at {uri}")
    
    def close(self):
        """Close the database connection."""
        if self.driver:
            self.driver.close()
    
    def validate_read_only(self, query: str) -> bool:
        """Validate that a query is read-only.
        
        Args:
            query: Cypher query string
            
        Returns:
            True if query appears to be read-only, False otherwise
            
        Raises:
            ValueError: If query contains write operations
        """
        query_upper = query.upper()
        
        # Check for write keywords
        for keyword in WRITE_KEYWORDS:
            # Use word boundaries to avoid false positives
            pattern = f"\\b{keyword}\\b"
            if re.search(pattern, query_upper):
                # Check if it's in a safe context (e.g., in a comment or string)
                # For now, we'll be strict and reject any occurrence
                raise ValueError(
                    f"Query contains write operation '{keyword}'. "
                    "Only read-only queries (MATCH, RETURN, WHERE, etc.) are allowed."
                )
        
        # Ensure query contains at least MATCH or RETURN
        if 'MATCH' not in query_upper and 'RETURN' not in query_upper:
            raise ValueError("Query must contain MATCH or RETURN clause")
        
        return True
    
    def validate_pagination_params(self, query: str) -> bool:
        """Validate that a query contains $skip and $limit parameters.
        
        Args:
            query: Cypher query string
            
        Returns:
            True if query contains both $skip and $limit parameters
            
        Raises:
            ValueError: If query is missing $skip or $limit parameters
        """
        has_skip = '$skip' in query or '$SKIP' in query
        has_limit = '$limit' in query or '$LIMIT' in query
        
        if not has_skip:
            raise ValueError("Query must contain $skip parameter")
        
        if not has_limit:
            raise ValueError("Query must contain $limit parameter")
        
        return True
    
    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a read-only Cypher query.
        
        Args:
            query: Cypher query string (must not be modified, must contain $skip and $limit)
            parameters: Optional query parameters
            
        Returns:
            List of result dictionaries
            
        Raises:
            ValueError: If query is missing $skip or $limit parameters
        """
        # Validate query is read-only
        self.validate_read_only(query)
        
        # Validate pagination parameters are present
        self.validate_pagination_params(query)
        
        parameters = parameters or {}
        
        results = []
        with self.driver.session() as session:
            result = session.run(query, parameters)
            for record in result:
                # Convert record to dictionary
                results.append(dict(record))
        
        logger.debug(f"Executed query, returned {len(results)} results")
        return results
    
    def execute_paginated_query(self, query: str, parameters: Optional[Dict[str, Any]] = None,
                               page_size: int = 10000, index_name: Optional[str] = None,
                               query_name: Optional[str] = None,
                               test_mode: bool = False) -> Iterator[List[Dict[str, Any]]]:
        """Execute a query with pagination, yielding pages one at a time.
        
        Also tracks total Memgraph query execution time (excluding OpenSearch operations)
        in self.last_query_time after the generator is exhausted.
        
        Args:
            query: Cypher query string (must contain $skip and $limit parameters)
            parameters: Optional query parameters (will be merged with pagination params)
            page_size: Number of results per page (default: 10000)
            index_name: Optional index name for logging prefix
            query_name: Optional query name for logging prefix
            test_mode: If True, only process the first page to validate the query
            
        Yields:
            List of result dictionaries for each page
        """
        offset = 0
        total_results = 0
        total_query_time = 0.0  # Track total Memgraph query execution time
        
        # Reset last_query_time at the start to avoid stale values
        self.last_query_time = 0.0
        
        while True:
            # Merge pagination parameters with existing parameters
            pagination_params = {'skip': offset, 'limit': page_size}
            merged_params = {**(parameters or {}), **pagination_params}
            
            # Track time for this query execution only (not OpenSearch operations)
            query_start = time.time()
            page_results = self.execute_query(query, merged_params)
            query_end = time.time()
            total_query_time += (query_end - query_start)
            
            if not page_results:
                break
            
            total_results += len(page_results)
            if index_name and query_name:
                logger.info(f"{index_name}:{query_name}: Memgraph query returned {len(page_results)} records")
            elif index_name:
                logger.info(f"{index_name}: Memgraph query returned {len(page_results)} records")
            else:
                logger.info(f"Memgraph query returned {len(page_results)} records")
            yield page_results
            
            # In test mode, only process the first page to validate the query
            if test_mode:
                if index_name and query_name:
                    logger.info(f"{index_name}:{query_name}: Test mode - stopping after first page")
                elif index_name:
                    logger.info(f"{index_name}: Test mode - stopping after first page")
                else:
                    logger.info("Test mode - stopping after first page")
                break
            
            # If we got fewer results than page_size, we're done
            if len(page_results) < page_size:
                break
            
            offset += page_size
        
        # Store the total query time for this paginated query
        # This will be available after the generator is exhausted
        self.last_query_time = total_query_time
        logger.debug(f"Executed paginated query, yielded {total_results} total results across pages")

