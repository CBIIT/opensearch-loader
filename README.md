# OpenSearch Loader

A Python utility that synchronizes data from Memgraph to OpenSearch using an initial query to populate documents and follow-up queries to update them.

## Features

- **Read-only query validation**: Ensures Memgraph queries are read-only for safety
- **Multiple indices support**: Define multiple OpenSearch indices in a single specification file
- **Pagination**: Support for paginated queries to handle large datasets
- **Query variables**: Pass parameters to Cypher queries
- **Flexible configuration**: YAML files, environment variables, and CLI arguments with precedence: CLI > env > YAML
- **Index management**: Option to delete and create indices automatically
- **Document merging**: Updates preserve existing fields while overwriting specified fields

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd opensearch-loader
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

### Main Configuration File

Create a `config.yaml` file (or use `config.example.yaml` as a template):

```yaml
memgraph:
  host: localhost
  port: 7687
  username: null
  password: null

opensearch:
  host: "http://localhost:9200"
  use_ssl: false
  verify_certs: false
  username: null
  password: null

index_spec_file: "indices.yaml"  # Path to index specification file
clear_existing_indices: false  # Clear all existing indices before starting (universal)
allow_index_creation: true  # Allow creation of indices if missing (universal)
```

### Index Specification File

Create an `indices.yaml` file (or use `indices.example.yaml` as a template):

```yaml
indices:
  - index_name: "users"
    id_field: "user_id"  # Field from query results to use as OpenSearch document _id
    initial_query:
      query: "MATCH (u:User) RETURN u.id as user_id, u.name as name, u.email as email, u.created_at as created_at"
      variables: {}  # Optional query parameters/variables
      page_size: 1000  # Optional pagination size
    update_queries:
      - name: "update_timestamps"
        query: "MATCH (u:User) WHERE u.updated_at > $last_update RETURN u.id as user_id, u.updated_at as updated_at"
        variables:
          last_update: "2024-01-01T00:00:00Z"  # Query variables/parameters
        page_size: 500
```

## Usage

### Command Line

Basic usage:
```bash
python -m opensearch_loader.cli --config config.yaml
```

With CLI arguments (highest precedence):
```bash
python -m opensearch_loader.cli \
  --config config.yaml \
  --memgraph-host localhost \
  --memgraph-port 7687 \
  --opensearch-host http://localhost:9200 \
  --index-spec-file indices.yaml \
  --clear-existing-indices \
  --verbose
```

### Environment Variables

All configuration options can be set via environment variables with the prefix `OS_LOADER_`:

```bash
export OS_LOADER_MEMGRAPH_HOST=localhost
export OS_LOADER_MEMGRAPH_PORT=7687
export OS_LOADER_OPENSEARCH_HOST=http://localhost:9200
export OS_LOADER_INDEX_SPEC_FILE=indices.yaml
export OS_LOADER_CLEAR_EXISTING_INDICES=false
export OS_LOADER_ALLOW_INDEX_CREATION=true
```

### Configuration Precedence

Configuration values are resolved in the following order (highest to lowest precedence):

1. **Command-line arguments** (highest)
2. **Environment variables**
3. **YAML configuration file** (lowest)

## How It Works

1. **Initial Query**: For each index, executes the initial query to fetch base documents from Memgraph
2. **Document Creation**: Transforms query results to OpenSearch documents using direct field mapping and upserts them
3. **Update Queries**: Executes each update query sequentially, merging new values into existing documents
4. **Field Merging**: When updating, fields with new values are overwritten, while existing fields without new values are preserved

## Query Requirements

- All Memgraph queries must be **read-only** (only MATCH, RETURN, WHERE, etc.)
- Queries containing write operations (CREATE, SET, DELETE, etc.) will be rejected
- Each query must return a field matching the `id_field` specified in the index configuration
- Query results use direct field mapping (column names → OpenSearch document fields)

## Examples

### Simple Index

```yaml
indices:
  - index_name: "products"
    id_field: "product_id"
    initial_query:
      query: "MATCH (p:Product) RETURN p.id as product_id, p.name as name, p.price as price"
      page_size: 1000
    update_queries:
      - name: "update_prices"
        query: "MATCH (p:Product) WHERE p.price_updated > $since RETURN p.id as product_id, p.price as price"
        variables:
          since: "2024-01-01T00:00:00Z"
        page_size: 500
```

### Query with Variables

```yaml
initial_query:
  query: "MATCH (u:User) WHERE u.created_at > $min_date RETURN u.id as user_id, u.name as name"
  variables:
    min_date: "2024-01-01T00:00:00Z"
  page_size: 1000
```

## Error Handling

The loader includes comprehensive error handling:

- Connection errors are logged with details
- Invalid queries are rejected before execution
- Missing configuration values raise clear error messages
- Failed document operations are logged as warnings

## Logging

Enable verbose logging with the `-v` or `--verbose` flag:

```bash
python -m opensearch_loader.cli --config config.yaml --verbose
```

## License

[Add your license here]

