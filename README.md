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
max_nesting_depth: 5  # Max dot-notation levels in indices.yaml mapping paths
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

### Prefect DCC Shared Pool

The `gc-data-integrated-dashboard-new-opensearch-loader` deployment in
[prefect/gc_prefect_opensearch_loader.yaml](prefect/gc_prefect_opensearch_loader.yaml)
uses `ccdi-dcc-16gb-prefect-3.4.19-python3.13`, replacing the C3DC pool running
Prefect 3.2.14 and Python 3.9. The flow entrypoint and source branch remain
unchanged. `environment: dev_cdk` selects the Prefect variable
`gc_memgraph_secret_name_dev_cdk_id`, whose value names the Secrets Manager
secret containing Memgraph connection details and the OpenSearch host.

The shared worker's AWS identity may differ from the old worker's identity.
For AWS-hosted OpenSearch, the deployment explicitly sets `aws_operations_role`
to `arn:aws:iam::624882275043:role/power-user-prefect-operations`. The flow passes
this role through its configuration to the OpenSearch client, which calls STS
`AssumeRole` and signs requests with temporary credentials that refresh before
expiration. Role selection is: nonempty flow parameter, then the Prefect variable
`aws_operations_role`, then the worker's default AWS credentials. A blank flow
parameter does not bypass a configured Prefect variable.

Role assumption applies only to OpenSearch requests, not Secrets Manager calls.
Before deploying, confirm:

- The shared worker can call `sts:AssumeRole` on the operations role, and the
  operations role's trust policy permits that worker identity.
- The assumed role is authorized by the OpenSearch domain policy and, when
  enabled, fine-grained access control for the loader's operations.
- The shared worker can read the environment's Secrets Manager secret and
  decrypt it when a customer-managed KMS key requires that permission.
- The pool has network access to STS, Secrets Manager, Memgraph, OpenSearch,
  GitHub, and the package source used by the pull-time requirements install.
- OpenSearch is in `us-east-1`, the region currently used by the request signer.

After pushing the source changes, update the Prefect deployment separately:

```bash
prefect deploy --prefect-file prefect/gc_prefect_opensearch_loader.yaml \
  --name gc-data-integrated-dashboard-new-opensearch-loader
```

Verify the deployment's pool and parameters in Prefect, then run a controlled
load against the intended environment to verify live IAM and connectivity.
`selected_indices: []` selects all indices, so choose a limited index when
validating. A Git push alone does not update the registered deployment.
Local tests mock AWS and cannot certify the pool image or live infrastructure.

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
export OS_LOADER_MAX_NESTING_DEPTH=5
```

### Configuration Precedence

Configuration values are resolved in the following order (highest to lowest precedence):

1. **Command-line arguments** (highest)
2. **Environment variables**
3. **YAML configuration file** (lowest)

## How It Works

To select update queries per index in `config.yaml`:

```yaml
selected_indices:
  - participants_table:
      - sample_diagnosis_genetic_analysis_file_filters_direct_samples
  - samples_table
```

An index followed by a colon and a nonempty query list runs only those updates
against its existing documents. A plain index name without a colon, such as
`samples_table` above, runs its normal full load, including the initial query and
all update queries. Query names are validated against their own index before
any indices are processed. Empty nested lists and unknown index/query names are
rejected.

The Prefect flow's `selected_indices` parameter accepts the same list, including
mixed plain index names and per-index query lists. In the Prefect run form's JSON
editor, enter:

```json
[
  {"participants_table": ["sample_diagnosis_genetic_analysis_file_filters_direct_samples"]},
  "samples_table"
]
```

You can also use the YAML structure above under `deployments[].parameters` in
`prefect/gc_prefect_opensearch_loader.yaml`. Prefect builds its configuration from
flow parameters rather than reading the local `config.yaml`. After deploying the
updated flow code, both paths use the same selection and validation logic.

The global query-selection format is also supported:

```bash
python -m opensearch_loader.cli --config config.yaml \
  --selected-indices cohorts --selected-update-queries diagnoses,exposures
```

Use the `name` values from `update_queries` in the index specification. Selection
also accepts `selected_update_queries: [diagnoses, exposures]` in configuration
YAML or `OS_LOADER_SELECTED_UPDATE_QUERIES=diagnoses,exposures`. Precedence is
CLI, then environment, then YAML. Combine with `selected_indices` to limit which
indices are considered; otherwise matching queries run across all query indices.
Queries run in their specification order. Unknown names fail validation before
processing indices; an empty selection is rejected. Omit the setting for a normal
full load.

Use either per-index nested query lists or the global `selected_update_queries`
setting; combining them is rejected. CLI and environment `selected_indices`
overrides replace the YAML selection, including its nested query lists.

When selecting update queries, the loader skips initial queries, index creation
and deletion (even with `clear_existing_indices` enabled), and model/about-file
indices. With per-index selection, this applies only to entries with nested query
lists; plain entries retain their full-load behavior. Target indices for selected
updates must already exist. Existing update/overwrite behavior
is unchanged by query selection. The summary reports zero initial documents
loaded in this mode; per-query logs report the updates processed.

1. **Initial Query**: For each index, executes the initial query to fetch base documents from Memgraph
2. **Document Creation**: Transforms query results to OpenSearch documents using direct field mapping and upserts them
3. **Update Queries**: Executes each update query sequentially, merging new values into existing documents
4. **Field Merging**: When updating, fields with new values are overwritten, while existing fields without new values are preserved

## Query Requirements

- All Memgraph queries must be **read-only** (only MATCH, RETURN, WHERE, etc.)
- Queries containing write operations (CREATE, SET, DELETE, etc.) will be rejected
- Each query must return a field matching the `id_field` specified in the index configuration
- Query results use direct field mapping (column names → OpenSearch document fields)
- For nested fields, Memgraph queries must return **nested dict structures** that match the mapping tree (dot notation is only used in `indices.yaml` mapping definitions, not in query result keys)

### Nested field mapping

In each index's `mapping` block, use dot notation to declare nested paths (up to `max_nesting_depth` levels, default 5). Any parent field that contains child properties is mapped as OpenSearch `nested`:

```yaml
mapping:
  keyword:
    - order_id
    - shipping.address.street  # 3-level path → shipping.address.street in OpenSearch
  text:
    - shipping.address.instructions
```

Documents from Memgraph must already be nested JSON, for example:

```json
{
  "order_id": "123",
  "shipping": {
    "address": {
      "street": "Main St",
      "instructions": "Leave at door"
    }
  }
}
```

Adjust the limit via `max_nesting_depth` in `config.yaml`, `OS_LOADER_MAX_NESTING_DEPTH`, or `--max-nesting-depth`.

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
