# Local Kafka writer experiment

The existing Loader executes the queries. A writer adapter publishes JSON tasks
into a two-partition Kafka topic. Two separate Python consumers write to the
existing local OpenSearch cluster (two data nodes). OpenSearch, not the consumer,
selects the shard/node for each document.

`transport.py` implements the adapter and consumer. `benchmark.py` exercises all
21 index handlers, with 5% of each Cypher source-root label selected deterministically.
About/model indexes are loaded in full. Updates targeting documents outside the
initial sample are excluded in both modes. This is not necessarily 5% of final
output documents, and it does not validate unsampled relationships.

- Same `(index, ID)` always uses the same partition and one serial consumer.
- Index creation remains synchronous in the loader.
- Initial-load refresh waits for all consumer acknowledgments before updates.
- Consumers acknowledge only after successful OpenSearch operations and Kafka
  offset commit. Bulk failures fail the test; producer buffering has backpressure.
- At most 10 data messages await acknowledgment. Each message contains at most
  5,000 documents / approximately 48 MiB serialized document content.
- Kafka delivery is at least once; this is a local experiment, not a production
  exactly-once implementation. Only deterministic index/partial-doc operations
  are used. A crash requires an explicit clean experiment restart.
- Test indices have unique `bench_kafka_` names and are deleted after validation.
- Final document hashes (array order normalized) are compared against direct writes.

Run from the repository root, with the benchmark venv and local services running:

```sh
PYTHONPATH=. /tmp/os-consumer-bench-env/bin/python -u experiments/kafka/benchmark.py
```

Endpoints: Memgraph `127.0.0.1:17688`, OpenSearch `127.0.0.1:19204`,
Kafka `127.0.0.1:19092`. Python dependency: `confluent-kafka==2.6.1`.
The runner also uses cached general-commons 4.9.0 and model 13.0.1 files under
`/tmp/opensearch-indices-4.9.0-review.yaml` and `tmp/all-index-worker-validation/`.

Direct control uses the existing two-thread ParallelWrites implementation and
its current HTTP batch size. Kafka messages can produce smaller HTTP batches;
this is a comparison of these two implementations, not transport overhead alone.
Both modes run indexes sequentially and use the same queries and sample.
The broker should remain running for both rounds to keep resource overhead similar.

Results and consumer logs are in `tmp/kafka-five-percent/`. Index timings include
query and completed writes. Query timing sums are recorded separately; consumer
busy-time sums overlap loader work and must not be added to wall time.

No production loader, Prefect deployment, or AWS setting is modified.

## Four simultaneous index loaders

```sh
PYTHONPATH=. /tmp/os-consumer-bench-env/bin/python -u experiments/kafka/benchmark.py --mode kafka --index-workers 4
```

This mode uses four separate Loader/Memgraph connections, one shared bounded Kafka
transport, and exactly two consumer processes. Per-index refresh barriers wait
only for that index's outstanding messages. At most four index tasks are active.
It records completed writes/counts and normalized document content digests for comparison between runs.
Results are under `tmp/kafka-five-percent/parallel-<timestamp>/`.

The benchmark restarts the local test Memgraph once before preparation. It does
not restart it between concurrent index tasks. For this fixed read-only dataset,
a single snapshot newly generated during that initial restart is removed by exact
filename after recovery and a file-node count check; pre-existing snapshots are
preserved. The older sequential experiment restarts between indexes to isolate
memory growth, so its timings are not a fair concurrency baseline.

The Kafka broker is currently hosted on the Mac JVM (Java 11, Kafka 3.9.1), using
`/tmp/os-loader-kafka-host.properties` and `/tmp/os-loader-kafka-host-data`, to avoid
adding broker memory pressure to the 6 GB Colima VM. Existing unrelated containers
are not stopped. This local resource constraint may prevent full completion of
four concurrent graph queries; failures must not be presented as speed results.


## Process-isolated queries (2026-11-27 investigation)

The local `pymgclient` build blocked a heartbeat thread during a 29.30-second
query execution. `process_queries.py` moves the existing paginated query calls
into a bounded spawn-based process pool. Index orchestration remains four
threads; the number of active database queries can be limited independently.
The original query text and write semantics are retained apart from sampling.

```sh
PYTHONPATH=. /tmp/os-consumer-bench-env/bin/python -u experiments/kafka/benchmark.py \
  --mode kafka --index-workers 4 --memgraph-port 17689 --skip-memgraph-restart \
  --sample-percent 5 --query-processes --query-workers 2 --dashboard-first \
  --write-mode direct
```

Use `--write-mode kafka` for two independent Kafka consumers instead of the
shared two-thread direct writer. These are experimental CLI flags, not Prefect
parameters. Port 17689 is the SSH tunnel to the migrated Blink Mac Memgraph.
`dashboard-first` changes launch order; it does not reserve query capacity.

Per-index `seconds` ends after writes and count verification, before document
hashing. Overall `wall_seconds` includes content verification and test-index
cleanup. `query_seconds` includes process-pool queueing and IPC when process
isolation is enabled. It must not be described as pure database execution time.
Digest verification ignores array ordering, so it verifies content under that
normalization rather than exact array order. Sampling does not establish a
full-dataset runtime guarantee.

Memgraph also documents GIL-related thread limitations and concurrent processes: https://memgraph.com/docs/client-libraries/python#concurrent-transactions . The local heartbeat probe supplies the workload-specific evidence; the documentation alone does not establish a speedup.
