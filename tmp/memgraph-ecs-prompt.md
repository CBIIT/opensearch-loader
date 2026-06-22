We have a Python application that loads data from Memgraph (graph database) into OpenSearch. Memgraph is running in **AWS ECS**, exposed via a **Network Load Balancer (NLB)**. We're seeing Bolt connections dropped mid-run with:

> `Failed to read from defunct connection ... OSError('No data')`

We believe the cause is `--bolt-session-inactivity-timeout` being set too low in the Memgraph ECS task definition.

**Question:** How do I update the Memgraph ECS task definition to set `--bolt-session-inactivity-timeout=0` (or a high value like 3600)?

Specifically:
1. Where in the ECS Task Definition JSON does this go — as a container command, entrypoint, or environment variable?
2. How do I find the correct parameter name — does Memgraph support env vars for this, or must it be a CLI flag?
3. What's the safest way to roll out the new task definition revision with minimal downtime?

The Memgraph Docker image is the official `memgraph/memgraph` image. The ECS service is in AWS us-east-1.
