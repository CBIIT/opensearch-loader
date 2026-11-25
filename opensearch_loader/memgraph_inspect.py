"""Read-only look inside Memgraph: running transactions, indexes, storage and thread settings."""

import time

import mgclient
from bento.common.secret_manager import get_secret
from prefect import flow

CONFIG_KEYS = ("bolt_num_workers", "storage_mode", "isolation", "query_execution_timeout", "storage_parallel",
               "gc_", "snapshot", "memory_limit")


def _rows(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [c.name for c in cursor.description or []]
    return columns, cursor.fetchall()


@flow(name="Memgraph Inspect", log_prints=True)
def memgraph_inspect(secret_name: str = "gen/amit-opensearch", samples: int = 6, interval_seconds: int = 15):
    """Sample running transactions a few times, then print indexes, storage info and key settings."""
    secret = get_secret(secret_name)
    conn = mgclient.connect(host=secret["memgraph_endpoint"], port=7687,
                            username=secret["memgraph_user"], password=secret["memgraph_password"])
    conn.autocommit = True
    print(f"memgraph={secret['memgraph_endpoint']}")

    for sample in range(1, samples + 1):
        columns, rows = _rows(conn, "SHOW TRANSACTIONS;")
        queries = [dict(zip(columns, row, strict=True)) for row in rows]
        print(f"sample {sample}: {len(queries)} transactions")
        for q in queries:
            text = " ".join(str(q.get("query", "")).split())[:160]
            print(f"  user={q.get('username')} id={q.get('transaction_id')} query={text}")
        if sample < samples:
            time.sleep(interval_seconds)

    for query in ("SHOW INDEX INFO;", "SHOW STORAGE INFO;"):
        columns, rows = _rows(conn, query)
        print(query)
        for row in rows:
            print("  " + ", ".join(f"{c}={v}" for c, v in zip(columns, row, strict=True)))

    columns, rows = _rows(conn, "SHOW CONFIG;")
    print("SHOW CONFIG (selected):")
    for row in rows:
        entry = dict(zip(columns, row, strict=True))
        if any(key in str(entry.get("name", "")) for key in CONFIG_KEYS):
            print(f"  {entry.get('name')}={entry.get('current_value')} (default {entry.get('default_value')})")
    conn.close()
