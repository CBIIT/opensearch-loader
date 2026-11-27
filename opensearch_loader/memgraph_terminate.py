"""Terminate one running Memgraph transaction by ID, after checking its query text."""

import time

import mgclient

from bento.common.secret_manager import get_secret
from prefect import flow


def _transactions(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW TRANSACTIONS;")
    columns = [c.name for c in cursor.description or []]
    return {str(row["transaction_id"]): row for row in (dict(zip(columns, r, strict=True)) for r in cursor.fetchall())}


@flow(name="Memgraph Terminate Transaction", log_prints=True)
def memgraph_terminate(transaction_id: str, expected_text: str, secret_name: str = "gen/amit-opensearch"):
    """Terminate the transaction only if it is running and its query contains expected_text."""
    secret = get_secret(secret_name)
    conn = mgclient.connect(host=secret["memgraph_endpoint"], port=7687,
                            username=secret["memgraph_user"], password=secret["memgraph_password"])
    conn.autocommit = True
    running = _transactions(conn)
    target = running.get(transaction_id)
    if target is None:
        print(f"transaction {transaction_id} is not running; nothing to do")
        return
    text = target.get("query", "")
    query = " ".join((" ".join(map(str, text)) if isinstance(text, list) else str(text)).split())
    if " ".join(expected_text.split()) not in query:
        raise ValueError(f"transaction {transaction_id} query does not contain the expected text: {query[:200]}")
    cursor = conn.cursor()
    cursor.execute(f'TERMINATE TRANSACTIONS "{transaction_id}";')
    print(f"terminate result: {cursor.fetchall()}")
    time.sleep(5)
    print(f"still running after 5s: {transaction_id in _transactions(conn)}")
    conn.close()
