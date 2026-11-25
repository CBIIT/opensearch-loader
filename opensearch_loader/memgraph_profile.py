"""Read-only profiling of selected loader queries: one page each, timed, with Memgraph's PROFILE plan."""

import subprocess
import time

import mgclient
import yaml

from bento.common.secret_manager import get_secret
from prefect import flow

DEFAULT_TARGETS = [
    "dashboard_participant|File Diagnoses",
    "dashboard_study_data_type|Studies",
    "dashboard_file|Diagnoses",
    "dashboard_file|caNanoLab Protocol",
    "gs_file|Initial Query",
]
CANANOLAB_COUNT = ("MATCH (f:file)--(x) WHERE x:Protocol OR x:Publication OR x:Composition OR x:Characterization "
                   "RETURN count(DISTINCT f) AS files_with_cananolab_links")


def _query(spec, index_name, name):
    for index in spec["indices"]:
        if index.get("index_name") == index_name:
            if name == "Initial Query":
                return index["initial_query"]
            for update in index.get("update_queries", []):
                if update.get("name") == name:
                    return update
    raise ValueError(f"{index_name}|{name} not found in indices.yaml")


def _run(conn, query, params=None):
    cursor = conn.cursor()
    started = time.monotonic()
    cursor.execute(query, params or {})
    rows = cursor.fetchall()
    return [c.name for c in cursor.description or []], rows, time.monotonic() - started


@flow(name="Memgraph Profile", log_prints=True)
def memgraph_profile(secret_name: str = "gen/amit-opensearch", monorepo_branch: str = "4.9.0",
                     targets: list[str] = DEFAULT_TARGETS):
    """Time one page of each target query, show its PROFILE plan, and count files with caNanoLab links."""
    subprocess.run(["git", "clone", "-q", "--depth", "1", "--branch", monorepo_branch,
                    "https://github.com/CBIIT/general-commons.git", "gc"], check=True)
    with open("gc/backend/src/main/resources/yaml/indices.yaml") as f:
        spec = yaml.safe_load(f)

    secret = get_secret(secret_name)
    conn = mgclient.connect(host=secret["memgraph_endpoint"], port=7687,
                            username=secret["memgraph_user"], password=secret["memgraph_password"])
    conn.autocommit = True
    print(f"memgraph={secret['memgraph_endpoint']} indices.yaml@{monorepo_branch}")

    for target in targets:
        index_name, name = target.split("|", 1)
        q = _query(spec, index_name, name)
        params = {**q.get("variables", {}), "skip": 0, "limit": q.get("page_size", 10000)}
        _, rows, seconds = _run(conn, q["query"], params)
        print(f"\n== {target}: page_size={params['limit']} first page rows={len(rows)} seconds={seconds:.1f}")
        columns, plan, _ = _run(conn, "PROFILE " + q["query"], params)
        for row in plan:
            entry = dict(zip(columns, row, strict=True))
            print("  " + " | ".join(f"{k}={v}" for k, v in entry.items()))

    _, rows, seconds = _run(conn, CANANOLAB_COUNT)
    _, total, _ = _run(conn, "MATCH (f:file) RETURN count(f)")
    print(f"\nfiles with caNanoLab links: {rows[0][0]} of {total[0][0]} ({seconds:.1f}s)")
    conn.close()
