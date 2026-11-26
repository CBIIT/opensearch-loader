"""Read-only test: full pass of a file query with SKIP pagination vs keyset pagination, timed and compared."""

import json
import subprocess
import time

import mgclient
import yaml

from bento.common.secret_manager import get_secret
from prefect import flow

FILE_SCAN = "MATCH (f:file)\n"
PAGE = "SKIP $skip LIMIT $limit"


def _fetch(conn, query, params):
    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [c.name for c in cursor.description or []]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _skip_pass(conn, query, size):
    rows, skip = [], 0
    while True:
        page = _fetch(conn, query, {"skip": skip, "limit": size})
        rows += page
        if len(page) < size:
            return rows
        skip += size


def _keyset_pass(conn, query, size):
    rows, last = [], ""
    while True:
        page = _fetch(conn, query, {"last": last, "limit": size})
        rows += page
        if len(page) < size:
            return rows
        last = max(r["file_id"] for r in page)


def _keyed(rows):
    out = {}
    for r in rows:
        out.setdefault(r["file_id"], []).append(json.dumps(r, sort_keys=True, default=str))
    return {k: sorted(v) for k, v in out.items()}


@flow(name="Memgraph Keyset Test", log_prints=True)
def memgraph_keyset_test(secret_name: str = "gen/amit-opensearch", monorepo_branch: str = "4.9.0",
                         index_name: str = "dashboard_file",
                         queries: list[str] = ["Diagnoses", "caNanoLab Protocol"]):  # noqa: B006
    """Time SKIP, keyset and ordered keyset versions of each query and check they return the same rows."""
    subprocess.run(["git", "clone", "-q", "--depth", "1", "--branch", monorepo_branch,
                    "https://github.com/CBIIT/general-commons.git", "gc"], check=True)
    with open("gc/backend/src/main/resources/yaml/indices.yaml") as f:
        spec = yaml.safe_load(f)
    index = next(i for i in spec["indices"] if i.get("index_name") == index_name)
    secret = get_secret(secret_name)
    conn = mgclient.connect(host=secret["memgraph_endpoint"], port=7687,
                            username=secret["memgraph_user"], password=secret["memgraph_password"])
    conn.autocommit = True

    for name in queries:
        q = next(u for u in index["update_queries"] if u["name"] == name)
        base, size = q["query"], q.get("page_size", 10000)
        assert base.startswith(FILE_SCAN) and base.count(PAGE) == 1, f"{name}: unexpected query shape"
        keyset = base.replace(FILE_SCAN, "MATCH (f:file) WHERE f.file_id > $last\n", 1).replace(PAGE, "LIMIT $limit")
        ordered = base.replace(FILE_SCAN, "MATCH (f:file) WHERE f.file_id > $last\n", 1) \
                      .replace(PAGE, "ORDER BY f.file_id LIMIT $limit")
        results = {}
        for label, runner, text in (("skip", _skip_pass, base), ("keyset", _keyset_pass, keyset),
                                    ("keyset+order", _keyset_pass, ordered)):
            started = time.monotonic()
            rows = runner(conn, text, size)
            seconds = time.monotonic() - started
            results[label] = _keyed(rows)
            ids = [r["file_id"] for r in rows]
            print(f"{name} [{label}]: {seconds:.1f}s rows={len(rows)} distinct_files={len(set(ids))}")
        for label in ("keyset", "keyset+order"):
            same = results[label] == results["skip"]
            missing = len(set(results["skip"]) - set(results[label]))
            print(f"{name} [{label}] identical to skip: {same} (files missing vs skip: {missing})")
    conn.close()
