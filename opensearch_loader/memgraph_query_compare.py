"""Read-only test: run a loader query and a rewritten version over every page, time both and compare the rows.

The rewrite is given as text replacements applied to the query from indices.yaml, so the
candidate differs from the original only where intended.
"""

import json
import subprocess
import time

import mgclient
import yaml

from bento.common.secret_manager import get_secret
from prefect import flow

# File Diagnoses: drop duplicate participants per file before looking up diagnoses, so each
# participant's diagnoses are read once instead of once per shared file. Output is meant to be unchanged.
FILE_DIAGNOSES_REWRITE = [[
    ('OPTIONAL MATCH (f:file)-->(p)\n'
     'WITH DISTINCT f, p.study_participant_id AS study_participant_id, "Not specified in data" AS nsd, '
     '"Not Applicable" AS na\n'
     'OPTIONAL MATCH (f)-->(p:participant)<--(diag:diagnosis)\n'),
    ('OPTIONAL MATCH (f:file)-->(p)\n'
     'WITH DISTINCT f, p.study_participant_id AS study_participant_id\n'
     'OPTIONAL MATCH (f)-->(p2:participant)\n'
     'WITH DISTINCT study_participant_id, p2, "Not specified in data" AS nsd, "Not Applicable" AS na\n'
     'OPTIONAL MATCH (p2)<--(diag:diagnosis)\n'),
]]


def _fetch(conn, query, params):
    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [c.name for c in cursor.description or []]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _full_pass(conn, query, variables, size, label):
    """Page through the query with SKIP/LIMIT like the loader does; return rows and seconds."""
    rows, skip, started = [], 0, time.monotonic()
    while True:
        t = time.monotonic()
        page = _fetch(conn, query, {**variables, "skip": skip, "limit": size})
        print(f"[{label}] page skip={skip} rows={len(page)} {time.monotonic() - t:.1f}s")
        rows += page
        if len(page) < size:
            return rows, time.monotonic() - started
        skip += size


def _multiset(rows):
    return sorted(json.dumps(r, sort_keys=True, default=str) for r in rows)


@flow(name="Memgraph Query Compare", log_prints=True)
def memgraph_query_compare(secret_name: str = "gen/amit-opensearch", monorepo_branch: str = "4.9.0",
                           index_name: str = "dashboard_participant", query_name: str = "File Diagnoses",
                           replacements: list[list[str]] = FILE_DIAGNOSES_REWRITE):
    """Time the original and rewritten query over all pages and report whether the rows are identical."""
    subprocess.run(["git", "clone", "-q", "--depth", "1", "--branch", monorepo_branch,
                    "https://github.com/CBIIT/general-commons.git", "gc"], check=True)
    with open("gc/backend/src/main/resources/yaml/indices.yaml") as f:
        spec = yaml.safe_load(f)
    index = next(i for i in spec["indices"] if i.get("index_name") == index_name)
    q = next(u for u in index["update_queries"] if u.get("name") == query_name)
    original, size, variables = q["query"], q.get("page_size", 10000), q.get("variables", {})
    candidate = original
    for old, new in replacements:
        if candidate.count(old) != 1:
            raise ValueError(f"replacement text found {candidate.count(old)} times, expected once: {old[:80]!r}")
        candidate = candidate.replace(old, new)
    print(f"candidate query:\n{candidate}")

    secret = get_secret(secret_name)
    conn = mgclient.connect(host=secret["memgraph_endpoint"], port=7687,
                            username=secret["memgraph_user"], password=secret["memgraph_password"])
    conn.autocommit = True
    # Rewrite first so a failing rewrite shows up before the long original pass.
    new_rows, new_seconds = _full_pass(conn, candidate, variables, size, "rewrite")
    print(f"rewrite pass done: {new_seconds:.1f}s rows={len(new_rows)}")
    time.sleep(5)  # let Prefect ship the logs before the long original pass starts
    old_rows, old_seconds = _full_pass(conn, original, variables, size, "original")
    conn.close()

    old_set, new_set = _multiset(old_rows), _multiset(new_rows)
    print(f"original: {old_seconds:.1f}s rows={len(old_rows)}")
    print(f"rewrite:  {new_seconds:.1f}s rows={len(new_rows)}")
    print(f"identical rows: {old_set == new_set}")
    if old_set != new_set:
        only_old = sorted(set(old_set) - set(new_set))
        only_new = sorted(set(new_set) - set(old_set))
        print(f"rows only in original: {len(only_old)}, only in rewrite: {len(only_new)}")
        for row in only_old[:3]:
            print(f"  original: {row[:400]}")
        for row in only_new[:3]:
            print(f"  rewrite:  {row[:400]}")
