"""Read-only comparison of one index on two OpenSearch domains, document by document.

Only _count, _mapping, scroll/_search and _mget are called, so neither domain is changed.
"""

import json
from collections import Counter, defaultdict

from opensearchpy.helpers import scan

from bento.common.secret_manager import get_secret
from prefect import flow

from .opensearch_client import OpenSearchClient

BATCH = 1000
EXAMPLES = 3


def _client(secret_name):
    host = get_secret(secret_name)["es_host"]
    return host, OpenSearchClient(host=host, use_ssl=True, verify_certs=True).client


def _properties(client, index):
    mapping = client.indices.get_mapping(index=index)
    return next(iter(mapping.values()))["mappings"].get("properties", {})


def _ids(client, index):
    return {hit["_id"] for hit in scan(client, index=index, query={"query": {"match_all": {}}},
                                       _source=False, size=5000)}


def _short(value):
    text = json.dumps(value, sort_keys=True, default=str)
    return text if len(text) <= 300 else text[:300] + "..."


@flow(name="OpenSearch Index Compare", log_prints=True)
def compare_indices(reference_secret: str = "gen/dev2-cdk", candidate_secret: str = "gen/amit-opensearch",
                    index: str = "dashboard_file"):
    """Compare mappings, counts, IDs and every document of one index between two domains."""
    ref_host, ref = _client(reference_secret)
    cand_host, cand = _client(candidate_secret)
    print(f"reference={ref_host} candidate={cand_host} index={index}")

    ref_props, cand_props = _properties(ref, index), _properties(cand, index)
    print(f"mappings identical: {ref_props == cand_props}")
    if ref_props != cand_props:
        for field in sorted(set(ref_props) | set(cand_props)):
            if ref_props.get(field) != cand_props.get(field):
                print(f"  mapping differs: {field}: reference={ref_props.get(field)} candidate={cand_props.get(field)}")

    print(f"counts: reference={ref.count(index=index)['count']} candidate={cand.count(index=index)['count']}")

    ref_ids, cand_ids = _ids(ref, index), _ids(cand, index)
    only_ref, only_cand = ref_ids - cand_ids, cand_ids - ref_ids
    print(f"ids: reference={len(ref_ids)} candidate={len(cand_ids)} only_reference={len(only_ref)} "
          f"only_candidate={len(only_cand)} examples_ref={sorted(only_ref)[:EXAMPLES]} "
          f"examples_cand={sorted(only_cand)[:EXAMPLES]}")

    field_diffs = Counter()
    examples = defaultdict(list)
    compared = differing = 0
    batch = []

    def check(rows):
        nonlocal compared, differing
        found = ref.mget(index=index, body={"ids": [doc_id for doc_id, _ in rows]})["docs"]
        for (doc_id, cand_doc), ref_hit in zip(rows, found, strict=True):
            if not ref_hit.get("found"):
                continue
            ref_doc = ref_hit["_source"]
            compared += 1
            fields = [f for f in set(ref_doc) | set(cand_doc) if ref_doc.get(f) != cand_doc.get(f)]
            if fields:
                differing += 1
            for field in fields:
                field_diffs[field] += 1
                if len(examples[field]) < EXAMPLES:
                    examples[field].append((doc_id, _short(ref_doc.get(field)), _short(cand_doc.get(field))))

    for hit in scan(cand, index=index, query={"query": {"match_all": {}}}, size=BATCH):
        batch.append((hit["_id"], hit["_source"]))
        if len(batch) == BATCH:
            check(batch)
            batch = []
    if batch:
        check(batch)

    print(f"documents compared={compared} differing={differing}")
    for field, count in field_diffs.most_common():
        print(f"  field {field}: {count} documents differ")
        for doc_id, ref_value, cand_value in examples[field]:
            print(f"    {doc_id}\n      reference: {ref_value}\n      candidate: {cand_value}")
    return {"compared": compared, "differing": differing, "fields": dict(field_diffs),
            "only_reference": len(only_ref), "only_candidate": len(only_cand)}
