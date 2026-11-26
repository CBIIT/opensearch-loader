"""List indices on an OpenSearch domain and optionally delete named ones.

Deletes only the exact index names given, and only when the domain host contains required_host.
"""

from bento.common.secret_manager import get_secret
from prefect import flow

from .opensearch_client import OpenSearchClient


def _list(client):
    rows = client.cat.indices(format="json", h="index,docs.count,store.size")
    aliases = client.cat.aliases(format="json", h="alias,index")
    print(f"{len(rows)} indices:")
    for row in sorted(rows, key=lambda r: r["index"]):
        print(f"  {row['index']} docs={row['docs.count']} size={row['store.size']}")
    for alias in aliases:
        print(f"  alias {alias['alias']} -> {alias['index']}")
    return {row["index"] for row in rows}


@flow(name="OpenSearch Manage Indices", log_prints=True)
def manage_indices(secret_name: str = "gen/amit-opensearch", required_host: str = "amit-opensearch",
                   delete_indices: list[str] = []):  # noqa: B006
    """Print indices and aliases; delete the named indices if any are given."""
    host = get_secret(secret_name)["es_host"]
    if not required_host or required_host not in host:
        raise ValueError(f"host {host} does not contain required_host {required_host!r}")
    client = OpenSearchClient(host=host, use_ssl=True, verify_certs=True).client
    print(f"host={host}")
    existing = _list(client)
    if not delete_indices:
        return
    missing = [name for name in delete_indices if name not in existing]
    if missing:
        raise ValueError(f"not on this domain, nothing deleted: {missing}")
    for name in delete_indices:
        if name.startswith("."):
            raise ValueError(f"refusing to delete system index {name}")
    for name in delete_indices:
        client.indices.delete(index=name)
        print(f"deleted {name}")
    _list(client)
