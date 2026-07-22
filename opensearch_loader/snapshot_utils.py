"""Stateless utilities for interpreting OpenSearch snapshot data and errors."""

from typing import Any, Dict, List


def format_snapshot_names(snapshots: List[Dict[str, Any]]) -> str:
    """Format repository and snapshot names for logs and errors."""
    return ', '.join(
        '/'.join(
            part for part in (
                str(snapshot.get('repository', 'unknown-repository')),
                str(snapshot.get('snapshot', 'unknown-snapshot')),
            ) if part
        )
        for snapshot in snapshots
    )


def is_snapshot_in_progress_error(error: Exception) -> bool:
    """Return whether an OpenSearch request failed due to an active snapshot."""
    return (
        getattr(error, 'error', None) == 'snapshot_in_progress_exception'
        or 'snapshot_in_progress_exception' in str(error)
    )
