"""Measure Bulk API calls without logging document contents or credentials."""
import json
import logging
import time
from collections import Counter

logger = logging.getLogger('OpenSearchLoader')


class MeasuredBulkClient:
    """Forward helper calls; never alter actions, transport settings or retries."""
    def __init__(self, client, index, query, operation):
        self._client = client
        self._context = {'index': index, 'query': query, 'operation': operation}

    def __getattr__(self, name):
        return getattr(self._client, name)

    def bulk(self, *args, **kwargs):
        body = kwargs.get('body', args[0] if args else None)
        payload_bytes = (len(body.encode('utf-8')) if isinstance(body, str)
                         else len(body) if isinstance(body, bytes) else None)
        started = time.monotonic()
        metrics = dict(self._context, payload_bytes=payload_bytes)
        try:
            response = self._client.bulk(*args, **kwargs)
            metrics['server_took_ms'] = response.get('took')
            statuses = Counter()
            for item in response.get('items', []):
                for result in item.values():
                    statuses[str(result.get('status', 'unknown'))] += 1
            metrics.update(items=len(response.get('items', [])),
                           errors=response.get('errors'), item_statuses=dict(statuses))
            return response
        except Exception as exc:
            metrics.update(error_type=type(exc).__name__,
                           status=getattr(exc, 'status_code', None))
            raise
        finally:
            metrics['client_wall_ms'] = round((time.monotonic() - started) * 1000, 3)
            # wall includes client/network/transport retries; took is not pure CPU time.
            try:
                logger.info('BULK_REQUEST_METRICS %s', json.dumps(metrics, sort_keys=True))
            except Exception:
                pass  # Diagnostic failures must not hide a write result or exception.
