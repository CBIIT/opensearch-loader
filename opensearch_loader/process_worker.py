"""Private local IPC worker. Configuration travels through a socket, not argv."""
import pickle
import socket
import sys


def main():
    sock = socket.socket(fileno=int(sys.argv[1]))
    stream = sock.makefile('rwb')
    client = None
    try:
        kind, config = pickle.load(stream)
        if kind == 'query':
            from .memgraph_client import MemgraphClient
            client = MemgraphClient(**config)
        elif kind == 'write':
            from .opensearch_client import OpenSearchClient
            client = OpenSearchClient(**config)
        else:
            raise ValueError('Unknown worker kind')
        pickle.dump((True, None), stream); stream.flush()
        while True:
            request = pickle.load(stream)
            if request is None:
                break
            method, args, kwargs = request
            try:
                if kind == 'query' and method == 'execute_query':
                    result = client.execute_query(*args, **kwargs)
                elif kind == 'write' and method in {'bulk_upsert', 'bulk_update', 'upsert_document'}:
                    result = getattr(client, method)(*args, **kwargs)
                else:
                    raise ValueError('Unsupported worker operation')
                response = (True, result)
            except Exception as exc:
                response = (False, f'{type(exc).__name__}: {exc}')
            pickle.dump(response, stream); stream.flush()
    except EOFError:
        pass
    except Exception as exc:
        try:
            pickle.dump((False, f'{type(exc).__name__}: {exc}'), stream); stream.flush()
        except (OSError, ValueError):
            pass
    finally:
        if client is not None:
            if kind == 'query':
                client.close()
            else:
                client.client.close()
        stream.close(); sock.close()


if __name__ == '__main__':
    main()
