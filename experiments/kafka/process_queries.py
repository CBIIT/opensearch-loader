"""Benchmark-only process isolation for blocking native Memgraph queries."""
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from opensearch_loader.memgraph_client import MemgraphClient
_connection=None

def connect(host,port):
 global _connection
 import mgclient
 _connection=mgclient.connect(host=host,port=port);_connection.autocommit=True

def execute(query,parameters):
 cursor=_connection.cursor()
 try:
  cursor.execute(query,parameters)
  fields=[col.name for col in cursor.description]
  return [dict(zip(fields,row)) for row in cursor.fetchall()]
 finally:cursor.close()

class ProcessQueries(MemgraphClient):
 def __init__(self,pool):self.pool=pool;self.last_query_time=0.0;self.connection=None
 def execute_query(self,query,parameters=None):
  self.validate_read_only(query);self.validate_pagination_params(query)
  return self.pool.submit(execute,query,parameters or {}).result()
 def close(self):pass

def make_pool(workers,host,port):
 return ProcessPoolExecutor(max_workers=workers,mp_context=get_context('spawn'),initializer=connect,initargs=(host,port))

def ready(_):
 import os,time
 time.sleep(.1)
 return os.getpid()
