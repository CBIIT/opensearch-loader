import argparse
import json
import os
import socket
import sys
import time
from opensearch_loader.write_consumer import serve

class Client:
    def __init__(self, path=None, fail_init=False, **kw):
        if fail_init:
            raise ValueError('deliberate initialization failure')
        self.path=path
        self.client=self
    def close(self): pass
    def bulk_upsert(self,index,docs,key,**kw):self.record('create',docs)
    def bulk_update(self,index,docs,key,**kw):self.record('update',docs)
    def record(self,operation,docs):
        if docs[0].get('crash'):os._exit(7)
        if docs[0].get('hang'):time.sleep(30)
        if docs[0].get('fail'):raise ValueError('deliberate write failure')
        with open(self.path,'a') as f:f.write(json.dumps([os.getpid(),operation,docs])+'\n')

p=argparse.ArgumentParser();p.add_argument('--fd',type=int,required=True);args=p.parse_args()
sys.exit(serve(socket.socket(fileno=args.fd),Client))
