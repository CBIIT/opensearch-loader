"""Local benchmark transport: bounded Kafka queue, ordered partitions, write ACKs."""
import json,sys,time,subprocess,threading
from pathlib import Path
from zlib import crc32
from confluent_kafka import Producer,Consumer,TopicPartition,OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient,NewTopic

BROKER='127.0.0.1:19092'
MAX_DOCUMENTS=5000
MAX_PAYLOAD_BYTES=48*1024*1024
MAX_MESSAGE_BYTES=50*1024*1024

def producer():
 return Producer({'bootstrap.servers':BROKER,'enable.idempotence':True,'message.max.bytes':MAX_MESSAGE_BYTES,'compression.type':'gzip'})

def consume(topic,ack,partition):
 from opensearch_loader.opensearch_client import OpenSearchClient
 c=Consumer({'bootstrap.servers':BROKER,'group.id':topic,'enable.auto.commit':False,'auto.offset.reset':'earliest','max.partition.fetch.bytes':MAX_MESSAGE_BYTES,'fetch.max.bytes':2*MAX_MESSAGE_BYTES})
 c.assign([TopicPartition(topic,int(partition),OFFSET_BEGINNING)])
 p=producer();client=OpenSearchClient('http://127.0.0.1:19204')
 try:
  while True:
   m=c.poll(1)
   if m is None:continue
   if m.error():raise RuntimeError(str(m.error()))
   task=json.loads(m.value());t=time.monotonic();error=None
   try:
    getattr(client,task['method'])(task['index'],task['documents'],task['key'])
    c.commit(message=m,asynchronous=False)
   except Exception as exc:error=repr(exc)
   p.produce(ack,json.dumps({'seq':task['seq'],'seconds':time.monotonic()-t,'error':error}).encode())
   if p.flush(30):raise RuntimeError('ACK delivery timeout')
   if error:raise RuntimeError(error)
 finally:c.close();client.client.close()

class KafkaWrites:
 def __init__(self,delegate,topic,out,consumers=2,max_payload_bytes=MAX_PAYLOAD_BYTES):
  self.max_payload_bytes=max_payload_bytes
  self.consumers=consumers
  self.delegate=delegate;self.topic=topic;self.ack=topic+'-ack';self.pending={};self.lock=threading.RLock();self.seq=0;self.peak=0;self.child_seconds=0;self.wait_seconds=0;self.ops=0
  self.errors=[];self.children=[];self.logs=[]
  admin=AdminClient({'bootstrap.servers':BROKER})
  for f in admin.create_topics([NewTopic(topic,consumers,1,config={'retention.ms':'3600000'}),NewTopic(self.ack,1,1,config={'retention.ms':'3600000'})]).values():f.result(30)
  self.p=producer();self.c=Consumer({'bootstrap.servers':BROKER,'group.id':self.ack,'enable.auto.commit':False})
  self.c.assign([TopicPartition(self.ack,0,OFFSET_BEGINNING)])
  for part in range(consumers):
   log=open(out/f'consumer-{part}.log','w');self.logs.append(log)
   self.children.append(subprocess.Popen([sys.executable,'-u',str(Path(__file__).resolve()),'consume',topic,self.ack,str(part)],stdout=log,stderr=log))
 def __getattr__(self,name):return getattr(self.delegate,name)
 def delivery(self,err,msg):
  if err:self.errors.append(str(err))
 def _receive(self,timeout):
  if getattr(self,'fatal',None):raise RuntimeError(self.fatal)
  self.p.poll(0)
  if self.errors:raise RuntimeError(self.errors)
  if any(p.poll() is not None for p in self.children):raise RuntimeError('Kafka consumer exited; inspect consumer logs')
  m=self.c.poll(timeout)
  if m:
   if m.error():raise RuntimeError(str(m.error()))
   a=json.loads(m.value())
   if a['error']:
    self.fatal=a['error'];raise RuntimeError(a['error'])
   self.pending.pop(a['seq'],None);self.child_seconds+=a['seconds']
 def receive(self,timeout):
  with self.lock:self._receive(timeout)
 def drain(self,index=None):
  t=time.monotonic()
  if self.p.flush(30):raise RuntimeError('Kafka producer flush timed out')
  while True:
   with self.lock:remaining=any(index is None or target==index for target in self.pending.values())
   if not remaining:break
   self.receive(.2)
   if time.monotonic()-t>600:raise TimeoutError('OpenSearch ACK timeout')
  self.wait_seconds+=time.monotonic()-t
 def _write(self,method,index,docs,key=None,**kw):
  key=key or kw.pop('id_field',None)
  if not key:raise ValueError('id_field is required')
  partitions=[[] for _ in range(self.consumers)]
  for d in docs:
   if d.get(key):partitions[crc32((index+'\0'+str(d[key])).encode())%self.consumers].append(d)
  for part,rows in enumerate(partitions):
   # Bound both record count and serialized bytes; one record cannot exceed broker limit.
   batch=[];size=0
   for row in rows:
    encoded=len(json.dumps(row).encode())
    if encoded>MAX_PAYLOAD_BYTES:raise ValueError('Single document exceeds benchmark Kafka limit')
    if batch and (len(batch)>=MAX_DOCUMENTS or size+encoded>self.max_payload_bytes):self.send(method,index,batch,key,part);batch=[];size=0
    batch.append(row);size+=encoded
   if batch:self.send(method,index,batch,key,part)
 def send(self,method,index,rows,key,part):
  with self.lock:self._send(method,index,rows,key,part)
 def _send(self,method,index,rows,key,part):
  t=time.monotonic()
  while len(self.pending)>=10:
   self.receive(.2)
   if time.monotonic()-t>600:raise TimeoutError('Kafka backpressure timeout')
  self.wait_seconds+=time.monotonic()-t
  self.seq+=1;self.pending[self.seq]=index;self.ops+=len(rows);self.peak=max(self.peak,len(self.pending))
  self.p.produce(self.topic,json.dumps({'seq':self.seq,'method':method,'index':index,'documents':rows,'key':key}).encode(),partition=part,on_delivery=self.delivery)
  self.receive(0)
 def bulk_upsert(self,*a,**kw):self._write('bulk_upsert',*a,**kw)
 def bulk_update(self,*a,**kw):self._write('bulk_update',*a,**kw)
 def upsert_pages(self,index,pages):
  # Preserve page sources: use a synthetic routing field stripped by consumer below.
  for key,page in pages:self.upsert_document(index,key,page)
 def upsert_document(self,index,key,page):
  self.drain(index)
  with self.lock:self._upsert_document(index,key,page)
 def _upsert_document(self,index,key,page):
  # About pages use the same Kafka channel through a consumer adapter method.
  self.seq+=1;self.pending[self.seq]=index
  self.p.produce(self.topic,json.dumps({'seq':self.seq,'method':'upsert_document','index':index,'documents':key,'key':page}).encode(),partition=crc32(str(key).encode())%self.consumers,on_delivery=self.delivery)
 def refresh_index(self,index):self.drain(index);self.delegate.refresh_index(index)
 def shutdown(self):
  for p in self.children:
   if p.poll() is None:p.terminate()
  for p in self.children:
   try:p.wait(10)
   except subprocess.TimeoutExpired:p.kill();p.wait()
  self.c.close()
  for f in self.logs:f.close()

if __name__=='__main__':
 sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
 consume(*sys.argv[2:])
