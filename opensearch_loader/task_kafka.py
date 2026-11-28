"""Ephemeral, loopback-only Kafka broker for a single Prefect test task."""
import hashlib
import logging
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

log = logging.getLogger('OpenSearchLoader')


class TaskKafka:
    def __init__(self):
        self.process = None
        self.directory = None
        self.previous_address = os.environ.get("LOADER_KAFKA_BROKER")

    def __enter__(self):
        try:
            return self.start()
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def start(self):
        # Installation only happens for the explicitly enabled test mode.
        if not shutil.which('java'):
            if os.geteuid() != 0 or not shutil.which('apt-get'):
                raise RuntimeError('Kafka test requires Java: install default-jre-headless in the ECS image or supply JAVA_HOME/PATH')
            log.info('Installing Java for task-local Kafka')
            subprocess.run(['apt-get', 'update', '-qq'], check=True, timeout=300)
            subprocess.run(['apt-get', 'install', '-y', '--no-install-recommends', 'default-jre-headless'], check=True, timeout=300)
        home = os.environ.get('LOADER_KAFKA_HOME')
        self.directory = tempfile.TemporaryDirectory(prefix='loader-kafka-')
        root = Path(self.directory.name)
        if not home:
            name = 'kafka_2.13-3.9.1.tgz'
            url = 'https://archive.apache.org/dist/kafka/3.9.1/' + name
            archive = root / name
            log.info('Downloading Kafka 3.9.1 for this test task')
            with urllib.request.urlopen(url, timeout=120) as source, archive.open('wb') as target:
                shutil.copyfileobj(source, target)
            with urllib.request.urlopen(url + '.sha512', timeout=60) as response:
                checksum = response.read().decode()
            # Apache uses a filename followed by grouped hexadecimal digest text.
            digest_text = checksum.split(':', 1)[-1]
            expected = ''.join(re.findall(r'\b[0-9a-fA-F]{8,128}\b', digest_text)).lower()
            actual = hashlib.sha512(archive.read_bytes()).hexdigest()
            if expected != actual:
                raise RuntimeError('Kafka download SHA512 mismatch')
            subprocess.run(['tar', '-xzf', str(archive), '-C', str(root)], check=True, timeout=120)
            archive.unlink()
            home = str(root / 'kafka_2.13-3.9.1')
        ports = []
        sockets = []
        try:
            for _ in range(2):
                sock = socket.socket(); sock.bind(('127.0.0.1', 0))
                sockets.append(sock); ports.append(sock.getsockname()[1])
        finally:
            for sock in sockets: sock.close()
        port, controller = ports
        self.address = f'127.0.0.1:{port}'
        config = root / 'server.properties'
        config.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{port}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={root / 'data'}
num.partitions=4
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
message.max.bytes=52428800
replica.fetch.max.bytes=52428800
log.segment.bytes=67108864
log.retention.ms=600000
log.retention.check.interval.ms=60000
log.retention.bytes=536870912
log.segment.ms=60000
auto.create.topics.enable=false
''')
        env = dict(os.environ, KAFKA_HEAP_OPTS='-Xms256m -Xmx512m', LOG_DIR=str(root / 'logs'))
        storage = str(Path(home) / 'bin/kafka-storage.sh')
        cluster_id = subprocess.check_output([storage, 'random-uuid'], env=env, text=True, timeout=60).strip()
        subprocess.run([storage, 'format', '-t', cluster_id, '-c', str(config)], env=env, check=True, timeout=60)
        self.process = subprocess.Popen([str(Path(home) / 'bin/kafka-server-start.sh'), str(config)], env=env)
        from confluent_kafka.admin import AdminClient
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('Task-local Kafka exited during startup')
            try:
                AdminClient({'bootstrap.servers': self.address}).list_topics(timeout=2)
                os.environ['LOADER_KAFKA_BROKER'] = self.address
                log.info('Task-local Kafka ready: address=%s heap_max=512MiB', self.address)
                return self
            except Exception:
                time.sleep(1)
        raise TimeoutError('Task-local Kafka startup timed out')

    def __exit__(self, *args):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try: self.process.wait(30)
            except subprocess.TimeoutExpired:
                self.process.kill(); self.process.wait()
        if self.directory:
            self.directory.cleanup()
        if self.previous_address is None:
            os.environ.pop("LOADER_KAFKA_BROKER", None)
        else:
            os.environ["LOADER_KAFKA_BROKER"] = self.previous_address
