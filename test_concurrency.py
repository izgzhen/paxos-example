#!/usr/bin/env python3

'''
Usage: python test_concurrency.py

Here we show that clients can submit multiple requests concurrently.
Test scenario:
 - 10 clients sending requests concurrently
 - No node failures
 - Message loss rate of 5%
'''

import subprocess
import time
import random
from paxos_client import LockClient
import threading

class LockServerThread(threading.Thread):
    def __init__(self, cli: LockClient):
        threading.Thread.__init__(self)
        self.cli = cli

    def run(self):
        self.cli.serve_forever()

N_SERVERS = 5
server_ps = {} # id -> process

print("starting servers...")
for i in range(N_SERVERS):
    id_ = str(i + 1)
    p = subprocess.Popen(["python3", "paxos_server.py", id_, str(N_SERVERS), "-v"])
    server_ps[id_] = p
time.sleep(1)

class ClientThread(threading.Thread):
    def __init__(self, cli_id: int):
        self.cli = LockClient(cli_id, N_SERVERS)
        self.cli_id = cli_id
        threading.Thread.__init__(self)

    def run(self):
        time.sleep(random.random() * 2)
        n_lock = random.randint(0, 1)
        reply = self.cli.lock(n_lock)
        if reply['status'] == 'ok':
            assert self.cli.lock(n_lock) == {'status': 'locked', 'by': self.cli_id}
            assert self.cli.unlock(n_lock) == {'status': 'ok' }
        elif reply['status'] == 'locked':
            assert reply['status']['by'] != self.cli_id

print("processing clients")
cli_thrs = {}
latencies = []
for i in range(1, 10):
    cli_thrs[i] = ClientThread(i)
    cli_thrs[i].start()
for t in cli_thrs.values():
    t.join()
    latencies.extend(t.cli.api_latencies)

print("stopping servers...")
server_ps['5'].terminate()
server_ps['4'].terminate()
server_ps['3'].terminate()
server_ps['2'].terminate()
server_ps['1'].terminate()

print('Client API latencies: {}'.format(latencies))
