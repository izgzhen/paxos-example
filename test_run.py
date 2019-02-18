#!/usr/bin/env python3

'''
Usage: python test_run.py

Here we test normal lock service.
Test scenario:
 - One client
 - No node failures
 - Message loss rate of 5%
'''

import subprocess
import time
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

print("init client and processing client lock")
cli = LockClient(1, N_SERVERS)
assert cli.lock(1) == {'status': 'ok'}
assert cli.lock(1) == {'status': 'locked', 'by': 1}
time.sleep(1)

print("processing client unlock")
assert cli.unlock(1) == {'status': 'ok'}
assert cli.lock(1) == {'status': 'ok'}

msg_counts = []
for i in range(N_SERVERS):
    cli.leader_id = i + 1
    msg_counts.append(cli.get_msg_count()['msg_count'])

print("stopping servers...")
for i in range(N_SERVERS):
    id_ = str(i + 1)
    server_ps[id_].terminate()
time.sleep(2)
print("msg_counts: {}".format(msg_counts))
print("client API latencies: {}".format(cli.api_latencies))
