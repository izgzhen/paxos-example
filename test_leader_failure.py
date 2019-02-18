#!/usr/bin/env python3

'''
Usage: python test_leader_failure.py

Here we show that the system can handle leader failure.
Test scenario:
 - One client
 - The Paxos leader fails in the middle
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

N_SERVERS = 3
server_ps = {} # id -> process

print("starting servers...")
for i in range(N_SERVERS):
    id_ = str(i + 1)
    p = subprocess.Popen(["python3", "paxos_server.py", id_, str(N_SERVERS), "-v"])
    server_ps[id_] = p
time.sleep(1)

print("init client and processing client lock")
cli = LockClient(1, N_SERVERS)
assert cli.lock(1) == { 'status': 'ok' }
time.sleep(1)

fires = "🔥" * 10
print(fires + " node 1 (leader) failed in between " + fires)
server_ps['1'].terminate()

print("paxos-based leader election *should* happen now")
time.sleep(5)

print("processing client unlock")
assert cli.unlock(1) == { 'status': 'ok' }

print("stopping servers...")
server_ps['3'].terminate()
server_ps['2'].terminate()
