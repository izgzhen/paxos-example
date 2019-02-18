#!/usr/bin/env python3

'''
Usage: python test_lock_semantics.py

Here we test when the client asks for the same lock repeatedly.
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

N_SERVERS = 3
server_ps = {} # id -> process

print("starting servers...")
for i in range(N_SERVERS):
    id_ = str(i + 1)
    p = subprocess.Popen(["python3", "paxos_server.py", id_, str(N_SERVERS), "-v"])
    server_ps[id_] = p
time.sleep(1)

client_id = 1
print("init client and processing client lock")
cli = LockClient(client_id, N_SERVERS)
assert cli.lock(1) == { 'status': 'ok' }
time.sleep(1)

print("processing client unlock")
assert cli.lock(1) == { 'status': 'locked', 'by': 1 }

print("stopping servers...")
server_ps['1'].terminate()
server_ps['2'].terminate()
server_ps['3'].terminate()
