#!/usr/bin/env python3

'''
Usage: python test_random_server.py

Here we show that client can contact different servers.
Note that normally, the client will remember the Paxos leader.
Here we force it to contact a different leader each time. 
Test scenario:
 - One client
 - No node failures
 - Message loss rate of 5%
 - Client contacts different servers in the Paxos group
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
assert cli.lock(1) == {'status': 'ok'}
cli.rotate_leader()
assert cli.lock(1) == {'status': 'locked', 'by': 1}
time.sleep(1)

print("processing client unlock")
cli.rotate_leader()
assert cli.unlock(1) == {'status': 'ok'}
cli.rotate_leader()
assert cli.lock(1) == {'status': 'ok'}
print("waiting for failure recovery")
time.sleep(10)

print("stopping servers...")
server_ps['3'].terminate()
server_ps['2'].terminate()
server_ps['1'].terminate()
