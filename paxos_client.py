#!/usr/bin/env python3

'''
A persistent client capable of sending multiple messages, resending messages on
timeout or failure, and remembering the stable paxos leader.
'''

import json
import signal
import sys
import socket
import time
from typing import Tuple, List
from socketserver import ThreadingMixIn, TCPServer, BaseRequestHandler

SERVER_PORT = 3880
MAX_RETRIES = 10

class LockClient(object):
    def __init__(self, client_id: int, total_servers: int):
        self.total_servers = total_servers
        self.client_id = client_id
        self.leader_id = 1
        self.retries = 0
        self.api_latencies = [] # type: List[float]

    def server_addr_port(self):
        return ('localhost', SERVER_PORT + self.leader_id)

    def rotate_leader(self):
        self.leader_id = ((self.leader_id + 1) % self.total_servers) + 1

    def incr_and_get_retry(self):
        if self.retries > MAX_RETRIES:
            return None
        self.retries += 1
        return self.retries

    def send_msg(self, command: str):
        self.log('Sending {} to leader {}'.format(command, self.leader_id))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        payload = { 'source': 'client', 'client': self.client_id, 'command': command }
        try:
            sock.connect(self.server_addr_port())
            sock.sendall(json.dumps(payload).encode('utf-8'))
            data = json.loads(sock.recv(1024).decode('utf-8'))
            if data['status'] == 'waiting':
                n = self.incr_and_get_retry()
                if n is None: return
                self.log("waiting leader and #{} retry...".format(n))
                time.sleep(10)
                return self.send_msg(command)
            elif data['status'] == 'redirect':
                n = self.incr_and_get_retry()
                if n is None: return
                self.log("redirect to new leader and #{} retry...".format(n))
                self.leader_id = data['leader']
                return self.send_msg(command)
            return data
        except (ConnectionRefusedError, ConnectionResetError) as e:
            self.rotate_leader()
            n = self.incr_and_get_retry()
            if n is None: return
            self.log("rotating leader and #{} retry...".format(n))
            time.sleep(1)
            return self.send_msg(command)
        finally:
            sock.close()

    def lock(self, lock_n: int):
        now = time.time()
        ret = self.send_msg("lock_{}".format(lock_n))
        self.api_latencies.append(time.time() - now)
        return ret

    def get_msg_count(self):
        return self.send_msg("get_msg_count")

    def unlock(self, lock_n: int):
        now = time.time()
        ret = self.send_msg("unlock_{}".format(lock_n))
        self.api_latencies.append(time.time() - now)
        return ret

    def log (self, msg):
        print('[C{}] {}'.format(self.client_id, msg))
