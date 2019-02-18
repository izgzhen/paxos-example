#/usr/bin/env python3

import random
import time
import signal
import sys
import json
import socket
import threading
from typing import List, Any, Dict, Set
from socketserver import TCPServer, BaseRequestHandler

PORT = 3880            # group port range start, port = PORT + server_id
PORT_HEARTBEAT = 3870  # group heartbeat port range start
WAITING_TIMEOUT = 3    # time to wait before resending potentially lost msgs

# enums for state machine command sequence
S_UNSURE = 'undefined'
S_NO_OP = 'no-op'
S_ELECT_LEADER_PREFIX = 'S_ELECT_LEADER_'
def S_ELECT_LEADER(leader_id):
    return S_ELECT_LEADER_PREFIX + str(leader_id)

# enums for paxos message types
T_PREP = 'prepare'
T_PREP_R = 'prepare-reply'
T_ACC = 'accept'
T_ACC_R = 'accept-reply'
T_LEARN = 'learn'

# phases
P_PREP = 0
P_ACC = 1
P_LEARN = 2

# waiting status, used in resend logic to handle network outage
W_OK = 0
W_WAITING_PREPARE_REPLIES = 1
W_WAITING_ACCEPT_REPLIES = 2
# TODO: waiting on learn?

def INIT_PROGRESS():
    return {
        'phase': P_PREP,
        'base_n': 0, # proposal number base
        'highest_proposed_v': '',
        'highest_proposed_n': -1, # highest proposal number I've promised
        'prepare': {}, # stores prepare responses
        'accepted_reply': {} # stores accept responses
    }

class HeartBeatRecvHandler(BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024).strip().decode('utf-8')
        try:
            data = json.loads(data)
            self.server.receive_msg(data)
        except ValueError:
            self.server.lock_server.log('[HeartbeatRecv] Could not parse the data as JSON: {}'.format(data))
        finally:
            # close the connection because everything is async
            self.request.close()

class HeartBeatRecvServer(TCPServer):
    allow_reuse_address = True
    last_heartbeat = None
    def __init__ (self, sid: int, lock_server):
        addr = ('localhost', PORT_HEARTBEAT + sid)
        self.sid = sid
        self.lock_server = lock_server
        TCPServer.__init__(self, addr, HeartBeatRecvHandler)

    def receive_msg(self, data):
        if data != self.lock_server.leader:
            # my leader might be incorrect, so trigger election by not updating heartbeat
            return
        self.last_heartbeat = time.time()

class HeartBeatRecvThread(threading.Thread):
    def __init__(self, sid: int, lock_server):
        threading.Thread.__init__(self)
        self.server = HeartBeatRecvServer(sid, lock_server)

    def run(self):
        self.server.serve_forever()

class HeartBeatCheckerThread(threading.Thread):
    def __init__(self, heartbeat_server, lock_server):
        threading.Thread.__init__(self)
        self.heartbeat_server = heartbeat_server
        self.lock_server = lock_server
        self.stopped = False

    def run(self):
        while not self.stopped:
            if self.lock_server.is_leader():
                self.lock_server.notify()
            elif self.heartbeat_server.last_heartbeat is not None:
                wait_time = (self.lock_server.server_id + random.random()) * 3
                if self.heartbeat_server.last_heartbeat + wait_time < time.time():
                    self.lock_server.log('heartbeat check failed, trigger election')
                    self.lock_server.elect_leader()
                    return
            time.sleep(1.5)

class HeartBeatSenderThread(threading.Thread):
    def __init__(self, lock_server):
        threading.Thread.__init__(self)
        self.lock_server = lock_server
        self.stopped = False

    def run(self):
        time.sleep(1)
        while not self.stopped:
            self.lock_server.send_heartbeat()
            time.sleep(1)

class LockServer(TCPServer):
    # whether the server will allow the reuse of an address
    allow_reuse_address = True

    leader = 1 # initial leader
    is_electing = False # if this node is during the election period

    # state machine
    next_exe = 0 # the next executable command index

    # an array of command sequence
    # the i-th element corresponds to the result of the i-th paxos instance
    # undecided commands are marked by S_UNSURE
    states = [] # type: List[str]

    locks = [None, None] # locked by client n or None

    msg_count = 0

    # stores per instance information
    # leader: prepare responses, accept responses, proposal number
    # follower: highest promised proposal number, highest accept
    progress = {} # type: Dict[int, Any]

    failed_nodes = set() # type: Set[int]

    handler_lock = threading.Lock()

    waiting = W_OK
    waiting_settime = 0
    wait_args = None

    # constructor
    def __init__ (self, sid: int, total: int):
        self.server_id = sid
        self.total_nodes = total
        addr = ('localhost', PORT + sid)
        TCPServer.__init__(self, addr, LockHandler)

    # bump and get the next proposal number
    def bump_next_proposal_n (self, idx: int) -> int:
        self.progress[idx]['base_n'] += 1
        n = self.progress[idx]['base_n'] * self.total_nodes + self.server_id
        return n

    # receive a message from peer
    def receive_msg (self, request, data):
        tp = data['type']
        reply = { 'status': 'ack' }

        if 'instance' in data and self.is_stale(data['instance']):
            reply['status'] = 'stale'
            idx = data['instance']
            pg = self.progress[idx]
            reply['progress'] = {
                'instance': idx,
                'command' : pg['highest_proposed_v'],
                'type' : T_LEARN
            }
            if 'client' in pg:
                reply['progress']['client'] = pg['client']
        # send back an ACK fast -- it shouldn't block client/peer
        # this is not the actual reply of request
        # just a basic sanity check
        request.sendall(json.dumps(reply).encode('utf-8'))
        request.close()
        if tp == T_PREP:
            self.on_prepare_request(data)
        elif tp == T_PREP_R:
            self.on_prepare_reply(data)
        elif tp == T_ACC:
            self.on_accept_request(data)
        elif tp == T_ACC_R:
            self.on_accept_reply(data)
        elif tp == T_LEARN:
            self.on_learn(data)
        else:
            print("unknown tp: {}".format(tp))
            sys.exit(-1)

    def get_next_instance_idx(self) -> int:
        if len(self.progress.keys()) == 0:
            return 0
        else:
            return max(self.progress.keys()) + 1

    # handle a client connection
    def handle_client(self, data):
        cmd = data['command']
        client = data['client']
        reply = { "status": "ok" }

        if self.is_leader():
            if self.waiting > W_OK:
                reply['status'] = 'waiting'
            else:
                if cmd == "get_msg_count":
                    reply['msg_count'] = self.msg_count
                    return reply
                # no I/O, just mem-based sanity check, this should not block
                elif cmd.startswith("lock_"):
                    n_lock = int(cmd.lstrip("lock_"))
                    assert n_lock >= 0, str(n_lock)
                    assert n_lock < len(self.locks), str(n_lock)
                    if self.locks[n_lock] is not None:
                        reply['status'] = 'locked'
                        reply['by'] = self.locks[n_lock]
                        return reply
                elif cmd.startswith("unlock_"):
                    n_lock = int(cmd.lstrip("unlock_"))
                    assert n_lock >= 0, str(n_lock)
                    assert n_lock < len(self.locks), str(n_lock)
                    if self.locks[n_lock] != client:
                        reply['status'] = 'locked'
                        reply['by'] = self.locks[n_lock]
                        return reply
                # create the paxos instance number
                # we assume holes are filled with no-op, so we always increment here
                idx = self.get_next_instance_idx()
                assert 'instance' not in data
                data['instance'] = idx

                # remember intended proposal command
                assert idx not in self.progress
                self.progress[idx] = INIT_PROGRESS()
                data['proposal_n'] = self.bump_next_proposal_n(idx)

                self.send_accept(data)
        else:
            reply['status'] = 'redirect'
            reply['leader'] = self.leader
        return reply

    # when receiving a reply to accept request
    def on_accept_reply (self, reply):
        if not self.is_leader() and not self.is_electing:
            return
        idx = reply['instance']

        if self.progress[idx]['phase'] >= P_LEARN:
            return

        acc = self.progress[idx]['accepted_reply'] # NOTE: acc is a reference
        assert reply['server_id'] not in acc
        acc[reply['server_id']] = reply

        half = self.total_nodes / 2
        if len(acc) > half:
            self.waiting = W_OK
            count = 0
            for accepted in acc.values():
                if accepted['status'] == "success":
                    count += 1
            if count > half:
                self.send_learn(reply, self.progress[idx]['highest_proposed_v'])
                self.progress[idx]['phase'] = P_LEARN

    # when receiving a request to accept a proposal
    def on_accept_request (self, request):
        self.log('on_accept_request({})'.format(request))
        n = request['proposal_n']
        v = request['command']
        idx = request['instance']
        sender = request['server_id']
        response = self.init_payload(request)
        response['status'] = "success"
        response['type'] = T_ACC_R
        self.init_progress(idx)
        if n < self.progress[idx]['highest_proposed_n']:
            self.log('on_accept_request failed: {} < {}'.format(n, self.progress[idx]['highest_proposed_n']))
            response['status'] = "failure"
        else:
            self.progress[idx]['highest_proposed_n'] = n
            self.progress[idx]['highest_proposed_v'] = v

        if sender == self.server_id:
            response['server_id'] = self.server_id
            self.on_accept_reply(response)
        else:
            self.send_msg(sender, response, recv=False)

    # send accept to all nodes
    def send_accept (self, request):
        self.log("send_accept ({})".format(request))
        self.waiting = W_WAITING_ACCEPT_REPLIES
        self.waiting_settime = time.time()
        self.wait_args = (request,)
        if not self.is_leader() and not self.is_electing:
            return # only leader sends accept

        idx = request['instance']
        if self.progress[idx]['phase'] >= P_ACC:
            return # we've already sent accept messages

        n = self.progress[idx]['highest_proposed_n']

        request['type'] = T_ACC
        request['proposal_n'] = n

        # send actual msgs to all other nodes
        replies = self.send_all(request)
        reply = self.check_stale(replies)
        if reply is not None:
            self.on_learn(reply)
            return

        # fake msg to self
        request['server_id'] = self.server_id
        self.on_accept_request(request)

        self.progress[idx]['phase'] = P_ACC

    def check_stale(self, replies):
        rs = [r[1]['progress'] for r in replies if r[1]['status'] == 'stale' for r in replies]
        if len(rs) == 0:
            return None
        r = rs[0]
        for r2 in rs[1:]:
            assert r2 == r
        return r

    def progress_str(self):
        return json.dumps(self.progress, sort_keys=True, indent=4)

    def log_progress(self):
        print("*" * 80 + "\n" + self.progress_str() + "\n" + "*" * 80)

    # when receiving a prepare reply
    def on_prepare_reply (self, reply):
        if not self.is_leader() and not self.is_electing:
            return # only leader follows up with accept

        idx = reply['instance']
        # stores info into progress
        pg_ref = self.progress[idx]
        prep_ref = pg_ref['prepare']
        if reply['server_id'] in prep_ref:
            assert prep_ref[reply['server_id']] == reply, \
                    '{} != {}'.format(prep_ref[reply['server_id']], reply)
        prep_ref[reply['server_id']] = reply

        # do we receive a majority of replies?
        if len(prep_ref) > self.total_nodes / 2:
            # find out the highest numbered proposal
            hn = -1
            hv = ''
            for sid, replied_reply in prep_ref.items():
                if replied_reply['prep_n'] > hn:
                    hn = replied_reply['prep_n']
                    hv = replied_reply['prep_value']

            assert hn >= 0
            reply['command'] = hv
            self.send_accept(reply)

    def is_stale(self, idx):
        return len(self.states) > idx and self.states[idx] != S_UNSURE

    # when receiving a prepare request
    def on_prepare_request (self, request):
        n = request['proposal_n']
        v = request['command']
        idx = request['instance']
        self.init_progress(idx)

        # proposal n is higher than what we've promised
        if n > self.progress[idx]['highest_proposed_n']:
            self.progress[idx]['highest_proposed_n'] = n
            self.progress[idx]['highest_proposed_v'] = v

        # reply
        response = self.init_payload(request)
        response['type'] = T_PREP_R
        response['prep_n'] = self.progress[idx]['highest_proposed_n']
        response['prep_value'] = self.progress[idx]['highest_proposed_v']

        if request['server_id'] == self.server_id:
            response['server_id'] = self.server_id
            self.on_prepare_reply(response)
        else:
            self.send_msg(request['server_id'], response, recv=False)

    # send prepare messages to all nodes
    def send_prepare (self, payload):
        self.waiting = W_WAITING_PREPARE_REPLIES
        self.waiting_settime = time.time()
        self.wait_args = (payload,)
        self.log("send_prepare({})".format(payload))
        payload['type'] = T_PREP

        # send actual msgs to all other nodes
        replies = self.send_all(payload)
        reply = self.check_stale(replies)
        if reply is not None:
            self.on_learn(reply)
            return

        # fake msg to self
        payload['server_id'] = self.server_id
        self.on_prepare_request(payload)

    # send heartbeat to followers
    def send_heartbeat(self):
        if self.server_id == self.leader:
            msg = self.server_id
            for i in range(1, self.total_nodes + 1):
                if not i == self.server_id and not (i in self.failed_nodes):
                    self.send_heartbeat_message(i, msg)

    def send_heartbeat_message(self, target_id: int, msg):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr = ('localhost', PORT_HEARTBEAT + target_id)
        try:
            sock.connect(addr)
            sock.sendall(json.dumps(msg).encode('utf-8'))
        except ConnectionRefusedError:
            # send heartbeat always -- don't care about node failure or
            # network failure. Best efforts.
            pass
        finally:
            sock.close()

    # send learn messages to all nodes
    def send_learn (self, data, cmd):
        payload = self.init_payload(data)
        payload['type'] = T_LEARN
        payload['command'] = cmd
        replies = self.send_all(payload)
        reply = self.check_stale(replies)
        if reply is not None:
            self.on_learn(reply)
            return

        payload['server_id'] = self.server_id
        self.on_learn(payload)

    # when a learn message is received
    def on_learn (self, request):
        idx = request['instance']
        cmd = request['command']

        # update states
        self.init_states_to(idx)
        self.states[idx] = cmd

        # update progress
        self.init_progress(idx)
        self.progress[idx]['command'] = cmd
        if 'client' in request:
            self.progress[idx]['client'] = request['client']

        # execute if possible
        self.execute()

        # debug
        self.log(self.states, verbose=True)
        self.log(self.progress, verbose=True)

    # execute the commands
    def execute(self):
        for i in range(self.next_exe, len(self.states)):
            cmd = self.states[i]
            if cmd == S_UNSURE:
                break # a hole in the sequence, we can't execute further

            self.next_exe += 1

            if cmd.startswith(S_ELECT_LEADER_PREFIX):
                new_leader_id = int(cmd[len(S_ELECT_LEADER_PREFIX):])
                self.log("resetting leader to {}".format(new_leader_id))
                self.leader = new_leader_id
            elif cmd.startswith("lock_"):
                n_lock = int(cmd.lstrip("lock_"))
                assert n_lock >= 0, str(n_lock)
                assert n_lock < len(self.locks), str(n_lock)
                client = self.progress[i]['client']
                if self.locks[n_lock] is None:
                    self.locks[n_lock] = client
            elif cmd.startswith("unlock_"):
                n_lock = int(cmd.lstrip("unlock_"))
                assert n_lock >= 0, str(n_lock)
                assert n_lock < len(self.locks), str(n_lock)
                client = self.progress[i]['client']
                if self.locks[n_lock] == client:
                    self.locks[n_lock] = None
            else:
                raise Exception("unknown command: " + cmd)

    # insert initial instance if it doesn't exist before
    def init_progress(self, idx: int):
        if not idx in self.progress:
            self.progress[idx] = INIT_PROGRESS()
            
    # insert values into states up to idx
    def init_states_to(self, idx):
        end = len(self.states)
        if len(self.states) > idx:
            return # the idx already exists

        for i in range(end, idx + 1):
            self.states.append(S_UNSURE) # fill holes with S_UNSURE

    # copy re-usable fields into a new payload object
    def init_payload (self, payload):
        stripped = {
            'instance': payload['instance']
        }
        if 'client' in payload:
            stripped['client'] = payload['client']
        return stripped

    # send a message to peer and immediately close connection
    def send_msg (self, target_id: int, payload, recv=True):
        if random.random() > 0.95:
            self.log("^" * 80 + "\ndropping {} to {}\n".format(payload, target_id) + "^" * 80)
            return # randomly drop message
        self.msg_count += 1
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if target_id == self.leader:
            sock.settimeout((self.server_id + random.random()) * 3)
        else:
            sock.settimeout(1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        addr = ('localhost', PORT + target_id)
        payload['source'] = 'server'
        payload['server_id'] = self.server_id
        try:
            sock.connect(addr)
            sock.sendall(json.dumps(payload).encode('utf-8'))
            self.log('Paxos #{}, sending {} to {}'.format(payload['instance'],
                      payload, target_id))
            if recv:
                try:
                    return json.loads(sock.recv(1024).decode('utf-8'))
                except ConnectionResetError as e:
                    self.log(">>>>>>\ntarget_id: {}\npayload: {}\n<<<<<<<".format(target_id, payload))
                    raise e
        except ConnectionRefusedError:
            self.log("peer {} is down, removing it".format(addr))
            self.failed_nodes.add(target_id)
            if target_id == self.leader:
                self.log("send_msg to leader {} failed, trigger election".format(addr))
                self.elect_leader()
        except socket.timeout as e:
            self.log("socket timeout {}\ntarget_id: {}\npayload: {}".format(e, target_id, payload))
            raise e
        finally:
            sock.close()

    # send to all followers (every nodes except myself)
    def send_all (self, payload):
        self.log("send_all({})".format(payload))
        replies = []
        for i in range(1, self.total_nodes + 1):
            if not i == self.server_id and not (i in self.failed_nodes):
                self.log('send_all from {} to {}'.format(self.server_id, i))
                reply = self.send_msg(i, payload)
                if reply is not None:
                    replies.append((i, reply))
        return replies

    # check whether this server is the current leader
    def is_leader (self):
        return self.leader == self.server_id

    def notify(self):
        with self.handler_lock:
            if self.waiting == W_OK:
                return
            if time.time() > self.waiting_settime + WAITING_TIMEOUT:
                if self.waiting == W_WAITING_PREPARE_REPLIES:
                    self.send_prepare(*self.wait_args)
                elif self.waiting == W_WAITING_ACCEPT_REPLIES:
                    self.send_accept(*self.wait_args)
                else:
                    assert False, "invalid waiting: {}".format(self.waiting)

    def elect_leader(self):
        with self.handler_lock:
            if self.leader not in self.failed_nodes:
                self.failed_nodes.add(self.leader)
            assert not self.is_electing
            self.is_electing = True
            idx = self.get_next_instance_idx()
            data = {
                'instance': idx,
                'command': S_ELECT_LEADER(self.server_id)
            }
            assert idx not in self.progress
            self.init_progress(idx)
            data['proposal_n'] = self.bump_next_proposal_n(idx)
            self.send_prepare(data)

    def log (self, msg, verbose=False):
        if (not VERBOSE) and verbose:
            return
        print('[S{}] {}'.format(self.server_id, msg))

class LockHandler(BaseRequestHandler):
    '''
    Override the handle method to handle each request
    '''
    def handle (self):
        self.server.log("acquiring...")
        with self.server.handler_lock:
            data = self.request.recv(1024).strip().decode('utf-8')
            self.server.log('Received message {}'.format(data), verbose=True)
            try:
                data = json.loads(data)
                if data['source'] == 'client':
                    reply = self.server.handle_client(data)
                    self.request.sendall(json.dumps(reply).encode('utf-8'))
                else:
                    self.server.receive_msg(self.request, data)
            except ValueError:
                self.server.log('Could not parse the data as JSON: {}'.format(data))
            finally:
                # close the connection because everything is async
                self.request.close()
            self.server.log("LockHandler.handle done")

# start server
def start (sid, total):
    server = LockServer(sid, total)
    heartbeat_server_thr = HeartBeatRecvThread(sid, server)
    heartbeat_sender_thr = HeartBeatSenderThread(server)
    heartbeat_checker_thr = HeartBeatCheckerThread(heartbeat_server_thr.server, server)
    heartbeat_server_thr.start()
    heartbeat_sender_thr.start()
    heartbeat_checker_thr.start()
    # handle signal
    def sig_handler (signum, frame):
        heartbeat_checker_thr.stopped = True
        heartbeat_sender_thr.stopped = True
        heartbeat_checker_thr.join()
        heartbeat_sender_thr.join()
        heartbeat_server_thr.server.shutdown()
        heartbeat_server_thr.server.server_close()
        heartbeat_server_thr.join()
        server.server_close()
        exit(0)

    # register signal handler
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    # serve until explicit shutdown
    ip, port = server.server_address
    server.log('Listening on {}:{} ...'.format(ip, port))
    server.serve_forever()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('Usage:\npython paxos_server.py [server_id] [total_nodes] [-v]')
        exit(0)
    if len(sys.argv) == 4 and sys.argv[3] == "-v":
        VERBOSE = True
    else:
        VERBOSE = False

    start(int(sys.argv[1]), int(sys.argv[2]))
