import json
import sys
import time
import Pyro5.api
import threading
import random

class Node:
    def __init__(self, node_id, state='follower', current_term=0, voted_for=None, log=None, commit_index=-1, last_applied=-1, friends=None, daemon=None, uri=None, election_timeout=random.uniform(1, 10)):
        self.node_id = node_id
        self.state = state
        self.current_term = current_term
        self.voted_for = voted_for
        self.log = log if log is not None else []
        self.commit_index = commit_index
        self.last_applied = last_applied
        self.friends = friends if friends is not None else {}
        self.valid_friends = friends if friends is not None else {}
        self.daemon = daemon
        self.uri = uri
        self.election_timeout = election_timeout
        self.term = 0
        self.voted_for = None
        self.votes = 0
        self.received_heartbeat = False
        self.nextIndex = {}
        self.matchIndex = {}

        with open(f'nodes_settings.json', 'r') as f:
            json_data = json.load(f)

        node = json_data[node_id]
        self.node_id = node['id']
        self.host = node['host']
        self.port = node['port']

        for id, node in json_data.items():
            if node['id'] != self.node_id:
                self.friends[node['id']] = node['uri']

        self.valid_friends = self.friends.copy()

        self.daemon = Pyro5.api.Daemon(host=self.host, port=self.port)
        self.uri = self.daemon.register(self, objectId=self.node_id)

        print(f"Node {self.node_id} is ready. Object uri =", self.uri)

        time.sleep(10)

        self.start_daemon()
        self.election_timer()

    def start_daemon(self):
        t = threading.Thread(target=self.daemon.requestLoop)
        t.daemon = True
        t.start()

    def last_log_index(self):
        return len(self.log) - 1
    
    def last_log_term(self):
        if self.log:
            return self.log[-1]['term']
        return 0

    @Pyro5.api.expose
    def request_vote(self, candidate_id, term, last_log_index, last_log_term):
        if candidate_id not in self.valid_friends and candidate_id != self.node_id:
            with open(f'nodes_settings.json', 'r') as f:
                json_data = json.load(f)
            node = json_data[candidate_id]
            self.valid_friends[candidate_id] = node['uri']
        
        if term > self.current_term:
            self.current_term = term
            self.state = 'follower'
            self.voted_for = None

        if term < self.current_term:
            return False

        if (self.voted_for is None or self.voted_for == candidate_id) and (last_log_term > self.last_log_term() or (last_log_term == self.last_log_term() and last_log_index >= self.last_log_index())):
            self.voted_for = candidate_id
            self.received_heartbeat = True
            print(f"{self.node_id} voted for {candidate_id} in term {term}")
            return True

        return False

    def election_timer(self):
        while True:
            self.election_timeout = random.uniform(1, 10)
            start = time.time()

            while time.time() - start < self.election_timeout:
                time.sleep(0.1)
                
                if self.received_heartbeat:
                    self.received_heartbeat = False
                    start = time.time()

            if self.state != "leader":
                self.state = 'candidate'
                self.election_term()

    def election_term(self):
        self.current_term += 1
        self.votes = 1
        self.voted_for = self.node_id

        print(f"Node {self.node_id} is starting an election for term {self.current_term}")

        for friend_id, friend_uri in self.friends.items():
            try:
                with Pyro5.api.Proxy(friend_uri) as proxy:
                    proxy._pyroTimeout = 0.5
                    if proxy.request_vote(self.node_id, self.current_term, self.last_log_index(), self.last_log_term()):
                        print(f"{self.node_id} received vote from {friend_id}")
                        self.votes += 1
            
            except (Pyro5.errors.TimeoutError, Pyro5.errors.CommunicationError):
                print(f"Timeout while requesting vote from {friend_id}")
                if friend_id in self.valid_friends:
                    del self.valid_friends[friend_id]
            
            except Exception as e:
                print(f"Failed to request vote from {friend_id}: {e}")
                

        if self.votes > (len(self.valid_friends) + 1) // 2 or len(self.valid_friends) == 0:
            self.state = 'leader'
            print(f"Node {self.node_id} became the leader for term {self.current_term}")
            
            for nid in self.valid_friends:
                self.nextIndex[nid] = self.last_log_index() + 1
                self.matchIndex[nid] = -1

            ns = Pyro5.api.locate_ns()
            ns.register("Leader", self.uri) 
            self.send_heartbeat()
            
        else:
            self.state = 'follower'

    def send_heartbeat(self):
        for friend_id, friend_uri in self.friends.items():
            t = threading.Thread(target=self.heartbeat, args=(friend_id, friend_uri))
            t.start()

    def heartbeat(self, friend_id, friend_uri):
        while self.state == 'leader':
            if friend_id not in self.valid_friends:
                try:
                    with Pyro5.api.Proxy(friend_uri) as proxy:
                        proxy._pyroTimeout = 0.5
                        proxy.append_entry(self.node_id, self.current_term, -1, 0, [], self.commit_index)
                        self.valid_friends[friend_id] = friend_uri
                        self.nextIndex[friend_id] = 0
                        self.matchIndex[friend_id] = -1
                        print(f"{friend_id} is back online. Syncing log...")
                        self.handle_log_consistency(friend_id, friend_uri) 
                except (Pyro5.errors.TimeoutError, Pyro5.errors.CommunicationError):
                    print(f"Timeout while checking {friend_id}. Still offline.")
                except Exception as e:
                    print(f"Failed to check {friend_id}: {e}")
                
                time.sleep(0.5)
            else:
                try: 
                    prev_log_index = self.nextIndex[friend_id] - 1

                    if prev_log_index >= 0:
                        prev_log_term = self.log[prev_log_index]['term']
                    else:
                        prev_log_term = 0
                    
                    with Pyro5.api.Proxy(friend_uri) as proxy:
                        proxy._pyroTimeout = 0.5
                        proxy.append_entry(self.node_id, self.current_term, prev_log_index, prev_log_term, [], self.commit_index)
                
                except (Pyro5.errors.TimeoutError, Pyro5.errors.CommunicationError):
                    print(f"Timeout while sending heartbeat to {friend_id}")
                    
                    if friend_id in self.valid_friends:
                        del self.valid_friends[friend_id]
                
                except Exception as e:
                    print(f"Failed to send heartbeat to {friend_id}: {e}")     

                time.sleep(0.25)

    @Pyro5.api.expose
    def append_entry(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None

        elif term < self.current_term:
            return False
        
        self.state = 'follower'
        self.received_heartbeat = True

        if prev_log_index >= 0:
            if prev_log_index >= len(self.log) or self.log[prev_log_index]['term'] != prev_log_term:
                return False
                    
        if not entries:
            print(f"{self.node_id} received heartbeat from {leader_id}")
        
        else:
            self.log = self.log[:prev_log_index + 1] + entries

        if leader_commit > self.commit_index:
            self.commit_entries(min(leader_commit, len(self.log) - 1))

        print(self.log)

        return True

    def replicate_log_entries(self, entry):
        if self.state != "leader":
            return False

        acks = 1

        for friend_id, friend_uri in self.valid_friends.items():
            try:
                prev_log_index = self.nextIndex[friend_id] - 1
                if prev_log_index >= 0:
                    prev_log_term = self.log[prev_log_index]['term']
                else:
                    prev_log_term = 0
                
                with Pyro5.api.Proxy(friend_uri) as proxy:
                    proxy._pyroTimeout = 0.5
                    response = proxy.append_entry(self.node_id, self.current_term, prev_log_index, prev_log_term, [entry], self.commit_index)
                    
                    if response:
                        self.matchIndex[friend_id] = prev_log_index + 1
                        self.nextIndex[friend_id] = self.matchIndex[friend_id] + 1
                        acks += 1
                    else:
                        threading.Thread(target=self.handle_log_consistency, args=(friend_id, friend_uri), daemon=True).start()
            
            except Exception as e:
                print(f"Failed to replicate log entry to {friend_id}: {e}")

        total_nodes = len(self.valid_friends) + 1
        
        if acks > total_nodes // 2:            
            self.commit_entries(prev_log_index + 1)
            print(f"{self.node_id} committed log entry: {entry}")
            
            for friend_id, friend_uri in self.valid_friends.items():
                try:
                    with Pyro5.api.Proxy(friend_uri) as proxy:
                        proxy.commit_entries(self.commit_index)
                except Exception as e:
                    print(f"Failed to send commit index to {friend_id}: {e}")
            
            return True
        else:   
            print(f"{self.node_id} failed to commit log entry: {entry}")
            return False

    def handle_log_consistency(self, friend_id, friend_uri):
        while self.state == 'leader':
            with Pyro5.api.Proxy(friend_uri) as proxy:
                self.nextIndex[friend_id] = max(0, self.nextIndex[friend_id] - 1)
                new_idx = self.nextIndex[friend_id] - 1 
                if new_idx >= 0: 
                    term = self.log[new_idx]['term']
                else: 
                    term = 0

                entries_missing = self.log[self.nextIndex[friend_id]:]
                try:
                    response = proxy.append_entry(self.node_id, self.current_term, new_idx, term, entries_missing, self.commit_index)
                    if response:
                        self.matchIndex[friend_id] = len(self.log) - 1
                        self.nextIndex[friend_id] = len(self.log)
                        proxy.commit_entries(self.commit_index)
                        break
                except Exception as e:
                    print(f"Failed to handle log consistency with {friend_id}: {e}")
                    break 
                    
                if self.nextIndex[friend_id] <= 0:
                    break

    @Pyro5.api.expose
    def commit_entries(self, commit_index):
        if commit_index > self.commit_index:
            self.commit_index = commit_index
            
            while (self.last_applied < self.commit_index) and (self.last_applied + 1 < len(self.log)):
                self.last_applied += 1
                entry = self.log[self.last_applied]
                print(f"{self.node_id} applied log entry: {entry}")

            print(f"{self.node_id} updated commit index to {commit_index}")

    @Pyro5.api.expose
    def client_request(self, command):
        if self.state != 'leader':
            return False

        entry = {'term': self.current_term, 'command': command}
        
        self.log.append(entry)
        if self.replicate_log_entries(entry): 
            return "Command committed"
        else:
            return "Failed to commit command"

if __name__ == "__main__":
    node_id = sys.argv[1]
    if node_id not in ["node1", "node2", "node3", "node4"]:
        node_id = "node" + input("Enter the node ID: ")
    node = Node(node_id)