import json
import time
import Pyro5.api
import threading
import random

class Node:
    def __init__(self, node_id, state='follower', current_term=0, voted_for=None, log=None, commit_index=0, friends=None, daemon=None, uri=None, election_timeout=random.uniform(1, 10)):
        self.node_id = node_id
        self.state = state
        self.current_term = current_term
        self.voted_for = voted_for
        self.log = log if log is not None else []
        self.commit_index = commit_index
        self.friends = friends if friends is not None else {}
        self.daemon = daemon
        self.uri = uri
        self.election_timeout = election_timeout
        self.term = 0
        self.voted_for = None
        self.votes = 0
        self.received_heartbeat = False

        with open(f'nodes_settings.json', 'r') as f:
            json_data = json.load(f)

        node = json_data[node_id]
        self.node_id = node['id']
        self.host = node['host']
        self.port = node['port']

        for id, node in json_data.items():
            if node['id'] != self.node_id:
                self.friends[node['id']] = node['uri']

        print(self.friends)

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

    @Pyro5.api.expose
    def request_vote(self, candidate_id, term):
        if candidate_id not in self.friends and candidate_id != self.node_id:
            with open(f'nodes_settings.json', 'r') as f:
                json_data = json.load(f)
            node = json_data[candidate_id]
            self.friends[candidate_id] = node['uri']
        
        if term > self.current_term:
            self.current_term = term
            self.state = 'follower'
            self.voted_for = None

        if term < self.current_term:
            return False

        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            self.received_heartbeat = True
            print(f"Node {self.node_id} voted for {candidate_id} in term {term}")
            return True

        return False

    def election_timer(self):
        while True:
            start = time.time()

            while time.time() - start < self.election_timeout:
                time.sleep(0.1)
                if self.received_heartbeat:
                    self.received_heartbeat = False
                    start = time.time()

            self.state = 'candidate'

            self.election_term()

            if self.state == 'leader':
                break

    def election_term(self):
        self.current_term += 1
        self.votes = 1
        self.voted_for = self.node_id

        print(f"Node {self.node_id} is starting an election for term {self.current_term}")

        for friend_id, friend_uri in list(self.friends.items()):
            try:
                with Pyro5.api.Proxy(friend_uri) as proxy:
                    proxy._pyroTimeout = 0.5
                    if proxy.request_vote(self.node_id, self.current_term):
                        self.votes += 1
            except (Pyro5.errors.TimeoutError, Pyro5.errors.CommunicationError):
                print(f"Timeout while requesting vote from {friend_id}")
                if friend_id in self.friends:
                    del self.friends[friend_id]
            except Exception as e:
                print(f"Failed to request vote from {friend_id}: {e}")
                

        if self.votes > (len(self.friends) + 1) // 2 or len(self.friends) == 0:
            self.state = 'leader'
            print(f"Node {self.node_id} became the leader for term {self.current_term}")
            ns = Pyro5.api.locate_ns()
            ns.register("Leader", self.uri) 
            self.send_heartbeat()
            
        else:
            self.state = 'follower'

    def send_heartbeat(self):
        t = threading.Thread(target=self.heartbeat)
        t.start()

    def heartbeat(self):
        print(len(self.friends))
        while self.state == 'leader':
            for friend_id, friend_uri in list(self.friends.items()):
                try:
                    with Pyro5.api.Proxy(friend_uri) as proxy:
                        proxy._pyroTimeout = 0.5
                        proxy.append_entry(self.node_id, self.current_term)
                except (Pyro5.errors.TimeoutError, Pyro5.errors.CommunicationError):
                    print(f"Timeout while sending heartbeat to {friend_id}")
                    if friend_id in self.friends:
                        del self.friends[friend_id]
                except Exception as e:
                    print(f"Failed to send heartbeat to {friend_id}: {e}")     
            time.sleep(0.25)

    @Pyro5.api.expose
    def append_entry(self, node_id, term, entry=None):
        if term > self.current_term:
            self.current_term = term
            self.state = 'follower'
            self.received_heartbeat = True
            self.voted_for = None

        elif term < self.current_term:
            return False
        
        if entry is None:
            print(f"Heartbeat received from {node_id} in term {term}")
            self.received_heartbeat = True
        
        return True

if __name__ == "__main__":
    node_id = "node" + input("Enter the node ID: ")
    node = Node(node_id)