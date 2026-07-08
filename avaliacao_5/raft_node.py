import json
import sys
import time
import threading
import random
import grpc
import os
from concurrent import futures

import raft_pb2
import raft_pb2_grpc

CLUSTER_SIZE = 4

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def __init__(self, node):
        self.node = node

    def request_vote(self, request, context):
        vote = self.node.request_vote(request.candidate_id, request.term, request.last_log_index, request.last_log_term)
        return raft_pb2.VoteResponse(vote=vote)

    def append_entry(self, request, context):
        entries = [{"term": e.term, "command": e.command, "committed": e.committed} for e in request.entries]
        response, conflict_index, conflict_term = self.node.append_entry(request.leader_id, request.term, request.prev_log_index, request.prev_log_term, entries, request.leader_commit)
        return raft_pb2.AppendEntryResponse(response=response, conflict_index=conflict_index, conflict_term=conflict_term)

    def commit_entries(self, request, context):
        self.node.commit_entries(request.commit_index)
        return raft_pb2.CommitResponse(response=True)
    
class ClientServicer(raft_pb2_grpc.ClientServicer):
    def __init__(self, node):
        self.node = node

    def client_put(self, request, context):
        status, value = self.node.client_put(request.key, request.value)
        return raft_pb2.ClientPutResponse(status=status, value=value)

    def client_get(self, request, context):
        status, value = self.node.client_get(request.key)
        return raft_pb2.ClientGetResponse(status=status, value=value)

class Node:
    def __init__(self, node_id, state='follower', current_term=0, voted_for=None, log=None, commit_index=-1, last_applied=-1, friends=None, daemon=None, election_timeout=random.uniform(1, 10)):
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
        self.election_timeout = election_timeout
        self.term = 0
        self.voted_for = None
        self.votes = 0
        self.received_heartbeat = False
        self.nextIndex = {}
        self.matchIndex = {}
        self.leader_address_host = None
        self.leader_address_port = None
        self.state = 'follower'

        with open(f'nodes_settings.json', 'r') as f:
            json_data = json.load(f)

        node = json_data[node_id]
        self.node_id = node['id']
        self.host = node['host']
        self.port = node['port']

        for id, node in json_data.items():
            if node['id'] != self.node_id:
                self.friends[node['id']] = f'{node['host']}:{node['port']}'

        self.valid_friends = self.friends.copy()

        os.makedirs("nodes_info", exist_ok=True)
        self.state_file = f"nodes_info/{self.node_id}.json"
        self.load_state()

        self.raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(self), self.raft_server)
        self.raft_server.add_insecure_port(f'{self.host}:{self.port}')
        self.raft_server.start()

        self.client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_ClientServicer_to_server(ClientServicer(self), self.client_server)
        self.client_server.add_insecure_port(f'{self.host}:{self.port - 1000}')
        self.client_server.start()

        print(f"Node {self.node_id} is ready. Object address = {self.host}:{self.port}")

        time.sleep(10)

        self.election_timer()

    def save_state(self):
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log": self.log
        }
        with open(self.state_file, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

    def load_state(self):
        if not os.path.exists(self.state_file):
            return
            
        with open(self.state_file, 'r') as file:
            data = json.load(file)
        self.current_term = data.get("current_term", 0)
        self.voted_for = data.get("voted_for", None)
        self.commit_index = data.get("commit_index", -1)
        self.last_applied = data.get("last_applied", -1)
        self.log = data.get("log", [])

        print(f"{self.node_id} recovered persisted state")

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
            self.save_state()

        if term < self.current_term:
            return False

        if self.log:
            mylast_log_term = self.log[-1]['term']
        else:
            mylast_log_term = 0

        if (self.voted_for is None or self.voted_for == candidate_id) and (last_log_term > mylast_log_term or (last_log_term == mylast_log_term and last_log_index >= len(self.log) - 1)):
            self.voted_for = candidate_id
            self.received_heartbeat = True
            self.save_state()
            print(f"{self.node_id} voted for {candidate_id} in term {term}")
            return True

        return False

    def election_timer(self):
        while True:
            self.election_timeout = random.uniform(1, 10)
            start = time.time()

            while time.time() - start < self.election_timeout:                
                if self.received_heartbeat:
                    self.received_heartbeat = False
                    start = time.time()

                time.sleep(0.1)

            if self.state != "leader":
                self.state = 'candidate'
                self.election_term()

    def election_term(self):
        self.current_term += 1
        self.votes = 1
        self.voted_for = self.node_id
        self.save_state()

        if self.log:
            mylast_log_term = self.log[-1]['term']
        else:
            mylast_log_term = 0

        if self.state == 'candidate':
            print(f"Node {self.node_id} is starting an election for term {self.current_term}")

            for friend_id, friend_address in self.friends.items():
                try:
                    channel = grpc.insecure_channel(friend_address)
                    stub = raft_pb2_grpc.RaftStub(channel)
                    req = raft_pb2.VoteRequest(candidate_id=self.node_id, term=self.current_term, last_log_index=len(self.log) - 1, last_log_term=mylast_log_term)
                    if stub.request_vote(req, timeout=0.5):
                        print(f"{self.node_id} received vote from {friend_id}")
                        self.votes += 1
                    channel.close()
                
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        print(f"Timeout while requesting vote from {friend_id}")
                        if friend_id in self.valid_friends:
                            del self.valid_friends[friend_id]
                    else:
                        print(f"Failed to request vote from {friend_id}: {e}")
                
                except Exception as e:
                    print(f"Failed to request vote from {friend_id}: {e}")
                    

            if self.votes > (CLUSTER_SIZE + 1) // 2:
                self.state = 'leader'
                print(f"Node {self.node_id} became the leader for term {self.current_term}")
                self.save_state()

                self.valid_friends = self.friends.copy()
                for nid in self.friends:
                    self.nextIndex[nid] = len(self.log)
                    self.matchIndex[nid] = -1

                self.leader_address_host = self.host
                self.leader_address_port = self.port - 1000
                self.send_heartbeat()
                
            else:
                self.state = 'follower'

    def send_heartbeat(self):
        for friend_id, friend_address in self.friends.items():
            t = threading.Thread(target=self.heartbeat, args=(friend_id, friend_address), daemon=True)
            t.start()

    def heartbeat(self, friend_id, friend_address):
        while self.state == 'leader':
            try: 
                prev_log_index = self.nextIndex[friend_id] - 1

                if prev_log_index >= 0:
                    prev_log_term = self.log[prev_log_index]['term']
                else:
                    prev_log_term = 0
                
                entries_to_send = self.log[self.nextIndex[friend_id]:]
                if entries_to_send:
                    print(f"Log entries pending for {friend_id}: {entries_to_send}")
                channel = grpc.insecure_channel(friend_address)
                stub = raft_pb2_grpc.RaftStub(channel)
                req = raft_pb2.AppendEntryRequest(leader_id=self.node_id, term=self.current_term, prev_log_index=prev_log_index, prev_log_term=prev_log_term, entries=entries_to_send, leader_commit=self.commit_index)
                response = stub.append_entry(req, timeout=0.5)
                if entries_to_send and response.response:
                    #self.handle_log_consistency(friend_id, friend_address, response.conflict_index, response.conflict_term)
                    self.matchIndex[friend_id] = prev_log_index + len(entries_to_send)
                    self.nextIndex[friend_id] = self.matchIndex[friend_id] + 1
                    self.valid_friends[friend_id] = friend_address
                    if (len(self.valid_friends) == 1 or len(self.valid_friends) == 2) and self.commit_index < len(self.log) - 1:
                        self.commit_entries(len(self.log) - 1)
                if not response.response:
                    print(f"{friend_id} is not in sync. Syncing log...")
                    self.handle_log_consistency(friend_id, friend_address, response.conflict_index, response.conflict_term)
                    if (len(self.valid_friends) == 1 or len(self.valid_friends) == 2) and self.commit_index < len(self.log) - 1:
                        self.commit_entries(len(self.log) - 1)
                channel.close()
            
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    print(f"Timeout while checking {friend_id}. Still offline.")
                else:
                    print(f"Error while checking {friend_id}: {e}. Still offline.")
                
                if friend_id in self.valid_friends:
                    del self.valid_friends[friend_id]
            
            except Exception as e:
                print(f"Failed to send heartbeat to {friend_id}: {e}")     

            time.sleep(0.25)

    def append_entry(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = 'follower'

        elif term < self.current_term:
            print(1)
            return False, -1, -1
        
        self.state = 'follower'
        self.received_heartbeat = True

        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                print(2)
                return False, len(self.log), self.log[-1]['term'] if self.log else 0
            
            if self.log[prev_log_index]['term'] != prev_log_term:
                print(3)
                conflict_term = self.log[prev_log_index]['term']
                conflict_index = prev_log_index
                while conflict_index > 0 and self.log[conflict_index - 1]['term'] == conflict_term:
                    conflict_index -= 1
                self.log = self.log[:conflict_index]
                self.save_state()
                return False, conflict_index, conflict_term
            
        if not entries:
            print(f"{self.node_id} received heartbeat from {leader_id}")
            self.leader_address_host = self.friends[leader_id].split(':')[0]
            self.leader_address_port = int(self.friends[leader_id].split(':')[1]) - 1000
        
        else:
            self.log = self.log[:prev_log_index + 1] + entries
            self.save_state()
            print("Entry replicated.")

        if leader_commit > self.commit_index:
            self.commit_entries(min(leader_commit, len(self.log) - 1))

        print(self.log)

        return True, -1, -1
    
    def replicate_log_entries(self, entry):
        if self.state != "leader":
            return False

        acks = 1

        for friend_id, friend_address in self.valid_friends.items():
            if friend_id == self.node_id:
                continue
            try:
                prev_log_index = self.nextIndex[friend_id] - 1
                if prev_log_index >= 0:
                    prev_log_term = self.log[prev_log_index]['term']
                else:
                    prev_log_term = 0

                channel = grpc.insecure_channel(friend_address)
                stub = raft_pb2_grpc.RaftStub(channel)
                req = raft_pb2.AppendEntryRequest(leader_id=self.node_id, term=self.current_term, prev_log_index=prev_log_index, prev_log_term=prev_log_term, entries=[entry], leader_commit=self.commit_index)
                response = stub.append_entry(req, timeout=0.5)
                channel.close()
                             
                if response.response:
                    self.matchIndex[friend_id] = prev_log_index + 1
                    self.nextIndex[friend_id] = self.matchIndex[friend_id] + 1
                    acks += 1
                    print(f"{friend_id} replicated log entry: {entry}")
                else:
                    threading.Thread(target=self.handle_log_consistency, args=(friend_id, friend_address, response.conflict_index, response.conflict_term), daemon=True).start()
            
            except Exception as e:
                print(f"Failed to replicate log entry to {friend_id}: {e}")

        total_nodes = CLUSTER_SIZE + 1
        
        if acks > total_nodes // 2:            
            self.commit_entries(prev_log_index + 1)
            for friend_id, friend_address in self.friends.items():
                try:
                    channel = grpc.insecure_channel(friend_address)
                    stub = raft_pb2_grpc.RaftStub(channel)
                    req = raft_pb2.CommitRequest(commit_index=self.commit_index)
                    stub.commit_entries(req, timeout=0.5)
                    channel.close()
                except Exception as e:
                    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        print(f"Timeout while sending commit index to {friend_id}")
                    print(f"Failed to send commit index to {friend_id}: {e}")
            return True
        else:   
            print(f"{self.node_id} failed to commit log entry: {entry}")
            return False

    def handle_log_consistency(self, friend_id, friend_address, conflict_index, conflict_term):
        while self.state == 'leader':
            print(conflict_index, conflict_term)
            self.nextIndex[friend_id] = max(0, conflict_index)          
            prev_log_index = self.nextIndex[friend_id] - 1              
            prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0

            entries_missing = self.log[self.nextIndex[friend_id]:]     
            print(f"Entries missing for {friend_id}: {entries_missing}")
            try:
                channel = grpc.insecure_channel(friend_address)
                stub = raft_pb2_grpc.RaftStub(channel)
                req = raft_pb2.AppendEntryRequest(leader_id=self.node_id, term=self.current_term, prev_log_index=prev_log_index, prev_log_term=prev_log_term, entries=entries_missing, leader_commit=self.commit_index)
                response = stub.append_entry(req, timeout=0.5)
                if response.response:
                    self.matchIndex[friend_id] = len(self.log) - 1
                    self.nextIndex[friend_id] = len(self.log)
                    req = raft_pb2.CommitRequest(commit_index=self.commit_index)
                    stub.commit_entries(req, timeout=0.5)
                    channel.close()    
                    break
                channel.close()
            except Exception as e:
                print(f"Failed to handle log consistency with {friend_id}: {e}")
                break 
                
            if self.nextIndex[friend_id] <= 0:
                break

    def commit_entries(self, commit_index):
        if commit_index > self.commit_index:
            self.commit_index = commit_index
            
            while (self.last_applied < self.commit_index) and (self.last_applied + 1 < len(self.log)):
                self.last_applied += 1
                entry = self.log[self.last_applied]
                entry['committed'] = True
                print(f"{self.node_id} committed log entry: {entry}")
            
            self.save_state()

            print(f"{self.node_id} updated commit index to {commit_index}")

    def client_put(self, key, value):
        if self.state != 'leader':
            return "Not the leader", f"{self.leader_address_host}:{self.leader_address_port}"

        entry = {'term': self.current_term, 'command': f'{key}={value}', 'committed': False}
        
        self.log.append(entry)
        self.save_state()
        if self.replicate_log_entries(entry): 
            return "Command committed", entry['command']
        else:
            return "Failed to commit command", ""

    def client_get(self, key):
        committed_log = self.log[:self.commit_index + 1]

        for entry in reversed(committed_log):
            command = entry.get('command', '')
            stored_key, separator, stored_value = command.partition('=')
            if separator and stored_key == key:
                return "OK", stored_value

        return "Not found", ""

if __name__ == "__main__":
    node_id = sys.argv[1]
    if node_id not in ["node1", "node2", "node3", "node4"]:
        node_id = "node" + input("Enter the node ID: ")
    node = Node(node_id)