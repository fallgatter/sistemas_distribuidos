@echo off

cmd /c start "Name Server" cmd /k python -m Pyro5.nameserver        
cmd /c start "Node 1" cmd /k python raft_node.py node1
cmd /c start "Node 2" cmd /k python raft_node.py node2
cmd /c start "Node 3" cmd /k python raft_node.py node3
cmd /c start "Node 4" cmd /k python raft_node.py node4
cmd /c start "Cliente" cmd /k python cliente.py