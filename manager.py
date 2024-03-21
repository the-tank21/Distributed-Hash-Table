# Manager process for socket project
# Author: Ethan Hodge

import socket
from dataclasses import dataclass
import pickle

# Define variables
peer_list = []
dht_list = []
dht_state = "NOT CREATED"
host = ''
port_number = 0

@dataclass
class Peer:
    peer_name: str
    addr: str
    m_port: int
    p_port: int
    state: str = "Free"

    def print(self):
        print("Peer Name: " + self.peer_name + ", Address: " + self.addr + ", State: " + self.state)


# Define register function
def register(peer_name, addr, m_port, p_port):
    # Check for any duplicate data in the peer list
    for peer in peer_list:
        if peer.peer_name == peer_name or peer.m_port == m_port or peer.p_port == p_port:
            return "FAILURE register"
    # Add peer to peer list
    peer = Peer(peer_name, addr, m_port, p_port)
    peer_list.append(peer)
    return "SUCCESS register"

# Define setup_dht function
def setup_dht(peer_name, num, year):
    global dht_state
    global dht_list
    dht_list.clear()
    num = int(num)
    contains_name = False
    in_dht = 0
    if num < 3:
        return [], "FAILURE setup-dht"
    if len(peer_list) < num:
        return [], "FAILURE setup-dht"
    if dht_state == "CREATED":
        return [], "FAILURE setup-dht"
    for peer in peer_list:
        if peer.peer_name == peer_name:
            contains_name = True
            peer.state = "Leader"
            dht_list.append((peer.peer_name, peer.addr, peer.p_port))
    if (contains_name == False):
        return [], "FAILURE setup-dht"
    for peer in peer_list:
        if peer.state == "Free":
            peer.state = "InDHT"
            in_dht += 1
            dht_list.append((peer.peer_name, peer.addr, peer.p_port))
        if in_dht == num - 1:
            break
    dht_state = "IN PROGRESS"
    return dht_list, "SUCCESS setup-dht"

# Define dht_complete function
def dht_complete(peer_name):
    global dht_state
    #if peer_name != dht_list[0][0]:
    #   return "FAILURE dht-complete"
    dht_state = "CREATED"
    return "SUCCESS dht-complete"

# main function
def main():
    global dht_list, dht_state
    # Create and bind socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((host, port_number))

    # Run server
    while True:
    # Wait for command
        print("Waiting for command...")
        data, address = s.recvfrom(1024)
    # Identify type of command
        data = data.decode('ascii').split()
        print(data)
        command = str(data[0])
        print("Command: " + command + " received.")
    # Execute command
        if command == "register":
            peer_name = data[1]
            addr = data[2]
            m_port = int(data[3])
            p_port = int(data[4])
            response = register(peer_name, addr, m_port, p_port)
            print(response)
            s.sendto(response.encode('ascii'), address)
            for peer in peer_list:
                peer.print()
        if command == "setup-dht":
            peer_name = data[1]
            num = data[2]
            year = data[3]
            dht_list, response = setup_dht(peer_name, num, year)
            # address is a tuple returned from recvfrom()
            s.sendto(response.encode('ascii'), address)
            if response == "SUCCESS setup-dht":
                serialized_dht_list = pickle.dumps(dht_list)
                s.sendto(serialized_dht_list, address)
        if command == "dht-complete":
            response = dht_complete(data[1])
            s.sendto(response.encode('ascii'), address)
        if command == "teardown-dht":
            print(dht_list)
            if data[1] != dht_list[0][0]:
                s.sendto("FAILURE teardown-dht".encode('ascii'), address)
            else: 
                dht_state = "IN PROGRESS"
                for peer in peer_list:
                    peer.state = "Free"
                s.sendto("SUCCESS teardown-dht".encode('ascii'), address)
            print("Peer list:")
            for peer in peer_list:
                peer.print()
        if command == "teardown-complete":
            print(dht_list)
            if data[1] != dht_list[0][0]:
                s.sendto("FAILURE teardown-complete".encode('ascii'), address)
            else:
                dht_state = "NOT CREATED"
                dht_list.clear()
            print("Peer list:")
            for peer in peer_list:
                peer.print()
        if command == "deregister":
            for peer in peer_list:
                if peer.peer_name == data[1] and peer.state == "Free":
                    peer_list.remove(peer)
                    s.sendto("SUCCESS deregister".encode('ascii'), address)
                    break
                elif peer.peer_name == data[1] and peer.state != "Free":
                    s.sendto("FAILURE deregister".encode('ascii'), address)
                    break
            print("Peer list:")
            for peer in peer_list:
                peer.print()
        if command == "leave-dht":
            # Somehow made this O(n^2) but it's fine
            found = False
            for i in range(len(dht_list)):
                if dht_list[i][0] == data[1]:
                    dht_list.pop(i)
                    for peer in peer_list:
                        if peer.peer_name == data[1]:
                            peer.state = "Free"
                    s.sendto("SUCCESS leave-dht".encode('ascii'), address)
                    found = True
                    for peer in peer_list:
                        if peer.state == "Leader":
                            leader = peer
                            s.sendto("SUCCESS teardown-dht".encode('ascii'), (peer.addr, peer.m_port))
                            s.recv(1024)
                if found:
                    break
            print("Peer list:")
            for peer in peer_list:
                peer.print()
            if found:
                s.sendto("SUCCESS setup-dht".encode('ascii'), (leader.addr, leader.m_port))
                s.sendto(pickle.dumps(dht_list), (leader.addr, leader.m_port))
            else:
                s.sendto("FAILURE leave-dht".encode('ascii'), address)
        if command == "join-dht":
            if dht_state == "IN PROGRESS" or dht_state == "NOT CREATED":
                s.sendto("FAILURE join-dht".encode('ascii'), address)
                break
            for peer in peer_list:
                if peer.peer_name == data[1]:
                    if peer.state == "InDHT":
                        s.sendto("FAILURE join-dht".encode('ascii'), address)
                        break
                    dht_list.append((peer.peer_name, peer.addr, peer.p_port))
                    peer.state = "InDHT"
                    s.sendto("SUCCESS join-dht".encode('ascii'), address)
                    for peer2 in peer_list:
                        if peer2.state == "Leader":
                            leader = peer2
                            s.sendto("SUCCESS teardown-dht".encode('ascii'), (peer2.addr, peer2.m_port))
                            s.recv(1024)
                            break
                    break
            print("Peer list:")
            for peer in peer_list:
                peer.print()
            s.sendto("SUCCESS setup-dht".encode('ascii'), (leader.addr, leader.m_port))
            s.sendto(pickle.dumps(dht_list), (leader.addr, leader.m_port))
        if command == "query-dht":
            if dht_state != "CREATED":
                s.sendto("FAILURE query-dht".encode('ascii'), address)
                break
            found = False
            for peer in peer_list:
                if data[1] == peer.peer_name and peer.state == "Free":
                    s.sendto("SUCCESS query-dht".encode('ascii'), address)
                    s.sendto(pickle.dumps(dht_list[0]), address)
                    found = True
                    break
            if not found:
                s.sendto("FAILURE query-dht".encode('ascii'), address)


host = input("Enter the host address: ")
port_number = int(input("Enter the port number: "))       
main()
