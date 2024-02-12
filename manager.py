# Manager process for socket project
# Author: Ethan Hodge

import socket
from dataclasses import dataclass

# Define variables
peer_list = []
dht_state = False

@dataclass
class Peer:
    peer_name: str
    addr: str
    m_port: int
    p_port: int
    state: str = "Free"

# Define register function
def register(peer_name, addr, m_port, p_port):
    # Check for any duplicate data in the peer list
    for peer in peer_list:
        if peer.peer_name == peer_name or peer.addr == addr or peer.m_port == m_port or peer.p_port == p_port:
            return "FAILURE"
    # Add peer to peer list
    peer = Peer(peer_name, addr, m_port, p_port)
    peer_list.append(peer)
    dht_state = True
    return "SUCCESS"

# Define setup_dht function
def setup_dht(peer_name, num, year):
    contains_name = False
    in_dht = 0
    dht_list = []
    if num < 3:
        return "FAILURE"
    if peer_list.length < num:
        return "FAILURE"
    if dht_state == True:
        return "FAILURE"
    for peer in peer_list:
        if peer.peer_name == peer_name:
            contains_name = True
            peer.state = "Leader"
            dht_list.append((peer.peer_name, peer.addr, peer.p_port))
    if (contains_name == False):
        return "FAILURE"
    while in_dht < num - 1:
        for peer in peer_list:
            if peer.state == "Free":
                peer.state = "InDHT"
                in_dht += 1
                dht_list.append((peer.peer_name, peer.addr, peer.p_port))
    return dht_list, "SUCCESS"

# Define dht_complete function


# main function
def main():
    # Create and bind socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    host = socket.gethostname()
    port_number = int(input("Enter port number between 14000 and 14500: "))
    while (port_number < 14000 or port_number > 14500):
        port_number = int(input("Enter port number between 14000 and 14500: "))
    s.bind((host, port_number))

    # Run server
    while True:
    # Listen for command
        data = s.recv(1024)
    # Identify type of command
        data.decode('utf-8').split(' ')
        command = data[0]
    # Execute command
        if command == "register":
            peer_name = data[1]
            addr = data[2]
            m_port = data[3]
            p_port = data[4]
            response = register(peer_name, addr, m_port, p_port)
            s.sendto(response.encode('utf-8'), (addr, m_port))
        if command == "setup_dht":
            peer_name = data[1]
            num = data[2]
            year = data[3]
            dht_list, response = setup_dht(peer_name, num, year)
            # send dht_list to leader, don't know how to get port it was sent from

