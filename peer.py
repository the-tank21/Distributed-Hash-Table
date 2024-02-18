# Peer process for socket project
import socket
from dataclasses import dataclass
import pickle

# Define variables
peer_name = ''
peer_addr = ''
m_port = 0
p_port = 0
peer_state = "Free"
id = 0 # Identifier for each peer in the DHT ring
dht_list = [] # List of peers in the DHT ring, only delivered to leader

# Manager info
manager_addr = ""
manager_port = 0

# Neighbor info
neightbor_addr = ""
neighbor_port = 0

# Define peer functions

def register(peer_name, addr, m_port, p_port):
    # Send register command to manager with peer info
    message = "register " + peer_name + " " + addr + " " + m_port + " " + p_port
    return message

def setup_dht(peer_name, num, year):
    message = "setup-dht " + peer_name + " " + num + " " + year
    return message


# Define main loop and response to each function
def main():
    peer_addr = input("Enter host address: ")
    m_port = int(input("Enter peer-manager port: "))
    p_port = int(input("Enter peer-peer port: "))
    # Create socket for peer-peer communication
    p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p_socket.bind((peer_addr, p_port))
    # Store address of manager
    manager_addr = input("Enter manager address: ")
    manager_port = int(input("Enter manager port: "))
    # Create socket for peer-manager communication
    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    m_socket.bind((peer_addr, m_port))


    while True:
        # Read command from stdin
        command = input("Enter command: ")
        command = command.split(" ")
        print(command)
        if command[0] == "register":
            print("Registering peer...")
            peer_name = command[1]
            message = register(command[1], command[2], command[3], command[4])
            m_socket.sendto(message.encode('utf-8'), (manager_addr, manager_port))
            response, address = m_socket.recvfrom(1024)
            response = response.decode('utf-8')
            print(response)
        if command[0] == "setup-dht":
            message = setup_dht(command[1], command[2], command[3])
            m_socket.sendto(message.encode('utf-8'), (manager_addr, manager_port))
            print("Waiting for response...")
            response, address = m_socket.recvfrom(1024)
            response = response.decode('utf-8')
            print(response)
            if response == "SUCCESS":
                peer_state = "Leader"
                response = m_socket.recv(1024)
                dht_list = pickle.loads(response)
                print(dht_list)
                for i in range(len(dht_list)):
                    data = pickle.dumps((dht_list, i))
                    p_socket.sendto(data, (dht_list[i][1], dht_list[i][2]))
                # Set neighbor info
                neighbor_addr = dht_list[1][1]
                neighbor_port = dht_list[1][2]
                id = 0

        if command[0] == "join-dht":
            # Wait for message from dht leader
            print("Waiting for DHT leader...")
            dht_list, id = pickle.loads(p_socket.recv(1024))
            # Check to make sure list and id are correct
            print(dht_list)
            print("DHT ID:", id)
            peer_state = "InDHT"
            # Set neighbor info
            if id < len(dht_list) - 1:
                neighbor_addr = dht_list[id+1][1]
                neighbor_port = dht_list[id+1][2]
            else: 
                neighbor_addr = dht_list[0][1]
                neighbor_port = dht_list[0][2]
            # Check neighbor info
            print("Neighbor: " + neighbor_addr + " " + str(neighbor_port))
            


main()

    