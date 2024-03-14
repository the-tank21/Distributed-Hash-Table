# New version of peer program with threading for multiple connections

import socket, pickle, csv, os, fnmatch, math, threading

global peer_name, peer_addr, m_port, p_port
global manager_addr, manager_port
global m_socket, p_socket
global neighbor_addr, neighbor_port
global id, dht_list, hash_table

# Define peer functions
def register(peer_name, addr, m_port, p_port):
    # Send register command to manager with peer info
    message = "register " + peer_name + " " + addr + " " + m_port + " " + p_port
    return message

def setup_dht(peer_name, num, year):
    message = "setup-dht " + peer_name + " " + num + " " + year
    return message

# Function used to find the size of the hash table
def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(math.sqrt(n))):
        if n % i == 0:
            return False
    return True

def next_largest_prime(n):
    n += 1
    while not is_prime(n):
        n += 1
    return n

# Define threads for different inputs
def manager_thread():
    global m_socket
    while True:
        print("Listening on manager socket...")
        message = m_socket.recv(1024).decode()
        message = message.split()
        command = message[0]
        print(command)
        # Check if message is receipt of different command
        if command == 'SUCCESS':
            print(message[1] + " successful")
        elif command == 'FAILURE':
            print(message[1] + " failed")

def peer_thread():
    global p_socket
    while True:
        print("Listening on peer socket...")
        message = p_socket.recv(1024).decode()
        command = message.split()
        print(command)
        command = message[0]


def stdio_thread():
    global peer_name, peer_addr, m_port, p_port
    global m_socket, p_socket
    while True:
        message = input("Enter command: ").split()
        command = message[0]
        if command == "register": # Register with manager
            if len(message) != 5:
                print("Invalid command")
            else:
                peer_name = message[1]
                peer_addr = message[2]
                m_port = int(message[3])
                p_port = int(message[4])
                m_socket.bind((peer_addr, m_port))
                p_socket.bind((peer_addr, p_port))
                m_socket.sendto(register(peer_name, peer_addr, str(m_port), str(p_port)).encode(), (manager_addr, manager_port))
                

def main():
    # Set up manager connection
    global manager_addr, manager_port
    global m_socket, p_socket
    manager_addr = input("Enter manager address: ")
    manager_port = int(input("Enter manager port: "))
    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


    # Set up threads for listening and stdio
    t1 = threading.Thread(target=manager_thread)
    t2 = threading.Thread(target=peer_thread)
    t3 = threading.Thread(target=stdio_thread)
    t1.start()
    t2.start()
    t3.start()

    # Join threads
    t1.join()
    t2.join()
    t3.join()

main()