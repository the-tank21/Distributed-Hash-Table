# New version of peer program with threading for multiple connections

import socket, pickle, csv, os, fnmatch, math, threading, time

global peer_name, peer_addr, m_port, p_port
global manager_addr, manager_port
global m_socket, p_socket
global neighbor_addr, neighbor_port
global id, dht_list, hash_table, ht_size
hash_table = {}

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
    global m_socket, p_socket, dht_list, id, year, hash_table, neighbor_addr, neighbor_port, query_id, ht_size
    while True:
        print("Listening on manager socket...")
        message = m_socket.recv(1024).decode('ascii')
        message = message.split()
        print(message)
        # Check if message is receipt of different command
        if message[0] == 'FAILURE':
            print(message[1] + " failed")
        elif message[0] == 'SUCCESS':
            print(message[1] + " successful")
            if message[1] == 'setup-dht':
                print("Setting up DHT")
                dht_list = pickle.loads(m_socket.recv(1024))
                print(dht_list)
                id = 0
                neighbor_addr = dht_list[1][1]
                neighbor_port = dht_list[1][2]
                for i in range(1, len(dht_list)):
                    p_socket.sendto('Welcome to DHT'.encode('ascii'), (dht_list[i][1], int(dht_list[i][2])))
                    data = pickle.dumps((dht_list, i))
                    p_socket.sendto(data, (dht_list[i][1], int(dht_list[i][2])))
                # Open csv file and distrubute data
                files = os.listdir()
                csv_file = (fnmatch.filter(files, "*" + year + "*" + ".csv"))[0] # searches for csv file w/ year in name
                # Convert file into list of tuples
                entries = []
                print("Reading file...")
                with open(csv_file,'r') as file:
                    reader = csv.reader(file)
                    # Skip header
                    next(reader)
                    # Go through each row and add relevant data to list
                    for row in reader:
                        entries.append((row[7], row[8], row[10], row[11], row[12], row[13], row[15], row[20], row[21], row[22], row[23], row[24], row[25], row[31]))
                        print(row[7], row[8], row[10], row[11], row[12], row[13], row[15], row[20], row[21], row[22], row[23], row[24], row[25], row[31])
                # Create hast table and send entries to other peers or store local entries
                print("Number of entries:", len(entries))
                ht_size = next_largest_prime(2 * len(entries))
                print("Distributing entries...")
                num_entries = []
                for i in range(len(dht_list)):
                    num_entries.append(0)
                for i in range(len(entries)):
                    entry_pos = int(entries[i][0]) % ht_size
                    entry_id = entry_pos % len(dht_list)
                    print("Entry ID: " + str(entry_id) + ", Entry Pos: " + str(entry_pos))
                    if (entry_id == id):
                        hash_table[entry_pos] = entries[i]
                    else:
                        data = pickle.dumps((entries[i], entry_pos, entry_id))
                        p_socket.sendto(data, (neighbor_addr, neighbor_port))
                    num_entries[entry_id] += 1
                print("Entries distributed. Sending done message...")
                time.sleep(0.5) # Process was sending done message before all entries propagated
                for i in range(len(dht_list)):
                    p_socket.sendto(b'DONE', (dht_list[i][1], dht_list[i][2]))
                # Print number of entries at each peer
                for i in range(len(dht_list)):
                    print("Peer " + dht_list[i][0] + ", Entries: " + str(num_entries[i]))
                # Sending 'dht-complete' message to manager
                m_socket.sendto(("dht-complete " + peer_name).encode('ascii'), (manager_addr, manager_port))
            elif message[1] == 'teardown-dht':
                print("Tearing down DHT...")
                hash_table.clear()
                dht_list.clear()
                p_socket.sendto('teardown-dht'.encode('ascii'), (neighbor_addr, neighbor_port))
                neighbor_addr = ""
                neighbor_port = 0
                m_socket.sendto(("teardown-complete " + peer_name).encode('ascii'), (manager_addr, manager_port))
            elif message[1] == 'deregister':
                print("Deregistering...")
                m_socket.close()
                p_socket.close()
                exit(0)
            elif message[1] == 'leave-dht':
                print("Leaving DHT...")
            elif message[1] == 'join-dht':
                print("Joining DHT...")
            elif message[1] == 'query-dht':
                print("Querying DHT for event ID " + query_id)
                query_peer = pickle.loads(m_socket.recv(1024))
                p_socket.sendto(("query-dht " + query_id).encode('ascii'), (query_peer[1], query_peer[2]))
                p_socket.sendto(pickle.dumps((peer_name, peer_addr, p_port)), (query_peer[1], query_peer[2]))

                

def peer_thread():
    global p_socket, dht_list, id, neighbor_addr, neighbor_port, hash_table, ht_size
    while True:
        print("Listening on peer socket...")
        message = p_socket.recv(1024).decode('ascii')
        message = message.split()
        print(message[0])
        # Dht building
        if message[0] == "Welcome":
            print("Connected to DHT")
            message = p_socket.recv(1024)
            dht_list, id = pickle.loads(message)
            print("ID: " + str(id))
            print(dht_list)
            if id < len(dht_list) - 1:
                neighbor_addr = dht_list[id+1][1]
                neighbor_port = dht_list[id+1][2]
            else:
                neighbor_addr = dht_list[0][1]
                neighbor_port = dht_list[0][2]
            while True:
                message = p_socket.recv(1024)
                if message == b'DONE':
                    print("Done receiving entries")
                    break
                data, entry_pos, entry_id = pickle.loads(message)
                if entry_id == id:
                    hash_table[entry_pos] = data
                else:
                    p_socket.sendto(pickle.dumps((data, entry_pos, entry_id)), (neighbor_addr, neighbor_port))
            
        # Dht teardown
        elif message[0] == "teardown-dht":
            print("Tearing down DHT...")
            if (id != len(dht_list) - 1):
                p_socket.sendto(b'teardown-dht', (neighbor_addr, neighbor_port))
            hash_table.clear()
            dht_list.clear()
            neighbor_addr = ""
            neighbor_port = 0

        elif message[0] == "find-event":
            print("Searching for event: " + message[1])
            retaddr, query_pos = pickle.loads(p_socket.recv(1024))
            if query_pos in hash_table:
                print("Event found")
                p_socket.sendto("event-found".encode('ascii'), (retaddr[1], retaddr[2]))
                p_socket.sendto(pickle.dumps(hash_table[query_pos]), (retaddr[1], retaddr[2]))
            else: 
                if (id == 0):
                    print("Event not found")
                    p_socket.sendto("event-not-found".encode('ascii'), (retaddr[1], retaddr[2]))
                else:
                    print("Sending request forward...")
                    p_socket.sendto(("find-event " + message[1]).encode('ascii'), (neighbor_addr, neighbor_port))
                    p_socket.sendto(pickle.dumps((retaddr, query_pos)), (neighbor_addr, neighbor_port))
        elif message[0] == "query-dht":
            query_id = int(message[1])
            query_pos = query_id % ht_size
            retaddr = pickle.loads(p_socket.recv(1024))
            p_socket.sendto(("find-event " + message[1]).encode('ascii'), (neighbor_addr, neighbor_port))
            p_socket.sendto(pickle.dumps((retaddr, query_pos)), (neighbor_addr, neighbor_port))
        elif message[0] == "event-found":
            event = pickle.loads(p_socket.recv(1024))
            print("Event found: ")
            print(event)


def stdio_thread():
    global peer_name, peer_addr, m_port, p_port
    global m_socket, p_socket
    global year, num, query_id
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
                m_socket.sendto(register(peer_name, peer_addr, str(m_port), str(p_port)).encode('ascii'), (manager_addr, manager_port))
        elif command == "setup-dht": # Set up DHT
            if len(message) != 4:
                print("Invalid command")
            else:
                num = message[2]
                year = message[3]
                m_socket.sendto(setup_dht(peer_name, num, year).encode('ascii'), (manager_addr, manager_port))
        elif command == "teardown-dht":
            m_socket.sendto(("teardown-dht " + peer_name).encode('ascii'), (manager_addr, manager_port))
        elif command == "deregister":
            m_socket.sendto(("deregister " + peer_name).encode('ascii'), (manager_addr, manager_port))
        elif command == "leave-dht":
            m_socket.sendto(("leave-dht " + peer_name).encode('ascii'), (manager_addr, manager_port))
        elif command == "join-dht":
            m_socket.sendto(("join-dht " + peer_name).encode('ascii'), (manager_addr, manager_port))
        elif command == "query-dht":
            if len(message) != 2:
                print("Invalid command")
            else:
                m_socket.sendto(("query-dht " + peer_name + " " + message[1]).encode('ascii'), (manager_addr, manager_port))
                query_id = message[1]
        elif command == "exit":
            exit(0)


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
    t2.join()
    t1.join()
    t3.join()

main()