# Peer process for socket project
import socket, pickle, csv, os, fnmatch, math

# Define variables
global peer_name
global peer_addr
global m_port
global p_port
peer_state = "Free"
id = 0 # Identifier for each peer in the DHT ring
dht_list = [] # List of peers in the DHT ring, only delivered to leader
hash_table = {} # Local hash table


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

# Define main loop and response to each function
def main():
    # Store address of manager
    manager_addr = input("Enter manager address: ")
    manager_port = int(input("Enter manager port: "))

    while True:
        # Read command from stdin
        command = input("Enter command: ")
        command = command.split(" ")
        print(command)
        if command[0] == "register":
            print("Registering peer...")
            # Store peer info
            peer_name = command[1]
            peer_addr = command[2]
            m_port = int(command[3])
            p_port = int(command[4])
            # Create socket for peer-peer communication
            p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            p_socket.bind((peer_addr, p_port))
            # Create socket for peer-manager communication
            m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            m_socket.bind((peer_addr, m_port))
            message = register(command[1], command[2], command[3], command[4])
            m_socket.sendto(message.encode(), (manager_addr, manager_port))
            response, address = m_socket.recvfrom(1024)
            response = response.decode()
            print(response)
        if command[0] == "setup-dht":
            message = setup_dht(command[1], command[2], command[3])
            m_socket.sendto(message.encode(), (manager_addr, manager_port))
            print("Waiting for response...")
            response, address = m_socket.recvfrom(1024)
            response = response.decode()
            print(response)
            if response == "FAILURE":
                break
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
            # Find full filename
            year = command[3]
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
            for i in range(len(dht_list)):
                p_socket.sendto(b'DONE', (dht_list[i][1], dht_list[i][2]))
            # Print number of entries at each peer
            for i in range(len(dht_list)):
                print("Peer " + dht_list[i][0] + ", Entries: " + str(num_entries[i]))

            
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
            # Listen for hash table entries and check if they belong to this peer
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

        if command[0] == "dht-complete":
            # Send done message to manager
            message = "dht-complete " + peer_name
            m_socket.sendto(message.encode(), (manager_addr, manager_port))
            data = m_socket.recv(1024)
            print(data.decode())



main()

    