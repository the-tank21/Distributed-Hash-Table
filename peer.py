# Peer process for socket project
import socket

# Define variables
peer_name = ""
peer_addr = ""
m_port = 0
p_port = 0
peer_state = "Free"
id = 0 # Identifier for each peer in the DHT ring

# Manager info
manager_addr = ""
manager_port = 0

# Define peer functions

def register():
    # Send register command to manager with peer info
    message = "register " + peer_name + " " + socket.AF_INET + " " + m_port + " " + p_port
    # Send packet to manager address


# Define main loop and response to each function
def main():
    # Establish name and port numbers
    peer_name = input("Enter peer name: ")
    peer_addr = socket.gethostbyname(socket.gethostname())
    m_port = int(input("Enter peer-manager port: "))
    while (m_port < 14000 or m_port > 14500):
        m_port = int(input("Invalid port number. Please enter a valid port number: "))
    p_port = int(input("Enter peer-peer port: "))
    while (p_port < 14000 or p_port > 14500):
        p_port = int(input("Invalid port number. Please enter a valid port number: "))
    manager_addr = input("Enter manager address: ")
    manager_port = int(input("Enter manager port: "))
    while (manager_port < 14000 or manager_port > 14500):
        manager_port = int(input("Invalid port number. Please enter a valid port number: "))
    # Create socket for peer-manager communication
    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    m_socket.bind((peer_addr, m_port))

    # Create socket for peer-peer communication
    p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p_socket.bind((peer_addr, p_port))


    