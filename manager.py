# Manager process for socket project

import socket

# Create and bind socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
host = socket.gethostname()
port_number = int(input("Enter port number: "))
s.bind((host, port_number))

# Run server
# Listen for command
# Identify type of command
# Execute command