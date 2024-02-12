# Manager process for socket project

import socket

# Create and bind socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
host = socket.gethostname()
port_number = int(input("Enter port number between 14000 and 14500: "))
while (port_number < 14000 or port_nummber > 14500):
    port_number = int(input("Enter port number between 14000 and 14500: "))
s.bind((host, port_number))

# Run server
# Listen for command
# Identify type of command
# Execute command


# Define register function

# Define setep_dht function

# Define dht_complete function