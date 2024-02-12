# Manager process for socket project

import socket

port_number = int(input())
hostname = socket.gethostname()
host_socket = socket(hostname, port_number)

