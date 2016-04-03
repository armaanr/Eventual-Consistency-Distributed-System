import socket
import time
import sys

IP = "127.0.0.1"
PORT = 1234

def main(output_file):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto("START,666", (IP, PORT))
    with open(output_file, "r") as f:
        for line in f:
            sock.sendto(line, (IP, PORT))


"""
For testing the visualaztion tool.
"""
if __name__ == '__main__':
    main(sys.argv[1])
