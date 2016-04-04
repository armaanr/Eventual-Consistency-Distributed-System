import socket
import time
import sys
import os

IP = "127.0.0.1"
PORT = 1234

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto("START,666", (IP, PORT))
    for fl in os.listdir(os.getcwd()):
        if fl.endswith(".txt"):
            with open(fl, "r") as f:
                for line in f:
                    sock.sendto(line, (IP, PORT))


"""
For testing the visualaztion tool.
"""
if __name__ == '__main__':
    main()
