import socket
import pickle
import sys
from time import sleep

address = ('localhost', 6000)

conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn.connect(address)

conn.sendall(pickle.dumps(['get', 'ID']))
while True:
    conn.sendall(b'ready')
    progress = pickle.loads(conn.recv(1024))
    print(progress)
    sleep(.2)
