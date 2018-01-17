import socket
import pickle
import sys
from time import sleep
from pinboard.networking import send_msg, recv_msg

address = ('localhost', 6000)

conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn.connect(address)

send_msg(conn, pickle.dumps(['get', 'ID']))
recv_msg(conn)
while True:
    send_msg(conn, b'ready')
    progress = pickle.loads(recv_msg(conn))
    print(progress)
    sleep(.2)
