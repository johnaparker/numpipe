import socket
import pickle
import sys
import threading
 
HOST = 'localhost'
PORT = 6000
 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('Socket created')
 
s.bind((HOST, PORT))
print('Socket bind complete')
 
s.listen(10)
print('Socket now listening')

progress = {}

def client_thread(conn):
    print('thread created')
    while True:
        data = conn.recv(1024)
        if not data:
            break

        data = pickle.loads(data)
        print('Received: ', data)

    conn.close()

while True:
    conn, addr = s.accept()
    print('Connected with ', addr)
    print(pickle.loads(conn.recv(1024)))

    t = threading.Thread(target=client_thread, args=(conn,))
    t.start()
     
s.close()
