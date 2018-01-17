import socket
import pickle
import sys
import threading
from time import sleep
 
HOST = 'localhost'
PORT = 6000
 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('Socket created')
 
s.bind((HOST, PORT))
print('Socket bind complete')
 
s.listen(10)
print('Socket now listening')

def client_thread(conn, idx):
    print('thread created')
    progress = {}
    all_progress[idx] = progress

    while conn:
        data = conn.recv(1024)
        if not data:
            break

        data = pickle.loads(data)
        progress.update(data)
        print(all_progress)
        conn.sendall(b'good')
        sleep(.1)

    print('closing connection')
    conn.close()


children = {}
progress = {}

def get_progress_thread(conn, idx):
    print('thread created')
    conn.send(b'ready')

    while conn:
        data = conn.recv(1024)
        print('sending data')
        update_progress()
        conn.sendall(pickle.dumps(progress))

    print('closing connection')
    conn.close()


def update_progress():
    print('updating progress...')
    for idx,conn in children.items():
        if conn:
            conn.sendall(b'ready')
            data = conn.recv(100024)
            data = pickle.loads(data)
            progress[idx].update(data)
    print('progress updated!')


while True:
    conn, addr = s.accept()
    print('Connected with ', addr)
    request = pickle.loads(conn.recv(1024))
    print(request)

    if request[0] == 'new':
        idx = request[1]
        children[idx] = conn
        progress[idx] = {}
        
        print('new child created')
        # t = threading.Thread(target=client_thread, args=(conn, idx))
        # t.start()

    elif request[0] == 'get':
        idx = request[1]
        t = threading.Thread(target=get_progress_thread, args=(conn, idx))
        t.start()

     
s.close()
