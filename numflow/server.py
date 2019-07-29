import socket
import pickle
import sys
import threading
from time import sleep
from networking import recv_msg, send_msg
 
HOST = 'localhost'
PORT = 6000
 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('Socket created')

# set socket to close properly
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

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
        send_msg(conn, b'good')
        sleep(.1)

    print('closing connection')
    conn.close()


children = {}
progress = {}

def get_progress_thread(conn, idx):
    print('thread created')
    send_msg(conn, b'ready')

    while conn:
        recv_msg(conn)
        print('sending data')
        update_progress()
        send_msg(conn, pickle.dumps(progress))

    print('closing connection')
    conn.close()


def update_progress():
    print('updating progress...')
    for idx,conn in children.items():
        if conn:
            send_msg(conn, b'ready')
            data = pickle.loads(recv_msg(conn))
            print(data)
            progress[idx].update(data)
    print('progress updated!')


while True:
    conn, addr = s.accept()
    print('Connected with ', addr)
    request = pickle.loads(recv_msg(conn))
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
