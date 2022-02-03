'''
Command to start the server
/usr/local/Cellar/python\@3.8/3.8.1/bin/python3 count_server.py 8000
'''

import sys
import socket
# use json for client-server communication
import json
import random

from collections import Counter
from multiprocessing import Process, Queue

from word_count import word_counts

response = Queue(3)

def send_message(client, message):
    client.send("{0:5d}".format(len(message)).encode())
    client.send(message)


def send_slice(server_name, data):
    '''
    Sends a slice of data to the other server for processing
    :param server_name:   a hostname:port   sring to which we will contect
    :param slice: a slice of data from the file that we want to preocess
    :return: none
    '''
    parts = server_name.split(":")
    client = socket.socket()
    client.connect((parts[0], int(parts[1])))

    data = "".join(data).encode()

    send_message(client, "count".encode())
    send_message(client, data)

    # a json response which contains the word counts for the slice
    resp = read_message(client)

    response.put(resp)
    print("Response received")
    #print("Size of response array", response.qsize())


def read_message(client):
    '''
    First read the size of the message to be read.
    This is an ASCII, which we will convert to an int.

    Next, we need to be aware that the data will be sent one packet at a time,
    therefore we need to read all of that and assemble it back together.

    So we push that packet that was read into a list. After all the data is read,
    re-run it by joining the list together to create a single string.
    :param client:
    :return:
    '''
    length = int(client.recv(5).decode().strip())
    data = []
    count = 0

    while count < length:
        item = client.recv(length - count)
        count += len(item)
        data.append(item.decode())

    return "".join(data)


def notify_leader(leader, fname):
    '''
    Open a connection to the leader and send it the leader instruction
    this is followed by the name of the file.
    :param leader: as hostname:port
    :param fname:  the name of the file to process
    :return: leader response
    '''
    leader="localhost:8000"
    print("the leader is", leader)
    parts = leader.split(":")
    client = socket.socket()
    client.connect((parts[0], int(parts[1])))

    send_message(client, "leader".encode())
    send_message(client, fname)

    resp1 = read_message(client)
    resp2 = read_message(client)

    client.close()
    return resp1, resp2


def server_process(client, config):

    instruction = read_message(client)

    if instruction == "process":
        print("process")
        # leader is chosen randomly
        leader = random.choice(config)

        fname = read_message(client)
        resp1, resp2 = notify_leader(leader, fname.encode())
        send_message(client, resp1.encode())
        send_message(client, resp2.encode())

    elif instruction == "count":
        data = read_message(client)
        counts = word_counts(data.split("\n"))
        send_message(client, json.dumps(counts).encode())

    elif instruction == "leader":
        fname = read_message(client)
        with open(fname.strip()) as fp:
            lines = fp.readlines()
            size = len(lines) // 3
            start = 0

            # list of processes
            procs = []
            count = Counter()
            for server_name in config:
                print(server_name, start, size + start)
                p = Process(target=send_slice, args=[server_name, lines[start:start+size]])
                start += size

                procs.append(p)
                p.start()

            for server_name in config:
                resp = response.get()
                count.update(json.loads(resp))
                print("Updated word counts")


            print("Waiting for threads to di")
            for proc in procs:
                # wait for the process to finish
                # the loop ensures that all the threads will finish
                proc.join()

            print(count.most_common(3))
            print(count.most_common()[:-4:-1])

            # send it to the client
            send_message(client, json.dumps(count.most_common(3)).encode())
            send_message(client, json.dumps(count.most_common()[:-4:-1]).encode())

    client.close()


def start_server(port, config):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', port))
    print("Server is starting")
    sock.listen()

    while True:
        client, addr = sock.accept()
        p = Process(target=server_process, args=[client, config])
        p.start();


if __name__ == '__main__':
    # public static void main(Stgring[] args) {}
     if len(sys.argv) > 1:
         with open("config.json") as fp:
             config = json.load(fp)
             start_server(int(sys.argv[1]), config)

     else:
         print("Please specify the port number as a command line argument")