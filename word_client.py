import socket

'''
Protocol:

the instruction may be
process
process_chunk
leader
then the size of the data
then the data

leader means that a particular server has been assigned to be leader.
it will give instructions to the others on what to do.

for process the next instruction is the name of the file
for process_chunk the next instruction is a list of sentences

'''

from count_server import send_message, read_message

client = socket.socket()
client.connect(("localhost", 8000))
cmd = "process".encode()
fname = "AliceInWonderland.txt".encode()

send_message(client, cmd)
send_message(client, fname)

print(read_message(client))
print(read_message(client))

client.close()