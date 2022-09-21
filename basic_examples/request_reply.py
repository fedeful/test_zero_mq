import time
import zmq


def server(node=1, port=None):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    port = port or '5560'
    socket.connect(f"tcp://localhost:{port}")

    while True:
        #  Wait for next request from client
        message = socket.recv()
        print(f"Server node {node} Received request: {message}")

        #  Do some 'stuff'
        time.sleep(1)

        socket.send_string("World")


def client(node=1, port=None):
    #  Prepare our context and sockets
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    port = port or '5559'
    socket.connect(f"tcp://localhost:{port}")

    #  Do 10 requests, waiting each time for a response
    for request in range(1, 11):
        socket.send(b"Hello")
        message = socket.recv()
        print(f"Node client {node} Received reply {request} [{message}]")