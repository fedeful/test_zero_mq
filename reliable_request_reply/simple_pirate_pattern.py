import time
import zmq
import click

from random import randint

from reliable_request_reply.lazy_pirate import client_node


LRU_READY = "\x01"


def queue():
    context = zmq.Context(1)

    frontend = context.socket(zmq.ROUTER)  # ROUTER
    backend = context.socket(zmq.ROUTER)  # ROUTER
    frontend.bind("tcp://*:5555")  # For clients
    backend.bind("tcp://*:5556")  # For workers

    poll_workers = zmq.Poller()
    poll_workers.register(backend, zmq.POLLIN)

    poll_both = zmq.Poller()
    poll_both.register(frontend, zmq.POLLIN)
    poll_both.register(backend, zmq.POLLIN)

    workers = []

    while True:
        if workers:
            socks = dict(poll_both.poll())
        else:
            socks = dict(poll_workers.poll())

        # Handle worker activity on backend
        if socks.get(backend) == zmq.POLLIN:
            # Use worker address for LRU routing
            msg = backend.recv_multipart()
            if not msg:
                break
            address = msg[0]
            workers.append(address)

            # Everything after the second (delimiter) frame is reply
            reply = msg[2:]

            # Forward message to client if it's not a READY
            if reply[0] != LRU_READY:
                frontend.send_multipart(reply)

        if socks.get(frontend) == zmq.POLLIN:
            #  Get client request, route to first available worker
            msg = frontend.recv_multipart()
            request = [workers.pop(0), ''.encode()] + msg
            backend.send_multipart(request)


def worker():
    context = zmq.Context(1)
    worker = context.socket(zmq.REQ)

    identity = "%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
    worker.setsockopt_string(zmq.IDENTITY, identity)
    worker.connect("tcp://localhost:5556")

    print("I: (%s) worker ready" % identity)
    worker.send_string(LRU_READY)

    cycles = 0
    while True:
        msg = worker.recv_multipart()
        if not msg:
            break

        cycles += 1
        if cycles > 0 and randint(0, 5) == 0:
            print("I: (%s) simulating a crash" % identity)
            break
        elif cycles > 3 and randint(0, 5) == 0:
            print("I: (%s) simulating CPU overload" % identity)
            time.sleep(3)
        print("I: (%s) normal reply" % identity)
        time.sleep(1)  # Do some heavy work
        worker.send_multipart(msg)


@click.command()
@click.option('--mode', help='node type')
@click.option('--node', default=1)
def main(mode, node):
    if mode == 'w':
        worker()
    elif mode == 'c':
        client_node(node)
    elif mode == 'q':
        queue()


if __name__ == '__main__':
    main()
