import time
import zmq
import click

from collections import OrderedDict
from random import randint

from reliable_request_reply.lazy_pirate import client_node

HEARTBEAT_LIVENESS = 3
HEARTBEAT_INTERVAL = 1
INTERVAL_INIT = 1
INTERVAL_MAX = 32

#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat


class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS


class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for address, worker in self.queue.items():
            if t > worker.expiry:  # Worker expired
                expired.append(address)
        for address in expired:
            print("W: Idle worker expired: %s" % address)
            self.queue.pop(address, None)

    def next(self):
        address, worker = self.queue.popitem(False)
        return address


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

    workers = WorkerQueue()

    heartbeat_at = time.time() + HEARTBEAT_INTERVAL

    while True:
        if len(workers.queue) > 0:
            poller = poll_both
        else:
            poller = poll_workers
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))

        # Handle worker activity on backend
        if socks.get(backend) == zmq.POLLIN:
            # Use worker address for LRU routing
            frames = backend.recv_multipart()
            if not frames:
                break

            address = frames[0]
            workers.ready(Worker(address))

            # Validate control message, or return reply to client
            msg = frames[1:]
            if len(msg) == 1:
                if msg[0] not in (PPP_READY, PPP_HEARTBEAT):
                    print("E: Invalid message from worker: %s" % msg)
            else:
                frontend.send_multipart(msg)

            # Send heartbeats to idle workers if it's time
            if time.time() >= heartbeat_at:
                for worker in workers.queue:
                    msg = [worker, PPP_HEARTBEAT]
                    backend.send_multipart(msg)
                heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        if socks.get(frontend) == zmq.POLLIN:
            frames = frontend.recv_multipart()
            if not frames:
                break

            frames.insert(0, workers.next())
            backend.send_multipart(frames)

        workers.purge()


def worker_socket(context, poller):
    """Helper function that returns a new configured socket
       connected to the Paranoid Pirate queue"""
    worker = context.socket(zmq.DEALER)  # DEALER
    identity = b"%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
    worker.setsockopt(zmq.IDENTITY, identity)
    poller.register(worker, zmq.POLLIN)
    worker.connect("tcp://localhost:5556")
    worker.send(PPP_READY)
    return worker


def worker():
    context = zmq.Context(1)
    poller = zmq.Poller()

    liveness = HEARTBEAT_LIVENESS
    interval = INTERVAL_INIT

    heartbeat_at = time.time() + HEARTBEAT_INTERVAL

    worker = worker_socket(context, poller)
    cycles = 0
    while True:
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))

        # Handle worker activity on backend
        if socks.get(worker) == zmq.POLLIN:
            #  Get message
            #  - 3-part envelope + content -> request
            #  - 1-part HEARTBEAT -> heartbeat
            frames = worker.recv_multipart()
            if not frames:
                break  # Interrupted

            if len(frames) == 3:
                # Simulate various problems, after a few cycles
                cycles += 1
                if cycles > 3 and randint(0, 5) == 0:
                    print("I: Simulating a crash")
                    break
                if cycles > 3 and randint(0, 5) == 0:
                    print("I: Simulating CPU overload")
                    time.sleep(3)
                print("I: Normal reply")
                worker.send_multipart(frames)
                liveness = HEARTBEAT_LIVENESS
                time.sleep(1)  # Do some heavy work
            elif len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                print("I: Queue heartbeat")
                liveness = HEARTBEAT_LIVENESS
            else:
                print("E: Invalid message: %s" % frames)
            interval = INTERVAL_INIT
        else:
            liveness -= 1
            if liveness == 0:
                print("W: Heartbeat failure, can't reach queue")
                print("W: Reconnecting in %0.2fs..." % interval)
                time.sleep(interval)

                if interval < INTERVAL_MAX:
                    interval *= 2
                poller.unregister(worker)
                worker.setsockopt(zmq.LINGER, 0)
                worker.close()
                worker = worker_socket(context, poller)
                liveness = HEARTBEAT_LIVENESS
        if time.time() > heartbeat_at:
            heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            print("I: Worker heartbeat")
            worker.send(PPP_HEARTBEAT)


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
