#   Author: Daniel Lundin <dln(at)eintr(dot)org>

import itertools
import logging
import sys
import time
import zmq
import click

from random import randint

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 3
SERVER_ENDPOINT = "tcp://localhost:5555"


def client_node(node):
    context = zmq.Context()

    logging.info(f"Node {node} connecting to server…")
    client = context.socket(zmq.REQ)
    client.connect(SERVER_ENDPOINT)

    for sequence in itertools.count():
        request = str(sequence).encode()
        logging.info("Sending (%s)", request)
        client.send(request)

        retries_left = REQUEST_RETRIES
        while True:
            if (client.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:
                reply = client.recv()
                if int(reply) == sequence:
                    logging.info("Server replied OK (%s)", reply)
                    break
                else:
                    logging.error("Malformed reply from server: %s", reply)
                    continue

            retries_left -= 1
            logging.warning("No response from server")
            # Socket is confused. Close and remove it.
            client.setsockopt(zmq.LINGER, 0)
            client.close()
            if retries_left == 0:
                logging.error("Server seems to be offline, abandoning")
                sys.exit()

            logging.info("Reconnecting to server…")
            # Create new connection
            client = context.socket(zmq.REQ)
            client.connect(SERVER_ENDPOINT)
            logging.info("Resending (%s)", request)
            client.send(request)


def server_node():
    context = zmq.Context()
    server = context.socket(zmq.REP)
    server.bind("tcp://*:5555")

    for cycles in itertools.count():
        request = server.recv()

        # Simulate various problems, after a few cycles
        if cycles > 3 and randint(0, 3) == 0:
            logging.info("Simulating a crash")
            break
        elif cycles > 3 and randint(0, 3) == 0:
            logging.info("Simulating CPU overload")
            time.sleep(2)

        logging.info("Normal request (%s)", request)
        time.sleep(1)  # Do some heavy work
        server.send(request)


@click.command()
@click.option('--mode', help='node type')
@click.option('--node', default=1)
def main(mode, node):
    if mode == 's':
        server_node()
    elif mode == 'c':
        client_node(node)


if __name__ == '__main__':
    main()
