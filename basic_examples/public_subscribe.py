#   Author: Daniel Lundin <dln(at)eintr(dot)org>

import sys

import zmq
import random
import time
import click


############################
#   Public Subscribe       #
############################

def publisher(sleep_time=None):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5556")

    while True:
        zipcode = random.randrange(10001, 10005)
        temperature = random.randrange(-80, 135)
        relhumidity = random.randrange(10, 60)

        if sleep_time:
            time.sleep(sleep_time)

        msg = f"{zipcode} {temperature} {relhumidity}"
        print(msg)
        socket.send_string(msg)


def subscriber(node, filter_rule):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)

    print("Collecting updates from weather server...")
    socket.connect("tcp://localhost:5556")

    socket.setsockopt_string(zmq.SUBSCRIBE, filter_rule)

    print(f"node {node}")
    # Process 5 updates
    total_temp = 0
    for update_nbr in range(5):
        string = socket.recv_string()
        zipcode, temperature, relhumidity = string.split()
        total_temp += int(temperature)

        print((f"Average temperature for zipcode "
               f"'{filter_rule}' was {total_temp / (update_nbr + 1)} F"))


@click.command()
@click.option('--mode', help='node type')
@click.option('--node', default=1)
@click.option('--filter_rule', default="10001")
def main(mode, node, filter_rule):
    if mode == 'p':
        publisher()
    elif mode == 's':
        subscriber(node, filter_rule)


if __name__ == '__main__':
    main()
