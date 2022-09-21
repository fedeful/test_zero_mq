import random
import click
import zmq
import time


from basic_examples.public_subscribe import publisher


def ventilator(sleep_time=None):
    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5557")

    print("Press Enter when the workers are ready: ")
    _ = input()
    print("Sending tasks to workers...")

    # Initialize random number generator
    random.seed()

    # Send 100 tasks
    total_msec = 0
    for _ in range(100):
        # Random workload from 1 to 100 msecs
        workload = random.randint(1, 100)
        total_msec += workload

        sender.send_string(f"{workload}")

        if sleep_time:
            time.sleep(sleep_time)

    print(f"Total expected cost: {total_msec} msec")

    # Give 0MQ time to deliver
    time.sleep(1)


def client():
    # Prepare our context and sockets
    context = zmq.Context()

    # Connect to task ventilator
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:5557")

    # Connect to weather server
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://localhost:5556")
    subscriber.setsockopt(zmq.SUBSCRIBE, b"10001")

    # Process messages from both sockets
    # We prioritize traffic from the task ventilator
    while True:

        # Process any waiting tasks
        while True:
            try:
                # specifica alla socket di non rimanere bloccata in attesa della risposta
                # se non ha niente lancia un'eccezione
                msg = receiver.recv(zmq.DONTWAIT)
                print(msg)
            except zmq.Again:
                break
            # process task

        # Process any waiting weather updates
        while True:
            try:
                msg = subscriber.recv(zmq.DONTWAIT)
                print(msg)
            except zmq.Again:
                break
            # process weather update

        # No activity, so sleep for 1 msec
        time.sleep(0.001)


def client2():
    # Prepare our context and sockets
    context = zmq.Context()

    # Connect to task ventilator
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:5557")

    # Connect to weather server
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://localhost:5556")
    subscriber.setsockopt(zmq.SUBSCRIBE, b"10001")

    # Initialize poll set
    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)

    # Process messages from both sockets
    while True:
        try:
            socks = dict(poller.poll())
        except KeyboardInterrupt:
            break

        messages = []
        if receiver in socks:
            messages.append(receiver.recv())

        if subscriber in socks:
            messages.append(subscriber.recv())

        _ = [print(msg) for msg in messages]


@click.command()
@click.option('--mode', help='node type')
def main(mode):
    if mode == 'v':
        ventilator(2)
    elif mode == 'p':
        publisher(1)
    elif mode == 'c':
        client()
    elif mode == 'c2':
        client2()


if __name__ == '__main__':
    main()

