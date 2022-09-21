#   Author: Daniel Lundin <dln(at)eintr(dot)org>

import zmq
import random
import time
import click


############################
#   Divide and Conquer     #
############################


def ventilator():
    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5557")

    # Socket with direct access to the sink: used to synchronize start of batch
    sink = context.socket(zmq.PUSH)
    sink.connect("tcp://localhost:5558")

    print("Press Enter when the workers are ready: ")
    _ = input()
    print("Sending tasks to workers...")

    # The first message is "0" and signals start of batch
    sink.send(b'0')

    # Initialize random number generator
    random.seed()

    # Send 100 tasks
    total_msec = 0
    for _ in range(100):
        # Random workload from 1 to 100 msecs
        workload = random.randint(1, 100)
        total_msec += workload

        sender.send_string(f"{workload}")

    print(f"Total expected cost: {total_msec} msec")

    # Give 0MQ time to deliver
    time.sleep(1)


def worker(node):
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:5557")

    # Socket to send messages to
    sender = context.socket(zmq.PUSH)
    sender.connect("tcp://localhost:5558")

    print(f'started worker node {node}')
    # Process tasks forever
    while True:
        s = receiver.recv()

        # Simple progress indicator for the viewer
        print(f'worker node {node} so something')

        # Do the work
        time.sleep(int(s) * 0.001)

        # Send results to sink
        msg = f'msg from node {node}'
        sender.send(msg.encode())


def sink():
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.bind("tcp://*:5558")

    # Wait for start of batch
    s = receiver.recv()
    print(s.decode())

    # Start our clock now
    tstart = time.time()

    # Process 100 confirmations
    for _ in range(100):
        s = receiver.recv()
        s = s.decode()
        print(s)

    # Calculate and report duration of batch
    tend = time.time()
    print(f"Total elapsed time: {(tend - tstart) * 1000} msec")


@click.command()
@click.option('--mode', help='node type')
@click.option('--node', default=1)
def main(mode, node):
    if mode == 'v':
        ventilator()
    elif mode == 'w':
        worker(node)
    elif mode == 's':
        sink()


if __name__ == '__main__':
    main()

