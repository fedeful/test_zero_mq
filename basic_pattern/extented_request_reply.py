import zmq
import click

from basic_examples.request_reply import client, server


def broker():
    # Prepare our context and sockets
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.DEALER)
    frontend.bind("tcp://*:5559")
    backend.bind("tcp://*:5560")

    # Initialize poll set
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    # Switch messages between sockets
    while True:
        socks = dict(poller.poll())

        if socks.get(frontend) == zmq.POLLIN:
            message = frontend.recv_multipart()
            backend.send_multipart(message)

        if socks.get(backend) == zmq.POLLIN:
            message = backend.recv_multipart()
            frontend.send_multipart(message)


@click.command()
@click.option('--mode', help='node type')
@click.option('--node', default=1)
def main(mode, node):
    if mode == 's':
        server(node)
    elif mode == 'c':
        client(node)
    elif mode == 'b':
        broker()


if __name__ == '__main__':
    main()
