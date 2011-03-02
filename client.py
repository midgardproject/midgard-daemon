import zmq

def main(addr):
    ctx = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.connect(addr)

    while True:
        msg = bytes(input("enter message> "), 'utf8')
        socket.send(msg)
        resp = socket.recv()
        print("got: %s\n" % (resp))

if __name__ == '__main__':
    import sys

    if len(sys.argv) != 2:
        print("usage: client.py <address>")
        raise SystemExit

    main(sys.argv[1])
