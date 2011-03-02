import zmq

def main(addr, who):
    ctx = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.connect(addr)

    while True:
        msg = bytes(input("%s> " % who), 'utf8')
        socket.send(msg)
        resp = socket.recv()
        print("got: %s\n" % (resp))

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("usage: client.py <address> <username>")
        raise SystemExit
    main(sys.argv[1], sys.argv[2])
