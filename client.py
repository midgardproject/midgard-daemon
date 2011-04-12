import zmq

def main(addr):
    ctx = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.connect(addr)

    while True:
        try:
            msg = raw_input("enter message> ")
            socket.send(msg)
            resp = socket.recv()
            print("\ngot: %s\n" % (resp))
        except EOFError:
            print("") #new line
            break

if __name__ == '__main__':
    import sys

    if len(sys.argv) != 2:
        print("usage: client.py <address>")
        raise SystemExit

    main(sys.argv[1])
