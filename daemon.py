import json_ld_processor as jlp
import zmq
import pygtk
import gi

from gi.repository import Midgard

def init():
    Midgard.init()

    config = Midgard.Config()
    config.props.dbtype = "SQLite"
    config.set_property ("database", "testdb")
    config.set_property ("loglevel", "warn")

    mgd = Midgard.Connection()
    mgd.open_config (config)

    storage = Midgard.Storage()
    storage.create_base_storage(mgd)

def main(addr):
    context = zmq.Context()
    socket = context.socket(zmq.REP)

    socket.bind(addr)

    while True:
        msg = socket.recv()
        print("%s" % (msg))
        socket.send(b'hey!')

if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("usage: daemon.py <address>")
        raise SystemExit

    init()
    main(sys.argv[1])
