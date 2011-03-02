import json_ld_processor as jlp

import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

import pygtk
import gi
from gi.repository import Midgard

class MidgardDaemon:
    def __init__(self, addr):
        Midgard.init()

        config = Midgard.Config()
        config.props.dbtype = "SQLite"
        config.set_property ("database", "testdb")
        config.set_property ("loglevel", "warn")

        mgd = Midgard.Connection()
        mgd.open_config (config)

        storage = Midgard.Storage()
        storage.create_base_storage(mgd)

        context = zmq.Context()
        socket = context.socket(zmq.REP)

        socket.bind(addr)

        self.loop = ioloop.IOLoop.instance()

        self.stream = ZMQStream(socket, self.loop)
        self.stream.on_recv(self.handler)

    def handler(self, message):
        msg = str(message[0], 'utf8')

        processor = jlp.Processor()
        for triple in processor.triples(msg):
            print('triple: objtype: %s, obj: %s' % (triple['objtype'], triple['obj']))

        self.stream.send(b'hey!')

    def run(self):
        self.loop.start()

if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("usage: daemon.py <address>")
        raise SystemExit

    daemon = MidgardDaemon(sys.argv[1])
    daemon.run()
