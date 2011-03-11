import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

import gobject
from gi.repository import Midgard

import json

# local imports
#import json_ld_processor as jlp
from DaemonConfig import DaemonConfig
from RdfMapper import RdfMapper

from handlers import QueryHandler, UpdateHandler

class MidgardDaemon:
    def __init__(self, addr):
        self.init_midgard()
        self.init_rdf_mapper()
        self.init_zmq(addr)

    def init_midgard(self):
        print("Connecting to midgard")
        Midgard.init()
        self.mgd = Midgard.Connection()
        self.mgd.open_config(DaemonConfig())
        print("... DONE")

    def init_rdf_mapper(self):
        print("Parsing RDF mapping info")
        self.rm = RdfMapper(self.mgd)
        print("... DONE")

    def init_zmq(self, addr):
        print("starting 0MQ thread")
        context = zmq.Context()

        socket = context.socket(zmq.REP)
        socket.bind(addr)

        self.loop = ioloop.IOLoop.instance()

        self.stream = ZMQStream(socket, self.loop)
        self.stream.on_recv(self.handler)
        print("... DONE")


    def handler(self, message):
        msg = str(message[0], 'utf8')

        try:
            data = json.loads(msg)
            if 'query' in data:
                response = self.handleQuery(data['query'])
            else if 'update' in data:
                response = self.handleUpdate(data['update'])
        except (TypeError, ValueError) as e:
            resp_obj = {"status": {"code": -128, "error": "Invalid request. %s" % (e) }}
            response = json.dumps(resp_obj)
        except gobject.GError as e:
            resp_obj = {"status": {"code": e.code, "error": "Invalid request. %s" % (e.message)}}
            response = json.dumps(resp_obj)

        self.stream.send(bytes(response, 'utf8'))

    def handleQuery(self, fields):
        handler = QueryHandler(self.mgd, self.rm, fields)
        return handler.handle()

    def handleUpdate(self, fields):
        handler = UpdateHandler(self.mgd, self.rm, fields)
        return handler.handle()

    def run(self):
        print("\nwaiting for requests...")
        self.loop.start()

if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("usage: daemon.py <address>")
        raise SystemExit

    daemon = MidgardDaemon(sys.argv[1])
    daemon.run()
