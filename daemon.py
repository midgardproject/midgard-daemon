import json
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

        self.mgd = Midgard.Connection()
        self.mgd.open_config (config)

        storage = Midgard.Storage()
        storage.create_base_storage(self.mgd)

        context = zmq.Context()
        socket = context.socket(zmq.REP)

        socket.bind(addr)

        self.loop = ioloop.IOLoop.instance()

        self.stream = ZMQStream(socket, self.loop)
        self.stream.on_recv(self.handler)

    def handler(self, message):
        msg = str(message[0], 'utf8')

        data = json.loads(msg)
        if 'query' in data:
            response = self.handleQuery(data['query'])

        self.stream.send(bytes(response, 'utf8'))

    def handleQuery(self, fields):
        if 'a' not in fields:
            raise Exception("Don't know what to return")

        mgd_type_name = self.decodeType(fields['a'])

        if 'constraints' in fields:
            """add constraints"""
            pass

        if 'order' in fields:
            """add order"""
            pass

        qstor = Midgard.QueryStorage(dbclass=mgd_type_name)
        sel = Midgard.QuerySelect(connection=self.mgd, storage=qstor)
        sel.execute()

        objects = [obj.get_property('guid') for obj in sel.list_objects()]
        return json.dumps(objects)

    def decodeType(self, rdfName):
        """convert RDF-name of type to Midgard-name of type"""
        ns_name, _, class_name = rdfName.rpartition(':')

        if ns_name != 'mgd':
            raise Exception('"%s" namespace is not supported' % (ns_name))

        return class_name

    def run(self):
        self.loop.start()

if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("usage: daemon.py <address>")
        raise SystemExit

    daemon = MidgardDaemon(sys.argv[1])
    daemon.run()
