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

        mgd_type_name = self.decodeRdfName(fields['a'])

        if 'constraints' in fields:
            """add constraints"""
            pass

        if 'order' in fields:
            """add order"""
            pass

        qstor = Midgard.QueryStorage(dbclass=mgd_type_name)
        sel = Midgard.QuerySelect(connection=self.mgd, storage=qstor)
        sel.execute()

        objects = [self.encodeObj(obj) for obj in sel.list_objects()]
        return json.dumps(objects)

    def decodeRdfName(self, rdfName):
        """convert RDF-name of type/field to Midgard-name of type/field"""
        ns_name, _, class_name = rdfName.rpartition(':')

        if ns_name != 'mgd':
            raise Exception('"%s" namespace is not supported' % (ns_name))

        midgard_name = class_name

        return midgard_name

    def encodeObj(self, obj):
        retVal = {
            '#': {'mgd': 'http://www.midgard-project.org/midgard2/10.05/'},
            'a': 'mgd:%s' % (obj.__class__.__name__.rpartition('.')[2]),
        }

        names = [pspec.name for pspec in obj.props if not pspec.value_type.is_classed()]
        for name in names:
            if name == 'guid':
                retVal['@'] = '<urn:uuid:%s>' % (obj.props.guid)
            else:
                retVal['mgd:' + name] = obj.get_property(name)

        return retVal

    def run(self):
        self.loop.start()

if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("usage: daemon.py <address>")
        raise SystemExit

    daemon = MidgardDaemon(sys.argv[1])
    daemon.run()
