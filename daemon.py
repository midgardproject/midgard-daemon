import json
import json_ld_processor as jlp

import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

import pygtk
import gobject
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

        try:
            data = json.loads(msg)
            if 'query' in data:
                response = self.handleQuery(data['query'])
        except (TypeError, ValueError) as e:
            resp_obj = {"status": {"code": -128, "error": "Invalid request. %s" % (e) }}
            response = json.dumps(resp_obj)

        self.stream.send(bytes(response, 'utf8'))

    def handleQuery(self, fields):
        if 'a' not in fields:
            raise Exception("Don't know what to return")

        mgd_type_name = self.decodeRdfName(fields['a'])

        try:
            qstor = Midgard.QueryStorage(dbclass=mgd_type_name)
            sel = Midgard.QuerySelect(connection=self.mgd, storage=qstor)

            if 'constraints' in fields and len(fields['constraints']) > 0:
                # this should be simplified, by using only "else" part, as soon as
                # core can handle that
                if len(fields['constraints']) == 1:
                    constraint_dict = fields['constraints'][0]
                    constraint = self.decodeConstraint(constraint_dict)
                    sel.set_constraint(constraint)
                else:
                    constr_group = Midgard.QueryConstraintGroup(grouptype="AND")
                    for constraint_dict in fields['constraints']:
                        constraint = self.decodeConstraint(constraint_dict)
                        constr_group.add_constraint(constraint)
                    sel.set_constraint(constr_group)

            if 'order' in fields:
                for order in fields['order']:
                    # in practice there will be only one element here
                    for key, direction in order.items():
                        qprop = Midgard.QueryProperty(property = self.decodeRdfName(key))
                        sel.add_order(qprop, direction)

            sel.execute()
        except gobject.GError as e:
            return json.dumps({"status": {"code": e.code, "error": "Invalid request. %s" % (e.message)}})

        objects = [self.encodeObj(obj) for obj in sel.list_objects()]
        return json.dumps(objects)

    def decodeConstraint(self, constraint_dict):
        value = Midgard.QueryValue()
        value.set_value(constraint_dict[2])

        property = Midgard.QueryProperty(property = self.decodeRdfName(constraint_dict[0]))
        constraint = Midgard.QueryConstraint(property = property,
                                             operator = constraint_dict[1],
                                             holder = value
        )

        return constraint

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
