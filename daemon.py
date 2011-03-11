import json
import json_ld_processor as jlp

import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

import gobject
from gi.repository import Midgard

# local imports
from DaemonConfig import DaemonConfig
from RdfMapper import RdfMapper

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
        print("DONE")


    def handler(self, message):
        msg = str(message[0], 'utf8')

        try:
            data = json.loads(msg)
            if 'query' in data:
                response = self.handleQuery(data['query'])
        except (TypeError, ValueError) as e:
            resp_obj = {"status": {"code": -128, "error": "Invalid request. %s" % (e) }}
            response = json.dumps(resp_obj)
        except gobject.GError as e:
            resp_obj = {"status": {"code": e.code, "error": "Invalid request. %s" % (e.message)}}
            response = json.dumps(resp_obj)

        self.stream.send(bytes(response, 'utf8'))

    def handleQuery(self, fields):
        if 'a' not in fields:
            raise ValueError("Don't know what to return")

        mgd_type_name = self.decodeRdfName(fields['a'])

        qstor = Midgard.QueryStorage(dbclass=mgd_type_name)
        qstor.validate()

        sel = Midgard.QuerySelect(connection=self.mgd, storage=qstor)
        sel.validate()

        if 'constraints' in fields and len(fields['constraints']) > 0:
            constraint = self.decodeConstraints(fields['constraints'])
            sel.set_constraint(constraint)

        if 'order' in fields:
            for order in fields['order']:
                for key, direction in order.items():
                    qprop = Midgard.QueryProperty(property = self.decodeRdfName(key))
                    qprop.validate()

                    sel.add_order(qprop, direction)

        sel.execute()

        objects = [self.encodeObj(obj) for obj in sel.list_objects()]
        return json.dumps(objects)

    def decodeConstraints(self, constraints_list):
        # this should be simplified, by using only "else" part, as soon as
        # core can handle that
        if len(constraints_list) == 1:
            constraint_dict = constraints_list[0]
            return self.decodeConstraint(constraint_dict)
        else:
            constr_group = Midgard.QueryConstraintGroup(grouptype="AND")
            for constraint_dict in constraints_list:
                constraint = self.decodeConstraint(constraint_dict)
                constr_group.add_constraint(constraint)
            return constr_group

    def decodeConstraint(self, constraint_dict):
        value = Midgard.QueryValue()
        value.set_value(constraint_dict[2])

        property = Midgard.QueryProperty(property = self.decodeRdfName(constraint_dict[0]))
        property.validate()

        constraint = Midgard.QueryConstraint(property = property,
                                             operator = constraint_dict[1],
                                             holder = value
        )
        constraint.validate()

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
