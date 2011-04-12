from gi.repository import Midgard
import json
import re

class Handler:
    MGD_NAMESPACE = 'http://www.midgard-project.org/midgard2/10.05/'

    def __init__(self, mgd, rm, input):
        self.mgd = mgd
        self.rm = rm
        self.input = input

    def encodeObj(self, obj):
        classname = obj.__class__.__gtype__.name

        retVal = {
            '#': {'mgd': self.MGD_NAMESPACE},
            'a': self.encodeClassname(classname),
        }

        names = [pspec.name for pspec in obj.props if not pspec.value_type.is_classed()]
        for name in names:
            if name == 'guid':
                retVal['@'] = '<urn:uuid:%s>' % (obj.props.guid)
            else:
                fieldname = self.encodeFieldname(classname, name)
                retVal[fieldname] = obj.get_property(name)

        return retVal

    def encodeClassname(self, classname):
        if classname in self.rm.classes_to_rdf:
            return self.rm.classes_to_rdf[classname]

        return 'mgd:' + classname

    def encodeFieldname(self, classname, fieldname):
        if classname in self.rm.fields_to_rdf:
            if fieldname in self.rm.fields_to_rdf[classname]:
                return self.rm.fields_to_rdf[classname][fieldname]

        return 'mgd:' + fieldname

    def decodeRdfClass(self, rdfName):
        if rdfName.startswith(self.MGD_NAMESPACE):
            return rdfName[len(self.MGD_NAMESPACE):]

        if rdfName not in self.rm.rdf_to_classes:
            raise ValueError("Don't know how to handle " + rdfName)

        return self.rm.rdf_to_classes[rdfName]

    def decodeRdfProperty(self, mgd_class, rdfName):
        if rdfName.startswith(self.MGD_NAMESPACE):
            return rdfName[len(self.MGD_NAMESPACE):]

        if rdfName not in self.rm.rdf_to_fields[mgd_class]:
            raise ValueError("Don't know how to handle " + rdfName + " field")

        return self.rm.rdf_to_fields[mgd_class][rdfName]

    # Static methods
    @staticmethod
    def canonicalRdfName(map, rdfName):
        """convert short RDF-name to the full form"""

        for short,long in map.items():
            if rdfName.startswith(short + ':'):
                return rdfName.replace(short + ":", long, 1)

        return rdfName

class QueryHandler (Handler):
    def __init__(self, mgd, rm, input):
        if 'a' not in input:
            raise ValueError("Don't know what to return")

        Handler.__init__(self, mgd, rm, input)

        if '#' in self.input:
            self.rdf_map = self.input['#']
        else:
            self.rdf_map = {}

        full_name = Handler.canonicalRdfName(self.rdf_map, self.input['a'])
        self.mgd_type_name = self.decodeRdfClass(full_name)

    def handle(self):
        qstor = Midgard.QueryStorage(dbclass=self.mgd_type_name)
        qstor.validate()

        sel = Midgard.QuerySelect(connection=self.mgd, storage=qstor)
        sel.validate()

        if 'constraints' in self.input and len(self.input['constraints']) > 0:
            constraint = self.decodeConstraints(self.input['constraints'])
            sel.set_constraint(constraint)

        if 'order' in self.input:
            for order in self.input['order']:
                for key, direction in order.items():
                    full_name = Handler.canonicalRdfName(self.rdf_map, key)
                    qprop = Midgard.QueryProperty(property = self.decodeRdfProperty(self.mgd_type_name, full_name))
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
        value.set_value(str(constraint_dict[2]))

        full_name = Handler.canonicalRdfName(self.rdf_map, constraint_dict[0])
        property = Midgard.QueryProperty(property = self.decodeRdfProperty(self.mgd_type_name, full_name))
        property.validate()

        constraint = Midgard.QueryConstraint(property = property,
                                             operator = constraint_dict[1],
                                             holder = value
        )
        constraint.validate()

        return constraint

class UpdateHandler (Handler):
    def __init__(self, mgd, rm, input):
        if 'a' not in input:
            raise ValueError("Type is not specified")

        if '@' not in input:
            raise ValueError('UUID not specified')

        Handler.__init__(self, mgd, rm, input)

        if '#' in self.input:
            self.rdf_map = self.input['#']
        else:
            self.rdf_map = {}

        full_name = Handler.canonicalRdfName(self.rdf_map, self.input['a'])
        self.mgd_type_name = self.decodeRdfClass(full_name)
        self.guid = self.decodeGuid(self.input['@'])

    def decodeGuid(self, urn):
        m = re.search('<urn:uuid:(.*)>', urn)
        return m.group(1)

    def handle(self):
        obj = Midgard.Object.new(self.mgd, self.mgd_type_name, self.guid)

        if None == obj:
            raise ValueError("Object not found")

        for k,v in self.input.items():
            if k in ['#', '@', 'a']:
                continue

            obj.set_property(k, v)

        if obj.update():
            resp_obj = {"status": {"code": 0, "error": "MGD_ERR_OK" }}
            return json.dumps(resp_obj)

        raise ValueError("Update failed. %s" % (self.mgd.get_error_string()))
