import gobject
from gi.repository import Midgard

class RdfMapper:
    def __init__(self, mgd):
        self.mgd = mgd

        self.classes_to_rdf = {}
        self.rdf_to_classes = {}

        self.fields_to_rdf = {}
        self.rdf_to_fields = {}

        self.read_types()

    def read_types(self):
        for gtype in Midgard.Object.__gtype__.children:
            self.read_type(gtype)

        for gtype in Midgard.View.__gtype__.children:
            self.read_type(gtype)

    def read_type(self, gtype):
        rdf = Midgard.ReflectorObject.get_schema_value(gtype.name, 'typeof')
        namespaces_str = Midgard.ReflectorObject.get_schema_value(gtype.name, 'namespaces')
        mrp = Midgard.ReflectorProperty(dbclass = gtype.name)

        if rdf:
            self.rdf_to_classes[rdf] = gtype.name
            self.classes_to_rdf[gtype.name] = rdf

        namespaces = {}
        if namespaces_str:
            for short, long in [namespace_str.split(":", 1) for namespace_str in namespaces_str.split(",")]:
                namespaces[short] = long

        rdf_to_fields = {}
        fields_to_rdf = {}
        for property in gobject.list_properties(gtype):
            rdf_prop = mrp.get_user_value(property.name, 'property')

            if not rdf_prop:
                continue

            for k,v in namespaces.items():
                if rdf_prop.startswith(k + ":"):
                    rdf_prop = rdf_prop.replace(k + ":", v, 1)
                    break

            rdf_to_fields[rdf_prop] = property.name
            fields_to_rdf[property.name] = rdf_prop

        if len(rdf_to_fields):
            self.rdf_to_fields[gtype.name] = rdf_to_fields
            self.fields_to_rdf[gtype.name] = fields_to_rdf

        for kid in gtype.children:
            self.read_type(kid)
