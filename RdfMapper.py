import gobject
from gi.repository import Midgard

class RdfMapper:
    def __init__(self, mgd):
        self.mgd = mgd

        self.classes = {}
        self.fields = {}

        self.read_types()
        print(self.classes)
        print(self.fields)

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
            self.classes[rdf] = gtype.name
            #print("%s = %s" % (gtype.name, rdf))
        else:
            #print("%s" % (gtype.name))
            pass

        namespaces = {}
        if namespaces_str:
            for short, long in [namespace_str.split(":", 1) for namespace_str in namespaces_str.split(",")]:
                namespaces[short] = long
            #print(namespaces)

        properties = {}
        for property in gobject.list_properties(gtype):
            rdf_prop = mrp.get_user_value(property.name, 'property')

            if not rdf_prop:
                continue

            for k,v in namespaces.items():
                if rdf_prop.startswith(k + ":"):
                    rdf_prop = rdf_prop.replace(k + ":", v, 1)
                    break

            properties[rdf_prop] = property.name
            #print("    %s = %s" % (property.name, rdf_prop))

        if len(properties):
            self.fields[gtype.name] = properties

        for kid in gtype.children:
            self.read_type(kid)
