import inspect

import gi
from gi.repository import Midgard
from DaemonConfig import DaemonConfig

class MidgardIniter:
    def __init__(self):
        Midgard.init()
        self.mgd = Midgard.Connection()
        self.mgd.open_config(DaemonConfig())

    def create_base(self):
        if Midgard.Storage.create_base_storage(self.mgd):
            print("Created base storage")
        else:
            raise Exception("Couldn't create base storage")

    def create_mgdschemas(self):
        transaction = Midgard.Transaction(connection = self.mgd)
        transaction.begin()

        for gtype in Midgard.Object.__gtype__.children:
            self.create_mgdschema(gtype)

        for gtype in Midgard.View.__gtype__.children:
            self.create_mgdschema(gtype)

        transaction.commit()

    def create_mgdschema(self, gtype):
        if Midgard.Storage.create(self.mgd, gtype.name):
            print("Created storage for: %s" % (gtype.name))
        else:
            raise Exception("Couldn't create storage for: %s" % (gtype.name))

        # update is needed for derived classes, and won't hurt others
        if not Midgard.Storage.update(self.mgd, gtype.name):
            raise Exception("Couldn't update storage for: %s" % (gtype.name))

        for kid in gtype.children:
            self.create_mgdschema(kid)

obj = MidgardIniter()
obj.create_base()
obj.create_mgdschemas()
