import inspect

import gi
from gi.repository import Midgard
from DaemonConfig import DaemonConfig

Midgard.init()
mgd = Midgard.Connection()
mgd.open_config(DaemonConfig())

storage = Midgard.Storage()

if storage.create_base_storage(mgd):
    print("Created base storage")
else:
    raise Exception("Couldn't create base storage")

for gtype in Midgard.Object.__gtype__.children:
    if storage.create(mgd, gtype.name):
        print("Created storage for: %s" % (gtype.name))
    else:
        raise Exception("Couldn't create storage for: %s" % (gtype.name))

for gtype in Midgard.View.__gtype__.children:
    if storage.create(mgd, gtype.name):
        print("Created storage for: %s" % (gtype.name))
    else:
        raise Exception("Couldn't create storage for: %s" % (gtype.name))
