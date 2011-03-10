import os
from gi.repository import Midgard

class DaemonConfig(Midgard.Config):
    def __init__(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))

        Midgard.Config.__init__(self,
                                dbtype      = "SQLite",
                                database    = "testdb",
                                loglevel    = "warn",
                                dbdir       = current_dir + "/test",
                                sharedir    = current_dir + "/test/share",
                                vardir      = current_dir + "/test/var",
                                cachedir    = current_dir + "/test/var/cache"
        )
        
