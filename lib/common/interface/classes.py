from common.interface.phedexdbs import PhEDExDBS
from common.interface.mysqlstore import MySQLStore
from common.interface.dbs import DBS
from common.interface.popdb import PopDB

class Generator(object):
    """
    Generator of various objects with a storage for singleton objects.
    """

    _singletons = {}

    def __init__(self, cls):
        self._cls = cls

    def __call__(self):
        try:
            obj = Generator._singletons[self._cls]
        except KeyError:
            obj = self._cls()
            Generator._singletons[self._cls] = obj

        return obj


class DummyInterface(object):
    def __init__(self):
        pass
            

default_interface = {
    'dataset_source': Generator(PhEDExDBS),
    'site_source': Generator(PhEDExDBS),
    'replica_source': Generator(PhEDExDBS),
    'copy': Generator(PhEDExDBS),
    'deletion': Generator(PhEDExDBS),
    'store': Generator(MySQLStore),
    'lock': Generator(DummyInterface),
    'access_history': Generator(PopDB)
}
