from common.interface.phedex import PhEDExInterface
from common.interface.mysql import MySQLInterface
from common.interface.dbs import DBSInterface

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
    'dataset_source': Generator(PhEDExInterface),
    'site_source': Generator(PhEDExInterface),
    'replica_source': Generator(PhEDExInterface),
    'copy': Generator(PhEDExInterface),
    'deletion': Generator(PhEDExInterface),
    'inventory': Generator(MySQLInterface),
    'popularity': Generator(DummyInterface),
    'lock': Generator(DummyInterface)
}
